"""Persistent, asynchronous per-call AI enrichment.

The source ``calls`` table remains authoritative. Model output is stored in the
separate intelligence database with a fingerprint of the exact source material
that produced it, so transcript edits automatically invalidate stale results.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from shared.ai_availability import AIServiceUnavailable

from .daily_take import (
    DEFAULT_DB_PATH,
    PROMPT_VERSION,
    _connect,
    _json_object,
    _resolve_storage_path,
    ensure_intelligence_schema,
    parse_day,
)


CALL_ENRICHMENT_PROMPT_VERSION = f"{PROMPT_VERSION}-call-enrichment-v1"
logger = logging.getLogger("scanner_intelligence.call_enrichment")


def _json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


def _normalized_source_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "call_id": int(row["id"]),
        "timestamp": row["timestamp"] or "",
        "town": row["town"] or row["derived_town"] or "Unknown",
        "department": row["dept"] or "unknown",
        "feed": row["category"] or "",
        "filename": row["filename"] or "",
        "wav_path": row["wav_path"] or "",
        "duration_seconds": float(row["duration"] or 0),
        "rms": float(row["rms"] or 0),
        "transcription_score": row["transcription_score"],
        "needs_retry": bool(row["needs_retry"]),
        "needs_review": bool(row["needs_review"]),
        "quality_reasons": _json_list(row["quality_reasons"]),
        "profile_used": row["profile_used"] or "",
        "transcript": (row["best_transcript"] or "").strip(),
        "classification": _json_object(row["classification"]),
    }


def source_fingerprint(payload: Dict[str, Any]) -> str:
    """Fingerprint every source field that can materially change enrichment."""
    source = {
        "call_id": payload.get("call_id"),
        "timestamp": payload.get("timestamp"),
        "town": payload.get("town"),
        "department": payload.get("department"),
        "feed": payload.get("feed"),
        "duration_seconds": payload.get("duration_seconds"),
        "rms": payload.get("rms"),
        "transcription_score": payload.get("transcription_score"),
        "needs_retry": payload.get("needs_retry"),
        "needs_review": payload.get("needs_review"),
        "quality_reasons": payload.get("quality_reasons") or [],
        "profile_used": payload.get("profile_used"),
        "transcript": payload.get("transcript"),
        "classification": payload.get("classification") or {},
    }
    encoded = json.dumps(
        source,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_source_calls(
    db_path: str,
    day: str,
) -> List[Dict[str, Any]]:
    next_day = (
        datetime.fromisoformat(day) + timedelta(days=1)
    ).date().isoformat()
    with _connect(db_path, readonly=True) as conn:
        rows = conn.execute(
            """
            SELECT
                id, timestamp, town, derived_town, dept, category,
                filename, wav_path, duration, rms, transcription_score,
                needs_retry, needs_review, quality_reasons, profile_used,
                classification,
                COALESCE(
                    NULLIF(TRIM(edited_transcript), ''),
                    NULLIF(TRIM(transcript), ''),
                    NULLIF(TRIM(normalized_transcript), ''),
                    NULLIF(TRIM(raw_transcript), '')
                ) AS best_transcript
            FROM calls
            WHERE timestamp >= ? AND timestamp < ?
              AND COALESCE(
                    NULLIF(TRIM(edited_transcript), ''),
                    NULLIF(TRIM(transcript), ''),
                    NULLIF(TRIM(normalized_transcript), ''),
                    NULLIF(TRIM(raw_transcript), '')
                  ) IS NOT NULL
            ORDER BY id DESC
            """,
            (day, next_day),
        ).fetchall()
    payloads = [_normalized_source_payload(row) for row in rows]
    for payload in payloads:
        payload["source_fingerprint"] = source_fingerprint(payload)
    return payloads


def get_pending_call_enrichments(
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
    day: Optional[str] = None,
    limit: int = 24,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Return newest missing, changed, failed, or abandoned enrichment work."""
    resolved_day = parse_day(day, now=now)
    storage_path = _resolve_storage_path(db_path, storage_db_path)
    Path(storage_path).parent.mkdir(parents=True, exist_ok=True)
    current = now or datetime.now()
    abandoned_before = (current - timedelta(minutes=15)).isoformat(
        timespec="seconds"
    )
    with _connect(storage_path) as conn:
        ensure_intelligence_schema(conn)
        existing = {
            int(row["call_id"]): dict(row)
            for row in conn.execute(
                """
                SELECT call_id, source_fingerprint, status, attempts, updated_at
                FROM scanner_call_enrichments
                """
            )
        }

    candidates: List[Dict[str, Any]] = []
    for payload in _load_source_calls(db_path, resolved_day):
        row = existing.get(payload["call_id"])
        if row and row["source_fingerprint"] == payload["source_fingerprint"]:
            status = row["status"]
            if status in {"complete", "skipped"}:
                continue
            if status == "processing" and (row["updated_at"] or "") >= abandoned_before:
                continue
            if status == "failed" and int(row["attempts"] or 0) >= 3:
                continue
        candidates.append(payload)
        if len(candidates) >= max(1, min(int(limit), 100)):
            break
    return candidates


def _mark_processing(
    conn: sqlite3.Connection,
    candidates: Sequence[Dict[str, Any]],
    now_iso: str,
) -> None:
    for candidate in candidates:
        existing = conn.execute(
            """
            SELECT source_fingerprint, attempts, created_at
            FROM scanner_call_enrichments
            WHERE call_id = ?
            """,
            (candidate["call_id"],),
        ).fetchone()
        same_source = bool(
            existing
            and existing["source_fingerprint"]
            == candidate["source_fingerprint"]
        )
        attempts = int(existing["attempts"] or 0) + 1 if same_source else 1
        created_at = existing["created_at"] if same_source else now_iso
        conn.execute(
            """
            INSERT INTO scanner_call_enrichments (
                call_id, source_fingerprint, source_timestamp, town, dept,
                category, status, classification_json, evidence_json,
                prompt_version, attempts, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'processing', '{}', '[]', ?, ?, ?, ?)
            ON CONFLICT(call_id) DO UPDATE SET
                source_fingerprint = excluded.source_fingerprint,
                source_timestamp = excluded.source_timestamp,
                town = excluded.town,
                dept = excluded.dept,
                category = excluded.category,
                status = 'processing',
                enhanced_transcript = NULL,
                factual_summary = NULL,
                commentary = NULL,
                classification_json = '{}',
                evidence_json = '[]',
                transcript_validation_json = '{}',
                confidence = NULL,
                model = NULL,
                prompt_version = excluded.prompt_version,
                attempts = excluded.attempts,
                last_error = NULL,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                completed_at = NULL
            """,
            (
                candidate["call_id"],
                candidate["source_fingerprint"],
                candidate.get("timestamp"),
                candidate.get("town"),
                candidate.get("department"),
                candidate.get("feed"),
                CALL_ENRICHMENT_PROMPT_VERSION,
                attempts,
                created_at,
                now_iso,
            ),
        )


def _release_failed_batch(
    conn: sqlite3.Connection,
    candidates: Sequence[Dict[str, Any]],
    error: str,
    now_iso: str,
) -> None:
    call_ids = [int(candidate["call_id"]) for candidate in candidates]
    if not call_ids:
        return
    placeholders = ",".join("?" for _ in call_ids)
    conn.execute(
        f"""
        UPDATE scanner_call_enrichments
        SET status = 'pending',
            attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
            last_error = ?, updated_at = ?
        WHERE call_id IN ({placeholders})
        """,
        [error[:1000], now_iso, *call_ids],
    )


def _persist_results(
    conn: sqlite3.Connection,
    candidates: Sequence[Dict[str, Any]],
    results: Sequence[Dict[str, Any]],
    now_iso: str,
) -> Dict[str, int]:
    candidate_by_id = {
        int(candidate["call_id"]): candidate for candidate in candidates
    }
    result_by_id = {
        int(result["call_id"]): result
        for result in results
        if isinstance(result, dict)
        and isinstance(result.get("call_id"), int)
        and int(result["call_id"]) in candidate_by_id
    }
    completed = 0
    failed = 0
    for call_id, candidate in candidate_by_id.items():
        result = result_by_id.get(call_id)
        if not result:
            conn.execute(
                """
                UPDATE scanner_call_enrichments
                SET status = 'failed', last_error = ?, updated_at = ?
                WHERE call_id = ?
                """,
                ("Model response omitted this call.", now_iso, call_id),
            )
            failed += 1
            continue
        conn.execute(
            """
            UPDATE scanner_call_enrichments
            SET status = 'complete',
                enhanced_transcript = ?,
                factual_summary = ?,
                commentary = ?,
                classification_json = ?,
                evidence_json = ?,
                transcript_validation_json = ?,
                confidence = ?,
                model = ?,
                prompt_version = ?,
                last_error = NULL,
                updated_at = ?,
                completed_at = ?
            WHERE call_id = ? AND source_fingerprint = ?
            """,
            (
                result.get("enhanced_transcript") or "",
                result.get("factual_summary") or "",
                result.get("commentary") or "",
                json.dumps(result.get("classification") or {}),
                json.dumps(result.get("evidence") or []),
                json.dumps(result.get("transcript_validation") or {}),
                result.get("confidence"),
                result.get("model"),
                CALL_ENRICHMENT_PROMPT_VERSION,
                now_iso,
                now_iso,
                call_id,
                candidate["source_fingerprint"],
            ),
        )
        validation = result.get("transcript_validation") or {}
        if (
            validation.get("request_retranscription") is True
            and validation.get("status") in {"questionable", "unusable"}
            and float(validation.get("confidence") or 0) >= 0.8
            and validation.get("reasons")
        ):
            prior_requests = conn.execute(
                """
                SELECT COUNT(*)
                FROM scanner_retranscription_requests
                WHERE call_id = ?
                """,
                (call_id,),
            ).fetchone()[0]
            if int(prior_requests or 0) < 2:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO scanner_retranscription_requests (
                        call_id, source_fingerprint, status, reasons_json,
                        explanation, validation_confidence, requested_at
                    ) VALUES (?, ?, 'queued', ?, ?, ?, ?)
                    """,
                    (
                        call_id,
                        candidate["source_fingerprint"],
                        json.dumps(validation.get("reasons") or []),
                        validation.get("explanation") or "",
                        validation.get("confidence"),
                        now_iso,
                    ),
                )
        completed += 1
    return {"completed": completed, "failed": failed}


def process_call_enrichment_batch(
    generator: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]],
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
    day: Optional[str] = None,
    limit: int = 24,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Lease one batch, invoke the model callback, and persist validated output."""
    current = now or datetime.now()
    now_iso = current.isoformat(timespec="seconds")
    storage_path = _resolve_storage_path(db_path, storage_db_path)
    candidates = get_pending_call_enrichments(
        db_path=db_path,
        storage_db_path=storage_path,
        day=day,
        limit=limit,
        now=current,
    )
    if not candidates:
        return {"ok": True, "selected": 0, "completed": 0, "failed": 0}

    with _connect(storage_path) as conn:
        ensure_intelligence_schema(conn)
        _mark_processing(conn, candidates, now_iso)

    try:
        results = generator(candidates)
        if not isinstance(results, list):
            raise ValueError("call enrichment generator must return a list")
    except AIServiceUnavailable as exc:
        logger.warning("call_enrichment.ai_unavailable batch_deferred=%s", len(candidates))
        with _connect(storage_path) as conn:
            ensure_intelligence_schema(conn)
            _release_failed_batch(conn, candidates, str(exc), now_iso)
        return {
            "ok": True,
            "degraded": True,
            "selected": len(candidates),
            "completed": 0,
            "failed": 0,
            "deferred": len(candidates),
            "error": str(exc),
        }
    except Exception as exc:
        logger.exception("call_enrichment.batch_failed")
        with _connect(storage_path) as conn:
            ensure_intelligence_schema(conn)
            _release_failed_batch(conn, candidates, str(exc), now_iso)
        return {
            "ok": False,
            "selected": len(candidates),
            "completed": 0,
            "failed": len(candidates),
            "error": str(exc),
        }

    with _connect(storage_path) as conn:
        ensure_intelligence_schema(conn)
        counts = _persist_results(conn, candidates, results, now_iso)
    return {"ok": True, "selected": len(candidates), **counts}


def get_call_enrichment(
    call_id: int,
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return a complete enrichment only while its source fingerprint matches."""
    return get_call_enrichment_map(
        [int(call_id)],
        db_path=db_path,
        storage_db_path=storage_db_path,
    ).get(int(call_id))


def _enrichment_payload(enrichment: sqlite3.Row) -> Dict[str, Any]:
    return {
        "call_id": int(enrichment["call_id"]),
        "status": enrichment["status"],
        "enhanced_transcript": enrichment["enhanced_transcript"] or "",
        "factual_summary": enrichment["factual_summary"] or "",
        "commentary": enrichment["commentary"] or "",
        "classification": _json_object(enrichment["classification_json"]),
        "evidence": json.loads(enrichment["evidence_json"] or "[]"),
        "transcript_validation": _json_object(
            enrichment["transcript_validation_json"]
        ),
        "confidence": enrichment["confidence"],
        "model": enrichment["model"],
        "prompt_version": enrichment["prompt_version"],
        "completed_at": enrichment["completed_at"],
        "source_fingerprint": enrichment["source_fingerprint"],
    }


def get_call_enrichment_map(
    call_ids: Sequence[int],
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
) -> Dict[int, Dict[str, Any]]:
    """Load matching complete enrichments with one read from each database."""
    normalized_ids = list(dict.fromkeys(int(call_id) for call_id in call_ids))
    if not normalized_ids:
        return {}
    storage_path = _resolve_storage_path(db_path, storage_db_path)
    if not Path(storage_path).exists():
        return {}
    placeholders = ",".join("?" for _ in normalized_ids)
    with _connect(db_path, readonly=True) as conn:
        source_rows = conn.execute(
            f"""
            SELECT
                id, timestamp, town, derived_town, dept, category,
                filename, wav_path, duration, rms, transcription_score,
                needs_retry, needs_review, quality_reasons, profile_used,
                classification,
                COALESCE(
                    NULLIF(TRIM(edited_transcript), ''),
                    NULLIF(TRIM(transcript), ''),
                    NULLIF(TRIM(normalized_transcript), ''),
                    NULLIF(TRIM(raw_transcript), '')
                ) AS best_transcript
            FROM calls
            WHERE id IN ({placeholders})
            """,
            normalized_ids,
        ).fetchall()
    fingerprints = {
        int(row["id"]): source_fingerprint(_normalized_source_payload(row))
        for row in source_rows
    }
    with _connect(storage_path) as conn:
        table_exists = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = 'scanner_call_enrichments'
            """
        ).fetchone()
        if not table_exists:
            return {}
        enrichment_rows = conn.execute(
            f"""
            SELECT *
            FROM scanner_call_enrichments
            WHERE call_id IN ({placeholders}) AND status = 'complete'
            """,
            normalized_ids,
        ).fetchall()
    return {
        int(row["call_id"]): _enrichment_payload(row)
        for row in enrichment_rows
        if fingerprints.get(int(row["call_id"])) == row["source_fingerprint"]
    }


def dispatch_pending_retranscriptions(
    redis_client: Any,
    db_path: str = DEFAULT_DB_PATH,
    storage_db_path: Optional[str] = None,
    limit: int = 12,
    stream_key: str = "scanner:stream:retranscribe",
    profile: str = "aggressive",
    now: Optional[datetime] = None,
) -> Dict[str, int]:
    """Publish durable queued retries to the transcriber-specific Redis stream."""
    storage_path = _resolve_storage_path(db_path, storage_db_path)
    if not Path(storage_path).exists():
        return {"queued": 0, "dispatched": 0, "failed": 0}
    current = now or datetime.now()
    now_iso = current.isoformat(timespec="seconds")
    with _connect(storage_path) as conn:
        ensure_intelligence_schema(conn)
        requests = conn.execute(
            """
            SELECT *
            FROM scanner_retranscription_requests
            WHERE status = 'queued'
            ORDER BY requested_at ASC
            LIMIT ?
            """,
            (max(1, min(int(limit), 50)),),
        ).fetchall()
    if not requests:
        return {"queued": 0, "dispatched": 0, "failed": 0}

    call_ids = [int(item["call_id"]) for item in requests]
    placeholders = ",".join("?" for _ in call_ids)
    with _connect(db_path, readonly=True) as conn:
        source_rows = {
            int(row["id"]): row
            for row in conn.execute(
                f"""
                SELECT
                    id, timestamp, town, derived_town, dept, category,
                    filename, wav_path, duration, rms, transcription_score,
                    needs_retry, needs_review, quality_reasons, profile_used,
                    classification,
                    COALESCE(
                        NULLIF(TRIM(edited_transcript), ''),
                        NULLIF(TRIM(transcript), ''),
                        NULLIF(TRIM(normalized_transcript), ''),
                        NULLIF(TRIM(raw_transcript), '')
                    ) AS best_transcript
                FROM calls
                WHERE id IN ({placeholders})
                """,
                call_ids,
            )
        }

    dispatched = 0
    failed = 0
    with _connect(storage_path) as conn:
        ensure_intelligence_schema(conn)
        for request in requests:
            call_id = int(request["call_id"])
            row = source_rows.get(call_id)
            if not row:
                conn.execute(
                    """
                    UPDATE scanner_retranscription_requests
                    SET status = 'cancelled', last_error = ?
                    WHERE id = ?
                    """,
                    ("Source call no longer exists.", request["id"]),
                )
                failed += 1
                continue
            payload = _normalized_source_payload(row)
            if source_fingerprint(payload) != request["source_fingerprint"]:
                conn.execute(
                    """
                    UPDATE scanner_retranscription_requests
                    SET status = 'cancelled', last_error = ?
                    WHERE id = ?
                    """,
                    ("Source transcript changed before dispatch.", request["id"]),
                )
                failed += 1
                continue
            try:
                redis_client.xadd(
                    stream_key,
                    {
                        "request_id": str(request["id"]),
                        "call_id": str(call_id),
                        "source_fingerprint": request["source_fingerprint"],
                        "file": payload.get("wav_path") or "",
                        "filename": payload.get("filename") or "",
                        "feed": payload.get("feed") or "",
                        "profile": profile or "aggressive",
                        "reasons": request["reasons_json"] or "[]",
                    },
                )
                conn.execute(
                    """
                    UPDATE scanner_retranscription_requests
                    SET status = 'dispatched', dispatched_at = ?,
                        last_error = NULL
                    WHERE id = ?
                    """,
                    (now_iso, request["id"]),
                )
                dispatched += 1
            except Exception as exc:
                conn.execute(
                    """
                    UPDATE scanner_retranscription_requests
                    SET last_error = ?
                    WHERE id = ?
                    """,
                    (str(exc)[:1000], request["id"]),
                )
                failed += 1
    return {
        "queued": len(requests),
        "dispatched": dispatched,
        "failed": failed,
    }


def complete_retranscription_request(
    request_id: int,
    status: str,
    result: Dict[str, Any],
    storage_db_path: Optional[str] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    """Record completion from the transcriber worker."""
    normalized_status = status if status in {"complete", "failed"} else "failed"
    storage_path = _resolve_storage_path(db_path, storage_db_path)
    with _connect(storage_path) as conn:
        ensure_intelligence_schema(conn)
        conn.execute(
            """
            UPDATE scanner_retranscription_requests
            SET status = ?, completed_at = ?, result_json = ?,
                last_error = ?
            WHERE id = ?
            """,
            (
                normalized_status,
                datetime.now().isoformat(timespec="seconds"),
                json.dumps(result),
                None if normalized_status == "complete" else str(result.get("error", ""))[:1000],
                int(request_id),
            ),
        )
