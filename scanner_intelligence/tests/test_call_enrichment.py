import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from scanner_intelligence import (
    dispatch_pending_retranscriptions,
    get_call_enrichment,
    get_pending_call_enrichments,
    process_call_enrichment_batch,
)
from shared.ai_availability import AIServiceUnavailable


class CallEnrichmentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.calls_db = Path(self.temp_dir.name) / "calls.db"
        self.intelligence_db = Path(self.temp_dir.name) / "intelligence.db"
        with sqlite3.connect(self.calls_db) as conn:
            conn.execute(
                """
                CREATE TABLE calls (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT,
                    town TEXT,
                    derived_town TEXT,
                    dept TEXT,
                    category TEXT,
                    filename TEXT,
                    wav_path TEXT,
                    duration REAL DEFAULT 0,
                    rms REAL DEFAULT 0,
                    transcription_score REAL,
                    needs_retry INTEGER DEFAULT 0,
                    needs_review INTEGER DEFAULT 0,
                    quality_reasons TEXT,
                    profile_used TEXT,
                    transcript TEXT,
                    edited_transcript TEXT,
                    normalized_transcript TEXT,
                    raw_transcript TEXT,
                    classification TEXT
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO calls (
                    id, timestamp, town, derived_town, dept, category,
                    filename, wav_path, duration, quality_reasons,
                    transcript, classification
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        10,
                        "2026-07-29T10:00:00",
                        "Hopedale",
                        "Hopedale",
                        "police",
                        "pd",
                        "rec_10.wav",
                        "/tmp/rec_10.wav",
                        5.0,
                        "[]",
                        "Owner operator, verbal warning for speed, clear.",
                        json.dumps({"call_type": "Traffic Stop"}),
                    ),
                    (
                        11,
                        "2026-07-29T10:01:00",
                        "Hopedale",
                        "Hopedale",
                        "fire",
                        "fd",
                        "rec_11.wav",
                        "/tmp/rec_11.wav",
                        4.0,
                        "[]",
                        "Engine 3 responding.",
                        "{}",
                    ),
                ],
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_batch_persists_separately_and_transcript_edit_invalidates(self) -> None:
        now = datetime(2026, 7, 29, 10, 5)
        pending = get_pending_call_enrichments(
            db_path=str(self.calls_db),
            storage_db_path=str(self.intelligence_db),
            day="2026-07-29",
            now=now,
        )
        self.assertEqual([item["call_id"] for item in pending], [11, 10])

        def generator(candidates):
            return [
                {
                    "call_id": candidate["call_id"],
                    "enhanced_transcript": (
                        "Owner/operator, verbal warning for speed, clear."
                        if candidate["call_id"] == 10
                        else "Engine 3 responding to the call."
                    ),
                    "factual_summary": f"Prepared call {candidate['call_id']}",
                    "commentary": "Prepared commentary.",
                    "classification": {
                        "call_type": "Traffic Stop"
                        if candidate["call_id"] == 10
                        else "Fire response",
                        "urgency": "routine",
                        "lifecycle_hint": "update",
                        "outcome_type": "unknown",
                        "continuity_terms": [],
                    },
                    "confidence": 0.8,
                    "evidence": [],
                    "model": "test-model",
                }
                for candidate in candidates
            ]

        result = process_call_enrichment_batch(
            generator=generator,
            db_path=str(self.calls_db),
            storage_db_path=str(self.intelligence_db),
            day="2026-07-29",
            now=now,
        )
        self.assertEqual(
            result,
            {"ok": True, "selected": 2, "completed": 2, "failed": 0},
        )
        enrichment = get_call_enrichment(
            10,
            db_path=str(self.calls_db),
            storage_db_path=str(self.intelligence_db),
        )
        self.assertEqual(enrichment["status"], "complete")
        self.assertEqual(
            enrichment["enhanced_transcript"],
            "Owner/operator, verbal warning for speed, clear.",
        )
        self.assertEqual(
            enrichment["classification"]["call_type"],
            "Traffic Stop",
        )
        self.assertEqual(enrichment["model"], "test-model")
        self.assertEqual(
            get_pending_call_enrichments(
                db_path=str(self.calls_db),
                storage_db_path=str(self.intelligence_db),
                day="2026-07-29",
                now=now,
            ),
            [],
        )

        with sqlite3.connect(self.calls_db) as conn:
            original_classification = conn.execute(
                "SELECT classification FROM calls WHERE id = 10"
            ).fetchone()[0]
            conn.execute(
                """
                UPDATE calls
                SET edited_transcript = ?
                WHERE id = 10
                """,
                ("Owner operator, citation issued for speed, clear.",),
            )
        self.assertEqual(
            json.loads(original_classification),
            {"call_type": "Traffic Stop"},
        )
        self.assertIsNone(
            get_call_enrichment(
                10,
                db_path=str(self.calls_db),
                storage_db_path=str(self.intelligence_db),
            )
        )
        changed = get_pending_call_enrichments(
            db_path=str(self.calls_db),
            storage_db_path=str(self.intelligence_db),
            day="2026-07-29",
            now=now,
        )
        self.assertEqual([item["call_id"] for item in changed], [10])

    def test_high_confidence_validation_queues_one_bounded_retry(self) -> None:
        def generator(candidates):
            results = []
            for candidate in candidates:
                validation = {
                    "status": "plausible",
                    "request_retranscription": False,
                    "confidence": 0.9,
                    "reasons": [],
                    "explanation": "",
                }
                if candidate["call_id"] == 10:
                    validation = {
                        "status": "questionable",
                        "request_retranscription": True,
                        "confidence": 0.92,
                        "reasons": ["incoherent"],
                        "explanation": "The transcript is internally incoherent.",
                    }
                results.append(
                    {
                        "call_id": candidate["call_id"],
                        "enhanced_transcript": "",
                        "factual_summary": "Prepared summary.",
                        "commentary": "",
                        "classification": {},
                        "confidence": 0.8,
                        "evidence": [],
                        "transcript_validation": validation,
                        "model": "test-model",
                    }
                )
            return results

        process_call_enrichment_batch(
            generator=generator,
            db_path=str(self.calls_db),
            storage_db_path=str(self.intelligence_db),
            day="2026-07-29",
            now=datetime(2026, 7, 29, 10, 5),
        )
        with sqlite3.connect(self.intelligence_db) as conn:
            queued = conn.execute(
                """
                SELECT call_id, status
                FROM scanner_retranscription_requests
                """
            ).fetchall()
        self.assertEqual(queued, [(10, "queued")])

        class FakeRedis:
            def __init__(self):
                self.messages = []

            def xadd(self, stream, fields):
                self.messages.append((stream, fields))
                return "1-0"

        fake = FakeRedis()
        dispatched = dispatch_pending_retranscriptions(
            fake,
            db_path=str(self.calls_db),
            storage_db_path=str(self.intelligence_db),
            now=datetime(2026, 7, 29, 10, 6),
        )
        self.assertEqual(dispatched["dispatched"], 1)
        self.assertEqual(fake.messages[0][0], "scanner:stream:retranscribe")
        self.assertEqual(fake.messages[0][1]["call_id"], "10")
        with sqlite3.connect(self.intelligence_db) as conn:
            status = conn.execute(
                """
                SELECT status
                FROM scanner_retranscription_requests
                WHERE call_id = 10
                """
            ).fetchone()[0]
        self.assertEqual(status, "dispatched")

    def test_model_outage_releases_batch_without_burning_attempts(self) -> None:
        def unavailable(_candidates):
            raise RuntimeError("model unavailable")

        result = process_call_enrichment_batch(
            generator=unavailable,
            db_path=str(self.calls_db),
            storage_db_path=str(self.intelligence_db),
            day="2026-07-29",
            now=datetime(2026, 7, 29, 10, 5),
        )
        self.assertFalse(result["ok"])
        with sqlite3.connect(self.intelligence_db) as conn:
            rows = conn.execute(
                """
                SELECT status, attempts
                FROM scanner_call_enrichments
                ORDER BY call_id
                """
            ).fetchall()
        self.assertEqual(rows, [("pending", 0), ("pending", 0)])
        pending = get_pending_call_enrichments(
            db_path=str(self.calls_db),
            storage_db_path=str(self.intelligence_db),
            day="2026-07-29",
            now=datetime(2026, 7, 29, 10, 6),
        )
        self.assertEqual([item["call_id"] for item in pending], [11, 10])

    def test_ai_outage_defers_batch_without_reporting_failed_calls(self) -> None:
        def unavailable(_candidates):
            raise AIServiceUnavailable("AI offline")

        result = process_call_enrichment_batch(
            generator=unavailable,
            db_path=str(self.calls_db),
            storage_db_path=str(self.intelligence_db),
            day="2026-07-29",
            now=datetime(2026, 7, 29, 10, 5),
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["degraded"])
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["deferred"], 2)
        with sqlite3.connect(self.intelligence_db) as conn:
            rows = conn.execute(
                """
                SELECT status, attempts
                FROM scanner_call_enrichments
                ORDER BY call_id
                """
            ).fetchall()
        self.assertEqual(rows, [("pending", 0), ("pending", 0)])


if __name__ == "__main__":
    unittest.main()
