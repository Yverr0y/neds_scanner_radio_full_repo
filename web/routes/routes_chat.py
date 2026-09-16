import json
import logging
import os
import sqlite3
import threading
import time
from collections import defaultdict, deque
from typing import Any, Dict, List

import requests
from flask import Blueprint, jsonify, request

import chatbot.app as chatbot_app
from scanner_intelligence import get_incident_detail, get_or_generate_daily_take
from scanner_config import TOWNS, build_chat_preset_tool_call, get_chat_preset_catalog
from shared.ai_availability import AIServiceUnavailable


chat_bp = Blueprint("scanner_chat", __name__)
logger = logging.getLogger("scanner_web.chat")
MAX_CHAT_MESSAGES = 12
MAX_CHAT_MESSAGE_CHARS = 4000
MAX_CHAT_TOTAL_CHARS = 16000
CHAT_RATE_LIMIT = int(os.environ.get("CHAT_RATE_LIMIT_PER_MINUTE", "30"))
_RATE_WINDOW_SECONDS = 60
_rate_buckets = defaultdict(deque)
_rate_lock = threading.Lock()


def _no_store_json(payload: Dict[str, Any], status: int = 200):
    response = jsonify(payload)
    response.status_code = status
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def _chat_rate_limited() -> bool:
    client_key = request.remote_addr or "unknown"
    cutoff = time.monotonic() - _RATE_WINDOW_SECONDS
    with _rate_lock:
        bucket = _rate_buckets[client_key]
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= CHAT_RATE_LIMIT:
            return True
        bucket.append(time.monotonic())
        return False


def _validated_messages(value: Any) -> List[Dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise ValueError("Body must include a non-empty 'messages' list.")
    if len(value) > MAX_CHAT_MESSAGES:
        raise ValueError(f"Chat history is limited to {MAX_CHAT_MESSAGES} messages.")

    cleaned: List[Dict[str, str]] = []
    total_chars = 0
    for item in value:
        if not isinstance(item, dict) or item.get("role") not in {"user", "assistant"}:
            raise ValueError("Messages may only use user and assistant roles.")
        content = item.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Each message must have non-empty text content.")
        content = content.strip()
        if len(content) > MAX_CHAT_MESSAGE_CHARS:
            raise ValueError(
                f"Each message is limited to {MAX_CHAT_MESSAGE_CHARS} characters."
            )
        total_chars += len(content)
        cleaned.append({"role": item["role"], "content": content})
    if total_chars > MAX_CHAT_TOTAL_CHARS:
        raise ValueError("Chat history is too large.")
    return cleaned


def _compact_items(items: List[Dict[str, Any]], label: str) -> List[str]:
    lines = []
    for item in items[:5]:
        call_id = item.get("id") or item.get("call_id")
        timestamp = item.get("timestamp") or "unknown time"
        town = item.get("town") or item.get("derived_town") or "unknown town"
        excerpt = item.get("best_transcript") or item.get("excerpt") or ""
        prefix = f"- {label}"
        if call_id:
            prefix += f" #{call_id}"
        lines.append(f"{prefix}: {timestamp}, {town}. {excerpt}".strip())
    return lines


def _answer_from_tool_result(tool_result: Dict[str, Any]) -> str:
    if not tool_result.get("ok"):
        return tool_result.get("error") or "I could not complete that request."

    if "take" in tool_result:
        take = tool_result.get("take") or {}
        lines = [
            take.get("headline") or "Ned’s Take",
            "",
            take.get("straight_summary") or "",
            "",
            f"Ned’s take: {take.get('ned_take') or ''}",
        ]
        highlights = take.get("highlights") or []
        if highlights:
            lines.extend(["", "Highlights:"])
            for highlight in highlights[:3]:
                line = f"- {highlight.get('summary') or highlight.get('title') or 'Scanner activity'}"
                if highlight.get("ned_note"):
                    line += f" {highlight['ned_note']}"
                lines.append(line)
        if take.get("disclaimer"):
            lines.extend(["", take["disclaimer"]])
        return "\n".join(line for line in lines if line is not None)

    if "total_calls" in tool_result:
        filters = tool_result.get("filters", {})
        scope = ", ".join(str(v) for v in filters.values() if v) or "all scanner calls"
        return (
            f"For {scope}: {tool_result.get('total_calls', 0)} total calls, "
            f"{tool_result.get('calls_with_transcript', 0)} with transcripts."
        )

    if "ticket_breakdown" in tool_result:
        breakdown = tool_result["ticket_breakdown"]
        lines = [
            "Likely ticket-related outcomes:",
            f"- Warnings: {breakdown.get('warnings', 0)}",
            f"- Citations: {breakdown.get('citations', 0)}",
            f"- Total likely outcomes: {breakdown.get('total_likely_enforcement_outcomes', 0)}",
        ]
        evidence = _compact_items(tool_result.get("citations", []), "Call")
        if evidence:
            lines.extend(["", "Evidence:", *evidence])
        note = tool_result.get("note")
        if note:
            lines.extend(["", note])
        return "\n".join(lines)

    for key, label in (("warning_count", "likely warnings"), ("citation_count", "likely citations")):
        if key in tool_result:
            lines = [f"I found {tool_result.get(key, 0)} {label}."]
            evidence = _compact_items(tool_result.get("citations", []), "Call")
            if evidence:
                lines.extend(["", "Evidence:", *evidence])
            note = tool_result.get("note")
            if note:
                lines.extend(["", note])
            return "\n".join(lines)

    if "type_breakdown" in tool_result:
        count = tool_result.get("count", 0)
        breakdown = tool_result.get("type_breakdown", {})
        lines = [f"I found {count} fire announcement matches."]
        if breakdown:
            lines.append(
                "Breakdown: "
                + ", ".join(f"{name}: {value}" for name, value in breakdown.items())
            )
        evidence = _compact_items(tool_result.get("results", []), "Call")
        if evidence:
            lines.extend(["", "Evidence:", *evidence])
        note = tool_result.get("note")
        if note:
            lines.extend(["", note])
        return "\n".join(lines)

    if "results" in tool_result:
        count = tool_result.get("count", len(tool_result.get("results", [])))
        lines = [f"I found {count} matching calls."]
        matches = _compact_items(tool_result.get("results", []), "Call")
        if matches:
            lines.extend(["", *matches])
        return "\n".join(lines)

    if "call" in tool_result:
        call = tool_result.get("call") or {}
        if not tool_result.get("found", True):
            return "I could not find that call."
        return "\n".join(
            [
                f"Call #{call.get('id')}",
                f"Time: {call.get('timestamp') or 'unknown'}",
                f"Town: {call.get('town') or call.get('derived_town') or 'unknown'}",
                f"Department: {call.get('dept') or 'unknown'}",
                "",
                call.get("best_transcript") or "No transcript available.",
            ]
        )

    return json.dumps(tool_result, indent=2, default=str)


def _take_citations(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    citations: List[Dict[str, Any]] = []

    def collect(section: Dict[str, Any]) -> None:
        for highlight in section.get("highlights") or []:
            citations.extend(highlight.get("citations") or [])
        for department in section.get("departments") or []:
            collect(department)

    take = result.get("take") or {}
    collect(take)
    for town in take.get("towns") or []:
        collect(town)
    return citations


def _offline_chat_fallback(messages: List[Dict[str, str]]) -> Dict[str, Any]:
    question = messages[-1]["content"] if messages else ""
    normalized = question.casefold()
    summary_requested = any(
        phrase in normalized
        for phrase in (
            "summary",
            "summarize",
            "recap",
            "ned's take",
            "neds take",
            "what happened",
        )
    )
    if not summary_requested:
        return {
            "ok": True,
            "degraded": True,
            "ai_available": False,
            "answer": (
                "The AI assistant is temporarily offline. Scanner audio, "
                "transcripts, the archive, and saved quick reports are still available."
            ),
            "citations": [],
        }

    town = next(
        (
            config["name"]
            for slug, config in TOWNS.items()
            if slug in normalized or config["name"].casefold() in normalized
        ),
        None,
    )
    result = get_or_generate_daily_take(
        db_path=chatbot_app.SCANNER_DB_PATH,
        day="today",
        town=town,
    )
    return {
        "ok": True,
        "degraded": True,
        "ai_available": False,
        "answer": (
            "AI commentary is temporarily offline, so here is the verified "
            "scanner recap:\n\n"
            + _answer_from_tool_result(result)
        ),
        "citations": _take_citations(result),
    }


@chat_bp.route("/scanner/api/chat/local", methods=["POST"])
def api_chat_local():
    if _chat_rate_limited():
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Scanner Chat is receiving too many requests. Try again shortly.",
                }
            ),
            429,
            {"Retry-After": "60"},
        )

    payload = request.get_json(silent=True) or {}
    preset_id = (payload.get("preset_id") or "").strip()
    town_slug = (payload.get("town_slug") or "").strip().lower()
    user_messages = payload.get("messages")

    if preset_id:
        preset = build_chat_preset_tool_call(preset_id, town_slug)
        if not preset:
            return jsonify({"ok": False, "error": "Unknown preset or town."}), 400

        try:
            tool_result = chatbot_app.execute_tool_call_from_dict(preset["tool_call"])
            answer = _answer_from_tool_result(tool_result)
            status = 200 if tool_result.get("ok") else 500
            return jsonify(
                {
                    "ok": tool_result.get("ok", False),
                    "answer": answer,
                    "citations": tool_result.get("citations", []),
                    "preset_id": preset["preset_id"],
                    "preset_label": preset["preset_label"],
                    "prompt": preset["prompt"],
                    "town_slug": preset["town_slug"],
                    "town_name": preset["town_name"],
                }
            ), status
        except Exception:
            logger.exception("chat.local.preset_failed preset=%s town=%s", preset_id, town_slug)
            return jsonify({"ok": False, "error": "The saved scanner prompt could not be completed."}), 500

    try:
        user_messages = _validated_messages(user_messages)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    try:
        result = chatbot_app.run_tool_loop(user_messages)
        if result.get("ok") and not result.get("answer") and result.get("tool_result"):
            result["answer"] = _answer_from_tool_result(result["tool_result"])
        status = 200 if result.get("ok") else 500
        result.pop("raw", None)
        result.pop("tool_result", None)
        return jsonify(result), status
    except AIServiceUnavailable:
        logger.info("chat.local.ai_unavailable fallback=deterministic")
        try:
            return jsonify(_offline_chat_fallback(user_messages)), 200
        except Exception:
            logger.warning("chat.local.offline_fallback_failed", exc_info=True)
            return jsonify(
                {
                    "ok": True,
                    "degraded": True,
                    "ai_available": False,
                    "answer": (
                        "The AI assistant is temporarily offline. Live scanner audio "
                        "and the archive are still available."
                    ),
                    "citations": [],
                }
            ), 200
    except requests.HTTPError:
        logger.exception("chat.local.vllm_http_failed")
        return jsonify(
            {
                "ok": False,
                "error": "The local language model is unavailable right now.",
            }
        ), 502
    except Exception:
        logger.exception("chat.local.failed")
        return jsonify({"ok": False, "error": "Scanner Chat could not complete that request."}), 500


@chat_bp.route("/scanner/api/chat/local/health", methods=["GET"])
def api_chat_local_health():
    db_ok = False
    vllm_ok = False
    try:
        db_uri = f"file:{os.path.abspath(chatbot_app.SCANNER_DB_PATH)}?mode=ro"
        with sqlite3.connect(db_uri, uri=True, timeout=2) as conn:
            db_ok = conn.execute("SELECT 1").fetchone()[0] == 1
    except Exception:
        logger.warning("chat.health.database_unavailable")
    try:
        response = requests.get(f"{chatbot_app.VLLM_BASE_URL}/models", timeout=2)
        vllm_ok = response.ok
    except requests.RequestException:
        logger.info("chat.health.vllm_unavailable")
    return jsonify(
        {
            "ok": db_ok and vllm_ok,
            "status": "healthy" if db_ok and vllm_ok else "degraded",
            "database_ok": db_ok,
            "vllm_ok": vllm_ok,
            "model": chatbot_app.VLLM_MODEL,
            "tools": [tool["function"]["name"] for tool in chatbot_app.TOOLS],
            "presets_available": db_ok,
        }
    )


@chat_bp.route("/scanner/api/chat/local/tools", methods=["GET"])
def api_chat_local_tools():
    return jsonify({"ok": True, "tools": chatbot_app.TOOLS})


@chat_bp.route("/scanner/api/chat/local/presets", methods=["GET"])
def api_chat_local_presets():
    catalog = get_chat_preset_catalog()
    return jsonify({"ok": True, **catalog})


@chat_bp.route("/scanner/api/neds-take", methods=["GET"])
def api_neds_take():
    day = request.args.get("date", "today")
    town = (request.args.get("town") or "").strip() or None
    edition_type = (request.args.get("edition") or "").strip() or None
    try:
        result = get_or_generate_daily_take(
            db_path=chatbot_app.SCANNER_DB_PATH,
            day=day,
            town=town,
            edition_type=edition_type,
        )
        return _no_store_json(result)
    except ValueError as exc:
        return _no_store_json({"ok": False, "error": str(exc)}, 400)
    except Exception:
        logger.exception("neds_take.failed date=%s town=%s", day, town or "all")
        return _no_store_json(
            {"ok": False, "error": "Ned’s Take is unavailable right now."},
            500,
        )


@chat_bp.route("/scanner/api/incident/<incident_key>/take", methods=["GET"])
def api_incident_take(incident_key):
    detail = get_incident_detail(
        incident_key,
        db_path=chatbot_app.SCANNER_DB_PATH,
    )
    if not detail:
        return _no_store_json(
            {"ok": False, "error": "Scanner incident not found."},
            404,
        )
    # The incident page is a factual evidence view. Commentary is intentionally
    # confined to the dedicated Ned's Take daily report.
    detail.pop("commentary", None)
    for call in detail.get("calls", []):
        call.pop("ai_enrichment", None)
    return _no_store_json(detail)
