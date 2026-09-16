# -----------------------------------------------------------------------------
# Tool call extraction and execution from AI response
# -----------------------------------------------------------------------------
import json
import re

def extract_and_execute_tool_call_from_response(response: str) -> dict:
    """
    Detects a <tools>...</tools> block in the AI response, extracts the JSON tool call,
    executes it using execute_tool_call_from_dict, and returns the result.
    Returns None if no tool call is found.
    """
    # Look for <tools>...</tools> block
    match = re.search(r'<tools>\s*(\{.*?\})\s*</tools>', response, re.DOTALL)
    if not match:
        return None
    try:
        tool_call_json = match.group(1)
        tool_call = json.loads(tool_call_json)
        return execute_tool_call_from_dict(tool_call)
    except Exception as e:
        return {"ok": False, "error": f"Failed to extract/execute tool call: {e}"}

# Example usage:
# ai_response = "...<tools>\n{\n  \"name\": \"find_fire_announcements\", ...}\n</tools>..."
# result = extract_and_execute_tool_call_from_response(ai_response)
import logging
import os
import sqlite3
import copy
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request
from scanner_intelligence import get_or_generate_daily_take
from shared.ai_availability import AIServiceUnavailable
from shared.transcript_quality import repetition_hallucination_metrics

logger = logging.getLogger("scanner_chatbot")

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

VLLM_BASE_URL = os.environ.get("VLLM_BASE_URL", "http://127.0.0.1:30001/v1")
MODEL_CATALOG_PATH = os.environ.get(
    "MODEL_CATALOG_PATH",
    "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/transcriber/model_catalog.json",
)
SCANNER_DB_PATH = os.environ.get(
    "SCANNER_DB_PATH",
    "/home/ned/data/scanner_calls/scanner_calls.db",
)
CHAT_MAX_TOOL_ROUNDS = int(os.environ.get("CHAT_MAX_TOOL_ROUNDS", "4"))
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("VLLM_TIMEOUT_SECONDS", "120"))
VLLM_UNAVAILABLE_BACKOFF_SECONDS = max(
    30,
    int(os.environ.get("VLLM_UNAVAILABLE_BACKOFF_SECONDS", "300")),
)
_vllm_unavailable_until = 0.0


def vllm_in_backoff() -> bool:
    return time.monotonic() < _vllm_unavailable_until


def _mark_vllm_unavailable() -> None:
    global _vllm_unavailable_until
    _vllm_unavailable_until = time.monotonic() + VLLM_UNAVAILABLE_BACKOFF_SECONDS


def _mark_vllm_available() -> None:
    global _vllm_unavailable_until
    _vllm_unavailable_until = 0.0


def _post_vllm(url: str, payload: Dict[str, Any], timeout_seconds: int):
    if vllm_in_backoff():
        raise AIServiceUnavailable("The local AI service is temporarily offline.")
    try:
        response = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=timeout_seconds,
        )
    except (requests.ConnectionError, requests.Timeout) as exc:
        _mark_vllm_unavailable()
        raise AIServiceUnavailable(
            "The local AI service is temporarily offline."
        ) from exc
    if response.status_code == 429 or response.status_code >= 500:
        _mark_vllm_unavailable()
        raise AIServiceUnavailable(
            "The local AI service is temporarily offline."
        )
    return response


def load_default_chat_model_from_catalog() -> Optional[str]:
    try:
        with open(MODEL_CATALOG_PATH, "r", encoding="utf-8") as fh:
            catalog = json.load(fh)
    except Exception:
        return None

    models = catalog.get("models", {})
    preferred_key = os.environ.get("VLLM_MODEL_KEY") or os.environ.get("LOCAL_LLM_MODEL_KEY")
    if preferred_key and isinstance(models.get(preferred_key), dict):
        model_value = models[preferred_key].get("model")
        if model_value:
            return model_value

    default_key = catalog.get("default_chat_model") or catalog.get("default_model")
    if default_key and isinstance(models.get(default_key), dict):
        default_cfg = models[default_key]
        if default_cfg.get("kind") == "chat" and default_cfg.get("model"):
            return default_cfg["model"]

    for model_cfg in models.values():
        if isinstance(model_cfg, dict) and model_cfg.get("kind") == "chat" and model_cfg.get("model"):
            return model_cfg["model"]

    return None


VLLM_MODEL = (
    os.environ.get("VLLM_MODEL")
    or os.environ.get("LOCAL_LLM_MODEL")
    or load_default_chat_model_from_catalog()
    or "Qwen/Qwen2.5-14B-Instruct-AWQ"
)

# -----------------------------------------------------------------------------
# Flask app
# -----------------------------------------------------------------------------

app = Flask(__name__, static_folder="static", static_url_path="/static")

# -----------------------------------------------------------------------------
# Schema constants
# -----------------------------------------------------------------------------

TABLE_NAME = "calls"

COL_ID = "id"
COL_TOWN = "town"
COL_STATE = "state"
COL_DEPT = "dept"
COL_CATEGORY = "category"
COL_FILENAME = "filename"
COL_JSON_PATH = "json_path"
COL_WAV_PATH = "wav_path"
COL_DURATION = "duration"
COL_RMS = "rms"
COL_TRANSCRIPT = "transcript"
COL_EDITED_TRANSCRIPT = "edited_transcript"
COL_TIMESTAMP = "timestamp"
COL_REVIEWED = "reviewed"
COL_PLAY_COUNT = "play_count"
COL_CLASSIFICATION = "classification"
COL_INTENT_LABELED = "intent_labeled"
COL_INTENT_LABELED_AT = "intent_labeled_at"
COL_EMBEDDING = "embedding"
COL_EXTRA = "extra"
COL_RAW_TRANSCRIPT = "raw_transcript"
COL_NORMALIZED_TRANSCRIPT = "normalized_transcript"
COL_TRANSCRIPTION_SCORE = "transcription_score"
COL_NEEDS_RETRY = "needs_retry"
COL_NEEDS_REVIEW = "needs_review"
COL_QUALITY_REASONS = "quality_reasons"
COL_PROFILE_USED = "profile_used"
COL_RETRY_PROFILES_TRIED = "retry_profiles_tried"
COL_TRANSCRIPTION_ENGINE = "transcription_engine"
COL_TRANSCRIPTION_MODEL = "transcription_model"
COL_HOOK_REQUEST = "hook_request"
COL_DERIVED_ADDRESS = "derived_address"
COL_DERIVED_STREET = "derived_street"
COL_DERIVED_ADDR_NUM = "derived_addr_num"
COL_DERIVED_TOWN = "derived_town"
COL_DERIVED_LAT = "derived_lat"
COL_DERIVED_LNG = "derived_lng"
COL_ADDRESS_CONFIDENCE = "address_confidence"
COL_SAVE_FOR_EVAL = "save_for_eval"
COL_FREEZE_FOR_TESTING = "freeze_for_testing"

# -----------------------------------------------------------------------------
# Heuristic outcome patterns
# -----------------------------------------------------------------------------

WARNING_PATTERNS = [
    "%warning issued%",
    "%issued a warning%",
    "%verbal warning%",
    "%written warning%",
    "%given a warning%",
    "%warning for%",
    "%advised and warned%",
    "%operator warned%",
    "%warning to operator%",
    "%warning given%",
]

CITATION_PATTERNS = [
    "%citation issued%",
    "%issued a citation%",
    "%issued citation%",
    "%written citation%",
    "%citation for%",
    "%ticket issued%",
    "%issued a ticket%",
    "%issued ticket%",
    "%summons issued%",
    "%criminal application issued%",
    "%civil citation%",
]

FIRE_ANNOUNCEMENT_PATTERNS = [
    # recall / recalling
    "%recall%",
    "%recalling%",
    "%announcing a recall%",
    "%product recall%",
    "%consumer recall%",
    "%units recalled%",
    "%recalling units%",
    "%recall to station%",
    # coverage / covering
    "%coverage%",
    "%providing coverage%",
    "%covering%",
    "%cover assignment%",
    "%station coverage%",
    "%mutual aid coverage%",
    # broadcast style
    "%be advised%",
    "%all units%",
]

# -----------------------------------------------------------------------------
# Prompt
# -----------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a local scanner-call assistant for a private scanner application.

Rules:
1. Use tools whenever the user asks for current or database-backed information.
2. Never invent calls, units, addresses, times, counts, outcomes, classifications, or citations.
3. If a tool returns no results, say so plainly.
4. Be concise, accurate, and direct.
5. Prefer tool use over guessing.
6. Never mention internal implementation details, SQL, schemas, or server internals.
7. If the user asks an ambiguous question, do your best with the available tools and explain any ambiguity briefly.
8. If the user asks about warnings, use count_warnings when appropriate.
9. If the user asks about citations, tickets, or summonses, use count_citations or count_tickets as appropriate.
10. If the user asks about fire recall announcements, recalling units, coverage, or fire broadcast traffic, use find_fire_announcements.
11. If a tool returns citations, include a short Evidence section with call IDs and timestamps. Do not invent citations.
12. For "tickets", treat that as enforcement outcome analysis and return a breakdown of likely warnings versus likely citations when available.
13. If the user asks for "Ned's Take", a daily recap, or what happened today, use get_neds_take.
14. Treat incident counts from get_neds_take as estimates because one incident may span several transmissions.
"""

# -----------------------------------------------------------------------------
# Tool definitions
# -----------------------------------------------------------------------------

TOOLS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_stats",
            "description": "Get high-level counts and basic stats from the scanner calls database.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {"type": "string"},
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_calls",
            "description": "Search scanner calls by town, department, date range, street, unit, or text.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {"type": "string"},
                    "start_time": {
                        "type": "string",
                        "description": "ISO datetime, YYYY-MM-DD, or relative term like today or yesterday"
                    },
                    "end_time": {
                        "type": "string",
                        "description": "ISO datetime, YYYY-MM-DD, or relative term like today or yesterday"
                    },
                    "street": {"type": "string"},
                    "unit": {"type": "string"},
                    "text": {"type": "string"},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_call_details",
            "description": "Get details for one specific call by ID.",
            "parameters": {
                "type": "object",
                "properties": {
                    "call_id": {"type": "integer"}
                },
                "required": ["call_id"],
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "semantic_search_transcripts",
            "description": "Search transcripts by meaning or keywords. This v1 implementation uses best-effort text matching across transcript fields.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "town": {"type": "string"},
                    "department": {"type": "string"},
                    "start_time": {
                        "type": "string",
                        "description": "ISO datetime, YYYY-MM-DD, or relative term like today or yesterday"
                    },
                    "end_time": {
                        "type": "string",
                        "description": "ISO datetime, YYYY-MM-DD, or relative term like today or yesterday"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 25
                    }
                },
                "required": ["query"],
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "count_warnings",
            "description": "Count likely warnings using transcript text heuristics. Useful for questions like how many warnings were issued in Hopedale yesterday.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {
                        "type": "string",
                        "description": "Usually police for warning-related enforcement questions."
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    },
                    "limit_examples": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 10
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "count_citations",
            "description": "Count likely citations or tickets using transcript text heuristics. Useful for questions about citations, tickets, or summonses.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {
                        "type": "string",
                        "description": "Usually police for citation-related enforcement questions."
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    },
                    "limit_examples": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 10
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "count_tickets",
            "description": "For ticket-related questions, return an enforcement breakdown of likely warnings versus likely citations, with totals and evidence.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {
                        "type": "string",
                        "description": "Usually police for ticket-related enforcement questions."
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    },
                    "limit_examples": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 10
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_fire_announcements",
            "description": "Find fire department announcement-style traffic mentioning recall, recalling, coverage, mutual aid coverage, or broadcast phrases like all units or be advised.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "department": {
                        "type": "string",
                        "description": "Defaults to fire if omitted."
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or relative values like today or yesterday."
                    },
                    "query": {
                        "type": "string",
                        "description": "Optional extra keyword like battery, product, station, mutual aid, or recall."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 25
                    }
                },
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_neds_take",
            "description": "Get a grounded daily scanner recap with department-aware incident lifecycle grouping, source citations, and Ned-style humor without subject filtering.",
            "parameters": {
                "type": "object",
                "properties": {
                    "town": {"type": "string"},
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format or a relative value like today or yesterday."
                    },
                    "edition_type": {
                        "type": "string",
                        "enum": ["rolling", "final"]
                    }
                },
                "additionalProperties": False
            }
        }
    },
]

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(SCANNER_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_department(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    v = value.strip().lower()

    mapping = {
        "fd": "fire",
        "fire": "fire",
        "fire department": "fire",
        "pd": "police",
        "police": "police",
        "police department": "police",
        "ticket": "police",
        "tickets": "police",
        "warning": "police",
        "warnings": "police",
        "citation": "police",
        "citations": "police",
        "summons": "police",
        "summonses": "police",
        "recall": "fire",
        "coverage": "fire",
        "ems": "unknown",
        "medical": "unknown",
        "unknown": "unknown",
    }

    return mapping.get(v, v)


def normalize_town(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.strip()


def resolve_relative_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    v = value.strip().lower()
    now = datetime.now()

    if v == "today":
        return now.strftime("%Y-%m-%d")
    if v == "yesterday":
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    if v == "tomorrow":
        return (now + timedelta(days=1)).strftime("%Y-%m-%d")

    return value.strip()


def try_parse_dateish(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    value = resolve_relative_date(value)

    fmts = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in fmts:
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%Y-%m-%d":
                return dt.strftime("%Y-%m-%d")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    return value


def shorten_text(value: Optional[str], max_len: int = 400) -> Optional[str]:
    if not value:
        return value
    value = value.strip()
    if len(value) <= max_len:
        return value
    return value[:max_len].rstrip() + "..."


def get_best_transcript_expr() -> str:
    return (
        f"COALESCE(NULLIF(TRIM({COL_EDITED_TRANSCRIPT}), ''), "
        f"NULLIF(TRIM({COL_TRANSCRIPT}), ''), "
        f"NULLIF(TRIM({COL_NORMALIZED_TRANSCRIPT}), ''), "
        f"NULLIF(TRIM({COL_RAW_TRANSCRIPT}), ''))"
    )


def build_date_filters(
    where: List[str],
    params: List[Any],
    start_time: Optional[str],
    end_time: Optional[str],
) -> None:
    if start_time:
        if len(start_time) == 10:
            where.append(f"date({COL_TIMESTAMP}) >= date(?)")
        else:
            where.append(f"{COL_TIMESTAMP} >= ?")
        params.append(start_time)

    if end_time:
        if len(end_time) == 10:
            where.append(f"date({COL_TIMESTAMP}) <= date(?)")
        else:
            where.append(f"{COL_TIMESTAMP} <= ?")
        params.append(end_time)


def build_citations_from_rows(rows: List[sqlite3.Row], transcript_key: str = "best_transcript") -> List[Dict[str, Any]]:
    citations: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        citations.append(
            {
                "call_id": item.get("id"),
                "timestamp": item.get("timestamp"),
                "town": item.get("town"),
                "dept": item.get("dept"),
                "derived_street": item.get("derived_street"),
                "derived_town": item.get("derived_town"),
                "wav_path": item.get("wav_path"),
                "excerpt": shorten_text(item.get(transcript_key), 180),
            }
        )
    return citations


def build_pattern_clause(expr: str, patterns: List[str]) -> Tuple[str, List[Any]]:
    clauses = []
    params: List[Any] = []
    for pattern in patterns:
        clauses.append(f"LOWER({expr}) LIKE LOWER(?)")
        params.append(pattern)
    return "(" + " OR ".join(clauses) + ")", params


def classify_fire_announcement(text: Optional[str]) -> str:
    if not text:
        return "unknown"

    t = text.lower()

    if "recall" in t or "recalling" in t:
        return "recall"
    if "coverage" in t or "covering" in t or "cover assignment" in t:
        return "coverage"
    if "be advised" in t or "all units" in t:
        return "broadcast"

    return "announcement"

# -----------------------------------------------------------------------------
# Tool implementations
# -----------------------------------------------------------------------------

def tool_get_stats(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department)
    date = try_parse_dateish(date)

    sql = f"SELECT COUNT(*) AS total_calls FROM {TABLE_NAME}"
    where: List[str] = []
    params: List[Any] = []

    if town:
        where.append(f"LOWER({COL_TOWN}) = LOWER(?)")
        params.append(town)

    if department:
        where.append(f"LOWER({COL_DEPT}) = LOWER(?)")
        params.append(department)

    if date:
        where.append(f"date({COL_TIMESTAMP}) = date(?)")
        params.append(date)

    if where:
        sql += " WHERE " + " AND ".join(where)

    transcript_expr = get_best_transcript_expr()
    transcript_sql = f"SELECT COUNT(*) AS transcript_count FROM {TABLE_NAME}"
    transcript_where = where.copy()
    transcript_where.append(f"{transcript_expr} IS NOT NULL")
    transcript_sql += " WHERE " + " AND ".join(transcript_where)

    logger.debug("tool.get_stats.query filters=%s", len(params))

    with db_connect() as conn:
        total_calls = conn.execute(sql, params).fetchone()["total_calls"]
        transcript_count = conn.execute(transcript_sql, params).fetchone()["transcript_count"]

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "date": date,
        },
        "total_calls": total_calls,
        "calls_with_transcript": transcript_count,
        "citations": [],
    }


def tool_search_calls(
    town: Optional[str] = None,
    department: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    street: Optional[str] = None,
    unit: Optional[str] = None,
    text: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department)
    start_time = try_parse_dateish(start_time)
    end_time = try_parse_dateish(end_time)
    limit = max(1, min(int(limit or 20), 50))

    best_transcript_expr = get_best_transcript_expr()

    sql = f"""
        SELECT
            {COL_ID} AS id,
            {COL_TOWN} AS town,
            {COL_DEPT} AS dept,
            {COL_TIMESTAMP} AS timestamp,
            {COL_DERIVED_STREET} AS derived_street,
            {COL_DERIVED_TOWN} AS derived_town,
            {COL_WAV_PATH} AS wav_path,
            {COL_REVIEWED} AS reviewed,
            {COL_NEEDS_REVIEW} AS needs_review,
            {COL_NEEDS_RETRY} AS needs_retry,
            {best_transcript_expr} AS best_transcript
        FROM {TABLE_NAME}
    """

    where: List[str] = []
    params: List[Any] = []

    if town:
        where.append(f"LOWER({COL_TOWN}) = LOWER(?)")
        params.append(town)

    if department:
        where.append(f"LOWER({COL_DEPT}) = LOWER(?)")
        params.append(department)

    build_date_filters(where, params, start_time, end_time)

    if street:
        street_like = f"%{street}%"
        where.append(
            f"""(
                LOWER(COALESCE({COL_DERIVED_STREET}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_EDITED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_NORMALIZED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_RAW_TRANSCRIPT}, '')) LIKE LOWER(?)
            )"""
        )
        params.extend([street_like, street_like, street_like, street_like, street_like])

    if unit:
        unit_like = f"%{unit}%"
        where.append(
            f"""(
                LOWER(COALESCE({COL_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_EDITED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_NORMALIZED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_RAW_TRANSCRIPT}, '')) LIKE LOWER(?)
            )"""
        )
        params.extend([unit_like, unit_like, unit_like, unit_like])

    if text:
        text_like = f"%{text}%"
        where.append(
            f"""(
                LOWER(COALESCE({COL_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_EDITED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_NORMALIZED_TRANSCRIPT}, '')) LIKE LOWER(?)
                OR LOWER(COALESCE({COL_RAW_TRANSCRIPT}, '')) LIKE LOWER(?)
            )"""
        )
        params.extend([text_like, text_like, text_like, text_like])

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += f" ORDER BY {COL_TIMESTAMP} DESC LIMIT ?"
    params.append(limit)

    logger.debug("tool.search_calls.query filters=%s limit=%s", len(params) - 1, limit)

    with db_connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    results = []
    for row in rows:
        item = dict(row)
        item["best_transcript"] = shorten_text(item.get("best_transcript"), 400)
        results.append(item)

    citations = build_citations_from_rows(rows)

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "start_time": start_time,
            "end_time": end_time,
            "street": street,
            "unit": unit,
            "text": text,
            "limit": limit,
        },
        "count": len(results),
        "results": results,
        "citations": citations,
    }


def tool_get_call_details(call_id: int) -> Dict[str, Any]:
    best_transcript_expr = get_best_transcript_expr()

    sql = f"""
        SELECT
            *,
            {best_transcript_expr} AS best_transcript
        FROM {TABLE_NAME}
        WHERE {COL_ID} = ?
        LIMIT 1
    """

    logger.debug("tool.get_call_details.query call_id=%s", call_id)

    with db_connect() as conn:
        row = conn.execute(sql, (call_id,)).fetchone()

    if not row:
        return {
            "ok": True,
            "found": False,
            "call_id": call_id,
            "citations": [],
        }

    item = dict(row)
    item["best_transcript"] = shorten_text(item.get("best_transcript"), 800)

    citations = [
        {
            "call_id": item.get("id"),
            "timestamp": item.get("timestamp"),
            "town": item.get("town"),
            "dept": item.get("dept"),
            "derived_street": item.get("derived_street"),
            "derived_town": item.get("derived_town"),
            "wav_path": item.get("wav_path"),
            "excerpt": shorten_text(item.get("best_transcript"), 180),
        }
    ]

    return {
        "ok": True,
        "found": True,
        "call": item,
        "citations": citations,
    }


def tool_semantic_search_transcripts(
    query: str,
    town: Optional[str] = None,
    department: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    return tool_search_calls(
        town=town,
        department=department,
        start_time=start_time,
        end_time=end_time,
        text=query,
        limit=max(1, min(int(limit or 10), 25)),
    )


def _count_outcome_by_patterns(
    patterns: List[str],
    town: Optional[str],
    department: Optional[str],
    date: Optional[str],
    limit_examples: int,
    label: str,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department or "police")
    date = try_parse_dateish(date)
    limit_examples = max(0, min(int(limit_examples or 5), 10))

    transcript_expr = get_best_transcript_expr()
    pattern_clause, pattern_params = build_pattern_clause(transcript_expr, patterns)

    base_sql = f"""
        SELECT
            {COL_ID} AS id,
            {COL_TOWN} AS town,
            {COL_DEPT} AS dept,
            {COL_TIMESTAMP} AS timestamp,
            {COL_DERIVED_STREET} AS derived_street,
            {COL_DERIVED_TOWN} AS derived_town,
            {COL_WAV_PATH} AS wav_path,
            {transcript_expr} AS best_transcript
        FROM {TABLE_NAME}
    """

    where = [f"{transcript_expr} IS NOT NULL"]
    params: List[Any] = []

    if town:
        where.append(f"LOWER({COL_TOWN}) = LOWER(?)")
        params.append(town)

    if department:
        where.append(f"LOWER({COL_DEPT}) = LOWER(?)")
        params.append(department)

    if date:
        where.append(f"date({COL_TIMESTAMP}) = date(?)")
        params.append(date)

    where.append(pattern_clause)
    params.extend(pattern_params)

    filtered_sql = base_sql + " WHERE " + " AND ".join(where)
    count_sql = f"SELECT COUNT(*) AS outcome_count FROM ({filtered_sql})"

    logger.debug("tool.%s.count_query filters=%s", label, len(params))

    with db_connect() as conn:
        outcome_count = conn.execute(count_sql, params).fetchone()["outcome_count"]

        rows: List[sqlite3.Row] = []
        if limit_examples > 0:
            examples_sql = filtered_sql + f" ORDER BY {COL_TIMESTAMP} DESC LIMIT ?"
            example_params = params + [limit_examples]

            logger.debug(
                "tool.%s.examples_query filters=%s limit=%s",
                label,
                len(params),
                limit_examples,
            )

            rows = conn.execute(examples_sql, example_params).fetchall()

    examples = []
    for row in rows:
        item = dict(row)
        item["best_transcript"] = shorten_text(item.get("best_transcript"), 300)
        examples.append(item)

    citations = build_citations_from_rows(rows)

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "date": date,
        },
        f"{label}_count": outcome_count,
        "heuristic": True,
        "note": f"{label.capitalize()} count is based on transcript text patterns and may miss cases if no transcript exists or the wording differs.",
        "examples": examples,
        "citations": citations,
    }


def tool_count_warnings(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
    limit_examples: int = 5,
) -> Dict[str, Any]:
    return _count_outcome_by_patterns(
        patterns=WARNING_PATTERNS,
        town=town,
        department=department,
        date=date,
        limit_examples=limit_examples,
        label="warning",
    )


def tool_count_citations(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
    limit_examples: int = 5,
) -> Dict[str, Any]:
    return _count_outcome_by_patterns(
        patterns=CITATION_PATTERNS,
        town=town,
        department=department,
        date=date,
        limit_examples=limit_examples,
        label="citation",
    )


def tool_count_tickets(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
    limit_examples: int = 5,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department or "police")
    date = try_parse_dateish(date)
    limit_examples = max(0, min(int(limit_examples or 5), 10))

    warning_result = tool_count_warnings(
        town=town,
        department=department,
        date=date,
        limit_examples=limit_examples,
    )
    citation_result = tool_count_citations(
        town=town,
        department=department,
        date=date,
        limit_examples=limit_examples,
    )

    if not warning_result.get("ok", False):
        return warning_result
    if not citation_result.get("ok", False):
        return citation_result

    warning_count = warning_result.get("warning_count", 0)
    citation_count = citation_result.get("citation_count", 0)

    combined_citations = []
    seen_ids = set()

    for source in [warning_result.get("citations", []), citation_result.get("citations", [])]:
        for citation in source:
            key = citation.get("call_id")
            if key not in seen_ids:
                seen_ids.add(key)
                combined_citations.append(citation)

    combined_citations = combined_citations[: max(limit_examples, 1) * 2]

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "date": date,
        },
        "heuristic": True,
        "note": "Ticket-related breakdown is based on transcript text patterns and may miss cases if no transcript exists or the wording differs.",
        "ticket_breakdown": {
            "warnings": warning_count,
            "citations": citation_count,
            "total_likely_enforcement_outcomes": warning_count + citation_count,
        },
        "warning_examples": warning_result.get("examples", []),
        "citation_examples": citation_result.get("examples", []),
        "citations": combined_citations,
    }


def tool_find_fire_announcements(
    town: Optional[str] = None,
    department: Optional[str] = None,
    date: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    town = normalize_town(town)
    department = normalize_department(department or "fire")
    date = try_parse_dateish(date)
    limit = max(1, min(int(limit or 10), 25))

    transcript_expr = get_best_transcript_expr()
    pattern_clause, pattern_params = build_pattern_clause(transcript_expr, FIRE_ANNOUNCEMENT_PATTERNS)

    sql = f"""
        SELECT
            {COL_ID} AS id,
            {COL_TOWN} AS town,
            {COL_DEPT} AS dept,
            {COL_TIMESTAMP} AS timestamp,
            {COL_DERIVED_STREET} AS derived_street,
            {COL_DERIVED_TOWN} AS derived_town,
            {COL_WAV_PATH} AS wav_path,
            {transcript_expr} AS best_transcript
        FROM {TABLE_NAME}
    """

    where = [f"{transcript_expr} IS NOT NULL"]
    params: List[Any] = []

    if town:
        where.append(f"LOWER({COL_TOWN}) = LOWER(?)")
        params.append(town)

    if department:
        where.append(f"LOWER({COL_DEPT}) = LOWER(?)")
        params.append(department)

    if date:
        where.append(f"date({COL_TIMESTAMP}) = date(?)")
        params.append(date)

    where.append(pattern_clause)
    params.extend(pattern_params)

    if query:
        q_like = f"%{query}%"
        where.append(f"LOWER({transcript_expr}) LIKE LOWER(?)")
        params.append(q_like)

    sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {COL_TIMESTAMP} DESC LIMIT ?"
    params.append(limit)

    logger.debug("tool.find_fire_announcements.query filters=%s limit=%s", len(params) - 1, limit)

    with db_connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    results = []
    for row in rows:
        item = dict(row)
        item["best_transcript"] = shorten_text(item.get("best_transcript"), 300)
        item["announcement_type"] = classify_fire_announcement(item.get("best_transcript"))
        results.append(item)

    citations = build_citations_from_rows(rows)

    type_breakdown: Dict[str, int] = {}
    for item in results:
        atype = item["announcement_type"]
        type_breakdown[atype] = type_breakdown.get(atype, 0) + 1

    return {
        "ok": True,
        "filters": {
            "town": town,
            "department": department,
            "date": date,
            "query": query,
            "limit": limit,
        },
        "count": len(results),
        "type_breakdown": type_breakdown,
        "results": results,
        "citations": citations,
        "note": "Fire announcement matching is heuristic and based on transcript text patterns such as recall, recalling, coverage, all units, and be advised.",
    }


def tool_get_neds_take(
    town: Optional[str] = None,
    date: Optional[str] = None,
    edition_type: Optional[str] = None,
) -> Dict[str, Any]:
    result = get_or_generate_daily_take(
        db_path=SCANNER_DB_PATH,
        town=normalize_town(town),
        day=date,
        edition_type=edition_type,
        commentary_generator=generate_neds_take_commentary,
    )
    citations: List[Dict[str, Any]] = []
    for highlight in result.get("take", {}).get("highlights", []):
        citations.extend(highlight.get("citations", []))
    result["citations"] = citations
    return result


# -----------------------------------------------------------------------------
# Tool registry
# -----------------------------------------------------------------------------

TOOL_FUNCTIONS = {
    "get_stats": tool_get_stats,
    "search_calls": tool_search_calls,
    "get_call_details": tool_get_call_details,
    "semantic_search_transcripts": tool_semantic_search_transcripts,
    "count_warnings": tool_count_warnings,
    "count_citations": tool_count_citations,
    "count_tickets": tool_count_tickets,
    "find_fire_announcements": tool_find_fire_announcements,
    "get_neds_take": tool_get_neds_take,
}

# -----------------------------------------------------------------------------
# Tool call interpreter
# -----------------------------------------------------------------------------

def execute_tool_call_from_dict(tool_call: dict) -> dict:
    """
    Given a tool call dict like:
    {
        "name": "find_fire_announcements",
        "arguments": {
            "town": "Hopedale",
            "department": "fire",
            "date": "today"
        }
    }
    execute the corresponding tool and return the result.
    """
    name = tool_call.get("name")
    arguments = tool_call.get("arguments", {})
    if name not in TOOL_FUNCTIONS:
        return {"ok": False, "error": f"Unknown tool: {name}"}
    try:
        result = TOOL_FUNCTIONS[name](**arguments)
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}

# Example usage:
# tool_call = {
#     "name": "find_fire_announcements",
#     "arguments": {"town": "Hopedale", "department": "fire", "date": "today"}
# }
# result = execute_tool_call_from_dict(tool_call)

# -----------------------------------------------------------------------------
# vLLM client helpers
# -----------------------------------------------------------------------------

def get_served_vllm_models() -> List[str]:
    response = requests.get(
        f"{VLLM_BASE_URL}/models",
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    models = []
    for item in payload.get("data", []):
        if isinstance(item, dict) and item.get("id"):
            models.append(item["id"])
    return models


def call_vllm_chat(
    messages: List[Dict[str, Any]],
    tools: Optional[List[Dict[str, Any]]] = None,
    temperature: float = 0,
    max_tokens: Optional[int] = None,
    timeout_seconds: Optional[int] = None,
    reasoning_effort: Optional[str] = None,
    response_format: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    global VLLM_MODEL

    payload: Dict[str, Any] = {
        "model": VLLM_MODEL,
        "messages": messages,
        "temperature": temperature,
    }
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    if reasoning_effort:
        payload["chat_template_kwargs"] = {
            "reasoning_effort": reasoning_effort,
        }
    if response_format:
        payload["response_format"] = response_format

    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    url = f"{VLLM_BASE_URL}/chat/completions"
    request_timeout = timeout_seconds or REQUEST_TIMEOUT_SECONDS
    response = _post_vllm(url, payload, request_timeout)

    if response.status_code == 404:
        try:
            served_models = get_served_vllm_models()
        except Exception:
            served_models = []

        if served_models and VLLM_MODEL not in served_models:
            old_model = VLLM_MODEL
            VLLM_MODEL = served_models[0]
            payload["model"] = VLLM_MODEL
            logger.warning(
                "vllm.model_fallback requested=%s served=%s",
                old_model,
                VLLM_MODEL,
            )
            response = _post_vllm(url, payload, request_timeout)

    response.raise_for_status()
    _mark_vllm_available()
    return response.json()


def _neds_take_scope_key(scope: Dict[str, Any]) -> str:
    town = scope.get("town")
    department = scope.get("department")
    if town and department:
        return f"town:{town}/department:{department}"
    if town:
        return f"town:{town}"
    if department:
        return f"network/department:{department}"
    return "network"


def _neds_take_section_facts(
    take_section: Dict[str, Any],
    fact_section: Dict[str, Any],
) -> Dict[str, Any]:
    breakdowns = fact_section.get("breakdowns", {})
    classified_types = [
        (call_type, count)
        for call_type, count in breakdowns.get("call_types", {}).items()
        if call_type != "Unclassified"
    ]
    return {
        "key": _neds_take_scope_key(take_section.get("scope", {})),
        "scope": take_section.get("scope", {}),
        "top_call_types": classified_types[:5],
        "enforcement": breakdowns.get("enforcement", {}),
    }


def _build_neds_take_commentary_facts(result: Dict[str, Any]) -> Dict[str, Any]:
    take = result.get("take", {})
    fact_pack = result.get("fact_pack", {})
    sections = [_neds_take_section_facts(take, fact_pack)]
    fact_towns = {
        town.get("scope", {}).get("town"): town
        for town in fact_pack.get("towns", [])
    }
    for take_town in take.get("towns", []):
        town_name = take_town.get("scope", {}).get("town")
        fact_town = fact_towns.get(town_name, {})
        sections.append(_neds_take_section_facts(take_town, fact_town))
        fact_departments = {
            department.get("scope", {}).get("department"): department
            for department in fact_town.get("departments", [])
        }
        for take_department in take_town.get("departments", []):
            department_name = take_department.get("scope", {}).get("department")
            sections.append(
                _neds_take_section_facts(
                    take_department,
                    fact_departments.get(department_name, {}),
                )
            )
    incidents: Dict[str, Dict[str, Any]] = {}

    def collect_incidents(section: Any) -> None:
        if isinstance(section, dict):
            incident_key = section.get("incident_key")
            if isinstance(incident_key, str) and incident_key not in incidents:
                incidents[incident_key] = {
                    "key": incident_key,
                    "town": section.get("town"),
                    "service": section.get("service"),
                    "call_type": section.get("call_type"),
                    "verified_summary": section.get("summary"),
                    "verified_outcome": section.get("outcome"),
                    "transmissions": [
                        {
                            "call_id": citation.get("call_id"),
                            "transcript": citation.get("excerpt"),
                        }
                        for citation in section.get("citations", [])
                        if isinstance(citation, dict)
                    ],
                }
            for nested in section.values():
                collect_incidents(nested)
        elif isinstance(section, list):
            for nested in section:
                collect_incidents(nested)

    collect_incidents(take)
    return {
        "day": result.get("day"),
        "source_watermark": result.get("source_watermark", {}),
        "sections": sections,
        "incidents": list(incidents.values()),
    }


def _parse_json_object(text: str) -> Dict[str, Any]:
    normalized = (text or "").strip()
    if normalized.startswith("```"):
        normalized = re.sub(r"^```(?:json)?\s*", "", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\s*```$", "", normalized)
    parsed = json.loads(normalized)
    if not isinstance(parsed, dict):
        raise ValueError("LLM commentary response was not a JSON object")
    return parsed


def _valid_generated_commentary(
    value: Any,
) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = re.sub(r"\s+", " ", value).strip()
    if not normalized or len(normalized) > 600:
        return None
    # Counts and timings remain code-rendered facts. Reject prose that tries to
    # introduce new numeric claims.
    if re.search(r"\d", normalized):
        return None
    return normalized


def generate_neds_take_commentary(result: Dict[str, Any]) -> Dict[str, Any]:
    """Generate grounded commentary, retrying one truncated JSON response."""
    facts = _build_neds_take_commentary_facts(result)
    system_prompt = """
You write only the sarcastic riff for "Ned's Take." Verified summaries and call
details are rendered separately. Return JSON only. Do not summarize the calls
and do not infer what happened. Never add an incident, motive, severity,
outcome, response, unit, timing, person, fire condition, medical condition,
arrest, transport, weapon, or enforcement action. Treat each supplied section
as a comedy brief, not as an invitation to complete a story.

Write one or two concise punchline sentences for every supplied section key,
plus one concise sentence for each supplied incident. Incident jokes may use
only that incident's verified summary, outcome, and actual transcript excerpts.
Use dry local humor about radio chatter, bureaucracy, paperwork, dispatch
rhythms, and the exact supplied call-type or enforcement labels. Vary metaphors,
sentence structures, and targets on every request. Avoid reusable catchphrases.
Do not write any digits. Commentary can be conversational and profane.

Special rule: if a Hopedale section has citations greater than zero, its
commentary must begin exactly: "Holy shit, they gave a citation!!!" If Hopedale
has warnings but no citations, riff freshly on Hopedale's tendency to issue
warnings; do not use a fixed stock line.

Required shape:
{"sections":{"<section key>":{"commentary":"..."}},"incidents":{"<incident key>":{"commentary":"..."}}}
""".strip()
    configured_temperature = float(
        os.environ.get("NEDS_TAKE_LLM_TEMPERATURE", "0.9")
    )
    configured_max_tokens = int(
        os.environ.get("NEDS_TAKE_LLM_MAX_TOKENS", "3000")
    )
    configured_timeout = int(
        os.environ.get("NEDS_TAKE_LLM_TIMEOUT_SECONDS", "30")
    )
    attempts = (
        (
            configured_temperature,
            configured_max_tokens,
            configured_timeout,
            system_prompt,
        ),
        (
            min(configured_temperature, 0.55),
            min(max(configured_max_tokens * 2, 6000), 8000),
            max(configured_timeout * 3, 90),
            system_prompt
            + "\n\nBe especially terse. The previous JSON response was "
            "incomplete; close every object and return the required shape.",
        ),
    )
    generated_sections: Dict[str, Any] = {}
    generated_incidents: Dict[str, Any] = {}
    for attempt_number, (
        temperature,
        max_tokens,
        timeout_seconds,
        prompt,
    ) in enumerate(
        attempts,
        start=1,
    ):
        choice: Dict[str, Any] = {}
        content = ""
        try:
            response = call_vllm_chat(
                [
                    {"role": "system", "content": prompt},
                    {
                        "role": "user",
                        "content": json.dumps(
                            facts,
                            separators=(",", ":"),
                        ),
                    },
                ],
                temperature=temperature,
                max_tokens=max_tokens,
                timeout_seconds=timeout_seconds,
                reasoning_effort="low",
                response_format={"type": "json_object"},
            )
            choice = response.get("choices", [{}])[0]
            content = choice.get("message", {}).get("content") or ""
            generated = _parse_json_object(content)
            candidate_sections = generated.get("sections")
            if not isinstance(candidate_sections, dict):
                raise ValueError(
                    "LLM commentary response did not contain sections"
                )
            candidate_incidents = generated.get("incidents")
            generated_sections = candidate_sections
            generated_incidents = (
                candidate_incidents
                if isinstance(candidate_incidents, dict)
                else {}
            )
            break
        except AIServiceUnavailable:
            raise
        except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
            if attempt_number >= len(attempts):
                raise
            logger.warning(
                "neds_take.commentary_response_retry "
                "attempt=%s finish_reason=%s chars=%s error=%s",
                attempt_number,
                choice.get("finish_reason") or "request-error",
                len(content),
                str(exc)[:240],
            )

    enriched = copy.deepcopy(result.get("take", {}))
    def apply_section(section: Dict[str, Any]) -> None:
        scope_key = _neds_take_scope_key(section.get("scope", {}))
        generated_section = generated_sections.get(scope_key, {})
        if isinstance(generated_section, dict):
            commentary = _valid_generated_commentary(
                generated_section.get("commentary"),
            )
            if commentary:
                section["ned_take"] = commentary
        for highlight in section.get("highlights", []):
            incident = generated_incidents.get(highlight.get("incident_key"), {})
            commentary = _valid_generated_commentary(
                incident.get("commentary") if isinstance(incident, dict) else None
            )
            if commentary:
                highlight["ned_note"] = commentary
        for department in section.get("departments", []):
            apply_section(department)

    apply_section(enriched)
    for town in enriched.get("towns", []):
        apply_section(town)
    return enriched


def generate_incident_commentary(detail: Dict[str, Any]) -> Dict[str, Any]:
    """Write one variable riff using an incident's actual grouped transmissions."""
    facts = {
        "town": detail.get("town"),
        "department": detail.get("department"),
        "call_type": detail.get("call_type"),
        "verified_summary": detail.get("summary"),
        "outcome": detail.get("outcome"),
        "closed": detail.get("closed"),
        "lifecycle_stages": detail.get("lifecycle_stages", []),
        "transmissions": [
            {
                "lifecycle_stage": call.get("lifecycle_stage"),
                "transcript": (call.get("transcript") or "")[:500],
            }
            for call in detail.get("calls", [])[:20]
        ],
    }
    prompt = """
Write one short, variable "Ned's Take" paragraph about this grouped scanner
incident. Use only the supplied facts and actual transcript text. Sarcasm,
local color, and profanity are allowed. Do not invent a person, action, motive,
severity, outcome, response, citation, arrest, transport, fire condition, or
medical condition. If the radio never stated an outcome, do not supply one.
Do not repeat numeric timing; the page renders that separately.

Return JSON only: {"commentary":"..."}
""".strip()
    response = call_vllm_chat(
        [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(facts, separators=(",", ":"))},
        ],
        temperature=float(os.environ.get("NEDS_TAKE_LLM_TEMPERATURE", "0.9")),
        max_tokens=int(os.environ.get("NEDS_TAKE_INCIDENT_MAX_TOKENS", "500")),
        timeout_seconds=int(os.environ.get("NEDS_TAKE_LLM_TIMEOUT_SECONDS", "30")),
        reasoning_effort="low",
        response_format={"type": "json_object"},
    )
    content = (
        response.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    generated = _parse_json_object(content)
    commentary = _valid_generated_commentary(generated.get("commentary"))
    if not commentary:
        raise ValueError("incident commentary was empty or introduced numeric claims")
    return {
        **detail,
        "commentary": commentary,
        "commentary_generator": "local-llm",
        "commentary_generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _bounded_generated_text(value: Any, max_chars: int) -> str:
    if not isinstance(value, str):
        return ""
    normalized = re.sub(r"\s+", " ", value).strip()
    return normalized[:max_chars].rstrip()


def _verified_evidence_quotes(
    transcript: str,
    value: Any,
) -> List[str]:
    """Keep only model evidence that is actually present in the transcript."""
    if not isinstance(value, list):
        return []
    normalized_transcript = re.sub(r"\s+", " ", transcript).casefold()
    verified: List[str] = []
    for item in value[:5]:
        quote = _bounded_generated_text(item, 180)
        if not quote:
            continue
        if re.sub(r"\s+", " ", quote).casefold() in normalized_transcript:
            verified.append(quote)
    return verified


def _validated_enhanced_transcript(original: str, value: Any) -> str:
    """Accept conservative cleanups while rejecting summary-like inventions."""
    enhanced = _bounded_generated_text(value, 3000)
    normalized_original = re.sub(r"\s+", " ", original or "").strip()
    if not enhanced or not normalized_original:
        return ""
    if (
        repetition_hallucination_metrics(normalized_original)[
            "is_repetition_loop"
        ]
        or repetition_hallucination_metrics(enhanced)["is_repetition_loop"]
    ):
        return ""
    if enhanced.casefold() == normalized_original.casefold():
        return ""
    original_tokens = re.findall(r"[a-z0-9]+", normalized_original.casefold())
    enhanced_tokens = re.findall(r"[a-z0-9]+", enhanced.casefold())
    if not enhanced_tokens:
        return ""
    if len(enhanced_tokens) > (len(original_tokens) * 1.6) + 8:
        return ""
    if len(original_tokens) >= 4:
        original_vocabulary = set(original_tokens)
        overlap = sum(
            token in original_vocabulary for token in set(enhanced_tokens)
        ) / max(len(set(enhanced_tokens)), 1)
        if overlap < 0.45:
            return ""
    return enhanced


def _generate_call_enrichment_batch_once(
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Make one model request for a bounded group of transmissions."""
    if not candidates:
        return []
    request_calls = [
        {
            "call_id": candidate.get("call_id"),
            "town": candidate.get("town"),
            "department": candidate.get("department"),
            "feed": candidate.get("feed"),
            "timestamp": candidate.get("timestamp"),
            "duration_seconds": candidate.get("duration_seconds"),
            "audio_quality": {
                "rms": candidate.get("rms"),
                "transcription_score": candidate.get("transcription_score"),
                "needs_retry": candidate.get("needs_retry"),
                "needs_review": candidate.get("needs_review"),
                "quality_reasons": candidate.get("quality_reasons") or [],
                "profile_used": candidate.get("profile_used"),
            },
            "existing_classification": candidate.get("classification") or {},
            "transcript": candidate.get("transcript") or "",
        }
        for candidate in candidates
    ]
    prompt = """
You prepare asynchronous scanner-call enrichment suggestions. Each input is one
completed radio transmission, which may be only a fragment of a larger
incident. Use only its transcript and supplied metadata. Do not invent missing
facts or complete an unfinished story.

For every call return:
- an enhanced_transcript that conservatively cleans punctuation, spacing, and
  obvious scanner transcription formatting while preserving the original
  meaning and uncertainty;
- a concise factual summary;
- an optional one-sentence sarcastic "Ned's Take" only when the transcript
  contains enough substance for a fair joke; otherwise use an empty string;
- suggested classification fields: call_type, agency, urgency,
  lifecycle_hint, outcome_type, and continuity_terms;
- confidence from zero to one;
- up to five short verbatim evidence quotes copied from the transcript.
- transcript_validation with status, request_retranscription, confidence,
  reasons, and a short explanation.

Allowed urgency values: routine, elevated, urgent, emergency, unknown.
Allowed lifecycle_hint values: dispatched, responding, on_scene, update,
returning, in_quarters, cleared, terminated, unknown.
Allowed outcome_type values: warning, citation, arrest, transport,
report_taken, gone_on_arrival, no_action, cleared, not_heard, unknown.
Allowed transcript validation statuses: plausible, questionable, unusable.
Allowed validation reasons: repetition, incoherent, hallucination_pattern,
language_mismatch, truncated, impossible_phrase, metadata_conflict.

Use unknown or an empty value when evidence is absent. A classification is a
suggestion and must not claim that an outcome occurred unless an evidence quote
states it. Commentary may be conversational or profane, but cannot invent a
person, action, severity, motive, outcome, weapon, medical condition, fire
condition, citation, arrest, or transport.

Transcript validation cannot hear the audio. It may use the supplied acoustic
quality metrics and internal transcript coherence only. Do not request another
transcription merely because a scanner transmission is short, says "received,"
or contains jargon. The enhanced transcript is not a chance to summarize,
expand abbreviations speculatively, add missing words, or repair uncertainty by
guessing. Use "[unclear]" when necessary. Request another transcription only
for a concrete allowed reason with high confidence. Return JSON only.

Required shape:
{"calls":[{"call_id":1,"enhanced_transcript":"","factual_summary":"","commentary":"","classification":{"call_type":"","agency":"","urgency":"unknown","lifecycle_hint":"unknown","outcome_type":"unknown","continuity_terms":[]},"confidence":0.0,"evidence":[],"transcript_validation":{"status":"plausible","request_retranscription":false,"confidence":0.0,"reasons":[],"explanation":""}}]}
""".strip()
    response = call_vllm_chat(
        [
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": json.dumps(
                    {"calls": request_calls},
                    separators=(",", ":"),
                ),
            },
        ],
        temperature=float(
            os.environ.get("NEDS_TAKE_CALL_ENRICHMENT_TEMPERATURE", "0.35")
        ),
        max_tokens=int(
            os.environ.get("NEDS_TAKE_CALL_ENRICHMENT_MAX_TOKENS", "6000")
        ),
        timeout_seconds=int(
            os.environ.get("NEDS_TAKE_CALL_ENRICHMENT_TIMEOUT_SECONDS", "60")
        ),
        reasoning_effort="low",
        response_format={"type": "json_object"},
    )
    content = (
        response.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    parsed = _parse_json_object(content)
    raw_calls = parsed.get("calls")
    if not isinstance(raw_calls, list):
        raise ValueError("call enrichment response did not contain calls")

    candidate_by_id = {
        int(candidate["call_id"]): candidate
        for candidate in candidates
        if isinstance(candidate.get("call_id"), int)
    }
    allowed_urgency = {"routine", "elevated", "urgent", "emergency", "unknown"}
    allowed_lifecycle = {
        "dispatched",
        "responding",
        "on_scene",
        "update",
        "returning",
        "in_quarters",
        "cleared",
        "terminated",
        "unknown",
    }
    allowed_outcomes = {
        "warning",
        "citation",
        "arrest",
        "transport",
        "report_taken",
        "gone_on_arrival",
        "no_action",
        "cleared",
        "not_heard",
        "unknown",
    }
    allowed_validation_statuses = {"plausible", "questionable", "unusable"}
    allowed_validation_reasons = {
        "repetition",
        "incoherent",
        "hallucination_pattern",
        "language_mismatch",
        "truncated",
        "impossible_phrase",
        "metadata_conflict",
    }
    results: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for raw in raw_calls:
        if not isinstance(raw, dict):
            continue
        try:
            call_id = int(raw.get("call_id"))
        except (TypeError, ValueError):
            continue
        candidate = candidate_by_id.get(call_id)
        if not candidate or call_id in seen:
            continue
        seen.add(call_id)
        transcript = candidate.get("transcript") or ""
        evidence = _verified_evidence_quotes(transcript, raw.get("evidence"))
        raw_classification = raw.get("classification")
        if not isinstance(raw_classification, dict):
            raw_classification = {}
        urgency = _bounded_generated_text(
            raw_classification.get("urgency"),
            20,
        ).lower()
        lifecycle = _bounded_generated_text(
            raw_classification.get("lifecycle_hint"),
            30,
        ).lower()
        outcome = _bounded_generated_text(
            raw_classification.get("outcome_type"),
            30,
        ).lower()
        continuity_terms = []
        normalized_transcript = re.sub(r"\s+", " ", transcript).casefold()
        raw_terms = raw_classification.get("continuity_terms")
        if isinstance(raw_terms, list):
            for term_value in raw_terms[:12]:
                term = _bounded_generated_text(term_value, 80)
                if (
                    term
                    and term.casefold() in normalized_transcript
                    and term not in continuity_terms
                ):
                    continuity_terms.append(term)
        try:
            confidence = max(0.0, min(float(raw.get("confidence")), 1.0))
        except (TypeError, ValueError):
            confidence = 0.0
        if not evidence:
            confidence = min(confidence, 0.45)
            if outcome not in {"not_heard", "unknown"}:
                outcome = "unknown"
        classification = {
            "call_type": _bounded_generated_text(
                raw_classification.get("call_type"),
                80,
            ),
            "agency": _bounded_generated_text(
                raw_classification.get("agency"),
                80,
            ),
            "urgency": urgency if urgency in allowed_urgency else "unknown",
            "lifecycle_hint": (
                lifecycle if lifecycle in allowed_lifecycle else "unknown"
            ),
            "outcome_type": (
                outcome if outcome in allowed_outcomes else "unknown"
            ),
            "continuity_terms": continuity_terms,
        }
        raw_validation = raw.get("transcript_validation")
        if not isinstance(raw_validation, dict):
            raw_validation = {}
        validation_status = _bounded_generated_text(
            raw_validation.get("status"),
            20,
        ).lower()
        if validation_status not in allowed_validation_statuses:
            validation_status = "plausible"
        validation_reasons = []
        raw_reasons = raw_validation.get("reasons")
        if isinstance(raw_reasons, list):
            for reason_value in raw_reasons:
                reason = _bounded_generated_text(reason_value, 40).lower()
                if (
                    reason in allowed_validation_reasons
                    and reason not in validation_reasons
                ):
                    validation_reasons.append(reason)
        try:
            validation_confidence = max(
                0.0,
                min(float(raw_validation.get("confidence")), 1.0),
            )
        except (TypeError, ValueError):
            validation_confidence = 0.0
        request_retranscription = bool(
            raw_validation.get("request_retranscription") is True
            and validation_status in {"questionable", "unusable"}
            and validation_confidence >= 0.8
            and validation_reasons
        )
        transcript_validation = {
            "status": validation_status,
            "request_retranscription": request_retranscription,
            "confidence": round(validation_confidence, 3),
            "reasons": validation_reasons,
            "explanation": _bounded_generated_text(
                raw_validation.get("explanation"),
                300,
            ),
        }
        commentary = _valid_generated_commentary(raw.get("commentary")) or ""
        results.append(
            {
                "call_id": call_id,
                "enhanced_transcript": _validated_enhanced_transcript(
                    transcript,
                    raw.get("enhanced_transcript"),
                ),
                "factual_summary": _bounded_generated_text(
                    raw.get("factual_summary"),
                    320,
                ),
                "commentary": commentary,
                "classification": classification,
                "confidence": round(confidence, 3),
                "evidence": evidence,
                "transcript_validation": transcript_validation,
                "model": VLLM_MODEL,
            }
        )
    return results


def _repetition_loop_enrichment(
    candidate: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Return a deterministic rejection for a severe transcription loop."""
    transcript = candidate.get("transcript") or ""
    repetition = repetition_hallucination_metrics(transcript)
    if not repetition["is_repetition_loop"]:
        return None
    repeated_word = repetition.get("dominant_token") or "word"
    return {
        "call_id": int(candidate["call_id"]),
        "enhanced_transcript": "",
        "factual_summary": (
            f'Transcript flagged as a likely repetition loop involving '
            f'"{repeated_word}"; a comparison transcription was requested.'
        ),
        "commentary": "",
        "classification": {
            "call_type": "",
            "agency": "",
            "urgency": "unknown",
            "lifecycle_hint": "unknown",
            "outcome_type": "unknown",
            "continuity_terms": [],
        },
        "confidence": 0.0,
        "evidence": [],
        "transcript_validation": {
            "status": "unusable",
            "request_retranscription": True,
            "confidence": 0.99,
            "reasons": ["repetition", "hallucination_pattern"],
            "explanation": (
                "Deterministic validation found a dominant repeated token or "
                "phrase covering most of the transcript."
            ),
        },
        "model": "deterministic-repetition-validator",
    }


def generate_call_enrichment_batch(
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate enrichment in small batches, splitting malformed responses."""
    if not candidates:
        return []
    deterministic_results: List[Dict[str, Any]] = []
    model_candidates: List[Dict[str, Any]] = []
    for candidate in candidates:
        repetition_result = _repetition_loop_enrichment(candidate)
        if repetition_result:
            deterministic_results.append(repetition_result)
        else:
            model_candidates.append(candidate)
    configured_size = int(
        os.environ.get("NEDS_TAKE_CALL_ENRICHMENT_MODEL_BATCH_SIZE", "6")
    )
    chunk_size = max(1, min(configured_size, 12))

    def generate_with_recovery(
        batch: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        try:
            return _generate_call_enrichment_batch_once(batch)
        except (json.JSONDecodeError, ValueError) as exc:
            if len(batch) <= 1:
                raise
            midpoint = len(batch) // 2
            logger.warning(
                "call_enrichment.response_split batch_size=%s error=%s",
                len(batch),
                str(exc)[:240],
            )
            return [
                *generate_with_recovery(batch[:midpoint]),
                *generate_with_recovery(batch[midpoint:]),
            ]

    results: List[Dict[str, Any]] = list(deterministic_results)
    for start in range(0, len(model_candidates), chunk_size):
        results.extend(
            generate_with_recovery(
                model_candidates[start : start + chunk_size]
            )
        )
    return results


def execute_tool_call(tool_call: Dict[str, Any]) -> Dict[str, Any]:
    fn = tool_call.get("function", {})
    name = fn.get("name")
    raw_arguments = fn.get("arguments", "{}")

    logger.info("tool.execute name=%s", name)

    if name not in TOOL_FUNCTIONS:
        return {
            "ok": False,
            "error": f"Unknown tool: {name}"
        }

    try:
        arguments = json.loads(raw_arguments) if isinstance(raw_arguments, str) else raw_arguments
    except json.JSONDecodeError as exc:
        return {
            "ok": False,
            "error": f"Invalid JSON arguments for tool {name}: {str(exc)}"
        }

    try:
        result = TOOL_FUNCTIONS[name](**arguments)
        logger.info("tool.execute.complete name=%s ok=%s", name, result.get("ok"))
        return result
    except TypeError as exc:
        return {
            "ok": False,
            "error": f"Invalid arguments for tool {name}: {str(exc)}"
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": f"Tool {name} failed: {str(exc)}"
        }

# -----------------------------------------------------------------------------
# Citation extraction
# -----------------------------------------------------------------------------

def extract_citations_from_tool_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    seen: set = set()

    for msg in messages:
        if msg.get("role") != "tool":
            continue

        content = msg.get("content")
        if not content:
            continue

        try:
            payload = json.loads(content)
        except Exception:
            continue

        citations = payload.get("citations", [])
        if not isinstance(citations, list):
            continue

        for citation in citations:
            if not isinstance(citation, dict):
                continue

            key = (
                citation.get("call_id"),
                citation.get("timestamp"),
                citation.get("dept"),
            )

            if key in seen:
                continue

            seen.add(key)
            collected.append(citation)

    return collected

# -----------------------------------------------------------------------------
# Chat orchestration
# -----------------------------------------------------------------------------

def run_tool_loop(user_messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    messages: List[Dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(user_messages)

    for round_num in range(CHAT_MAX_TOOL_ROUNDS):
        logger.debug(
            "tool_loop.round.start round=%s messages=%s",
            round_num + 1,
            len(messages),
        )
        response = call_vllm_chat(messages, tools=TOOLS)
        logger.debug("tool_loop.round.response round=%s", round_num + 1)
        choice = response["choices"][0]["message"]


        tool_calls = choice.get("tool_calls", [])
        content = choice.get("content")

        # --- Qwen-style tool call support ---
        qwen_tool_call = None
        if not tool_calls and content:
            try:
                parsed = json.loads(content)
                if (
                    isinstance(parsed, dict)
                    and "name" in parsed
                    and "arguments" in parsed
                ):
                    # Synthesize a tool_call dict compatible with execute_tool_call
                    qwen_tool_call = {
                        "id": "qwen-fake-id",
                        "function": {
                            "name": parsed["name"],
                            "arguments": json.dumps(parsed["arguments"]),
                        },
                    }
            except Exception:
                pass

        if not tool_calls and not qwen_tool_call:
            citations = extract_citations_from_tool_messages(messages)
            return {
                "ok": True,
                "answer": content or "",
                "citations": citations,
                "raw": response,
            }

        # If Qwen tool call detected, treat as single tool call

        if qwen_tool_call:
            # Qwen-style: execute tool and return result directly, skip follow-up LLM round
            tool_result = execute_tool_call(qwen_tool_call)
            return {
                "ok": True,
                "answer": None,
                "tool_result": tool_result,
                "citations": tool_result.get("citations", []),
                "raw": tool_result,
            }

        # OpenAI-style: continue as before
        assistant_message: Dict[str, Any] = {
            "role": "assistant",
            "content": content,
            "tool_calls": tool_calls,
        }
        messages.append(assistant_message)

        for tool_call in tool_calls:
            tool_result = execute_tool_call(tool_call)
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.get("id", "unknown"),
                    "content": json.dumps(tool_result, default=str),
                }
            )

    citations = extract_citations_from_tool_messages(messages)
    return {
        "ok": False,
        "error": f"Exceeded max tool rounds ({CHAT_MAX_TOOL_ROUNDS}).",
        "citations": citations,
    }


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

# Serve the chatbot UI at the root URL
@app.route("/")
def index():
    return app.send_static_file("index.html")

@app.route("/api/chat/local", methods=["POST"])
def api_chat_local():
    payload = request.get_json(silent=True) or {}
    user_messages = payload.get("messages")

    if not isinstance(user_messages, list) or not user_messages:
        return jsonify(
            {
                "ok": False,
                "error": "Body must include a non-empty 'messages' list."
            }
        ), 400

    try:
        result = run_tool_loop(user_messages)
        status = 200 if result.get("ok") else 500
        return jsonify(result), status
    except requests.HTTPError as exc:
        return jsonify(
            {
                "ok": False,
                "error": f"vLLM HTTP error: {str(exc)}",
                "details": getattr(exc.response, "text", None),
            }
        ), 502
    except Exception as exc:
        return jsonify(
            {
                "ok": False,
                "error": f"Unhandled server error: {str(exc)}"
            }
        ), 500


@app.route("/api/chat/local/health", methods=["GET"])
def api_chat_local_health():
    return jsonify(
        {
            "ok": True,
            "vllm_base_url": VLLM_BASE_URL,
            "model": VLLM_MODEL,
            "db_path": SCANNER_DB_PATH,
            "tools": [t["function"]["name"] for t in TOOLS],
        }
    )


@app.route("/api/chat/local/tools", methods=["GET"])
def api_chat_local_tools():
    return jsonify(
        {
            "ok": True,
            "tools": TOOLS,
        }
    )

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print("\nAVAILABLE TOOLS:")
    for tool in TOOLS:
        print("-", tool["function"]["name"])
    print()

    app.run(host="0.0.0.0", port=5011, debug=True)
