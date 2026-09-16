# --- CRITICAL: Eventlet must be patched FIRST ---
# This must happen before any other modules are imported.
import eventlet
eventlet.monkey_patch()

# --- 1. Standard Library Imports ---
import os
import sys

# --- Load .env files so os.environ is populated before any config reads ---
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(dotenv_path=os.path.join(_project_root, '.env'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import json
import time
import glob
import math
import logging
import logging.handlers
from logging.config import dictConfig
from uuid import uuid4
import atexit
from datetime import datetime, date, timedelta
from pathlib import Path
from urllib.parse import urljoin

# --- 2. Third-Party Imports ---
import redis
import pytz
import requests
from bs4 import BeautifulSoup
from flask import Flask, send_from_directory, send_file, jsonify, render_template, request, g, has_request_context, got_request_exception
from apscheduler.schedulers.background import BackgroundScheduler

# --- 3. Local Application Imports ---
from sockets import socketio, init_sockets
from routes.routes_scanner import scanner_bp, warm_api_cache, get_active_listener_state
from routes.routes_api_scanner import api_scanner_bp
from routes.routes_auth import auth_bp
from routes.routes_push import push_bp
from routes.routes_chat import chat_bp
from scanner_config import TOWNS
import push_db
from push_db import list_loggedin_users as get_loggedin_users_count
# Added get_todays_stats, which is needed by the background task
from shared.scanner_db import get_todays_stats
from scanner_intelligence import (
    dispatch_pending_retranscriptions,
    get_or_generate_daily_take,
    process_call_enrichment_batch,
)
from chatbot.app import (
    generate_call_enrichment_batch,
    generate_neds_take_commentary,
    vllm_in_backoff,
)
from user_logger import init_user_activity_table

# --- 4. Configuration & Constants ---

# --- Logging Config ---
SERVICE_NAME = os.environ.get("SERVICE_NAME", "scanner_web")
APP_ENV = os.environ.get("APP_ENV", "development")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.environ.get("LOG_FILE", os.path.join(os.path.dirname(__file__), "scanner_web.log"))

# Ensure the log directory exists before dictConfig tries to open the file
os.makedirs(os.path.dirname(os.path.abspath(LOG_FILE)), exist_ok=True)


class RequestContextFilter(logging.Filter):
    def filter(self, record):
        record.service = SERVICE_NAME
        record.env = APP_ENV
        if has_request_context():
            record.request_id = getattr(g, "request_id", "-")
            record.remote_addr = request.headers.get("X-Forwarded-For", request.remote_addr)
            record.method = request.method
            record.path = request.path
            record.user_agent = request.headers.get("User-Agent", "-")
            record.endpoint = request.endpoint or "-"
            record.status_code = getattr(g, "log_status_code", "-")
            record.duration_ms = getattr(g, "log_duration_ms", "-")
        else:
            record.request_id = "-"
            record.remote_addr = "-"
            record.method = "-"
            record.path = "-"
            record.user_agent = "-"
            record.endpoint = "-"
            record.status_code = "-"
            record.duration_ms = "-"
        return True


dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "request_context": {"()": RequestContextFilter}
    },
    "formatters": {
        "console": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s | %(request_id)s %(remote_addr)s %(method)s %(path)s"
        },
        "json": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "fmt": "%(asctime)s %(levelname)s %(name)s %(message)s %(service)s %(env)s %(request_id)s %(remote_addr)s %(method)s %(path)s %(endpoint)s %(status_code)s %(duration_ms)s %(user_agent)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": LOG_LEVEL,
            "formatter": "console",
            "filters": ["request_context"],
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": LOG_LEVEL,
            "formatter": "json",
            "filters": ["request_context"],
            "filename": LOG_FILE,
            "maxBytes": 10485760,
            "backupCount": 10
        }
    },
    "loggers": {
        "scanner_web": {
            "level": LOG_LEVEL,
            "handlers": ["console", "file"],
            "propagate": False
        },
        "scanner_web.requests": {
            "level": LOG_LEVEL,
            "handlers": ["console", "file"],
            "propagate": False
        },
        "werkzeug": {
            "level": "WARNING",
            "handlers": ["console", "file"],
            "propagate": False
        },
        "engineio": {
            "level": "WARNING",
            "handlers": ["console", "file"],
            "propagate": False
        },
        "socketio": {
            "level": "WARNING",
            "handlers": ["console", "file"],
            "propagate": False
        }
    },
    "root": {
        "level": LOG_LEVEL,
        "handlers": ["console", "file"]
    }
})

logger = logging.getLogger("scanner_web")
request_logger = logging.getLogger("scanner_web.requests")

# --- App & DB Config ---
LOCAL_TIMEZONE = pytz.timezone('America/New_York')
ARCHIVE_BASE = os.environ.get("ARCHIVE_BASE", "/home/ned/data/scanner_calls/scanner_archive")
CLEAN_ARCHIVE_DIR = os.path.join(ARCHIVE_BASE, 'clean') # Specific dir for clean audio
REDIS_URL = os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0')
REDIS_STATS_KEY = "scanner:api:stats" # The key we will use for caching

ALL_FEEDS_LIST = [ # Renamed from ALL_FEEDS to avoid confusion with FEEDS dict
    'pd', 'fd', 'mpd', 'mfd', 'bpd', 'bfd', 'mndpd', 'mndfd',
    'uptpd', 'uptfd', 'blkpd', 'blkfd', 'frkpd', 'frkfd'
]

# Flattened list of all department IDs for easier iteration/validation
ALL_DEPARTMENT_IDS = [dept_id for town_info in TOWNS.values() for dept_id in town_info["departments"]]

# Dynamically build the FEEDS dictionary for the stats function
# This uses the 'clean' dir, which matches your other routes
FEEDS = {
    dept_id: Path(f"{CLEAN_ARCHIVE_DIR}/{dept_id}") for dept_id in ALL_DEPARTMENT_IDS
}

# --- Patriot Properties Scraper Config ---
BASE = "https://hopedale.patriotproperties.com/"
SEARCH_URL = urljoin(BASE, "SearchResults.asp")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; HopedaleScraper/1.0)"}


# --- 5. Global Object Initialization ---

app = Flask(__name__, template_folder='templates')
# app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'a-very-secret-key')

def _fmt_duration(seconds):
    """Format a duration in seconds as M:SS. Returns '--:--' for None/0/invalid."""
    try:
        s = float(seconds)
        if s <= 0 or not (s == s):  # handles None, 0, NaN
            return '--:--'
        m = int(s // 60)
        sec = int(s % 60)
        return f"{m}:{sec:02d}"
    except (TypeError, ValueError):
        return '--:--'

app.jinja_env.filters['fmt_duration'] = _fmt_duration


def _asset_version(relative_path: str) -> str:
    """Return a stable cache-busting token based on the asset mtime."""
    try:
        asset_path = Path(app.static_folder) / relative_path
        return str(int(asset_path.stat().st_mtime))
    except OSError:
        return "0"


@app.context_processor
def inject_asset_helpers():
    return {"asset_version": _asset_version}

try:
    # Standardized on 'redis_client' as the single global Redis connection
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()
    logger.info(f"[+] Connected to Redis at {REDIS_URL}")
except Exception as e:
    logger.critical(f"[!] FAILED to connect to Redis at {REDIS_URL}: {e}")
    redis_client = None # Set to None so other functions can check

SCHEDULER_MISFIRE_GRACE_SECONDS = int(
    os.environ.get("SCANNER_SCHEDULER_MISFIRE_GRACE_SECONDS", "60")
)
scheduler = BackgroundScheduler(
    daemon=True,
    timezone=LOCAL_TIMEZONE,
    job_defaults={
        "misfire_grace_time": SCHEDULER_MISFIRE_GRACE_SECONDS,
        "coalesce": True,
        "max_instances": 1,
    },
)


# --- 6. Helper Functions ---

def _bytes_to_readable(size_bytes):
    """Helper function to format bytes."""
    if size_bytes < 1024:
        return f"{size_bytes} Bytes"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes / 1024**2:.2f} MB"
    else:
        return f"{size_bytes / 1024**3:.2f} GB"

def search_by_address(street: str, number: str, town: str, base: str, search_url: str):
    """
    Perform a property search by street name and number.
    Returns list of matches (usually 1).
    """
    params = {
        "SearchStreetName": street,
        "SearchStreetNumber": number,
        "SearchTown": town
    }
    r = requests.get(search_url, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    rows = []
    for row in soup.select("#T1 tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 11:
            continue

        parcel_link = cells[0].find("a")
        parcel_id = parcel_link.get_text(strip=True) if parcel_link else None
        parcel_url = urljoin(base, parcel_link["href"]) if parcel_link else None

        owner = cells[2].get_text(";", strip=True)
        rec = {
            "parcel_id": parcel_id,
            "parcel_url": parcel_url,
            "location": cells[1].get_text(" ", strip=True),
            "owner": owner,
            "year_built": cells[3].get_text(" ", strip=True),
            "total_value": cells[4].get_text(" ", strip=True),
            "beds_baths": cells[5].get_text(" ", strip=True),
            "lot_finarea": cells[6].get_text(" ", strip=True),
            "land_use": cells[7].get_text(" ", strip=True),
            "neighborhood": cells[8].get_text(" ", strip=True),
            "sale_info": cells[9].get_text(" ", strip=True),
            "book_page": cells[10].get_text(" ", strip=True),
        }
        rows.append(rec)
    return rows

def parse_filename_timestamp(filename):
    """Extracts datetime object from 'rec_YYYY-MM-DD_HH-MM-SS_tag.wav'"""
    try:
        base = os.path.basename(filename)
        parts = base.split('_') 
        if len(parts) >= 3 and parts[0] == 'rec':
            date_str = parts[1]
            time_str = parts[2].replace('-', ':') 
            dt_str = f"{date_str} {time_str}"
            return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(f"Error parsing timestamp from {filename}: {e}")
    return None

def get_total_disk_usage(path):
    """Calculates total disk usage for a directory."""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except OSError as e:
        logger.error(f"Error calculating disk usage for {path}: {e}")
    return total_size

def make_readable_size(size_bytes):
   """Converts bytes to a human-readable format (KB, MB, GB)."""
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return f"{s} {size_name[i]}"

def get_filtered_department_ids(town_slug=None, department_id=None):
    """Returns a list of department IDs based on town/department filters."""
    if department_id and department_id in ALL_DEPARTMENT_IDS:
        return [department_id] # Specific department selected
    if town_slug and town_slug in TOWNS:
        return TOWNS[town_slug]["departments"] # All departments for a town
    return ALL_DEPARTMENT_IDS # No filter or invalid filter, return all


# --- 7. Background Task (for APScheduler) ---

def calculate_all_stats():
    """
    Calculates all stats from both the DB and the filesystem.
    This is the function your background task will call.
    It caches the result in Redis AND broadcasts it via Socket.IO.
    """
    if not redis_client:
        logger.error("stats.calculate.skipped redis_unavailable=true")
        return

    logger.debug("stats.calculate.start")
    
    # --- Part 1: Fast Database Stats ---
    try:
        # This function must be imported from scanner_db.py
        db_stats = get_todays_stats() 
        # db_stats is {'total_minutes': X, 'active_feeds': Y, 'total_calls': Z}
    except Exception as e:
        logger.error("stats.calculate.db_failed error=%s", e)
        db_stats = {'total_minutes': 0, 'active_feeds': 0, 'total_calls': 0}

    # --- Part 2: Slow Filesystem Stats ---
    stats = {
        "total_calls_today_fs": 0, # Renamed to avoid collision with db_stats
        "total_calls_all_time": 0,
        "total_disk_usage_bytes": 0,
    }
    today_date = date.today()
    all_files = []

    # Use the global FEEDS dictionary (defined in your Config section)
    for key, path in FEEDS.items():
        try:
            if not path.exists():
                continue # Skip dirs that don't exist
            
            # Glob both file types
            files_in_feed = list(path.glob("rec_*.wav")) + list(path.glob("rec_*.mp3"))
            all_files.extend(files_in_feed)
        except Exception as e:
            logger.error("stats.calculate.directory_failed feed=%s error=%s", key, e)
            continue

    for file_path in all_files:
        try:
            file_stat = file_path.stat()
            file_mtime = date.fromtimestamp(file_stat.st_mtime)
            if file_mtime == today_date:
                stats["total_calls_today_fs"] += 1
            stats["total_disk_usage_bytes"] += file_stat.st_size
        except Exception as e:
            logger.warning("stats.calculate.file_skipped file=%s error=%s", file_path.name, e)

    stats["total_calls_all_time"] = len(all_files)
    # Use the helper function (defined in your Helper Functions section)
    stats["total_disk_usage_readable"] = _bytes_to_readable(stats["total_disk_usage_bytes"])

    # --- Part 3: Combine, Cache, and Broadcast ---
    
    # Combine stats, giving preference to DB stats where names overlap
    final_stats = {**stats, **db_stats} 
    
    # Add a timestamp so you know how fresh the data is
    final_stats["last_updated"] = datetime.now().isoformat()

    try:
        # Use browser heartbeats so one person doesn't get overcounted by reconnects.
        final_stats["listeners"] = get_active_listener_state()["active_count"]
    except Exception as e:
        logger.warning("stats.calculate.listener_count_failed error=%s", e)
        final_stats["listeners"] = 0

    try:
        # 1. Cache in Redis
        redis_client.set(REDIS_STATS_KEY, json.dumps(final_stats))
        
        # 2. Broadcast via Socket.IO
        # (This 'socketio' object must be the one initialized in your app.py)
        socketio.emit('stats_update', final_stats)
        
        logger.debug("stats.calculate.complete total_calls=%s", final_stats.get('total_calls', 0))

    except Exception as e:
        logger.error("stats.calculate.redis_write_failed error=%s", e)


def refresh_call_enrichments():
    """Prepare per-call AI views and dispatch bounded comparison retries."""
    try:
        if vllm_in_backoff():
            logger.debug("neds_take.call_enrichment deferred=ai-backoff")
        else:
            enrichment = process_call_enrichment_batch(
                generator=generate_call_enrichment_batch,
                day="today",
                limit=int(os.environ.get("NEDS_TAKE_CALL_ENRICHMENT_BATCH_SIZE", "6")),
            )
            logger.info(
                "neds_take.call_enrichment selected=%s completed=%s failed=%s deferred=%s",
                enrichment.get("selected", 0),
                enrichment.get("completed", 0),
                enrichment.get("failed", 0),
                enrichment.get("deferred", 0),
            )
        if redis_client:
            retries = dispatch_pending_retranscriptions(
                redis_client,
                limit=int(
                    os.environ.get(
                        "NEDS_TAKE_RETRANSCRIPTION_DISPATCH_LIMIT",
                        "2",
                    )
                ),
                profile=os.environ.get(
                    "NEDS_TAKE_RETRANSCRIPTION_PROFILE",
                    "aggressive",
                ),
            )
            logger.info(
                "neds_take.retranscription_dispatch queued=%s dispatched=%s failed=%s",
                retries.get("queued", 0),
                retries.get("dispatched", 0),
                retries.get("failed", 0),
            )
    except Exception:
        logger.exception("neds_take.call_enrichment_failed")


def refresh_neds_take():
    """Build and publish the LLM edition away from the browser request path."""
    try:
        ai_available = not vllm_in_backoff()
        result = get_or_generate_daily_take(
            day="today",
            edition_type="rolling",
            force=True,
            commentary_generator=(generate_neds_take_commentary if ai_available else None),
        )
        logger.info(
            "neds_take.rolling.generated day=%s transmissions=%s incidents=%s",
            result["day"],
            result["fact_pack"]["totals"]["transmissions"],
            result["fact_pack"]["totals"]["estimated_incidents"],
        )
    except Exception:
        logger.exception("neds_take.rolling.failed")


def finalize_previous_neds_take():
    """Freeze yesterday's end-of-day edition after late transmissions settle."""
    previous_day = (datetime.now(LOCAL_TIMEZONE) - timedelta(days=1)).date().isoformat()
    try:
        ai_available = not vllm_in_backoff()
        result = get_or_generate_daily_take(
            day=previous_day,
            edition_type="final",
            force=True,
            commentary_generator=(generate_neds_take_commentary if ai_available else None),
        )
        logger.info(
            "neds_take.final.generated day=%s transmissions=%s incidents=%s",
            result["day"],
            result["fact_pack"]["totals"]["transmissions"],
            result["fact_pack"]["totals"]["estimated_incidents"],
        )
    except Exception:
        logger.exception("neds_take.final.failed day=%s", previous_day)


# --- 8. App Setup, Hooks & Blueprints ---

@app.before_request
def log_request_info():
    """Log incoming requests, but filter out the noisy ones."""
    if 'socket.io' in request.path or '/static/' in request.path:
        return
    g.request_id = (
        request.headers.get("X-Request-Id")
        or request.headers.get("X-Correlation-Id")
        or str(uuid4())
    )
    g.start_time = time.time()
    request_logger.info("request.start")

@app.after_request
def log_response_info(response):
    """Log outgoing responses for the requests we logged."""
    if 'socket.io' in request.path or '/static/' in request.path:
        return response
    duration_ms = None
    if hasattr(g, "start_time"):
        duration_ms = int((time.time() - g.start_time) * 1000)
    g.log_status_code = response.status_code
    g.log_duration_ms = duration_ms
    response.headers["X-Request-Id"] = getattr(g, "request_id", "-")
    log_fn = request_logger.info
    if response.status_code >= 500:
        log_fn = request_logger.error
    elif response.status_code >= 400:
        log_fn = request_logger.warning
    log_fn("request.end")
    return response


@got_request_exception.connect_via(app)
def log_unhandled_exception(sender, exception, **extra):
    logger.exception("unhandled.exception endpoint=%s", request.endpoint or "-")

@app.template_filter("datetimeformat")
def datetimeformat_filter(value, format="%b %d, %I:%M %p"):
    if isinstance(value, (int, float)):
        try: value = datetime.fromtimestamp(value)
        except ValueError: return value 
    elif isinstance(value, str):
        try: value = datetime.fromisoformat(value)
        except ValueError:
             try: value = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
             except ValueError: return value 
    if isinstance(value, datetime): return value.strftime(format)
    else: return value 

# Register your blueprints
app.register_blueprint(scanner_bp)
app.register_blueprint(api_scanner_bp, url_prefix="/scanner")
app.register_blueprint(push_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(chat_bp)


# --- 9. Application Routes ---

# --- PWA & Static File Routes ---
@app.route('/sw.js')
def service_worker():
    logger.debug("service_worker.requested")
    sw_path = os.path.join(app.root_path, app.static_folder, 'sw.js')
    if os.path.exists(sw_path):
        return send_file(sw_path, mimetype='application/javascript')
    else:
        return send_from_directory(app.static_folder, 'sw.js', mimetype='application/javascript')

@app.route('/scanner/manifest.json')
def manifest():
    path = os.path.join(app.static_folder, 'manifest.json')
    if os.path.exists(path):
        return send_file(path, mimetype='application/json')
    return {"error": "manifest not found"}, 404

@app.route('/icons/<path:filename>')
def icons(filename):
    return send_from_directory(os.path.join(app.static_folder, 'icons'), filename)

@app.route('/scanner/sw.js')
def scanner_service_worker():
    logger.debug("scanner_service_worker.requested")
    return service_worker()

@app.route('/scanner/static/<path:path>')
def scanner_static(path):
    safe_path = os.path.normpath(path).lstrip('/') 
    if '..' in safe_path: return "Invalid path", 404
    return send_from_directory(app.static_folder, safe_path)

@app.route('/scanner/static/icons/<path:filename>')
def scanner_icons(filename):
    safe_filename = os.path.normpath(filename).lstrip('/')
    if '..' in safe_filename: return "Invalid path", 404
    icons_dir = os.path.join(app.static_folder, 'icons')
    return send_from_directory(icons_dir, safe_filename)

@app.route('/scanner/offline.html')
def scanner_offline():
    return send_from_directory(app.static_folder, 'offline.html')

@app.route('/scanner/stats')
def scanner_stats_page():
    return render_template('scanner_stats.html')

# --- Data API Routes ---
@app.route('/scanner/api/users')
def api_users():
    try:
        count = get_loggedin_users_count()
        logger.debug("active_users.count=%s", count)
        return jsonify({"active_users": count}), 200
    except Exception as e:
        logger.error(f"Error fetching logged-in user count: {e}")
        return jsonify({"error": "Could not retrieve logged-in user count"}), 500

@app.route('/api/users')
def api_users_alias():
    return api_users()

@app.route('/scanner/api/ws_users')
def api_ws_users():
    try:
        snapshot = get_active_listener_state()
        raw_socket_connections = len(socketio.server.eio.sockets)
        return jsonify({
            "connected_users": snapshot["active_count"],
            "active_count": snapshot["active_count"],
            "raw_socket_connections": raw_socket_connections,
        })
    except Exception as e:
        logger.error(f"Error getting websocket user count: {e}")
        return jsonify({"connected_users": 0, "error": str(e)}), 500

@app.route('/scanner/audio/<department>/<filename>')
def serve_audio_file(department, filename):
    """Serve audio files from the clean archive directory"""
    try:
        file_path = os.path.join(CLEAN_ARCHIVE_DIR, department, filename)
        
        if '..' in filename or '..' in department:
            return "Invalid path", 400
            
        if not os.path.exists(file_path):
            logger.error(f"Audio file not found: {file_path}")
            return "File not found", 404
            
        return send_file(file_path, mimetype='audio/wav')
        
    except Exception as e:
        logger.error(f"Error serving audio file: {e}")
        return "Error serving file", 500

@app.route('/scanner/api/stats_data')
def api_stats_data():
    logger.debug("stats_data.requested")
    town_filter = request.args.get('town') 
    dept_filter = request.args.get('department') 

    target_dept_ids = get_filtered_department_ids(town_filter, dept_filter)
    
    today_date = date.today()
    start_of_week = today_date - timedelta(days=6) 
    calls_last_7_days_counts = { (start_of_week + timedelta(days=i)).strftime('%m/%d'): 0 for i in range(7) } 
    calls_today_list = [] 
    total_calls_all_time_filtered = 0
    
    for dept_id in target_dept_ids:
        dept_dir = os.path.join(CLEAN_ARCHIVE_DIR, dept_id)
        if not os.path.isdir(dept_dir): continue

        try:
            all_dept_files = glob.glob(os.path.join(dept_dir, "rec_*.wav"))
            total_calls_all_time_filtered += len(all_dept_files) 
            
            for wav_path in all_dept_files:
                filename = os.path.basename(wav_path)
                dt_obj = parse_filename_timestamp(filename)
                if not dt_obj: continue 

                call_date = dt_obj.date()
                
                if start_of_week <= call_date <= today_date:
                    date_key = call_date.strftime('%m/%d')
                    if date_key in calls_last_7_days_counts: 
                         calls_last_7_days_counts[date_key] += 1
                
                if call_date == today_date:
                    calls_today_list.append({"dept": dept_id, "hour": dt_obj.hour})

        except Exception as e:
            logger.error(f"Error processing stats for department {dept_id}: {e}")

    # --- Aggregate Today's Data ---
    calls_per_hour_today = [0] * 24 
    calls_per_dept_today = {dept_id: 0 for dept_id in target_dept_ids} 
    for call in calls_today_list:
        if 0 <= call["hour"] < 24:
            calls_per_hour_today[call["hour"]] += 1
        if call["dept"] in calls_per_dept_today: 
             calls_per_dept_today[call["dept"]] += 1

    # --- Prepare Chart Data ---
    hour_labels = [(datetime.min + timedelta(hours=h)).strftime('%-I %p') for h in range(24)] 
    calls_per_hour_data = {"labels": hour_labels, "values": calls_per_hour_today}

    dept_labels = [dept for dept, count in calls_per_dept_today.items() if count > 0]
    dept_values = [count for dept, count in calls_per_dept_today.items() if count > 0]
    calls_per_dept_data = {"labels": dept_labels, "values": dept_values}

    day_labels = list(calls_last_7_days_counts.keys())
    day_values = list(calls_last_7_days_counts.values())
    calls_per_day_data = {"labels": day_labels, "values": day_values}

    # --- Key Stats ---
    total_disk_usage_bytes = 0
    disk_usage_readable = "N/A (Filtered)"
    
    if not town_filter and not dept_filter:
       try:
           if os.path.isdir(CLEAN_ARCHIVE_DIR):
               total_disk_usage_bytes = get_total_disk_usage(CLEAN_ARCHIVE_DIR)
               disk_usage_readable = make_readable_size(total_disk_usage_bytes)
           else:
               disk_usage_readable = "N/A (Dir not found)"
       except Exception as e:
           logger.error(f"Error getting overall stats: {e}")
           disk_usage_readable = "Error"

    key_stats = {
        "total_calls_today": len(calls_today_list),
        "total_calls_all_time": total_calls_all_time_filtered,
        "total_disk_usage_readable": disk_usage_readable
    }

    return jsonify({
        "key_stats": key_stats,
        "calls_per_hour_today": calls_per_hour_data,
        "calls_per_dept_today": calls_per_dept_data,
        "calls_per_day_last_7": calls_per_day_data,
        "filters_applied": { "town": town_filter, "department": dept_filter }
    })

@app.route("/scanner/api/property", methods=["GET"])
def api_property():
    street = request.args.get("street")
    number = request.args.get("number")
    town = request.args.get("town", "").lower()

    town_map = {
        "hopedale": "https://hopedale.patriotproperties.com/",
        "milford": "https://milford.patriotproperties.com/",
        "bellingham": "https://bellingham.patriotproperties.com/",
    }
    if not street or not number:
        return jsonify({"error": "Missing required parameters: street and number"}), 400
    if town not in town_map:
        return jsonify({"error": f"Unsupported or missing town parameter: {town}"}), 400

    base = town_map[town]
    search_url = urljoin(base, "SearchResults.asp")
    try:
        results = search_by_address(street, number, town, base, search_url)
        if not results:
            return jsonify({"message": "No records found"}), 404
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500




# --- 10. Application Startup ---

def initialize_application():
    """Initialize application components and background tasks"""
    logger.info("application.init.start")

    init_user_activity_table()

    push_db.ensure_db()
    
    init_sockets(app, redis_client, ALL_FEEDS_LIST, ALL_DEPARTMENT_IDS, LOCAL_TIMEZONE)

    logger.info(
        "application.init.ready archive_base=%s timezone=%s departments=%s redis_configured=%s",
        ARCHIVE_BASE,
        LOCAL_TIMEZONE,
        len(ALL_DEPARTMENT_IDS),
        bool(redis_client),
    )

    logger.info("application.cache_warm.deferred")

# Initialize application components
initialize_application()

# --- Main Application Runner ---
if __name__ == "__main__":
    host = "0.0.0.0"
    port = 5005
    
    # This check prevents the scheduler from running twice when debug=True
    # or when run by a reloader.
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        logger.info("scheduler.starting")
        # BackgroundScheduler already invokes jobs in native executor threads.
        # Do not wrap these calls with eventlet.tpool.execute(): tpool must be
        # entered from an Eventlet green thread, and entering it from an
        # APScheduler thread can corrupt the hub with a cross-thread greenlet
        # switch and leave the listener accepting sockets without serving them.
        scheduler.add_job(
            calculate_all_stats,
            'interval', 
            seconds=10, # Note: 10s is very fast for a full disk scan!
            id='scanner_stats_job',
        )
        scheduler.add_job(
            warm_api_cache,
            'interval',
            seconds=20,
            id='scanner_api_cache_job',
        )
        scheduler.add_job(
            refresh_call_enrichments,
            'interval',
            minutes=1,
            id='scanner_call_enrichment_job',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=SCHEDULER_MISFIRE_GRACE_SECONDS,
            next_run_time=datetime.now(LOCAL_TIMEZONE) + timedelta(seconds=10),
        )
        scheduler.add_job(
            refresh_neds_take,
            'interval',
            minutes=5,
            id='neds_take_rolling_job',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=SCHEDULER_MISFIRE_GRACE_SECONDS,
            next_run_time=datetime.now(LOCAL_TIMEZONE) + timedelta(seconds=45),
        )
        scheduler.add_job(
            finalize_previous_neds_take,
            'cron',
            hour=0,
            minute=10,
            id='neds_take_final_job',
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        
        # Register a function to shut down the scheduler when the app exits
        atexit.register(lambda: scheduler.shutdown())
        logger.info("scheduler.started")
    else:
        logger.info("scheduler.skip_reloader_process")

    logger.info(
        "service.start env=%s host=%s port=%s debug=%s reloader=%s",
        APP_ENV,
        host,
        port,
        True,
        False,
    )
    
    # Use the socketio.run() to correctly start the eventlet server
    socketio.run(app, host=host, port=port, debug=True, use_reloader=False)
