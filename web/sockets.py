# /home/ned/Documents/neds_scanner_radio_d102925/scanner_web/sockets.py
import logging
import redis
import json
import datetime
import os
from flask import request
from flask_socketio import SocketIO, emit

import push_db
import push_utils



from client_tracker import init_client_table, log_client_connection, fetch_client_geo

# Will be initialized in app_socket.py via init_sockets
socketio = SocketIO()
r = None
ALL_FEEDS = []
ALL_DEPARTMENT_IDS = []
LOCAL_TIMEZONE = None
logger = logging.getLogger('scanner_web')
PUSH_COOLDOWN_SECONDS = max(60, int(os.environ.get('PUSH_COOLDOWN_SECONDS', '300')))


# -------------------------
# WebSocket Event Handlers
# -------------------------
@socketio.on('connect')
def handle_connect():
    sid = request.sid
    headers = request.headers

    # --- Capture key fields ---
    client_ip = (
        headers.get('CF-Connecting-IP') or
        headers.get('X-Forwarded-For', request.remote_addr)
    )
    user_agent = headers.get('User-Agent', 'Unknown')
    origin = headers.get('Origin', 'Unknown')
    referrer = headers.get('Referer', 'Unknown')
    language = headers.get('Accept-Language', 'Unknown')
    client_id = request.cookies.get('client_id')  # Optional JS-side identifier

    # --- Optional: Geo lookup ---
    geo = fetch_client_geo(client_ip)

    # --- Log to SQLite ---
    log_client_connection(
        client_id=client_id,
        ip=client_ip,
        user_agent=user_agent,
        origin=origin,
        referrer=referrer,
        language=language,
        geo_json=geo
    )

    # --- Standard Socket.IO response ---
    emit('connection_response', {
        'status': 'connected',
        'timestamp': datetime.datetime.now().isoformat(),
        'sessionId': sid
    }, to=sid)

    logger.info("socket.connect sid=%s ip=%s origin=%s", sid, client_ip, origin)


@socketio.on('disconnect')
def handle_disconnect():
    logger.info("socket.disconnect sid=%s", request.sid)

@socketio.on('client_message')
def handle_client_message(json_data):
    payload_preview = ""
    if isinstance(json_data, dict):
        payload_preview = str(json_data.get('data', ''))[:120]
    logger.debug("socket.client_message sid=%s payload=%s", request.sid, payload_preview)
    
    # This broadcasts to EVERYONE. 
    # If you only want to reply to the sender, add 'to=request.sid'
    socketio.emit('server_response', {'data': f"Server received: {json_data.get('data', 'No data')}"})

# -------------------------
# Background Workers
# -------------------------
def _push_recipients(job):
    """Return (subscription, message_mode) pairs for a queued push job."""
    targeted_endpoints = job.get('targeted_endpoints')
    endpoint_set = set(targeted_endpoints) if targeted_endpoints is not None else None
    feed = job.get('feed', '')

    if job.get('kind') == 'call_ready':
        recipients = []
        for subscription, feeds, message_mode in push_db.list_subscriptions_with_settings():
            endpoint = subscription.get('endpoint')
            if endpoint_set is not None and endpoint not in endpoint_set:
                continue
            if feeds is None or feed in feeds:
                recipients.append((subscription, message_mode))
        return recipients

    subscriptions = push_db.list_subscriptions()
    if endpoint_set is not None:
        subscriptions = [
            subscription for subscription in subscriptions
            if subscription.get('endpoint') in endpoint_set
        ]
    return [(subscription, 'alert_only') for subscription in subscriptions]


def _push_message(job, message_mode):
    if job.get('kind') == 'call_ready':
        transcript = ' '.join(str(job.get('transcript') or '').split())
        if message_mode == 'transcript' and transcript:
            return transcript
        return 'A new scanner call is ready to listen.'
    return job.get('message', 'New scanner call')


def push_worker():
    worker_logger = logging.getLogger('scanner_web.push_worker')
    worker_logger.info("push_worker.start")
    
    try:
        vapid_pub, vapid_priv = push_utils.load_vapid_keys()
        vapid_claims = {'sub': 'mailto:admin@iamcalledned.ai'}
        worker_logger.info("push_worker.vapid_keys_ready")
    except Exception as e:
        worker_logger.critical("push_worker.vapid_keys_failed error=%s", e)
        return
        
    while True:
        try:
            item = r.brpop('push_queue', timeout=10)
            if not item:
                socketio.sleep(0.1)
                continue
                
            _, payload_raw = item
            # r uses decode_responses=True so values are already str; guard for bytes just in case
            payload_str = payload_raw.decode('utf-8') if isinstance(payload_raw, (bytes, bytearray)) else payload_raw
            
            try:
                job = json.loads(payload_str)
                title = job.get('title', 'Scanner Activity')
                feed = job.get('feed', '')
                recipients = _push_recipients(job)

                if job.get('kind') == 'call_ready' and recipients:
                    cooldown_key = f'scanner:push:cooldown:{feed}'
                    if not r.set(
                        cooldown_key,
                        job.get('call_id') or job.get('filename') or '1',
                        nx=True,
                        ex=PUSH_COOLDOWN_SECONDS,
                    ):
                        worker_logger.debug("push_worker.cooldown feed=%s", feed)
                        continue

                worker_logger.info(
                    "push_worker.job_start recipients=%s feed=%s",
                    len(recipients),
                    feed or "-",
                )

                success_count = 0
                failed_count = 0
                for sub, message_mode in recipients:
                    endpoint = sub.get('endpoint', 'unknown')
                    push_payload = {
                        'title': title,
                        'message': _push_message(job, message_mode),
                        'data': job.get('data') or {},
                        'tag': job.get('tag') or '',
                    }
                    if feed:
                        push_payload['feed'] = feed
                    try:
                        ok, err = push_utils.send_push(sub, push_payload, vapid_priv, vapid_claims)
                        if ok:
                            success_count += 1
                        else:
                            failed_count += 1
                            if err and ('410' in err or '404' in err or 'unsubscribed' in err.lower() or 'expired' in err.lower()):
                                push_db.remove_subscription(endpoint)
                                worker_logger.info("push_worker.subscription_removed endpoint=%s", endpoint)
                            worker_logger.warning("push_worker.delivery_failed endpoint=%s error=%s", endpoint, err)
                    except Exception as push_err:
                        failed_count += 1
                        worker_logger.error("push_worker.delivery_error endpoint=%s error=%s", endpoint, push_err)

                worker_logger.info(
                    "push_worker.job_complete recipients=%s delivered=%s failed=%s",
                    len(recipients),
                    success_count,
                    failed_count,
                )
                
            except json.JSONDecodeError:
                worker_logger.error("push_worker.invalid_json")
            except Exception as e:
                worker_logger.error("push_worker.job_error error=%s", e)
                
        except redis.RedisError as redis_err:
            worker_logger.error("push_worker.redis_error error=%s retry_seconds=%s", redis_err, 5)
            socketio.sleep(5)
        except Exception as e:
            worker_logger.error("push_worker.unexpected_error error=%s retry_seconds=%s", e, 5)
            socketio.sleep(5)

def transmitting_worker():
    worker_logger = logging.getLogger('scanner_web.transmitting_worker')
    worker_logger.info("transmitting_worker.start")
    
    last_status = {}
    status_check_count = 0
    while True:
        current_status = {}
        active_found = False
        status_check_count += 1
        
        try:
            # Fetch all transmitting status keys
            keys = r.keys('scanner:*:transmitting')
            if keys:
                worker_logger.debug("transmitting_worker.status_check count=%s keys=%s", status_check_count, len(keys))
                
                # Pipeline Redis gets for efficiency
                pipe = r.pipeline()
                for key in keys:
                    pipe.get(key)
                values = pipe.execute()
                
                # Process each key-value pair
                for key, value in zip(keys, values):
                    try:
                        parts = key.split(':')
                        if len(parts) == 3:
                            dept_id = parts[1]
                            current_status[dept_id] = value or 'N'
                            if value == 'Y':
                                active_found = True
                    except IndexError:
                        worker_logger.warning("transmitting_worker.malformed_key key=%s", key)
            
            # Set default 'N' status for departments not found in Redis
            for dept_id in ALL_DEPARTMENT_IDS:
                if dept_id not in current_status:
                    current_status[dept_id] = 'N'
            
            # Track status changes
            changed_statuses = {
                dept_id: status
                for dept_id, status in current_status.items()
                if last_status.get(dept_id) != status
            }
            
            if changed_statuses:
                socketio.emit('transmitting_update', changed_statuses)
            
            last_status = current_status.copy()
            
        except redis.RedisError as e:
            worker_logger.error("transmitting_worker.redis_error error=%s retry_seconds=%s", e, 5)
            last_status = {}
            socketio.sleep(5)
        except Exception as e:
            worker_logger.error("transmitting_worker.unexpected_error error=%s retry_seconds=%s", e, 1)
            socketio.sleep(1)
        
        # Adaptive sleep duration based on activity
        sleep_duration = 0.25 if active_found else 1.0
        socketio.sleep(sleep_duration)

def init_sockets(app, redis_client, all_feeds_list, all_department_ids_list, local_timezone):
    """Initializes the SocketIO extension and starts background workers."""
    global r, ALL_FEEDS, ALL_DEPARTMENT_IDS, LOCAL_TIMEZONE
    r = redis_client
    ALL_FEEDS = all_feeds_list
    ALL_DEPARTMENT_IDS = all_department_ids_list
    LOCAL_TIMEZONE = local_timezone
    init_client_table()
    
    socketio.init_app(app, async_mode='eventlet', cors_allowed_origins="*")
    
    # Start background workers
    logger.info("socket_workers.start")
    socketio.start_background_task(target=push_worker)
    socketio.start_background_task(target=transmitting_worker)
