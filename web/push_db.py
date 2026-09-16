import sqlite3
import os
import json
import logging

DB_PATH = os.path.join(os.path.dirname(__file__), 'push_subs.sqlite3')
logger = logging.getLogger("scanner_web.push_db")

DB_PATH_login = os.environ.get("LOGIN_DB_PATH", "/home/ned/data/login/login.sqlite3")

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        endpoint TEXT UNIQUE,
        subscription_json TEXT,
        created_at INTEGER,
        feed_prefs TEXT,
        prefs_version INTEGER NOT NULL DEFAULT 1,
        message_mode TEXT NOT NULL DEFAULT 'alert_only'
    )
    ''')
    # Add feed_prefs column to existing DBs that predate this migration
    try:
        cur.execute('ALTER TABLE subscriptions ADD COLUMN feed_prefs TEXT')
    except Exception:
        pass  # Column already exists
    try:
        cur.execute('ALTER TABLE subscriptions ADD COLUMN prefs_version INTEGER NOT NULL DEFAULT 1')
    except Exception:
        pass  # Column already exists
    try:
        cur.execute("ALTER TABLE subscriptions ADD COLUMN message_mode TEXT NOT NULL DEFAULT 'alert_only'")
    except Exception:
        pass  # Column already exists
    conn.commit()
    conn.close()


def save_prefs(endpoint, feeds, message_mode=None):
    """Persist the exact feed IDs a subscriber wants notifications for."""
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if message_mode is None:
        cur.execute(
            'UPDATE subscriptions SET feed_prefs = ?, prefs_version = 2 WHERE endpoint = ?',
            (json.dumps(feeds), endpoint)
        )
    else:
        cur.execute(
            '''
            UPDATE subscriptions
            SET feed_prefs = ?, prefs_version = 2, message_mode = ?
            WHERE endpoint = ?
            ''',
            (json.dumps(feeds), message_mode, endpoint)
        )
    updated = cur.rowcount > 0
    conn.commit()
    conn.close()
    return updated


def get_prefs(endpoint):
    """Return exact feed IDs, or None for a legacy all-feed subscription."""
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT feed_prefs, prefs_version FROM subscriptions WHERE endpoint = ?', (endpoint,))
    row = cur.fetchone()
    conn.close()
    if not row or row[0] is None:
        return None
    if row[0]:
        try:
            feeds = json.loads(row[0])
            if int(row[1] or 1) < 2 and feeds == []:
                return None
            return feeds
        except Exception:
            return None
    return []


def get_preferences(endpoint):
    """Return feed and message-style settings for a subscription."""
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        'SELECT message_mode FROM subscriptions WHERE endpoint = ?',
        (endpoint,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        'feeds': get_prefs(endpoint),
        'message_mode': row[0] or 'alert_only',
    }


def list_subscriptions_with_prefs():
    """Return list of (subscription_json_dict, [feed_ids]) tuples."""
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT subscription_json, feed_prefs, prefs_version FROM subscriptions')
    rows = cur.fetchall()
    conn.close()
    result = []
    for sub_json, prefs_json, prefs_version in rows:
        try:
            sub = json.loads(sub_json)
        except Exception:
            continue
        try:
            prefs = json.loads(prefs_json) if prefs_json is not None else None
            if int(prefs_version or 1) < 2 and prefs == []:
                prefs = None
        except Exception:
            prefs = None
        result.append((sub, prefs))
    return result


def list_subscriptions_with_settings():
    """Return (subscription, feeds, message_mode) for push delivery."""
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT subscription_json, feed_prefs, prefs_version, message_mode
        FROM subscriptions
        '''
    )
    rows = cur.fetchall()
    conn.close()
    result = []
    for sub_json, prefs_json, prefs_version, message_mode in rows:
        try:
            subscription = json.loads(sub_json)
        except Exception:
            continue
        try:
            feeds = json.loads(prefs_json) if prefs_json is not None else None
            if int(prefs_version or 1) < 2 and feeds == []:
                feeds = None
        except Exception:
            feeds = None
        result.append((subscription, feeds, message_mode or 'alert_only'))
    return result


def save_subscription(subscription_json, feeds=None, message_mode=None):
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if feeds is None:
        cur.execute(
            '''
            INSERT INTO subscriptions (endpoint, subscription_json, created_at)
            VALUES (?, ?, strftime("%s","now"))
            ON CONFLICT(endpoint) DO UPDATE SET
                subscription_json = excluded.subscription_json,
                created_at = excluded.created_at
            ''',
            (subscription_json.get('endpoint'), json.dumps(subscription_json))
        )
    else:
        message_mode = message_mode or 'alert_only'
        cur.execute(
            '''
            INSERT INTO subscriptions (
                endpoint, subscription_json, created_at, feed_prefs,
                prefs_version, message_mode
            )
            VALUES (?, ?, strftime("%s","now"), ?, 2, ?)
            ON CONFLICT(endpoint) DO UPDATE SET
                subscription_json = excluded.subscription_json,
                created_at = excluded.created_at,
                feed_prefs = excluded.feed_prefs,
                prefs_version = 2,
                message_mode = excluded.message_mode
            ''',
            (
                subscription_json.get('endpoint'),
                json.dumps(subscription_json),
                json.dumps(feeds),
                message_mode,
            )
        )
    conn.commit()
    conn.close()


def subscription_exists(endpoint):
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT 1 FROM subscriptions WHERE endpoint = ? LIMIT 1', (endpoint,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def list_subscriptions():
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT subscription_json FROM subscriptions')
    rows = [json.loads(r[0]) for r in cur.fetchall()]
    conn.close()
    return rows


def remove_subscription(endpoint):
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('DELETE FROM subscriptions WHERE endpoint = ?', (endpoint,))
    conn.commit()
    conn.close()


def list_loggedin_users():
    """
    Counts the number of users with a non-null current_session_id,
    indicating they are currently logged in.
    Returns a list of dictionaries, each representing a logged-in user.
    """
    conn = sqlite3.connect(DB_PATH_login)
    conn.row_factory = sqlite3.Row # This allows accessing columns by name
    cur = conn.cursor()
    
    # Select userID, username, and current_session_id for users where session is active
    cur.execute('SELECT user_ID, username, current_session_id FROM user_data WHERE current_session_id IS NOT NULL')
    
    # Convert rows to a list of dictionaries
    logged_in_users = [dict(row) for row in cur.fetchall()]
    # get the count
    count = len(logged_in_users)
    
    
    conn.close()
    return count

def get_loggedin_users_count():
    """Returns just the count of logged-in users."""
    count = list_loggedin_users()
    logger.debug("logged_in_users.count=%s", count)
    return count

# You would also need a function to ensure the user_data table exists,
# similar to push_db.ensure_db(), if it's not already handled elsewhere.
# For example:
def ensure_user_data_table():
    conn = sqlite3.connect(DB_PATH_login)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_data (
            userID INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT,
            name TEXT,
            setup_date TEXT,
            last_login_date TEXT,
            current_session_id TEXT
        );
    """)
    conn.commit()
    conn.close()
