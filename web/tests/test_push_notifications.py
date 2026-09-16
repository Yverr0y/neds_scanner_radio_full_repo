import tempfile
import unittest
import sqlite3
from pathlib import Path

from flask import Flask

import push_db
import sockets
from routes import routes_push


def _subscription(endpoint="https://push.example/device"):
    return {
        "endpoint": endpoint,
        "expirationTime": None,
        "keys": {"p256dh": "public-key", "auth": "auth-secret"},
    }


class PushNotificationPreferencesTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.original_db_path = push_db.DB_PATH
        self.original_admin_token = routes_push.PUSH_ADMIN_TOKEN
        push_db.DB_PATH = str(Path(self.tempdir.name) / "push.sqlite3")
        routes_push.PUSH_ADMIN_TOKEN = ""

        app = Flask(__name__)
        app.register_blueprint(routes_push.push_bp)
        app.config["TESTING"] = True
        self.client = app.test_client()

    def tearDown(self):
        push_db.DB_PATH = self.original_db_path
        routes_push.PUSH_ADMIN_TOKEN = self.original_admin_token
        self.tempdir.cleanup()

    def test_new_subscription_saves_exact_feed_choices(self):
        response = self.client.post(
            "/scanner/push/subscribe",
            json={
                "subscription": _subscription(),
                "feeds": ["pd", "fd"],
                "message_mode": "transcript",
            },
        )
        self.assertEqual(response.status_code, 200)

        preferences = self.client.get(
            "/scanner/push/prefs",
            query_string={"endpoint": _subscription()["endpoint"]},
        )
        self.assertEqual(preferences.status_code, 200)
        self.assertEqual(preferences.get_json()["feeds"], ["pd", "fd"])
        self.assertFalse(preferences.get_json()["all_feeds"])
        self.assertEqual(preferences.get_json()["message_mode"], "transcript")

    def test_empty_feed_list_means_no_feeds(self):
        self.client.post(
            "/scanner/push/subscribe",
            json={"subscription": _subscription(), "feeds": ["pd"]},
        )
        response = self.client.post(
            "/scanner/push/prefs",
            json={"endpoint": _subscription()["endpoint"], "feeds": []},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(push_db.get_prefs(_subscription()["endpoint"]), [])

        subscriptions = push_db.list_subscriptions_with_prefs()
        self.assertEqual(subscriptions[0][1], [])

    def test_legacy_subscription_keeps_all_feed_compatibility(self):
        response = self.client.post("/scanner/push/subscribe", json=_subscription())
        self.assertEqual(response.status_code, 200)

        preferences = self.client.get(
            "/scanner/push/prefs",
            query_string={"endpoint": _subscription()["endpoint"]},
        ).get_json()
        self.assertTrue(preferences["legacy_all"])
        self.assertEqual(len(preferences["feeds"]), len(routes_push.CHANNELS))
        self.assertEqual(preferences["message_mode"], "alert_only")

    def test_legacy_empty_list_still_means_all_feeds(self):
        self.client.post("/scanner/push/subscribe", json=_subscription())
        with sqlite3.connect(push_db.DB_PATH) as connection:
            connection.execute(
                "UPDATE subscriptions SET feed_prefs = '[]', prefs_version = 1 WHERE endpoint = ?",
                (_subscription()["endpoint"],),
            )
        self.assertIsNone(push_db.get_prefs(_subscription()["endpoint"]))
        self.assertIsNone(push_db.list_subscriptions_with_prefs()[0][1])

    def test_unknown_feed_is_rejected(self):
        response = self.client.post(
            "/scanner/push/subscribe",
            json={"subscription": _subscription(), "feeds": ["not-a-feed"]},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("unknown feeds", response.get_json()["error"])

    def test_message_mode_can_be_changed_per_device(self):
        self.client.post(
            "/scanner/push/subscribe",
            json={
                "subscription": _subscription(),
                "feeds": ["pd"],
                "message_mode": "transcript",
            },
        )
        response = self.client.post(
            "/scanner/push/prefs",
            json={
                "endpoint": _subscription()["endpoint"],
                "feeds": ["pd"],
                "message_mode": "alert_only",
            },
        )
        self.assertEqual(response.status_code, 200)

        preferences = push_db.get_preferences(_subscription()["endpoint"])
        self.assertEqual(preferences["message_mode"], "alert_only")
        settings = push_db.list_subscriptions_with_settings()
        self.assertEqual(settings[0][2], "alert_only")

        legacy_update = self.client.post(
            "/scanner/push/prefs",
            json={"endpoint": _subscription()["endpoint"], "feeds": ["fd"]},
        )
        self.assertEqual(legacy_update.status_code, 200)
        self.assertEqual(
            push_db.get_preferences(_subscription()["endpoint"])["message_mode"],
            "alert_only",
        )

    def test_unknown_message_mode_is_rejected(self):
        response = self.client.post(
            "/scanner/push/subscribe",
            json={
                "subscription": _subscription(),
                "feeds": ["pd"],
                "message_mode": "everything",
            },
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("message_mode", response.get_json()["error"])

    def test_delivery_uses_each_devices_message_mode(self):
        transcript_endpoint = "https://push.example/transcript"
        alert_endpoint = "https://push.example/alert-only"
        push_db.save_subscription(
            _subscription(transcript_endpoint),
            feeds=["pd"],
            message_mode="transcript",
        )
        push_db.save_subscription(
            _subscription(alert_endpoint),
            feeds=["pd"],
            message_mode="alert_only",
        )

        job = {
            "kind": "call_ready",
            "feed": "pd",
            "transcript": "Unit 12 responding to Main Street.",
        }
        recipients = sockets._push_recipients(job)
        modes = {subscription["endpoint"]: mode for subscription, mode in recipients}

        self.assertEqual(modes[transcript_endpoint], "transcript")
        self.assertEqual(modes[alert_endpoint], "alert_only")
        self.assertEqual(
            sockets._push_message(job, modes[transcript_endpoint]),
            "Unit 12 responding to Main Street.",
        )
        self.assertEqual(
            sockets._push_message(job, modes[alert_endpoint]),
            "A new scanner call is ready to listen.",
        )

    def test_public_test_broadcast_endpoint_is_hidden(self):
        response = self.client.post("/scanner/push/send", json={"message": "test"})
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
