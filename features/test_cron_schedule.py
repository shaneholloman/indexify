"""End-to-end test for cron scheduling via the HTTP API.

Cron schedules are now independent entities created/listed/deleted via
dedicated endpoints, decoupled from the application manifest.

Requires a running server + dataplane:
  TENSORLAKE_API_URL=http://localhost:8900 python features/test_cron_schedule.py
"""

import base64
import os
import time
import unittest

import httpx

from tensorlake.applications import application, function
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function(description="cron test")
def cron_test_app(payload: dict) -> str:
    return "cron fired"


class TestCronSchedule(unittest.TestCase):
    """Tests cron schedule CRUD via the HTTP API."""

    @classmethod
    def setUpClass(cls):
        cls.api_url = os.environ.get("TENSORLAKE_API_URL", "http://localhost:8900")
        cls.namespace = "default"
        cls.app_name = "cron_test_app"
        cls.client = httpx.Client(base_url=cls.api_url, timeout=30)

        # Verify server is up
        resp = cls.client.get("/")
        assert resp.status_code == 200, f"Server not reachable at {cls.api_url}"

        # Deploy the test application using the SDK
        deploy_applications(__file__)

        # Verify the app exists
        resp = cls.client.get(
            f"/v1/namespaces/{cls.namespace}/applications/{cls.app_name}"
        )
        assert resp.status_code == 200, f"App not found after deploy: {resp.text}"

    @classmethod
    def tearDownClass(cls):
        # Clean up: delete deployed app
        try:
            cls.client.delete(
                f"/v1/namespaces/{cls.namespace}/applications/{cls.app_name}"
            )
        except Exception:
            pass
        cls.client.close()

    def _cron_url(self) -> str:
        return f"/v1/namespaces/{self.namespace}/applications/{self.app_name}/cron-schedules"

    def _list_requests(self) -> list:
        resp = self.client.get(
            f"/v1/namespaces/{self.namespace}/applications/{self.app_name}/requests"
        )
        self.assertEqual(resp.status_code, 200, f"GET requests failed: {resp.text}")
        return resp.json().get("requests", [])

    def test_01_create_cron_schedule(self):
        """Create a cron schedule and verify it appears in the list."""
        resp = self.client.post(
            self._cron_url(),
            json={"cron_expression": "* * * * *"},
        )
        self.assertEqual(resp.status_code, 200, f"POST cron failed: {resp.text}")
        body = resp.json()
        self.assertIn("schedule_id", body)
        self.assertTrue(len(body["schedule_id"]) > 0)
        self.__class__._schedule_id_1 = body["schedule_id"]

        # List and verify
        resp = self.client.get(self._cron_url())
        self.assertEqual(resp.status_code, 200)
        schedules = resp.json()["schedules"]
        self.assertEqual(len(schedules), 1)
        self.assertEqual(schedules[0]["id"], self._schedule_id_1)
        self.assertEqual(schedules[0]["cron_expression"], "* * * * *")
        self.assertTrue(schedules[0]["enabled"])
        self.assertGreater(schedules[0]["next_fire_time_ms"], 0)

    def test_02_create_second_schedule_with_input(self):
        """Create a second schedule with an input payload."""
        input_data = b'{"type": "hourly"}'
        input_b64 = base64.b64encode(input_data).decode()

        resp = self.client.post(
            self._cron_url(),
            json={
                "cron_expression": "0 * * * *",
                "input_base64": input_b64,
            },
        )
        self.assertEqual(resp.status_code, 200, f"POST cron failed: {resp.text}")
        self.__class__._schedule_id_2 = resp.json()["schedule_id"]

        # List should now have 2 schedules
        resp = self.client.get(self._cron_url())
        self.assertEqual(resp.status_code, 200)
        schedules = resp.json()["schedules"]
        self.assertEqual(len(schedules), 2)

        # Find the hourly schedule and verify input was stored
        hourly = [s for s in schedules if s["cron_expression"] == "0 * * * *"]
        self.assertEqual(len(hourly), 1)
        self.assertIsNotNone(hourly[0].get("input_payload"))
        self.assertGreater(hourly[0]["input_payload"]["size"], 0)

    def test_03_invalid_cron_expression_rejected(self):
        """A bad cron expression should return 400."""
        resp = self.client.post(
            self._cron_url(),
            json={"cron_expression": "not a cron"},
        )
        self.assertEqual(resp.status_code, 400)

    def test_04_cron_fires_invocation(self):
        """Wait for the every-minute cron to fire and verify a request was created.

        The schedule from test_01 has '* * * * *' (every minute).
        The CronProcessor should fire it within ~60 seconds.
        """
        initial_requests = self._list_requests()
        initial_count = len(initial_requests)

        # Wait up to 90 seconds for a new request
        deadline = time.time() + 90
        new_count = initial_count
        while time.time() < deadline:
            time.sleep(5)
            current_requests = self._list_requests()
            new_count = len(current_requests)
            if new_count > initial_count:
                break

        self.assertGreater(
            new_count,
            initial_count,
            f"Expected cron to fire at least one request within 90s. "
            f"Initial: {initial_count}, Current: {new_count}",
        )

    def test_05_delete_one_schedule(self):
        """Delete one schedule and verify the other remains."""
        resp = self.client.delete(
            f"{self._cron_url()}/{self._schedule_id_2}"
        )
        self.assertIn(resp.status_code, [200, 204])

        resp = self.client.get(self._cron_url())
        self.assertEqual(resp.status_code, 200)
        schedules = resp.json()["schedules"]
        self.assertEqual(len(schedules), 1)
        self.assertEqual(schedules[0]["id"], self._schedule_id_1)

    def test_06_delete_app_cleans_up_schedules(self):
        """Deleting the app should remove all its cron schedules."""
        resp = self.client.delete(
            f"/v1/namespaces/{self.namespace}/applications/{self.app_name}"
        )
        self.assertIn(resp.status_code, [200, 204])

        # Poll until app is gone or tombstoned
        app_deleted = False
        deadline = time.time() + 15
        while time.time() < deadline:
            resp = self.client.get(
                f"/v1/namespaces/{self.namespace}/applications/{self.app_name}"
            )
            if resp.status_code == 404:
                app_deleted = True
                break
            if resp.status_code == 200 and resp.json().get("tombstoned", False):
                app_deleted = True
                break
            time.sleep(1)

        self.assertTrue(
            app_deleted,
            "Expected app to be tombstoned or deleted after DELETE",
        )

        # Verify cron schedules were cascade-deleted
        resp = self.client.get(self._cron_url())
        self.assertEqual(resp.status_code, 200, f"GET cron-schedules failed: {resp.text}")
        schedules = resp.json().get("schedules", [])
        self.assertEqual(
            len(schedules), 0,
            f"Expected 0 cron schedules after app deletion, got {len(schedules)}",
        )


if __name__ == "__main__":
    unittest.main()
