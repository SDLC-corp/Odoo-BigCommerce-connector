"""API transport: rate limits, bounded waits, idempotent retries."""
import requests

from odoo.tests.common import tagged

from ..services.api_client import BigCommerceApiClient
from .common import STORE_PATH, BigCommerceCommon, FakeResponse


@tagged("post_install", "-at_install", "bigcommerce")
class TestApiTransport(BigCommerceCommon):

    def client(self):
        return BigCommerceApiClient(self.instance)

    def test_429_waits_for_bigcommerce_reset_window(self):
        self.http.add("GET", STORE_PATH + "/v2/store", sequence=[
            FakeResponse({"title": "Too many requests"}, 429, {"X-Rate-Limit-Time-Reset-Ms": "1500"}),
            {"id": 1},
        ])
        result = self.client().get("/v2/store")
        self.assertTrue(result["success"])
        self.assertEqual(result["retry_count"], 1)
        self.assertEqual(self.waits, [1.5])

    def test_hostile_retry_after_is_capped(self):
        self.http.add("GET", STORE_PATH + "/v2/store", sequence=[
            FakeResponse({}, 429, {"Retry-After": "86400"}), {"id": 1}])
        self.client().get("/v2/store")
        self.assertEqual(self.waits, [BigCommerceApiClient.MAX_WAIT_SECONDS])

    def test_total_wait_budget_is_bounded(self):
        self.http.add("GET", STORE_PATH + "/v2/store", response={}, status=429,
                      headers={"X-Rate-Limit-Time-Reset-Ms": "15000"})
        result = self.client().get("/v2/store", retries=10)
        self.assertFalse(result["success"])
        self.assertEqual(result["status_code"], 429)
        self.assertLessEqual(sum(self.waits), BigCommerceApiClient.MAX_TOTAL_WAIT_SECONDS)

    def test_get_retried_on_503_but_post_is_not(self):
        self.http.add("GET", STORE_PATH + "/v3/catalog/products", sequence=[FakeResponse({}, 503), {"data": []}])
        self.assertTrue(self.client().get("/v3/catalog/products")["success"])
        self.http.reset()
        self.http.add("POST", STORE_PATH + "/v2/orders/1/shipments", response={}, status=503)
        result = self.client().post("/v2/orders/1/shipments", payload={"x": 1})
        self.assertFalse(result["success"])
        self.assertEqual(len(self.http.calls), 1, "a non-idempotent POST must not be repeated after a 5xx")

    def test_post_not_repeated_after_read_timeout(self):
        self.http.add("POST", STORE_PATH + "/v3/customers", exc=requests.exceptions.ReadTimeout())
        result = self.client().post("/v3/customers", payload=[{"email": "a@b.c"}])
        self.assertFalse(result["success"])
        self.assertEqual(len(self.http.calls), 1)
        self.assertIn("not repeated", result["message"])

    def test_post_retried_after_429(self):
        self.http.add("POST", STORE_PATH + "/v3/customers", sequence=[FakeResponse({}, 429), {"data": [{"id": 5}]}])
        self.assertTrue(self.client().post("/v3/customers", payload=[{}])["success"])
        self.assertEqual(len(self.http.calls), 2)

    def test_connection_errors_never_raise(self):
        self.http.add("GET", STORE_PATH + "/v2/store", exc=requests.exceptions.ConnectionError())
        result = self.client().get("/v2/store", retries=1)
        self.assertFalse(result["success"])
        self.assertIsNone(result["status_code"])

    def test_throttles_when_quota_is_exhausted(self):
        self.http.add("GET", STORE_PATH + "/v2/store", response={"id": 1},
                      headers={"X-Rate-Limit-Requests-Left": "0", "X-Rate-Limit-Time-Reset-Ms": "800"})
        self.client().get("/v2/store")
        self.assertEqual(self.waits, [0.8])

    def test_token_sent_but_never_logged(self):
        self.http.add("GET", STORE_PATH + "/v2/store", response={"title": "nope"}, status=401)
        self.instance.action_test_connection()
        log = self.env["bigcommerce.sync.log"].search(
            [("instance_id", "=", self.instance.id), ("operation_type", "=", "connection_test")], limit=1)
        self.assertEqual(log.status, "failed")
        self.assertNotIn("TEST-TOKEN", "%s %s %s" % (log.request_payload, log.response_body, log.error_message))
