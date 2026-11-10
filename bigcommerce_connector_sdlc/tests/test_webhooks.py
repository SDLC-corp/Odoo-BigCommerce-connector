"""Webhook intake (real HTTP route) and asynchronous processing."""
import json
from datetime import timedelta

from odoo import fields
from odoo.tests.common import tagged

from ..services.webhook_service import BigCommerceWebhookService
from .common import STORE_A, STORE_PATH, BigCommerceCommon, BigCommerceHttpCommon, FakeResponse

URL = "/bigcommerce/webhook?store_hash=%s" % STORE_A


@tagged("post_install", "-at_install", "bigcommerce")
class TestWebhookEndpoint(BigCommerceHttpCommon):

    def post(self, payload, secret="TEST-WEBHOOK-SECRET-A", url=URL):
        headers = {"Content-Type": "application/json", "X-Odoo-Database": self.env.cr.dbname}
        if secret is not None:
            headers["X-Webhook-Secret"] = secret
        body = payload if isinstance(payload, (str, bytes)) else json.dumps(payload)
        return self.url_open(url, data=body, headers=headers)

    def events(self, instance=None):
        return self.env["bigcommerce.webhook.event"].search(
            [("instance_id", "=", (instance or self.instance).id)])

    def test_authentic_callback_is_queued_with_scope_from_body(self):
        """Regression: the scope was read from an X-BC-Scope header BigCommerce never sends."""
        response = self.post(self.webhook_payload())
        self.assertEqual(response.status_code, 200, response.text[:500])
        event = self.events()
        self.assertEqual(len(event), 1)
        self.assertEqual(event.scope, "store/order/updated")
        self.assertEqual(event.resource_id, "101")
        self.assertEqual(event.status, "pending")
        self.assertNotIn("TEST-WEBHOOK-SECRET", event.headers_json)

    def test_wrong_or_missing_secret_is_rejected_and_never_processed(self):
        self.assertEqual(self.post(self.webhook_payload(), secret="nope").status_code, 401)
        self.assertEqual(self.post(self.webhook_payload(payload_hash="x2"), secret=None).status_code, 401)
        rejected = self.events().filtered(lambda e: e.status == "rejected")
        self.assertEqual(len(rejected), 2)
        result = BigCommerceWebhookService(self.env).process_pending_events(instance=self.instance)
        self.assertEqual(result["total"], 0)
        self.assertFalse(self.http.calls, "a forged callback must never trigger an API import")
        self.assertFalse(BigCommerceWebhookService(self.env).process_event(rejected[0])["success"])

    def test_redelivery_is_recognised(self):
        payload = self.webhook_payload()
        self.post(payload)
        second = self.post(payload)
        self.assertEqual(second.status_code, 200)
        self.assertTrue(second.json()["duplicate"])
        self.assertEqual(len(self.events()), 1)

    def test_same_store_hash_in_two_companies_routes_by_secret(self):
        self.post(self.webhook_payload(), secret="TEST-WEBHOOK-SECRET-B")
        self.assertEqual(len(self.events(self.instance_b)), 1)
        self.assertFalse(self.events(self.instance).filtered(lambda e: e.status == "pending"))

    def test_store_hash_from_producer_when_query_missing(self):
        response = self.post(self.webhook_payload(), url="/bigcommerce/webhook")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(self.events()), 1)

    def test_malformed_unknown_and_oversized_bodies(self):
        self.assertEqual(self.post("[1, 2]").status_code, 400)
        self.assertEqual(self.post(self.webhook_payload(), url="/bigcommerce/webhook?store_hash=nobody").status_code, 404)
        big = json.dumps({"scope": "x", "pad": "a" * (1024 * 1024 + 10)})
        self.assertEqual(self.post(big).status_code, 413)
        self.assertFalse(self.events())

    def test_disabled_connector_refuses(self):
        self.instance.webhook_enabled = False
        self.assertEqual(self.post(self.webhook_payload()).status_code, 503)
        self.assertEqual(self.events().status, "rejected")


@tagged("post_install", "-at_install", "bigcommerce")
class TestWebhookProcessing(BigCommerceCommon):

    def ingest(self, **kw):
        service = BigCommerceWebhookService(self.env)
        result = service.ingest_webhook(self.instance, self.webhook_payload(**kw), {}, raw_body="{}")
        return self.env["bigcommerce.webhook.event"].browse(result["event_id"]), result

    def test_pending_event_for_same_resource_is_merged(self):
        first, _r = self.ingest(created_at=1)
        second, result = self.ingest(created_at=2, payload_hash="other")
        self.assertEqual(first, second)
        self.assertTrue(result["duplicate"])

    def test_order_event_imports_order(self):
        product = self.make_product()
        self.bind_product(product)
        self.mock_order(self.order_payload(), [self.order_line()])
        event, _r = self.ingest()
        result = BigCommerceWebhookService(self.env).process_pending_events(instance=self.instance)
        self.assertEqual(result["processed"], 1, result)
        self.assertEqual(event.status, "done")
        self.assertTrue(self.env["bigcommerce.order.binding"].search(
            [("instance_id", "=", self.instance.id), ("bigcommerce_order_id", "=", "101")]).sale_order_id)

    def test_failure_backs_off_then_stops_after_max_retries(self):
        self.instance.webhook_max_retries = 2
        self.http.add("GET", STORE_PATH + "/v2/orders/101", response={"title": "boom"}, status=500)
        event, _r = self.ingest()
        service = BigCommerceWebhookService(self.env)
        service.process_pending_events(instance=self.instance)
        self.assertEqual(event.status, "failed")
        self.assertEqual(event.retry_count, 1)
        self.assertGreater(event.next_retry_at, fields.Datetime.now())
        # not due yet -> not picked
        self.assertEqual(service.process_pending_events(instance=self.instance)["total"], 0)
        event.next_retry_at = fields.Datetime.now() - timedelta(minutes=1)
        service.process_pending_events(instance=self.instance)
        self.assertEqual(event.retry_count, 2)
        self.assertFalse(event.next_retry_at, "no automatic retry after the maximum")
        event.next_retry_at = False
        self.assertEqual(service.process_pending_events(instance=self.instance)["total"], 0)
        # a manager can still retry by hand
        event.with_user(self.user_manager).action_retry()
        self.assertEqual((event.status, event.retry_count), ("pending", 0))

    def test_failed_event_leaves_no_half_import(self):
        product = self.make_product()
        self.bind_product(product)
        self.http.add("GET", STORE_PATH + "/v2/orders/101/products", None)
        self.http.add("GET", STORE_PATH + "/v2/orders/101", self.order_payload())
        before = self.env["sale.order"].search_count([])
        event, _r = self.ingest()
        BigCommerceWebhookService(self.env).process_event(event)
        self.assertEqual(event.status, "failed")
        self.assertEqual(self.env["sale.order"].search_count([]), before)

    def test_product_deleted_flags_binding_without_api_call(self):
        product = self.make_product()
        binding = self.bind_product(product, remote_product="77", remote_variant=False)
        event, _r = self.ingest(scope="store/product/deleted", resource_id=77)
        BigCommerceWebhookService(self.env).process_event(event)
        self.assertEqual(event.status, "done")
        self.assertEqual(binding.sync_state, "error")
        self.assertIn("Deleted in BigCommerce", binding.last_error)
        self.assertFalse(self.http.calls)

    def test_unknown_scope_is_closed(self):
        event, _r = self.ingest(scope="store/cart/created", resource_id=5)
        BigCommerceWebhookService(self.env).process_event(event)
        self.assertEqual(event.status, "done")
        self.assertIn("No processor", event.note)

    def test_rejected_callbacks_are_capped(self):
        service = BigCommerceWebhookService(self.env)
        service.MAX_REJECTED_PER_HOUR = 3
        for _i in range(5):
            service.ingest_rejected_webhook(self.instance, {"scope": "x"}, {}, "Invalid webhook secret.")
        self.assertEqual(self.env["bigcommerce.webhook.event"].search_count(
            [("instance_id", "=", self.instance.id), ("status", "=", "rejected")]), 3)

    def test_registration_sends_secret_and_database_headers(self):
        self.http.add("GET", STORE_PATH + "/v3/hooks", {"data": [], "meta": {"pagination": {
            "total": 0, "total_pages": 1, "current_page": 1}}})
        self.http.add("POST", STORE_PATH + "/v3/hooks", callback=lambda rec: FakeResponse(
            {"data": dict(rec["json"], id=len(self.http.calls), is_active=True)}))
        self.instance.with_user(self.user_admin).action_register_webhooks()
        posts = self.http.calls_to("POST", STORE_PATH + "/v3/hooks")
        scopes = {p["json"]["scope"] for p in posts}
        self.assertIn("store/order/updated", scopes)
        self.assertIn("store/product/deleted", scopes)
        headers = posts[0]["json"]["headers"]
        self.assertEqual(headers["X-Webhook-Secret"], "TEST-WEBHOOK-SECRET-A")
        self.assertEqual(headers["X-Odoo-Database"], self.env.cr.dbname)
        self.assertEqual(posts[0]["json"]["destination"],
                         "https://odoo.example.test/bigcommerce/webhook?store_hash=%s" % STORE_A)
        subs = self.env["bigcommerce.webhook.subscription"].search([("instance_id", "=", self.instance.id)])
        self.assertEqual(len(subs), len(scopes))
