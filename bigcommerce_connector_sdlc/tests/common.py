"""Shared fixtures for the BigCommerce connector Odoo test-suite.

Safety rules enforced for every test:

* No real HTTP request leaves the process. ``requests.request/get/post/put/delete`` are replaced
  by a router that raises ``AssertionError`` for any URL a test did not mock, so nothing can reach
  a real BigCommerce store (or Gemini).
* Credentials are obviously fake (``TEST-...``).
* The API client's back-off sleep is replaced by a recorder, so retry paths are instant and the
  waits can be asserted.

Payload shapes follow BigCommerce's published API reference (v2 orders, v3 catalog/customers,
webhook callbacks). They have NOT been compared with a live store.
"""
import json
from unittest import mock
from urllib.parse import urlsplit

from odoo.tests.common import HttpCase, TransactionCase, new_test_user

from ..services.api_client import BigCommerceApiClient

GROUP_ADMIN = "bigcommerce_connector_sdlc.group_bigcommerce_admin"
GROUP_MANAGER = "bigcommerce_connector_sdlc.group_bigcommerce_manager"
GROUP_USER = "bigcommerce_connector_sdlc.group_bigcommerce_user"
STORE_A = "teststorea"
STORE_PATH = "/stores/%s" % STORE_A


class FakeResponse:
    """Minimal ``requests.Response`` stand-in."""

    def __init__(self, payload=None, status=200, headers=None, text=None):
        self.status_code = status
        self.headers = {"Content-Type": "application/json"}
        self.headers.update(headers or {})
        if text is not None:
            self.text = text
            self.content = text.encode()
        elif payload is None:
            self.text = ""
            self.content = b""
        else:
            self.text = json.dumps(payload)
            self.content = self.text.encode()
        self._payload = payload

    @property
    def ok(self):
        return 200 <= self.status_code < 400

    def json(self):
        if self._payload is None and not self.text:
            raise ValueError("no body")
        return self._payload if self._payload is not None else json.loads(self.text)


class HttpRouter:
    """Route mocked HTTP calls by ``(METHOD, path-prefix)`` and remember every call."""

    def __init__(self):
        self.routes = []
        self.calls = []

    def reset(self):
        self.routes = []
        self.calls = []

    def add(self, method, path, response=None, status=200, headers=None, callback=None, exc=None,
            sequence=None):
        """Register a route; the longest matching path wins, a later route beats an earlier one."""
        self.routes.append({
            "method": method.upper(), "path": path, "response": response, "sequence": sequence,
            "status": status, "headers": headers, "callback": callback, "exc": exc,
        })

    def calls_to(self, method, path):
        return [c for c in self.calls if c["method"] == method.upper() and c["path"].startswith(path)]

    def __call__(self, method, url=None, **kwargs):
        method = method.upper()
        parts = urlsplit(url)
        record = {"method": method, "url": url, "host": parts.netloc, "path": parts.path,
                  "params": kwargs.get("params"), "json": kwargs.get("json"),
                  "headers": kwargs.get("headers")}
        self.calls.append(record)
        matches = [(len(r["path"]), i, r) for i, r in enumerate(self.routes)
                   if r["method"] == method and parts.path.startswith(r["path"])]
        if not matches:
            raise AssertionError("Unmocked HTTP call: %s %s" % (method, url))
        route = max(matches, key=lambda m: (m[0], m[1]))[2]
        if route["sequence"] is not None:
            seq = route["sequence"]
            item = seq.pop(0) if len(seq) > 1 else seq[0]
            if isinstance(item, Exception):
                raise item
            if isinstance(item, FakeResponse):
                return item
            return FakeResponse(item, route["status"], route["headers"])
        if route["exc"]:
            raise route["exc"]
        if route["callback"]:
            return route["callback"](record)
        return FakeResponse(route["response"], route["status"], route["headers"])


def v2_pages(items, max_page_size=250):
    """Callback serving a BigCommerce V2 list endpoint page by page (``?page=&limit=``).

    V2 answers a plain JSON list per page (50 lines when no ``limit`` is sent, 250 at most) and
    HTTP 204 with an empty body once the requested page is past the end. ``max_page_size`` lets a
    test force several pages with few records.
    """
    items = list(items or [])

    def _serve(record):
        params = record.get("params") or {}
        limit = min(int(params.get("limit") or 50), max_page_size)
        page = int(params.get("page") or 1)
        chunk = items[(page - 1) * limit:page * limit]
        if not chunk:
            return FakeResponse(None, 204, {"Content-Type": "text/html"})
        return FakeResponse(chunk)

    return _serve


class BigCommerceFixtures:
    """Mixin: network guard, two companies, one user per role, connectors and payload helpers."""

    @classmethod
    def setup_bigcommerce_fixtures(cls):
        cls.http = HttpRouter()
        patcher = mock.patch("requests.request", side_effect=cls.http)
        patcher.start()
        cls.addClassCleanup(patcher.stop)
        for name in ("get", "post", "put", "delete"):
            patcher = mock.patch(
                "requests." + name,
                side_effect=lambda url, _n=name, **kw: cls.http(_n, url, **kw),
            )
            patcher.start()
            cls.addClassCleanup(patcher.stop)
        cls.waits = []
        sleeper = mock.patch.object(BigCommerceApiClient, "_sleep", lambda self, s: cls.waits.append(s))
        sleeper.start()
        cls.addClassCleanup(sleeper.stop)

        cls.env["ir.config_parameter"].sudo().set_param("web.base.url", "https://odoo.example.test")

        cls.company = cls.env.company
        cls.company_b = cls.env["res.company"].create({"name": "BigCommerce Test Company B"})
        both = [(6, 0, [cls.company.id, cls.company_b.id])]
        only_a = [(6, 0, [cls.company.id])]
        common = "base.group_user,sales_team.group_sale_salesman,stock.group_stock_user,"
        cls.user_admin = new_test_user(
            cls.env, login="bc_admin_user", groups=common + GROUP_ADMIN,
            company_ids=both, company_id=cls.company.id)
        cls.user_manager = new_test_user(
            cls.env, login="bc_manager_user", groups="base.group_user," + GROUP_MANAGER,
            company_ids=only_a, company_id=cls.company.id)
        cls.user_manager_b = new_test_user(
            cls.env, login="bc_manager_b_user", groups="base.group_user," + GROUP_MANAGER,
            company_ids=[(6, 0, [cls.company_b.id])], company_id=cls.company_b.id)
        cls.user_reader = new_test_user(
            cls.env, login="bc_reader_user", groups="base.group_user," + GROUP_USER,
            company_ids=only_a, company_id=cls.company.id)
        cls.user_plain = new_test_user(
            cls.env, login="bc_plain_user", groups="base.group_user",
            company_ids=only_a, company_id=cls.company.id)

        Connector = cls.env["bigcommerce.connector"]
        cls.warehouse = cls.env["stock.warehouse"].search([("company_id", "=", cls.company.id)], limit=1)
        cls.instance = Connector.create({
            "name": "Test Store A", "store_hash": STORE_A, "access_token": "TEST-TOKEN-A",
            "client_secret": "TEST-CLIENT-SECRET-A", "webhook_secret": "TEST-WEBHOOK-SECRET-A",
            "company_id": cls.company.id, "warehouse_id": cls.warehouse.id,
            "order_sync_checkpoint": "2026-11-01 00:00:00",
        })
        # Same store hash in another company (allowed: the hash is unique per company only).
        cls.instance_b = Connector.create({
            "name": "Test Store B", "store_hash": STORE_A, "access_token": "TEST-TOKEN-B",
            "webhook_secret": "TEST-WEBHOOK-SECRET-B", "company_id": cls.company_b.id,
        })

    # ---- payload helpers (shapes from BigCommerce's API reference) --------------------
    def order_payload(self, order_id=101, status="Awaiting Fulfillment", modified="Wed, 04 Nov 2026 10:00:00 +0000",
                      total="25.0000", shipping="5.0000", currency="USD", email="buyer@example.test", **extra):
        payload = {
            "id": order_id,
            "customer_id": 0,
            "status": status,
            "status_id": 11,
            "date_created": "Tue, 03 Nov 2026 10:00:00 +0000",
            "date_modified": modified,
            "currency_code": currency,
            "subtotal_ex_tax": "20.0000",
            "total_ex_tax": total,
            "total_inc_tax": total,
            "shipping_cost_ex_tax": shipping,
            "shipping_cost_inc_tax": shipping,
            "discount_amount": "0.0000",
            "coupon_discount": "0.0000",
            "billing_address": {"first_name": "Ada", "last_name": "Buyer", "email": email,
                                "phone": "555-0100"},
        }
        payload.update(extra)
        return payload

    def order_line(self, line_id=501, order_id=101, product_id=77, variant_id=88, sku="BC-SKU-1", qty=2,
                   price="10.0000", shipped=0, name="Test Widget"):
        return {"id": line_id, "order_id": order_id, "product_id": product_id, "variant_id": variant_id,
                "sku": sku, "name": name, "quantity": qty, "quantity_shipped": shipped,
                "price_ex_tax": price, "price_inc_tax": price}

    def mock_order(self, order, lines, path=STORE_PATH):
        oid = order["id"]
        if lines is None:
            self.http.add("GET", "%s/v2/orders/%s/products" % (path, oid), None)
        else:
            self.http.add("GET", "%s/v2/orders/%s/products" % (path, oid), callback=v2_pages(lines))
        self.http.add("GET", "%s/v2/orders/%s" % (path, oid), order)

    def make_product(self, sku="BC-SKU-1", name="Test Widget", price=10.0, company=False):
        return self.env["product.product"].create({
            "name": name, "default_code": sku, "type": "consu", "is_storable": True,
            "list_price": price, "taxes_id": [(5, 0, 0)], "company_id": company and company.id,
        })

    def bind_product(self, product, remote_product="77", remote_variant="88", instance=None):
        return self.env["bigcommerce.product.binding"].create({
            "instance_id": (instance or self.instance).id,
            "product_id": product.id,
            "bigcommerce_product_id": remote_product,
            "bigcommerce_variant_id": remote_variant,
            "bigcommerce_sku": product.default_code,
            "sync_state": "synced",
        })

    def webhook_payload(self, scope="store/order/updated", resource_id=101, created_at=1793700000,
                        payload_hash="a1b2c3", producer="stores/%s" % STORE_A):
        return {
            "scope": scope,
            "store_id": "1025646",
            "producer": producer,
            "hash": payload_hash,
            "created_at": created_at,
            "data": {"type": scope.split("/")[1], "id": resource_id},
        }


class BigCommerceCommon(BigCommerceFixtures, TransactionCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.setup_bigcommerce_fixtures()

    def setUp(self):
        super().setUp()
        self.http.reset()
        self.waits.clear()


class BigCommerceHttpCommon(BigCommerceFixtures, HttpCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.setup_bigcommerce_fixtures()

    def setUp(self):
        super().setUp()
        self.http.reset()
        self.waits.clear()
