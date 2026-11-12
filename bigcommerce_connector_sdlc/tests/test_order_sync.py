"""Order import: incremental checkpoint, idempotency, store isolation, exception queue."""
from odoo import fields
from odoo.tests.common import tagged

from ..services.order_sync_service import BigCommerceOrderSyncService
from .common import STORE_PATH, BigCommerceCommon, v2_pages


@tagged("post_install", "-at_install", "bigcommerce")
class TestOrderSync(BigCommerceCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.product = cls.env["product.product"].create({
            "name": "Test Widget", "default_code": "BC-SKU-1", "type": "consu", "is_storable": True,
            "list_price": 10.0, "taxes_id": [(5, 0, 0)],
        })

    def setUp(self):
        super().setUp()
        self.bind_product(self.product)

    def service(self):
        return BigCommerceOrderSyncService(self.instance._bc_service_instance())

    def mock_list(self, orders):
        self.http.add("GET", STORE_PATH + "/v2/orders", sequence=[orders, None])

    def binding(self, order_id="101", instance=None):
        return self.env["bigcommerce.order.binding"].search([
            ("instance_id", "=", (instance or self.instance).id), ("bigcommerce_order_id", "=", order_id)])

    # ---- incremental contract ---------------------------------------------------
    def test_requests_orders_modified_after_checkpoint_oldest_first(self):
        """Regression: the import always re-read the store's oldest orders and never reached new ones."""
        order = self.order_payload()
        self.mock_list([order])
        self.mock_order(order, [self.order_line()])
        result = self.service().import_orders(limit=50)
        self.assertTrue(result["success"], result)
        params = self.http.calls_to("GET", STORE_PATH + "/v2/orders")[0]["params"]
        self.assertEqual(params["sort"], "date_modified:asc")
        self.assertEqual(params["min_date_modified"], "2026-11-01T00:00:00+00:00")
        self.assertEqual(fields.Datetime.to_string(self.instance.order_sync_checkpoint), "2026-11-04 10:00:00")

    def test_checkpoint_moves_forward_when_page_is_stuck_on_one_second(self):
        self.instance.order_sync_checkpoint = "2026-11-04 10:00:00"
        order = self.order_payload()
        self.mock_list([order])
        self.mock_order(order, [self.order_line()])
        self.service().import_orders(limit=50)
        self.assertEqual(fields.Datetime.to_string(self.instance.order_sync_checkpoint), "2026-11-04 10:00:01")

    def test_reimport_is_idempotent(self):
        order = self.order_payload()
        self.mock_order(order, [self.order_line()])
        self.assertTrue(self.service().import_order_by_id(101)["success"])
        self.assertTrue(self.service().import_order_by_id(101)["success"])
        binding = self.binding()
        self.assertEqual(len(binding), 1)
        self.assertEqual(len(binding.sale_order_id.order_line.filtered(lambda l: l.product_id == self.product)), 1)

    # ---- defects fixed --------------------------------------------------------------
    def test_order_with_more_than_one_page_of_lines_imports_every_line(self):
        """Regression: /v2/orders/{id}/products was read once (50 lines per page by default), so lines
        51+ of a large order were silently dropped."""
        order = self.order_payload(total="60.0000", shipping="0.0000")
        lines = [self.order_line(line_id=1000 + i, qty=1, price="1.0000") for i in range(60)]
        self.mock_order(order, [])
        self.http.add("GET", STORE_PATH + "/v2/orders/101/products", callback=v2_pages(lines, max_page_size=50))
        result = self.service().import_order_by_id(101)
        self.assertTrue(result["success"], result)
        sale = self.binding().sale_order_id
        self.assertEqual(len(sale.order_line.filtered(lambda l: l.product_id == self.product)), 60)
        calls = self.http.calls_to("GET", STORE_PATH + "/v2/orders/101/products")
        self.assertEqual([c["params"]["page"] for c in calls], [1, 2, 3])
        self.assertEqual(calls[0]["params"]["limit"], 250)

    def test_order_with_shipping_and_unknown_product_imports(self):
        """Regression: service products were created with uom_po_id (gone in Odoo 19) -> every such order failed."""
        order = self.order_payload(discount_amount="1.0000", coupon_discount="2.0000",
                                   total="22.0000")
        lines = [self.order_line(), self.order_line(line_id=502, product_id=999, variant_id=998, sku="UNKNOWN-SKU",
                                                    qty=1, price="0.0000", name="Mystery item")]
        self.mock_order(order, lines)
        result = self.service().import_order_by_id(101)
        self.assertTrue(result["success"], result)
        sale = self.binding().sale_order_id
        names = sale.order_line.mapped("product_id.default_code")
        self.assertIn("BIGCOMMERCE_SHIPPING", names)
        self.assertIn("BIGCOMMERCE_DISCOUNT", names)
        self.assertIn("BC-MISSING-UNKNOWN-SKU", names, "the placeholder must not take the real SKU")
        # 20 (lines) + 5 (shipping) - 3 (discount + coupon); Odoo may add its default tax on top.
        self.assertEqual(sale.amount_untaxed, 22.0)
        binding = self.binding()
        self.assertTrue(binding.has_placeholder_lines)
        self.assertTrue(binding.needs_attention)
        self.assertIn("Unmatched products", binding.exception_reason)

    def test_second_store_does_not_adopt_first_store_order_with_same_number(self):
        """Regression: store B's order #101 was merged into store A's sale order #101."""
        order = self.order_payload()
        self.mock_order(order, [self.order_line()])
        self.service().import_order_by_id(101)
        sale_a = self.binding().sale_order_id
        instance_a2 = self.env["bigcommerce.connector"].create({
            "name": "Second store, same company", "store_hash": "otherstore", "access_token": "TEST-TOKEN-C",
            "company_id": self.company.id,
        })
        self.bind_product(self.product, remote_product="77", remote_variant="88", instance=instance_a2)
        self.mock_order(order, [self.order_line()], path="/stores/otherstore")
        BigCommerceOrderSyncService(instance_a2._bc_service_instance()).import_order_by_id(101)
        sale_b = self.binding(instance=instance_a2).sale_order_id
        self.assertTrue(sale_b)
        self.assertNotEqual(sale_a, sale_b)

    def test_failed_order_is_parked_and_does_not_block_the_checkpoint(self):
        """Also a regression for lookup caches pointing at records a failed savepoint rolled back."""
        ok = self.order_payload(order_id=102, modified="Thu, 05 Nov 2026 10:00:00 +0000")
        broken = self.order_payload(order_id=101)
        self.mock_list([broken, ok])
        self.http.add("GET", STORE_PATH + "/v2/orders/101/products", None)  # no lines -> fails
        self.mock_order(ok, [self.order_line(order_id=102)])
        result = self.service().import_orders(limit=50)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["created"], 1)
        failed = self.binding("101")
        self.assertEqual(failed.sync_state, "error")
        self.assertFalse(failed.sale_order_id)
        self.assertTrue(failed.needs_attention)
        self.assertEqual(fields.Datetime.to_string(self.instance.order_sync_checkpoint), "2026-11-05 10:00:00")

    def test_resync_button_repairs_a_parked_order(self):
        broken = self.order_payload(shipping="0.0000", total="20.0000")
        self.mock_list([broken])
        self.http.add("GET", STORE_PATH + "/v2/orders/101/products", None)
        self.service().import_orders(limit=50)
        binding = self.binding()
        self.assertEqual(binding.sync_state, "error")
        self.http.reset()
        self.mock_order(broken, [self.order_line()])
        binding.with_user(self.user_manager).action_resync_from_bigcommerce()
        self.assertEqual(binding.sync_state, "synced")
        self.assertTrue(binding.sale_order_id)
        self.assertFalse(binding.needs_attention)

    def test_total_mismatch_is_flagged(self):
        order = self.order_payload(total="40.0000")
        self.mock_order(order, [self.order_line()])
        self.service().import_order_by_id(101)
        binding = self.binding()
        self.assertTrue(binding.amount_mismatch)
        self.assertIn("Total differs", binding.exception_reason)
        self.instance.order_total_tolerance = 20.0
        self.assertFalse(binding.amount_mismatch)

    def test_currency_mismatch_is_flagged(self):
        other = "EUR" if self.company.currency_id.name != "EUR" else "GBP"
        order = self.order_payload(currency=other)
        self.mock_order(order, [self.order_line()])
        self.service().import_order_by_id(101)
        self.assertTrue(self.binding().currency_mismatch)

    def test_order_uses_connector_warehouse(self):
        order = self.order_payload()
        self.mock_order(order, [self.order_line()])
        self.service().import_order_by_id(101)
        self.assertEqual(self.binding().sale_order_id.warehouse_id, self.warehouse)

    def test_customer_of_other_company_is_not_reused(self):
        foreign = self.env["res.partner"].create({
            "name": "Foreign", "email": "buyer@example.test", "company_id": self.company_b.id})
        order = self.order_payload()
        self.mock_order(order, [self.order_line()])
        self.service().import_order_by_id(101)
        self.assertNotEqual(self.binding().sale_order_id.partner_id, foreign)

    def test_manual_sync_by_manager_runs_through_sudo_service(self):
        order = self.order_payload()
        self.mock_list([order])
        self.mock_order(order, [self.order_line()])
        self.instance.with_user(self.user_manager).action_sync_orders()
        self.assertTrue(self.binding().sale_order_id)
