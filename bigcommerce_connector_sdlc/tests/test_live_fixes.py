"""Defects found in the live BigCommerce test (20.0.2.1.2).

A. A new connector had no field mappings (the default ones were only created by a method no button
   called), so the order status was never applied: BigCommerce order #100 "Awaiting Fulfillment"
   stayed a quotation without delivery.
B. Odoo added its product default tax (15%) to an order on which BigCommerce charged no tax:
   104.85 became 120.58.
"""
import importlib.util
from pathlib import Path

from odoo.tests.common import tagged

from ..services.order_sync_service import BigCommerceOrderSyncService
from .common import BigCommerceCommon

DEFAULT_MAPPING_COUNT = 11


def _post_migration():
    path = Path(__file__).resolve().parent.parent / "migrations" / "20.0.2.1.2" / "post-migration.py"
    spec = importlib.util.spec_from_file_location("bc_post_migration_20_0_2_1_2", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@tagged("post_install", "-at_install", "bigcommerce")
class TestLiveFixes(BigCommerceCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tax15 = cls.env["account.tax"].create({
            "name": "BC Test Sales Tax 15%", "amount": 15.0, "amount_type": "percent",
            "type_tax_use": "sale", "company_id": cls.company.id,
        })
        cls.currency_code = cls.company.currency_id.name
        cls.prod_a = cls.env["product.product"].create({
            "name": "Live Widget A", "default_code": "LIVE-A", "type": "consu", "is_storable": True,
            "list_price": 34.95, "taxes_id": [(6, 0, cls.tax15.ids)],
        })
        cls.prod_b = cls.env["product.product"].create({
            "name": "Live Widget B", "default_code": "LIVE-B", "type": "consu", "is_storable": True,
            "list_price": 29.95, "taxes_id": [(6, 0, cls.tax15.ids)],
        })

    def setUp(self):
        super().setUp()
        self.bind_product(self.prod_a, remote_product="111", remote_variant="1111")
        self.bind_product(self.prod_b, remote_product="222", remote_variant="2222")

    def mappings(self, instance=None):
        return self.env["bigcommerce.field.mapping"].with_context(active_test=False).search(
            [("connector_id", "=", (instance or self.instance).id)])

    def import_live_order(self, **extra):
        """Replica of live order #100: 34.95 + 2 x 29.95 + 10.00 shipping = 104.85."""
        vals = {"order_id": 100, "total": "104.8500", "shipping": "10.0000", "currency": self.currency_code,
                "subtotal_ex_tax": "94.8500", "total_tax": "0.0000"}
        vals.update(extra)
        order = self.order_payload(**vals)
        if order.get("total_tax") is None:
            order.pop("total_tax")
        lines = [
            self.order_line(line_id=601, order_id=100, product_id=111, variant_id=1111, sku="LIVE-A", qty=1,
                            price="34.9500", name="Live Widget A"),
            self.order_line(line_id=602, order_id=100, product_id=222, variant_id=2222, sku="LIVE-B", qty=2,
                            price="29.9500", name="Live Widget B"),
        ]
        self.mock_order(order, lines)
        result = BigCommerceOrderSyncService(self.instance._bc_service_instance()).import_order_by_id(100)
        self.assertTrue(result["success"], result)
        return self.env["bigcommerce.order.binding"].search(
            [("instance_id", "=", self.instance.id), ("bigcommerce_order_id", "=", "100")])

    # ---- Defect A: default mappings ------------------------------------------------
    def test_new_connector_has_default_mappings_and_confirms_awaiting_fulfillment(self):
        """Regression: without calling any action, the order is confirmed and has a delivery."""
        connector = self.env["bigcommerce.connector"].create({
            "name": "Fresh connector", "store_hash": "freshstore", "access_token": "TEST-TOKEN-F",
            "company_id": self.company.id,
        })
        mappings = self.mappings(connector)
        self.assertEqual(len(mappings), DEFAULT_MAPPING_COUNT)
        self.assertTrue(all(mappings.mapped("is_system")))
        status = mappings.filtered(lambda m: m.mapping_type == "order" and m.odoo_field_name == "state")
        self.assertEqual(len(status), 1)
        self.assertEqual(status.bigcommerce_field_name, "status")
        self.assertIn('"Awaiting Fulfillment": "sale"', status.selection_map_json)
        # The fixture connector was created the same way; the import uses its mappings.
        self.assertEqual(len(self.mappings()), DEFAULT_MAPPING_COUNT)
        sale = self.import_live_order().sale_order_id
        self.assertEqual(sale.state, "sale")
        deliveries = sale.picking_ids.filtered(lambda p: p.picking_type_code == "outgoing")
        self.assertTrue(deliveries)
        # Confirmed exactly once: a re-sync of the same "Awaiting Fulfillment" order (status map
        # from the default mappings + the October 20.0 status fix) neither re-confirms it nor
        # creates a second delivery.
        date_order = sale.date_order
        resynced = self.import_live_order().sale_order_id
        self.assertEqual(resynced, sale)
        self.assertEqual(sale.state, "sale")
        self.assertEqual(sale.picking_ids.filtered(lambda p: p.picking_type_code == "outgoing"), deliveries)
        self.assertEqual(sale.date_order, date_order)

    def test_default_mapping_generation_is_idempotent(self):
        before = self.mappings()
        self.assertEqual(len(before), DEFAULT_MAPPING_COUNT)
        result = self.instance._generate_default_mappings()[self.instance.id]
        self.assertEqual((result["created"], result["skipped"]), (0, DEFAULT_MAPPING_COUNT))
        self.instance.with_user(self.user_admin).action_generate_default_mappings()
        self.assertEqual(self.mappings(), before)
        # An archived default rule is neither duplicated (unique constraint) nor re-activated.
        archived = before.filtered(lambda m: m.odoo_field_name == "phone")
        archived.active = False
        self.instance._generate_default_mappings()
        self.assertEqual(self.mappings(), before)
        self.assertFalse(archived.active)

    def test_migration_fills_only_connectors_without_mappings(self):
        Connector = self.env["bigcommerce.connector"]
        empty = Connector.create({"name": "Old, never mapped", "store_hash": "oldstore1",
                                  "access_token": "TEST-TOKEN-O1", "company_id": self.company.id})
        self.mappings(empty).unlink()  # as created before 20.0.2.1.2
        customised = Connector.create({"name": "Old, customised", "store_hash": "oldstore2",
                                       "access_token": "TEST-TOKEN-O2", "company_id": self.company.id})
        self.mappings(customised).filtered(lambda m: m.odoo_field_name == "state").unlink()
        self.assertFalse(self.mappings(empty))
        kept = self.mappings(customised)
        self.assertEqual(len(kept), DEFAULT_MAPPING_COUNT - 1)

        filled = _post_migration().generate_missing_default_mappings(self.env)

        self.assertEqual(filled, empty)
        self.assertEqual(len(self.mappings(empty)), DEFAULT_MAPPING_COUNT)
        self.assertEqual(self.mappings(customised), kept, "an administrator's removal is respected")
        self.assertFalse(_post_migration().generate_missing_default_mappings(self.env), "idempotent")

    # ---- Defect B: taxes ---------------------------------------------------------------
    def test_order_without_tax_gets_no_odoo_taxes(self):
        """Regression: live order #100 (no tax) got Odoo's 15% default tax, 104.85 -> 120.58."""
        binding = self.import_live_order()
        sale = binding.sale_order_id
        self.assertEqual(len(sale.order_line), 3)
        self.assertIn("BIGCOMMERCE_SHIPPING", sale.order_line.mapped("product_id.default_code"))
        self.assertFalse(sale.order_line.tax_ids, "no line may carry a tax")
        self.assertAlmostEqual(sale.amount_tax, 0.0)
        self.assertAlmostEqual(sale.amount_total, 104.85)
        self.assertFalse(binding.amount_mismatch)
        self.assertFalse(binding.needs_attention, binding.exception_reason)
        # The behaviour before the fix (Odoo's 15% on the product lines) is caught by "Total differs".
        sale.order_line.filtered(lambda l: l.product_id in (self.prod_a | self.prod_b)).tax_ids = self.tax15
        self.assertAlmostEqual(sale.amount_total, 104.85 + 0.15 * 94.85, places=2)
        self.assertTrue(binding.amount_mismatch)
        self.assertIn("Total differs", binding.exception_reason)

    def test_order_without_tax_detected_from_inc_and_ex_totals(self):
        """``total_tax`` absent: equal tax-inclusive and tax-exclusive totals mean no tax."""
        binding = self.import_live_order(total_tax=None)
        self.assertFalse(binding.sale_order_id.order_line.tax_ids)
        self.assertAlmostEqual(binding.sale_order_id.amount_total, 104.85)

    def test_order_with_tax_keeps_odoo_taxes(self):
        binding = self.import_live_order(total_tax="14.2275", total_ex_tax="104.8500", total_inc_tax="119.0775")
        sale = binding.sale_order_id
        product_lines = sale.order_line.filtered(lambda l: l.product_id in (self.prod_a | self.prod_b))
        self.assertEqual(len(product_lines), 2)
        for line in product_lines:
            self.assertEqual(line.tax_ids, self.tax15)
        self.assertGreater(sale.amount_tax, 0.0)
