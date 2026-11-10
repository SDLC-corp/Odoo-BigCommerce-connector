"""Products (variants, export), customers (export scope) and inventory (both directions)."""
from unittest import mock

from odoo.tests.common import tagged

from ..services.customer_sync_service import BigCommerceCustomerSyncService
from ..services.inventory_sync_service import BigCommerceInventorySyncService
from ..services.product_sync_service import BigCommerceProductSyncService
from .common import STORE_PATH, BigCommerceCommon


def product_payload(pid=77, name="T-Shirt", sku="TSHIRT", variants=None, modified="2026-11-04T10:00:00+00:00",
                    **extra):
    payload = {
        "id": pid, "name": name, "sku": sku, "type": "physical", "price": 20.0, "weight": 1.0,
        "is_visible": True, "inventory_level": 0, "inventory_tracking": "variant", "categories": [],
        "date_modified": modified, "images": [],
        "variants": variants if variants is not None else [
            {"id": 881, "product_id": pid, "sku": "TSHIRT-S-RED", "inventory_level": 4,
             "option_values": [{"id": 1, "label": "S", "option_id": 10, "option_display_name": "Size"},
                               {"id": 2, "label": "Red", "option_id": 11, "option_display_name": "Colour"}]},
            {"id": 882, "product_id": pid, "sku": "TSHIRT-M-RED", "inventory_level": 6,
             "option_values": [{"id": 3, "label": "M", "option_id": 10, "option_display_name": "Size"},
                               {"id": 2, "label": "Red", "option_id": 11, "option_display_name": "Colour"}]},
        ],
    }
    payload.update(extra)
    return payload


@tagged("post_install", "-at_install", "bigcommerce")
class TestCatalogInventory(BigCommerceCommon):

    def setUp(self):
        super().setUp()
        # Categories are imported first when none is bound yet.
        self.http.add("GET", STORE_PATH + "/v3/catalog/categories", {"data": [], "meta": {"pagination": {
            "total": 0, "total_pages": 1, "current_page": 1}}})

    def product_service(self):
        return BigCommerceProductSyncService(self.instance._bc_service_instance())

    def page(self, items):
        return {"data": items, "meta": {"pagination": {"total": len(items), "total_pages": 1, "current_page": 1}}}

    # ---- products -----------------------------------------------------------------
    def test_options_become_attributes_and_distinct_variants(self):
        """Regression: every BigCommerce variant was written onto the same single Odoo variant."""
        self.http.add("GET", STORE_PATH + "/v3/catalog/products", self.page([product_payload()]))
        result = self.product_service().import_products(limit=10)
        self.assertTrue(result["success"], result)
        bindings = self.env["bigcommerce.product.binding"].search(
            [("instance_id", "=", self.instance.id), ("bigcommerce_variant_id", "in", ["881", "882"])])
        self.assertEqual(len(bindings), 2)
        self.assertEqual(len(bindings.product_id), 2, "each BigCommerce variant needs its own Odoo variant")
        small = bindings.filtered(lambda b: b.bigcommerce_variant_id == "881").product_id
        self.assertEqual(small.default_code, "TSHIRT-S-RED")
        self.assertEqual(sorted(small.product_template_attribute_value_ids.mapped("name")), ["Red", "S"])
        self.assertEqual(small.with_context(location=self.warehouse.lot_stock_id.id).qty_available, 4)
        params = self.http.calls_to("GET", STORE_PATH + "/v3/catalog/products")[0]["params"]
        self.assertEqual(params["sort"], "date_modified")
        self.assertEqual(self.instance.product_sync_checkpoint.isoformat(), "2026-11-04T10:00:00")

    def test_one_bad_product_does_not_abort_the_batch(self):
        good = product_payload(pid=78, name="Mug", sku="MUG", variants=[])
        bad = product_payload(pid=79, name="Broken", sku="BROKEN", variants=[])
        self.http.add("GET", STORE_PATH + "/v3/catalog/products", self.page([bad, good]))
        service = self.product_service()
        original = service._create_or_update_template_binding

        def explode(template, bc_product):
            if bc_product.get("id") == 79:
                self.env.cr.execute("SELECT 1/0")
            return original(template, bc_product)

        with mock.patch.object(service, "_create_or_update_template_binding", side_effect=explode):
            result = service.import_products(limit=10)
        self.assertEqual((result["created"], result["failed"]), (1, 1))
        self.assertTrue(self.env["product.template"].search([("default_code", "=", "MUG")]))

    def test_export_sends_weight_and_sku_and_skips_foreign_products(self):
        """Regression: product creation lacked the required weight; every company's products were pushed."""
        mine = self.env["product.template"].create({
            "name": "Export Me", "default_code": "EXP-1", "type": "consu", "weight": 2.5, "list_price": 9,
            "company_id": self.company.id})
        foreign = self.env["product.template"].create({
            "name": "Not Mine", "default_code": "EXP-2", "type": "consu", "company_id": self.company_b.id})
        service_tmpl = self.env["product.template"].create({"name": "Consulting", "type": "service"})
        self.http.add("POST", STORE_PATH + "/v3/catalog/products", {"data": {"id": 555}})
        selected = self.product_service()._templates_to_export(500)
        self.assertIn(mine, selected)
        self.assertNotIn(foreign, selected)
        self.assertNotIn(service_tmpl, selected)
        self.product_service()._push_product(mine)
        payload = self.http.calls_to("POST", STORE_PATH + "/v3/catalog/products")[0]["json"]
        self.assertEqual(payload["weight"], 2.5)
        self.assertEqual(payload["sku"], "EXP-1")
        # unchanged since the push -> not selected again
        self.assertNotIn(mine, self.product_service()._templates_to_export(500))

    # ---- customers ------------------------------------------------------------------
    def test_customer_export_only_pushes_customers_of_the_company(self):
        """Regression: every contact with an e-mail (vendors, other companies) was pushed to the store."""
        customer = self.env["res.partner"].create({
            "name": "Grace Hopper", "email": "grace@example.test", "customer_rank": 1})
        vendor = self.env["res.partner"].create({"name": "Vendor", "email": "vendor@example.test", "supplier_rank": 1})
        foreign = self.env["res.partner"].create({
            "name": "Other Co Customer", "email": "o@example.test", "customer_rank": 1, "company_id": self.company_b.id})
        self.http.add("POST", STORE_PATH + "/v3/customers", {"data": [{"id": 42}]})
        BigCommerceCustomerSyncService(self.instance._bc_service_instance()).export_customers(limit=500)
        sent = [c["json"][0]["email"] for c in self.http.calls_to("POST", STORE_PATH + "/v3/customers")]
        self.assertIn("grace@example.test", sent)
        self.assertNotIn(vendor.email, sent)
        self.assertNotIn(foreign.email, sent)
        grace = [c["json"][0] for c in self.http.calls_to("POST", STORE_PATH + "/v3/customers")
                 if c["json"][0]["email"] == customer.email][0]
        self.assertEqual((grace["first_name"], grace["last_name"]), ("Grace", "Hopper"))

    # ---- inventory --------------------------------------------------------------------
    def test_inventory_import_is_a_traceable_adjustment(self):
        """Regression: quants were overwritten directly, leaving no stock move."""
        product = self.make_product(sku="INV-1")
        self.bind_product(product, remote_product="90", remote_variant=False)
        self.http.add("GET", STORE_PATH + "/v3/catalog/products", self.page([
            {"id": 90, "name": "Inv", "inventory_level": 12, "inventory_tracking": "product", "variants": []}]))
        result = BigCommerceInventorySyncService(self.instance._bc_service_instance()).import_inventory(limit=50)
        self.assertEqual(result["updated"], 1, result)
        self.assertEqual(product.with_context(location=self.warehouse.lot_stock_id.id).qty_available, 12)
        move = self.env["stock.move"].search([("product_id", "=", product.id), ("is_inventory", "=", True)])
        self.assertTrue(move)

    def test_inventory_export_pushes_only_changed_quantities(self):
        self.instance.inventory_master = "odoo"
        changed = self.make_product(sku="INV-A")
        same = self.make_product(sku="INV-B")
        self.env["stock.quant"]._update_available_quantity(changed, self.warehouse.lot_stock_id, 7)
        self.env["stock.quant"]._update_available_quantity(same, self.warehouse.lot_stock_id, 3)
        b1 = self.bind_product(changed, remote_product="91", remote_variant=False)
        b2 = self.bind_product(same, remote_product="92", remote_variant=False)
        b1.write({"bigcommerce_inventory_level": 1, "bigcommerce_inventory_known": True})
        b2.write({"bigcommerce_inventory_level": 3, "bigcommerce_inventory_known": True})
        self.http.add("PUT", STORE_PATH + "/v3/catalog/products/91", {"data": {"id": 91}})
        result = BigCommerceInventorySyncService(self.instance._bc_service_instance()).export_inventory(limit=50)
        self.assertEqual((result["exported"], result["unchanged"]), (1, 1), result)
        self.assertEqual(self.http.calls_to("PUT", STORE_PATH + "/v3/catalog/products/91")[0]["json"],
                         {"inventory_level": 7})
        self.assertEqual(b1.bigcommerce_inventory_level, 7)

    def test_cron_exports_when_odoo_is_master(self):
        """Regression: the scheduled inventory job only ever imported."""
        self.instance.write({"inventory_master": "odoo", "auto_inventory_export": True})
        self.instance_b.auto_inventory_export = False
        with mock.patch.object(BigCommerceInventorySyncService, "export_inventory", return_value={}) as exp, \
                mock.patch.object(BigCommerceInventorySyncService, "import_inventory", return_value={}) as imp:
            self.env["bigcommerce.connector"].run_cron_sync_inventory()
        self.assertTrue(exp.called)
        self.assertFalse(imp.called)

    def test_sync_inventory_button_follows_master(self):
        self.instance.inventory_master = "odoo"
        with mock.patch.object(BigCommerceInventorySyncService, "export_inventory",
                               return_value={"success": True, "message": "ok"}) as exp:
            self.instance.with_user(self.user_manager).action_sync_inventory()
        self.assertTrue(exp.called)

    def test_inventory_import_zero_empties_the_stock(self):
        """Regression: a BigCommerce level of 0 was treated as "empty" and never applied."""
        product = self.make_product(sku="INV-0")
        self.env["stock.quant"]._update_available_quantity(product, self.warehouse.lot_stock_id, 5)
        self.bind_product(product, remote_product="93", remote_variant=False)
        self.http.add("GET", STORE_PATH + "/v3/catalog/products", self.page([
            {"id": 93, "name": "Sold out", "inventory_level": 0, "inventory_tracking": "product", "variants": []}]))
        result = BigCommerceInventorySyncService(self.instance._bc_service_instance()).import_inventory(limit=50)
        self.assertEqual(result["updated"], 1, result)
        self.assertEqual(product.with_context(location=self.warehouse.lot_stock_id.id).qty_available, 0)
