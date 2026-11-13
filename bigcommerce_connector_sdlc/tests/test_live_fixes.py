"""Defects found in the live BigCommerce test (19.0.2.1.2).

A. A new connector had no field mappings (the default ones were only created by a method no button
   called), so the order status was never applied: BigCommerce order #100 "Awaiting Fulfillment"
   stayed a quotation without delivery.
B. Odoo added its product default tax (15%) to an order on which BigCommerce charged no tax:
   104.85 became 120.58.
C. (browser check, 19.0.2.1.2) the default mappings showed a blank "Odoo Field" column and a blank
   rule name ("Unnamed"): they stored only the technical field name, while the lists show the
   Many2one ``odoo_field_id`` and the record name came from it.
"""
import importlib.util
from pathlib import Path

from odoo.exceptions import ValidationError
from odoo.tests import Form
from odoo.tests.common import tagged

from ..services.order_sync_service import BigCommerceOrderSyncService
from .common import BigCommerceCommon

DEFAULT_MAPPING_COUNT = 11


def _load_migration(version):
    path = Path(__file__).resolve().parent.parent / "migrations" / version / "post-migration.py"
    spec = importlib.util.spec_from_file_location("bc_post_migration_" + version.replace(".", "_"), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _post_migration():
    return _load_migration("19.0.2.1.2")


def _post_migration_2_1_3():
    return _load_migration("19.0.2.1.3")


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
        self.assertTrue(sale.picking_ids.filtered(lambda p: p.picking_type_code == "outgoing"))

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
        self.mappings(empty).unlink()  # as created before 19.0.2.1.2
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

    # ---- Defect C: blank "Odoo Field" and rule name of the default mappings ---------------
    def _displayed_rows(self, connector):
        """What the Field Mapping tab of the connector form loads, as the web client reads it."""
        spec = {"mapping_ids": {"fields": {
            "display_name": {},
            "mapping_type": {},
            "odoo_field_id": {"fields": {"display_name": {}}},
            "bigcommerce_field_id": {"fields": {"display_name": {}}},
        }, "context": {"active_test": False}}}
        return connector.web_read(spec)[0]["mapping_ids"]

    def test_default_mappings_display_odoo_field_and_rule_name(self):
        connector = self.env["bigcommerce.connector"].create({
            "name": "Display connector", "store_hash": "displaystore", "access_token": "TEST-TOKEN-D",
            "company_id": self.company.id,
        })
        definitions = {
            (d["mapping_type"], d["odoo_field_name"], d["bigcommerce_field_name"]): d
            for d in self.env["bigcommerce.field.mapping"]._default_mapping_definitions()
        }
        mappings = self.mappings(connector)
        self.assertEqual(len(mappings), DEFAULT_MAPPING_COUNT)
        for mapping in mappings:
            definition = definitions[(mapping.mapping_type, mapping.odoo_field_name, mapping.bigcommerce_field_name)]
            self.assertEqual(mapping.odoo_field_id.model, mapping.odoo_model)
            self.assertEqual(mapping.odoo_field_id.name, mapping.odoo_field_name)
            self.assertEqual(mapping.bigcommerce_field_id.name, mapping.bigcommerce_field_name)
            self.assertEqual(mapping.name, definition["name"])
            self.assertEqual(mapping.display_name, definition["name"])
        rows = self._displayed_rows(connector)
        self.assertEqual(len(rows), DEFAULT_MAPPING_COUNT)
        by_id = {m.id: m for m in mappings}
        for row in rows:
            mapping = by_id[row["id"]]
            self.assertTrue(row["odoo_field_id"], "blank Odoo Field cell")
            self.assertEqual(row["odoo_field_id"]["display_name"], mapping.odoo_field_id.display_name)
            self.assertTrue(row["odoo_field_id"]["display_name"])
            self.assertEqual(row["bigcommerce_field_id"]["display_name"], mapping.bigcommerce_field_name)
            self.assertEqual(row["display_name"], mapping.name)
        status = mappings.filtered(lambda m: m.odoo_field_name == "state")
        state_field = self.env["ir.model.fields"]._get("sale.order", "state")
        self.assertEqual(status.odoo_field_id, state_field)
        self.assertEqual(status.display_name, "status -> sale.order.state")
        status_row = next(r for r in rows if r["id"] == status.id)
        self.assertEqual(status_row["odoo_field_id"]["display_name"],
                         "%s (%s)" % (state_field.field_description, state_field.model_id.name))

    def test_mapping_created_by_admin_through_the_form(self):
        """The form sets ``odoo_field_id`` (the technical name follows) and the rule gets a name."""
        partner_ref = self.env["ir.model.fields"]._get("res.partner", "ref")
        # The "BigCommerce Field" dropdown offers the connector's catalog (filled when the tab loads).
        catalog = self.env["bigcommerce.field.mapping"]._get_or_create_catalog_field(
            self.instance.id, "customer", "company")
        with Form(self.env["bigcommerce.field.mapping"].with_user(self.user_admin)) as form:
            form.connector_id = self.instance
            form.mapping_type = "customer"
            form.odoo_field_id = partner_ref
            form.bigcommerce_field_id = catalog
        mapping = form.record
        self.assertEqual((mapping.odoo_model, mapping.odoo_field_name), ("res.partner", "ref"))
        self.assertEqual(mapping.odoo_field_id, partner_ref)
        self.assertEqual(mapping.bigcommerce_field_name, "company")
        self.assertEqual(mapping.display_name, "company -> ref")
        row = next(r for r in self._displayed_rows(self.instance) if r["id"] == mapping.id)
        self.assertEqual(row["odoo_field_id"]["display_name"], partner_ref.display_name)
        # The customer import applies it.
        result = self.env["bigcommerce.field.mapping"]._prepare_odoo_vals_from_mapping(
            {"company": "Acme Inc", "email": "a@example.com"}, "customer", self.instance)
        self.assertEqual(result["vals"]["ref"], "Acme Inc")

    def test_technical_name_change_keeps_odoo_field_in_line(self):
        mapping = self.mappings().filtered(lambda m: m.odoo_field_name == "phone")
        mapping.write({"odoo_field_name": "ref"})
        self.assertEqual(mapping.odoo_field_id, self.env["ir.model.fields"]._get("res.partner", "ref"))
        with self.assertRaises(ValidationError):
            mapping.write({"odoo_field_id": False})

    def test_migration_fills_blank_values_and_keeps_customised_ones(self):
        mappings = self.mappings()
        status = mappings.filtered(lambda m: m.odoo_field_name == "state")
        email = mappings.filtered(lambda m: m.odoo_field_name == "email")
        sku = mappings.filtered(lambda m: m.odoo_field_name == "default_code")
        phone = mappings.filtered(lambda m: m.odoo_field_name == "phone")
        # A rule customised by an administrator: another Odoo field and its own name.
        partner_ref = self.env["ir.model.fields"]._get("res.partner", "ref")
        phone.write({"odoo_field_id": partner_ref.id, "name": "Admin phone rule"})
        email.name = "Custom email rule"
        self.env.flush_all()
        # As the published build stored them: no odoo_field_id; one rule also without name and
        # BigCommerce field reference.
        self.env.cr.execute(
            "UPDATE bigcommerce_field_mapping SET odoo_field_id = NULL WHERE id IN %s",
            (tuple((status | email | sku).ids),))
        self.env.cr.execute(
            "UPDATE bigcommerce_field_mapping SET name = NULL, bigcommerce_field_id = NULL WHERE id = %s",
            (sku.id,))
        self.env.invalidate_all()
        self.assertFalse(status.odoo_field_id)
        self.assertFalse(sku.display_name)

        counts = _post_migration_2_1_3().fill_blank_mapping_values(self.env)

        self.assertEqual(counts, {"odoo_field": 3, "bigcommerce_field": 1, "name": 1})
        self.assertEqual(status.odoo_field_id, self.env["ir.model.fields"]._get("sale.order", "state"))
        self.assertEqual(status.display_name, "status -> sale.order.state")
        self.assertEqual(sku.odoo_field_id, self.env["ir.model.fields"]._get("product.template", "default_code"))
        self.assertEqual(sku.bigcommerce_field_id.name, "sku")
        self.assertEqual(sku.name, "sku -> default_code")
        self.assertEqual(email.odoo_field_id, self.env["ir.model.fields"]._get("res.partner", "email"))
        self.assertEqual(email.name, "Custom email rule", "a customised name is kept")
        self.assertEqual(phone.odoo_field_id, partner_ref, "a customised Odoo field is kept")
        self.assertEqual((phone.odoo_field_name, phone.name), ("ref", "Admin phone rule"))
        for row in self._displayed_rows(self.instance):
            self.assertTrue(row["odoo_field_id"] and row["odoo_field_id"]["display_name"])
            self.assertTrue(row["bigcommerce_field_id"] and row["bigcommerce_field_id"]["display_name"])
            self.assertTrue(row["display_name"])
        self.assertEqual(_post_migration_2_1_3().fill_blank_mapping_values(self.env),
                         {"odoo_field": 0, "bigcommerce_field": 0, "name": 0}, "idempotent")
        # The repaired status map still confirms an "Awaiting Fulfillment" order.
        sale = self.import_live_order().sale_order_id
        self.assertEqual(sale.state, "sale")

    def test_bigcommerce_roles_read_rules_with_odoo_field(self):
        """``ir.model.fields`` is readable by "Access Rights" administrators only: the BigCommerce
        roles (no Settings rights) still open a default rule, now that it has an Odoo field."""
        status = self.mappings().filtered(lambda m: m.odoo_field_name == "state")
        self.assertTrue(status.odoo_field_id)
        for user in (self.user_reader, self.user_manager, self.user_admin):
            self.assertFalse(user.has_group("base.group_erp_manager"))
            self.env.invalidate_all()  # nothing of ir.model.fields cached by the superuser
            self.assertEqual(status.with_user(user).read(["odoo_field_type"])[0]["odoo_field_type"], "selection")
            self.env.invalidate_all()
            self.assertEqual(status.with_user(user)._get_odoo_field_def().name, "state")
            self.env.invalidate_all()
            data = status.with_user(user).web_read({
                "display_name": {}, "odoo_field_label": {}, "odoo_field_type": {},
                "odoo_field_id": {"fields": {"display_name": {}}},
            })[0]
            self.assertEqual(data["display_name"], "status -> sale.order.state")
            self.assertEqual(data["odoo_field_type"], "selection")
            self.assertTrue(data["odoo_field_label"])
            self.assertTrue(data["odoo_field_id"]["display_name"])
        self.env.invalidate_all()
        result = status.with_user(self.user_admin).action_test_mapping()
        wizard = self.env[result["res_model"]].browse(result["res_id"])
        self.assertIn('"state": "sale"', wizard.mapped_result)

    def test_admin_adds_mapping_in_connector_form(self):
        """"Add a line" on the Field Mapping tab: choosing the mapping type failed ("Missing required
        value for the field 'Connector'"), the onchange created catalog fields for the connector's
        NewId. The calls are those the web client makes (captured in the browser)."""
        Mapping = self.env["bigcommerce.field.mapping"].with_user(self.user_admin).with_context(
            default_connector_id=self.instance.id)
        spec = {name: {} for name in (
            "active", "mapping_type", "direction", "is_required", "default_value", "name", "odoo_model",
            "odoo_field_name", "bigcommerce_field_name", "bigcommerce_field_path", "transform_type")}
        spec.update({name: {"fields": {"display_name": {}}}
                     for name in ("connector_id", "odoo_field_id", "bigcommerce_field_id")})
        values = {"active": True, "mapping_type": "customer", "direction": "import",
                  "connector_id": {"id": self.instance.id}, "odoo_field_id": False,
                  "bigcommerce_field_id": False, "odoo_model": False, "odoo_field_name": False,
                  "bigcommerce_field_name": False, "transform_type": "none"}
        catalog_domain = [("connector_id", "=", self.instance.id), ("mapping_type", "=", "customer")]
        self.env["bigcommerce.field"].search(catalog_domain + [("name", "=", "company")]).unlink()
        result = Mapping.onchange(values, ["mapping_type"], spec)
        self.assertEqual(result["value"].get("odoo_model"), "res.partner")
        catalog = self.env["bigcommerce.field"].search(catalog_domain + [("name", "=", "company")])
        self.assertTrue(catalog, "the dropdown of BigCommerce fields is filled for the connector")

        partner_ref = self.env["ir.model.fields"]._get("res.partner", "ref")
        values.update(odoo_model="res.partner", odoo_field_id={"id": partner_ref.id})
        result = Mapping.onchange(values, ["odoo_field_id"], spec)
        self.assertEqual(result["value"].get("odoo_field_name"), "ref")
        values.update(odoo_field_name="ref", bigcommerce_field_id={"id": catalog.id})
        result = Mapping.onchange(values, ["bigcommerce_field_id"], spec)
        self.assertEqual(result["value"].get("bigcommerce_field_name"), "company")

        before = self.mappings()
        self.instance.with_user(self.user_admin).write({"mapping_ids": [(0, 0, {
            "active": True, "mapping_type": "customer", "direction": "import", "odoo_model": "res.partner",
            "odoo_field_id": partner_ref.id, "odoo_field_name": "ref", "bigcommerce_field_id": catalog.id,
            "bigcommerce_field_name": "company", "bigcommerce_field_path": "company", "transform_type": "none",
        })]})
        mapping = self.mappings() - before
        self.assertEqual(len(mapping), 1)
        self.assertEqual((mapping.odoo_field_id, mapping.odoo_field_name), (partner_ref, "ref"))
        self.assertEqual((mapping.bigcommerce_field_id, mapping.bigcommerce_field_name), (catalog, "company"))
        self.assertEqual(mapping.display_name, "company -> ref")
