"""Runtime security: roles, hidden credentials, privileged actions and multi-company isolation."""
from odoo.exceptions import AccessError
from odoo.tests.common import tagged

from .common import STORE_PATH, BigCommerceCommon

SECRET_FIELDS = ("access_token", "client_secret", "webhook_secret", "gemini_api_key")


@tagged("post_install", "-at_install", "bigcommerce")
class TestSecurity(BigCommerceCommon):

    # ---- credentials -----------------------------------------------------------
    def test_credentials_hidden_from_users_and_managers(self):
        """Regression: every internal user could read the API token (full CRUD for base.group_user)."""
        for user in (self.user_reader, self.user_manager):
            instance = self.instance.with_user(user)
            visible = instance.fields_get()
            for name in SECRET_FIELDS:
                self.assertNotIn(name, visible, "%s sees %s" % (user.login, name))
                with self.assertRaises(AccessError):
                    instance.read([name])
            found = instance.search_read([("id", "=", self.instance.id)], ["name"])
            self.assertNotIn("TEST-TOKEN", str(found))

    def test_admin_reads_credentials(self):
        self.assertEqual(self.instance.with_user(self.user_admin).access_token, "TEST-TOKEN-A")

    def test_plain_internal_user_has_no_access(self):
        with self.assertRaises(AccessError):
            self.env["bigcommerce.connector"].with_user(self.user_plain).search([])
        with self.assertRaises(AccessError):
            self.env["bigcommerce.sync.log"].with_user(self.user_plain).search([])

    def test_reader_cannot_write_or_create(self):
        with self.assertRaises(AccessError):
            self.instance.with_user(self.user_reader).write({"name": "Hacked"})
        with self.assertRaises(AccessError):
            self.env["bigcommerce.field.mapping"].with_user(self.user_manager).create({
                "connector_id": self.instance.id, "mapping_type": "product", "odoo_model": "product.template",
                "odoo_field_name": "name", "bigcommerce_field_name": "name",
            })

    # ---- privileged actions -----------------------------------------------------
    def test_reader_cannot_run_any_sync_and_nothing_reaches_the_network(self):
        actions = ("action_test_connection", "action_sync_orders", "action_sync_products", "action_sync_customers",
                   "action_sync_categories", "action_sync_inventory", "action_export_shipments",
                   "action_process_webhooks", "action_export_products", "action_export_customers",
                   "action_register_webhooks", "action_delete_webhooks", "action_generate_webhook_secret")
        for user in (self.user_reader, self.user_plain):
            for action in actions:
                with self.subTest(user=user.login, action=action), self.assertRaises(AccessError):
                    getattr(self.instance.with_user(user), action)()
        self.assertFalse(self.http.calls)

    def test_manager_cannot_run_admin_actions(self):
        for action in ("action_export_products", "action_export_customers", "action_register_webhooks",
                       "action_delete_webhooks", "action_generate_webhook_secret",
                       "action_generate_default_mappings"):
            with self.subTest(action=action), self.assertRaises(AccessError):
                getattr(self.instance.with_user(self.user_manager), action)()
        self.assertFalse(self.http.calls)

    def test_manager_can_test_connection_without_seeing_the_token(self):
        self.http.add("GET", STORE_PATH + "/v2/store", {"id": "1025646", "name": "Demo"})
        self.instance.with_user(self.user_manager).action_test_connection()
        self.assertEqual(self.instance.state, "connected")
        call = self.http.calls_to("GET", STORE_PATH + "/v2/store")[0]
        self.assertEqual(call["headers"]["X-Auth-Token"], "TEST-TOKEN-A")

    # ---- multi-company -----------------------------------------------------------
    def test_company_isolation_of_connectors_bindings_logs_and_events(self):
        self.env["bigcommerce.sync.log"].create({
            "instance_id": self.instance.id, "operation_type": "manual_action", "status": "success"})
        self.env["bigcommerce.order.binding"].create({
            "instance_id": self.instance.id, "bigcommerce_order_id": "999", "sync_state": "error"})
        self.env["bigcommerce.webhook.event"].create({
            "instance_id": self.instance.id, "payload_json": "{}", "status": "failed"})
        as_b = self.user_manager_b
        self.assertEqual(self.env["bigcommerce.connector"].with_user(as_b).search([]), self.instance_b)
        for model in ("bigcommerce.sync.log", "bigcommerce.order.binding", "bigcommerce.webhook.event"):
            records = self.env[model].with_user(as_b).search([])
            self.assertFalse(records.filtered(lambda r: r.instance_id == self.instance), model)
        with self.assertRaises(AccessError):
            self.instance.with_user(as_b).action_sync_orders()

    def test_dashboard_scoped_to_user_companies(self):
        data = self.env["bigcommerce.dashboard"].with_user(self.user_manager_b).get_dashboard_data()
        self.assertEqual([row["id"] for row in data["instances"]], [self.instance_b.id])

    def test_webhook_events_manager_only_actions(self):
        event = self.env["bigcommerce.webhook.event"].create({
            "instance_id": self.instance.id, "payload_json": "{}", "status": "failed"})
        with self.assertRaises(AccessError):
            event.with_user(self.user_reader).action_retry()
        event.with_user(self.user_manager).action_retry()
        self.assertEqual(event.status, "pending")
