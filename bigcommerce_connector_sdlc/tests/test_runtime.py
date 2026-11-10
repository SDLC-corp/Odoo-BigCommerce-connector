"""Runtime integrity: views, actions, dashboard, purge, manifest/data consistency."""
from datetime import timedelta

from odoo import fields
from odoo.tests.common import tagged

from .common import BigCommerceCommon


@tagged("post_install", "-at_install", "bigcommerce")
class TestRuntime(BigCommerceCommon):

    def test_every_action_view_renders_for_each_role(self):
        actions = self.env["ir.actions.act_window"].search([("res_model", "like", "bigcommerce.%")])
        self.assertTrue(actions)
        for user in (self.user_admin, self.user_manager, self.user_reader):
            for action in actions:
                model = self.env[action.res_model].with_user(user)
                if not model.has_access("read"):
                    continue
                for view_type in ("list", "form", "search"):
                    with self.subTest(user=user.login, action=action.name, view=view_type):
                        model.get_views([(False, view_type)])

    def test_every_list_action_has_empty_state_help(self):
        actions = self.env["ir.actions.act_window"].search([
            ("res_model", "like", "bigcommerce.%"), ("view_mode", "like", "list")])
        missing = actions.filtered(lambda a: not a.help)
        self.assertFalse(missing, "actions without empty-state help: %s" % missing.mapped("name"))

    def test_dashboard_data_for_reader(self):
        data = self.env["bigcommerce.dashboard"].with_user(self.user_reader).get_dashboard_data("30", "all")
        self.assertIn("open_exceptions", data["totals"])
        ids = [r["id"] for r in data["instances"]]
        self.assertIn(self.instance.id, ids)
        self.assertNotIn(self.instance_b.id, ids)

    def test_dashboard_trends_group_by_day_and_status(self):
        Log = self.env["bigcommerce.sync.log"]
        Log.create([
            {"instance_id": self.instance.id, "operation_type": "order_import", "status": "success"},
            {"instance_id": self.instance.id, "operation_type": "order_import", "status": "failed"},
            {"instance_id": self.instance.id, "operation_type": "order_import", "status": "failed"},
        ])
        trends = self.env["bigcommerce.dashboard"].get_dashboard_data("7", str(self.instance.id))["trends"]
        self.assertEqual((trends[-1]["success"], trends[-1]["failed"]), (1, 2))

    def test_exception_counter_and_action(self):
        self.env["bigcommerce.order.binding"].create({
            "instance_id": self.instance.id, "bigcommerce_order_id": "500", "sync_state": "error"})
        self.env["bigcommerce.webhook.event"].create({
            "instance_id": self.instance.id, "payload_json": "{}", "status": "failed"})
        self.assertEqual(self.instance.dashboard_exception_count, 2)
        action = self.instance.with_user(self.user_reader).action_open_exceptions()
        self.assertEqual(action["res_model"], "bigcommerce.order.binding")

    def test_purge_keeps_open_work(self):
        old = fields.Datetime.now() - timedelta(days=40)
        Log = self.env["bigcommerce.sync.log"]
        Event = self.env["bigcommerce.webhook.event"]
        old_log = Log.create({"instance_id": self.instance.id, "operation_type": "order_import", "status": "success"})
        new_log = Log.create({"instance_id": self.instance.id, "operation_type": "order_import", "status": "success"})
        self.env.cr.execute("UPDATE bigcommerce_sync_log SET create_date = %s WHERE id = %s", (old, old_log.id))
        done = Event.create({"instance_id": self.instance.id, "payload_json": "{}", "status": "done", "received_at": old})
        failed = Event.create({"instance_id": self.instance.id, "payload_json": "{}", "status": "failed", "received_at": old})
        pending = Event.create({"instance_id": self.instance.id, "payload_json": "{}", "status": "pending", "received_at": old})
        self.env.invalidate_all()
        self.env["bigcommerce.connector"].run_cron_purge_history()
        self.assertFalse(old_log.exists())
        self.assertTrue(new_log.exists())
        self.assertFalse(done.exists())
        self.assertTrue(failed.exists() and pending.exists())

    def test_purge_disabled_with_zero_retention(self):
        self.instance.log_retention_days = 0
        log = self.env["bigcommerce.sync.log"].create(
            {"instance_id": self.instance.id, "operation_type": "order_import", "status": "success"})
        self.env.cr.execute("UPDATE bigcommerce_sync_log SET create_date = now() - interval '400 days' WHERE id = %s",
                            (log.id,))
        self.env["bigcommerce.connector"].run_cron_purge_history()
        self.assertTrue(log.exists())

    def test_crons_exist_and_point_to_existing_methods(self):
        crons = self.env["ir.cron"].search([("model_id.model", "=", "bigcommerce.connector")])
        self.assertGreaterEqual(len(crons), 7)
        for cron in crons:
            method = cron.code.split("model.")[1].split("(")[0]
            self.assertTrue(hasattr(self.env["bigcommerce.connector"], method), method)

    def test_new_connector_does_not_import_whole_history(self):
        connector = self.env["bigcommerce.connector"].create({
            "name": "Fresh", "store_hash": "fresh", "access_token": "TEST-TOKEN-F"})
        self.assertGreater(connector.order_sync_checkpoint, fields.Datetime.now() - timedelta(days=31))

    def test_product_binding_display_name(self):
        product = self.make_product(sku="DISP-1", name="Display Widget")
        binding = self.bind_product(product, remote_product="1", remote_variant="2")
        self.assertIn("Display Widget", binding.display_name)
        self.assertIn("DISP-1", binding.display_name)
