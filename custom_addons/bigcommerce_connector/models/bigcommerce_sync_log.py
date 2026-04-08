# -*- coding: utf-8 -*-

from odoo import _, api, fields, models


class BigCommerceSyncLog(models.Model):
    """Stores BigCommerce sync/API operation history for audit and diagnostics."""

    _name = "bigcommerce.sync.log"
    _description = "BigCommerce Sync Log"
    _order = "create_date desc, id desc"
    _rec_name = "operation_type"

    instance_id = fields.Many2one(
        "bigcommerce.connector",
        required=True,
        index=True,
        ondelete="cascade",
    )
    operation_type = fields.Selection(
        selection=[
            ("connection_test", "Connection Test"),
            ("product_import", "Product Import"),
            ("product_export", "Product Export"),
            ("category_import", "Category Import"),
            ("category_export", "Category Export"),
            ("customer_import", "Customer Import"),
            ("customer_export", "Customer Export"),
            ("order_import", "Order Import"),
            ("inventory_import", "Inventory Import"),
            ("inventory_export", "Inventory Export"),
            ("shipment_export", "Shipment Export"),
            ("webhook_receive", "Webhook Receive"),
            ("webhook_process", "Webhook Process"),
            ("retry", "Retry"),
            ("manual_action", "Manual Action"),
        ],
        required=True,
        index=True,
    )
    trigger_source = fields.Selection(
        selection=[
            ("manual", "Manual"),
            ("webhook", "Webhook"),
            ("cron", "Cron"),
            ("system", "System"),
        ],
        required=True,
        default="manual",
        index=True,
        help="Source that triggered this log entry.",
    )
    resource_type = fields.Selection(
        selection=[
            ("instance", "Instance"),
            ("product", "Product"),
            ("category", "Category"),
            ("customer", "Customer"),
            ("order", "Order"),
            ("inventory", "Inventory"),
            ("shipment", "Shipment"),
            ("webhook", "Webhook"),
            ("system", "System"),
        ],
        index=True,
    )
    resource_remote_id = fields.Char(index=True)
    request_url = fields.Char()
    request_method = fields.Char()
    request_payload = fields.Text()
    response_status = fields.Char()
    response_body = fields.Text()
    duration_ms = fields.Float()
    retry_count = fields.Integer(default=0)
    severity = fields.Selection(
        selection=[
            ("info", "Info"),
            ("warning", "Warning"),
            ("error", "Error"),
        ],
        default="info",
        required=True,
        index=True,
    )
    status = fields.Selection(
        selection=[
            ("draft", "Draft"),
            ("success", "Success"),
            ("failed", "Failed"),
            ("skipped", "Skipped"),
        ],
        required=True,
        default="draft",
        index=True,
    )
    error_message = fields.Text()
    note = fields.Text()

    @api.model_create_multi
    def create(self, vals_list):
        """Normalize log records and apply debug-mode payload trimming."""
        instance_ids = {vals.get("instance_id") for vals in vals_list if vals.get("instance_id")}
        debug_map = {}
        if instance_ids:
            for instance in self.env["bigcommerce.connector"].sudo().browse(list(instance_ids)):
                debug_map[instance.id] = bool(instance.debug_mode)

        for vals in vals_list:
            status = vals.get("status") or "draft"
            if not vals.get("severity"):
                if status == "failed":
                    vals["severity"] = "error"
                elif status == "skipped":
                    vals["severity"] = "warning"
                else:
                    vals["severity"] = "info"

            if not vals.get("trigger_source"):
                context_source = (self.env.context.get("bigcommerce_log_source") or "").strip().lower()
                if context_source in ("manual", "webhook", "cron", "system"):
                    vals["trigger_source"] = context_source
                elif vals.get("operation_type") in ("webhook_receive", "webhook_process"):
                    vals["trigger_source"] = "webhook"
                elif self.env.context.get("cron_id"):
                    vals["trigger_source"] = "cron"
                elif vals.get("operation_type") == "manual_action":
                    vals["trigger_source"] = "manual"
                else:
                    vals["trigger_source"] = "manual"

            vals["retry_count"] = int(vals.get("retry_count") or 0)
            if vals.get("duration_ms") not in (None, False, ""):
                vals["duration_ms"] = float(vals.get("duration_ms") or 0.0)

            instance_id = vals.get("instance_id")
            debug_mode = debug_map.get(instance_id, False)
            if not debug_mode:
                if vals.get("request_payload"):
                    vals["request_payload"] = str(vals.get("request_payload"))[:500]
                if vals.get("response_body"):
                    vals["response_body"] = str(vals.get("response_body"))[:500]
            else:
                if vals.get("request_payload"):
                    vals["request_payload"] = str(vals.get("request_payload"))[:3000]
                if vals.get("response_body"):
                    vals["response_body"] = str(vals.get("response_body"))[:3000]

        return super().create(vals_list)

    def _notification_action(self, message, notif_type="info"):
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("BigCommerce"),
                "message": message,
                "type": notif_type,
                "sticky": False,
            },
        }

    def action_mark_failed(self):
        """Mark selected logs as failed as a manual admin action."""
        self.write({"status": "failed"})
        return self._notification_action(_("Selected logs were marked as failed."), notif_type="warning")

    def action_mark_success(self):
        """Mark selected logs as successful as a manual admin action."""
        self.write({"status": "success", "error_message": False})
        return self._notification_action(_("Selected logs were marked as success."), notif_type="success")
