# -*- coding: utf-8 -*-

import json

from odoo import _, api, fields, models

from ..services.webhook_service import BigCommerceWebhookService


class BigCommerceWebhookEvent(models.Model):
    """Stores raw incoming webhook events for future processing and retries."""

    _name = "bigcommerce.webhook.event"
    _description = "BigCommerce Webhook Event"
    _order = "received_at desc, id desc"
    _rec_name = "event_type"

    instance_id = fields.Many2one(
        "bigcommerce.connector",
        required=True,
        index=True,
        ondelete="cascade",
    )
    event_type = fields.Char(index=True)
    scope = fields.Char(index=True)
    webhook_id = fields.Char(index=True)
    resource_id = fields.Char(index=True)
    payload_json = fields.Text(required=True)
    headers_json = fields.Text()
    destination = fields.Char()
    signature = fields.Char()
    phase = fields.Selection(
        selection=[
            ("received", "Received"),
            ("validated", "Validated"),
            ("dispatched", "Dispatched"),
            ("synced", "Synced"),
            ("failed", "Failed"),
        ],
        required=True,
        default="received",
        index=True,
    )
    status = fields.Selection(
        selection=[
            ("pending", "Pending"),
            ("processing", "Processing"),
            ("done", "Done"),
            ("failed", "Failed"),
        ],
        required=True,
        default="pending",
        index=True,
    )
    received_at = fields.Datetime(required=True, default=fields.Datetime.now, index=True)
    last_received_at = fields.Datetime(index=True)
    validated_at = fields.Datetime()
    dispatched_at = fields.Datetime()
    processing_started_at = fields.Datetime()
    processed_at = fields.Datetime()
    synced_at = fields.Datetime()
    failed_at = fields.Datetime()
    processing_finished_at = fields.Datetime()
    retry_count = fields.Integer(default=0)
    error_message = fields.Text()
    note = fields.Text()

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

    def action_process(self):
        """Process selected webhook events immediately."""
        service = BigCommerceWebhookService(self.env)
        processed = 0
        failed = 0
        for event in self.filtered(lambda e: e.status in ("pending", "failed", "processing")):
            result = service.process_event(event)
            if result.get("success"):
                processed += 1
            else:
                failed += 1
        return self._notification_action(
            _("Webhook process completed. Processed: %(processed)s, Failed: %(failed)s.")
            % {"processed": processed, "failed": failed},
            notif_type="warning" if failed else "success",
        )

    def action_retry(self):
        """Reset failed events back to pending and clear processing errors."""
        failed_events = self.filtered(lambda event: event.status == "failed")
        for event in failed_events:
            event.write(
                {
                    "status": "pending",
                    "phase": "validated",
                    "processed_at": False,
                    "synced_at": False,
                    "failed_at": False,
                    "processing_finished_at": False,
                    "error_message": False,
                    "retry_count": event.retry_count + 1,
                }
            )
        return self._notification_action(_("Failed events were reset to pending for retry."), notif_type="success")

    @api.model
    def get_test_payload_examples(self):
        """Developer helper with sample webhook payloads for local testing."""
        return {
            "order_created": {
                "scope": "store/order/created",
                "data": {"id": 123},
                "store_id": "example-store-id",
                "created_at": 1711881000,
            },
            "order_updated": {
                "scope": "store/order/updated",
                "data": {"id": 123},
                "store_id": "example-store-id",
                "created_at": 1711881010,
            },
            "product_updated": {
                "scope": "store/product/updated",
                "data": {"id": 456},
                "store_id": "example-store-id",
                "created_at": 1711881020,
            },
        }

    def payload_dict(self):
        """Return payload json decoded into a dictionary when possible."""
        self.ensure_one()
        try:
            value = json.loads(self.payload_json or "{}")
            return value if isinstance(value, dict) else {}
        except (TypeError, ValueError):
            return {}
