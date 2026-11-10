# -*- coding: utf-8 -*-

import json

from odoo import _, api, fields, models
from odoo.exceptions import AccessError

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
    company_id = fields.Many2one(
        "res.company",
        related="instance_id.company_id",
        store=True,
        readonly=True,
        index=True,
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
            ("duplicate", "Duplicate"),
            ("rejected", "Rejected"),
        ],
        required=True,
        default="pending",
        index=True,
        help="Rejected: failed authentication or arrived for a disabled connector; it is kept for "
        "diagnostics only and is never processed. Duplicate: BigCommerce re-delivered an event that "
        "was already received; it is not processed again.",
    )
    dedup_key = fields.Char(
        index=True,
        copy=False,
        help="BigCommerce delivery identity (payload hash + creation time). A second delivery with "
        "the same key is recorded as a duplicate and not processed again.",
    )
    next_retry_at = fields.Datetime(
        index=True,
        copy=False,
        help="A failed event is retried automatically from this moment (exponential back-off), "
        "until the connector's maximum number of retries is reached.",
    )
    remote_created_at = fields.Datetime(
        string="Created in BigCommerce",
        help="Event creation time stated by BigCommerce (created_at).",
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

    _bigcommerce_webhook_event_dedup_uniq = models.UniqueIndex(
        "(instance_id, dedup_key) WHERE dedup_key IS NOT NULL AND status NOT IN ('duplicate', 'rejected')",
    )

    def _check_manager(self):
        if not self.env.su and not self.env.user.has_group("bigcommerce_connector_sdlc.group_bigcommerce_manager"):
            raise AccessError(_("Only BigCommerce managers can process or retry webhook events."))
        self.check_access("write")

    def action_process(self):
        """Process selected webhook events immediately (never rejected or duplicate ones)."""
        self._check_manager()
        self = self.sudo()
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
        """Reset failed events back to pending; the retry counter starts again from zero."""
        self._check_manager()
        failed_events = self.sudo().filtered(lambda event: event.status == "failed")
        failed_events.write(
            {
                "status": "pending",
                "phase": "validated",
                "processed_at": False,
                "synced_at": False,
                "failed_at": False,
                "processing_finished_at": False,
                "error_message": False,
                "next_retry_at": False,
                "retry_count": 0,
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
