# -*- coding: utf-8 -*-

from odoo import fields, models


class BigCommerceWebhookSubscription(models.Model):
    """Tracks remote BigCommerce webhook subscriptions per connector instance."""

    _name = "bigcommerce.webhook.subscription"
    _description = "BigCommerce Webhook Subscription"
    _order = "id desc"

    instance_id = fields.Many2one(
        "bigcommerce.connector",
        required=True,
        ondelete="cascade",
        index=True,
    )
    remote_webhook_id = fields.Char(index=True)
    scope = fields.Char(required=True, index=True)
    destination = fields.Char(required=True)
    is_active = fields.Boolean(default=True)
    last_sync_at = fields.Datetime()
    status = fields.Selection(
        selection=[
            ("active", "Active"),
            ("missing", "Missing"),
            ("deleted", "Deleted"),
            ("error", "Error"),
        ],
        default="active",
        required=True,
        index=True,
    )
    error_message = fields.Text()

    _sql_constraints = [
        (
            "bigcommerce_webhook_subscription_remote_uniq",
            "unique(instance_id, remote_webhook_id)",
            "Webhook remote id must be unique per connector instance.",
        ),
        (
            "bigcommerce_webhook_subscription_scope_dest_uniq",
            "unique(instance_id, scope, destination)",
            "Webhook scope/destination must be unique per connector instance.",
        ),
    ]
