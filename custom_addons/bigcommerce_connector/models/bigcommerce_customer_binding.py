# -*- coding: utf-8 -*-

from odoo import fields, models


class BigCommerceCustomerBinding(models.Model):
    """Maps Odoo partners to BigCommerce customer records."""

    _name = "bigcommerce.customer.binding"
    _description = "BigCommerce Customer Binding"
    _rec_name = "email"
    _order = "id desc"

    instance_id = fields.Many2one(
        "bigcommerce.connector",
        required=True,
        ondelete="cascade",
        index=True,
    )
    partner_id = fields.Many2one("res.partner", index=True, ondelete="set null")
    bigcommerce_customer_id = fields.Char(index=True)
    email = fields.Char(index=True)
    sync_state = fields.Selection(
        selection=[
            ("draft", "Draft"),
            ("synced", "Synced"),
            ("error", "Error"),
        ],
        default="draft",
        required=True,
        index=True,
    )
    last_synced_at = fields.Datetime(index=True)
    last_error = fields.Text()

    _sql_constraints = [
        (
            "bigcommerce_customer_binding_customer_uniq",
            "unique(instance_id, bigcommerce_customer_id)",
            "BigCommerce customer id must be unique per instance.",
        )
    ]
