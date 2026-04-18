# -*- coding: utf-8 -*-

from odoo import api, fields, models


class BigCommerceOrderBinding(models.Model):
    """Maps Odoo sales orders to BigCommerce order records."""

    _name = "bigcommerce.order.binding"
    _description = "BigCommerce Order Binding"
    _rec_name = "bigcommerce_order_number"
    _order = "id desc"

    instance_id = fields.Many2one(
        "bigcommerce.connector",
        required=True,
        ondelete="cascade",
        index=True,
    )
    sale_order_id = fields.Many2one("sale.order", index=True, ondelete="set null")
    bigcommerce_order_id = fields.Char(index=True)
    bigcommerce_order_number = fields.Char(index=True)
    status_on_bigcommerce = fields.Char(index=True)
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
    imported_at = fields.Datetime(index=True)
    exported_at = fields.Datetime(index=True)
    last_error = fields.Text()
    customer_name = fields.Char(
        related="sale_order_id.partner_id.name",
        store=True,
        readonly=True,
        index=True,
    )
    customer_email = fields.Char(
        related="sale_order_id.partner_id.email",
        store=True,
        readonly=True,
    )
    bigcommerce_total_amount = fields.Float(string="BigCommerce Total")
    bigcommerce_currency_code = fields.Char(string="BigCommerce Currency")
    bigcommerce_total_display = fields.Char(
        string="Total",
        compute="_compute_bigcommerce_total_display",
        store=True,
    )
    sale_order_date = fields.Datetime(
        related="sale_order_id.date_order",
        store=True,
        readonly=True,
        index=True,
    )
    currency_id = fields.Many2one(
        "res.currency",
        related="sale_order_id.currency_id",
        store=True,
        readonly=True,
    )
    sale_order_amount_total = fields.Monetary(
        related="sale_order_id.amount_total",
        currency_field="currency_id",
        store=True,
        readonly=True,
    )
    sale_order_state = fields.Selection(
        related="sale_order_id.state",
        store=True,
        readonly=True,
        index=True,
    )

    @api.depends("bigcommerce_total_amount", "bigcommerce_currency_code")
    def _compute_bigcommerce_total_display(self):
        symbol_fallback = {
            "USD": "$",
            "INR": "₹",
            "EUR": "€",
            "GBP": "£",
            "JPY": "¥",
        }
        currency_codes = sorted(
            {
                (rec.bigcommerce_currency_code or "").strip().upper()
                for rec in self
                if (rec.bigcommerce_currency_code or "").strip()
            }
        )
        currency_symbol_map = {}
        if currency_codes:
            currencies = self.env["res.currency"].sudo().search([("name", "in", currency_codes)])
            currency_symbol_map = {((cur.name or "").upper()): (cur.symbol or "") for cur in currencies}

        for rec in self:
            amount = rec.bigcommerce_total_amount or 0.0
            code = (rec.bigcommerce_currency_code or "").strip().upper()
            if code:
                symbol = currency_symbol_map.get(code) or symbol_fallback.get(code)
                if symbol:
                    rec.bigcommerce_total_display = "%s%.2f" % (symbol, amount)
                else:
                    rec.bigcommerce_total_display = "%.2f %s" % (amount, code)
            else:
                rec.bigcommerce_total_display = "%.2f" % amount

    _sql_constraints = [
        (
            "bigcommerce_order_binding_order_uniq",
            "unique(instance_id, bigcommerce_order_id)",
            "BigCommerce order id must be unique per instance.",
        ),
        (
            "bigcommerce_order_binding_sale_order_uniq",
            "unique(instance_id, sale_order_id)",
            "Only one BigCommerce order binding is allowed per sale order and instance.",
        )
    ]
