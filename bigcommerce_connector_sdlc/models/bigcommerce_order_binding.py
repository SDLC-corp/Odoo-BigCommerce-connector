# -*- coding: utf-8 -*-

from odoo import _, api, fields, models
from odoo.exceptions import AccessError


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
    company_id = fields.Many2one(
        "res.company",
        related="instance_id.company_id",
        store=True,
        readonly=True,
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
        string="Odoo Total",
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

    has_placeholder_lines = fields.Boolean(
        string="Unmatched Products",
        readonly=True,
        copy=False,
        help="At least one order line could not be matched to an Odoo product and uses a "
        "'[BigCommerce]' placeholder service product.",
    )
    amount_mismatch = fields.Boolean(
        string="Total Mismatch",
        compute="_compute_exception_flags",
        store=True,
        index=True,
        help="The Odoo order total differs from the BigCommerce total by more than the connector "
        "tolerance (usually a tax or price configuration difference).",
    )
    currency_mismatch = fields.Boolean(
        compute="_compute_exception_flags",
        store=True,
        help="BigCommerce charged in a different currency than the Odoo sale order uses.",
    )
    needs_attention = fields.Boolean(
        compute="_compute_exception_flags",
        store=True,
        index=True,
    )
    exception_reason = fields.Char(compute="_compute_exception_flags", store=True)

    @api.depends(
        "sync_state",
        "sale_order_id",
        "sale_order_amount_total",
        "currency_id",
        "bigcommerce_total_amount",
        "bigcommerce_currency_code",
        "has_placeholder_lines",
        "instance_id.order_total_tolerance",
    )
    def _compute_exception_flags(self):
        for rec in self:
            reasons = []
            currency_mismatch = bool(
                rec.sale_order_id
                and rec.bigcommerce_currency_code
                and rec.currency_id
                and rec.currency_id.name != (rec.bigcommerce_currency_code or "").upper()
            )
            amount_mismatch = False
            if rec.sale_order_id and not currency_mismatch and rec.sale_order_state != "cancel":
                tolerance = abs(rec.instance_id.order_total_tolerance or 0.0)
                amount_mismatch = abs((rec.sale_order_amount_total or 0.0) - (rec.bigcommerce_total_amount or 0.0)) > (
                    tolerance + 1e-9
                )
            if rec.sync_state == "error":
                reasons.append(_("Sync error"))
            if rec.bigcommerce_order_id and not rec.sale_order_id:
                reasons.append(_("No sale order"))
            if currency_mismatch:
                reasons.append(_("Currency differs"))
            if amount_mismatch:
                reasons.append(_("Total differs"))
            if rec.has_placeholder_lines:
                reasons.append(_("Unmatched products"))
            rec.currency_mismatch = currency_mismatch
            rec.amount_mismatch = amount_mismatch
            rec.needs_attention = bool(reasons)
            rec.exception_reason = ", ".join(reasons) or False

    def action_resync_from_bigcommerce(self):
        """Re-read the selected orders from BigCommerce now (managers)."""
        if not self.env.user.has_group("bigcommerce_connector_sdlc.group_bigcommerce_manager"):
            raise AccessError(_("Only BigCommerce managers can re-sync orders."))
        self.check_access("write")
        from ..services.order_sync_service import BigCommerceOrderSyncService

        ok = failed = 0
        messages = []
        for binding in self.sudo():
            instance = binding.instance_id._bc_service_instance()
            try:
                with self.env.cr.savepoint():
                    result = BigCommerceOrderSyncService(instance).import_order_by_id(binding.bigcommerce_order_id)
            except Exception as err:  # noqa: BLE001 - reported to the user, transaction kept usable
                result = {"success": False, "message": str(err)}
            if result.get("success"):
                ok += 1
            else:
                failed += 1
                binding.write({"sync_state": "error", "last_error": result.get("message")})
                messages.append("#%s: %s" % (binding.bigcommerce_order_number or binding.bigcommerce_order_id,
                                             result.get("message")))
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("BigCommerce"),
                "message": _("Re-sync finished. Updated: %(ok)s, Failed: %(failed)s. %(details)s")
                % {"ok": ok, "failed": failed, "details": " | ".join(messages[:3])},
                "type": "warning" if failed else "success",
                "sticky": bool(failed),
            },
        }

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

    _bigcommerce_order_binding_order_uniq = models.Constraint(
        "unique(instance_id, bigcommerce_order_id)",
        "BigCommerce order id must be unique per instance.",
    )
    _bigcommerce_order_binding_sale_order_uniq = models.Constraint(
        "unique(instance_id, sale_order_id)",
        "Only one BigCommerce order binding is allowed per sale order and instance.",
    )
