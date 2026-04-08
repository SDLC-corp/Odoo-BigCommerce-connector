from odoo import models, fields


class SaleOrderLine(models.Model):
    _inherit = "sale.order.line"

    # ✅ GIFT CARD DISPLAY FIELDS
    is_gift_card = fields.Boolean(
        string="Is Gift Card",
        default=False,
        readonly=True
    )

    gift_card_code = fields.Char(
        string="Gift Card Code",
        readonly=True
    )


    # -------------------------------------------------
    # ✅ SAFE DELETE (ODOO 18 FIX) – UNTOUCHED LOGIC
    # -------------------------------------------------
    def unlink(self):
        """Bypass restriction & allow deleting lines from confirmed orders."""
        for line in self:
            order = line.order_id

            # Odoo 18: Unlock order before modifying
            if order.state in ("sale", "sent", "done"):
                order.action_unlock()
                order.write({"state": "draft"})

            # Delete the line
            res = super(SaleOrderLine, line).unlink()

            # Re-confirm automatically
            if order.state == "draft":
                order.action_confirm()

        return True
