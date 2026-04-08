from odoo import models, fields, api
from odoo.exceptions import UserError
import requests
import logging

_logger = logging.getLogger(__name__)


class ShopifyGiftCard(models.Model):
    _name = "shopify.gift.card"
    _description = "Shopify Gift Card"

    # ==================================================
    # BASIC FIELDS
    # ==================================================
    name = fields.Char(default="Gift Card")
    code = fields.Char("Gift Card Code", readonly=True)
    value = fields.Float("Initial Value", readonly=True)
    balance = fields.Float("Remaining Balance")
    currency = fields.Char(default="INR")

    shopify_gift_card_id = fields.Char(
        "Shopify Gift Card ID",
        readonly=True,
        index=True
    )

    state = fields.Char("Status")

    active = fields.Boolean(default=True)
    expiry_date = fields.Date()

    instance_id = fields.Many2one(
        "shopify.instance",
        required=True,
        ondelete="cascade"
    )

    # ==================================================
    # INTERNAL REPORT HELPER (SAFE)
    # ==================================================
    def _create_gift_card_sync_report(
        self, instance, status, source_action, message=False
    ):
        Report = self.env["shopify.sync.report"].sudo()
        Line = self.env["shopify.sync.report.line"].sudo()

        report = Report.create({
            "instance_id": instance.id,
            "sync_type": "gift_card",
            "total_records": 1,
            "success_count": 1 if status == "success" else 0,
            "error_count": 1 if status == "error" else 0,
        })

        Line.create({
            "report_id": report.id,
            "record_type": "gift_card",
            "shopify_id": self.shopify_gift_card_id or "",
            "name": self.code or self.name,
            "status": status,
            "error_message": message if status == "error" else False,
        })

    # ==================================================
    # ❌ CREATE GIFT CARD (BLOCKED – SHOPIFY POLICY)
    # ==================================================
    def action_push_to_shopify(self):
        """
        Shopify DOES NOT allow gift card creation via API.
        This method is intentionally blocked to avoid silent failures.
        """
        self.ensure_one()

        raise UserError(
            "❌ Shopify API does NOT allow creating gift cards via custom apps.\n\n"
            "Please create gift cards directly in Shopify Admin.\n"
            "You can sync, update balance, or enable/disable from Odoo."
        )

    # ==================================================
    # ✅ UPDATE GIFT CARD (BALANCE + ACTIVE)
    # ==================================================
    def action_update_on_shopify(self):
        self.ensure_one()

        if not self.shopify_gift_card_id:
            raise UserError("Gift card not linked with Shopify.")

        instance = self.instance_id

        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

        # --------------------------------------------------
        # 1) FETCH CURRENT SHOPIFY CARD
        # --------------------------------------------------
        url_get = (
            f"{instance.shop_url.rstrip('/')}/admin/api/"
            f"{instance.api_version}/gift_cards/{self.shopify_gift_card_id}.json"
        )

        res_get = requests.get(url_get, headers=headers, timeout=30)
        if res_get.status_code != 200:
            self._create_gift_card_sync_report(
                instance,
                "error",
                "gift_card_update",
                res_get.text
            )
            raise UserError(res_get.text)

        shopify_gc = res_get.json().get("gift_card", {})
        shopify_balance = float(shopify_gc.get("balance") or 0.0)

        # --------------------------------------------------
        # 2) BALANCE ADJUSTMENT (ONLY VALID WAY)
        # --------------------------------------------------
        if self.balance is not None:
            diff = self.balance - shopify_balance
        else:
            diff = 0

        if diff != 0:
            url_adj = (
                f"{instance.shop_url.rstrip('/')}/admin/api/"
                f"{instance.api_version}/gift_cards/"
                f"{self.shopify_gift_card_id}/adjustments.json"
            )

            payload_adj = {
                "adjustment": {
                    "amount": diff,
                    "note": "Adjusted from Odoo"
                }
            }

            res_adj = requests.post(
                url_adj, headers=headers, json=payload_adj, timeout=30
            )

            if res_adj.status_code not in (200, 201):
                self._create_gift_card_sync_report(
                    instance,
                    "error",
                    "gift_card_update",
                    res_adj.text
                )
                raise UserError(res_adj.text)

        # --------------------------------------------------
        # 3) ENABLE / DISABLE CARD
        # --------------------------------------------------
        url_put = (
            f"{instance.shop_url.rstrip('/')}/admin/api/"
            f"{instance.api_version}/gift_cards/{self.shopify_gift_card_id}.json"
        )

        payload_put = {
            "gift_card": {
                "disabled": not self.active
            }
        }

        res_put = requests.put(
            url_put, headers=headers, json=payload_put, timeout=30
        )

        if res_put.status_code not in (200, 201):
            self._create_gift_card_sync_report(
                instance,
                "error",
                "gift_card_update",
                res_put.text
            )
            raise UserError(res_put.text)

        self._create_gift_card_sync_report(
            instance,
            "success",
            "gift_card_update"
        )

        return True

    # ==================================================
    # ✅ SYNC GIFT CARDS (SHOPIFY → ODOO)
    # ==================================================
    @api.model
    def sync_gift_cards_from_shopify(self, instance):
        """
        SAFE Shopify → Odoo sync
        """
        GiftCard = self.sudo()

        data = instance._get(
            "gift_cards.json", {"limit": 250}
        ).get("gift_cards", [])

        for g in data:
            vals = {
                "instance_id": instance.id,
                "shopify_gift_card_id": str(g.get("id")),
                "balance": float(g.get("balance") or 0.0),
                "currency": g.get("currency"),
                "active": not g.get("disabled", False),
                "expiry_date": g.get("expires_on"),
            }

            card = GiftCard.search([
                ("shopify_gift_card_id", "=", vals["shopify_gift_card_id"]),
                ("instance_id", "=", instance.id),
            ], limit=1)

            if card:
                card.write(vals)
            else:
                GiftCard.create(vals)

        _logger.info(
            "Gift card sync completed for instance %s", instance.name
        )
        return True
