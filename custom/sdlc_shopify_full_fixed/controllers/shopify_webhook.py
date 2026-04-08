from odoo import http
from odoo.http import request
import logging

_logger = logging.getLogger(__name__)


class ShopifyWebhook(http.Controller):

    @http.route(
        "/shopify/webhook",
        type="http",
        auth="public",
        methods=["POST"],
        csrf=False,
    )
    def shopify_webhook(self):
        payload = request.httprequest.get_json(silent=True) or {}

        topic = request.httprequest.headers.get("X-Shopify-Topic")
        shop_domain = request.httprequest.headers.get("X-Shopify-Shop-Domain")

        _logger.info("Shopify webhook hit | topic=%s | shop=%s", topic, shop_domain)

        Instance = request.env["shopify.instance"].sudo()
        # Prefer an instance matching the incoming shop domain; fallback to any active.
        domain_match = [("active", "=", True)]
        if shop_domain:
            domain_match = ["|", ("shop_url", "ilike", shop_domain), ("name", "ilike", shop_domain)]
        instance = Instance.search(domain_match, limit=1)

        if not instance:
            return {"status": "no_instance"}

        Sync = request.env["shopify.webhook.sync"].sudo()

        # ================= PRODUCT =================
        if topic == "products/create":
            if instance.webhook_product_create:
                Sync.sync_product(
                    payload,
                    instance,
                    source_action="product_create",
                )

        elif topic in ("products/update", "products/updated"):
            if instance.webhook_product_update:
                Sync.sync_product(
                    payload,
                    instance,
                    source_action="product_update",
                )

        # ================= CUSTOMER =================
        elif topic == "customers/create":
            if instance.webhook_customer_create:
                Sync.sync_customer(
                    payload,
                    instance,
                    source_action="customer_create",
                )

        elif topic in ("customers/update", "customers/updated"):
            if instance.webhook_customer_update:
                Sync.sync_customer(
                    payload,
                    instance,
                    source_action="customer_update",
                )

        # ================= ORDER =================
        elif topic == "orders/create":
            if instance.webhook_order_create:
                Sync.sync_order(
                    payload,
                    instance,
                    source_action="order_create",
                )

        elif topic in ("orders/update", "orders/updated"):
            if instance.webhook_order_update:
                Sync.sync_order(
                    payload,
                    instance,
                    source_action="order_update",
                )

        # ================= CATEGORY =================
        elif topic == "collections/create":
            if instance.webhook_category_create:
                Sync.sync_category(
                    payload,
                    instance,
                    source_action="category_create",
                )

        elif topic in ("collections/update", "collections/updated"):
            if instance.webhook_category_update:
                Sync.sync_category(
                    payload,
                    instance,
                    source_action="category_update",
                )

        # ================= GIFT CARD =================
        elif topic == "gift_cards/create":
            if instance.webhook_giftcard_create:
                Sync.sync_giftcard(
                    payload,
                    instance,
                    source_action="gift_card_create",
                )

        elif topic in ("gift_cards/update", "gift_cards/updated"):
            if instance.webhook_giftcard_update:
                Sync.sync_giftcard(
                    payload,
                    instance,
                    source_action="gift_card_update",
                )

        else:
            _logger.info("Unhandled Shopify webhook topic: %s", topic)

        return request.make_response('{"status":"ok"}', headers=[("Content-Type", "application/json")])
