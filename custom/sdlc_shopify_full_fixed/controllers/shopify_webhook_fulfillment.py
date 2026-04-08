import base64
import hmac
import json
import logging
from hashlib import sha256

from odoo import http
from odoo.http import request

_logger = logging.getLogger(__name__)


class ShopifyWebhookFulfillment(http.Controller):
    @http.route(
        "/shopify/webhook/fulfillment",
        type="http",
        auth="public",
        methods=["POST"],
        csrf=False,
    )
    def shopify_fulfillment(self, **kwargs):
        raw_body = request.httprequest.get_data() or b""
        headers = request.httprequest.headers
        hmac_header = headers.get("X-Shopify-Hmac-Sha256")
        shop_domain = headers.get("X-Shopify-Shop-Domain")

        Instance = request.env["shopify.instance"].sudo()
        instance = False
        if shop_domain:
            instance = Instance.search(
                [("shop_url", "ilike", shop_domain)], limit=1
            )
        if not instance:
            instance = Instance.search([("active", "=", True)], limit=1)

        # HMAC verification
        try:
            secret = (instance.webhook_secret or "").encode("utf-8")
            digest = base64.b64encode(hmac.new(secret, raw_body, sha256).digest())
            if not hmac.compare_digest(digest, (hmac_header or "").encode("utf-8")):
                _logger.warning("Shopify fulfillment webhook HMAC mismatch")
        except Exception as e:
            _logger.warning("Shopify fulfillment HMAC verify error: %s", e)

        # parse payload
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            payload = {}

        order_id = payload.get("order_id")
        tracking_number = payload.get("tracking_number")
        tracking_company = payload.get("tracking_company") or payload.get("company")
        tracking_urls = payload.get("tracking_urls") or []
        fulfillment_id = payload.get("id")

        if not order_id:
            return request.make_response("ok")

        SaleOrder = request.env["sale.order"].sudo()
        Picking = request.env["stock.picking"].sudo()
        CarrierMap = request.env["shopify.carrier.map"].sudo()

        order = SaleOrder.search(
            [("shopify_order_id", "=", str(order_id))],
            limit=1,
        )
        if not order:
            return request.make_response("ok")

        # pick latest outgoing (prefer not cancelled)
        pickings = order.picking_ids.filtered(lambda p: p.picking_type_code == "outgoing")
        picking = pickings.filtered(lambda p: p.state != "cancel")[:1] or pickings[:1]
        picking = picking[:1]
        if not picking:
            return request.make_response("ok")

        vals = {}
        if tracking_number:
            vals["carrier_tracking_ref"] = tracking_number
        if tracking_company:
            vals["shopify_tracking_company"] = tracking_company
            # map carrier
            if instance:
                m = CarrierMap.search(
                    [
                        ("instance_id", "=", instance.id),
                        ("shopify_carrier", "=", tracking_company),
                    ],
                    limit=1,
                )
                if m:
                    vals["carrier_id"] = m.odoo_carrier_id.id
        if fulfillment_id:
            vals["shopify_fulfillment_id"] = str(fulfillment_id)

        if vals:
            try:
                picking.write(vals)
            except Exception as e:
                _logger.warning("Failed to update picking tracking: %s", e)

        return request.make_response("ok")
