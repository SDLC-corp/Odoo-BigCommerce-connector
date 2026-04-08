from odoo import models, fields, _
from odoo.exceptions import UserError
import logging

_logger = logging.getLogger(__name__)

SHOPIFY_PAYMENT_STATUS_SELECTION = [
    ("pending", "Pending"),
    ("authorized", "Authorized"),
    ("paid", "Paid"),
    ("partially_paid", "Partially Paid"),
    ("partially_refunded", "Partially Refunded"),
    ("refunded", "Refunded"),
    ("voided", "Voided"),
    ("unpaid", "Unpaid"),
    ("failed", "Failed"),
    ("cancelled", "Cancelled"),
]


class StockPicking(models.Model):
    _inherit = "stock.picking"

    # =====================================================
    # SHOPIFY FIELDS
    # =====================================================
    shopify_fulfillment_id = fields.Char("Shopify Fulfillment ID", copy=False)
    shopify_tracking_company = fields.Char("Shopify Tracking Company", copy=False)
    shopify_last_push_status = fields.Selection(
        [("success", "Success"), ("failed", "Failed")],
        string="Shopify Push Status",
        copy=False,
    )
    shopify_last_push_message = fields.Text("Shopify Push Message", copy=False)

    shopify_instance_id = fields.Many2one(
        "shopify.instance",
        string="Shopify Instance",
        copy=False,
        help="Used when delivery is not linked to a Shopify sale order.",
    )
    shopify_order_id = fields.Char(
        "Shopify Order ID",
        copy=False,
        help="Used when delivery is not linked to a Shopify sale order.",
    )

    shopify_tracking_status = fields.Char("Shopify Tracking Status", copy=False)
    shopify_tracking_last_location = fields.Char("Shopify Last Location", copy=False)
    shopify_tracking_last_update = fields.Datetime("Shopify Tracking Updated", copy=False)
    shopify_tracking_url = fields.Char("Shopify Tracking URL", copy=False)
    shopify_delivered = fields.Boolean("Shopify Delivered", copy=False)
    shopify_delivered_at = fields.Datetime("Shopify Delivered On", copy=False)
    shopify_payment_status = fields.Selection(
        related="sale_id.shopify_financial_status",
        store=True,
        readonly=True,
    )
    shopify_payment_method = fields.Char(
        string="Shopify Payment Method",
        related="sale_id.shopify_payment_method",
        store=True,
        readonly=True,
    )
    shopify_customer_email = fields.Char(
        string="Customer Email",
        related="partner_id.email",
        store=True,
        readonly=True,
    )
    shopify_customer_phone = fields.Char(
        string="Customer Phone",
        related="partner_id.phone",
        store=True,
        readonly=True,
    )

    def _update_shopify_payment_info(self, instance, order_external_id=None):
        """Fetch payment info from Shopify order and update linked sale/picking fields."""
        if not instance:
            return
        # Prefer the picking's Shopify order id, else the linked sale's.
        order_external_id = order_external_id or self.shopify_order_id or getattr(self.sale_id, "shopify_order_id", False)
        if not order_external_id:
            return
        try:
            data = instance._get(f"orders/{order_external_id}.json") or {}
            order_data = data.get("order") or data
        except Exception:
            _logger.exception("Failed to fetch Shopify order for payment info")
            return

        financial_status = (order_data.get("financial_status") or "").lower()
        gateways = order_data.get("payment_gateway_names") or []
        payment_method = ", ".join([g for g in gateways if g]) or False
        if not payment_method:
            for tx in order_data.get("transactions") or []:
                gateway = tx.get("gateway")
                if gateway:
                    payment_method = gateway
                    break

        if self.sale_id:
            self.sale_id.write({
                "shopify_financial_status": financial_status or False,
                "shopify_payment_method": payment_method,
            })
        else:
            # Keep values on picking when no sale link exists.
            self.write({
                "shopify_payment_status": financial_status or False,
                "shopify_payment_method": payment_method,
            })

    # =====================================================
    # PUSH TRACKING TO SHOPIFY
    # =====================================================
    def action_push_tracking_to_shopify(self):
        self.ensure_one()

        if self.picking_type_id.code != "outgoing":
            raise UserError(_("Only outgoing deliveries can be pushed to Shopify."))

        if self.state != "done":
            raise UserError(_("Please validate the delivery before pushing tracking to Shopify."))

        # Try to detect sale order
        sale = self.sale_id if "sale_id" in self._fields else False
        if not sale and "move_ids_without_package" in self._fields:
            moves = self.move_ids_without_package
            if moves and "sale_line_id" in moves._fields:
                sale = moves.mapped("sale_line_id.order_id")[:1]

        instance = sale.shopify_instance_id if sale else self.shopify_instance_id
        order_external_id = sale.shopify_order_id if sale else self.shopify_order_id

        if not instance or not order_external_id:
            raise UserError(_("This delivery is not linked to a Shopify order."))

        # Persist link for future refresh
        self.shopify_instance_id = instance.id
        self.shopify_order_id = order_external_id

        if "carrier_tracking_ref" not in self._fields:
            raise UserError(_("Tracking Reference field is unavailable. Install Delivery/Stock Delivery to enable carrier tracking."))

        tracking_ref = self.carrier_tracking_ref
        if not tracking_ref:
            # Fallback to the picking name so we can still push fulfillment without manual entry.
            tracking_ref = self.name or _("Delivery without tracking")
            self.carrier_tracking_ref = tracking_ref

        ShopifyAPI = self.env["shopify.api.fulfillment"]

        tracking_company = self.carrier_id.name if "carrier_id" in self._fields and self.carrier_id else ""
        tracking_urls = [tracking_ref] if tracking_ref else []

        # If a fulfillment already exists in Shopify, update its tracking instead of creating a new one.
        existing_fulfillment = False
        existing_fulfillment_id = self.shopify_fulfillment_id
        if existing_fulfillment_id:
            try:
                existing_fulfillment = ShopifyAPI.get_fulfillment(instance, existing_fulfillment_id)
            except Exception:
                existing_fulfillment = False

        if not existing_fulfillment:
            fulfillments = ShopifyAPI.get_fulfillments(instance, order_external_id) or []
            if fulfillments:
                existing_fulfillment = fulfillments[0]
                existing_fulfillment_id = existing_fulfillment.get("id")
                if existing_fulfillment_id:
                    self.shopify_fulfillment_id = existing_fulfillment_id

        # If order already fulfilled, just update tracking and stop.
        if existing_fulfillment:
            try:
                updated = ShopifyAPI.update_fulfillment_tracking(
                    instance,
                    existing_fulfillment.get("id"),
                    tracking_number=tracking_ref,
                    tracking_company=tracking_company,
                    tracking_urls=tracking_urls,
                )
                shipment_status = (
                    updated.get("shipment_status")
                    or (updated.get("tracking_info") or {}).get("status")
                    or ""
                )
                self.shopify_tracking_status = shipment_status or updated.get("status") or ""
                self.shopify_delivered = (shipment_status or "").lower() == "delivered"
                self.shopify_tracking_last_update = fields.Datetime.now()
                if self.shopify_delivered:
                    self.shopify_delivered_at = fields.Datetime.now()
                self.shopify_last_push_status = "success"
                self.shopify_last_push_message = _("Tracking updated successfully.")
                return {
                    "type": "ir.actions.client",
                    "tag": "display_notification",
                    "params": {
                        "title": _("Shopify"),
                        "message": _("Tracking updated successfully. ID: %s") % (self.shopify_fulfillment_id or ""),
                        "type": "success",
                        "sticky": False,
                    },
                }
            except Exception as e:
                _logger.exception("Failed to update Shopify fulfillment tracking")
                self.shopify_last_push_status = "failed"
                self.shopify_last_push_message = str(e)
                raise UserError(_("Shopify tracking push failed: %s") % e)

        fulfillment_orders = ShopifyAPI.get_fulfillment_orders(instance, order_external_id)
        if not fulfillment_orders:
            self.shopify_last_push_status = "failed"
            self.shopify_last_push_message = _("No fulfillment orders found in Shopify.")
            raise UserError(_("No fulfillment orders found in Shopify."))

        def _fo_has_fulfillable_lines(fo_record):
            """Return True when the fulfillment order still has quantity to fulfill."""
            lines = []
            if fo_record.get("line_items"):
                lines.extend(fo_record.get("line_items"))
            if fo_record.get("fulfillment_order_line_items"):
                lines.extend(fo_record.get("fulfillment_order_line_items"))
            if fo_record.get("line_items_by_fulfillment_order"):
                for entry in fo_record.get("line_items_by_fulfillment_order"):
                    lines.extend(entry.get("fulfillment_order_line_items") or [])

            for li in lines:
                qty = next(
                    (
                        q
                        for q in (
                            li.get("fulfillable_quantity"),
                            li.get("remaining_fulfillable_quantity"),
                            li.get("quantity"),
                        )
                        if q is not None
                    ),
                    0,
                )
                if qty > 0:
                    return True
            return False

        fo = next((fo for fo in fulfillment_orders if _fo_has_fulfillable_lines(fo)), None)
        if not fo:
            # Fallback to the first fulfillment order even if Shopify marks quantities as 0,
            # so we can at least attempt a fulfillment (Shopify will return a precise error otherwise).
            fo = fulfillment_orders[0]
        fo_id = fo.get("id")
        if not fo_id:
            raise UserError(_("Invalid fulfillment order received from Shopify."))

        try:
            fulfillment = ShopifyAPI.create_fulfillment_with_tracking(
                instance,
                fo_id,
                tracking_number=tracking_ref,
                tracking_company=tracking_company,
                tracking_urls=tracking_urls,
                order_external_id=order_external_id,
            )
            self.shopify_fulfillment_id = fulfillment.get("id")
            shipment_status = (
                fulfillment.get("shipment_status")
                or (fulfillment.get("tracking_info") or {}).get("status")
                or ""
            )
            self.shopify_tracking_status = shipment_status or fulfillment.get("status") or ""
            self.shopify_delivered = (shipment_status or "").lower() == "delivered"
            self.shopify_tracking_last_update = fields.Datetime.now()
            if self.shopify_delivered:
                self.shopify_delivered_at = fields.Datetime.now()
            self.shopify_last_push_status = "success"
            self.shopify_last_push_message = _("Tracking pushed successfully.")
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("Shopify"),
                    "message": _("Tracking pushed successfully. ID: %s") % (self.shopify_fulfillment_id or ""),
                    "type": "success",
                    "sticky": False,
                },
            }
        except Exception as e:
            _logger.exception("Failed to push tracking to Shopify")
            self.shopify_last_push_status = "failed"
            self.shopify_last_push_message = str(e)
            raise UserError(_("Shopify tracking push failed: %s") % e)

    # =====================================================
    # REFRESH TRACKING FROM SHOPIFY
    # =====================================================
    def action_refresh_shopify_tracking(self):
        results = []
        for picking in self:
            result = picking._action_refresh_shopify_tracking_one()
            if result:
                results.append(result)
        # Cron/server actions may pass multiple pickings; return a single client action only when relevant.
        return results[0] if len(results) == 1 else None

    def _action_refresh_shopify_tracking_one(self):
        self.ensure_one()

        instance = self.shopify_instance_id
        order_external_id = self.shopify_order_id
        if not instance:
            raise UserError(_("No Shopify instance set for this delivery."))

        # Keep payment info fresh while refreshing tracking.
        self._update_shopify_payment_info(instance, order_external_id)

        ShopifyAPI = self.env["shopify.api.fulfillment"]

        fulfillment_id = self.shopify_fulfillment_id
        if not fulfillment_id and order_external_id:
            # Try to recover a fulfillment created directly in Shopify
            fulfillments = ShopifyAPI.get_fulfillments(instance, order_external_id)
            if fulfillments:
                fulfillment_id = fulfillments[0].get("id")
                if fulfillment_id:
                    self.shopify_fulfillment_id = fulfillment_id

        if not fulfillment_id:
            raise UserError(_("No Shopify fulfillment linked to this delivery. Push tracking to Shopify first."))

        def _fetch_fulfillment(fid):
            return ShopifyAPI.get_fulfillment(instance, fid)

        try:
            fulfillment = _fetch_fulfillment(fulfillment_id)
        except Exception as e:
            # Auto-recover when the fulfillment was deleted/rotated in Shopify
            if "fulfillment not found" in str(e).lower() and order_external_id:
                fulfillments = ShopifyAPI.get_fulfillments(instance, order_external_id)
                new_id = fulfillments and fulfillments[0].get("id")
                if new_id:
                    self.shopify_fulfillment_id = new_id
                    try:
                        fulfillment = _fetch_fulfillment(new_id)
                    except Exception:
                        fulfillment = False
                else:
                    fulfillment = False
                if not fulfillment:
                    return {
                        "type": "ir.actions.client",
                        "tag": "display_notification",
                        "params": {
                            "title": _("Shopify"),
                            "message": _("Shopify fulfillment not found. Please push tracking again."),
                            "type": "warning",
                            "sticky": False,
                        },
                    }
            else:
                _logger.exception("Failed to fetch Shopify fulfillment")
                raise UserError(_("Failed to fetch Shopify tracking: %s") % e)

        tracking_info = fulfillment.get("tracking_info") or {}
        shipment_status = fulfillment.get("shipment_status") or tracking_info.get("status") or ""

        self.shopify_tracking_status = shipment_status or fulfillment.get("status") or ""
        self.shopify_tracking_last_location = tracking_info.get("company") or ""
        self.shopify_tracking_last_update = fields.Datetime.now()
        self.shopify_tracking_url = tracking_info.get("url") or ""
        self.shopify_delivered = (shipment_status or "").lower() == "delivered"
        if self.shopify_delivered:
            self.shopify_delivered_at = fields.Datetime.now()
