from odoo import models, fields, api, _
from odoo.exceptions import UserError
import requests
import logging
import json
from datetime import datetime
from collections import defaultdict

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


class SaleOrder(models.Model):
    _inherit = "sale.order"

    shopify_order_id = fields.Char("Shopify Order ID", readonly=True)
    shopify_instance_id = fields.Many2one(
        "shopify.instance",
        string="Shopify Instance",
        readonly=False,
        copy=False,
    )
    customizable_pdf_form_fields = fields.Json(
        string="Custom PDF Fields",
        copy=False,
        default=list,
    )
    # Fallback for environments missing sale_pdf_quote_builder: keep the field so inherited views load.
    quotation_document_ids = fields.Many2many(
        "quotation.document",
        string="Headers/Footers",
        readonly=False,
        check_company=True,
    )

    is_pdf_quote_builder_available = fields.Boolean(
        string="PDF Quote Builder Available",
        compute="_compute_pdf_quote_available",
        store=False,
    )

    def _compute_pdf_quote_available(self):
        for order in self:
            order.is_pdf_quote_builder_available = False

    gift_card_applied = fields.Boolean(
        string="Gift Card Applied",
        default=False,
        copy=False,
        help="Prevents double gift card deduction on Shopify re-sync"
    )

    gift_card_refund_processed = fields.Boolean(
        string="Gift Card Refund Processed",
        default=False,
        copy=False,
        help="Prevents double gift card restore on refund webhook"
    )

    shopify_financial_status = fields.Selection(
        SHOPIFY_PAYMENT_STATUS_SELECTION,
        string="Shopify Payment Status",
        copy=False,
    )
    shopify_payment_method = fields.Char(
        string="Shopify Payment Method",
        copy=False,
        help="Payment gateway/method reported by Shopify",
    )
    shopify_refund_status = fields.Selection(
        [
            ("pending", "Pending"),
            ("success", "Success"),
            ("failed", "Failed"),
            ("partial", "Partial"),
        ],
        string="Shopify Refund Status",
        copy=False,
    )
    shopify_refund_message = fields.Text(
        string="Shopify Refund Message",
        copy=False,
    )
    shopify_odoo_refund_message = fields.Text(
        string="Odoo Refund Message",
        copy=False,
    )
    shopify_tracking_ref = fields.Char(
        string="Shopify Tracking",
        compute="_compute_shopify_tracking_ref",
        store=True,
        copy=False,
        help="Latest tracking number from Shopify fulfillments.",
    )
    shopify_cancel_reason = fields.Char(
        string="Shopify Cancel Reason",
        copy=False,
        help="Reason provided when the order was cancelled (for reporting).",
    )
    order_sync_status_odoo_to_shopify = fields.Selection(
        [
            ("pending", "Pending"),
            ("refund_requested", "Refund Requested"),
            ("refunded", "Refunded"),
            ("cancelled", "Cancelled"),
        ],
        string="Odoo → Shopify Status",
        compute="_compute_order_sync_status",
        store=True,
    )
    order_sync_status_shopify_to_odoo = fields.Selection(
        [
            ("pending", "Pending"),
            ("refund_requested", "Refund Requested"),
            ("refunded", "Refunded"),
            ("cancelled", "Cancelled"),
        ],
        string="Shopify → Odoo Status",
        compute="_compute_order_sync_status",
        store=True,
    )

    # ---------------------------------------------------
  
    # ---------------------------------------------------
    def safe_delete_line(self, line):
        self.ensure_one()

        if self.state in ["sale", "sent", "done"]:
            self.action_unlock()

        line.unlink()

        if self.state == "draft":
            self.action_confirm()

    # ---------------------------------------------------
    # BUILD SHOPIFY PAYLOAD
    # ---------------------------------------------------
    def _prepare_shopify_order_payload(self):
        self.ensure_one()

        payload = {
            "line_items": [],
            "customer": {},
        }

        partner = self.partner_id
        if partner:
            payload["customer"] = {
                "email": partner.email or "guest@example.com",
                "first_name": (partner.name or "Customer").split(" ")[0],
                "last_name": " ".join((partner.name or "").split(" ")[1:]) or "Shopify",
            }

        for line in self.order_line:
            product = line.product_id
            item = {
                "quantity": int(line.product_uom_qty),
                "title": product.name,
                "price": str(line.price_unit),
            }

            # If a variant id is known, send it, but keep price/title to satisfy Shopify
            if product.shopify_product_id and str(product.shopify_product_id).isdigit():
                item["variant_id"] = int(product.shopify_product_id)

            payload["line_items"].append(item)

        return {"order": payload}

    # ---------------------------------------------------
    # ✅ INTERNAL HELPER: ORDER SYNC REPORT
    # ---------------------------------------------------
    def _create_order_sync_report(self, instance, payload, response_text, status, source_action):
        Report = self.env["shopify.sync.report"]
        Line = self.env["shopify.sync.report.line"]

        report = Report.create({
            "instance_id": instance.id,
            "sync_type": "order",
            "total_records": 1,
            "success_count": 1 if status == "success" else 0,
            "error_count": 1 if status == "error" else 0,
        })

        Line.create({
            "report_id": report.id,
            "record_type": "order",
            "source_action": source_action,
            "shopify_id": self.shopify_order_id or "",
            "name": self.name,
            "status": status,
            "error_message": response_text if status == "error" else False,
        })

    @api.depends("picking_ids.carrier_tracking_ref", "picking_ids.state")
    def _compute_shopify_tracking_ref(self):
        for order in self:
            ref = ""
            pickings = order.picking_ids.filtered(lambda p: p.picking_type_code == "outgoing")
            for picking in pickings:
                if picking.carrier_tracking_ref:
                    ref = picking.carrier_tracking_ref
                    break
            order.shopify_tracking_ref = ref

    # ---------------------------------------------------
    # CREATE ORDER ON SHOPIFY 
    # ---------------------------------------------------
    def action_create_shopify_order(self):
        self.ensure_one()

        instance = self.shopify_instance_id or self.env["shopify.instance"].search(
            [("active", "=", True)], limit=1
        )
        if not instance:
            raise UserError("Please configure Shopify Instance first.")
        self.shopify_instance_id = instance.id

        if not self.order_line:
            raise UserError("Order has no lines!")

        payload = self._prepare_shopify_order_payload()
        payload["order"]["financial_status"] = "paid"

        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/orders.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

        response = requests.post(url, json=payload, headers=headers)
        if response.status_code not in (200, 201):
            self._create_order_sync_report(
                instance, payload, response.text, "error", "order_create"
            )
            raise UserError(f"Shopify Error: {response.text}")

        self.shopify_order_id = response.json()["order"]["id"]

        self._create_order_sync_report(
            instance, payload, response.text, "success", "order_create"
        )
        self.shopify_financial_status = "paid"
        # Shopify reports gateway names separately; mark source as Odoo manual create.
        self.shopify_payment_method = self.shopify_payment_method or "Odoo"

        return {
            "effect": {
                "fadeout": "slow",
                "message": _("Order created successfully in Shopify!"),
                "type": "rainbow_man",
            }
        }

    # ---------------------------------------------------
    # UPDATE ORDER ON SHOPIFY 
    # ---------------------------------------------------
    def action_update_shopify_order(self):
        self.ensure_one()

        if not self.shopify_order_id:
            raise UserError("This order is not linked with Shopify.")

        instance = self.shopify_instance_id or self.env["shopify.instance"].search(
            [("active", "=", True)], limit=1
        )
        if not instance:
            raise UserError("Shopify instance not configured.")

        line_items = []
        for line in self.order_line:
            if not line.product_id.shopify_product_id:
                continue

            line_items.append({
                "variant_id": int(line.product_id.shopify_product_id),
                "quantity": int(line.product_uom_qty),
                "price": str(line.price_unit),
            })

        if not line_items:
            raise UserError("No Shopify products found in order lines.")

        payload = {
            "order": {
                "id": int(self.shopify_order_id),
                "line_items": line_items,
                "note": f"Updated from Odoo Order {self.name}",
            }
        }

        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/orders/{self.shopify_order_id}.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

        response = requests.put(url, json=payload, headers=headers)
        if response.status_code not in (200, 201):
            self._create_order_sync_report(
                instance, payload, response.text, "error", "order_update"
            )
            raise UserError(f"Shopify Update Failed:\n{response.text}")

        self._create_order_sync_report(
            instance, payload, response.text, "success", "order_update"
        )

        return {
            "effect": {
                "fadeout": "slow",
                "message": "Order updated successfully in Shopify",
                "type": "rainbow_man",
            }
        }

    # ---------------------------------------------------
    # PULL ORDER FROM SHOPIFY
    # ---------------------------------------------------
    def action_pull_shopify_order(self):
        """Fetch latest order from Shopify and remap into Odoo (handles exchanges)."""
        self.ensure_one()
        if not self.shopify_order_id:
            raise UserError(_("This order is not linked with Shopify."))
        instance = self.shopify_instance_id
        if not instance:
            raise UserError(_("Shopify instance not configured on this order."))

        try:
            data = instance._get(f"orders/{self.shopify_order_id}.json") or {}
            shop_order = data.get("order") or data
            self.map_shopify_order_to_odoo(shop_order, instance=instance)
        except Exception as exc:
            raise UserError(_("Failed to pull order from Shopify: %s") % exc) from exc

        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("Shopify"),
                "message": _("Order pulled from Shopify and updated."),
                "type": "success",
                "sticky": False,
            },
        }

    # ---------------------------------------------------
    # CREATE EXCHANGE PICKING WITHOUT TOUCHING CONFIRMED LINES
    # ---------------------------------------------------
    def _create_shopify_exchange_picking(self, deltas):
        """Create a new outgoing picking for exchange items."""
        self.ensure_one()
        if not deltas:
            return False
        warehouse = self.warehouse_id or self.env["stock.warehouse"].sudo().search([], limit=1)
        picking_type = warehouse and warehouse.out_type_id
        if not picking_type:
            raise UserError(_("No outgoing picking type configured for the warehouse."))

        picking_vals = {
            "picking_type_id": picking_type.id,
            "partner_id": self.partner_shipping_id.id,
            "origin": _("%s (Shopify Exchange)") % self.name,
            "location_id": picking_type.default_location_src_id.id,
            "location_dest_id": picking_type.default_location_dest_id.id,
            "company_id": self.company_id.id,
        }
        picking = self.env["stock.picking"].create(picking_vals)

        moves = []
        Move = self.env["stock.move"]
        for product, qty in deltas.items():
            if not qty:
                continue
            move_vals = {
                "product_id": product.id,
                "product_uom": product.uom_id.id,
                "product_uom_qty": qty,
                "location_id": picking.location_id.id,
                "location_dest_id": picking.location_dest_id.id,
                "picking_id": picking.id,
            }
            if "description_picking" in Move._fields:
                move_vals["description_picking"] = product.display_name
            elif "name" in Move._fields:
                move_vals["name"] = product.display_name
            moves.append(move_vals)
        if moves:
            self.env["stock.move"].create(moves)
        return picking

    # ---------------------------------------------------
    # CANCEL ORDER ON SHOPIFY WHEN ODOO IS CANCELLED
    # ---------------------------------------------------
    def _shopify_cancel_order(self):
        """Cancel the linked Shopify order."""
        self.ensure_one()   
        if not self.shopify_order_id or not self.shopify_instance_id:
            return False

        instance = self.shopify_instance_id
        url = instance._build_url(f"orders/{self.shopify_order_id}/cancel.json")

        def _fetch_shop_order():
            try:
                data = instance._get(f"orders/{self.shopify_order_id}.json") or {}
                return data.get("order") or data
            except Exception:
                return {}

        try:
            resp = requests.post(url, json={}, headers=instance._headers(), timeout=30)
        except requests.exceptions.RequestException as exc:
            raise UserError(_("Failed to cancel order on Shopify: %s") % exc) from exc

        shop_order = {}
        if resp.status_code in (200, 201):
            shop_order = resp.json() or {}
            shop_order = shop_order.get("order") or shop_order
        else:
            body = resp.text or ""
            shop_order = _fetch_shop_order()
            cancelled_at = shop_order.get("cancelled_at") if shop_order else False
            if not cancelled_at:
                msg = _("Shopify cancel failed (status %s): %s") % (resp.status_code, body)
                if resp.status_code == 422:
                    msg += _(
                        "\nShopify only cancels orders that are unfulfilled. "
                        "Cancel or void fulfillments in Shopify, then retry."
                    )
                raise UserError(msg)

        cancelled_at = shop_order.get("cancelled_at")
        if not cancelled_at:
            # Double-check state after cancel attempt to avoid silent failure.
            shop_order = _fetch_shop_order() or shop_order
            cancelled_at = shop_order.get("cancelled_at")
        updates = {}
        cancel_reason = shop_order.get("cancel_reason")
        financial_status = (shop_order.get("financial_status") or "").lower()
        if cancel_reason:
            updates["shopify_cancel_reason"] = cancel_reason
        if financial_status:
            updates["shopify_financial_status"] = financial_status
        if cancelled_at:
            updates.setdefault("shopify_cancel_reason", cancel_reason or False)
        else:
            raise UserError(
                _("Shopify did not mark the order as cancelled. Cancel fulfillments in Shopify and try again.")
            )
        if updates:
            self.write(updates)
        return True

    def action_cancel(self):
        """Cancel in Odoo and propagate to Shopify when linked."""
        res = super().action_cancel()
        if self.env.context.get("skip_shopify_cancel"):
            return res
        for order in self:
            if order.shopify_order_id and order.shopify_instance_id:
                order._shopify_cancel_order()
        return res

    # SHOPIFY → ODOO ORDER SYNC (GIFT CARD LOGIC UNCHANGED)
    # ---------------------------------------------------
    @api.model
    def map_shopify_order_to_odoo(self, order_data, instance=None):
        """Create or update an Odoo sale order from a Shopify order payload."""
        order_data = order_data or {}
        if isinstance(order_data, dict) and order_data.get("order"):
            order_data = order_data.get("order") or {}
        if isinstance(order_data, dict) and order_data.get("orders"):
            # If a list was passed accidentally, pick first
            orders_list = order_data.get("orders") or []
            if orders_list:
                order_data = orders_list[0]
        _logger.info("SHOPIFY ORDER PAYLOAD KEYS -> %s", list(order_data.keys()))
        instance = instance or self.env["shopify.instance"].sudo().search(
            [("active", "=", True)], limit=1
        )

        Partner = self.env["res.partner"].sudo()
        ProductTemplate = self.env["product.template"].sudo()

        def _float(val):
            try:
                return float(val or 0.0)
            except Exception:
                return 0.0

        def _resolve_country_state(addr):
            country = None
            state = None
            country_code = (addr or {}).get("country_code") or (addr or {}).get("country")
            state_code = (addr or {}).get("province_code") or (addr or {}).get("province")
            if country_code:
                country = self.env["res.country"].sudo().search(
                    [("code", "=", country_code)], limit=1
                )
            if state_code and country:
                state = self.env["res.country.state"].sudo().search(
                    [("code", "=", state_code), ("country_id", "=", country.id)],
                    limit=1,
                )
            return country, state

        def _get_partner():
            customer = order_data.get("customer") or {}
            # Use existing customer mapping helper for consistency
            partner_vals = Partner.map_shopify_customer_to_odoo(customer, instance=instance)
            shopify_customer_id = partner_vals.get("shopify_customer_id")
            email = partner_vals.get("email") or order_data.get("email")

            partner = False
            if shopify_customer_id:
                partner = Partner.search(
                    [("shopify_customer_id", "=", str(shopify_customer_id))],
                    limit=1,
                )
            # Match only by Shopify customer id (no email fallback).

            if partner:
                partner.write({k: v for k, v in partner_vals.items() if v})
            else:
                partner = Partner.create({k: v for k, v in partner_vals.items() if v})
            return partner

        def _get_child_address(parent, addr, addr_type):
            if not parent or not addr:
                return parent
            country, state = _resolve_country_state(addr)
            vals = {
                "parent_id": parent.id,
                "type": addr_type,
                "name": addr.get("name") or parent.name,
                "street": addr.get("address1"),
                "street2": addr.get("address2"),
                "city": addr.get("city"),
                "zip": addr.get("zip"),
                "phone": addr.get("phone") or parent.phone,
                "country_id": country.id if country else False,
                "state_id": state.id if state else False,
            }
            existing = Partner.search(
                [
                    ("parent_id", "=", parent.id),
                    ("type", "=", addr_type),
                    ("street", "=", vals.get("street")),
                    ("zip", "=", vals.get("zip")),
                    ("country_id", "=", vals.get("country_id") or False),
                ],
                limit=1,
            )
            if existing:
                existing.write({k: v for k, v in vals.items() if v})
                return existing
            return Partner.create({k: v for k, v in vals.items() if v})

        partner = _get_partner()
        shipping_partner = _get_child_address(
            partner, order_data.get("shipping_address") or {}, "delivery"
        )
        invoice_partner = _get_child_address(
            partner, order_data.get("billing_address") or {}, "invoice"
        ) or partner

        currency = False
        currency_code = order_data.get("currency")
        if currency_code:
            currency = self.env["res.currency"].sudo().search([("name", "=", currency_code)], limit=1)

        def _parse_dt(dt_val):
            """Parse Shopify ISO datetime safely into Odoo datetime."""
            if not dt_val:
                return False
            try:
                return fields.Datetime.to_datetime(dt_val)
            except Exception:
                try:
                    s = str(dt_val).replace("Z", "+00:00")
                    # strip fractional seconds if present and retry
                    if "." in s:
                        base, rest = s.split(".", 1)
                        tz = ""
                        if "+" in rest:
                            tz = "+" + rest.split("+", 1)[1]
                        elif "-" in rest[1:]:
                            tz = "-" + rest.split("-", 1)[1]
                        s = base + tz
                    return fields.Datetime.to_datetime(datetime.fromisoformat(s))
                except Exception:
                    _logger.warning("Unable to parse Shopify datetime: %s", dt_val)
                    return False

        financial_status = (order_data.get("financial_status") or "").lower()
        gateways = order_data.get("payment_gateway_names") or []
        payment_method = ", ".join([g for g in gateways if g]) or False
        if not payment_method:
            for tx in order_data.get("transactions") or []:
                gateway = tx.get("gateway")
                if gateway:
                    payment_method = gateway
                    break

        order_vals = {
            "shopify_order_id": str(order_data.get("id")),
            "partner_id": partner.id if partner else False,
            "partner_invoice_id": invoice_partner.id if invoice_partner else partner.id if partner else False,
            "partner_shipping_id": shipping_partner.id if shipping_partner else partner.id if partner else False,
            "client_order_ref": order_data.get("name"),
            "origin": order_data.get("order_number") or order_data.get("name"),
            "note": order_data.get("note"),
            "shopify_instance_id": instance.id if instance else False,
            "shopify_financial_status": financial_status or False,
            "shopify_payment_method": payment_method,
        }
        # assign salesperson to the current user so records appear in "My" views
        if self.env.uid:
            order_vals["user_id"] = self.env.uid
        if currency:
            order_vals["currency_id"] = currency.id
        if partner and partner.property_product_pricelist:
            order_vals["pricelist_id"] = partner.property_product_pricelist.id
        parsed_dt = _parse_dt(order_data.get("created_at") or order_data.get("processed_at"))
        if parsed_dt:
            order_vals["date_order"] = parsed_dt
        # Cancellation flags from Shopify
        cancel_reason = order_data.get("cancel_reason") or ""
        cancelled_at = order_data.get("cancelled_at")
        order_vals["shopify_cancel_reason"] = cancel_reason or False

        order = self.search(
            [("shopify_order_id", "=", order_vals["shopify_order_id"])],
            limit=1
        )
        if order:
            order.write(order_vals)
        else:
            order = self.create(order_vals)

        # Ensure order is editable when needed
        if order.state not in ("draft", "sent"):
            order.action_unlock()
        # If Shopify says cancelled, reflect in Odoo
        if cancelled_at and order.state != "cancel":
            order.with_context(skip_shopify_cancel=True).action_cancel()

        # Fallback products for generic lines
        def _fallback_product(code, name):
            tmpl = ProductTemplate.search([("default_code", "=", code)], limit=1)
            if not tmpl:
                tmpl = ProductTemplate.create({
                    "name": name,
                    "default_code": code,
                    "type": "service",
                })
            return tmpl.product_variant_id

        generic_product = _fallback_product("SHOPIFY_GENERIC", "Shopify Item")
        shipping_product = _fallback_product("SHOPIFY_SHIPPING", "Shopify Shipping")
        discount_product = _fallback_product("SHOPIFY_DISCOUNT", "Shopify Discount")
        adjust_product = _fallback_product("SHOPIFY_ADJUST", "Shopify Adjustment")
        tax_cache = {}

        def _ensure_tax(rate, title):
            """Find or create a tax matching the Shopify rate."""
            percent = round(rate * 100, 4)
            cache_key = f"{percent}"
            if cache_key in tax_cache:
                return tax_cache[cache_key]
            tax = self.env["account.tax"].sudo().search(
                [("type_tax_use", "in", ["sale", "all"]), ("amount", "=", percent)],
                limit=1,
            )
            if not tax:
                tax = self.env["account.tax"].sudo().create({
                    "name": title or f"Shopify Tax {percent}%",
                    "amount": percent,
                    "amount_type": "percent",
                    "type_tax_use": "sale",
                })
            tax_cache[cache_key] = tax
            return tax

        def _resolve_product(item):
            """Map Shopify line to an Odoo product using product_id or SKU."""
            product = False
            product_id = item.get("product_id")
            if product_id:
                tmpl = ProductTemplate.search(
                    [("shopify_product_id", "=", str(product_id))],
                    limit=1,
                )
                if tmpl:
                    product = tmpl.product_variant_id
            if not product:
                sku = (item.get("sku") or item.get("name") or "").strip()
                if sku:
                    prod_variant = self.env["product.product"].sudo().search(
                        [("default_code", "=", sku)],
                        limit=1,
                    )
                    if prod_variant:
                        product = prod_variant
            return product or generic_product

        def _group_line_items(items):
            """Group Shopify items by product and sum quantities/amounts."""
            grouped = {}
            for item in items or []:
                qty = _float(item.get("quantity"))
                if not qty:
                    continue
                product = _resolve_product(item)
                product_key = product.id
                line_total = _float(item.get("price")) * qty
                discount_alloc = sum(
                    [_float(alloc.get("amount")) for alloc in item.get("discount_allocations", [])]
                )
                if discount_alloc:
                    line_total -= discount_alloc

                tax_ids = []
                for t in item.get("tax_lines", []):
                    rate = _float(t.get("rate"))
                    if rate:
                        tax_ids.append(_ensure_tax(rate, t.get("title")).id)

                if product_key not in grouped:
                    grouped[product_key] = {
                        "qty": qty,
                        "total": line_total,
                        "tax_ids": set(tax_ids),
                        "name": item.get("name") or item.get("title") or "Shopify Item",
                        "product": product,
                    }
                else:
                    grouped[product_key]["qty"] += qty
                    grouped[product_key]["total"] += line_total
                    grouped[product_key]["tax_ids"].update(tax_ids)
            return grouped

        grouped_items = _group_line_items(order_data.get("line_items"))

        if order.state in ("draft", "sent"):
            order.order_line.filtered(lambda l: not getattr(l, "is_gift_card", False)).unlink()
            for data in grouped_items.values():
                qty = data["qty"]
                if not qty:
                    continue
                price_unit = data["total"] / qty if qty else 0.0
                order.order_line.create({
                    "order_id": order.id,
                    "name": data["name"],
                    "product_uom_qty": qty,
                    "price_unit": price_unit,
                    "product_id": data["product"].id,
                    "tax_ids": [(6, 0, list(data["tax_ids"]))],
                    "customer_lead": 0.0,
                })
        else:
            # Confirmed order: do not modify existing lines. Create an exchange picking for deltas.
            existing_qty = defaultdict(float)
            for line in order.order_line.filtered(lambda l: not getattr(l, "is_gift_card", False)):
                existing_qty[line.product_id.id] += line.product_uom_qty

            deltas = {}
            for key, data in grouped_items.items():
                inc_qty = data["qty"]
                prev_qty = existing_qty.get(key, 0.0)
                delta = inc_qty - prev_qty
                if delta > 0:
                    deltas[data["product"]] = delta

            if deltas:
                order._create_shopify_exchange_picking(deltas)

        if order.state in ("draft", "sent"):
            # Shipping lines
            for ship in order_data.get("shipping_lines", []):
                ship_amount = _float(ship.get("price"))
                if not ship_amount:
                    continue
                tax_ids = []
                for t in ship.get("tax_lines", []):
                    rate = _float(t.get("rate"))
                    if rate:
                        tax_ids.append(_ensure_tax(rate, t.get("title")).id)
                order.order_line.create({
                    "order_id": order.id,
                    "name": ship.get("title") or "Shipping",
                    "product_uom_qty": 1,
                    "price_unit": ship_amount,
                    "product_id": shipping_product.id,
                    "tax_ids": [(6, 0, tax_ids)],
                })

            # Discount codes (as negative lines)
            for disc in order_data.get("discount_codes", []):
                disc_amount = _float(disc.get("amount"))
                if not disc_amount:
                    continue
                order.order_line.create({
                    "order_id": order.id,
                    "name": f"Discount {disc.get('code') or ''}".strip() or "Discount",
                    "product_uom_qty": 1,
                    "price_unit": -abs(disc_amount),
                    "product_id": discount_product.id,
                })

            # Gift card handling (existing logic kept, idempotent)
            if not order.gift_card_applied:
                for gc in order_data.get("applied_gift_cards", []):
                    code = gc.get("code")
                    amount = _float(gc.get("amount"))

                    gift = self.env["shopify.gift.card"].sudo().search(
                        [("code", "=", code)],
                        limit=1
                    )
                    if gift:
                        gift.balance -= amount

                    existing_line = order.order_line.search([
                        ("order_id", "=", order.id),
                        ("gift_card_code", "=", code),
                    ], limit=1)

                    if not existing_line:
                        order.order_line.create({
                            "order_id": order.id,
                            "name": f"Gift Card ({code})",
                            "product_uom_qty": 1,
                            "price_unit": -amount,
                            "is_gift_card": True,
                            "gift_card_code": code,
                        })

                order.gift_card_applied = True

            # Adjustment to force totals to match Shopify total_price if needed
            shopify_total = _float(order_data.get("total_price"))
            # Recompute amounts using the current Odoo compute helper
            order._compute_amounts()
            delta = round(shopify_total - order.amount_total, 2)
            if delta:
                order.order_line.create({
                    "order_id": order.id,
                    "name": "Shopify Total Adjustment",
                    "product_uom_qty": 1,
                    "price_unit": delta,
                    "product_id": adjust_product.id,
                })

        # Confirm order if Shopify reports paid/authorized
        if order.state in ["draft", "sent"] and financial_status in ["paid", "partially_paid", "authorized"]:
            order.action_confirm()

        # If open/invoiced statuses should mark as paid, register payment stub (optional)
        if financial_status in ["paid", "partially_paid"] and order.invoice_status != "invoiced":
            try:
                order._create_invoices()
                for inv in order.invoice_ids:
                    inv.action_post()
            except Exception:
                # Keep silent; payment flow optional
                pass

        return order

    # ---------------------------------------------------
    # SHOPIFY REFUND → GIFT CARD BALANCE RESTORE (UNCHANGED)
    # ---------------------------------------------------
    @api.model
    def apply_shopify_gift_card_refund(self, refund_data):

        order_id = refund_data.get("order_id")
        if not order_id:
            return False

        order = self.search(
            [("shopify_order_id", "=", order_id)],
            limit=1
        )
        if not order:
            return False

        if order.gift_card_refund_processed:
            return True

        for tr in refund_data.get("transactions", []):
            if tr.get("gateway") == "gift_card":
                amount = float(tr.get("amount", 0.0))
                gift = self.env["shopify.gift.card"].sudo().search([], limit=1)
                if gift:
                    gift.balance += amount

        order.gift_card_refund_processed = True
        return True


# ---------------------------------------------------
# REFUND & RESTOCK TO SHOPIFY (BUTTON)
# ---------------------------------------------------
    def action_shopify_refund_and_restock(self):
        self.ensure_one()
        if not self.shopify_order_id:
            raise UserError(_("Not a Shopify order."))
        if not self.shopify_instance_id:
            raise UserError(_("Shopify instance missing on order."))

        delivered = self.picking_ids.filtered(lambda p: p.state == "done")
        if not delivered:
            raise UserError(_("No delivered pickings to refund/restock."))

        try:
            refund_move = self._shopify_create_refund_credit_note()
            return_pick = self._shopify_create_return_picking(delivered[0])
            self._shopify_push_refund(refund_move, return_pick)
            self.shopify_refund_status = "success"
            self.shopify_refund_message = _("Refund and restock synced to Shopify.")
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("Shopify"),
                    "message": _("Refund and restock completed."),
                    "type": "success",
                    "sticky": False,
                },
            }
        except Exception as e:
            _logger.exception("Shopify refund failed for order %s", self.name)
            self.shopify_refund_status = "failed"
            self.shopify_refund_message = str(e)
            raise

    def action_odoo_refund_only(self):
        """Create and post a credit note in Odoo only (no Shopify call)."""
        self.ensure_one()
        try:
            refund_move = self._shopify_create_refund_credit_note()
            self.shopify_odoo_refund_message = _("Refund created: %s") % (refund_move.name or "")
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("Odoo"),
                    "message": _("Credit Note created: %s") % (refund_move.name or ""),
                    "type": "success",
                    "sticky": False,
                },
            }
        except Exception as e:
            _logger.exception("Odoo-only refund failed for order %s", self.name)
            self.shopify_odoo_refund_message = str(e)
            raise

    def _shopify_create_refund_credit_note(self):
        """Create and post a credit note for all order lines."""
        self.ensure_one()
        Move = self.env["account.move"]

        def _line_vals(line):
            vals = {
                "product_id": line.product_id.id,
                "name": line.name,
                "quantity": line.product_uom_qty,
                "price_unit": line.price_unit,
                "tax_ids": [(6, 0, line.tax_id.ids)],
            }
            # Add analytic only when the field exists on account.move.line in this database.
            if "analytic_account_id" in self.env["account.move.line"]._fields:
                vals["analytic_account_id"] = getattr(line, "analytic_account_id", False) and line.analytic_account_id.id or False
            return vals

        inv_vals = self._prepare_invoice()
        inv_vals["move_type"] = "out_refund"
        inv_lines = [(0, 0, _line_vals(l)) for l in self.order_line]
        inv_vals["invoice_line_ids"] = inv_lines

        refund = Move.create(inv_vals)
        refund.action_post()
        return refund

    def _shopify_create_return_picking(self, picking):
        """Create and validate a return picking for the given delivered picking."""
        self.ensure_one()
        Return = self.env["stock.return.picking"]
        ctx = {
            "active_id": picking.id,
            "active_ids": [picking.id],
            "active_model": "stock.picking",
        }
        return_wiz = Return.with_context(ctx).create({
            "picking_id": picking.id,
        })
        for move in return_wiz.product_return_moves:
            qty_done = getattr(move.move_id, "quantity_done", move.move_id.product_uom_qty)
            move.quantity = move.quantity or qty_done
        if hasattr(return_wiz, "create_returns"):
            res = return_wiz.create_returns()
            return_picking = self.env["stock.picking"].browse(res.get("res_id"))
        elif hasattr(return_wiz, "_create_returns"):
            res = return_wiz._create_returns()
            return_picking = self.env["stock.picking"].browse(res.get("res_id"))
        else:
            # Manual fallback: create a return picking reversing locations and quantities
            return_picking = self._shopify_manual_return_picking(picking, return_wiz)
        if return_picking:
            return_picking.button_validate()
        return return_picking

    def _shopify_manual_return_picking(self, picking, return_wiz):
        """Fallback return creation when wizard methods are unavailable."""
        Picking = self.env["stock.picking"]
        Move = self.env["stock.move"]
        return_type = picking.picking_type_id.return_picking_type_id or picking.picking_type_id
        new_pick = Picking.create({
            "picking_type_id": return_type.id,
            "location_id": picking.location_dest_id.id,
            "location_dest_id": picking.location_id.id,
            "origin": _("Return of %s") % picking.name,
            "partner_id": picking.partner_id.id,
        })
        for line in return_wiz.product_return_moves:
            qty = line.quantity or getattr(line.move_id, "quantity_done", line.move_id.product_uom_qty)
            if not qty:
                continue
            Move.create({
                "name": line.move_id.name or picking.name,
                "product_id": line.product_id.id,
                "product_uom": line.product_id.uom_id.id,
                "product_uom_qty": qty,
                "location_id": picking.location_dest_id.id,
                "location_dest_id": picking.location_id.id,
                "picking_id": new_pick.id,
                "origin_returned_move_id": line.move_id.id if "origin_returned_move_id" in Move._fields else False,
                "procure_method": "make_to_stock",
            })
        new_pick.action_confirm()
        for mv in new_pick.move_ids_without_package:
            if "quantity_done" in mv._fields:
                mv.quantity_done = mv.product_uom_qty
            else:
                mv.write({"product_uom_qty": mv.product_uom_qty})
        if hasattr(new_pick, "button_validate"):
            new_pick.button_validate()
        elif hasattr(new_pick, "action_done"):
            new_pick.action_done()
        return new_pick

    def _shopify_push_refund(self, refund_move, return_picking):
        """Push refund and restock info to Shopify."""
        self.ensure_one()
        instance = self.shopify_instance_id
        order_external_id = self.shopify_order_id
        if not order_external_id or not instance:
            raise UserError(_("Missing Shopify order or instance."))

        order_data = instance._get(f"orders/{order_external_id}.json") or {}
        shop_order = order_data.get("order") or order_data
        line_map = {}
        line_qty_map = {}
        line_purchased = {}
        for li in shop_order.get("line_items", []):
            pid = str(li.get("product_id") or "")
            if pid:
                line_map[pid] = li.get("id")
            li_id = li.get("id")
            if li_id:
                purchased = int(li.get("quantity") or 0)
                line_purchased[li_id] = purchased
                refunded_prev = int(li.get("quantity_refunded") or 0) if "quantity_refunded" in li else 0
                available = max(0, purchased - refunded_prev) if refunded_prev else purchased
                line_qty_map[li_id] = available

        # Subtract already-refunded quantities from Shopify refunds endpoint to avoid over-refund errors.
        refunded_map = {}
        try:
            refunds_raw = instance._get(f"orders/{order_external_id}/refunds.json") or {}
            refunds = refunds_raw.get("refunds") or refunds_raw.get("refund") or []
            for rf in refunds:
                for rli in rf.get("refund_line_items", []):
                    li_id = rli.get("line_item_id")
                    qty = int(rli.get("quantity") or 0)
                    if li_id and qty:
                        refunded_map[li_id] = refunded_map.get(li_id, 0) + qty
        except Exception as exc:
            _logger.warning("Could not fetch Shopify refunds for %s: %s", order_external_id, exc)

        for li_id, purchased in line_purchased.items():
            prev = refunded_map.get(li_id, 0)
            # If Shopify already shows some refund, reduce available qty accordingly.
            line_qty_map[li_id] = max(0, purchased - prev)

        # Determine default location for restock: pick order location or first fulfillment location
        default_location_id = shop_order.get("location_id")
        if not default_location_id:
            fulfillments = shop_order.get("fulfillments") or []
            if fulfillments:
                default_location_id = fulfillments[0].get("location_id")

        refund_lines = []
        missing_ids = []
        moves = return_picking.move_ids_without_package if return_picking else []
        for mv in moves:
            tmpl_pid = getattr(mv.product_id.product_tmpl_id, "shopify_product_id", False)
            line_item_id = tmpl_pid and line_map.get(str(tmpl_pid))
            restock_type = "return" if line_item_id else "no_restock"
            requested_qty = int(getattr(mv, "quantity_done", mv.product_uom_qty or 0))
            # Clamp quantity to available purchased quantity to avoid Shopify errors
            available_qty = line_qty_map.get(line_item_id, requested_qty) if line_item_id else requested_qty
            final_qty = min(requested_qty, available_qty)
            rl = {
                "line_item_id": line_item_id,
                "quantity": final_qty,
                "restock_type": restock_type,
            }
            if restock_type == "return" and default_location_id:
                rl["location_id"] = default_location_id
            # Skip lines with zero qty after clamp
            if final_qty > 0:
                refund_lines.append(rl)
            if not line_item_id:
                missing_ids.append(mv.product_id.display_name)

        total_amount = abs(refund_move.amount_total_signed)
        currency = refund_move.currency_id.name

        # Find parent transaction and gateway for refund
        parent_id = False
        gateway = False
        parent_currency = False
        for tx in shop_order.get("transactions", []):
            kind = tx.get("kind")
            if kind in ("sale", "capture", "authorization"):
                parent_id = tx.get("id") or parent_id
                gateway = tx.get("gateway") or gateway
                parent_currency = tx.get("currency") or parent_currency
                if parent_id and gateway:
                    break
        # Fallback: fetch transactions endpoint if order payload lacks them
        if not parent_id:
            try:
                tx_resp = instance._get(f"orders/{order_external_id}/transactions.json") or {}
                tx_list = tx_resp.get("transactions") or tx_resp.get("transaction") or []
                for tx in tx_list:
                    kind = tx.get("kind")
                    if kind in ("sale", "capture", "authorization"):
                        parent_id = tx.get("id") or parent_id
                        gateway = tx.get("gateway") or gateway
                        parent_currency = tx.get("currency") or parent_currency
                        if parent_id and gateway:
                            break
            except Exception:
                parent_id = parent_id
        if not gateway:
            gateway = self.shopify_payment_method or "manual"
        include_parent = bool(parent_id)
        # If parent currency mismatches refund currency, drop parent usage.
        if parent_currency and currency != parent_currency:
            include_parent = False
        # If still no parent or mismatched, use cash gateway and no parent to avoid parent requirement.
        if not include_parent:
            gateway = "cash"
        # Align currency with parent transaction when available and used
        if parent_currency and include_parent:
            currency = parent_currency

        payload = {
            "refund": {
                "note": f"Odoo refund for {self.name}",
                "refund_line_items": refund_lines,
                "transactions": [
                    {
                        "kind": "refund",
                        "amount": str(total_amount),
                        "currency": currency,
                        "gateway": gateway,
                    }
                ],
                "notify": False,
            }
        }
        if include_parent:
            try:
                payload["refund"]["transactions"][0]["parent_id"] = int(parent_id)
            except Exception:
                payload["refund"]["transactions"][0]["parent_id"] = parent_id

        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/orders/{order_external_id}/refunds.json"
        headers = instance._headers()
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        if resp.status_code not in (200, 201):
            raise UserError(_("Shopify refund failed: %s") % resp.text)

        if missing_ids:
            self.shopify_refund_status = "partial"
            self.shopify_refund_message = _("Refunded but not restocked for: %s") % ", ".join(missing_ids)
        else:
            self.shopify_refund_status = "success"
            self.shopify_refund_message = _("Refund synced to Shopify.")

    @api.depends(
        "state",
        "shopify_refund_status",
        "shopify_cancel_reason",
        "shopify_order_id",
        "shopify_financial_status",
    )
    def _compute_order_sync_status(self):
        for order in self:
            # Common flags
            is_cancel = order.state == "cancel"
            is_refunded = (order.shopify_refund_status or "") in ("success", "partial")
            refund_req = (order.shopify_refund_status or "") == "pending"
            fin_status = (order.shopify_financial_status or "").lower()
            if fin_status in ("refunded", "partially_refunded"):
                is_refunded = True

            def _status(direction):
                if is_cancel:
                    return "cancelled"
                if is_refunded:
                    return "refunded"
                if refund_req:
                    return "refund_requested"
                # Pending logic per direction
                return "pending"

            order.order_sync_status_odoo_to_shopify = _status("odoo_to_shopify")
            order.order_sync_status_shopify_to_odoo = _status("shopify_to_odoo")

    def action_refresh_order_sync_status(self):
        """Manual refresh to recompute sync status."""
        self._compute_order_sync_status()
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("Order Sync Report"),
                "message": _("Statuses refreshed."),
                "type": "success",
                "sticky": False,
            },
        }

    def action_sync_order_report(self):
        """Pull latest status from Shopify for selected orders and recompute."""
        for order in self:
            if not order.shopify_order_id or not order.shopify_instance_id:
                continue
            try:
                data = order.shopify_instance_id._get(f"orders/{order.shopify_order_id}.json") or {}
                shop_order = data.get("order") or data
                cancel_reason = shop_order.get("cancel_reason") or ""
                cancelled_at = shop_order.get("cancelled_at")
                financial_status = (shop_order.get("financial_status") or "").lower()

                vals = {
                    "shopify_cancel_reason": cancel_reason or False,
                    "shopify_financial_status": financial_status or False,
                }
                if cancelled_at and order.state != "cancel":
                    order.with_context(skip_shopify_cancel=True).action_cancel()
                order.write(vals)
            except Exception as e:
                _logger.warning("Failed to sync order status for %s: %s", order.name, e)
        self._compute_order_sync_status()
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("Order Sync Report"),
                "message": _("Shopify status synced."),
                "type": "success",
                "sticky": False,
            },
        }

    def action_sync_all_order_report(self):
        """Sync Shopify status for all Shopify-linked orders."""
        all_orders = self.search([("shopify_order_id", "!=", False)])
        all_orders.action_sync_order_report()
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("Order Sync Report"),
                "message": _("All Shopify orders synced."),
                "type": "success",
                "sticky": False,
            },
        }
