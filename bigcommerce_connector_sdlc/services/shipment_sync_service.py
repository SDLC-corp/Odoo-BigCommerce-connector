# -*- coding: utf-8 -*-

import json
import logging

from odoo import fields

from .api_client import BigCommerceApiClient

_logger = logging.getLogger(__name__)


class BigCommerceShipmentSyncService:
    """Export completed Odoo deliveries to BigCommerce as order shipments.

    BigCommerce's ``POST /v2/orders/{id}/shipments`` requires ``order_address_id`` and the shipped
    ``items`` (``order_product_id`` + ``quantity``); the payload sent before 19.0.2.1.0 carried
    neither, so the store refused every shipment. Each delivery (including backorders / partial
    shipments) becomes its own shipment, and the created shipment id is stored on the delivery so it
    is never exported twice.
    """

    # BigCommerce's documented values for ``shipping_provider``; anything else is refused, so an
    # unknown carrier is sent without a provider (the tracking number is still recorded).
    PROVIDER_KEYWORDS = (
        ("fedex", "fedex"),
        ("ups", "ups"),
        ("usps", "usps"),
        ("endicia", "endicia"),
        ("auspost", "auspost"),
        ("australia post", "auspost"),
        ("canada post", "canadapost"),
        ("canadapost", "canadapost"),
        ("royal mail", "royalmail"),
        ("royalmail", "royalmail"),
    )

    def __init__(self, instance):
        self.instance = instance
        self.env = instance.env
        self.client = BigCommerceApiClient(instance)

    # ------------------------------------------------------------------
    # Batch export
    # ------------------------------------------------------------------
    def export_shipments(self, limit=50):
        """Export completed, tracked deliveries of BigCommerce orders that were never exported."""
        limit = max(1, min(200, int(limit or 50)))
        pickings = self._get_pickings_to_export(limit=limit)
        if not pickings:
            message = "Shipment export completed. No delivery waiting for export."
            self._log(status="success", message=message)
            return {"success": True, "message": message, "total": 0, "exported": 0, "failed": 0,
                    "skipped": 0, "failed_items": []}

        exported = failed = skipped = 0
        failed_items = []
        for picking in pickings:
            try:
                with self.env.cr.savepoint():
                    result = self.export_picking(picking)
            except Exception as err:  # noqa: BLE001 - one bad delivery must not stop the batch
                _logger.warning("Shipment export failed for picking %s: %s", picking.id, err)
                result = {"status": "failed", "message": str(err)}
                picking.sudo().write({"bigcommerce_shipment_error": str(err)})
            if result["status"] == "exported":
                exported += 1
            elif result["status"] == "skipped":
                skipped += 1
            else:
                failed += 1
                failed_items.append("%s: %s" % (picking.name, result.get("message")))

        message = (
            "Shipment export completed. Total: %(total)s, Exported: %(exported)s, "
            "Failed: %(failed)s, Skipped: %(skipped)s."
        ) % {"total": len(pickings), "exported": exported, "failed": failed, "skipped": skipped}
        self._log(
            status="failed" if failed else "success",
            message=message,
            note=("First failures: %s" % " | ".join(failed_items[:3])) if failed_items else False,
        )
        return {
            "success": failed == 0,
            "message": message,
            "total": len(pickings),
            "exported": exported,
            "failed": failed,
            "skipped": skipped,
            "failed_items": failed_items[:5],
        }

    def _bound_sale_order_ids(self):
        return self.env["bigcommerce.order.binding"].sudo().search(
            [
                ("instance_id", "=", self.instance.id),
                ("sale_order_id", "!=", False),
                ("bigcommerce_order_id", "!=", False),
            ]
        ).sale_order_id.ids

    def _get_pickings_to_export(self, limit=50):
        """Done outgoing deliveries of this connector's orders, tracked and not yet exported.

        Deliveries of orders that do not come from this store are never even looked at (the old
        search took the company's latest 4 x limit deliveries and logged a "skipped" line for
        every non-BigCommerce one on every run).
        """
        sale_ids = self._bound_sale_order_ids()
        if not sale_ids:
            return self.env["stock.picking"].browse()
        return self.env["stock.picking"].sudo().search(
            [
                ("company_id", "=", self.instance.company_id.id),
                ("state", "=", "done"),
                ("picking_type_code", "=", "outgoing"),
                ("sale_id", "in", sale_ids),
                ("bigcommerce_shipment_id", "=", False),
                ("carrier_tracking_ref", "!=", False),
            ],
            order="date_done asc, id asc",
            limit=limit,
        )

    # ------------------------------------------------------------------
    # One delivery
    # ------------------------------------------------------------------
    def export_picking(self, picking):
        """Create the BigCommerce shipment for one delivery. Returns {"status", "message"}."""
        picking = picking.sudo()
        if picking.bigcommerce_shipment_id:
            return {"status": "skipped", "message": "Already exported as shipment %s." % picking.bigcommerce_shipment_id}
        binding = self._find_order_binding_for_picking(picking)
        if not binding:
            return {"status": "skipped", "message": "The sale order is not a BigCommerce order of this store."}
        tracking = (picking.carrier_tracking_ref or "").strip()
        if not tracking:
            return {"status": "skipped", "message": "The delivery has no tracking number yet."}

        order_id = binding.bigcommerce_order_id
        # A shipment with this tracking number may already exist (e.g. the response of an earlier
        # POST was lost): link it instead of creating a duplicate.
        existing = self._find_remote_shipment(order_id, tracking)
        if existing:
            self._mark_exported(picking, binding, existing, note="Linked to the existing BigCommerce shipment.")
            return {"status": "exported", "message": "Linked existing shipment %s." % existing}

        address_id = self._order_address_id(order_id)
        if not address_id:
            return self._fail(picking, binding, "BigCommerce returned no shipping address for order %s." % order_id)
        items, unmatched = self._shipment_items(picking, order_id, binding)
        if not items:
            return self._fail(
                picking,
                binding,
                "No delivered product could be matched to a line of BigCommerce order %s (unmatched: %s)."
                % (order_id, ", ".join(unmatched) or "none"),
            )

        payload = {
            "order_address_id": address_id,
            "tracking_number": tracking,
            "items": items,
            "comments": "Odoo delivery %s" % picking.name,
        }
        provider = self._shipping_provider(picking)
        if provider:
            payload["shipping_provider"] = provider

        result = self.client.post("/v2/orders/%s/shipments" % order_id, payload=payload)
        if not result.get("success"):
            message = result.get("message") or "BigCommerce shipment export failed."
            self._log(
                status="failed",
                message=message,
                request_url=result.get("url"),
                response_status=result.get("status_code"),
                response_body=result.get("response_body"),
                request_payload=payload,
                resource_remote_id=order_id,
                note="picking=%s" % picking.name,
            )
            return self._fail(picking, binding, message)

        shipment_id = self._extract_remote_id(result.get("response_body")) or "created"
        note = "Exported %s item line(s)." % len(items)
        if unmatched:
            note += " Not matched to an order line (not sent): %s." % ", ".join(unmatched)
        self._mark_exported(picking, binding, shipment_id, note=note)
        self._log(
            status="success",
            message="Shipment exported for %s." % picking.name,
            request_url=result.get("url"),
            response_status=result.get("status_code"),
            response_body=result.get("response_body"),
            request_payload=payload,
            resource_remote_id=order_id,
            note=note,
        )
        return {"status": "exported", "message": note, "shipment_id": shipment_id}

    def _mark_exported(self, picking, binding, shipment_id, note=False):
        now = fields.Datetime.now()
        picking.write(
            {
                "bigcommerce_shipment_id": str(shipment_id),
                "bigcommerce_shipment_exported_at": now,
                "bigcommerce_shipment_error": False,
            }
        )
        binding_vals = {"exported_at": now}
        if binding.sync_state == "error" and (binding.last_error or "").startswith("Shipment"):
            binding_vals.update({"sync_state": "synced", "last_error": False})
        binding.write(binding_vals)
        if note:
            picking.message_post(body="BigCommerce: %s" % note)

    def _fail(self, picking, binding, message):
        picking.write({"bigcommerce_shipment_error": message})
        binding.write({"sync_state": "error", "last_error": "Shipment %s: %s" % (picking.name, message)})
        return {"status": "failed", "message": message}

    def _find_order_binding_for_picking(self, picking):
        if not picking.sale_id:
            return self.env["bigcommerce.order.binding"].sudo()
        return self.env["bigcommerce.order.binding"].sudo().search(
            [
                ("instance_id", "=", self.instance.id),
                ("sale_order_id", "=", picking.sale_id.id),
                ("bigcommerce_order_id", "!=", False),
            ],
            limit=1,
        )

    def _find_remote_shipment(self, order_id, tracking):
        # V2 list endpoints page (50 per page by default): read every page so a shipment past
        # the first page is still recognised and never duplicated.
        result = self.client.get_paginated("/v2/orders/%s/shipments" % order_id, limit=250)
        if not result.get("success"):
            return False
        for shipment in result.get("items") or []:
            if isinstance(shipment, dict) and (shipment.get("tracking_number") or "").strip() == tracking:
                return str(shipment.get("id") or "") or False
        return False

    def _order_address_id(self, order_id):
        result = self.client.get("/v2/orders/%s/shipping_addresses" % order_id)
        body = result.get("response_body")
        if not result.get("success") or not isinstance(body, list) or not body:
            return False
        first = body[0] if isinstance(body[0], dict) else {}
        return first.get("id") or False

    def _shipment_items(self, picking, order_id, binding):
        """Map delivered quantities to BigCommerce order product lines.

        Matching order: product binding (variant id, then product id), then SKU. Quantities are
        capped at what is still unshipped on the BigCommerce line.
        """
        # GET /v2/orders/{id}/products returns 50 lines per page by default: read every page.
        result = self.client.get_paginated("/v2/orders/%s/products" % order_id, limit=250)
        lines = (result.get("items") or []) if result.get("success") else []
        remaining = {}
        for line in lines:
            if not isinstance(line, dict) or not line.get("id"):
                continue
            qty = int(float(line.get("quantity") or 0)) - int(float(line.get("quantity_shipped") or 0))
            remaining[line["id"]] = max(0, qty)

        product_bindings = self.env["bigcommerce.product.binding"].sudo().search(
            [("instance_id", "=", self.instance.id), ("product_id", "in", picking.move_ids.product_id.ids)]
        )

        def candidates_for(product):
            refs = product_bindings.filtered(lambda b: b.product_id == product)
            variant_ids = {str(b.bigcommerce_variant_id) for b in refs if b.bigcommerce_variant_id}
            product_ids = {str(b.bigcommerce_product_id) for b in refs if b.bigcommerce_product_id}
            sku = (product.default_code or "").strip().lower()
            by_variant = [l for l in lines if isinstance(l, dict) and str(l.get("variant_id") or "") in variant_ids]
            if by_variant:
                return by_variant
            by_product = [l for l in lines if isinstance(l, dict) and str(l.get("product_id") or "") in product_ids]
            if by_product:
                return by_product
            return [l for l in lines if isinstance(l, dict) and sku and (l.get("sku") or "").strip().lower() == sku]

        items = {}
        unmatched = []
        for move in picking.move_ids.filtered(lambda m: m.state == "done" and m.product_id.type != "service"):
            qty = int(round(move.product_uom._compute_quantity(move.quantity, move.product_id.uom_id)))
            if qty <= 0:
                continue
            for line in candidates_for(move.product_id):
                if qty <= 0:
                    break
                available = remaining.get(line.get("id"), 0)
                take = min(qty, available)
                if take <= 0:
                    continue
                items[line["id"]] = items.get(line["id"], 0) + take
                remaining[line["id"]] = available - take
                qty -= take
            if qty > 0:
                unmatched.append("%s x%s" % (move.product_id.display_name, qty))
        return [{"order_product_id": key, "quantity": value} for key, value in items.items()], unmatched

    def _shipping_provider(self, picking):
        carrier = picking.carrier_id
        if not carrier:
            return False
        text = " ".join(filter(None, [(carrier.name or ""), (carrier.delivery_type or "")])).lower()
        for keyword, provider in self.PROVIDER_KEYWORDS:
            if keyword in text:
                return provider
        return False

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------
    def _log(self, status, message, request_url=False, response_status=False, response_body=None,
             request_payload=False, resource_remote_id=False, note=False):
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "shipment_export",
                "resource_type": "shipment",
                "resource_remote_id": resource_remote_id,
                "request_url": request_url,
                "request_method": "POST",
                "request_payload": self._as_text(request_payload),
                "response_status": str(response_status) if response_status else False,
                "response_body": self._as_text(response_body),
                "status": status,
                "error_message": message if status == "failed" else False,
                "note": note or message,
            }
        )

    def _extract_remote_id(self, response_body):
        if isinstance(response_body, dict):
            if response_body.get("id"):
                return str(response_body.get("id"))
            if isinstance(response_body.get("data"), dict) and response_body["data"].get("id"):
                return str(response_body["data"].get("id"))
        return False

    def _as_text(self, value):
        if value in (None, False, ""):
            return False
        try:
            return json.dumps(value)[:3000]
        except TypeError:
            return str(value)[:3000]
