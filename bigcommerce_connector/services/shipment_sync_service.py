# -*- coding: utf-8 -*-

import json
import logging

from odoo import fields

from .api_client import BigCommerceApiClient

_logger = logging.getLogger(__name__)


class BigCommerceShipmentSyncService:
    """Service layer for manual BigCommerce shipment export."""

    def __init__(self, instance):
        self.instance = instance
        self.env = instance.env
        self.client = BigCommerceApiClient(instance)

    def export_shipments(self, limit=50):
        """Export completed outgoing deliveries from Odoo to BigCommerce."""
        limit = int(limit or 50)
        if limit < 1:
            limit = 1
        if limit > 200:
            limit = 200

        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "shipment_export",
                "resource_type": "shipment",
                "request_method": "POST",
                "request_url": self.client._build_url("/v2/orders"),
                "status": "draft",
                "note": "Manual shipment export started (limit=%s)." % limit,
            }
        )

        pickings = self._get_pickings_to_export(limit=limit)
        if not pickings:
            message = "Shipment export completed. No eligible completed deliveries were found."
            self._log_success(message=message)
            return {
                "success": True,
                "message": message,
                "total": 0,
                "exported": 0,
                "failed": 0,
                "skipped": 0,
                "failed_items": [],
            }

        exported = 0
        failed = 0
        skipped = 0
        failed_items = []

        for picking in pickings:
            try:
                order_binding = self._find_order_binding_for_picking(picking)
                if not order_binding:
                    skipped += 1
                    reason = (
                        "Shipment skipped for picking %s: no BigCommerce order binding was found."
                        % (picking.name or picking.id)
                    )
                    failed_items.append(reason)
                    self._log_failure(
                        message=reason,
                        note="sale_order_id=%s" % (picking.sale_id.id if picking.sale_id else "N/A"),
                        status="skipped",
                    )
                    continue

                if not (picking.carrier_tracking_ref or "").strip():
                    skipped += 1
                    reason = (
                        "Shipment skipped for picking %s: missing tracking number."
                        % (picking.name or picking.id)
                    )
                    failed_items.append(reason)
                    self._log_failure(
                        message=reason,
                        resource_remote_id=order_binding.bigcommerce_order_id,
                        note="Tracking number is required for this export flow.",
                        status="skipped",
                    )
                    continue

                if (
                    order_binding.exported_at
                    and picking.date_done
                    and order_binding.exported_at >= picking.date_done
                ):
                    skipped += 1
                    reason = (
                        "Shipment skipped for picking %s: already exported on %s."
                        % (
                            picking.name or picking.id,
                            fields.Datetime.to_string(order_binding.exported_at),
                        )
                    )
                    self._log_failure(
                        message=reason,
                        resource_remote_id=order_binding.bigcommerce_order_id,
                        note="Idempotency guard by exported_at/date_done.",
                        status="skipped",
                    )
                    continue

                push_result = self._push_shipment(picking, order_binding)
                if push_result.get("success"):
                    exported += 1
                    order_binding.sudo().write(
                        {
                            "exported_at": fields.Datetime.now(),
                            "sync_state": "synced",
                            "last_error": False,
                        }
                    )
                    self._log_success(
                        message="Shipment exported for picking %s." % (picking.name or picking.id),
                        request_url=push_result.get("url"),
                        response_status=push_result.get("status_code"),
                        response_body=push_result.get("response_body"),
                        resource_remote_id=push_result.get("remote_id")
                        or order_binding.bigcommerce_order_id,
                        note="sale_order=%s, tracking=%s"
                        % (
                            picking.sale_id.name if picking.sale_id else "N/A",
                            (picking.carrier_tracking_ref or "").strip(),
                        ),
                        request_payload=push_result.get("request_payload"),
                    )
                else:
                    failed += 1
                    failure_message = push_result.get("message") or "Shipment export failed."
                    failed_items.append(
                        "Picking %s: %s" % ((picking.name or picking.id), failure_message)
                    )
                    order_binding.sudo().write(
                        {
                            "sync_state": "error",
                            "last_error": failure_message,
                        }
                    )
                    self._log_failure(
                        message=failure_message,
                        request_url=push_result.get("url"),
                        response_status=push_result.get("status_code"),
                        response_body=push_result.get("response_body"),
                        resource_remote_id=order_binding.bigcommerce_order_id,
                        note="picking=%s" % (picking.name or picking.id),
                        request_payload=push_result.get("request_payload"),
                    )
            except Exception as err:
                failed += 1
                failure_message = str(err)
                failed_items.append(
                    "Picking %s: %s" % ((picking.name or picking.id), failure_message)
                )
                _logger.exception(
                    "Shipment export failed for instance_id=%s picking_id=%s picking_name=%s",
                    self.instance.id,
                    picking.id,
                    picking.name,
                )
                self._log_failure(
                    message=failure_message,
                    note="Unhandled exception while exporting picking id=%s." % picking.id,
                )

        total = len(pickings)
        success = failed == 0
        message = (
            "Shipment export completed. Total: %(total)s, Exported: %(exported)s, "
            "Failed: %(failed)s, Skipped: %(skipped)s."
        ) % {
            "total": total,
            "exported": exported,
            "failed": failed,
            "skipped": skipped,
        }

        if success:
            self._log_success(message=message)
        else:
            self._log_failure(
                message=message,
                note="First failures: %s" % " | ".join(failed_items[:3]),
            )

        return {
            "success": success,
            "message": message,
            "total": total,
            "exported": exported,
            "failed": failed,
            "skipped": skipped,
            "failed_items": failed_items[:5],
        }

    def _get_pickings_to_export(self, limit=50):
        """Return completed outgoing pickings linked to sale orders for this instance company."""
        picking_model = self.env["stock.picking"].with_context(active_test=False)
        if "sale_id" not in picking_model._fields:
            return picking_model.browse()

        candidate_limit = max(int(limit) * 4, int(limit))
        candidates = picking_model.search(
            [
                ("company_id", "=", self.instance.company_id.id),
                ("state", "=", "done"),
                ("picking_type_code", "=", "outgoing"),
                ("sale_id", "!=", False),
            ],
            order="date_done desc, id desc",
            limit=candidate_limit,
        )

        selected_ids = []
        seen_sale_ids = set()
        for picking in candidates:
            sale_id = picking.sale_id.id
            if not sale_id or sale_id in seen_sale_ids:
                continue
            seen_sale_ids.add(sale_id)
            selected_ids.append(picking.id)
            if len(selected_ids) >= limit:
                break

        return picking_model.browse(selected_ids)

    def _find_order_binding_for_picking(self, picking):
        """Resolve BigCommerce order binding for the picking's sale order."""
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

    def _prepare_shipment_payload(self, picking, order_binding):
        """Build a minimal safe shipment payload for BigCommerce."""
        tracking_number = (picking.carrier_tracking_ref or "").strip()
        if not tracking_number:
            raise ValueError("Tracking number is required for shipment export.")

        payload = {
            "tracking_number": tracking_number,
            "comments": "Exported from Odoo picking %s" % (picking.name or picking.id),
        }

        if picking.carrier_id:
            payload["shipping_provider"] = (picking.carrier_id.name or "").strip() or False
        return payload

    def _push_shipment(self, picking, order_binding):
        """Push one shipment payload to BigCommerce."""
        payload = self._prepare_shipment_payload(picking, order_binding)
        path = "/v2/orders/%s/shipments" % order_binding.bigcommerce_order_id
        result = self.client._request(
            method="POST",
            path=path,
            payload=payload,
            timeout=25,
        )

        if result.get("success"):
            remote_id = self._extract_remote_id(result.get("response_body"))
            return {
                "success": True,
                "message": "Shipment exported successfully.",
                "status_code": result.get("status_code"),
                "url": result.get("url"),
                "response_body": result.get("response_body"),
                "remote_id": remote_id,
                "request_payload": payload,
            }

        status_code = result.get("status_code")
        response_body = result.get("response_body")
        message = result.get("message") or "BigCommerce shipment export failed."
        if status_code:
            message = "%s (HTTP %s)" % (message, status_code)
        if response_body not in (None, False, ""):
            try:
                body_text = json.dumps(response_body)[:300]
            except TypeError:
                body_text = str(response_body)[:300]
            message = "%s Response: %s" % (message, body_text)

        return {
            "success": False,
            "message": message,
            "status_code": status_code,
            "url": result.get("url"),
            "response_body": response_body,
            "remote_id": False,
            "request_payload": payload,
        }

    def _log_success(
        self,
        message,
        request_url=False,
        response_status=False,
        response_body=None,
        resource_remote_id=False,
        note=False,
        request_payload=False,
    ):
        """Create a success sync log record for shipment export."""
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "shipment_export",
                "resource_type": "shipment",
                "resource_remote_id": resource_remote_id,
                "request_url": request_url,
                "request_method": "POST",
                "request_payload": self._as_request_payload(request_payload),
                "response_status": self._as_response_status(response_status),
                "response_body": self._as_response_body(response_body),
                "status": "success",
                "note": note or message,
            }
        )

    def _log_failure(
        self,
        message,
        request_url=False,
        response_status=False,
        response_body=None,
        resource_remote_id=False,
        note=False,
        request_payload=False,
        status="failed",
    ):
        """Create a failed/skipped sync log record for shipment export."""
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "shipment_export",
                "resource_type": "shipment",
                "resource_remote_id": resource_remote_id,
                "request_url": request_url,
                "request_method": "POST",
                "request_payload": self._as_request_payload(request_payload),
                "response_status": self._as_response_status(response_status),
                "response_body": self._as_response_body(response_body),
                "status": status if status in ("failed", "skipped") else "failed",
                "error_message": message,
                "note": note,
            }
        )

    def _extract_remote_id(self, response_body):
        if isinstance(response_body, dict):
            if response_body.get("id"):
                return str(response_body.get("id"))
            if isinstance(response_body.get("data"), dict) and response_body["data"].get("id"):
                return str(response_body["data"].get("id"))
        return False

    def _as_request_payload(self, request_payload):
        if request_payload in (None, False, ""):
            return False
        try:
            return json.dumps(request_payload)[:3000]
        except TypeError:
            return str(request_payload)[:3000]

    def _as_response_status(self, response_status):
        if response_status in (None, False, ""):
            return False
        return str(response_status)

    def _as_response_body(self, response_body):
        if response_body in (None, False, ""):
            return False
        return str(response_body)[:3000]
