# -*- coding: utf-8 -*-

import json
import logging

from odoo import fields

from .customer_sync_service import BigCommerceCustomerSyncService
from .order_sync_service import BigCommerceOrderSyncService
from .product_sync_service import BigCommerceProductSyncService

_logger = logging.getLogger(__name__)


class BigCommerceWebhookService:
    """Service layer for BigCommerce webhook ingestion and async processing."""

    def __init__(self, env):
        self.env = env

    def ingest_webhook(self, instance, payload, headers, raw_body=False, destination=False):
        """Store a webhook event in pending state for async processing."""
        payload = payload or {}
        headers = headers or {}
        safe_headers = self._sanitize_headers(headers)

        scope = self._header_value(safe_headers, "X-BC-Scope")
        webhook_id = self._header_value(safe_headers, "X-BC-Webhook-Id")
        signature = self._header_value(safe_headers, "X-BC-Signature")
        resource_id = self._extract_resource_id(payload)
        event_type = scope or payload.get("type") or "unknown"
        now = fields.Datetime.now()

        # NOTE: custom shared-secret validation is enforced in the controller via X-Webhook-Secret.
        # X-BC-Signature may vary by BigCommerce app mode and is not relied upon here.

        event = self.env["bigcommerce.webhook.event"].sudo().create(
            {
                "instance_id": instance.id,
                "event_type": event_type,
                "scope": scope,
                "webhook_id": webhook_id,
                "resource_id": resource_id,
                "signature": signature,
                "destination": destination or False,
                "payload_json": self._as_json(payload),
                "headers_json": self._as_json(safe_headers),
                "phase": "received",
                "status": "pending",
                "received_at": now,
                "last_received_at": now,
            }
        )
        _logger.info(
            "BigCommerce webhook received event_id=%s instance_id=%s scope=%s resource_id=%s",
            event.id,
            instance.id,
            scope or "N/A",
            resource_id or "N/A",
        )
        instance.sudo().write({"webhook_last_sync_at": now})

        self._create_log(
            instance=instance,
            operation_type="webhook_receive",
            resource_type="webhook",
            status="success",
            resource_remote_id=resource_id,
            note="phase=received scope=%s event=%s" % (scope or "N/A", event_type or "N/A"),
        )
        event.sudo().write(
            {
                "phase": "validated",
                "validated_at": fields.Datetime.now(),
                "note": "Webhook validated and queued for processing (scope=%s)." % (scope or "N/A"),
            }
        )
        self._create_log(
            instance=instance,
            operation_type="webhook_receive",
            resource_type="webhook",
            status="success",
            resource_remote_id=resource_id,
            note="phase=validated scope=%s event=%s" % (scope or "N/A", event_type or "N/A"),
        )
        return {
            "success": True,
            "message": "Webhook accepted.",
            "event_id": event.id,
        }

    def ingest_rejected_webhook(self, instance, payload, headers, message, destination=False):
        """Persist rejected webhook attempts for diagnostics (known instance only)."""
        payload = payload or {}
        headers = headers or {}
        safe_headers = self._sanitize_headers(headers)
        now = fields.Datetime.now()
        scope = self._header_value(safe_headers, "X-BC-Scope")
        event_type = scope or payload.get("type") or "unknown"
        resource_id = self._extract_resource_id(payload)

        event = self.env["bigcommerce.webhook.event"].sudo().create(
            {
                "instance_id": instance.id,
                "event_type": event_type,
                "scope": scope,
                "webhook_id": self._header_value(safe_headers, "X-BC-Webhook-Id"),
                "resource_id": resource_id,
                "signature": self._header_value(safe_headers, "X-BC-Signature"),
                "destination": destination or False,
                "payload_json": self._as_json(payload),
                "headers_json": self._as_json(safe_headers),
                "phase": "failed",
                "status": "failed",
                "received_at": now,
                "last_received_at": now,
                "processed_at": now,
                "failed_at": now,
                "processing_finished_at": now,
                "error_message": message,
                "note": "Webhook rejected before queueing.",
            }
        )
        instance.sudo().write({"webhook_last_sync_at": now})
        self._create_log(
            instance=instance,
            operation_type="webhook_receive",
            resource_type="webhook",
            status="failed",
            message=message,
            resource_remote_id=resource_id,
            note="phase=failed scope=%s event_id=%s" % (scope or "N/A", event.id),
        )
        return event

    def process_event(self, event):
        """Process one pending webhook event safely."""
        if event.status not in ("pending", "failed", "processing"):
            return {"success": True, "message": "Event already processed."}

        event.write(
            {
                "status": "processing",
                "phase": "dispatched",
                "dispatched_at": fields.Datetime.now(),
                "processing_started_at": fields.Datetime.now(),
            }
        )
        self._create_log(
            instance=event.instance_id,
            operation_type="webhook_process",
            resource_type="webhook",
            status="success",
            resource_remote_id=event.resource_id,
            note="phase=dispatched scope=%s" % (event.scope or "N/A"),
        )
        payload = event.payload_dict()
        scope = (event.scope or "").lower()

        try:
            dispatch_note = self._dispatch_webhook(
                scope=scope,
                payload=payload,
                connector=event.instance_id,
                event=event,
            )
            event.write(
                {
                    "status": "done",
                    "phase": "synced",
                    "processed_at": fields.Datetime.now(),
                    "synced_at": fields.Datetime.now(),
                    "processing_finished_at": fields.Datetime.now(),
                    "error_message": False,
                    "note": dispatch_note,
                }
            )
            self._create_log(
                instance=event.instance_id,
                operation_type="webhook_process",
                resource_type="webhook",
                status="success",
                resource_remote_id=event.resource_id,
                note="phase=synced scope=%s" % (event.scope or "N/A"),
            )
            _logger.info(
                "BigCommerce webhook synced event_id=%s scope=%s resource_id=%s",
                event.id,
                event.scope or "N/A",
                event.resource_id or "N/A",
            )
            return {"success": True, "message": "Webhook processed."}
        except Exception as err:
            _logger.exception("Webhook processing failed for event_id=%s", event.id)
            event.write(
                {
                    "status": "failed",
                    "phase": "failed",
                    "processed_at": fields.Datetime.now(),
                    "failed_at": fields.Datetime.now(),
                    "processing_finished_at": fields.Datetime.now(),
                    "error_message": str(err),
                    "retry_count": event.retry_count + 1,
                }
            )
            self._create_log(
                instance=event.instance_id,
                operation_type="webhook_process",
                resource_type="webhook",
                status="failed",
                resource_remote_id=event.resource_id,
                message=str(err),
                note="phase=failed scope=%s" % (event.scope or "N/A"),
            )
            return {"success": False, "message": str(err)}

    def process_pending_events(self, instance=None, limit=100):
        """Process pending webhook events in batch."""
        limit = int(limit or 100)
        if limit < 1:
            limit = 1
        if limit > 1000:
            limit = 1000

        domain = [("status", "in", ("pending", "failed"))]
        if instance:
            domain.append(("instance_id", "=", instance.id))

        events = self.env["bigcommerce.webhook.event"].sudo().search(
            domain,
            order="received_at asc,id asc",
            limit=limit,
        )

        processed = 0
        failed = 0
        for event in events:
            result = self.process_event(event)
            if result.get("success"):
                processed += 1
            else:
                failed += 1
        return {
            "success": failed == 0,
            "message": "Webhook processing completed. Processed: %s, Failed: %s." % (processed, failed),
            "total": len(events),
            "processed": processed,
            "failed": failed,
        }

    def _dispatch_webhook(self, scope, payload, connector, event):
        """Dispatch webhook payload by scope and return processing note."""
        if "store/order/" in scope:
            return self._handle_order_webhook(connector=connector, payload=payload, event=event)
        if "store/product/" in scope:
            return self._handle_product_webhook(connector=connector, payload=payload, event=event)
        if "store/customer/" in scope:
            return self._handle_customer_webhook(connector=connector, payload=payload, event=event)
        return "No processor configured for scope '%s'." % (event.scope or "unknown")

    def _handle_order_webhook(self, connector, payload, event):
        """Auto-import/update order from webhook payload."""
        order_id = self._extract_resource_id(payload) or event.resource_id
        if not order_id:
            raise ValueError("Order webhook is missing resource id.")

        service = BigCommerceOrderSyncService(
            connector.with_context(bigcommerce_log_source="webhook")
        )
        result = service.import_order_by_id(order_id)
        if not result.get("success"):
            raise ValueError(result.get("message") or "Failed to import order from webhook.")
        return "Order webhook synced order %s." % order_id

    def _handle_product_webhook(self, connector, payload, event):
        """Auto-import/update product from webhook payload."""
        product_id = self._extract_resource_id(payload) or event.resource_id
        if not product_id:
            raise ValueError("Product webhook is missing resource id.")

        service = BigCommerceProductSyncService(
            connector.with_context(bigcommerce_log_source="webhook")
        )
        result = service.import_product_by_id(product_id)
        if not result.get("success"):
            raise ValueError(result.get("message") or "Failed to import product from webhook.")
        return "Product webhook synced product %s." % product_id

    def _handle_customer_webhook(self, connector, payload, event):
        """Auto-import/update customer from webhook payload."""
        customer_id = self._extract_resource_id(payload) or event.resource_id
        if not customer_id:
            raise ValueError("Customer webhook is missing resource id.")

        service = BigCommerceCustomerSyncService(
            connector.with_context(bigcommerce_log_source="webhook")
        )
        result = service.import_customer_by_id(customer_id)
        if not result.get("success"):
            raise ValueError(result.get("message") or "Failed to import customer from webhook.")
        return "Customer webhook synced customer %s." % customer_id

    def _header_value(self, headers, key):
        if not headers:
            return False
        if key in headers:
            return headers.get(key)
        lower = key.lower()
        for hdr_key, hdr_val in headers.items():
            if str(hdr_key).lower() == lower:
                return hdr_val
        return False

    def _extract_resource_id(self, payload):
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict) and data.get("id") not in (None, False, ""):
                return str(data.get("id"))
            if payload.get("id") not in (None, False, ""):
                return str(payload.get("id"))
            if payload.get("resource_id") not in (None, False, ""):
                return str(payload.get("resource_id"))
        return False

    def _as_json(self, value):
        try:
            return json.dumps(value or {}, sort_keys=True)
        except (TypeError, ValueError):
            return "{}"

    def _sanitize_headers(self, headers):
        """Remove sensitive inbound headers before persistence/logging."""
        sanitized = {}
        for key, value in (headers or {}).items():
            key_str = str(key)
            lower = key_str.lower()
            if lower in ("x-webhook-secret", "authorization", "x-auth-token"):
                continue
            sanitized[key_str] = value
        return sanitized

    def _create_log(
        self,
        instance,
        operation_type,
        resource_type,
        status,
        message=False,
        note=False,
        resource_remote_id=False,
    ):
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": instance.id,
                "operation_type": operation_type,
                "resource_type": resource_type,
                "resource_remote_id": resource_remote_id,
                "status": status,
                "error_message": message if status == "failed" else False,
                "note": note or message,
            }
        )
