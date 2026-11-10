# -*- coding: utf-8 -*-

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone

from psycopg2 import IntegrityError

from odoo import fields

from .customer_sync_service import BigCommerceCustomerSyncService
from .order_sync_service import BigCommerceOrderSyncService
from .product_sync_service import BigCommerceProductSyncService

_logger = logging.getLogger(__name__)


class BigCommerceWebhookService:
    """Service layer for BigCommerce webhook ingestion and asynchronous processing.

    BigCommerce delivers callbacks as::

        {"scope": "store/order/updated", "store_id": "1025646", "producer": "stores/abc123",
         "hash": "<sha1 of data>", "created_at": 1561479335, "data": {"type": "order", "id": 250}}

    The scope travels in the body, not in a header (before 19.0.2.1.0 the connector read an
    ``X-BC-Scope`` header that BigCommerce never sends, so every real callback was stored without
    a scope and closed as "no processor configured").
    """

    # Rejected callbacks are stored for diagnostics, but never more than this many per connector
    # and hour: an unauthenticated sender must not be able to grow the database without bound.
    MAX_REJECTED_PER_HOUR = 100
    REJECTED_PAYLOAD_MAX = 2000
    RETRY_BASE_MINUTES = 2
    RETRY_MAX_MINUTES = 360

    def __init__(self, env):
        self.env = env

    # ------------------------------------------------------------------
    # Intake
    # ------------------------------------------------------------------
    def ingest_webhook(self, instance, payload, headers, raw_body=False, destination=False):
        """Store an authenticated webhook as a pending event (or recognise a re-delivery).

        Returns ``{"success": True, "event_id": id, "duplicate": bool}``.
        """
        payload = payload or {}
        safe_headers = self._sanitize_headers(headers or {})
        event_model = self.env["bigcommerce.webhook.event"].sudo()

        scope = self._scope(payload, safe_headers)
        resource_id = self._extract_resource_id(payload)
        event_type = scope or payload.get("type") or "unknown"
        dedup_key = self._dedup_key(payload, scope, raw_body)
        now = fields.Datetime.now()

        # Replay / re-delivery protection: BigCommerce retries a callback until it gets a 2xx,
        # so the same delivery can arrive several times. It is processed once.
        if dedup_key:
            original = event_model.search(
                [
                    ("instance_id", "=", instance.id),
                    ("dedup_key", "=", dedup_key),
                    ("status", "not in", ("duplicate", "rejected")),
                ],
                limit=1,
            )
            if original:
                original.write({"last_received_at": now})
                return {
                    "success": True,
                    "message": "Duplicate delivery ignored.",
                    "event_id": original.id,
                    "duplicate": True,
                }

        # Coalescing: an event for the same resource is already waiting; processing re-reads the
        # current state from the API, so a second pending event would only repeat the work.
        if resource_id and scope and not scope.endswith("/deleted"):
            pending = event_model.search(
                [
                    ("instance_id", "=", instance.id),
                    ("status", "=", "pending"),
                    ("resource_id", "=", resource_id),
                    ("scope", "=like", "%s%%" % self._scope_family(scope)),
                ],
                limit=1,
            )
            if pending:
                pending.write({"last_received_at": now})
                return {
                    "success": True,
                    "message": "Merged into a pending event for the same resource.",
                    "event_id": pending.id,
                    "duplicate": True,
                }

        vals = {
            "instance_id": instance.id,
            "event_type": event_type,
            "scope": scope or False,
            "webhook_id": self._header_value(safe_headers, "X-BC-Webhook-Id"),
            "resource_id": resource_id,
            "signature": self._header_value(safe_headers, "X-BC-Signature"),
            "destination": destination or False,
            "payload_json": self._as_json(payload),
            "headers_json": self._as_json(safe_headers),
            "dedup_key": dedup_key or False,
            "remote_created_at": self._remote_created_at(payload),
            "phase": "validated",
            "status": "pending",
            "received_at": now,
            "last_received_at": now,
            "validated_at": now,
            "note": "Webhook authenticated and queued for processing (scope=%s)." % (scope or "N/A"),
        }
        try:
            with self.env.cr.savepoint():
                event = event_model.create(vals)
        except IntegrityError:
            # A concurrent request stored the same delivery between the search and the insert.
            original = event_model.search(
                [("instance_id", "=", instance.id), ("dedup_key", "=", dedup_key)], limit=1
            )
            return {
                "success": True,
                "message": "Duplicate delivery ignored.",
                "event_id": original.id,
                "duplicate": True,
            }

        _logger.info(
            "BigCommerce webhook queued event_id=%s instance_id=%s scope=%s resource_id=%s",
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
            note="Queued scope=%s event_id=%s" % (scope or "N/A", event.id),
        )
        return {"success": True, "message": "Webhook accepted.", "event_id": event.id, "duplicate": False}

    def ingest_rejected_webhook(self, instance, payload, headers, message, destination=False):
        """Keep a trace of a refused callback (never processed, payload truncated, rate-capped)."""
        event_model = self.env["bigcommerce.webhook.event"].sudo()
        now = fields.Datetime.now()
        recent = event_model.search_count(
            [
                ("instance_id", "=", instance.id),
                ("status", "=", "rejected"),
                ("received_at", ">=", now - timedelta(hours=1)),
            ]
        )
        if recent >= self.MAX_REJECTED_PER_HOUR:
            _logger.warning(
                "BigCommerce webhook rejected for instance_id=%s (%s); not stored, hourly cap reached.",
                instance.id,
                message,
            )
            return event_model
        payload = payload if isinstance(payload, dict) else {}
        safe_headers = self._sanitize_headers(headers or {})
        scope = self._scope(payload, safe_headers)
        event = event_model.create(
            {
                "instance_id": instance.id,
                "event_type": scope or "unknown",
                "scope": scope or False,
                "resource_id": self._extract_resource_id(payload),
                "destination": destination or False,
                "payload_json": self._as_json(payload)[: self.REJECTED_PAYLOAD_MAX],
                "headers_json": self._as_json(safe_headers)[: self.REJECTED_PAYLOAD_MAX],
                "phase": "failed",
                "status": "rejected",
                "received_at": now,
                "last_received_at": now,
                "processing_finished_at": now,
                "error_message": message,
                "note": "Webhook rejected before queueing; it will never be processed.",
            }
        )
        self._create_log(
            instance=instance,
            operation_type="webhook_receive",
            resource_type="webhook",
            status="failed",
            message=message,
            resource_remote_id=event.resource_id,
            note="Rejected scope=%s event_id=%s" % (scope or "N/A", event.id),
        )
        return event

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------
    def process_event(self, event):
        """Process one pending/failed webhook event; rejected and duplicate events are refused."""
        event = event.sudo()
        if event.status in ("rejected", "duplicate"):
            return {"success": False, "message": "Rejected or duplicate events are never processed."}
        if event.status not in ("pending", "failed", "processing"):
            return {"success": True, "message": "Event already processed."}

        now = fields.Datetime.now()
        event.write(
            {
                "status": "processing",
                "phase": "dispatched",
                "dispatched_at": now,
                "processing_started_at": now,
            }
        )
        payload = event.payload_dict()
        scope = (event.scope or "").lower()

        try:
            # Everything the handler wrote is rolled back if it fails half-way, so a failed event
            # never leaves a half-imported order behind.
            with self.env.cr.savepoint():
                dispatch_note = self._dispatch_webhook(
                    scope=scope,
                    payload=payload,
                    connector=event.instance_id,
                    event=event,
                )
        except Exception as err:
            _logger.warning("Webhook processing failed for event_id=%s: %s", event.id, err)
            retry_count = event.retry_count + 1
            max_retries = max(0, int(event.instance_id.webhook_max_retries or 0))
            next_retry = False
            if retry_count < max_retries:
                minutes = min(self.RETRY_BASE_MINUTES * (2 ** (retry_count - 1)), self.RETRY_MAX_MINUTES)
                next_retry = fields.Datetime.now() + timedelta(minutes=minutes)
            event.write(
                {
                    "status": "failed",
                    "phase": "failed",
                    "processed_at": fields.Datetime.now(),
                    "failed_at": fields.Datetime.now(),
                    "processing_finished_at": fields.Datetime.now(),
                    "error_message": str(err),
                    "retry_count": retry_count,
                    "next_retry_at": next_retry,
                    "note": (
                        "Automatic retry scheduled." if next_retry
                        else "Maximum automatic retries reached; retry it manually once the cause is fixed."
                    ),
                }
            )
            self._create_log(
                instance=event.instance_id,
                operation_type="webhook_process",
                resource_type="webhook",
                status="failed",
                resource_remote_id=event.resource_id,
                message=str(err),
                note="scope=%s event_id=%s attempt=%s" % (event.scope or "N/A", event.id, retry_count),
            )
            return {"success": False, "message": str(err)}

        event.write(
            {
                "status": "done",
                "phase": "synced",
                "processed_at": fields.Datetime.now(),
                "synced_at": fields.Datetime.now(),
                "processing_finished_at": fields.Datetime.now(),
                "error_message": False,
                "next_retry_at": False,
                "note": dispatch_note,
            }
        )
        self._create_log(
            instance=event.instance_id,
            operation_type="webhook_process",
            resource_type="webhook",
            status="success",
            resource_remote_id=event.resource_id,
            note="%s (event_id=%s)" % (dispatch_note, event.id),
        )
        return {"success": True, "message": "Webhook processed."}

    def _claim_events(self, instance=None, limit=100):
        """Return due events, locking them so a concurrent run skips them (SKIP LOCKED)."""
        self.env.flush_all()
        now = fields.Datetime.now()
        query = """
            SELECT e.id
              FROM bigcommerce_webhook_event e
              JOIN bigcommerce_connector c ON c.id = e.instance_id
             WHERE (e.status = 'pending'
                    OR (e.status = 'failed'
                        AND e.retry_count < COALESCE(c.webhook_max_retries, 0)
                        AND (e.next_retry_at IS NULL OR e.next_retry_at <= %s)))
        """
        params = [now]
        if instance:
            query += " AND e.instance_id = %s"
            params.append(instance.id)
        query += " ORDER BY e.received_at, e.id LIMIT %s FOR UPDATE OF e SKIP LOCKED"
        params.append(limit)
        self.env.cr.execute(query, params)
        ids = [row[0] for row in self.env.cr.fetchall()]
        return self.env["bigcommerce.webhook.event"].sudo().browse(ids)

    def process_pending_events(self, instance=None, limit=100):
        """Process due webhook events in batch (pending, and failed ones whose back-off elapsed)."""
        limit = max(1, min(1000, int(limit or 100)))
        events = self._claim_events(instance=instance, limit=limit)

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
        if scope.endswith("/deleted"):
            return self._handle_deleted_webhook(scope=scope, connector=connector, payload=payload, event=event)
        if scope.startswith("store/order/"):
            return self._handle_order_webhook(connector=connector, payload=payload, event=event)
        if scope.startswith("store/product/"):
            return self._handle_product_webhook(connector=connector, payload=payload, event=event)
        if scope.startswith("store/customer/") and not scope.startswith("store/customer/address"):
            return self._handle_customer_webhook(connector=connector, payload=payload, event=event)
        return "No processor configured for scope '%s'." % (event.scope or "unknown")

    def _service_connector(self, connector):
        return connector.sudo().with_company(connector.company_id).with_context(bigcommerce_log_source="webhook")

    def _handle_order_webhook(self, connector, payload, event):
        """Import/update the order the callback refers to (its current state is read from the API)."""
        order_id = self._extract_resource_id(payload) or event.resource_id
        if not order_id:
            raise ValueError("Order webhook is missing resource id.")
        result = BigCommerceOrderSyncService(self._service_connector(connector)).import_order_by_id(order_id)
        if not result.get("success"):
            raise ValueError(result.get("message") or "Failed to import order from webhook.")
        return "Order webhook synced order %s." % order_id

    def _handle_product_webhook(self, connector, payload, event):
        """Import/update the product the callback refers to."""
        product_id = self._extract_resource_id(payload) or event.resource_id
        if not product_id:
            raise ValueError("Product webhook is missing resource id.")
        result = BigCommerceProductSyncService(self._service_connector(connector)).import_product_by_id(product_id)
        if not result.get("success"):
            raise ValueError(result.get("message") or "Failed to import product from webhook.")
        return "Product webhook synced product %s." % product_id

    def _handle_customer_webhook(self, connector, payload, event):
        """Import/update the customer the callback refers to."""
        customer_id = self._extract_resource_id(payload) or event.resource_id
        if not customer_id:
            raise ValueError("Customer webhook is missing resource id.")
        result = BigCommerceCustomerSyncService(self._service_connector(connector)).import_customer_by_id(
            customer_id
        )
        if not result.get("success"):
            raise ValueError(result.get("message") or "Failed to import customer from webhook.")
        return "Customer webhook synced customer %s." % customer_id

    def _handle_deleted_webhook(self, scope, connector, payload, event):
        """A resource was deleted in BigCommerce: flag its binding instead of trying to import it.

        Nothing is deleted or archived in Odoo automatically; the binding moves to the exception
        queue with an explanation so a person decides.
        """
        remote_id = self._extract_resource_id(payload) or event.resource_id
        if not remote_id:
            return "Deletion callback without resource id ignored."
        stamp = fields.Datetime.to_string(fields.Datetime.now())
        message = "Deleted in BigCommerce (%s, %s)." % (scope, stamp)
        if scope.startswith("store/product/"):
            bindings = self.env["bigcommerce.product.binding"].sudo().search(
                [("instance_id", "=", connector.id), ("bigcommerce_product_id", "=", str(remote_id))]
            )
        elif scope.startswith("store/customer/"):
            bindings = self.env["bigcommerce.customer.binding"].sudo().search(
                [("instance_id", "=", connector.id), ("bigcommerce_customer_id", "=", str(remote_id))]
            )
        elif scope.startswith("store/category/"):
            bindings = self.env["bigcommerce.category.binding"].sudo().search(
                [("instance_id", "=", connector.id), ("bigcommerce_category_id", "=", str(remote_id))]
            )
        else:
            return "No processor configured for scope '%s'." % scope
        bindings.write({"sync_state": "error", "last_error": message})
        return "%s %s binding(s) flagged." % (message, len(bindings))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _scope(self, payload, headers):
        scope = payload.get("scope") if isinstance(payload, dict) else False
        if not scope:
            # Legacy/synthetic events (e.g. the connector's own "Send Test Event").
            scope = self._header_value(headers, "X-BC-Scope")
        return (str(scope).strip() if scope else "") or False

    def _scope_family(self, scope):
        parts = (scope or "").split("/")
        return "/".join(parts[:2]) + "/" if len(parts) >= 2 else (scope or "")

    def _dedup_key(self, payload, scope, raw_body):
        payload_hash = payload.get("hash") if isinstance(payload, dict) else False
        created_at = payload.get("created_at") if isinstance(payload, dict) else False
        if payload_hash:
            return "%s|%s|%s" % (scope or "", payload_hash, created_at or "")
        if raw_body:
            body = raw_body.encode() if isinstance(raw_body, str) else raw_body
            return "sha256|%s" % hashlib.sha256(body).hexdigest()
        return False

    def _remote_created_at(self, payload):
        value = payload.get("created_at") if isinstance(payload, dict) else False
        try:
            stamp = int(value)
        except (TypeError, ValueError):
            return False
        try:
            return datetime.fromtimestamp(stamp, tz=timezone.utc).replace(tzinfo=None)
        except (OverflowError, OSError, ValueError):
            return False

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
            if lower in ("x-webhook-secret", "authorization", "x-auth-token", "cookie"):
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
