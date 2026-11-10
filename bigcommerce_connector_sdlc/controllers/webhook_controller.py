# -*- coding: utf-8 -*-

import hmac
import json
import logging

from odoo import http
from odoo.http import request

from ..services.webhook_service import BigCommerceWebhookService

_logger = logging.getLogger(__name__)


class BigCommerceWebhookController(http.Controller):
    """Public webhook endpoint for BigCommerce callbacks.

    Authentication: BigCommerce sends back the custom ``X-Webhook-Secret`` header the connector
    registered with each hook; it is compared in constant time with every connector of the
    store hash (a store hash is unique per company only, so several companies may share it).
    Only an authenticated callback is queued; anything else gets a generic answer and, when the
    store hash is known, a capped "rejected" trace that is never processed.
    """

    MAX_BODY_BYTES = 1024 * 1024

    @http.route(
        "/bigcommerce/webhook",
        type="http",
        auth="public",
        methods=["POST"],
        csrf=False,
        save_session=False,
    )
    def bigcommerce_webhook(self, **kwargs):
        """Authenticate, de-duplicate and queue a callback; processing is asynchronous."""
        httprequest = request.httprequest
        if (httprequest.content_length or 0) > self.MAX_BODY_BYTES:
            return request.make_json_response({"success": False, "message": "Payload too large."}, status=413)
        raw = httprequest.get_data(cache=False) or b"{}"
        if len(raw) > self.MAX_BODY_BYTES:
            return request.make_json_response({"success": False, "message": "Payload too large."}, status=413)
        raw_body = raw.decode("utf-8", errors="replace")
        headers = dict(httprequest.headers or {})

        try:
            payload = json.loads(raw_body)
        except ValueError:
            payload = None
        if not isinstance(payload, dict):
            _logger.warning("BigCommerce webhook rejected: body is not a JSON object.")
            return request.make_json_response(
                {"success": False, "message": "Malformed payload. JSON object required."}, status=400
            )

        store_hash = self._resolve_store_hash(kwargs, headers, payload)
        if not store_hash:
            return request.make_json_response({"success": False, "message": "Missing store hash."}, status=400)

        candidates = (
            request.env["bigcommerce.connector"]
            .sudo()
            .search([("store_hash", "=", store_hash), ("active", "=", True)])
        )
        if not candidates:
            _logger.warning("BigCommerce webhook rejected: unknown store hash.")
            return request.make_json_response({"success": False, "message": "Unknown store hash."}, status=404)

        service = BigCommerceWebhookService(request.env)
        received_secret = (self._get_header(headers, "X-Webhook-Secret") or "").strip()
        instance = self._match_instance(candidates, received_secret)
        destination = httprequest.base_url

        if not instance:
            reason = "Invalid webhook secret." if received_secret else "Missing webhook secret."
            _logger.warning("BigCommerce webhook rejected for store hash %s: %s", store_hash, reason)
            for candidate in candidates:
                service.ingest_rejected_webhook(
                    instance=candidate, payload=payload, headers=headers, message=reason, destination=destination
                )
            return request.make_json_response({"success": False, "message": "Unauthorized."}, status=401)

        if not instance.webhook_enabled:
            service.ingest_rejected_webhook(
                instance=instance,
                payload=payload,
                headers=headers,
                message="Webhooks are disabled for this connector.",
                destination=destination,
            )
            return request.make_json_response(
                {"success": False, "message": "Webhooks are disabled for this connector."}, status=503
            )

        result = service.ingest_webhook(
            instance=instance,
            payload=payload,
            headers=headers,
            raw_body=raw_body,
            destination=destination,
        )
        return request.make_json_response(
            {"success": bool(result.get("success")), "duplicate": bool(result.get("duplicate"))},
            status=200 if result.get("success") else 400,
        )

    def _resolve_store_hash(self, kwargs, headers, payload):
        store_hash = (kwargs.get("store_hash") or "").strip()
        if not store_hash:
            store_hash = (self._get_header(headers, "X-BC-Store-Hash") or "").strip()
        if not store_hash:
            producer = str(payload.get("producer") or "")
            if producer.startswith("stores/"):
                store_hash = producer.split("/", 1)[1].strip()
        return store_hash.strip("/") or False

    def _match_instance(self, candidates, received_secret):
        """Connector whose webhook secret equals the received one (constant-time comparison)."""
        if not received_secret:
            return candidates.browse()
        matched = candidates.browse()
        for candidate in candidates:
            expected = (candidate.sudo().webhook_secret or "").strip()
            if expected and hmac.compare_digest(received_secret.encode(), expected.encode()):
                matched = candidate
        return matched

    def _get_header(self, headers, name):
        if name in headers:
            return headers[name]
        lower = name.lower()
        for key, value in headers.items():
            if str(key).lower() == lower:
                return value
        return False
