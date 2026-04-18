# -*- coding: utf-8 -*-

import json
import logging
import hmac

from odoo import http
from odoo.http import request

from ..services.webhook_service import BigCommerceWebhookService

_logger = logging.getLogger(__name__)


class BigCommerceWebhookController(http.Controller):
    """Public webhook controller for BigCommerce inbound events."""

    @http.route(
        "/bigcommerce/webhook",
        type="http",
        auth="public",
        methods=["POST"],
        csrf=False,
        save_session=False,
    )
    def bigcommerce_webhook(self, **kwargs):
        """Accept webhook payload, persist event, and return HTTP acknowledgment."""
        raw_body = request.httprequest.get_data(as_text=True) or "{}"
        headers = dict(request.httprequest.headers or {})

        try:
            payload = json.loads(raw_body)
            if not isinstance(payload, dict):
                _logger.warning("BigCommerce webhook rejected: payload JSON is not an object.")
                return request.make_json_response(
                    {"success": False, "message": "Malformed payload. JSON object required."},
                    status=400,
                )
        except ValueError:
            _logger.warning("BigCommerce webhook rejected: malformed JSON body.")
            return request.make_json_response(
                {"success": False, "message": "Malformed JSON payload."},
                status=400,
            )

        store_hash = self._get_header(headers, "X-BC-Store-Hash")
        if not store_hash and kwargs.get("store_hash"):
            store_hash = kwargs.get("store_hash")
        if not store_hash:
            _logger.warning("BigCommerce webhook rejected: missing store hash.")
            return request.make_json_response(
                {"success": False, "message": "Missing store hash."},
                status=400,
            )

        instance = (
            request.env["bigcommerce.connector"]
            .sudo()
            .search([("store_hash", "=", store_hash)], limit=1)
        )
        if not instance:
            _logger.warning("BigCommerce webhook rejected: unknown store hash '%s'.", store_hash)
            return request.make_json_response(
                {"success": False, "message": "Unknown store hash."},
                status=404,
            )

        service = BigCommerceWebhookService(request.env)
        if not instance.webhook_enabled:
            _logger.warning("BigCommerce webhook rejected: webhooks disabled for instance '%s'.", instance.id)
            service.ingest_rejected_webhook(
                instance=instance,
                payload=payload,
                headers=headers,
                message="Webhooks are disabled for this connector.",
                destination=request.httprequest.url,
            )
            return request.make_json_response(
                {"success": False, "message": "Webhooks are disabled for this connector."},
                status=503,
            )

        expected_secret = (instance.sudo().webhook_secret or "").strip()
        if not expected_secret:
            _logger.warning("BigCommerce webhook rejected: instance '%s' has no webhook secret configured.", instance.id)
            service.ingest_rejected_webhook(
                instance=instance,
                payload=payload,
                headers=headers,
                message="Webhook secret is not configured.",
                destination=request.httprequest.url,
            )
            return request.make_json_response(
                {"success": False, "message": "Webhook secret is not configured."},
                status=503,
            )

        received_secret = (self._get_header(headers, "X-Webhook-Secret") or "").strip()
        if not received_secret or not hmac.compare_digest(received_secret, expected_secret):
            _logger.warning("BigCommerce webhook rejected: invalid custom webhook secret for instance '%s'.", instance.id)
            service.ingest_rejected_webhook(
                instance=instance,
                payload=payload,
                headers=headers,
                message="Invalid webhook secret.",
                destination=request.httprequest.url,
            )
            return request.make_json_response(
                {"success": False, "message": "Invalid webhook secret."},
                status=401,
            )

        result = service.ingest_webhook(
            instance=instance,
            payload=payload,
            headers=headers,
            raw_body=raw_body,
            destination=request.httprequest.url,
        )
        status_code = 200 if result.get("success") else 400
        return request.make_json_response(result, status=status_code)

    def _get_header(self, headers, name):
        if name in headers:
            return headers[name]
        lower = name.lower()
        for key, value in headers.items():
            if str(key).lower() == lower:
                return value
        return False
