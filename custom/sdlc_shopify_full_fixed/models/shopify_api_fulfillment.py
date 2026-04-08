import logging
import requests
from requests.exceptions import HTTPError

from odoo import models

_logger = logging.getLogger(__name__)


class ShopifyApiFulfillment(models.AbstractModel):
    _name = "shopify.api.fulfillment"
    _description = "Shopify Fulfillment API Helper"

    def _headers(self, instance):
        return {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

    def get_fulfillment_orders(self, instance, shopify_order_id):
        """Fetch fulfillment orders for a Shopify order."""
        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/orders/{shopify_order_id}/fulfillment_orders.json"
        try:
            res = requests.get(url, headers=self._headers(instance), timeout=20)
            res.raise_for_status()
            return (res.json() or {}).get("fulfillment_orders") or []
        except Exception as e:
            _logger.error("Shopify fulfillment_orders fetch failed: %s", e)
            return []

    def get_fulfillments(self, instance, shopify_order_id):
        """Fetch fulfillments for a Shopify order (used to recover missing fulfillment ids)."""
        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/orders/{shopify_order_id}/fulfillments.json"
        try:
            res = requests.get(url, headers=self._headers(instance), timeout=20)
            res.raise_for_status()
            return (res.json() or {}).get("fulfillments") or []
        except Exception as e:
            _logger.error("Shopify fulfillments fetch failed: %s", e)
            return []

    def _get_fulfillment_order_detail(self, instance, fulfillment_order_id):
        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/fulfillment_orders/{fulfillment_order_id}.json"
        try:
            res = requests.get(url, headers=self._headers(instance), timeout=20)
            res.raise_for_status()
            return (res.json() or {}).get("fulfillment_order") or {}
        except Exception as e:
            _logger.error("Shopify fulfillment_order detail failed: %s", e)
            return {}

    def create_fulfillment_with_tracking(
        self,
        instance,
        fulfillment_order_id,
        tracking_number,
        tracking_company=None,
        tracking_urls=None,
        order_external_id=None,
    ):
        """Create a fulfillment with tracking for a fulfillment order."""
        tracking_urls = tracking_urls or []
        fo = self._get_fulfillment_order_detail(instance, fulfillment_order_id)

        def _extract_line_items(fo_record):
            extracted = []
            if not fo_record:
                return extracted
            if fo_record.get("line_items"):
                extracted.extend(fo_record.get("line_items"))
            if fo_record.get("fulfillment_order_line_items"):
                extracted.extend(fo_record.get("fulfillment_order_line_items"))
            if fo_record.get("line_items_by_fulfillment_order"):
                for entry in fo_record.get("line_items_by_fulfillment_order"):
                    extracted.extend(entry.get("fulfillment_order_line_items") or [])
            return extracted

        def _line_item_qty(li):
            if li.get("fulfillable_quantity") is not None:
                return max(0, li.get("fulfillable_quantity"))
            if li.get("remaining_fulfillable_quantity") is not None:
                return max(0, li.get("remaining_fulfillable_quantity"))
            if li.get("quantity") is not None:
                return max(0, li.get("quantity"))
            if li.get("original_total_quantity") is not None:
                return max(0, li.get("original_total_quantity"))
            return 0

        line_items = _extract_line_items(fo)
        # Fallback: if detail call returned no lines, try the fulfillment_orders list
        if not line_items and order_external_id:
            fos = self.get_fulfillment_orders(instance, order_external_id)
            fo_match = next((x for x in fos if str(x.get("id")) == str(fulfillment_order_id)), None)
            if fo_match:
                line_items = _extract_line_items(fo_match)
        location_id = fo.get("assigned_location_id") or fo.get("location_id")

        if not location_id:
            raise Exception("Shopify fulfillment order missing assigned location.")

        items_payload = []
        for li in line_items:
            fid = li.get("id")
            # Shopify rejects if we request more than fulfillable_quantity.
            qty = _line_item_qty(li)
            if fid and qty:
                items_payload.append({"id": fid, "quantity": qty})

        if not items_payload:
            raise Exception(
                "No fulfillment line items found for Shopify fulfillment order. "
                f"FO keys={list(fo.keys()) if fo else []} raw_line_items={line_items}"
            )

        payload = {
            "fulfillment": {
                "location_id": int(location_id),
                "line_items_by_fulfillment_order": [
                    {
                        "fulfillment_order_id": int(fulfillment_order_id),
                        "fulfillment_order_line_items": items_payload,
                    }
                ],
                "tracking_info": {
                    "number": tracking_number,
                    "company": tracking_company or "",
                    "url": tracking_urls[0] if tracking_urls else "",
                },
            }
        }

        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/fulfillments.json"
        try:
            res = requests.post(
                url,
                headers=self._headers(instance),
                json=payload,
                timeout=20,
            )
            try:
                res.raise_for_status()
            except Exception:
                _logger.error("Shopify fulfillment create failed: %s", res.text)
                # Bubble the Shopify error body for easier debugging
                raise Exception(res.text)
            return (res.json() or {}).get("fulfillment") or {}
        except Exception as e:
            _logger.error("Shopify fulfillment create failed: %s", e)
            raise

    def get_fulfillment(self, instance, fulfillment_id):
        """Fetch a single fulfillment to retrieve tracking status/info."""
        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/fulfillments/{fulfillment_id}.json"
        try:
            res = requests.get(url, headers=self._headers(instance), timeout=20)
            res.raise_for_status()
            return (res.json() or {}).get("fulfillment") or {}
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                msg = "Shopify fulfillment not found (it may have been deleted or replaced). Please push tracking again."
                _logger.warning(msg)
                raise Exception(msg)
            _logger.error("Shopify fulfillment fetch failed: %s", e.response.text if e.response else e)
            raise

    def update_fulfillment_tracking(
        self,
        instance,
        fulfillment_id,
        tracking_number,
        tracking_company=None,
        tracking_urls=None,
    ):
        """Update tracking info on an existing fulfillment instead of creating a new one."""
        tracking_urls = tracking_urls or []
        payload = {
            "fulfillment": {
                "tracking_info": {
                    "number": tracking_number,
                    "company": tracking_company or "",
                    "url": tracking_urls[0] if tracking_urls else "",
                }
            }
        }
        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/fulfillments/{fulfillment_id}/update_tracking.json"
        try:
            res = requests.post(
                url,
                headers=self._headers(instance),
                json=payload,
                timeout=20,
            )
            try:
                res.raise_for_status()
            except Exception:
                _logger.error("Shopify fulfillment tracking update failed: %s", res.text)
                raise Exception(res.text)
            return (res.json() or {}).get("fulfillment") or {}
        except Exception as e:
            _logger.error("Shopify fulfillment tracking update failed: %s", e)
            raise
