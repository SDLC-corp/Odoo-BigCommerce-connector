# -*- coding: utf-8 -*-

import logging

from .api_client import BigCommerceApiClient

_logger = logging.getLogger(__name__)


class BigCommerceInventorySyncService:
    """Service layer for inventory sync between BigCommerce and Odoo."""

    def __init__(self, instance):
        self.instance = instance
        self.env = instance.env
        self.client = BigCommerceApiClient(instance)
        self.mapping_model = self.env["bigcommerce.field.mapping"]

    def import_inventory(self, limit=100):
        """Import inventory from BigCommerce into Odoo using GET-only API calls."""
        if self.instance.inventory_master != "bigcommerce":
            message = (
                "Inventory import skipped. Connector inventory master is '%s', expected 'bigcommerce'."
                % (self.instance.inventory_master or "")
            )
            self._log_failure(
                message=message,
                note="Manual inventory import aborted by inventory_master guard.",
            )
            return {
                "success": False,
                "message": message,
                "total": 0,
                "updated": 0,
                "failed": 0,
                "skipped": 0,
                "failed_items": [message],
            }
        return self._sync_inventory(limit=limit)

    def export_inventory(self, limit=100):
        """Export inventory from Odoo to BigCommerce using write endpoints."""
        limit = self._normalize_limit(limit=limit)
        if self.instance.inventory_master != "odoo":
            message = (
                "Inventory export skipped. Connector inventory master is '%s', expected 'odoo'."
                % (self.instance.inventory_master or "")
            )
            self._log_failure(
                message=message,
                note="Manual inventory export aborted by inventory_master guard.",
                request_method="PUT",
            )
            return {
                "success": False,
                "message": message,
                "total": 0,
                "exported": 0,
                "failed": 0,
                "skipped": 0,
                "failed_items": [message],
            }

        bindings = self._get_products_to_import(limit=limit)
        exported = failed = skipped = 0
        failed_items = []

        for binding in bindings:
            remote_id = binding.bigcommerce_variant_id or binding.bigcommerce_product_id
            product = binding.product_id or (
                binding.product_tmpl_id.product_variant_id if binding.product_tmpl_id else False
            )
            if not product:
                skipped += 1
                continue

            qty = self._get_available_qty(product)
            push_result = self._push_inventory_update(binding=binding, qty=qty)
            if push_result.get("success"):
                exported += 1
                binding.sudo().write({"bigcommerce_inventory_level": qty})
            else:
                failed += 1
                msg = push_result.get("message") or "Inventory export failed."
                failed_items.append("Remote %s: %s" % (remote_id or "N/A", msg))
                self._log_failure(
                    message=msg,
                    request_url=push_result.get("url"),
                    request_method=push_result.get("method") or "PUT",
                    response_status=push_result.get("status_code"),
                    response_body=push_result.get("response_body"),
                    resource_remote_id=remote_id,
                    note="Inventory export call failed.",
                )

        message = (
            "Inventory export completed. Total: %(total)s, Exported: %(exported)s, "
            "Failed: %(failed)s, Skipped: %(skipped)s."
        ) % {
            "total": len(bindings),
            "exported": exported,
            "failed": failed,
            "skipped": skipped,
        }

        if failed:
            self._log_failure(
                message=message,
                note="First failures: %s" % " | ".join(failed_items[:3]),
                request_method="PUT",
            )
        else:
            self._log_success(message=message, request_method="PUT")

        return {
            "success": failed == 0,
            "message": message,
            "total": len(bindings),
            "exported": exported,
            "failed": failed,
            "skipped": skipped,
            "failed_items": failed_items[:5],
        }

    def _sync_inventory(self, limit=100):
        """Core import loop for BigCommerce -> Odoo inventory sync."""
        limit = self._normalize_limit(limit=limit)
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "inventory_import",
                "resource_type": "inventory",
                "request_method": "GET",
                "request_url": self.client._build_url("/v3/catalog"),
                "status": "draft",
                "note": "Manual inventory import started (limit=%s)." % limit,
            }
        )

        bindings = self._get_products_to_import(limit=limit)
        if not bindings:
            message = "Inventory import completed. No bound products found for import."
            self._log_success(message=message)
            return {
                "success": True,
                "message": message,
                "total": 0,
                "updated": 0,
                "failed": 0,
                "skipped": 0,
                "failed_items": [],
            }

        location = self._get_inventory_location()
        if not location:
            message = "Inventory import failed. No internal stock location found."
            self._log_failure(message=message)
            return {
                "success": False,
                "message": message,
                "total": 0,
                "updated": 0,
                "failed": 1,
                "skipped": 0,
                "failed_items": [message],
            }

        updated = failed = skipped = 0
        failed_items = []
        for binding in bindings:
            remote_id = binding.bigcommerce_variant_id or binding.bigcommerce_product_id
            try:
                if binding.bigcommerce_variant_id:
                    result = self._sync_variant_inventory(binding=binding, location=location)
                else:
                    result = self._sync_product_inventory(binding=binding, location=location)
            except Exception as err:
                result = {
                    "status": "failed",
                    "message": str(err),
                    "remote_id": remote_id,
                }
                _logger.exception(
                    "Inventory import unhandled error instance_id=%s remote_id=%s",
                    self.instance.id,
                    remote_id,
                )

            status = result.get("status")
            message = result.get("message") or "Inventory sync processed."
            if status == "updated":
                updated += 1
                self._log_success(
                    message=message,
                    request_url=result.get("url"),
                    request_method=result.get("method") or "GET",
                    response_status=result.get("status_code"),
                    response_body=result.get("response_body"),
                    resource_remote_id=result.get("remote_id"),
                    note=result.get("note"),
                )
            elif status == "skipped":
                skipped += 1
                self._log_success(
                    message=message,
                    request_url=result.get("url"),
                    request_method=result.get("method") or "GET",
                    response_status=result.get("status_code"),
                    response_body=result.get("response_body"),
                    resource_remote_id=result.get("remote_id"),
                    note=result.get("note"),
                )
            else:
                failed += 1
                failed_items.append("Remote %s: %s" % (result.get("remote_id") or "N/A", message))
                self._log_failure(
                    message=message,
                    request_url=result.get("url"),
                    request_method=result.get("method") or "GET",
                    response_status=result.get("status_code"),
                    response_body=result.get("response_body"),
                    resource_remote_id=result.get("remote_id"),
                    note=result.get("note"),
                )

        total = len(bindings)
        summary = (
            "Inventory import completed. Total: %(total)s, Updated: %(updated)s, "
            "Failed: %(failed)s, Skipped: %(skipped)s."
        ) % {
            "total": total,
            "updated": updated,
            "failed": failed,
            "skipped": skipped,
        }
        if failed:
            self._log_failure(
                message=summary,
                note="First failures: %s" % " | ".join(failed_items[:3]),
            )
        else:
            self._log_success(message=summary)
        return {
            "success": failed == 0,
            "message": summary,
            "total": total,
            "updated": updated,
            "failed": failed,
            "skipped": skipped,
            "failed_items": failed_items[:5],
        }

    def _sync_product_inventory(self, binding, location):
        """Sync one simple product inventory from BigCommerce into Odoo."""
        remote_id = binding.bigcommerce_product_id
        product = binding.product_id or (
            binding.product_tmpl_id.product_variant_id if binding.product_tmpl_id else False
        )
        if not product:
            return {
                "status": "skipped",
                "message": "No linked Odoo product for binding.",
                "remote_id": remote_id,
                "note": "Missing product mapping.",
            }

        remote = self._fetch_remote_inventory(remote_product_id=remote_id)
        if not remote.get("success"):
            remote["status"] = "failed"
            remote["remote_id"] = remote_id
            return remote
        if remote.get("qty") not in (None, False):
            binding.sudo().write({"bigcommerce_inventory_level": remote.get("qty")})
        if not remote.get("tracked", True):
            remote["status"] = "skipped"
            remote["remote_id"] = remote_id
            remote["message"] = "Inventory tracking disabled in BigCommerce."
            return remote
        if remote.get("qty") in (None, False):
            remote["status"] = "skipped"
            remote["remote_id"] = remote_id
            remote["message"] = "Remote inventory level is empty."
            remote["note"] = "Inventory level missing in BigCommerce response."
            return remote

        apply_result = self._apply_inventory_adjustment(
            product=product,
            location=location,
            target_qty=remote.get("qty"),
        )
        if not apply_result.get("success"):
            remote["status"] = "failed"
            remote["remote_id"] = remote_id
            remote["message"] = apply_result.get("message")
            return remote

        remote["status"] = "updated"
        remote["remote_id"] = remote_id
        remote["message"] = "Inventory imported successfully."
        remote["note"] = "remote=%s previous=%s delta=%s" % (
            apply_result.get("target_qty"),
            apply_result.get("previous_qty"),
            apply_result.get("delta"),
        )
        return remote

    def _sync_variant_inventory(self, binding, location):
        """Sync one variant inventory from BigCommerce into Odoo."""
        remote_id = binding.bigcommerce_variant_id
        remote_product_id = binding.bigcommerce_product_id
        product = binding.product_id or (
            binding.product_tmpl_id.product_variant_id if binding.product_tmpl_id else False
        )
        if not product:
            return {
                "status": "skipped",
                "message": "No linked Odoo variant for binding.",
                "remote_id": remote_id,
                "note": "Missing variant product mapping.",
            }
        if not remote_product_id:
            return {
                "status": "skipped",
                "message": "Missing parent BigCommerce product id for variant binding.",
                "remote_id": remote_id,
                "note": "Cannot call variant GET endpoint without parent product id.",
            }

        remote = self._fetch_remote_inventory(
            remote_product_id=remote_product_id,
            remote_variant_id=remote_id,
        )
        if not remote.get("success"):
            remote["status"] = "failed"
            remote["remote_id"] = remote_id
            return remote
        if remote.get("qty") not in (None, False):
            binding.sudo().write({"bigcommerce_inventory_level": remote.get("qty")})
        if not remote.get("tracked", True):
            remote["status"] = "skipped"
            remote["remote_id"] = remote_id
            remote["message"] = "Variant inventory tracking disabled in BigCommerce."
            return remote
        if remote.get("qty") in (None, False):
            remote["status"] = "skipped"
            remote["remote_id"] = remote_id
            remote["message"] = "Remote inventory level is empty."
            remote["note"] = "Variant inventory level missing in BigCommerce response."
            return remote

        apply_result = self._apply_inventory_adjustment(
            product=product,
            location=location,
            target_qty=remote.get("qty"),
        )
        if not apply_result.get("success"):
            remote["status"] = "failed"
            remote["remote_id"] = remote_id
            remote["message"] = apply_result.get("message")
            return remote

        remote["status"] = "updated"
        remote["remote_id"] = remote_id
        remote["message"] = "Variant inventory imported successfully."
        remote["note"] = "remote=%s previous=%s delta=%s" % (
            apply_result.get("target_qty"),
            apply_result.get("previous_qty"),
            apply_result.get("delta"),
        )
        return remote

    def _fetch_remote_inventory(self, remote_product_id=False, remote_variant_id=False):
        """Fetch remote inventory using BigCommerce GET endpoints only."""
        if remote_variant_id:
            if not remote_product_id:
                return {
                    "success": False,
                    "message": "Missing parent product id for variant inventory fetch.",
                    "method": "GET",
                    "url": False,
                    "status_code": None,
                    "response_body": None,
                    "qty": None,
                    "tracked": False,
                    "note": "Variant endpoint requires product id + variant id.",
                }
            path = "/v3/catalog/products/%s/variants/%s" % (remote_product_id, remote_variant_id)
        else:
            if not remote_product_id:
                return {
                    "success": False,
                    "message": "Missing product id for inventory fetch.",
                    "method": "GET",
                    "url": False,
                    "status_code": None,
                    "response_body": None,
                    "qty": None,
                    "tracked": False,
                }
            path = "/v3/catalog/products/%s" % remote_product_id

        result = self.client.get(path, timeout=25, retries=2)
        _logger.info(
            "BigCommerce inventory fetch instance_id=%s remote_product_id=%s remote_variant_id=%s method=GET url=%s status=%s",
            self.instance.id,
            remote_product_id,
            remote_variant_id,
            result.get("url"),
            result.get("status_code"),
        )

        if not result.get("success"):
            return {
                "success": False,
                "message": result.get("message") or "BigCommerce inventory fetch failed.",
                "method": "GET",
                "url": result.get("url"),
                "status_code": result.get("status_code"),
                "response_body": result.get("response_body"),
                "qty": None,
                "tracked": False,
            }

        body = result.get("response_body") or {}
        data = body.get("data") if isinstance(body, dict) else body
        if not isinstance(data, dict):
            return {
                "success": False,
                "message": "BigCommerce inventory response format is invalid.",
                "method": "GET",
                "url": result.get("url"),
                "status_code": result.get("status_code"),
                "response_body": result.get("response_body"),
                "qty": None,
                "tracked": False,
            }

        tracking_raw = data.get("inventory_tracking")
        if remote_variant_id and tracking_raw in (None, False, ""):
            tracked = True
        else:
            tracked = str(tracking_raw or "").lower() not in ("none", "disabled", "")

        raw_qty = data.get("inventory_level")
        qty = None
        if raw_qty not in (None, False, ""):
            try:
                qty = int(float(raw_qty))
            except (TypeError, ValueError):
                qty = None

        return {
            "success": True,
            "message": "Inventory fetched.",
            "method": "GET",
            "url": result.get("url"),
            "status_code": result.get("status_code"),
            "response_body": result.get("response_body"),
            "qty": qty,
            "tracked": tracked,
        }

    def _apply_inventory_adjustment(self, product, location, target_qty):
        """Adjust Odoo stock at a location to match remote quantity."""
        if not location:
            return {"success": False, "message": "No internal stock location found."}
        if not product:
            return {"success": False, "message": "Missing Odoo product for inventory apply."}
        if not product.active:
            return {"success": False, "message": "Linked Odoo product is archived."}
        if target_qty in (None, False):
            return {"success": False, "message": "Remote inventory level is empty."}

        try:
            target_qty = int(max(0, target_qty))
        except (TypeError, ValueError):
            return {"success": False, "message": "Remote inventory level is invalid."}

        current_qty = product.with_context(location=location.id, active_test=False).qty_available
        current_qty = int(round(float(current_qty or 0.0)))
        delta = target_qty - current_qty
        if delta == 0:
            return {
                "success": True,
                "target_qty": target_qty,
                "previous_qty": current_qty,
                "delta": 0,
            }

        self.env["stock.quant"].sudo()._update_available_quantity(product, location, delta)
        return {
            "success": True,
            "target_qty": target_qty,
            "previous_qty": current_qty,
            "delta": delta,
        }

    def _prepare_inventory_payload(self, qty, product=False):
        """Build safe payload for inventory export updates."""
        payload = {"inventory_level": int(max(0, qty))}
        if product:
            mapping_payload = self.mapping_model._prepare_bigcommerce_payload_from_mapping(
                record=product,
                mapping_type="inventory",
                connector=self.instance,
                direction="export",
                raise_on_required=False,
            )
            payload = self.mapping_model._merge_payload_dict(payload, mapping_payload.get("payload") or {})
        return payload

    def _push_inventory_update(self, binding, qty):
        """Push inventory update to BigCommerce write endpoint (export path only)."""
        product = binding.product_id or (
            binding.product_tmpl_id.product_variant_id if binding.product_tmpl_id else False
        )
        payload = self._prepare_inventory_payload(qty=qty, product=product)
        if binding.bigcommerce_variant_id:
            if not binding.bigcommerce_product_id:
                return {
                    "success": False,
                    "message": "Missing parent product id for variant inventory update.",
                    "method": "PUT",
                    "url": False,
                    "status_code": None,
                    "response_body": None,
                }
            path = "/v3/catalog/products/%s/variants/%s" % (
                binding.bigcommerce_product_id,
                binding.bigcommerce_variant_id,
            )
        elif binding.bigcommerce_product_id:
            path = "/v3/catalog/products/%s" % binding.bigcommerce_product_id
        else:
            return {
                "success": False,
                "message": "Missing BigCommerce product/variant reference on binding.",
                "method": "PUT",
                "url": False,
                "status_code": None,
                "response_body": None,
            }

        result = self.client.put(path=path, payload=payload, timeout=25, retries=2)
        return {
            "success": bool(result.get("success")),
            "message": result.get("message"),
            "method": "PUT",
            "url": result.get("url"),
            "status_code": result.get("status_code"),
            "response_body": result.get("response_body"),
        }

    def _get_products_to_import(self, limit=100):
        """Return inventory-eligible bindings and avoid duplicate parent template rows."""
        all_bindings = self.env["bigcommerce.product.binding"].sudo().search(
            [
                ("instance_id", "=", self.instance.id),
                "|",
                ("bigcommerce_variant_id", "!=", False),
                ("bigcommerce_product_id", "!=", False),
            ],
        )
        if not all_bindings:
            return all_bindings

        variant_product_ids = {
            b.bigcommerce_product_id for b in all_bindings if b.bigcommerce_variant_id and b.bigcommerce_product_id
        }
        filtered = all_bindings.filtered(
            lambda b: not (
                b.bigcommerce_variant_id in (False, "", None)
                and b.bigcommerce_product_id in variant_product_ids
            )
        )
        return filtered[: self._normalize_limit(limit)]

    def _get_inventory_location(self):
        """Resolve target internal location for imported stock."""
        if self.instance.warehouse_id and self.instance.warehouse_id.lot_stock_id:
            return self.instance.warehouse_id.lot_stock_id
        return self.env["stock.location"].search(
            [
                ("usage", "=", "internal"),
                ("company_id", "in", [self.instance.company_id.id, False]),
            ],
            order="company_id desc, id asc",
            limit=1,
        )

    def _get_available_qty(self, product):
        """Get Odoo available quantity for export flows."""
        scoped = product.with_context(active_test=False)
        if self.instance.warehouse_id:
            scoped = scoped.with_context(warehouse=self.instance.warehouse_id.id)
        qty = scoped.free_qty if hasattr(scoped, "free_qty") else scoped.qty_available
        return int(max(0, round(float(qty or 0.0))))

    def _normalize_limit(self, limit):
        limit = int(limit or 100)
        if limit < 1:
            return 1
        if limit > 500:
            return 500
        return limit

    def _log_success(
        self,
        message,
        request_url=False,
        request_method="GET",
        response_status=False,
        response_body=None,
        resource_remote_id=False,
        note=False,
    ):
        """Create success log record."""
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "inventory_import" if request_method == "GET" else "inventory_export",
                "resource_type": "inventory",
                "resource_remote_id": resource_remote_id,
                "request_url": request_url,
                "request_method": request_method,
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
        request_method="GET",
        response_status=False,
        response_body=None,
        resource_remote_id=False,
        note=False,
    ):
        """Create failure log record."""
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "inventory_import" if request_method == "GET" else "inventory_export",
                "resource_type": "inventory",
                "resource_remote_id": resource_remote_id,
                "request_url": request_url,
                "request_method": request_method,
                "response_status": self._as_response_status(response_status),
                "response_body": self._as_response_body(response_body),
                "status": "failed",
                "error_message": message,
                "note": note,
            }
        )

    def _as_response_status(self, response_status):
        if response_status in (None, False, ""):
            return False
        return str(response_status)

    def _as_response_body(self, response_body):
        if response_body in (None, False, ""):
            return False
        return str(response_body)[:3000]
