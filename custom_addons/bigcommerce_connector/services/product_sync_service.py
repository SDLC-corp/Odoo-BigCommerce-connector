# -*- coding: utf-8 -*-

import base64
import logging

import requests
from odoo import fields

from .api_client import BigCommerceApiClient
from .category_sync_service import BigCommerceCategorySyncService
from .inventory_sync_service import BigCommerceInventorySyncService

_logger = logging.getLogger(__name__)


class BigCommerceProductSyncService:
    """Product import/export service with category, variant, and image support."""

    def __init__(self, instance):
        self.instance = instance
        self.env = instance.env
        self.client = BigCommerceApiClient(instance)
        self.mapping_model = self.env["bigcommerce.field.mapping"]
        self.category_service = BigCommerceCategorySyncService(instance)
        self.inventory_service = BigCommerceInventorySyncService(instance)
        self._template_binding_cache = {}
        self._variant_binding_cache = {}
        self._product_by_sku_cache = {}
        self._inventory_location = None

    def import_products(self, limit=50):
        if limit in (None, False, "", 0, "0"):
            limit = None
        else:
            limit = max(1, min(5000, int(limit or 50)))
        self._log(
            status="draft",
            note="Product import started (limit=%s)." % (limit or "all"),
            request_method="GET",
            request_url=self.client._build_url("/v3/catalog/products"),
        )
        # Ensure category bindings exist before product category mapping.
        BigCommerceCategorySyncService(
            self.instance.with_context(bigcommerce_skip_category_logs=True)
        ).import_categories(limit=500)
        created = updated = failed = 0
        failed_items = []
        processed = 0
        page_limit = min(100, limit) if limit else 100
        batch_size = self._resolve_batch_size(limit)

        for page_result in self.client.iter_paginated(
            "/v3/catalog/products",
            params={"include": "variants,images,options"},
            limit=page_limit,
            max_pages=None,
            data_key="data",
        ):
            if not page_result.get("success"):
                failed_items.append(page_result.get("message") or "Product fetch failed.")
                message = "Product import interrupted. Processed: %s, Failed fetch page: %s." % (
                    processed,
                    page_result.get("page"),
                )
                self._log(
                    status="failed",
                    message=message,
                    request_method="GET",
                    request_url=page_result.get("url"),
                    response_status=page_result.get("status_code"),
                    response_body=page_result.get("response_body"),
                )
                return {
                    "success": False,
                    "message": message,
                    "total": processed,
                    "created": created,
                    "updated": updated,
                    "failed": failed,
                    "failed_items": failed_items[:8],
                }

            page_products = page_result.get("items") or []
            if not page_products:
                break

            if limit and processed + len(page_products) > limit:
                page_products = page_products[: max(0, limit - processed)]
            if not page_products:
                break

            self._prime_product_caches(page_products)

            for batch in self._iter_batches(page_products, batch_size):
                for bc_product in batch:
                    try:
                        template, was_created = self._create_or_update_product(bc_product)
                        self._create_or_update_template_binding(template, bc_product)
                        self._sync_variants(template, bc_product)
                        self._sync_images(template, bc_product)
                        if was_created:
                            created += 1
                        else:
                            updated += 1
                    except Exception as err:
                        failed += 1
                        failed_items.append("%s: %s" % (bc_product.get("name") or "Unknown", str(err)))
                        _logger.exception("Product import failed for remote id %s", bc_product.get("id"))

            processed += len(page_products)
            if limit and processed >= limit:
                break

        message = "Product import completed. Total: %s, Created: %s, Updated: %s, Failed: %s." % (
            processed,
            created,
            updated,
            failed,
        )
        self._log(
            status="success" if failed == 0 else "failed",
            message=message,
            note=message,
            request_method="GET",
            request_url=self.client._build_url("/v3/catalog/products"),
        )
        return {
            "success": failed == 0,
            "message": message,
            "total": processed,
            "created": created,
            "updated": updated,
            "failed": failed,
            "failed_items": failed_items[:8],
        }

    def import_product_by_id(self, product_id):
        result = self.client.get("/v3/catalog/products/%s" % product_id, params={"include": "variants,images,options"})
        if not result.get("success"):
            return {"success": False, "message": result.get("message")}
        body = result.get("response_body") or {}
        product = body.get("data") if isinstance(body, dict) else False
        if not isinstance(product, dict):
            return {"success": False, "message": "Product payload format is invalid."}
        template, _ = self._create_or_update_product(product)
        self._create_or_update_template_binding(template, product)
        self._sync_variants(template, product)
        self._sync_images(template, product)
        return {"success": True, "message": "Product synced."}

    def export_products(self, limit=50):
        templates = self.env["product.template"].search([("sale_ok", "=", True)], limit=max(1, int(limit or 50)))
        exported = failed = 0
        failed_items = []
        for template in templates:
            push = self._push_product(template)
            if push.get("success"):
                exported += 1
            else:
                failed += 1
                failed_items.append("%s: %s" % (template.display_name, push.get("message")))
        return {
            "success": failed == 0,
            "message": "Product export completed. Total: %s, Exported: %s, Failed: %s." % (len(templates), exported, failed),
            "total": len(templates),
            "exported": exported,
            "failed": failed,
            "failed_items": failed_items[:8],
        }

    def _create_or_update_product(self, bc_product):
        remote_id = self._as_id(bc_product.get("id"))
        binding = self.env["bigcommerce.product.binding"].sudo()
        if remote_id:
            if remote_id in self._template_binding_cache:
                binding = self._template_binding_cache.get(remote_id)
            else:
                binding = self.env["bigcommerce.product.binding"].sudo().search(
                    [
                        ("instance_id", "=", self.instance.id),
                        ("bigcommerce_product_id", "=", remote_id),
                        ("bigcommerce_variant_id", "=", False),
                    ],
                    limit=1,
                )
                self._template_binding_cache[remote_id] = binding
        template = binding.product_tmpl_id if binding else self.env["product.template"]
        sku = self._clean_text(bc_product.get("sku"), max_len=255)
        if not template and sku:
            variant = self._product_by_sku_cache.get(sku)
            if variant is None:
                variant = self.env["product.product"].search([("default_code", "=", sku)], limit=1)
                self._product_by_sku_cache[sku] = variant or self.env["product.product"]
            template = variant.product_tmpl_id if variant else self.env["product.template"]
        vals = {
            "name": self._clean_text(bc_product.get("name"), max_len=255) or "BigCommerce Product",
            "sale_ok": True,
        }
        bc_product_type = str(bc_product.get("type") or "").strip().lower()
        if bc_product_type == "digital":
            vals["type"] = "service"
        elif bc_product_type in ("", "physical"):
            vals["type"] = "consu"
        if sku:
            vals["default_code"] = sku
        if bc_product.get("description"):
            vals["description_sale"] = self._clean_text(bc_product.get("description"))
        if bc_product.get("price") not in (None, ""):
            vals["list_price"] = self._safe_float(bc_product.get("price"), default=0.0)
        if "is_visible" in bc_product:
            vals["active"] = bool(bc_product.get("is_visible"))
        category_ids = self.category_service.map_bc_categories_to_odoo_ids(bc_product.get("categories") or [])
        if category_ids:
            vals["categ_id"] = category_ids[0]

        mapping_result = self.mapping_model._prepare_odoo_vals_from_mapping(
            payload=bc_product,
            mapping_type="product",
            connector=self.instance,
            direction="import",
            raise_on_required=False,
        )
        vals.update(mapping_result.get("vals") or {})
        vals = self._sanitize_template_vals(vals=vals, template=template)
        if template:
            template.write(vals)
            self._sync_template_inventory_level(template=template, bc_product=bc_product)
            return template, False
        created_template = self.env["product.template"].create(vals)
        self._sync_template_inventory_level(template=created_template, bc_product=bc_product)
        return created_template, True

    def _sync_variants(self, template, bc_product):
        variants = bc_product.get("variants") or []
        if not variants:
            return
        for bc_variant in variants:
            variant = self._find_variant(template, bc_variant)
            if not variant:
                variant = template.product_variant_id
            sku = (bc_variant.get("sku") or "").strip()
            write_vals = {}
            if sku:
                write_vals["default_code"] = sku
                self._product_by_sku_cache[sku] = variant
            if bc_variant.get("upc"):
                write_vals["barcode"] = (bc_variant.get("upc") or "").strip()
            if bc_variant.get("weight") not in (None, ""):
                write_vals["weight"] = self._safe_float(bc_variant.get("weight"), default=0.0)
            if write_vals:
                variant.write(write_vals)
            self._sync_variant_inventory_level(variant=variant, bc_variant=bc_variant)
            self._create_or_update_variant_binding(template, variant, bc_variant)

    def _sync_images(self, template, bc_product):
        images = bc_product.get("images") or []
        if not images:
            return
        primary = images[0]
        for image in images:
            if image.get("is_thumbnail"):
                primary = image
                break
        url = self._image_url(primary)
        if url:
            image_data = self._fetch_image(url)
            if image_data:
                template.image_1920 = image_data

    def _find_variant(self, template, bc_variant):
        remote_variant_id = self._as_id(bc_variant.get("id"))
        if remote_variant_id:
            binding = self._variant_binding_cache.get(remote_variant_id)
            if remote_variant_id not in self._variant_binding_cache:
                binding = self.env["bigcommerce.product.binding"].sudo().search(
                    [("instance_id", "=", self.instance.id), ("bigcommerce_variant_id", "=", remote_variant_id)],
                    limit=1,
                )
                self._variant_binding_cache[remote_variant_id] = binding
            if binding and binding.product_id:
                return binding.product_id.with_env(self.env)
        sku = (bc_variant.get("sku") or "").strip()
        if sku:
            if sku in self._product_by_sku_cache:
                return self._product_by_sku_cache.get(sku)
            product = self.env["product.product"].search([("default_code", "=", sku)], limit=1)
            self._product_by_sku_cache[sku] = product or self.env["product.product"]
            return product
        return self.env["product.product"]

    def _create_or_update_template_binding(self, template, bc_product):
        remote_id = self._as_id(bc_product.get("id"))
        if not remote_id:
            return
        inventory_level = self._safe_float(bc_product.get("inventory_level"), default=None)
        vals = {
            "instance_id": self.instance.id,
            "product_tmpl_id": template.id,
            "product_id": template.product_variant_id.id if template.product_variant_id else False,
            "bigcommerce_product_id": remote_id,
            "bigcommerce_variant_id": False,
            "bigcommerce_sku": (bc_product.get("sku") or "").strip() or False,
            "bigcommerce_inventory_level": inventory_level,
            "bigcommerce_is_featured": bool(bc_product.get("is_featured")),
            "bigcommerce_is_visible": bool(bc_product.get("is_visible", True)),
            "sync_state": "synced",
            "last_synced_at": fields.Datetime.now(),
            "last_error": False,
        }
        model = self.env["bigcommerce.product.binding"].sudo()
        rec = model.search([("instance_id", "=", self.instance.id), ("bigcommerce_product_id", "=", remote_id), ("bigcommerce_variant_id", "=", False)], limit=1)
        if rec:
            rec.write(vals)
            self._template_binding_cache[remote_id] = rec
        else:
            rec = model.create(vals)
            self._template_binding_cache[remote_id] = rec

    def _create_or_update_variant_binding(self, template, variant, bc_variant):
        remote_variant_id = self._as_id(bc_variant.get("id"))
        if not remote_variant_id:
            return
        inventory_level = self._safe_float(bc_variant.get("inventory_level"), default=None)
        vals = {
            "instance_id": self.instance.id,
            "product_tmpl_id": template.id,
            "product_id": variant.id,
            "bigcommerce_product_id": self._as_id(bc_variant.get("product_id")) or False,
            "bigcommerce_variant_id": remote_variant_id,
            "bigcommerce_sku": (bc_variant.get("sku") or "").strip() or False,
            "bigcommerce_inventory_level": inventory_level,
            "sync_state": "synced",
            "last_synced_at": fields.Datetime.now(),
            "last_error": False,
        }
        model = self.env["bigcommerce.product.binding"].sudo()
        rec = model.search([("instance_id", "=", self.instance.id), ("bigcommerce_variant_id", "=", remote_variant_id)], limit=1)
        if rec:
            rec.write(vals)
            self._variant_binding_cache[remote_variant_id] = rec
        else:
            rec = model.create(vals)
            self._variant_binding_cache[remote_variant_id] = rec

    def _push_product(self, template):
        binding = self.env["bigcommerce.product.binding"].sudo().search(
            [("instance_id", "=", self.instance.id), ("product_tmpl_id", "=", template.id), ("bigcommerce_variant_id", "=", False)],
            limit=1,
        )
        payload = {
            "name": template.name,
            "type": "physical",
            "price": template.list_price,
            "description": template.description_sale or "",
            "is_visible": bool(template.active),
        }
        mapping_payload = self.mapping_model._prepare_bigcommerce_payload_from_mapping(
            record=template,
            mapping_type="product",
            connector=self.instance,
            direction="export",
            raise_on_required=False,
        )
        payload = self.mapping_model._merge_payload_dict(payload, mapping_payload.get("payload") or {})
        result = self.client.put("/v3/catalog/products/%s" % binding.bigcommerce_product_id, payload=payload) if binding and binding.bigcommerce_product_id else self.client.post("/v3/catalog/products", payload=payload)
        if not result.get("success"):
            return {"success": False, "message": result.get("message"), "status_code": result.get("status_code"), "response_body": result.get("response_body")}
        if not (binding and binding.bigcommerce_product_id):
            body = result.get("response_body") or {}
            data = body.get("data") if isinstance(body, dict) else {}
            if isinstance(data, dict) and data.get("id"):
                self._create_or_update_template_binding(template, {"id": data.get("id"), "sku": template.default_code})
        return {"success": True}

    def _image_url(self, image):
        for key in ("url_zoom", "url_standard", "image_url", "url_thumbnail"):
            value = (image.get(key) or "").strip() if isinstance(image, dict) else ""
            if value:
                return value
        return False

    def _fetch_image(self, url):
        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                return base64.b64encode(response.content)
        except requests.RequestException:
            return False
        return False

    def _resolve_batch_size(self, limit):
        return max(20, min(100, int(limit or 50)))

    def _iter_batches(self, records, batch_size):
        records = records or []
        for index in range(0, len(records), batch_size):
            yield records[index : index + batch_size]

    def _prime_product_caches(self, products):
        remote_product_ids = [
            self._as_id(row.get("id"))
            for row in (products or [])
            if self._as_id(row.get("id"))
        ]
        variant_ids = []
        skus = []
        for row in products or []:
            sku = self._clean_text(row.get("sku"), max_len=255)
            if sku:
                skus.append(sku)
            for variant in row.get("variants") or []:
                variant_id = self._as_id(variant.get("id"))
                if variant_id:
                    variant_ids.append(variant_id)
                variant_sku = self._clean_text(variant.get("sku"), max_len=255)
                if variant_sku:
                    skus.append(variant_sku)

        missing_remote = [rid for rid in remote_product_ids if rid not in self._template_binding_cache]
        if missing_remote:
            bindings = self.env["bigcommerce.product.binding"].sudo().search(
                [
                    ("instance_id", "=", self.instance.id),
                    ("bigcommerce_product_id", "in", missing_remote),
                    ("bigcommerce_variant_id", "=", False),
                ]
            )
            by_remote = {binding.bigcommerce_product_id: binding for binding in bindings}
            for rid in missing_remote:
                self._template_binding_cache[rid] = by_remote.get(rid) or False

        missing_variants = [rid for rid in variant_ids if rid not in self._variant_binding_cache]
        if missing_variants:
            bindings = self.env["bigcommerce.product.binding"].sudo().search(
                [
                    ("instance_id", "=", self.instance.id),
                    ("bigcommerce_variant_id", "in", missing_variants),
                ]
            )
            by_remote = {binding.bigcommerce_variant_id: binding for binding in bindings}
            for rid in missing_variants:
                self._variant_binding_cache[rid] = by_remote.get(rid) or False

        unique_skus = list({sku for sku in skus if sku and sku not in self._product_by_sku_cache})
        if unique_skus:
            products_by_sku = self.env["product.product"].search([("default_code", "in", unique_skus)])
            sku_map = {product.default_code: product for product in products_by_sku if product.default_code}
            for sku in unique_skus:
                self._product_by_sku_cache[sku] = sku_map.get(sku) or self.env["product.product"]

    def _sanitize_template_vals(self, vals, template=False):
        cleaned = {}
        product_template = self.env["product.template"]
        field_defs = product_template._fields
        record_for_cache = template if template else product_template
        for field_name, value in (vals or {}).items():
            field = field_defs.get(field_name)
            if not field:
                _logger.warning(
                    "Skipping unknown mapped product field instance_id=%s field=%s",
                    self.instance.id,
                    field_name,
                )
                continue
            if field_name == "type" and value in ("product", "stockable", "physical"):
                value = "consu"
            try:
                field.convert_to_cache(value, record_for_cache)
            except Exception as err:
                _logger.warning(
                    "Skipping invalid mapped value for product field instance_id=%s field=%s value=%s err=%s",
                    self.instance.id,
                    field_name,
                    str(value)[:120],
                    str(err),
                )
                continue
            cleaned[field_name] = value
        return cleaned

    def _sync_template_inventory_level(self, template, bc_product):
        if self.instance.inventory_master != "bigcommerce":
            return
        if not template or not template.product_variant_id:
            return
        qty = self._safe_int(bc_product.get("inventory_level"), default=None)
        if qty is None:
            return
        location = self._get_inventory_location()
        if not location:
            return
        result = self.inventory_service._apply_inventory_adjustment(
            product=template.product_variant_id,
            location=location,
            target_qty=qty,
        )
        if not result.get("success"):
            _logger.warning(
                "Template inventory apply skipped instance_id=%s template_id=%s reason=%s",
                self.instance.id,
                template.id,
                result.get("message"),
            )

    def _sync_variant_inventory_level(self, variant, bc_variant):
        if self.instance.inventory_master != "bigcommerce":
            return
        if not variant:
            return
        qty = self._safe_int(bc_variant.get("inventory_level"), default=None)
        if qty is None:
            return
        location = self._get_inventory_location()
        if not location:
            return
        result = self.inventory_service._apply_inventory_adjustment(
            product=variant,
            location=location,
            target_qty=qty,
        )
        if not result.get("success"):
            _logger.warning(
                "Variant inventory apply skipped instance_id=%s product_id=%s reason=%s",
                self.instance.id,
                variant.id,
                result.get("message"),
            )

    def _get_inventory_location(self):
        if self._inventory_location is None:
            self._inventory_location = self.inventory_service._get_inventory_location() or False
        return self._inventory_location or False

    def _clean_text(self, value, max_len=False):
        if value in (None, False):
            return False
        text = str(value).strip()
        if not text:
            return False
        if max_len:
            return text[: int(max_len)]
        return text

    def _safe_float(self, value, default=0.0):
        if value in (None, False, ""):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _safe_int(self, value, default=0):
        if value in (None, False, ""):
            return default
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    def _as_id(self, value):
        return str(value) if value not in (None, False, "") else False

    def _log(
        self,
        status,
        message=False,
        request_method=False,
        request_url=False,
        response_status=False,
        response_body=False,
        resource_remote_id=False,
        note=False,
    ):
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "product_import",
                "resource_type": "product",
                "resource_remote_id": resource_remote_id,
                "request_method": request_method,
                "request_url": request_url,
                "response_status": str(response_status) if response_status else False,
                "response_body": str(response_body)[:3000] if response_body else False,
                "status": status,
                "error_message": message if status == "failed" else False,
                "note": note or message,
            }
        )
