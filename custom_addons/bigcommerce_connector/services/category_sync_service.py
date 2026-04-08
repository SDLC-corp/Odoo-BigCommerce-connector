# -*- coding: utf-8 -*-

import logging

from odoo import fields

from .api_client import BigCommerceApiClient

_logger = logging.getLogger(__name__)


class BigCommerceCategorySyncService:
    """Service layer for BigCommerce category import/export and mapping resolution."""

    def __init__(self, instance):
        self.instance = instance
        self.env = instance.env
        self.client = BigCommerceApiClient(instance)
        self.mapping_model = self.env["bigcommerce.field.mapping"]

    def import_categories(self, limit=200):
        """Import BigCommerce categories into Odoo product.category hierarchy."""
        limit = int(limit or 200)
        if limit < 1:
            limit = 1
        if limit > 1000:
            limit = 1000

        self._log(
            status="draft",
            operation_type="category_import",
            resource_type="category",
            note="Category import started (limit=%s)." % limit,
            request_method="GET",
            request_url=self.client._build_url("/v3/catalog/categories"),
        )

        result = self._fetch_categories(limit=limit)
        if not result.get("success"):
            message = result.get("message") or "Unable to fetch BigCommerce categories."
            self._log(
                status="failed",
                operation_type="category_import",
                resource_type="category",
                message=message,
                request_method="GET",
                request_url=result.get("url"),
                response_status=result.get("status_code"),
                response_body=result.get("response_body"),
            )
            return {
                "success": False,
                "message": message,
                "total": 0,
                "created": 0,
                "updated": 0,
                "failed": 0,
            }

        categories = result.get("items", [])[:limit]
        if not categories:
            message = "Category import completed. No categories returned."
            self._log(
                status="success",
                operation_type="category_import",
                resource_type="category",
                note=message,
                request_method="GET",
            )
            return {
                "success": True,
                "message": message,
                "total": 0,
                "created": 0,
                "updated": 0,
                "failed": 0,
            }

        created = 0
        updated = 0
        failed = 0

        categories_by_id = {}
        for category in categories:
            category_id = self._as_remote_id(category)
            if category_id:
                categories_by_id[category_id] = category

        ordered = sorted(
            categories,
            key=lambda item: int(item.get("parent_id") or 0),
        )

        for bc_category in ordered:
            try:
                category, was_created = self._create_or_update_category(
                    bc_category=bc_category,
                    categories_by_id=categories_by_id,
                )
                self._create_or_update_binding(category, bc_category)
                if was_created:
                    created += 1
                else:
                    updated += 1
            except Exception as err:
                failed += 1
                _logger.exception(
                    "Category import failed for instance_id=%s remote_category=%s",
                    self.instance.id,
                    self._as_remote_id(bc_category),
                )
                self._log(
                    status="failed",
                    operation_type="category_import",
                    resource_type="category",
                    message=str(err),
                    resource_remote_id=self._as_remote_id(bc_category),
                    note="Category '%s' import failed." % (bc_category.get("name") or "Unknown"),
                )

        total = len(categories)
        success = failed == 0
        message = (
            "Category import completed. Total: %(total)s, Created: %(created)s, "
            "Updated: %(updated)s, Failed: %(failed)s."
        ) % {
            "total": total,
            "created": created,
            "updated": updated,
            "failed": failed,
        }
        self._log(
            status="success" if success else "failed",
            operation_type="category_import",
            resource_type="category",
            message=message,
            note=message,
        )
        return {
            "success": success,
            "message": message,
            "total": total,
            "created": created,
            "updated": updated,
            "failed": failed,
        }

    def _fetch_categories(self, limit):
        """Fetch categories with production-safe retries for BigCommerce query quirks."""
        page_limit = min(limit, 250)
        max_pages = max(1, (limit + page_limit - 1) // page_limit)

        # BigCommerce category API can be strict about query value formats.
        result = self.client.get_paginated(
            "/v3/catalog/categories",
            params={"is_visible": "true"},
            limit=page_limit,
            max_pages=max_pages,
        )
        if result.get("success"):
            return result

        status_code = int(result.get("status_code") or 0)
        message = (result.get("message") or "").lower()
        if status_code == 422 and "is_visible" in message:
            return self.client.get_paginated(
                "/v3/catalog/categories",
                params={"is_visible": "1"},
                limit=page_limit,
                max_pages=max_pages,
            )

        return result

    def export_categories(self, limit=200):
        """Export Odoo categories to BigCommerce and maintain bindings."""
        limit = int(limit or 200)
        categories = self.env["product.category"].search([], order="parent_path,id", limit=limit)
        if not categories:
            return {
                "success": True,
                "message": "No categories found to export.",
                "total": 0,
                "exported": 0,
                "failed": 0,
            }

        exported = 0
        failed = 0
        for category in categories:
            try:
                binding = self.env["bigcommerce.category.binding"].sudo().search(
                    [
                        ("instance_id", "=", self.instance.id),
                        ("category_id", "=", category.id),
                    ],
                    limit=1,
                )
                payload = self._prepare_export_payload(category)
                if binding and binding.bigcommerce_category_id:
                    response = self.client.put(
                        "/v3/catalog/categories/%s" % binding.bigcommerce_category_id,
                        payload=payload,
                    )
                    remote_id = binding.bigcommerce_category_id
                else:
                    response = self.client.post("/v3/catalog/categories", payload=payload)
                    remote_id = self._extract_created_id(response.get("response_body"))

                if not response.get("success"):
                    failed += 1
                    self._log(
                        status="failed",
                        operation_type="category_export",
                        resource_type="category",
                        message=response.get("message"),
                        response_status=response.get("status_code"),
                        response_body=response.get("response_body"),
                        resource_remote_id=remote_id,
                        note="Category export failed for %s." % category.display_name,
                    )
                    continue

                if remote_id:
                    self._create_or_update_binding(
                        category,
                        {"id": remote_id, "name": category.name, "parent_id": False},
                    )
                exported += 1
            except Exception as err:
                failed += 1
                self._log(
                    status="failed",
                    operation_type="category_export",
                    resource_type="category",
                    message=str(err),
                    note="Category export failed for %s." % category.display_name,
                )

        total = len(categories)
        success = failed == 0
        message = "Category export completed. Total: %s, Exported: %s, Failed: %s." % (
            total,
            exported,
            failed,
        )
        self._log(
            status="success" if success else "failed",
            operation_type="category_export",
            resource_type="category",
            note=message,
            message=message,
        )
        return {
            "success": success,
            "message": message,
            "total": total,
            "exported": exported,
            "failed": failed,
        }

    def map_bc_categories_to_odoo_ids(self, bc_category_ids):
        """Return mapped Odoo category ids for BigCommerce category ids."""
        bc_category_ids = [str(v) for v in (bc_category_ids or []) if v not in (None, False, "")]
        if not bc_category_ids:
            return []
        bindings = self.env["bigcommerce.category.binding"].sudo().search(
            [
                ("instance_id", "=", self.instance.id),
                ("bigcommerce_category_id", "in", bc_category_ids),
                ("category_id", "!=", False),
            ]
        )
        return bindings.mapped("category_id.id")

    def _create_or_update_category(self, bc_category, categories_by_id):
        remote_id = self._as_remote_id(bc_category)
        if not remote_id:
            raise ValueError("BigCommerce category id is missing.")

        binding = self.env["bigcommerce.category.binding"].sudo().search(
            [
                ("instance_id", "=", self.instance.id),
                ("bigcommerce_category_id", "=", remote_id),
            ],
            limit=1,
        )
        category = binding.category_id if binding else self.env["product.category"]

        vals = {"name": (bc_category.get("name") or "").strip() or "BigCommerce Category"}
        mapping_result = self.mapping_model._prepare_odoo_vals_from_mapping(
            payload=bc_category,
            mapping_type="category",
            connector=self.instance,
            direction="import",
            raise_on_required=False,
        )
        vals.update(mapping_result.get("vals") or {})
        parent_id = bc_category.get("parent_id")
        if parent_id not in (None, False, "", 0, "0"):
            parent_binding = self.env["bigcommerce.category.binding"].sudo().search(
                [
                    ("instance_id", "=", self.instance.id),
                    ("bigcommerce_category_id", "=", str(parent_id)),
                ],
                limit=1,
            )
            if not parent_binding:
                parent_source = categories_by_id.get(str(parent_id))
                if parent_source:
                    parent_category, _ = self._create_or_update_category(parent_source, categories_by_id)
                    self._create_or_update_binding(parent_category, parent_source)
                    vals["parent_id"] = parent_category.id
            elif parent_binding.category_id:
                vals["parent_id"] = parent_binding.category_id.id

        if category:
            category.write(vals)
            return category, False

        created = self.env["product.category"].create(vals)
        return created, True

    def _create_or_update_binding(self, category, bc_category):
        remote_id = self._as_remote_id(bc_category)
        if not remote_id:
            raise ValueError("BigCommerce category id is missing.")

        binding_vals = {
            "instance_id": self.instance.id,
            "category_id": category.id,
            "bigcommerce_category_id": remote_id,
            "bigcommerce_parent_category_id": (
                str(bc_category.get("parent_id"))
                if bc_category.get("parent_id") not in (None, False, "")
                else False
            ),
            "bigcommerce_category_name": (bc_category.get("name") or "").strip() or category.name,
            "sync_state": "synced",
            "last_synced_at": fields.Datetime.now(),
            "last_error": False,
        }

        binding_model = self.env["bigcommerce.category.binding"].sudo()
        binding = binding_model.search(
            [
                ("instance_id", "=", self.instance.id),
                ("bigcommerce_category_id", "=", remote_id),
            ],
            limit=1,
        )
        if binding:
            binding.write(binding_vals)
            return binding
        return binding_model.create(binding_vals)

    def _prepare_export_payload(self, category):
        payload = {"name": category.name}
        mapping_payload = self.mapping_model._prepare_bigcommerce_payload_from_mapping(
            record=category,
            mapping_type="category",
            connector=self.instance,
            direction="export",
            raise_on_required=False,
        )
        payload = self.mapping_model._merge_payload_dict(payload, mapping_payload.get("payload") or {})
        if category.parent_id:
            parent_binding = self.env["bigcommerce.category.binding"].sudo().search(
                [
                    ("instance_id", "=", self.instance.id),
                    ("category_id", "=", category.parent_id.id),
                ],
                limit=1,
            )
            if parent_binding and parent_binding.bigcommerce_category_id:
                payload["parent_id"] = int(parent_binding.bigcommerce_category_id)
            else:
                payload["parent_id"] = 0
        return payload

    def _extract_created_id(self, response_body):
        if isinstance(response_body, dict):
            data = response_body.get("data")
            if isinstance(data, dict) and data.get("id") not in (None, False, ""):
                return str(data.get("id"))
            if response_body.get("id") not in (None, False, ""):
                return str(response_body.get("id"))
        return False

    def _as_remote_id(self, bc_category):
        if not isinstance(bc_category, dict):
            return False
        value = bc_category.get("id")
        if value in (None, False, ""):
            return False
        return str(value)

    def _log(
        self,
        status,
        operation_type,
        resource_type,
        message=False,
        request_method=False,
        request_url=False,
        response_status=False,
        response_body=False,
        resource_remote_id=False,
        note=False,
    ):
        if self.env.context.get("bigcommerce_skip_category_logs"):
            return
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": operation_type,
                "resource_type": resource_type,
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
