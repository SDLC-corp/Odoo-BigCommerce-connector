from odoo import models, fields, api, _
from odoo.exceptions import UserError
import requests
import logging
import json

_logger = logging.getLogger(__name__)

class ProductCategory(models.Model):
    _inherit = "product.category"

    shopify_collection_id = fields.Char(readonly=True)
    shopify_instance_id = fields.Many2one(
        "shopify.instance",
        string="Shopify Instance",
        readonly=False,
        copy=False,
    )

    # ============================================================
    # SHOPIFY → ODOO (IMPORT CATEGORY USING MAPPING) UNCHANGED
    # ============================================================
    @api.model
    def map_shopify_category_to_odoo(self, data, instance=None):
        mapping_model = self.env["shopify.field.mapping"]
        vals = {}

        domain = [
            ("mapping_type", "=", "category"),
            ("active", "=", True),
        ]
        if instance:
            domain.append(("instance_id", "=", instance.id))

        mappings = mapping_model.search(domain)

        for m in mappings:
            odoo_field = m.odoo_field.name if m.odoo_field else False
            shopify_field = m.shopify_field_id.code if m.shopify_field_id else False

            if not odoo_field or not shopify_field:
                continue

            value = data.get(shopify_field)
            if value in (None, False, ""):
                continue

            if odoo_field in self._fields:
                vals[odoo_field] = value

        if "name" not in vals:
            vals["name"] = data.get("title") or f"Shopify Category {data.get('id')}"

        vals["shopify_collection_id"] = str(data.get("id"))
        if instance:
            vals["shopify_instance_id"] = instance.id
        return vals

    # ============================================================
    # BUILD SHOPIFY PAYLOAD (ODOO → SHOPIFY USING MAPPING)
    # ============================================================
    def _prepare_shopify_category_payload(self):
        self.ensure_one()

        instance = self.shopify_instance_id or self.env["shopify.instance"].search(
            [("active", "=", True)], limit=1
        )
        if not instance:
            raise UserError(_("No Shopify instance configured."))

        try:
            payload, debug_lines = self.env["shopify.field.mapping"].build_payload(
                "category", self, instance
            )
            if debug_lines:
                _logger.info("CATEGORY MAPPING DEBUG: %s", debug_lines)
        except UserError:
            # Fall back to minimal payload when no mapping is configured
            payload = {"custom_collection": {"title": self.name or "Odoo Category"}}
        else:
            collection = payload.setdefault("custom_collection", {})
            # Always send a human-readable title (prefer the full path)
            human_title = (
                getattr(self, "complete_name", False)
                or self.display_name
                or self.name
                or "Odoo Category"
            )
            collection["title"] = human_title

        return payload

    # ------------------------------------------------------------
    # ✅ INTERNAL HELPER: CATEGORY SYNC REPORT
    # ------------------------------------------------------------
    def _create_category_sync_report(self, instance, payload, response_text, status, source_action):
        Report = self.env["shopify.sync.report"]
        Line = self.env["shopify.sync.report.line"]

        report = Report.create({
            "instance_id": instance.id,
            "sync_type": "category",
            "total_records": 1,
            "success_count": 1 if status == "success" else 0,
            "error_count": 1 if status == "error" else 0,
        })

        Line.create({
            "report_id": report.id,
            "record_type": "category",
            "source_action": source_action,
            "shopify_id": self.shopify_collection_id or "",
            "name": self.name,
            "status": status,
            "error_message": response_text if status == "error" else False,
        })

    # ============================================================
    # CREATE CATEGORY IN SHOPIFY OLD LOGIC + REPORT
    # ============================================================
    def action_create_shopify_category(self):
        self.ensure_one()

        if self.shopify_collection_id:
            raise UserError(_("This category already exists on Shopify."))

        instance = self.shopify_instance_id or self.env["shopify.instance"].search(
            [("active", "=", True)], limit=1
        )
        if not instance:
            raise UserError(_("No Shopify instance configured."))
        self.shopify_instance_id = instance.id

        payload = self._prepare_shopify_category_payload()

        url = (
            f"{instance.shop_url.rstrip('/')}/admin/api/"
            f"{instance.api_version}/custom_collections.json"
        )

        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

        _logger.info("Create Shopify Category Payload: %s", payload)

        response = requests.post(url, json=payload, headers=headers)
        if response.status_code not in (200, 201):
            self._create_category_sync_report(
                instance, payload, response.text, "error", "category_create"
            )
            raise UserError(f"Shopify Error: {response.text}")

        data = response.json().get("custom_collection", {})
        self.shopify_collection_id = str(data.get("id"))

        self._create_category_sync_report(
            instance, payload, response.text, "success", "category_create"
        )

        return {
            "effect": {
                "fadeout": "slow",
                "message": _("Category successfully created in Shopify!"),
                "type": "rainbow_man",
            }
        }

    # ============================================================
    # UPDATE CATEGORY IN SHOPIFY  OLD LOGIC + REPORT
    # ============================================================
    def action_update_shopify_category(self):
        self.ensure_one()

        if not self.shopify_collection_id:
            raise UserError(_("This category is not created in Shopify."))

        instance = self.shopify_instance_id or self.env["shopify.instance"].search(
            [("active", "=", True)], limit=1
        )
        if not instance:
            raise UserError(_("No Shopify instance configured."))

        payload = self._prepare_shopify_category_payload()
        payload["custom_collection"]["id"] = int(self.shopify_collection_id)

        url = (
            f"{instance.shop_url.rstrip('/')}/admin/api/"
            f"{instance.api_version}/custom_collections/"
            f"{self.shopify_collection_id}.json"
        )

        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

        _logger.info("Update Shopify Category Payload: %s", payload)

        response = requests.put(url, json=payload, headers=headers)
        if response.status_code not in (200, 201):
            self._create_category_sync_report(
                instance, payload, response.text, "error", "category_update"
            )
            raise UserError(f"Shopify Update Error: {response.text}")

        self._create_category_sync_report(
            instance, payload, response.text, "success", "category_update"
        )

        return {
            "effect": {
                "fadeout": "slow",
                "message": _("Category updated successfully on Shopify!"),
                "type": "rainbow_man",
            }
        }