from odoo import models, fields, api, _
from odoo.exceptions import UserError
import requests
import json
import logging
import base64

_logger = logging.getLogger(__name__)

SHOPIFY_VARIANT_FIELDS = {
    "price",
    "compare_at_price",
    "sku",
    "barcode",
    "inventory_quantity",
    "weight",
    "option1",
    "option2",
    "option3",
}
class ProductTemplate(models.Model):
    _inherit = "product.template"

    shopify_product_id = fields.Char(
        string="Shopify Product ID",
        copy=False,
        index=True,
    )
    shopify_available_qty = fields.Float(
        string="Shopify Available Qty",
        copy=False,
        help="Last known available quantity from Shopify (sum of variants).",
    )
    shopify_push_qty = fields.Float(
        string="Shopify Qty to Replace",
        copy=False,
        required=True,
        default=0.0,
        help="Quantity to send to Shopify when creating/updating the product.",
    )

    def _compute_push_qty_int(self):    
        """Return the integer quantity we intend to push to Shopify."""
        self.ensure_one()
        qty = self.shopify_push_qty
        if qty is None and hasattr(self, "qty_available"):
            qty = self.qty_available
        try:
            return int(round(qty or 0.0))
        except Exception:
            return 0
    shopify_instance_id = fields.Many2one(
        "shopify.instance",
        string="Shopify Instance",
        readonly=False,
        copy=False,
    )

    #  Optional store fields (mirror support)
    x_shopify_vendor = fields.Char("Shopify Vendor")
    x_shopify_tags = fields.Char("Shopify Tags")
    x_shopify_status = fields.Char("Shopify Status")
    x_shopify_product_type = fields.Char("Shopify Product Type")
    x_shopify_compare_at_price = fields.Float("Shopify Compare At Price")

    # ==================================================
    # SHOPIFY -> ODOO (IMPORT / MAP SHOPIFY TO ODOO)
    # ==================================================
    @api.model
    def map_shopify_product_to_odoo(self, data, instance=None):
        # Always resolve an instance to tag the product (fallback to any active)
        if not instance:
            instance = (
                self.env["shopify.instance"]
                .sudo()
                .search([("active", "=", True)], limit=1)
            )

        name = data.get("title") or data.get("handle") or f"Shopify Product #{data.get('id')}"

        vals = {
            "name": name,
            "shopify_product_id": str(data.get("id")),
        }
        if instance:
            vals["shopify_instance_id"] = instance.id

        # main product image (first image from Shopify)
        images = data.get("images") or []
        if images:
            src = (images[0] or {}).get("src")
            if src:
                try:
                    resp = requests.get(src, timeout=10)
                    if resp.status_code == 200:
                        vals["image_1920"] = base64.b64encode(resp.content)
                except Exception as e:
                    _logger.warning("Failed to fetch Shopify image %s: %s", src, e)

        Mapping = self.env["shopify.field.mapping"]

        # Prefer mappings for this instance; fallback to any active product mappings
        map_domain = [("mapping_type", "=", "product"), ("active", "=", True)]
        if instance:
            map_domain.append(("instance_id", "=", instance.id))
        mappings = Mapping.search(map_domain)
        if not mappings:
            mappings = Mapping.search([("mapping_type", "=", "product"), ("active", "=", True)])

        variant = data.get("variants", [{}])[0]  # 1st variant
        variant_targets = {"default_code", "barcode", "weight"}
        variant_vals = {}

        for m in mappings:
            if not m.odoo_field or not m.shopify_field_id:
                continue

            odoo_field = (m.odoo_field.name or "").strip()
            shopify_code = (m.shopify_field_id.code or "").strip()

            if not odoo_field or not shopify_code:
                continue

            is_variant_code = False
            clean_code = shopify_code
            if shopify_code.startswith("variant."):
                clean_code = shopify_code[len("variant."):]
                is_variant_code = True

            # Variant field is read from the first variant in Shopify payload
            if clean_code in SHOPIFY_VARIANT_FIELDS or is_variant_code:
                value = variant.get(clean_code)
            else:
                value = data.get(clean_code)

            if value in (None, False, ""):
                continue

            # Route variant targets to the variant (template lacks these fields)
            if odoo_field not in self._fields and odoo_field in variant_targets:
                variant_vals[odoo_field] = value
                continue

            if odoo_field in self._fields:
                field = self._fields[odoo_field]
                try:
                    if field.type in ("float", "monetary"):
                        value = float(value)
                except Exception:
                    pass

                vals[odoo_field] = value

        if variant_vals:
            vals["_variant_vals"] = variant_vals

        return vals

    # ==================================================
    # MAPPING AUDIT HELPERS (NEW)
    # ==================================================
    def _safe_get_audit_model(self):
        """Return audit model if installed, else False (no crash)."""
        try:
            return self.env["shopify.mapping.audit"].sudo()
        except Exception:
            return False

    def _snapshot_fields(self, field_names):
        """Read current values of given fields from record (safe)."""
        self.ensure_one()
        snap = {}
        for f in set(field_names or []):
            if not f or f not in self._fields:
                continue
            v = self[f]
            try:
                if hasattr(v, "id"):
                    v = v.id
            except Exception:
                pass
            snap[f] = v
        return snap

    def _diff_snapshot(self, before, after):
        """Return audit lines for Odoo side changes (old->new)."""
        lines = []
        keys = set((before or {}).keys()) | set((after or {}).keys())
        for k in sorted(keys):
            old = (before or {}).get(k)
            new = (after or {}).get(k)
            if str(old) != str(new):
                lines.append({
                    "odoo_field": k,
                    "shopify_field": "",
                    "scope": "product",
                    "old_value": str(old),
                    "new_value": str(new),
                    "applied": True,
                    "note": "mirrored_in_odoo",
                })
        return lines

    def _create_mapping_audit(
        self,
        instance,
        direction,
        source_action,
        payload_after=None,
        status="success",
        error_message=None,
        response_text=None,
        change_lines=None,
    ):
        Audit = self._safe_get_audit_model()
        if not Audit:
            return False

        # mapping engine audit lines (if set by mapping model context)
        ctx_lines = self.env.context.get("_last_mapping_audit_lines") or []
        all_lines = (change_lines or []) + ctx_lines

        return Audit.create_audit(
            instance=instance,
            mapping_type="product",
            direction=direction,
            record=self,
            source_action=source_action,
            payload_before=None,
            payload_after=payload_after,
            response_text=response_text,
            status=status,
            error_message=error_message,
            change_lines=all_lines,
        )

    # ==================================================
    # ODOO -> SHOPIFY PAYLOAD BUILDER
    # ==================================================
    def _build_shopify_payload(self):
        self.ensure_one()

        Mapping = self.env["shopify.field.mapping"]

        instance = self.env["shopify.instance"].search([("active", "=", True)], limit=1)
        if not instance:
            raise UserError(_("No active Shopify Instance found."))

        # ✅ snapshot BEFORE (mirror fields)
        mirror_fields = [
            "name", "description_sale",
            "x_shopify_vendor", "x_shopify_tags",
            "x_shopify_status", "x_shopify_product_type",
            "x_shopify_compare_at_price",
        ]
        before_snap = self._snapshot_fields(mirror_fields)

        before_variant_snap = {}
        if self.product_variant_id:
            before_variant_snap = self.product_variant_id.read(["default_code", "barcode", "weight"])[0]

        # ✅ build payload (safe unpack)
        result = Mapping.build_payload("product", self, instance)
        if isinstance(result, tuple):
            wrapped_payload = result[0]
            debug_lines = result[1] if len(result) > 1 else []
        else:
            wrapped_payload = result
            debug_lines = []

        if not isinstance(wrapped_payload, dict) or "product" not in wrapped_payload:
            raise UserError(_("Invalid payload returned from mapping engine."))

        if not wrapped_payload.get("product"):
            raise UserError(_("No mapped fields found for Shopify product."))

        # Attach main image as base64 to Shopify payload (first position)
        img = self.image_1920
        if img:
            attachment = img.decode() if isinstance(img, (bytes, bytearray)) else img
            if attachment:
                images = wrapped_payload["product"].get("images") or []
                images.insert(
                    0,
                    {
                        "attachment": attachment,
                        "filename": f"{self.name or 'product'}.png",
                    },
                )
                wrapped_payload["product"]["images"] = images

        _logger.info("FINAL SHOPIFY PAYLOAD -> %s", wrapped_payload)
        if debug_lines:
            _logger.info("MAPPING DEBUG LINES ->\n%s", "\n".join(debug_lines))

        # ✅ snapshot AFTER (if mapping mirrored into Odoo)
        after_snap = self._snapshot_fields(mirror_fields)

        after_variant_snap = {}
        if self.product_variant_id:
            after_variant_snap = self.product_variant_id.read(["default_code", "barcode", "weight"])[0]

        change_lines = self._diff_snapshot(before_snap, after_snap)

        # variant diffs
        for k in ["default_code", "barcode", "weight"]:
            if str(before_variant_snap.get(k)) != str(after_variant_snap.get(k)):
                change_lines.append({
                    "odoo_field": f"product_variant_id.{k}",
                    "shopify_field": "",
                    "scope": "variant",
                    "old_value": str(before_variant_snap.get(k)),
                    "new_value": str(after_variant_snap.get(k)),
                    "applied": True,
                    "note": "mirrored_in_odoo",
                })

        # ✅ keep these lines in context for create/update audit
        self = self.with_context(_last_mapping_mirror_lines=change_lines)

        return wrapped_payload

    # ==================================================
    # CREATE SYNC REPORT ENTRY (your old code kept)
    # ==================================================
    def _create_sync_report(self, instance, body, resp_text, status, source_action):
        Report = self.env["shopify.sync.report"]
        Line = self.env["shopify.sync.report.line"]

        report = Report.create({
            "instance_id": instance.id,
            "sync_type": "product",
            "start_time": fields.Datetime.now(),
            "end_time": fields.Datetime.now(),
            "total_records": 1,
            "success_count": 1 if status == "success" else 0,
            "error_count": 1 if status == "error" else 0,
        })

        Line.create({
            "report_id": report.id,
            "record_type": "product",
            "source_action": source_action,
            "shopify_id": self.shopify_product_id or "",
            "name": self.name,
            "status": status,
            "error_message": resp_text if status == "error" else False,
        })

    # ==================================================
    # CREATE PRODUCT IN SHOPIFY
    # ==================================================
    def action_create_in_shopify(self):
        self.ensure_one()

        if self.shopify_product_id:
            raise UserError(_("Product already exists on Shopify"))

        instance = self.shopify_instance_id or self.env["shopify.instance"].search(
            [("active", "=", True)], limit=1
        )
        if not instance:
            raise UserError(_("No active Shopify instance found"))
        # persist the chosen instance on the product for traceability
        self.shopify_instance_id = instance.id

        body = self._build_shopify_payload()
        mirror_lines = self.env.context.get("_last_mapping_mirror_lines") or []

        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/products.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

        _logger.info("CREATE PRODUCT ON SHOPIFY -> %s", body)
        resp = requests.post(url, headers=headers, data=json.dumps(body))

        if resp.status_code not in (200, 201):
            self._create_sync_report(instance, body, resp.text, "error", "product_create")

            # ✅ mapping audit error (if model installed)
            self._create_mapping_audit(
                instance=instance,
                direction="odoo_to_shopify",
                source_action="product_create",
                payload_after=body,
                status="error",
                error_message=resp.text,
                response_text=resp.text,
                change_lines=mirror_lines,
            )
            raise UserError(resp.text)

        data = (resp.json() or {}).get("product") or {}
        if data.get("id"):
            self.shopify_product_id = str(data.get("id"))

        self._create_sync_report(instance, body, resp.text, "success", "product_create")

        # ✅ mapping audit success
        self._create_mapping_audit(
            instance=instance,
            direction="odoo_to_shopify",
            source_action="product_create",
            payload_after=body,
            status="success",
            error_message=None,
            response_text=resp.text,
            change_lines=mirror_lines,
        )
        # Reflect pushed quantity locally
        self.shopify_available_qty = self._compute_push_qty_int()
        return {
            "effect": {
                "fadeout": "slow",
                "message": _("Product created in Shopify"),
                "type": "rainbow_man",
            }
        }

    # ==================================================
    # UPDATE PRODUCT IN SHOPIFY
    # ==================================================
    def action_update_in_shopify(self):
        self.ensure_one()

        if not self.shopify_product_id:
            raise UserError(_("Product not linked with Shopify"))

        instance = self.shopify_instance_id or self.env["shopify.instance"].search(
            [("active", "=", True)], limit=1
        )
        if not instance:
            raise UserError(_("No active Shopify instance found"))

        body = self._build_shopify_payload()
        mirror_lines = self.env.context.get("_last_mapping_mirror_lines") or []

        body["product"]["id"] = int(self.shopify_product_id)

        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/products/{self.shopify_product_id}.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

        _logger.info("UPDATE PRODUCT ON SHOPIFY -> %s", body)
        resp = requests.put(url, headers=headers, data=json.dumps(body))

        if resp.status_code not in (200, 201):
            self._create_sync_report(instance, body, resp.text, "error", "product_update")

            # ✅ mapping audit error
            self._create_mapping_audit(
                instance=instance,
                direction="odoo_to_shopify",
                source_action="product_update",
                payload_after=body,
                status="error",
                error_message=resp.text,
                response_text=resp.text,
                change_lines=mirror_lines,
            )
            raise UserError(_("Shopify update failed: %s") % resp.text)

        self._create_sync_report(instance, body, resp.text, "success", "product_update")

        # ✅ mapping audit success
        self._create_mapping_audit(
            instance=instance,
            direction="odoo_to_shopify",
            source_action="product_update",
            payload_after=body,
            status="success",
            error_message=None,
            response_text=resp.text,
            change_lines=mirror_lines,
        )
        # Reflect pushed quantity locally
        self.shopify_available_qty = self._compute_push_qty_int()
        return {
            "effect": {
                "fadeout": "slow",
                "message": _("Product updated in Shopify"),
                "type": "rainbow_man",
            }
        }


class ProductProduct(models.Model):
    _inherit = "product.product"

    shopify_instance_id = fields.Many2one(
        related="product_tmpl_id.shopify_instance_id",
        string="Shopify Instance",
        store=True,
        readonly=True,
    )
