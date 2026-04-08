from odoo import models, fields, api, _
from odoo.exceptions import UserError
from odoo.models import BaseModel
from pprint import pformat
import ast
import logging
import requests

_logger = logging.getLogger(__name__)


# =====================================================================
# SHOPIFY AVAILABLE FIELD MODEL
# =====================================================================
class ShopifyAvailableField(models.Model):
    _name = "shopify.available.field"
    _description = "Available Shopify Field"
    
    name = fields.Char(required=True)
    code = fields.Char(required=True)
    
    instance_id = fields.Many2one(
        "shopify.instance",
        required=True,
        ondelete="cascade",
    )

    mapping_type = fields.Selection(
        [
            ("product", "Product"),
            ("customer", "Customer"),
            ("order", "Order"),
            ("category", "Category"),
        ],
        required=True,
        default=lambda self: self.env.context.get("default_mapping_type", "product"),
    )

    _sql_constraints = [
        (
            "uniq_shopify_field",
            "unique(code, instance_id, mapping_type)",
            "This Shopify field already exists!",
        )
    ]


# =====================================================================
# MAIN MAPPING MODEL
# =====================================================================
class ShopifyFieldMapping(models.Model):
    _name = "shopify.field.mapping"
    _description = "Shopify Field Mapping"
    _order = "mapping_type, id"

    mapping_type = fields.Selection(
        [
            ("product", "Product"),
            ("customer", "Customer"),
            ("order", "Order"),
            ("category", "Category"),
        ],
        required=True,
        default="product",
    )

    instance_id = fields.Many2one(
        "shopify.instance",
        required=True,
        ondelete="cascade",
    )

    odoo_model = fields.Char(compute="_compute_odoo_model", store=True)
    odoo_model_id = fields.Many2one(
        "ir.model", compute="_compute_odoo_model", store=True, readonly=True
    )

    odoo_field = fields.Many2one(
        "ir.model.fields",
        string="Odoo Field",
        required=False,
        ondelete="set null",
        domain="[('model_id', '=', odoo_model_id)]",
    )

    shopify_field_id = fields.Many2one(
        "shopify.available.field",
        string="Shopify Field",
        required=True,
        domain="[('instance_id','=',instance_id), ('mapping_type','=',mapping_type)]",
    )

    active = fields.Boolean(default=True)

    # ✅ when mapping applied, mirror value into Odoo too
    mirror_in_odoo = fields.Boolean(string="Mirror in Odoo", default=True)

    # =====================================================================
    # COMPUTE ODOO MODEL
    # =====================================================================
    @api.depends("mapping_type")
    def _compute_odoo_model(self):
        Model = self.env["ir.model"].sudo()
        FieldModel = self.env["shopify.available.field"].sudo()

        for rec in self:
            already = 0
            model_name = {
                "product": "product.template",
                "customer": "res.partner",
                "order": "sale.order",
                "category": "product.category",
            }.get(rec.mapping_type)

            rec.odoo_model = model_name
            rec.odoo_model_id = False

            if model_name:
                rec.odoo_model_id = Model.search([("model", "=", model_name)], limit=1)

            # ✅ keep auto-load behavior, but with guard
            if rec.instance_id and rec.mapping_type:
                already = FieldModel.search_count(
                    [
                        ("instance_id", "=", rec.instance_id.id),
                        ("mapping_type", "=", rec.mapping_type),
                    ]
                )
            if not already:
                rec._sync_shopify_fields()

    @api.onchange("mapping_type", "instance_id")
    def _onchange_sync_shopify_fields(self):
        """Auto-load Shopify fields when selecting mapping type/instance."""
        for rec in self:
            if rec.instance_id and rec.mapping_type:
                rec._sync_shopify_fields()

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        for rec in records:
            if rec.instance_id and rec.mapping_type:
                rec._sync_shopify_fields()
        return records

    # =========================================================
    # FETCH SHOPIFY FIELDS
    # =========================================================
    def _fetch_shopify_fields(self):
        self.ensure_one()
        if not self.instance_id:
            return []

        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.instance_id.access_token,
        }

        root_key_map = {
            "product": "products",
            "customer": "customers",
            "order": "orders",
            "category": "custom_collections",
        }

        endpoint_map = {
            "product": f"/admin/api/{self.instance_id.api_version}/products.json?limit=50",
            "customer": f"/admin/api/{self.instance_id.api_version}/customers.json?limit=50",
            "order": f"/admin/api/{self.instance_id.api_version}/orders.json?limit=50&status=any",
            "category": f"/admin/api/{self.instance_id.api_version}/custom_collections.json?limit=50",
        }

        endpoint = endpoint_map.get(self.mapping_type)
        root_key = root_key_map.get(self.mapping_type)
        if not endpoint or not root_key:
            return []

        url = self.instance_id.shop_url.rstrip("/") + endpoint

        try:
            res = requests.get(url, headers=headers, timeout=30)
            res.raise_for_status()
            data = res.json() or {}
        except Exception as e:
            _logger.error("Shopify API Error: %s", e)
            return []

        records = data.get(root_key, [])
        if not records:
            _logger.warning(
                "No records found for %s. Response keys=%s",
                self.mapping_type,
                list(data.keys()),
            )
            return []

        def score(rec):
            s = len(rec.keys())
            if rec.get("variants"):
                s += 100
            if rec.get("images"):
                s += 20
            return s

        sample = max(records, key=score)

        fields_list = []
        for key in sample.keys():
            fields_list.append((key, key.replace("_", " ").title()))

        if self.mapping_type == "product" and sample.get("variants"):
            variant = sample["variants"][0]
            for key in variant.keys():
                fields_list.append((f"variant.{key}", f"Variant → {key}"))

        return fields_list

    # =========================================================
    # SYNC SHOPIFY FIELDS (CREATE + UPDATE NAME)
    # =========================================================
    def _sync_shopify_fields(self):
        Field = self.env["shopify.available.field"].sudo()

        for rec in self:
            fetched = rec._fetch_shopify_fields()
            _logger.warning(
                "FETCHED FIELDS COUNT=%s first10=%s", len(fetched), fetched[:10]
            )

            if not fetched:
                continue

            existing_recs = Field.search(
                [
                    ("instance_id", "=", rec.instance_id.id),
                    ("mapping_type", "=", rec.mapping_type),
                ]
            )
            existing_map = {r.code: r for r in existing_recs}

            for code, label in fetched:
                if code in existing_map:
                    if existing_map[code].name != label:
                        existing_map[code].name = label
                else:
                    Field.create(
                        {
                            "name": label,
                            "code": code,
                            "instance_id": rec.instance_id.id,
                            "mapping_type": rec.mapping_type,
                        }
                    )

    # =========================================================
    # MANUAL SYNC BUTTON
    # =========================================================
    def action_sync_shopify_fields(self):
        """Fetch latest Shopify fields for this mapping type/instance."""
        for rec in self:
            rec._sync_shopify_fields()
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("Shopify fields refreshed"),
                "message": _("Dropdowns updated with the latest Shopify fields."),
                "type": "success",
            },
        }

    # =========================================================
    # Shopify-key -> Odoo target field (Mirror save)
    # =========================================================
    def _shopify_to_odoo_target_field(self, mapping_type, shopify_code):
        if mapping_type == "product":
            base_map = {
                "title": "name",
                "body_html": "description_sale",
                "vendor": "x_shopify_vendor",
                "tags": "x_shopify_tags",
                "product_type": "x_shopify_product_type",
                "status": "x_shopify_status",
            }
            if shopify_code.startswith("variant."):
                v = shopify_code.split(".", 1)[1]
                vmap = {
                    "sku": "default_code",
                    "barcode": "barcode",
                    "weight": "weight",
                    "price": "list_price",
                    "compare_at_price": "x_shopify_compare_at_price",
                }
                return vmap.get(v)
            return base_map.get(shopify_code)

        if mapping_type == "category":
            if shopify_code in ("title", "name"):
                return "name"

        if mapping_type == "customer":
            base_map = {
                "email": "email",
                "phone": "phone",
                "first_name": "name",
                "last_name": "name",
            }
            return base_map.get(shopify_code)

        if mapping_type == "order":
            return {"note": "note"}.get(shopify_code)

        return None

    def _normalize_for_odoo_field(self, record, field_name, value):
        if field_name not in record._fields:
            return False, "target_field_not_found"

        fld = record._fields[field_name]
        if fld.readonly or fld.compute:
            return False, "target_field_readonly_or_compute"

        try:
            if fld.type in ("float", "monetary"):
                return float(value), ""
            if fld.type in ("integer",):
                return int(float(value)), ""
            if fld.type in ("boolean",):
                return bool(value), ""
            return str(value) if value is not None else False, ""
        except Exception:
            return False, "type_conversion_failed"

    def _mirror_value_in_odoo(self, record, mapping_type, shopify_code, value):
        if self.env.context.get("skip_mapping_mirror"):
            return

        target = self._shopify_to_odoo_target_field(mapping_type, shopify_code)
        if not target:
            return

        # variant targets: write on product.product
        if shopify_code.startswith("variant.") and target in (
            "default_code",
            "barcode",
            "weight",
        ):
            if record._name == "product.template" and record.product_variant_id:
                v, reason = self._normalize_for_odoo_field(
                    record.product_variant_id, target, value
                )
                if reason:
                    return
                record.product_variant_id.with_context(skip_mapping_mirror=True).write(
                    {target: v}
                )
            return

        v, reason = self._normalize_for_odoo_field(record, target, value)
        if reason:
            return

        record.with_context(skip_mapping_mirror=True).write({target: v})

    # =========================================================
    # BUILD FINAL PAYLOAD (ODOO → SHOPIFY)
    # =========================================================
    @api.model
    def build_payload(self, mapping_type, record, instance):
        # Use sudo to bypass record rules during sync/push operations
        record = record.sudo()
        mappings = self.search(
            [
                ("mapping_type", "=", mapping_type),
                ("instance_id", "=", instance.id),
                ("active", "=", True),
            ]
        )

        base_vals = {}
        variant_vals = {}
        debug_lines = []

        for m in mappings:
            if not m.odoo_field or not m.shopify_field_id:
                continue

            odoo_field_name = m.odoo_field.name
            shopify_code = m.shopify_field_id.code

            # Skip read-only Shopify fields that cannot be sent back
            if mapping_type == "category" and shopify_code in {
                "id",
                "admin_graphql_api_id",
                "handle",
                "updated_at",
                "published_at",
                "published_scope",
            }:
                continue

            if odoo_field_name not in record._fields:
                continue

            value = record[odoo_field_name]
            if isinstance(value, BaseModel):
                value = value.id

            # Keep category title/body human-readable; avoid pushing internal IDs.
            if mapping_type == "category" and shopify_code == "title":
                # If the mapped value looks like an ID, replace it with a readable name now.
                if isinstance(value, (int, float)) or str(value).strip() == str(record.id):
                    try:
                        name_pairs = record.name_get()
                        human = name_pairs[0][1] if name_pairs else False
                    except Exception:
                        human = False
                    value = (
                        human
                        or getattr(record, "complete_name", False)
                        or record.display_name
                        or record.name
                        or _("Untitled Category")
                    )
            if mapping_type == "category" and shopify_code == "body_html":
                # Drop meaningless numeric body_html values
                if isinstance(value, (int, float)) or str(value).strip() == str(record.id):
                    continue

            if (
                value is None
                or value is False
                or (isinstance(value, str) and not value.strip())
            ):
                continue

            # force string for these keys
            if shopify_code in ("title", "vendor", "tags", "body_html"):
                value = str(value)

            if mapping_type == "product" and shopify_code.startswith("variant."):
                key = shopify_code.split(".", 1)[1]
                variant_vals[key] = value
                debug_lines.append(f"[Variant] {key} <= {odoo_field_name} : {value!r}")
            else:
                base_vals[shopify_code] = value
                debug_lines.append(
                    f"[{mapping_type}] {shopify_code} <= {odoo_field_name} : {value!r}"
                )

            # ✅ mirror into odoo
            if m.mirror_in_odoo:
                m._mirror_value_in_odoo(record, mapping_type, shopify_code, value)

        # ---------- Fallbacks per mapping type (when no values mapped) ----------
        if not base_vals and not variant_vals:
            if mapping_type == "product":
                variant_rec = (
                    record.product_variant_id
                    if hasattr(record, "product_variant_id")
                    else record
                )
                base_vals = {
                    "title": record.name or _("Untitled Product"),
                }
                if getattr(record, "description_sale", False):
                    base_vals["body_html"] = record.description_sale
                if getattr(record, "categ_id", False) and record.categ_id:
                    base_vals["product_type"] = record.categ_id.display_name

                variant_vals = {
                    "sku": getattr(variant_rec, "default_code", False) or "",
                    "price": getattr(variant_rec, "list_price", 0.0) or 0.0,
                }
                if getattr(variant_rec, "barcode", False):
                    variant_vals["barcode"] = variant_rec.barcode
                if getattr(variant_rec, "weight", False):
                    variant_vals["weight"] = variant_rec.weight

                _logger.warning(
                    "SHOPIFY MAPPING FALLBACK used minimal payload because mapping produced empty payload."
                )
            elif mapping_type == "customer":
                # Minimal customer payload from partner name/email/phone
                name = (record.name or "").strip() or _("Customer")
                first, last = (name, "") if " " not in name else name.split(" ", 1)
                base_vals = {"first_name": first, "last_name": last}
                if getattr(record, "email", False):
                    base_vals["email"] = record.email
                if getattr(record, "phone", False):
                    base_vals["phone"] = record.phone
                _logger.warning(
                    "SHOPIFY CUSTOMER MAPPING FALLBACK applied minimal payload because mapping produced empty payload."
                )
            elif mapping_type == "category":
                base_vals = {"title": record.name or record.display_name or _("Untitled Category")}
                _logger.warning(
                    "SHOPIFY CATEGORY MAPPING FALLBACK applied minimal payload because mapping produced empty payload."
                )
            else:
                raise UserError(
                    _("Payload empty. Please check mapping & record values.")
                )

        # Normalize category title to a readable path (always prefer the Odoo category name)
        if mapping_type == "category":
            human = None
            try:
                name_pairs = record.name_get()
                if name_pairs:
                    human = name_pairs[0][1]
            except Exception:
                human = None
            if not human:
                human = (
                    getattr(record, "complete_name", False)
                    or record.display_name
                    or record.name
                    or _("Untitled Category")
                )
            base_vals["title"] = str(human)
            # Drop body_html if it equals the record id
            if "body_html" in base_vals:
                if (
                    isinstance(base_vals["body_html"], (int, float))
                    or str(base_vals["body_html"]).strip() == str(record.id)
                ):
                    base_vals.pop("body_html", None)

        # Ensure customer names are present even if only one side mapped
        if mapping_type == "customer":
            name = (record.name or "").strip()
            if name:
                if "first_name" not in base_vals and "last_name" not in base_vals:
                    first, last = (name, "") if " " not in name else name.split(" ", 1)
                    base_vals.setdefault("first_name", first)
                    base_vals.setdefault("last_name", last)
                else:
                    # Fill missing side from name if one part is missing
                    if "first_name" not in base_vals:
                        base_vals["first_name"] = name
                    if "last_name" not in base_vals:
                        base_vals["last_name"] = ""

        # Ensure we have an inventory quantity to push for products
        if mapping_type == "product":
            qty = getattr(record, "shopify_push_qty", None)
            if qty is None and hasattr(record, "qty_available"):
                qty = record.qty_available
            try:
                qty_val = float(qty) if qty is not None else None
            except Exception:
                qty_val = None
            if qty_val is not None:
                qty_int = int(round(qty_val))
                variant_vals.setdefault("inventory_management", "shopify")
                variant_vals.setdefault("inventory_quantity", qty_int)

        payload = base_vals.copy()
        if mapping_type == "product" and variant_vals:
            payload["variants"] = [variant_vals]

        wrapper_key = {
            "product": "product",
            "customer": "customer",
            "order": "order",
            "category": "custom_collection",
        }.get(mapping_type, mapping_type)

        _logger.warning(
            "SHOPIFY MAPPING DEBUG mapping_type=%s base=%s variant=%s record_name=%s record_display=%s record_complete=%s",
            mapping_type,
            base_vals,
            variant_vals,
            getattr(record, "name", False),
            getattr(record, "display_name", False),
            getattr(record, "complete_name", False),
        )
        return {wrapper_key: payload}, debug_lines

    # =========================================================
    # TEST MAPPING BUTTON
    # =========================================================
    def action_test_mapping(self):
        """Allow testing one or multiple mappings without requiring selection."""
        mappings = self

        if not mappings:
            domain = []
            ctx_domain = self.env.context.get("active_domain")
            if ctx_domain:
                try:
                    domain = ast.literal_eval(ctx_domain) if isinstance(ctx_domain, str) else ctx_domain
                except Exception:
                    _logger.exception("Failed parsing active_domain=%s", ctx_domain)
                    domain = []
            mappings = self.search(domain)

        if not mappings:
            raise UserError(_("No mapping records found to test."))

        action = None
        for rec in mappings:
            action = rec._action_test_mapping_single()
        return action

    def _action_test_mapping_single(self):
        self.ensure_one()

        model_map = {
            "product": "product.template",
            "customer": "res.partner",
            "order": "sale.order",
            "category": "product.category",
        }
        model = model_map[self.mapping_type]

        record = self.env[model].search([], order="id desc", limit=1)
        if not record:
            raise UserError(_("No %s record found to test.") % model)

        # -------------------------------
        # BEFORE snapshot (for audit diff)
        # -------------------------------
        mappings = self.search(
            [
                ("mapping_type", "=", self.mapping_type),
                ("instance_id", "=", self.instance_id.id),
                ("active", "=", True),
            ]
        )

        before = []
        for m in mappings:
            if not (m.odoo_field and m.shopify_field_id and m.mirror_in_odoo):
                continue

            shopify_code = m.shopify_field_id.code
            target = m._shopify_to_odoo_target_field(self.mapping_type, shopify_code)
            if not target:
                continue

            target_rec = record
            if (
                shopify_code.startswith("variant.")
                and record._name == "product.template"
            ):
                target_rec = record.product_variant_id or record

            if target in target_rec._fields:
                before.append((m, shopify_code, target_rec, target, target_rec[target]))

        # -------------------------------
        # BUILD PAYLOAD (mirrors to Odoo)
        # -------------------------------
        wrapped_payload, debug_lines = self.build_payload(
            self.mapping_type, record, self.instance_id
        )

        # -------------------------------
        # AFTER snapshot → audit diff
        # -------------------------------
        change_lines = []
        for m, shopify_code, target_rec, target, old_val in before:
            new_val = target_rec[target]
            if str(old_val) != str(new_val):
                change_lines.append(
                    {
                        "odoo_field": m.odoo_field.name,
                        "shopify_field": shopify_code,
                        "scope": (
                            "variant"
                            if shopify_code.startswith("variant.")
                            else "product"
                        ),
                        "old_value": str(old_val),
                        "new_value": str(new_val),
                        "applied": True,
                        "note": f"mirrored_to_odoo:{target_rec._name}.{target}",
                    }
                )

        # -------------------------------
        # CREATE AUDIT (no rollback)
        # -------------------------------
        try:
            self.env["shopify.mapping.audit"].sudo().create_audit(
                instance=self.instance_id,
                mapping_type=self.mapping_type,
                direction="odoo_to_shopify",
                record=record,
                source_action="test_mapping",
                payload_before=None,
                payload_after=pformat(wrapped_payload),
                response_text=None,
                status="success",
                error_message=None,
                change_lines=change_lines,
            )
        except Exception:
            _logger.exception("Audit create failed")

        # -------------------------------
        # POPUP RESULT
        # -------------------------------
        msg = (
            f"✅ Mapping Run Successfully\n\n"
            f"Record: (ID {record.id})\n\n"
            f"Payload Sent to Shopify:\n{pformat(wrapped_payload)}\n\n"
        )

        wizard = self.env["shopify.mapping.test.wizard"].create({"message": msg})

        return {
            "type": "ir.actions.act_window",
            "name": "Mapping Test Result",
            "res_model": "shopify.mapping.test.wizard",
            "view_mode": "form",
            "res_id": wizard.id,
            "target": "new",
        }
