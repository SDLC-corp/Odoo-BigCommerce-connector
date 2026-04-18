# -*- coding: utf-8 -*-

import json
import logging
from email.utils import parsedate_to_datetime

from odoo import _, api, fields, models
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)


class BigCommerceField(models.Model):
    _name = "bigcommerce.field"
    _description = "BigCommerce Field Catalog"
    _order = "connector_id,mapping_type,name,id"

    connector_id = fields.Many2one(
        "bigcommerce.connector",
        required=True,
        ondelete="cascade",
        index=True,
    )
    mapping_type = fields.Selection(
        [("product", "Product"), ("customer", "Customer"), ("order", "Order"), ("category", "Category")],
        required=True,
        index=True,
    )
    name = fields.Char(required=True, index=True)
    active = fields.Boolean(default=True, index=True)

    _sql_constraints = [
        (
            "bigcommerce_field_catalog_uniq",
            "unique(connector_id, mapping_type, name)",
            "BigCommerce field must be unique per connector and mapping type.",
        )
    ]


class BigCommerceFieldMapping(models.Model):
    _name = "bigcommerce.field.mapping"
    _description = "BigCommerce Field Mapping"
    _rec_name = "odoo_field_id"
    _order = "connector_id,mapping_type,id"

    _MAPPING_MODEL_BY_TYPE = {
        "product": "product.template",
        "customer": "res.partner",
        "order": "sale.order",
        "category": "product.category",
    }
    _BIGCOMMERCE_FIELDS_BY_TYPE = {
        "product": [("name", "name"), ("sku", "sku"), ("price", "price"), ("sale_price", "sale_price"),
                    ("description", "description"), ("weight", "weight"), ("inventory_level", "inventory_level"),
                    ("brand_name", "brand_name"), ("categories", "categories"), ("is_visible", "is_visible")],
        "customer": [("first_name", "first_name"), ("last_name", "last_name"), ("email", "email"),
                     ("phone", "phone"), ("company", "company"), ("customer_group_id", "customer_group_id")],
        "order": [("id", "id"), ("order_number", "order_number"), ("status", "status"),
                  ("date_created", "date_created"), ("currency_code", "currency_code"),
                  ("customer_id", "customer_id"), ("subtotal_ex_tax", "subtotal_ex_tax"),
                  ("total_inc_tax", "total_inc_tax")],
        "category": [
            ("id", "id"),
            ("name", "name"),
            ("description", "description"),
            ("parent_id", "parent_id"),
            ("is_visible", "is_visible"),
            ("sort_order", "sort_order"),
        ],
    }
    _SOURCE_KEY_ALIASES_BY_TYPE = {
        "product": {
            "product_name": "name",
            "regular_price": "price",
            "stock_quantity": "inventory_level",
            "category_ids": "categories",
            "visibility": "is_visible",
        },
        "customer": {
            "customer_email": "email",
        },
        "order": {
            "created_at": "date_created",
            "order_total": "total_inc_tax",
        },
        "category": {
            "parent": "parent_id",
        },
    }

    name = fields.Char()
    connector_id = fields.Many2one("bigcommerce.connector", required=True, ondelete="cascade", index=True)
    active = fields.Boolean(default=True, index=True)
    mapping_type = fields.Selection([("product", "Product"), ("customer", "Customer"), ("order", "Order"), ("category", "Category")], required=True, index=True)
    direction = fields.Selection([("import", "Import"), ("export", "Export")], required=True, default="import", index=True)
    odoo_model = fields.Selection([("product.template", "Product"), ("res.partner", "Customer"), ("sale.order", "Order"), ("product.category", "Category")], required=True, index=True)
    odoo_field_id = fields.Many2one(
        "ir.model.fields",
        string="Odoo Field",
        domain="[('model', '=', odoo_model), ('store', '=', True), ('name', 'not in', ['id','create_uid','create_date','write_uid','write_date','__last_update','display_name']), ('ttype', 'not in', ['one2many','many2many','many2one','binary','html'])]",
    )
    odoo_field_name = fields.Char(required=True, index=True)
    bigcommerce_field_id = fields.Many2one(
        "bigcommerce.field",
        string="BigCommerce Field",
        domain="[('connector_id', '=', connector_id), ('mapping_type', '=', mapping_type), ('active', '=', True)]",
        ondelete="restrict",
    )
    bigcommerce_field_name = fields.Selection(selection=lambda self: self._selection_bigcommerce_fields(), required=True, index=True, string="BigCommerce Field")
    bigcommerce_product_field = fields.Selection(
        selection=_BIGCOMMERCE_FIELDS_BY_TYPE["product"],
        compute="_compute_bigcommerce_selector_fields",
        inverse="_inverse_bigcommerce_product_field",
        store=False,
    )
    bigcommerce_customer_field = fields.Selection(
        selection=_BIGCOMMERCE_FIELDS_BY_TYPE["customer"],
        compute="_compute_bigcommerce_selector_fields",
        inverse="_inverse_bigcommerce_customer_field",
        store=False,
    )
    bigcommerce_order_field = fields.Selection(
        selection=_BIGCOMMERCE_FIELDS_BY_TYPE["order"],
        compute="_compute_bigcommerce_selector_fields",
        inverse="_inverse_bigcommerce_order_field",
        store=False,
    )
    is_required = fields.Boolean(default=False)
    default_value = fields.Char()

    # Legacy/advanced retained for compatibility (hidden from UI).
    company_id = fields.Many2one("res.company", related="connector_id.company_id", store=True, readonly=True)
    bigcommerce_field_path = fields.Char(index=True)
    sequence = fields.Integer(default=10)
    transform_type = fields.Selection(
        [("none", "None"), ("string", "String"), ("integer", "Integer"), ("float", "Float"),
         ("boolean", "Boolean"), ("date", "Date"), ("datetime", "Datetime"), ("json", "JSON"), ("selection", "Selection")],
        default="none",
        required=True,
    )
    notes = fields.Text()
    source_example = fields.Char()
    target_example = fields.Char()
    selection_map_json = fields.Text()
    is_system = fields.Boolean(default=False, copy=False)
    odoo_field_label = fields.Char(compute="_compute_field_metadata")
    odoo_field_type = fields.Char(compute="_compute_field_metadata")

    def read(self, fields=None, load="_classic_read"):
        for rec in self:
            if rec.connector_id and rec.mapping_type:
                field_count = self.env["bigcommerce.field"].search_count(
                    [
                        ("connector_id", "=", rec.connector_id.id),
                        ("mapping_type", "=", rec.mapping_type),
                        ("active", "=", True),
                    ]
                )
                if not field_count:
                    rec._ensure_bigcommerce_fields_catalog()
        return super().read(fields=fields, load=load)

    _sql_constraints = [
        ("bigcommerce_field_mapping_unique_rule",
         "unique(connector_id, mapping_type, direction, odoo_field_name, bigcommerce_field_name)",
         "Duplicate mapping rule for this connector/type/direction is not allowed.")
    ]

    @api.model
    def _selection_bigcommerce_fields(self):
        vals, seen = [], set()
        for pairs in self._BIGCOMMERCE_FIELDS_BY_TYPE.values():
            for key, label in pairs:
                if key in seen:
                    continue
                seen.add(key)
                vals.append((key, label))
        return vals

    @api.depends("odoo_field_id")
    def _compute_field_metadata(self):
        for rec in self:
            rec.odoo_field_label = rec.odoo_field_id.field_description or False
            rec.odoo_field_type = rec.odoo_field_id.ttype or False

    @api.depends("mapping_type", "bigcommerce_field_name")
    def _compute_bigcommerce_selector_fields(self):
        for rec in self:
            rec.bigcommerce_product_field = rec.bigcommerce_field_name if rec.mapping_type == "product" else False
            rec.bigcommerce_customer_field = rec.bigcommerce_field_name if rec.mapping_type == "customer" else False
            rec.bigcommerce_order_field = rec.bigcommerce_field_name if rec.mapping_type == "order" else False

    def _inverse_bigcommerce_product_field(self):
        for rec in self:
            if rec.mapping_type == "product":
                rec.bigcommerce_field_name = rec.bigcommerce_product_field

    def _inverse_bigcommerce_customer_field(self):
        for rec in self:
            if rec.mapping_type == "customer":
                rec.bigcommerce_field_name = rec.bigcommerce_customer_field

    def _inverse_bigcommerce_order_field(self):
        for rec in self:
            if rec.mapping_type == "order":
                rec.bigcommerce_field_name = rec.bigcommerce_order_field

    @api.onchange("mapping_type")
    def _onchange_mapping_type(self):
        for rec in self:
            model = self._MAPPING_MODEL_BY_TYPE.get(rec.mapping_type)
            if model:
                rec.odoo_model = model
            if rec.bigcommerce_field_id and rec.bigcommerce_field_id.mapping_type != rec.mapping_type:
                rec.bigcommerce_field_id = False
            if rec.bigcommerce_field_name and not rec._is_bigcommerce_field_allowed(rec.mapping_type, rec.bigcommerce_field_name):
                rec.bigcommerce_field_name = False
            rec._ensure_bigcommerce_fields_catalog()

    @api.onchange("connector_id", "mapping_type")
    def _onchange_connector_or_mapping_type(self):
        for rec in self:
            if rec.bigcommerce_field_id and (
                rec.bigcommerce_field_id.connector_id != rec.connector_id
                or rec.bigcommerce_field_id.mapping_type != rec.mapping_type
            ):
                rec.bigcommerce_field_id = False
            rec._ensure_bigcommerce_fields_catalog()

    @api.onchange("odoo_field_id")
    def _onchange_odoo_field_id(self):
        for rec in self:
            if rec.odoo_field_id:
                rec.odoo_model = rec.odoo_field_id.model
                rec.odoo_field_name = rec.odoo_field_id.name

    @api.onchange("bigcommerce_field_name")
    def _onchange_bigcommerce_field_name(self):
        for rec in self:
            rec.bigcommerce_field_path = rec.bigcommerce_field_name

    @api.onchange("bigcommerce_field_id")
    def _onchange_bigcommerce_field_id(self):
        for rec in self:
            if rec.bigcommerce_field_id:
                rec.bigcommerce_field_name = rec.bigcommerce_field_id.name
                rec.bigcommerce_field_path = rec.bigcommerce_field_id.name

    @api.constrains("mapping_type", "odoo_model")
    def _check_mapping_model(self):
        for rec in self:
            expected = self._MAPPING_MODEL_BY_TYPE.get(rec.mapping_type)
            if expected and rec.odoo_model != expected:
                raise ValidationError(_("Invalid Odoo model for mapping type '%s'.") % rec.mapping_type)

    @api.constrains("mapping_type", "bigcommerce_field_name")
    def _check_bigcommerce_field(self):
        for rec in self:
            if rec.bigcommerce_field_name and not rec._is_bigcommerce_field_allowed(rec.mapping_type, rec.bigcommerce_field_name):
                raise ValidationError(_("BigCommerce field '%s' is not allowed for '%s'.") % (rec.bigcommerce_field_name, rec.mapping_type))

    @api.constrains("bigcommerce_field_id", "connector_id", "mapping_type")
    def _check_bigcommerce_field_ref(self):
        for rec in self:
            if not rec.bigcommerce_field_id:
                continue
            if rec.bigcommerce_field_id.connector_id != rec.connector_id:
                raise ValidationError(_("Selected BigCommerce field does not belong to this connector."))
            if rec.bigcommerce_field_id.mapping_type != rec.mapping_type:
                raise ValidationError(_("Selected BigCommerce field does not belong to this mapping type."))

    @api.constrains("odoo_model", "odoo_field_name", "direction")
    def _check_field_is_supported(self):
        for rec in self:
            field_def = rec._get_odoo_field_def()
            if not field_def:
                raise ValidationError(_("Unknown Odoo field '%s.%s'.") % (rec.odoo_model, rec.odoo_field_name))
            if not rec._is_supported_field_for_direction(field_def, rec.direction):
                raise ValidationError(_("Field '%s.%s' is not supported for %s mappings.") % (rec.odoo_model, rec.odoo_field_name, rec.direction))

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get("odoo_model") and vals.get("mapping_type"):
                vals["odoo_model"] = self._MAPPING_MODEL_BY_TYPE.get(vals.get("mapping_type"))

            field_id = vals.get("bigcommerce_field_id")
            if field_id:
                bc_field = self.env["bigcommerce.field"].sudo().browse(field_id)
                if bc_field and bc_field.exists():
                    vals["bigcommerce_field_name"] = bc_field.name
                    vals["bigcommerce_field_path"] = bc_field.name

            selector_value = (
                vals.get("bigcommerce_product_field")
                or vals.get("bigcommerce_customer_field")
                or vals.get("bigcommerce_order_field")
            )
            if not vals.get("bigcommerce_field_name") and selector_value:
                vals["bigcommerce_field_name"] = selector_value

            field_id = vals.get("odoo_field_id")
            if field_id:
                field_def = self.env["ir.model.fields"].sudo().browse(field_id)
                if field_def and field_def.exists():
                    vals["odoo_field_name"] = field_def.name
                    vals["odoo_model"] = field_def.model
            bc_name = vals.get("bigcommerce_field_name")
            bc_path = vals.get("bigcommerce_field_path")
            if bc_name and not bc_path:
                vals["bigcommerce_field_path"] = bc_name
            if bc_path and not bc_name:
                vals["bigcommerce_field_name"] = bc_path
            if vals.get("connector_id") and vals.get("mapping_type") and vals.get("bigcommerce_field_name") and not vals.get("bigcommerce_field_id"):
                bc_field = self._get_or_create_catalog_field(
                    connector_id=vals.get("connector_id"),
                    mapping_type=vals.get("mapping_type"),
                    field_name=vals.get("bigcommerce_field_name"),
                )
                vals["bigcommerce_field_id"] = bc_field.id
            if not vals.get("bigcommerce_field_name"):
                raise ValidationError(_("Please select a BigCommerce Field before saving the mapping."))
            if not vals.get("odoo_field_name"):
                raise ValidationError(_("Please select an Odoo Field before saving the mapping."))
            if not vals.get("name") and vals.get("odoo_field_name") and vals.get("bigcommerce_field_name"):
                vals["name"] = "%s -> %s" % (vals["bigcommerce_field_name"], vals["odoo_field_name"])
        return super().create(vals_list)

    def write(self, vals):
        vals = dict(vals)
        if vals.get("mapping_type") and not vals.get("odoo_model"):
            vals["odoo_model"] = self._MAPPING_MODEL_BY_TYPE.get(vals.get("mapping_type"))

        field_id = vals.get("bigcommerce_field_id")
        if field_id:
            bc_field = self.env["bigcommerce.field"].sudo().browse(field_id)
            if bc_field and bc_field.exists():
                vals["bigcommerce_field_name"] = bc_field.name
                vals["bigcommerce_field_path"] = bc_field.name

        selector_value = (
            vals.get("bigcommerce_product_field")
            or vals.get("bigcommerce_customer_field")
            or vals.get("bigcommerce_order_field")
        )
        if not vals.get("bigcommerce_field_name") and selector_value:
            vals["bigcommerce_field_name"] = selector_value

        field_id = vals.get("odoo_field_id")
        if field_id:
            field_def = self.env["ir.model.fields"].sudo().browse(field_id)
            if field_def and field_def.exists():
                vals["odoo_field_name"] = field_def.name
                vals["odoo_model"] = field_def.model
        if vals.get("bigcommerce_field_name") and "bigcommerce_field_path" not in vals:
            vals["bigcommerce_field_path"] = vals["bigcommerce_field_name"]
        if vals.get("bigcommerce_field_path") and "bigcommerce_field_name" not in vals:
            vals["bigcommerce_field_name"] = vals["bigcommerce_field_path"]
        if vals.get("bigcommerce_field_name") and not vals.get("bigcommerce_field_id") and len(self) == 1:
            rec = self[:1]
            target_type = vals.get("mapping_type", rec.mapping_type)
            target_connector = vals.get("connector_id", rec.connector_id.id)
            bc_field = self._get_or_create_catalog_field(
                connector_id=target_connector,
                mapping_type=target_type,
                field_name=vals.get("bigcommerce_field_name"),
            )
            vals["bigcommerce_field_id"] = bc_field.id
        if "bigcommerce_field_name" in vals and not vals.get("bigcommerce_field_name"):
            raise ValidationError(_("Please select a BigCommerce Field before saving the mapping."))
        if "odoo_field_name" in vals and not vals.get("odoo_field_name"):
            raise ValidationError(_("Please select an Odoo Field before saving the mapping."))
        res = super().write(vals)
        for rec in self:
            if not rec.name and rec.bigcommerce_field_name and rec.odoo_field_name:
                rec.name = "%s -> %s" % (rec.bigcommerce_field_name, rec.odoo_field_name)
        return res

    def unlink(self):
        impacted_by_connector = {}
        for rec in self.filtered(lambda r: r.active and r.direction in ("import", "both")):
            if not rec.connector_id:
                continue
            impacted_by_connector.setdefault(rec.connector_id.id, set()).add(rec.mapping_type)

        res = super().unlink()
        if not impacted_by_connector:
            return res

        connector_model = self.env["bigcommerce.connector"].sudo()
        connectors = connector_model.browse(list(impacted_by_connector.keys())).exists()

        for connector in connectors:
            impacted_types = impacted_by_connector.get(connector.id, set())

            if "product" in impacted_types:
                product_bindings = self.env["bigcommerce.product.binding"].sudo().search(
                    [
                        ("instance_id", "=", connector.id),
                        ("sync_state", "=", "synced"),
                        ("bigcommerce_variant_id", "=", False),
                    ]
                )
                if product_bindings:
                    product_bindings.write(
                        {
                            "sync_state": "draft",
                            "last_error": _(
                                "Field mapping changed or removed. Product sync will refresh mapped data."
                            ),
                        }
                    )

            if "customer" in impacted_types:
                customer_bindings = self.env["bigcommerce.customer.binding"].sudo().search(
                    [
                        ("instance_id", "=", connector.id),
                        ("sync_state", "=", "synced"),
                    ]
                )
                if customer_bindings:
                    customer_bindings.write(
                        {
                            "sync_state": "draft",
                            "last_error": _(
                                "Field mapping changed or removed. Customer sync will refresh mapped data."
                            ),
                        }
                    )

            if "order" in impacted_types:
                order_bindings = self.env["bigcommerce.order.binding"].sudo().search(
                    [
                        ("instance_id", "=", connector.id),
                        ("sync_state", "=", "synced"),
                    ]
                )
                if order_bindings:
                    order_bindings.write(
                        {
                            "sync_state": "draft",
                            "last_error": _(
                                "Field mapping changed or removed. Order sync will refresh mapped data."
                            ),
                        }
                    )

            if "category" in impacted_types:
                category_bindings = self.env["bigcommerce.category.binding"].sudo().search(
                    [
                        ("instance_id", "=", connector.id),
                        ("sync_state", "=", "synced"),
                    ]
                )
                if category_bindings:
                    category_bindings.write(
                        {
                            "sync_state": "draft",
                            "last_error": _(
                                "Field mapping changed or removed. Category sync will refresh mapped data."
                            ),
                        }
                    )

            connector.connection_message = _(
                "Field mappings were updated. Run sync to refresh mapped data for: %s."
            ) % ", ".join(sorted(impacted_types))
        return res

    def _is_bigcommerce_field_allowed(self, mapping_type, field_name):
        return field_name in dict(self._BIGCOMMERCE_FIELDS_BY_TYPE.get(mapping_type, []))

    def _default_bigcommerce_field_keys(self, mapping_type):
        return [key for key, _label in self._BIGCOMMERCE_FIELDS_BY_TYPE.get(mapping_type, [])]

    def _normalize_source_field_path(self, mapping_type, field_path):
        path = (field_path or "").strip()
        if not path:
            return path
        aliases = self._SOURCE_KEY_ALIASES_BY_TYPE.get(mapping_type, {})
        parts = path.split(".")
        if not parts:
            return path
        head = parts[0]
        normalized_head = aliases.get(head, head)
        parts[0] = normalized_head
        return ".".join(parts)

    def _ensure_bigcommerce_fields_catalog(self):
        for rec in self:
            if not rec.connector_id or not rec.mapping_type:
                continue
            for key in rec._default_bigcommerce_field_keys(rec.mapping_type):
                rec._get_or_create_catalog_field(
                    connector_id=rec.connector_id.id,
                    mapping_type=rec.mapping_type,
                    field_name=key,
                )

    @api.model
    def _get_or_create_catalog_field(self, connector_id, mapping_type, field_name):
        field_model = self.env["bigcommerce.field"].sudo()
        existing = field_model.search(
            [
                ("connector_id", "=", connector_id),
                ("mapping_type", "=", mapping_type),
                ("name", "=", field_name),
            ],
            limit=1,
        )
        if existing:
            return existing
        return field_model.create(
            {
                "connector_id": connector_id,
                "mapping_type": mapping_type,
                "name": field_name,
                "active": True,
            }
        )

    def _get_odoo_field_def(self):
        self.ensure_one()
        if self.odoo_field_id and self.odoo_field_id.model == self.odoo_model and self.odoo_field_id.name == self.odoo_field_name:
            return self.odoo_field_id
        return self.env["ir.model.fields"].sudo().search(
            [("model", "=", self.odoo_model), ("name", "=", self.odoo_field_name)],
            limit=1,
        )

    def _is_supported_field_for_direction(self, field_def, direction):
        blocked = {"id", "create_uid", "create_date", "write_uid", "write_date", "__last_update", "display_name"}
        if not field_def or field_def.name in blocked:
            return False
        if field_def.ttype in ("one2many", "many2many", "many2one", "binary", "html"):
            return False
        if field_def.model == "sale.order" and field_def.name == "state":
            return True
        if direction in ("import", "both") and field_def.readonly:
            return False
        return True

    @api.model
    def _is_missing_mapped_value(self, value):
        """Treat only null/blank as missing; keep 0 and False as valid values."""
        if value is None:
            return True
        if isinstance(value, str) and not value.strip():
            return True
        return False

    @api.model
    def _coerce_for_odoo_field(self, mapping, value):
        field_def = mapping._get_odoo_field_def()
        if not field_def:
            raise ValueError("Unknown target field %s.%s" % (mapping.odoo_model, mapping.odoo_field_name))
        ttype = field_def.ttype
        if self._is_missing_mapped_value(value):
            return None
        if ttype in ("char", "text", "selection"):
            return str(value).strip()
        if ttype == "integer":
            return int(float(value))
        if ttype in ("float", "monetary"):
            return float(value)
        if ttype == "boolean":
            if isinstance(value, bool):
                return value
            txt = str(value).strip().lower()
            if txt in ("1", "true", "yes", "y", "on"):
                return True
            if txt in ("0", "false", "no", "n", "off"):
                return False
            return bool(value)
        if ttype == "date":
            dt = fields.Date.to_date(value)
            return fields.Date.to_string(dt) if dt else None
        if ttype == "datetime":
            dt = fields.Datetime.to_datetime(value)
            if not dt and isinstance(value, str):
                try:
                    dt = parsedate_to_datetime(value)
                except (TypeError, ValueError):
                    dt = None
            return fields.Datetime.to_string(dt) if dt else None
        return value

    @api.model
    def _normalize_connector(self, connector):
        if isinstance(connector, models.BaseModel):
            connector = connector[:1]
        else:
            connector = self.env["bigcommerce.connector"].browse(int(connector))
        if not connector or not connector.exists():
            raise ValidationError(_("Connector is required for field mapping operations."))
        return connector

    @api.model
    def _get_field_mappings(self, mapping_type, direction, connector):
        connector = self._normalize_connector(connector)
        allowed = [direction]
        if direction in ("import", "export"):
            allowed.append("both")
        mappings = self.sudo().search(
            [
                ("connector_id", "=", connector.id),
                ("mapping_type", "=", mapping_type),
                ("direction", "in", allowed),
                ("active", "=", True),
            ],
            order="id asc",
        )
        valid_mappings = self.browse()
        for mapping in mappings:
            field_def = mapping._get_odoo_field_def()
            if not field_def or not mapping._is_supported_field_for_direction(field_def, mapping.direction):
                _logger.warning(
                    "Skipping unsupported field mapping id=%s connector_id=%s mapping_type=%s target=%s.%s",
                    mapping.id,
                    connector.id,
                    mapping.mapping_type,
                    mapping.odoo_model,
                    mapping.odoo_field_name,
                )
                continue
            valid_mappings |= mapping
        return valid_mappings

    @api.model
    def get_available_odoo_fields(self, odoo_model, direction="import"):
        result = []
        field_defs = self.env["ir.model.fields"].sudo().search([("model", "=", odoo_model)], order="field_description asc,id asc")
        for field_def in field_defs:
            if not self._is_supported_field_for_direction(field_def, direction):
                continue
            result.append({"name": field_def.name, "label": field_def.field_description, "type": field_def.ttype})
        return result

    @api.model
    def _extract_path_value(self, payload, field_path):
        value = payload or {}
        path = (field_path or "").strip()
        if not path:
            return False, None
        for token in path.split("."):
            if isinstance(value, dict):
                if token not in value:
                    return False, None
                value = value[token]
                continue
            if isinstance(value, list):
                if not token.isdigit():
                    return False, None
                idx = int(token)
                if idx < 0 or idx >= len(value):
                    return False, None
                value = value[idx]
                continue
            return False, None
        return True, value

    @api.model
    def _extract_bigcommerce_value(self, payload, field_path, mapping_type=False):
        path = (field_path or "").strip()
        if not path:
            return None

        candidates = [path]
        normalized = self._normalize_source_field_path(mapping_type, path) if mapping_type else path
        if normalized and normalized not in candidates:
            candidates.append(normalized)

        for candidate in candidates:
            found, value = self._extract_path_value(payload=payload, field_path=candidate)
            if found:
                return value
        return None

    @api.model
    def _validate_value_for_target_field(self, mapping, value):
        target_model = self.env[mapping.odoo_model]
        target_field = target_model._fields.get(mapping.odoo_field_name)
        if not target_field:
            raise ValueError("Unknown target field %s.%s" % (mapping.odoo_model, mapping.odoo_field_name))
        # Match Woo behavior: silently skip values that cannot be assigned safely.
        target_field.convert_to_cache(value, target_model)
        return value

    @api.model
    def _parse_json_map(self, raw_json):
        if not raw_json:
            return {}
        try:
            parsed = json.loads(raw_json)
        except (TypeError, ValueError):
            _logger.warning("Invalid selection_map_json found in BigCommerce field mapping.")
            return {}
        if not isinstance(parsed, dict):
            return {}
        normalized = {}
        for key, val in parsed.items():
            normalized[str(key)] = val
            normalized[str(key).lower()] = val
        return normalized

    @api.model
    def _transform_mapping_value(self, value, transform_type, mapping=False):
        if self._is_missing_mapped_value(value):
            return None
        ttype = (transform_type or "none").strip()
        if ttype in ("none", ""):
            return value
        if ttype == "string":
            return str(value).strip()
        if ttype == "integer":
            return int(float(value))
        if ttype == "float":
            return float(value)
        if ttype == "boolean":
            if isinstance(value, bool):
                return value
            txt = str(value).strip().lower()
            if txt in ("1", "true", "yes", "y", "on"):
                return True
            if txt in ("0", "false", "no", "n", "off"):
                return False
            return bool(value)
        if ttype == "date":
            dt = fields.Date.to_date(value)
            return fields.Date.to_string(dt) if dt else None
        if ttype == "datetime":
            dt = fields.Datetime.to_datetime(value)
            if not dt and isinstance(value, str):
                try:
                    dt = parsedate_to_datetime(value)
                except (TypeError, ValueError):
                    dt = None
            return fields.Datetime.to_string(dt) if dt else None
        if ttype == "json":
            return json.loads(value) if isinstance(value, str) else value
        if ttype == "selection":
            map_dict = self._parse_json_map(mapping.selection_map_json if mapping else False)
            txt = str(value)
            return map_dict.get(txt, map_dict.get(txt.lower(), txt))
        return value

    @api.model
    def _prepare_odoo_vals_from_mapping(self, payload, mapping_type, connector, direction="import", raise_on_required=False):
        connector = self._normalize_connector(connector)
        mappings = self._get_field_mappings(mapping_type=mapping_type, direction=direction, connector=connector)
        vals, applied, skipped, missing = {}, [], [], []
        for mapping in mappings:
            source = mapping.bigcommerce_field_name or mapping.bigcommerce_field_path
            raw = self._extract_bigcommerce_value(
                payload=payload,
                field_path=source,
                mapping_type=mapping_type,
            )
            if self._is_missing_mapped_value(raw):
                raw = self._derive_missing_source_value(
                    payload=payload,
                    mapping_type=mapping_type,
                    source=source,
                )
            if self._is_missing_mapped_value(raw) and not self._is_missing_mapped_value(mapping.default_value):
                raw = mapping.default_value
            if self._is_missing_mapped_value(raw):
                if mapping.is_required:
                    missing.append(source)
                skipped.append(mapping.odoo_field_name)
                continue
            try:
                transformed = self._transform_mapping_value(raw, mapping.transform_type, mapping=mapping)
            except Exception as err:
                _logger.warning(
                    "Field mapping transform failed connector_id=%s mapping_type=%s field=%s source=%s err=%s",
                    connector.id, mapping_type, mapping.odoo_field_name, source, str(err),
                )
                if mapping.is_required:
                    missing.append(source)
                skipped.append(mapping.odoo_field_name)
                continue
            try:
                transformed = self._coerce_for_odoo_field(mapping, transformed)
            except Exception as err:
                _logger.warning(
                    "Field mapping type coercion skipped connector_id=%s mapping_type=%s field=%s source=%s value=%s err=%s",
                    connector.id,
                    mapping_type,
                    mapping.odoo_field_name,
                    source,
                    str(raw)[:80],
                    str(err),
                )
                if mapping.is_required:
                    missing.append(source)
                skipped.append(mapping.odoo_field_name)
                continue
            try:
                transformed = self._validate_value_for_target_field(mapping=mapping, value=transformed)
            except Exception as err:
                _logger.warning(
                    "Field mapping assignment skipped connector_id=%s mapping_type=%s field=%s source=%s value=%s err=%s",
                    connector.id,
                    mapping_type,
                    mapping.odoo_field_name,
                    source,
                    str(raw)[:80],
                    str(err),
                )
                if mapping.is_required:
                    missing.append(source)
                skipped.append(mapping.odoo_field_name)
                continue
            if self._is_missing_mapped_value(transformed):
                if mapping.is_required:
                    missing.append(source)
                skipped.append(mapping.odoo_field_name)
                continue
            vals[mapping.odoo_field_name] = transformed
            applied.append(mapping.odoo_field_name)
        if missing:
            msg = "Missing required field mappings for %s: %s" % (mapping_type, ", ".join(sorted(set(missing))))
            if raise_on_required:
                raise ValidationError(msg)
            _logger.warning("Field mapping missing required values connector_id=%s %s", connector.id, msg)
        return {"vals": vals, "applied_fields": applied, "skipped_fields": skipped, "missing_required": missing}

    @api.model
    def _derive_missing_source_value(self, payload, mapping_type, source):
        """Derive optional fallback values for known BigCommerce payload gaps."""
        if mapping_type != "product":
            return None

        source = (source or "").strip()
        if not isinstance(payload, dict):
            return None

        if source in ("price", "regular_price"):
            return self._extract_first_non_empty_product_value(
                payload=payload,
                direct_keys=("price", "calculated_price", "retail_price", "sale_price"),
                variant_keys=("price", "calculated_price", "retail_price", "sale_price"),
            )

        if source == "sale_price":
            return self._extract_first_non_empty_product_value(
                payload=payload,
                direct_keys=("sale_price", "price", "calculated_price", "retail_price"),
                variant_keys=("sale_price", "price", "calculated_price", "retail_price"),
            )

        if source not in ("inventory_level", "stock_quantity"):
            return None

        variants = payload.get("variants")
        if not isinstance(variants, list) or not variants:
            return None

        levels = []
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            value = variant.get("inventory_level")
            if self._is_missing_mapped_value(value):
                continue
            try:
                levels.append(float(value))
            except (TypeError, ValueError):
                continue
        if not levels:
            return 0.0
        return sum(levels)

    @api.model
    def _extract_first_non_empty_product_value(self, payload, direct_keys=(), variant_keys=()):
        payload = payload if isinstance(payload, dict) else {}

        for key in direct_keys:
            if key in payload:
                value = payload.get(key)
                if not self._is_missing_mapped_value(value):
                    return value

        variants = payload.get("variants")
        if isinstance(variants, list):
            for variant in variants:
                if not isinstance(variant, dict):
                    continue
                for key in variant_keys:
                    if key not in variant:
                        continue
                    value = variant.get(key)
                    if not self._is_missing_mapped_value(value):
                        return value
        return None

    @api.model
    def _read_record_field_value(self, record, mapping):
        fname = mapping.odoo_field_name
        if fname not in record._fields:
            return None
        value = record[fname]
        field = record._fields.get(fname)
        if field and field.type == "many2one":
            return value.id if value else None
        if field and field.type in ("one2many", "many2many"):
            return None
        if field and field.type == "date" and value:
            return fields.Date.to_string(value)
        if field and field.type == "datetime" and value:
            return fields.Datetime.to_string(value)
        return value

    @api.model
    def _set_bigcommerce_payload_value(self, payload, field_path, value):
        path = (field_path or "").strip()
        if not path:
            return
        current = payload
        tokens = path.split(".")
        for index, token in enumerate(tokens):
            is_last = index == len(tokens) - 1
            next_token = tokens[index + 1] if not is_last else None
            if is_last:
                if isinstance(current, dict):
                    current[token] = value
                elif isinstance(current, list) and token.isdigit():
                    list_index = int(token)
                    while len(current) <= list_index:
                        current.append(None)
                    current[list_index] = value
                return
            if isinstance(current, dict):
                if token not in current or self._is_path_container_value_missing(current[token]):
                    current[token] = [] if (next_token and next_token.isdigit()) else {}
                current = current[token]
                continue
            if isinstance(current, list) and token.isdigit():
                list_index = int(token)
                while len(current) <= list_index:
                    current.append({} if not (next_token and next_token.isdigit()) else [])
                if self._is_path_container_value_missing(current[list_index]):
                    current[list_index] = [] if (next_token and next_token.isdigit()) else {}
                current = current[list_index]
                continue
            return

    @api.model
    def _is_path_container_value_missing(self, value):
        if value is None or value is False:
            return True
        if isinstance(value, str) and not value.strip():
            return True
        return False

    @api.model
    def _prepare_bigcommerce_payload_from_mapping(self, record, mapping_type, connector, direction="export", raise_on_required=False):
        connector = self._normalize_connector(connector)
        mappings = self._get_field_mappings(mapping_type=mapping_type, direction=direction, connector=connector)
        payload, applied, skipped, missing = {}, [], [], []
        for mapping in mappings:
            value = self._read_record_field_value(record=record, mapping=mapping)
            if self._is_missing_mapped_value(value) and not self._is_missing_mapped_value(mapping.default_value):
                value = mapping.default_value
            if self._is_missing_mapped_value(value):
                if mapping.is_required:
                    missing.append(mapping.odoo_field_name)
                skipped.append(mapping.bigcommerce_field_name or mapping.bigcommerce_field_path)
                continue
            try:
                transformed = self._transform_mapping_value(value, mapping.transform_type, mapping=mapping)
            except Exception as err:
                _logger.warning(
                    "Field export transform failed connector_id=%s mapping_type=%s field=%s err=%s",
                    connector.id, mapping_type, mapping.odoo_field_name, str(err),
                )
                if mapping.is_required:
                    missing.append(mapping.odoo_field_name)
                skipped.append(mapping.bigcommerce_field_name or mapping.bigcommerce_field_path)
                continue
            if self._is_missing_mapped_value(transformed):
                if mapping.is_required:
                    missing.append(mapping.odoo_field_name)
                skipped.append(mapping.bigcommerce_field_name or mapping.bigcommerce_field_path)
                continue
            target = mapping.bigcommerce_field_name or mapping.bigcommerce_field_path
            self._set_bigcommerce_payload_value(payload, target, transformed)
            applied.append(target)
        if missing:
            msg = "Missing required export mappings for %s: %s" % (mapping_type, ", ".join(sorted(set(missing))))
            if raise_on_required:
                raise ValidationError(msg)
            _logger.warning("Field export mapping missing required values connector_id=%s %s", connector.id, msg)
        return {"payload": payload, "applied_paths": applied, "skipped_paths": skipped, "missing_required": missing}

    @api.model
    def _merge_payload_dict(self, base, extra):
        base = dict(base or {})
        extra = dict(extra or {})
        for key, value in extra.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                base[key] = self._merge_payload_dict(base[key], value)
            else:
                base[key] = value
        return base

    @api.model
    def generate_default_mappings(self, instance, reset=False):
        connector = self._normalize_connector(instance)
        model = self.sudo()
        created = skipped = removed = 0
        if reset:
            reset_rules = model.search([("connector_id", "=", connector.id), ("is_system", "=", True)])
            removed = len(reset_rules)
            reset_rules.unlink()

        definitions = self._default_mapping_definitions()
        existing = model.search([("connector_id", "=", connector.id)])
        existing_keys = {
            (rule.mapping_type, rule.direction, rule.odoo_field_name, rule.bigcommerce_field_name)
            for rule in existing
        }
        for definition in definitions:
            key = (
                definition["mapping_type"],
                definition["direction"],
                definition["odoo_field_name"],
                definition["bigcommerce_field_name"],
            )
            if key in existing_keys:
                skipped += 1
                continue
            values = dict(definition)
            values["connector_id"] = connector.id
            values["bigcommerce_field_path"] = values["bigcommerce_field_name"]
            model.create(values)
            existing_keys.add(key)
            created += 1
        return {"created": created, "skipped": skipped, "removed": removed}

    @api.model
    def validate_connector_mappings(self, instance):
        connector = self._normalize_connector(instance)
        mappings = self.sudo().search([("connector_id", "=", connector.id), ("active", "=", True)])
        errors = []
        for mapping in mappings:
            field_def = mapping._get_odoo_field_def()
            if not field_def:
                errors.append("Mapping '%s': field '%s.%s' not found." % (mapping.name or mapping.id, mapping.odoo_model, mapping.odoo_field_name))
                continue
            if not mapping._is_supported_field_for_direction(field_def, mapping.direction):
                errors.append(
                    "Mapping '%s': field '%s.%s' not supported for direction '%s'."
                    % (mapping.name or mapping.id, mapping.odoo_model, mapping.odoo_field_name, mapping.direction)
                )
        return {"total": len(mappings), "valid": len(mappings) - len(errors), "invalid": len(errors), "errors": errors}

    @api.model
    def _default_mapping_definitions(self):
        return [
            {"name": "name -> product.template.name", "mapping_type": "product", "direction": "import", "odoo_model": "product.template", "odoo_field_name": "name", "bigcommerce_field_name": "name", "transform_type": "string", "is_system": True},
            {"name": "sku -> product.template.default_code", "mapping_type": "product", "direction": "import", "odoo_model": "product.template", "odoo_field_name": "default_code", "bigcommerce_field_name": "sku", "transform_type": "string", "is_system": True},
            {"name": "price -> product.template.list_price", "mapping_type": "product", "direction": "import", "odoo_model": "product.template", "odoo_field_name": "list_price", "bigcommerce_field_name": "price", "transform_type": "float", "is_system": True},
            {"name": "description -> product.template.description_sale", "mapping_type": "product", "direction": "import", "odoo_model": "product.template", "odoo_field_name": "description_sale", "bigcommerce_field_name": "description", "transform_type": "string", "is_system": True},
            {"name": "name -> product.category.name", "mapping_type": "category", "direction": "import", "odoo_model": "product.category", "odoo_field_name": "name", "bigcommerce_field_name": "name", "transform_type": "string", "is_system": True},
            {"name": "email -> res.partner.email", "mapping_type": "customer", "direction": "import", "odoo_model": "res.partner", "odoo_field_name": "email", "bigcommerce_field_name": "email", "transform_type": "string", "is_system": True},
            {"name": "first_name -> res.partner.name", "mapping_type": "customer", "direction": "import", "odoo_model": "res.partner", "odoo_field_name": "name", "bigcommerce_field_name": "first_name", "transform_type": "string", "is_system": True},
            {"name": "phone -> res.partner.phone", "mapping_type": "customer", "direction": "import", "odoo_model": "res.partner", "odoo_field_name": "phone", "bigcommerce_field_name": "phone", "transform_type": "string", "is_system": True},
            {"name": "order_number -> sale.order.client_order_ref", "mapping_type": "order", "direction": "import", "odoo_model": "sale.order", "odoo_field_name": "client_order_ref", "bigcommerce_field_name": "order_number", "transform_type": "string", "is_system": True},
            {"name": "date_created -> sale.order.date_order", "mapping_type": "order", "direction": "import", "odoo_model": "sale.order", "odoo_field_name": "date_order", "bigcommerce_field_name": "date_created", "transform_type": "datetime", "is_system": True},
            {"name": "status -> sale.order.state", "mapping_type": "order", "direction": "import", "odoo_model": "sale.order", "odoo_field_name": "state", "bigcommerce_field_name": "status", "transform_type": "selection", "selection_map_json": "{\"awaiting_fulfillment\": \"sale\", \"completed\": \"sale\", \"cancelled\": \"cancel\"}", "is_system": True},
        ]

    def action_save_mapping(self):
        self.ensure_one()
        wizard = self.env["bigcommerce.mapping.message.wizard"].create(
            {
                "title": _("BigCommerce Mapping"),
                "message": _(
                    "Mapping saved successfully.\n\n"
                    "Type: %(mapping_type)s\n"
                    "Odoo Field: %(odoo)s\n"
                    "BigCommerce Field: %(bc)s"
                )
                % {
                    "mapping_type": dict(self._fields["mapping_type"].selection).get(
                        self.mapping_type, self.mapping_type or "-"
                    ),
                    "odoo": self.odoo_field_name or "-",
                    "bc": self.bigcommerce_field_name or "-",
                },
            }
        )
        return {
            "name": _("Mapping Saved"),
            "type": "ir.actions.act_window",
            "res_model": "bigcommerce.mapping.message.wizard",
            "view_mode": "form",
            "res_id": wizard.id,
            "target": "new",
        }

    def action_load_bigcommerce_fields(self):
        self.ensure_one()
        if not self.connector_id:
            raise ValidationError(_("Please select Connector first."))
        if not self.mapping_type:
            raise ValidationError(_("Please select Mapping Type first."))
        self._ensure_bigcommerce_fields_catalog()
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("BigCommerce Fields"),
                "message": _("BigCommerce field dropdown refreshed."),
                "type": "success",
                "sticky": False,
            },
        }

    def _get_sample_payload_for_test(self):
        self.ensure_one()
        if self.mapping_type == "product":
            return {"name": "Sample T-Shirt", "sku": "TS-001", "price": 19.99, "sale_price": 14.99, "description": "Cotton sample product", "weight": 0.5, "inventory_level": 42, "brand_name": "Sample Brand", "categories": [12, 15], "is_visible": True}
        if self.mapping_type == "category":
            return {"id": 12, "name": "Men T-Shirts", "description": "Category description", "parent_id": 5, "is_visible": True, "sort_order": 1}
        if self.mapping_type == "customer":
            return {"first_name": "John", "last_name": "Doe", "email": "john.doe@example.com", "phone": "+1-555-1000", "company": "Acme Inc", "customer_group_id": 3}
        return {"id": 12345, "order_number": 100045, "status": "awaiting_fulfillment", "date_created": "2026-03-30T10:10:00+00:00", "currency_code": "USD", "customer_id": 901, "subtotal_ex_tax": 90.0, "total_inc_tax": 99.0}

    def action_test_mapping(self):
        self.ensure_one()
        sample_payload = self._get_sample_payload_for_test()
        source = self.bigcommerce_field_name or self.bigcommerce_field_path
        raw = self._extract_bigcommerce_value(
            sample_payload,
            source,
            mapping_type=self.mapping_type,
        )
        if self._is_missing_mapped_value(raw):
            raw = self._derive_missing_source_value(
                payload=sample_payload,
                mapping_type=self.mapping_type,
                source=source,
            )
        if self._is_missing_mapped_value(raw) and not self._is_missing_mapped_value(self.default_value):
            raw = self.default_value
        transformed = self._transform_mapping_value(raw, self.transform_type or "none", mapping=self)
        wizard = self.env["bigcommerce.field.mapping.test.wizard"].create(
            {"mapping_id": self.id, "sample_payload": json.dumps(sample_payload, indent=2, ensure_ascii=True), "mapped_result": json.dumps({self.odoo_field_name: transformed}, indent=2, ensure_ascii=True)}
        )
        return {
            "name": _("Test Mapping Result"),
            "type": "ir.actions.act_window",
            "res_model": "bigcommerce.field.mapping.test.wizard",
            "view_mode": "form",
            "res_id": wizard.id,
            "target": "new",
        }
