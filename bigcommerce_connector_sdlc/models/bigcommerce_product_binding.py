# -*- coding: utf-8 -*-

from odoo import api, fields, models
from odoo.osv import expression


class BigCommerceProductBinding(models.Model):
    """Maps Odoo products/variants to BigCommerce product/variant identifiers."""

    _name = "bigcommerce.product.binding"
    _description = "BigCommerce Product Binding"
    _rec_name = "bigcommerce_sku"
    _order = "id desc"

    instance_id = fields.Many2one(
        "bigcommerce.connector",
        required=True,
        ondelete="cascade",
        index=True,
    )
    company_id = fields.Many2one(related="instance_id.company_id", store=True, readonly=True)
    product_tmpl_id = fields.Many2one("product.template", index=True, ondelete="set null")
    product_id = fields.Many2one("product.product", index=True, ondelete="set null")
    bigcommerce_product_id = fields.Char(index=True)
    bigcommerce_variant_id = fields.Char(index=True)
    bigcommerce_sku = fields.Char(index=True)
    bigcommerce_inventory_level = fields.Float(string="BigCommerce Stock")
    bigcommerce_is_featured = fields.Boolean(string="Featured")
    bigcommerce_is_visible = fields.Boolean(string="Visible")
    odoo_category_name = fields.Char(string="Categories", compute="_compute_product_metrics")
    current_stock = fields.Float(string="Current Stock", compute="_compute_product_metrics")
    default_price = fields.Float(string="Default Price", compute="_compute_product_metrics")
    calculated_price = fields.Float(string="Calculated Price", compute="_compute_product_metrics")
    channel_name = fields.Char(string="Channels", compute="_compute_product_metrics")
    visibility_label = fields.Char(string="Visibility", compute="_compute_product_metrics")
    sync_state = fields.Selection(
        selection=[
            ("draft", "Draft"),
            ("synced", "Synced"),
            ("error", "Error"),
        ],
        default="draft",
        required=True,
        index=True,
    )
    last_synced_at = fields.Datetime(index=True)
    last_error = fields.Text()
    search_term = fields.Char(
        string="Search",
        compute="_compute_search_term",
        search="_search_search_term",
    )

    _sql_constraints = [
        (
            "bigcommerce_product_binding_variant_uniq",
            "unique(instance_id, bigcommerce_variant_id)",
            "BigCommerce variant id must be unique per instance.",
        )
    ]

    def name_get(self):
        result = []
        for rec in self:
            label = rec.bigcommerce_sku or rec.bigcommerce_variant_id or rec.bigcommerce_product_id or ("Binding %s" % rec.id)
            if rec.product_id:
                label = "%s | %s" % (label, rec.product_id.display_name)
            elif rec.product_tmpl_id:
                label = "%s | %s" % (label, rec.product_tmpl_id.display_name)
            result.append((rec.id, label))
        return result

    @api.depends(
        "bigcommerce_sku",
        "bigcommerce_product_id",
        "bigcommerce_variant_id",
        "product_id",
        "product_tmpl_id",
        "instance_id",
    )
    def _compute_search_term(self):
        for rec in self:
            parts = [
                rec.bigcommerce_sku or "",
                rec.bigcommerce_product_id or "",
                rec.bigcommerce_variant_id or "",
                rec.product_id.default_code or "",
                rec.product_id.display_name or "",
                rec.product_tmpl_id.default_code or "",
                rec.product_tmpl_id.display_name or "",
                rec.instance_id.name or "",
            ]
            rec.search_term = " | ".join([part for part in parts if part])

    @api.depends(
        "instance_id",
        "instance_id.pricelist_id",
        "product_tmpl_id",
        "product_tmpl_id.categ_id",
        "product_tmpl_id.list_price",
        "product_id",
        "product_id.qty_available",
        "product_id.free_qty",
        "product_id.type",
        "instance_id.warehouse_id",
        "instance_id.warehouse_id.lot_stock_id",
        "bigcommerce_inventory_level",
        "bigcommerce_is_visible",
    )
    def _compute_product_metrics(self):
        for rec in self:
            template = rec.product_tmpl_id or rec.product_id.product_tmpl_id
            product = rec.product_id or (template.product_variant_id if template else self.env["product.product"])

            rec.odoo_category_name = template.categ_id.complete_name if template and template.categ_id else ""
            stock_value = 0.0
            if product:
                scoped_product = product.with_context(active_test=False)
                if rec.instance_id.warehouse_id and rec.instance_id.warehouse_id.lot_stock_id:
                    scoped_product = scoped_product.with_context(
                        location=rec.instance_id.warehouse_id.lot_stock_id.id
                    )
                is_storable = bool(getattr(product, "is_storable", False))
                if (product.type != "service") and (is_storable or product.type == "consu"):
                    if "free_qty" in scoped_product._fields:
                        stock_value = float(scoped_product.free_qty or 0.0)
                    else:
                        stock_value = float(scoped_product.qty_available or 0.0)

            if stock_value == 0.0 and rec.bigcommerce_inventory_level not in (None, False):
                stock_value = float(rec.bigcommerce_inventory_level or 0.0)
            rec.current_stock = stock_value
            rec.default_price = template.list_price if template else 0.0
            rec.channel_name = rec.instance_id.name or ""
            rec.visibility_label = "Visible" if rec.bigcommerce_is_visible else "Hidden"

            price = rec.default_price
            pricelist = rec.instance_id.pricelist_id
            if pricelist and product:
                try:
                    price = pricelist._get_product_price(product, 1.0)
                except TypeError:
                    try:
                        price = pricelist._get_product_price(product, 1.0, False)
                    except Exception:
                        price = rec.default_price
                except Exception:
                    price = rec.default_price
            rec.calculated_price = price or 0.0

    @api.model
    def _search_search_term(self, operator, value):
        if not value:
            return []
        if operator not in ("=", "!=", "ilike", "like", "=like", "=ilike", "not ilike", "not like"):
            operator = "ilike"
        return expression.OR(
            [
                [("bigcommerce_sku", operator, value)],
                [("bigcommerce_variant_id", operator, value)],
                [("bigcommerce_product_id", operator, value)],
                [("product_id.default_code", operator, value)],
                [("product_id.name", operator, value)],
                [("product_tmpl_id.default_code", operator, value)],
                [("product_tmpl_id.name", operator, value)],
                [("instance_id.name", operator, value)],
            ]
        )

    @api.model
    def _name_search(self, name="", args=None, operator="ilike", limit=100, order=None):
        args = list(args or [])
        if name:
            search_domain = expression.OR(
                [
                    [("bigcommerce_sku", operator, name)],
                    [("bigcommerce_variant_id", operator, name)],
                    [("bigcommerce_product_id", operator, name)],
                    [("product_id.default_code", operator, name)],
                    [("product_id.name", operator, name)],
                    [("product_tmpl_id.default_code", operator, name)],
                    [("product_tmpl_id.name", operator, name)],
                    [("instance_id.name", operator, name)],
                ]
            )
            args = expression.AND([args, search_domain])
        return self._search(args, limit=limit, order=order)

    @api.onchange("product_id")
    def _onchange_product_id(self):
        for rec in self:
            if rec.product_id:
                rec.product_tmpl_id = rec.product_id.product_tmpl_id

    @api.onchange("product_tmpl_id")
    def _onchange_product_tmpl_id(self):
        for rec in self:
            if rec.product_tmpl_id:
                rec.product_id = rec.product_tmpl_id.product_variant_id

    def _normalized_product_link_vals(self, vals):
        values = dict(vals or {})
        product_id = values.get("product_id")
        template_id = values.get("product_tmpl_id")

        product = self.env["product.product"].browse(product_id) if product_id else self.env["product.product"]
        template = (
            self.env["product.template"].browse(template_id)
            if template_id
            else self.env["product.template"]
        )

        if product and product.exists():
            values["product_tmpl_id"] = product.product_tmpl_id.id
            if "product_id" not in values:
                values["product_id"] = product.id
            return values

        if template and template.exists():
            values["product_tmpl_id"] = template.id
            values["product_id"] = template.product_variant_id.id if template.product_variant_id else False
            return values

        return values

    @api.model_create_multi
    def create(self, vals_list):
        vals_list = [self._normalized_product_link_vals(vals) for vals in vals_list]
        return super().create(vals_list)

    def write(self, vals):
        vals = self._normalized_product_link_vals(vals)
        return super().write(vals)
