# -*- coding: utf-8 -*-

from odoo import fields, models


class BigCommerceCategoryBinding(models.Model):
    """Maps Odoo product categories to BigCommerce category identifiers."""

    _name = "bigcommerce.category.binding"
    _description = "BigCommerce Category Binding"
    _rec_name = "bigcommerce_category_name"
    _order = "id desc"

    instance_id = fields.Many2one(
        "bigcommerce.connector",
        required=True,
        ondelete="cascade",
        index=True,
    )
    category_id = fields.Many2one("product.category", required=True, ondelete="cascade", index=True)
    bigcommerce_category_id = fields.Char(required=True, index=True)
    bigcommerce_parent_category_id = fields.Char(index=True)
    bigcommerce_category_name = fields.Char()
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

    _sql_constraints = [
        (
            "bigcommerce_category_binding_bc_uniq",
            "unique(instance_id, bigcommerce_category_id)",
            "BigCommerce category id must be unique per instance.",
        ),
        (
            "bigcommerce_category_binding_odoo_uniq",
            "unique(instance_id, category_id)",
            "Odoo category must be unique per instance.",
        ),
    ]
