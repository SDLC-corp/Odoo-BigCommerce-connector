from odoo import models, fields, api, _
import logging

_logger = logging.getLogger(__name__)

class ShopifyInventoryReport(models.Model):
    _name = "shopify.inventory.report"
    _description = "Shopify Inventory Report"
    _order = "qty_available asc, id desc"

    shopify_location_id = fields.Char(string="Shopify Location ID")

    name = fields.Char(string="Product", compute="_compute_name", store=False)
    product_id = fields.Many2one("product.product", required=True, ondelete="cascade")
    product_tmpl_id = fields.Many2one(
        "product.template", related="product_id.product_tmpl_id", store=True
    )
    instance_id = fields.Many2one(
        "shopify.instance", string="Instance", ondelete="cascade"
    )
    shopify_product_id = fields.Char(string="Shopify Product ID")
    sku = fields.Char(string="SKU", related="product_id.default_code", store=True)
    qty_available = fields.Float(string="On Hand (Odoo)")
    virtual_available = fields.Float(string="Forecasted (Odoo)")
    shopify_available = fields.Float(string="Available (Shopify)")

    threshold = fields.Float(string="Threshold", default=0.0)
    status = fields.Selection(
        [
            
            ("out_of_stock", "Out of Stock"),
            ("low_stock", "Low Stock"),
            ("in_stock", "In Stock"),
        ],
        string="Status",
        default="out_of_stock",
        index=True,
    )
    difference = fields.Float(string="Delta to Threshold")
    last_sync = fields.Datetime(string="Last Sync")
    company_id = fields.Many2one(
        "res.company", string="Company", default=lambda self: self.env.company
    )

    def _compute_name(self):
        for rec in self:
            rec.name = rec.product_id.display_name or _("Product")

    @api.model
    def action_refresh_inventory(self, *args, **kwargs):
        threshold = float(self.env.context.get("inventory_threshold", 0.0) or 0.0)
        instances = self.env["shopify.instance"].sudo().search([("active", "=", True)])
        for inst in instances:
            inst.action_sync_inventory_levels()

        action = self.env.ref("sdlc_shopify_connector.action_shopify_inventory_report").sudo().read()[0]
        action["effect"] = {
            "fadeout": "slow",
            "message": _("Inventory refreshed"),
            "type": "rainbow_man",
        }
        return action

    # ---------------------------------------------------
    # REFRESH REPORT (called from menu/server action)
    # ---------------------------------------------------
    @api.model
    def rebuild_inventory_report(self, instance=None, threshold=0.0, include_in_stock=False):
        """Rebuild inventory rows for given instance (or all).
        Any product back in stock is removed from the report unless include_in_stock=True.
        """
        self = self.sudo()
        domain = [("product_tmpl_id.shopify_product_id", "!=", False)]
        if instance:
            domain.append(("product_tmpl_id.shopify_instance_id", "=", instance.id))

        Product = self.env["product.product"].sudo()
        products = Product.search(domain)

        now = fields.Datetime.now()
        search_domain = [("instance_id", "=", instance.id)] if instance else []
        self.search(search_domain).unlink()

        vals_list = []
        for p in products:
            qty = p.qty_available
            virt = p.virtual_available
            shopify_avail = p.product_tmpl_id.shopify_available_qty or 0.0

            # Use the best-known availability (Odoo or Shopify) for status
            available_for_status = max(qty, shopify_avail)

            status = (
                "out_of_stock"
                if available_for_status <= 0
                else "low_stock"
                if available_for_status <= threshold
                else "in_stock"
            )
            if status == "in_stock" and not include_in_stock:
                continue

            vals_list.append(
                {
                    "product_id": p.id,
                    "instance_id": instance.id if instance else p.product_tmpl_id.shopify_instance_id.id,
                    "shopify_product_id": p.product_tmpl_id.shopify_product_id or "",
                    "qty_available": qty,
                    "virtual_available": virt,
                    "shopify_available": shopify_avail,
                    "threshold": threshold,
                    "status": status,
                    "difference": available_for_status - threshold,
                    "last_sync": now,
                    "company_id": p.company_id.id if p.company_id else False,
                }
            )

        if vals_list:
            self.create(vals_list)

        _logger.info(
            "Shopify inventory report rebuilt (instance=%s, threshold=%s, rows=%s)",
            instance and instance.id,
            threshold,
            len(vals_list),
        )
        return True
