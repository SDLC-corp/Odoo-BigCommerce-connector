from odoo import models, fields


class ShopifyCarrierMap(models.Model):
    _name = "shopify.carrier.map"
    _description = "Shopify Carrier Mapping"
    _order = "instance_id, shopify_carrier"

    instance_id = fields.Many2one("shopify.instance", required=True, ondelete="cascade")
    shopify_carrier = fields.Char(required=True)
    odoo_carrier_id = fields.Many2one("delivery.carrier", string="Odoo Carrier", required=True, ondelete="restrict")

    _sql_constraints = [
        (
            "uniq_carrier_instance",
            "unique(instance_id, shopify_carrier)",
            "Carrier mapping already exists for this instance.",
        )
    ]
