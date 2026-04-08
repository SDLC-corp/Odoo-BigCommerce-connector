from odoo import models, fields

class ShopifyMappingTestWizard(models.TransientModel):
    _name = "shopify.mapping.test.wizard"
    _description = "Shopify Mapping Test Result"

    message = fields.Text(string="Result", readonly=True)
