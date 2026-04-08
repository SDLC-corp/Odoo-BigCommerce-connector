# -*- coding: utf-8 -*-

from odoo import fields, models


class BigCommerceMappingMessageWizard(models.TransientModel):
    _name = "bigcommerce.mapping.message.wizard"
    _description = "BigCommerce Mapping Message Wizard"

    title = fields.Char(required=True, default="BigCommerce Mapping")
    message = fields.Text(required=True)
