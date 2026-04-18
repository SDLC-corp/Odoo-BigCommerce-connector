# -*- coding: utf-8 -*-

from odoo import fields, models


class BigCommerceFieldMappingTestWizard(models.TransientModel):
    _name = "bigcommerce.field.mapping.test.wizard"
    _description = "BigCommerce Field Mapping Test Wizard"

    mapping_id = fields.Many2one("bigcommerce.field.mapping", string="Mapping", readonly=True)
    sample_payload = fields.Text(readonly=True)
    mapped_result = fields.Text(readonly=True)
