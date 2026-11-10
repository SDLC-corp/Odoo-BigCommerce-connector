# -*- coding: utf-8 -*-

from odoo import fields, models


class StockPicking(models.Model):
    _inherit = "stock.picking"

    bigcommerce_shipment_id = fields.Char(
        string="BigCommerce Shipment",
        copy=False,
        index=True,
        readonly=True,
        help="Id of the shipment created in BigCommerce for this delivery. A delivery with a "
        "shipment id is never exported again.",
    )
    bigcommerce_shipment_exported_at = fields.Datetime(
        string="Shipment Exported At",
        copy=False,
        readonly=True,
    )
    bigcommerce_shipment_error = fields.Text(
        string="Shipment Export Error",
        copy=False,
        readonly=True,
    )
