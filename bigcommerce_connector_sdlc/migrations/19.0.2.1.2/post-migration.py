"""Upgrade to 19.0.2.1.2: default field mappings for connectors that have none.

Found in the live BigCommerce test: the default mappings (among them the "Order Status" map that
confirms paid orders and cancels cancelled ones) were only created by a method no button called,
so on a connector created before 19.0.2.1.2 imported orders stayed quotations: no delivery, no
shipment export. New connectors now get them on creation.

Only connectors with no ``bigcommerce.field.mapping`` record at all (active or archived) are
filled: a connector with any mapping was configured by an administrator, who may have removed
or changed default rules on purpose. Idempotent.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def generate_missing_default_mappings(env):
    """Generate the default mappings of every connector without any mapping; return those connectors."""
    connectors = env["bigcommerce.connector"].sudo().with_context(active_test=False).search([])
    mapped = env["bigcommerce.field.mapping"].sudo().with_context(active_test=False).search(
        [("connector_id", "in", connectors.ids)]
    ).connector_id
    targets = connectors - mapped
    targets._generate_default_mappings()
    return targets


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    filled = generate_missing_default_mappings(env)
    _logger.info(
        "BigCommerce upgrade: default field mappings generated for %s connector(s) without mappings: %s",
        len(filled),
        ", ".join(filled.mapped("name")[:50]),
    )
