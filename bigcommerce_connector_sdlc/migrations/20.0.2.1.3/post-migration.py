"""Upgrade to 20.0.2.1.3: fill the "Odoo Field" and rule name of the existing field mappings.

Up to 20.0.2.1.2 the default field mappings (created on connector creation, by the "Generate
Default Mappings" button and by the 20.0.2.1.2 upgrade) stored only the technical Odoo field name
(``odoo_field_name``). The Field Mapping lists show the Many2one ``odoo_field_id``, which stayed
empty, and the record name was taken from that same empty field: the "Odoo Field" column was blank
and every default rule showed as "Unnamed". The sync used the technical name, so it worked.

Only empty values are filled, from the rule's own stored technical names; a value an
administrator chose (another Odoo field, a renamed rule) is left as it is. Idempotent.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def fill_blank_mapping_values(env):
    """Fill the empty displayed values of every field mapping; return the counts per column."""
    return env["bigcommerce.field.mapping"].sudo()._repair_blank_display_values()


def migrate(cr, version):
    if not version:
        return
    env = api.Environment(cr, SUPERUSER_ID, {})
    counts = fill_blank_mapping_values(env)
    _logger.info(
        "BigCommerce upgrade: field mappings repaired, Odoo field filled on %(odoo_field)s, "
        "BigCommerce field on %(bigcommerce_field)s, rule name on %(name)s",
        counts,
    )
