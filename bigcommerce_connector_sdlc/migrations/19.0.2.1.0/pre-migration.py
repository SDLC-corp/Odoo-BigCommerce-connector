"""Upgrade from the published 19.0.2.0.0 build (before the module data is reloaded).

The published build declared its category, privilege and two groups inside a ``noupdate="1"``
block, so a plain update would never rename them or attach the new Administrator role above
Manager. They are switched to updatable so the new security definition applies.
"""
import logging

_logger = logging.getLogger(__name__)

XMLIDS = (
    "module_category_bigcommerce",
    "res_groups_privilege_bigcommerce",
    "group_bigcommerce_user",
    "group_bigcommerce_manager",
)


def migrate(cr, version):
    if not version:
        return
    cr.execute(
        """
        UPDATE ir_model_data
           SET noupdate = FALSE
         WHERE module = 'bigcommerce_connector_sdlc'
           AND name IN %s
        """,
        (XMLIDS,),
    )
    _logger.info("BigCommerce upgrade: %s security records made updatable.", cr.rowcount)
