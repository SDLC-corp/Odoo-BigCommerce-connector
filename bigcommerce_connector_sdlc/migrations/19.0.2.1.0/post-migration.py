"""Upgrade from the published 19.0.2.0.0 build.

What a plain module update would get wrong for existing databases:

1. Rejected webhook callbacks (wrong or missing secret, disabled connector) were stored with status
   "failed", and the published processing loop retried every "failed" event, so forged callbacks
   were dispatched. They become "rejected", which is never processed.
2. Shipments are now tracked per delivery (``stock.picking.bigcommerce_shipment_id``). Deliveries
   the published build already reported as exported (the order binding carries ``exported_at``
   and the delivery was validated before it) are marked as legacy exports, so they are not sent to
   BigCommerce a second time. Deliveries never exported stay eligible.
3. The new incremental order checkpoint defaults to "30 days before the upgrade" for existing
   connectors, so the first run does not import (and confirm, and create deliveries for) the
   store's whole order history. An administrator can clear it for a full re-import.
4. The published build gave every internal user full access through ``base.group_user``. Access
   now requires a BigCommerce role, so every active internal (non-share) user who has neither the
   Manager nor the Administrator role receives the read-only "BigCommerce User" role and keeps the
   menu (decided by the owner). Fresh installs grant nothing automatically.

All steps are idempotent.
"""
import logging

from odoo import SUPERUSER_ID, api

_logger = logging.getLogger(__name__)


def _quarantine_rejected_events(cr):
    cr.execute(
        """
        UPDATE bigcommerce_webhook_event
           SET status = 'rejected'
         WHERE status = 'failed'
           AND (note = 'Webhook rejected before queueing.'
                OR error_message IN ('Invalid webhook secret.',
                                     'Webhook secret is not configured.',
                                     'Webhooks are disabled for this connector.'))
        """
    )
    return cr.rowcount


def _mark_legacy_shipments(cr):
    cr.execute(
        """
        UPDATE stock_picking p
           SET bigcommerce_shipment_id = 'legacy-export',
               bigcommerce_shipment_exported_at = b.exported_at
          FROM bigcommerce_order_binding b, stock_picking_type t
         WHERE p.sale_id = b.sale_order_id
           AND p.picking_type_id = t.id
           AND t.code = 'outgoing'
           AND p.state = 'done'
           AND b.exported_at IS NOT NULL
           AND p.date_done <= b.exported_at
           AND p.bigcommerce_shipment_id IS NULL
        """
    )
    return cr.rowcount


def _default_order_checkpoint(cr):
    cr.execute(
        """
        UPDATE bigcommerce_connector
           SET order_sync_checkpoint = (now() at time zone 'UTC') - interval '30 days'
         WHERE order_sync_checkpoint IS NULL
        """
    )
    return cr.rowcount


def grant_read_only_role(env):
    """Give the read-only BigCommerce role to active internal users without a BigCommerce role.

    Returns the users that received it. Users who already are BigCommerce Managers or
    Administrators (explicitly or through an implied group, e.g. Settings administrators) and
    portal/public (share) users are left alone.
    """
    group_user = env.ref("bigcommerce_connector_sdlc.group_bigcommerce_user")
    users = env["res.users"].with_context(active_test=True).search([("share", "=", False)])
    targets = users.filtered(
        lambda u: not u.has_group("bigcommerce_connector_sdlc.group_bigcommerce_manager")
        and not u.has_group("bigcommerce_connector_sdlc.group_bigcommerce_user")
    )
    if targets:
        group_user.write({"user_ids": [(4, user.id) for user in targets]})
    return targets


def migrate(cr, version):
    if not version:
        return
    rejected = _quarantine_rejected_events(cr)
    legacy = _mark_legacy_shipments(cr)
    checkpoints = _default_order_checkpoint(cr)
    env = api.Environment(cr, SUPERUSER_ID, {})
    granted = grant_read_only_role(env)
    _logger.info(
        "BigCommerce upgrade: read-only BigCommerce User role given to %s internal user(s): %s",
        len(granted),
        ", ".join(granted.mapped("login")[:50]),
    )
    _logger.info(
        "BigCommerce upgrade: %s rejected webhook events quarantined, %s deliveries marked as already "
        "exported, %s order checkpoints initialised.",
        rejected,
        legacy,
        checkpoints,
    )
