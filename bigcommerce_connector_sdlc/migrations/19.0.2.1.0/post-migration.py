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

All steps are idempotent plain SQL.
"""
import logging

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


def migrate(cr, version):
    if not version:
        return
    rejected = _quarantine_rejected_events(cr)
    legacy = _mark_legacy_shipments(cr)
    checkpoints = _default_order_checkpoint(cr)
    _logger.info(
        "BigCommerce upgrade: %s rejected webhook events quarantined, %s deliveries marked as already "
        "exported, %s order checkpoints initialised.",
        rejected,
        legacy,
        checkpoints,
    )
