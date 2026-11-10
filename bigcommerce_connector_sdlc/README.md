# BigCommerce Connector for Odoo 19 (`bigcommerce_connector_sdlc`)

Connects one or more BigCommerce stores to Odoo 19 through BigCommerce's REST API (v2 orders,
v3 catalog/customers/webhooks).

> **Verification status.** Everything below is covered by the Odoo runtime test-suite against
> *mocked* BigCommerce responses that follow BigCommerce's published API reference. **No real
> BigCommerce store has been used.** See `../devtools/RELEASE_READINESS_19.md` before using it in
> production.

## What it does

| Area | Direction | Behaviour |
|---|---|---|
| Orders | BigCommerce -> Odoo | Incremental import (orders modified after the connector checkpoint, oldest first); sale order with lines, shipping, discount and coupon lines; customer matched by BigCommerce id then e-mail within the connector company; status mapped to confirm/cancel through the "Order Status" field mapping; idempotent re-import. A new connector starts 30 days back. |
| Products | BigCommerce -> Odoo | Incremental import; options become Odoo attributes/values and each BigCommerce variant gets its own Odoo variant (SKU, barcode, weight, stock); main image; categories. |
| Products | Odoo -> BigCommerce | Admin action: creates/updates saleable goods of the connector company that are new or changed since their last sync (name, SKU, price, weight, description). Variants are not exported. |
| Categories | BigCommerce -> Odoo | Category tree import. |
| Customers | both | Incremental import; export (admin action) only of customers (`customer_rank > 0`) of the connector company. |
| Inventory | one way per store | "Inventory Master" = BigCommerce: levels imported as Odoo inventory adjustments (stock moves). = Odoo: free quantities exported, only those that changed. The button and the scheduled job follow the master. Lot/serial-tracked products are not adjusted. |
| Shipments | Odoo -> BigCommerce | Each validated delivery of a BigCommerce order with a tracking number becomes one BigCommerce shipment (address, items, tracking, provider for FedEx/UPS/USPS/Endicia/Australia Post/Canada Post/Royal Mail). Partial deliveries included; exported exactly once (shipment id stored on the delivery; an existing shipment with the same tracking number is linked instead of duplicated). Order status is not written back. |
| Webhooks | BigCommerce -> Odoo | Registration (admin) of order/product/customer scopes with a shared secret header. Callbacks are authenticated in constant time, de-duplicated (BigCommerce re-deliveries), queued and processed every 5 minutes; failures retried with exponential back-off up to a maximum; rejected callbacks are kept (capped) for diagnostics and never processed; product/customer deletions flag the binding instead of deleting anything. |
| Exceptions | - | Order bindings flagged when the import or shipment export failed, the Odoo total differs from the BigCommerce total beyond a tolerance, the currency differs, or a line used a placeholder product. "Re-sync from BigCommerce" re-reads an order. Failed webhook events have their own queue. |
| Operations | - | Sync logs, dashboard (incl. open exceptions), daily purge of old logs and finished webhook events (retention per connector), optional Gemini Q&A assistant (your own key). |

## Roles

| Role | Can |
|---|---|
| BigCommerce User (Read Only) | see connectors (without credentials), bindings, logs, exceptions, dashboard |
| BigCommerce Manager | + run syncs, process/retry webhook events, re-sync orders |
| BigCommerce Administrator | + create/configure connectors, see credentials, register webhooks, export products/customers, field mappings. Settings administrators have this role implicitly. |

All records are isolated per company (record rules). API tokens, client secret, webhook secret and
the Gemini key are readable by BigCommerce administrators only.

## Setup

1. In BigCommerce: *Settings -> API -> Store-level API accounts*, create an account (Products,
   Orders, Customers, Information & Settings, Store Inventory: modify), note the store hash and
   access token.
2. In Odoo (as BigCommerce administrator): *BigCommerce -> Connectors -> New*, enter store hash and
   access token, choose warehouse, price list and inventory master, then **Test Connection**.
3. Optional: **Generate Default Mappings** (adds the order-status mapping that confirms paid orders).
4. Webhooks need a public HTTPS `web.base.url` (or a callback URL override): **Generate Secret**,
   then **Register Webhooks**.
5. Enable the automatic jobs on the *Automation* tab.

## Upgrading from 19.0.2.0.0

The migration script:

* re-classifies stored *rejected* webhook callbacks so they are never processed;
* marks deliveries the previous version already reported as exported, so they are not sent twice;
* starts the incremental order checkpoint 30 days before the upgrade (clear it to re-import history);

* gives the read-only **BigCommerce User** role to every active internal user who has neither the
  Manager nor the Administrator role, so nobody loses the BigCommerce menu (the published build let
  every internal user in). Portal/public and archived users get nothing.

Access model after the upgrade: existing BigCommerce Managers keep the Manager role, settings
administrators become BigCommerce Administrators, everyone else internal is a read-only User.
Remove the role from users who should not see BigCommerce (Settings -> Users). On a **fresh
install** no role is granted automatically: only settings administrators get access until you
assign roles.

## Tests

```
odoo-bin -c <conf> -d <throw-away db> -i bigcommerce_connector_sdlc --test-enable \
         --test-tags /bigcommerce_connector_sdlc --stop-after-init
```

The suite blocks every outgoing HTTP request that a test did not mock.
