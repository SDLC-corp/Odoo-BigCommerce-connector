# Release readiness - bigcommerce_connector_sdlc 19.0.2.1.0

Status: **not production-verified.** No real BigCommerce store has ever been called. Everything
below is verified against mocked HTTP responses built from BigCommerce's published API reference,
on Odoo 19 Community with PostgreSQL. The 19.0 build is not yet listed on the Odoo Apps store (the
store listing is 18.0); do not publish it until blocker B1 is closed.

Maturity levels: **implemented** - **source-tested** (offline) - **runtime-tested** (Odoo test
suite, real PostgreSQL database, HTTP mocked) - **browser-tested** (real headless Chrome, isolated
server, synthetic data) - **production-verified** (real store).

## 1. Verification table

| Area | Runtime | Browser | Production |
|---|:-:|:-:|:-:|
| Roles (user / manager / administrator), credentials hidden from non-admins, plain users locked out | yes | yes (3 roles) | n/a |
| Multi-company record rules (connectors, bindings, logs, events, dashboard) | yes | - | n/a |
| Transport: 429 reset window, capped waits and budget, POST not repeated after timeout/5xx, quota throttle | yes | - | no |
| Webhook endpoint (real HTTP route): body scope, secret per connector of a shared store hash, producer fallback, 400/401/404/413/503, rejected trace capped and never processed | yes | - | no |
| Webhook processing: de-duplication, coalescing, back-off retries, max retries, manual retry, savepoint, deletions, registration headers | yes | failed-event queue renders | no |
| Incremental order import (checkpoint, sort, same-second guard), idempotent re-import, failed orders parked, re-sync | yes | exception queue renders | no |
| Order details: shipping/discount/coupon lines (Odoo 19 service product), placeholder for unknown SKU, store isolation, warehouse, pricelist/currency, company-scoped partners | yes | - | no |
| Exception flags (sync error, no sale order, total/currency mismatch, unmatched products) and counters | yes | yes | n/a |
| Shipment export: required payload, partial deliveries, exactly once, existing shipment linked, refusal -> exception, non-BigCommerce deliveries ignored, carrier mapping | yes | - | no |
| Product import with options -> attributes/variants, per-variant stock, savepoint per product, checkpoint | yes | - | no |
| Product export (weight, SKU, company scope, delta) | yes | - | no |
| Customer export scope and name split, v3 list payloads | yes | - | no |
| Inventory: traceable adjustments, level 0 applied, delta export, button and cron follow the master | yes | - | no |
| Purge cron, dashboard (`_read_group`), views for every action and role, empty states | yes | yes | n/a |
| **Upgrade from the published build (38e19473)** | yes | yes (upgraded DB) | - |

Evidence:

- Odoo runtime suite: **72 test methods (88 including subtests), 0 failed, 0 errors** on a fresh
  database, and the same suite **0 failed, 0 errors** on a database installed from the published
  build and upgraded. 0 view/translation warnings, 0 duplicate-label warnings at install.
- Browser: private headless Chrome against the upgraded database, 10 screens x 3 roles
  (administrator, manager, read-only user): **0 JavaScript console errors, 0 error dialogs**;
  read-only users see no action buttons, managers no administrator buttons.

Command:

```
odoo-bin -c odoo.conf --addons-path=<odoo>/odoo/addons,<odoo>/addons,<this repo> \
  -d <throw-away db> -i bigcommerce_connector_sdlc --test-enable \
  --test-tags /bigcommerce_connector_sdlc --stop-after-init
```

### Upgrade test

The published commit was installed in a fresh database, seeded through its own code (two connectors,
a bound product with stock, two confirmed and delivered BigCommerce orders - one already "exported" -,
a rejected webhook, a failed webhook, a done webhook, logs, default mappings), then upgraded with
`-u`. Result: migration ran (`4 security records made updatable`, `1 rejected webhook event
quarantined`, `1 delivery marked as already exported`), groups renamed and re-chained, the
administrator got the BigCommerce Administrator role, 10 record rules and 7 crons present, existing
bindings got their company, the order checkpoint started 30 days before the upgrade, the failed
event stays retryable. Both historical orders were flagged "Total differs" because Odoo added its
default 15% tax to prices BigCommerce reported tax-exclusive - this is the reconciliation working,
but it means an upgraded store may see many historical orders in the exception queue (see R3).

## 2. Defects fixed (each with a regression test)

Critical: credentials and full write access for every internal user (C1); no multi-company isolation
(C2); webhook scope read from a header BigCommerce never sends - no callback ever processed (C3);
order import stuck on the oldest orders (C4); Odoo-19 incompatibilities in service-product creation
(`uom_po_id`, `product.product_category_all`) breaking every order with shipping/discount/unknown
product (C5 - the second reference was found by the test run, not by reading); shipment payload
missing `order_address_id` and `items` (C6).

High: forged/rejected callbacks processed by the cron (H1); no re-delivery protection, endless retries
(H2); second company sharing a store hash could not authenticate (H3); cross-store order adoption
(H4); inventory export never scheduled and no button (H5); product export without weight and across
companies (H6); customer export of all contacts, wrong v3 endpoints (H7); all variants collapsed on one
Odoo variant (H8); POST repeated after timeouts, unbounded sleeps (H9).

Found by real execution (not visible by reading): `product.product_category_all` no longer exists in
Odoo 19; a BigCommerce stock level of **0** was treated as "empty" (`0 in (None, False)` is true),
so stock never went down to zero; lookup caches kept partners created in a rolled-back savepoint, so
the order after a failed one crashed ("Record does not exist"); the webhook route needs
`X-Odoo-Database` on servers hosting several databases (now registered with every hook).

Medium/low: see `AUDIT_19.md` (savepoints, traceable stock moves, per-delivery shipments, warehouse
and pricelist, placeholder SKU, unbounded history, deprecated `read_group`, dead `name_get`, stale
files, README, listing claims).

## 3. Remaining blocker

| # | Blocker | Action |
|---|---|---|
| B1 | **No real BigCommerce call has ever been made.** | Run against a BigCommerce sandbox store: connection test, order import with `min_date_modified` + `sort`, product import of a product with options, customer import/export (v3 list payloads), inventory export, one real shipment (address + items), webhook registration and a real callback (body shape, custom headers, re-delivery). Fix whatever the real API contradicts. |

## 4. Residual risks

- R1 Payload shapes follow the reference, not observed traffic: notably the v3 `sort` values for
  products (`sort=date_modified&direction=asc`) and customers (`date_modified:asc`), `id:in` for a
  single customer, and the `shipping_provider` list.
- R2 Product/customer checkpoints have no same-second guard (orders do): more than one page of records
  modified in the same second could be re-read on every run.
- R3 Upgraded databases may show many historical orders as "Total differs" when Odoo's taxes differ
  from BigCommerce's; raise the connector tolerance or fix tax configuration.
- R4 Webhook processing runs every 5 minutes (not instant); deletions only flag bindings.
- R5 Order status is not written back; refunds/returns, brands, multi-location inventory and variant
  export are not implemented (the listing now says so).
- R6 Enterprise and Odoo.sh untested. Scale not measured.
- R7 Users who relied on implicit access (any internal user) lose the BigCommerce menu after the
  upgrade until a role is assigned (documented in the README).

## 5. Owner decisions needed

- Testimonials, deployment counts ("200+ live stores", "since 2018") and the 10-hour implementation
  offer on the store page are business claims this review cannot verify; they were left unchanged.
- Whether to auto-assign the read-only role to existing internal users on upgrade (currently not).
- Whether to publish 19.0 before B1 is closed.
