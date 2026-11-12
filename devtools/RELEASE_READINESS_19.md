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
| Read-only role granted on upgrade to internal users without a BigCommerce role | yes (test + upgrade check) | - | n/a |

Evidence:

- Odoo runtime suite: **73 test methods (91 including subtests), 0 failed, 0 errors** on the test
  database, and the same suite **0 failed, 0 errors** on a database installed from the published
  build and upgraded (`odoo19_enh_bigcommerce_upg2`). 0 view/translation warnings, 0 duplicate-label warnings at install.
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
a rejected webhook, a failed webhook, a done webhook, logs, default mappings, and users: a plain
employee, a BigCommerce Manager, an archived employee, a portal user), then upgraded with `-u`.
Result: migration ran (`4 security records made updatable`, `read-only BigCommerce User role given
to 1 internal user(s): upg_plain`, `1 rejected webhook event quarantined`, `1 delivery marked as
already exported`); the manager kept Manager, the administrator kept Administrator, the archived
and portal users got nothing; groups renamed and re-chained, the
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

### 19.0.2.1.1 - API contract fixes (checked against docs.bigcommerce.com)

| # | Defect | Fix | Regression test |
|---|---|---|---|
| P1 | Category export omitted `parent_id` for a root category; `POST /v3/catalog/categories` requires it (0 = top level), so root categories got HTTP 422. | `services/category_sync_service.py` `_prepare_export_payload`: always sends `parent_id` (0 for a root; a linked parent is still sent by its remote id). | `test_catalog_inventory.py::test_category_export_sends_parent_id_zero_for_root` |
| P2 | `GET /v2/orders/{id}/products` (order import and shipment export) and `GET /v2/orders/{id}/shipments` were read once; V2 returns 50 records per page by default, so lines and shipments after the first page were lost. | `order_sync_service._fetch_order_products`, `shipment_sync_service._shipment_items` and `_find_remote_shipment` use the client's `get_paginated` (`limit=250`, until an empty page / HTTP 204). | `test_order_sync.py::test_order_with_more_than_one_page_of_lines_imports_every_line` (60 lines over two pages), `test_shipments.py::test_order_lines_beyond_the_first_page_are_matched`, `test_shipments.py::test_existing_shipment_on_a_later_page_is_linked_not_duplicated` |
| P3 | Webhook destination check required HTTPS and a public host but accepted any port; BigCommerce only delivers webhooks to port 443. | `models/bigcommerce_instance.py` `_get_webhook_destination_validation_error`: an explicit port other than 443 (or an invalid port) is refused with a clear message; no port or `:443` is accepted. | `test_webhooks.py::test_registration_refuses_a_destination_port_other_than_443`, `test_webhooks.py::test_registration_accepts_default_or_explicit_443_port` |

Test fixture: `tests/common.py` gained `v2_pages()`, which serves V2 list routes page by page and
answers HTTP 204 past the end; `mock_order` and the shipment mocks use it.

### 19.0.2.1.2 - found in the live BigCommerce test

First run against a real BigCommerce trial store: order #100 ("Awaiting Fulfillment", INR,
104.85 = 34.95 + 2 x 29.95 + 10.00 shipping, `total_tax` 0) was imported as a quotation without
delivery, with Odoo's default 15% tax added (120.58).

| # | Defect | Fix | Regression test |
|---|---|---|---|
| L1 | The default field mappings, among them the "Order Status" map that confirms or cancels orders, were only created by `action_generate_default_mappings`, which no view called. On a new connector imported orders were never confirmed: no delivery, so shipment export could never run. Two copies of the defaults existed (connector and mapping model). | `bigcommerce.field.mapping._default_mapping_definitions` / `generate_default_mappings` is the single source (the connector copy is removed); it now also skips archived rules. `bigcommerce.connector.create` generates them through `_generate_default_mappings` (no access check, notification or log), which the admin action reuses. *Generate Default Mappings* button on the *Field Mapping* tab (administrators). `migrations/19.0.2.1.2/post-migration.py` fills only connectors with zero mappings. | `test_live_fixes.py::test_new_connector_has_default_mappings_and_confirms_awaiting_fulfillment`, `::test_default_mapping_generation_is_idempotent`, `::test_migration_fills_only_connectors_without_mappings` |
| L2 | Imported lines took Odoo's product default taxes although BigCommerce charged none, so the Odoo total exceeded the amount the customer paid. | `services/order_sync_service.py` `_order_charged_no_tax` / `_line_tax_vals`: when `total_tax` is 0 (or, without it, `total_inc_tax` = `total_ex_tax`) product, shipping and discount lines get no taxes. When tax was charged, Odoo taxes and fiscal position apply as before; differences beyond the connector's order total tolerance are flagged "Total differs" (the live case, 104.85 vs 120.58, would have been flagged had the currencies matched; with INR vs the company currency it was flagged "Currency differs" instead). | `test_live_fixes.py::test_order_without_tax_gets_no_odoo_taxes` (also re-applies the 15% tax and asserts "Total differs"), `::test_order_without_tax_detected_from_inc_and_ex_totals`, `::test_order_with_tax_keeps_odoo_taxes` |

`test_shipments.py` no longer confirms the imported order itself: it asserts the import confirmed
it. Full suite on a fresh database: 0 failed, 0 error(s) of 85 tests.

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
- R7 On upgrade every active internal user without a BigCommerce role receives the read-only User
  role (owner decision), so they can still *read* connectors without credentials, bindings, logs and
  the dashboard. Administrators should remove it from users who should not see store data.

## 5. Owner decisions needed

- Testimonials, deployment counts ("200+ live stores", "since 2018") and the 10-hour implementation
  offer on the store page are business claims this review cannot verify; they were left unchanged.
- ~~Auto-assign the read-only role on upgrade~~ - decided by the owner: yes on upgrade from the
  published build, no on fresh installs. Implemented in `migrations/19.0.2.1.0/post-migration.py`.
- Whether to publish 19.0 before B1 is closed.

## Live end-to-end verification (2026-11-12, real BigCommerce trial store, INR)

Run against a real store-level API account (Orders/Products/Customers modify, Information read-only)
on 19.0.2.1.2 after a real `-u` upgrade from 19.0.2.1.1:

| Step | Result |
|---|---|
| Test Connection (`GET /v2/store`) | 200, connected |
| Sync Categories / Products | 6 categories, 13 products (with variants) created |
| Sync Customers | 1 customer created (Demo Buyer) |
| Sync Orders | order #100 -> confirmed sale order in INR, 3 lines (2 products + shipping), no tax, total 104.85 = BigCommerce total, delivery created, no exception flag |
| Validate delivery (tracking DEMO-TRACK-100) + Export Shipments | `POST /v2/orders/100/shipments` 201; BigCommerce shows the order **Shipped**, Shipment #1, 3 items, tracking DEMO-TRACK-100 |

Not verifiable from a local machine: webhooks (BigCommerce needs a public HTTPS callback on port 443).
Store currency must exist in Odoo: with INR inactive the order was imported in the company currency
and correctly flagged "Currency differs"; activating INR and setting an INR pricelist on the
connector fixed it (configuration, not code).
