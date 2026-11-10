# Audit - bigcommerce_connector_sdlc 19.0.2.0.0 (published 19.0 branch, commit 38e19473)

Scope: every model, service, controller, view, security file, cron, the Owl dashboard, the manifest,
README and `static/description/index.html`. About 10k lines of Python; the branch had **no tests**.
Method: code reading against Odoo 19 source and BigCommerce's published API reference, then
confirmation of each defect by a runtime test written before the fix (see
`RELEASE_READINESS_19.md`). No real BigCommerce store was available.

## 1. Findings, by severity

### Critical

| # | Area | Finding |
|---|---|---|
| C1 | Security | `ir.model.access.csv` gave **every internal user** (`base.group_user`) full create/write/delete on connectors, field mappings and product/category bindings. `access_token` and `client_secret` had no field groups, so any employee could read the store's API token from the form or RPC. |
| C2 | Multi-company | No record rules at all; bindings, logs, webhook events and subscriptions had no `company_id`. Users of company B saw company A's stores, orders and payloads. Partner matching (order and customer import) searched partners of all companies. |
| C3 | Webhooks | BigCommerce sends the scope in the JSON body (`scope`), the code read an `X-BC-Scope` header BigCommerce never sends: every real callback was stored without a scope and closed as "No processor configured". Real-time sync never worked. |
| C4 | Orders | `GET /v2/orders` without a date filter or sort returns the store's **oldest** orders first; with a fixed limit (50) the import re-read the same oldest orders forever and never imported new ones once a store had more than 50 orders. |
| C5 | Orders | The shipping/discount/placeholder service product was created with `uom_po_id`, a field removed in Odoo 19: every order with shipping cost, a discount or an unknown product failed. |
| C6 | Shipments | `POST /v2/orders/{id}/shipments` requires `order_address_id` and `items`; the payload had neither (and sent the carrier's display name as `shipping_provider`, which only accepts a fixed list). BigCommerce refuses every such shipment, so shipment export could not work. |

### High

| # | Area | Finding |
|---|---|---|
| H1 | Webhooks | Rejected callbacks (bad secret, disabled connector) were stored as `failed`, and the processing loop retried every `failed` event: a forged callback was dispatched by the cron. Unauthenticated senders could also grow the table without bound. |
| H2 | Webhooks | No re-delivery/replay protection: BigCommerce retries a callback until it gets a 2xx; each delivery was processed again. Failed events were retried forever, every 5 minutes, without back-off. |
| H3 | Webhooks | The store hash is unique per company only, but the endpoint took the first connector with that hash and compared its secret: a second company using the same store could never authenticate. |
| H4 | Orders | Fallback adoption by order number ignored the store: with two stores in one company (both number orders from 100), store B's order #100 was merged into store A's sale order. |
| H5 | Inventory | The "Inventory Sync" cron and button only imported; with Odoo as inventory master, quantities were never exported automatically and there was no export button. |
| H6 | Products | Product creation in BigCommerce requires `weight`; the payload lacked it (HTTP 422 for every new product). Export pushed the first N saleable templates of **every company** (services and placeholders included) on every run. |
| H7 | Customers | Customer export pushed every contact with an e-mail (vendors, employees, other companies' contacts) to the storefront, with `first_name = last_name = full name`; it used `/v3/customers/{id}` and single objects although the v3 API takes lists on `/v3/customers`. Single-customer import (webhooks) used the same non-existent endpoint. |
| H8 | Variants | All BigCommerce variants of a product were written onto the **same** Odoo variant (default code and stock overwritten in turn); attributes were never created, although the listing claimed variant sync. |
| H9 | Transport | 5xx answers and read timeouts were retried for POST too (duplicate shipments/products/customers possible). BigCommerce's `X-Rate-Limit-Time-Reset-Ms` was ignored; a `Retry-After` value was slept without bound. |

### Medium

| # | Area | Finding |
|---|---|---|
| M1 | Robustness | No savepoint per product/customer, nor around webhook processing: one database error aborted the whole batch (or left a half-imported order behind a failed event). |
| M2 | Inventory | Imported stock overwrote quants with `_update_available_quantity`: no stock move, no history, no valuation entry. |
| M3 | Shipments | Only one delivery per order was ever considered (backorders/partial shipments never exported); idempotency was per order; every run logged a "skipped" line for every non-BigCommerce delivery of the company. |
| M4 | Orders | The connector price list and warehouse were never used on imported orders; currency differences went unnoticed. |
| M5 | Placeholder | The unknown-product placeholder took the real SKU as internal reference, so later SKU matching picked the placeholder service instead of the real product. |
| M6 | Operations | Sync logs and webhook events grew without bound (several log lines per cron run). |
| M7 | Product/customer import | Always re-read the first N records (no incremental filter). |

### Low

| # | Finding |
|---|---|
| L1 | `read_group()` (deprecated in 19, one warning per call) in the dashboard; dead `name_get()` on product bindings; six duplicate field labels logged as warnings at install. |
| L2 | Two stale view files (`menu.xml`, `bigcommerce_instance_views.xml` for a non-existent model) not in the manifest. |
| L3 | README described an empty scaffold ("API client not included yet"). |
| L4 | Default Gemini model `gemini-1.5-flash` is retired. |
| L5 | No empty-state help on most list actions. |

### Listing (`static/description/index.html`) claims not backed by code

Returns, refunds, payouts, net profit/loss; order status write-back (shipped/cancelled/refunded);
brand mapping; addresses and B2B/B2C; two-way variants and categories; tax-handling options;
auto-invoicing; e-mail/Slack alerts; AI product content; "Odoo 16, 17 & 18, CE & EE" on the 19.0
build; industry features (serial/lot tracking, subscriptions, tier pricing, multi-warehouse routing).

## 2. Competitor scan (apps.odoo.com, 18.0 listings)

Paid connectors (Webkul, Globalteckz, TeqStars, Jupical, ECOSIRE, Candidroot) commonly advertise:
order import with status sync, product/variant import, inventory export per warehouse, webhooks with
a queue/retry manager, multi-store. Buyers expect: no missed orders (incremental import), no
duplicates, working shipment/tracking export, a visible queue of what failed, and safe credentials.

## 3. Prioritised plan (what was implemented, and why)

1. **Fix C1-C6 and H1-H9** - without them the connector loses orders, never ships, leaks credentials.
2. **Roles + multi-company rules** (User / Manager / Administrator, credentials admin-only) - basic
   requirement for any ERP connector handling API tokens.
3. **Incremental, idempotent imports with checkpoints** (orders, products, customers) - the core
   promise "no missed orders"; failed orders parked instead of blocking the checkpoint.
4. **Webhook inbox hardening** - de-duplication, coalescing, back-off retries with a maximum,
   rejected-callback quarantine with a cap, deletion handling.
5. **Shipment export per delivery** - BigCommerce's real contract, partial shipments, exactly-once,
   linking of an existing shipment after a lost response, carrier mapping.
6. **Exception queue + order reconciliation** - total/currency mismatch, unmatched products,
   failed imports/exports, one-click re-sync; dashboard KPI.
7. **Variant/attribute import** - the listing's main catalog claim, previously broken.
8. **Inventory**: direction-aware button and cron, traceable adjustments, delta export.
9. **Log/event retention purge**, Odoo 19 API clean-up, empty states, honest listing.

Not done (needs a real store or an owner decision): order status write-back, refunds/returns,
brand mapping, multi-location (MSI) inventory, variant export.
