# Release readiness - bigcommerce_connector_sdlc 20.0.2.1.3

The 20.0 branch now carries everything 19.0.2.1.2 has (audit fixes, roles, exception queue,
live-test fixes), adapted to Odoo 20. Details of the fixes themselves: `AUDIT_19.md` and
`RELEASE_READINESS_19.md` (the same code, verified on 19.0 and, for 19.0.2.1.2, against a real
BigCommerce store). The 20.0 build has been tested against mocked BigCommerce responses only.

## Ported

All 16 commits of `origin/20.0..origin/19.0` cherry-picked in order (151942d0 ... cecf0abe):
roles/credentials/multi-company, webhook/order/shipment/inventory/transport fixes, exception
queue and role-aware screens (19.0.2.1.0), upgrade role grant, Odoo runtime suite, API contract
fixes (19.0.2.1.1: root category `parent_id` 0, paged order lines/shipments, webhook port 443),
default field mappings for every connector (19.0.2.1.2), no Odoo taxes on tax-free orders, docs.
Conflicts were limited to the files the October [MIG] touched (ACL file, dashboard, manifest).

## 20.0-only commits kept

- [MIG] 110a4e48: kept (ir.access.csv, `get_str`, base64 str images, Owl 3 dashboard). Its
  dashboard `read_group` rewrites were superseded by the 19.0 `_read_group` code, which already
  runs on Odoo 20.
- [FIX] aded87d0 (trends, low stock, product import, customer export, chat): the same fix as
  19.0's a0df518d; kept in its Owl 3 form (`signal.ref`, `proxy`).
- [FIX] 241962fe (confirm imported orders from their BigCommerce status): byte-identical to 19.0's
  38e19473 (same patch id), so no merge was needed. Combined with the 19.0.2.1.2 default mappings,
  an "Awaiting Fulfillment" order is confirmed through one path only
  (`_apply_bigcommerce_state`, guarded by draft/sent state). Added assertion in
  `test_live_fixes.py::test_new_connector_has_default_mappings_and_confirms_awaiting_fulfillment`:
  a re-sync does not re-confirm, keeps the order date and creates no second delivery.

## Odoo 20 adaptations

- `ir.rule` no longer exists: the 10 multi-company global rules are group-less (restriction)
  `ir.access` entries with the same xml ids and domains; the 19.0 role ACLs converted to
  `ir.access` operations.
- `stock.move.product_uom` -> `uom_id` (shipment item quantities; found by the test run).
- `ir.config_parameter.set_param` -> `set_str` (tests).
- Dashboard "Open Exceptions" card: `this.`-prefixed template members, `t-out`; `useService("action")`.
- Migrations `20.0.2.1.0` (pre/post) and `20.0.2.1.2` (post), same logic as the 19.0 scripts,
  starting from the published 20.0.2.0.0 build.

## Results

- Fresh database (`-i`, Odoo 20 Community): `0 failed, 0 error(s) of 87 tests` = the module's
  85 test methods (same as 19.0) + 2 web unit-suite tests run by the test runner. The
  `SELECT 1/0` error in the log is provoked on purpose by
  `test_one_bad_product_does_not_abort_the_batch`.
- Upgrade: published origin/20.0 (241962fe) installed in a fresh DB, seeded with a connector
  (0 mappings, as the published build creates them), a plain internal user, a rejected
  ("Invalid webhook secret.") and a done webhook event; `-u` to this code: 0 errors/warnings
  from the module; logs `4 security records made updatable`, `role given to 1 internal user(s):
  upg_plain`, `1 rejected webhook events quarantined`, `default field mappings generated for 1
  connector(s)`. After: 11 mappings, events done/rejected, admin has BigCommerce Administrator,
  10 restriction entries, no `base.group_user` access left, checkpoint 30 days back.
- Browser check (2026-11-12, headless Chrome 1440x900, Odoo 20 + demo data, admin): Owl dashboard
  (KPI cards incl. Open Exceptions, sync health, queue, trend, recent activity, AI button), all 18
  menu actions (connectors, order/webhook exceptions, 4 field-mapping lists, product/category/
  customer/order bindings, webhook events/subscriptions, inventory, delivery orders, tracking, sync
  logs) with the first record form of each, connector form with all 8 tabs and a New form. Test
  Connection with a fake token and API base http://127.0.0.1:9 gives a clean notification
  ("Connected: 0, Error: 1"); Sync Webhooks gives "Unable to reach BigCommerce..."; Generate
  Default Mappings reports created/skipped; Validate Field Mappings and Send Test Event run
  without error. No console errors, no failed /web or /json requests, no error dialogs.
  Fixed: the Exceptions stat button used `icon="fa-exclamation-triangle"` (Font Awesome is gone in
  Odoo 20, the icon rendered blank) -> `icon="warning"`.
  Fixed in 20.0.2.1.3 (see below): mappings created by Generate Default Mappings stored only
  `odoo_field_name`, so the "Odoo Field" column (and the rule's display name) of the Field
  Mapping tab was empty although the rules worked. Restricted-user screens were not re-checked in
  the browser (dropped by the coordinator to save time; covered by the access tests).

## 20.0.2.1.3 - Field Mapping tab: blank "Odoo Field" and rule names

Same change as 19.0.2.1.3 (details and root cause: `RELEASE_READINESS_19.md`, rows M1 and M2),
no Odoo 20 specific code needed (`web_read`, `onchange`, `_origin`, `odoo.tests.Form` unchanged).

- Root cause: the default rules were created with the technical `odoo_field_name` only; the lists
  and forms show the Many2one `odoo_field_id`, which was never filled from it, and `_rec_name`
  was `odoo_field_id`, so the rule name was empty ("Unnamed"). The sync reads `odoo_field_name`.
- Fix: `create`/`write` keep `odoo_field_id` in line with the technical name, `_rec_name =
  "name"`, field definitions read through sudo (`ir.model.fields` is readable by "Access Rights"
  administrators only), `migrations/20.0.2.1.3/post-migration.py` fills only empty
  `odoo_field_id` / `bigcommerce_field_id` / `name` (SQL, idempotent, customised values kept).
- Also fixed (published build too): *Add a line* on the tab failed on choosing the mapping type
  (catalog created for the connector's NewId, `connector_id` NULL).
- Results: fresh database `0 failed, 0 error(s) of 93 tests` (87 + 6 new in `test_live_fixes.py`;
  one earlier run had a transient `ConnectionResetError` in
  `test_webhooks.py::test_malformed_unknown_and_oversized_bodies`, the 413 reply closing the socket
  while the 1 MB body was still being sent, unrelated and passing on re-run). Upgrade from origin
  20.0.2.1.2 (blank defaults, one rule re-pointed to `ref` and renamed, one renamed): Odoo field
  filled on 10 rules, customised values unchanged, no errors. Browser: the tab shows every Odoo
  field, the breadcrumb the rule name; a UI-created connector and a rule added through *Add a
  line* display correctly.

## Open items

- Browser check of the role-aware screens as a read-only BigCommerce user on Odoo 20.
- Live verification of the 20.0 build against a real store (19.0 was verified 2026-11-12).
- Remaining items of `RELEASE_READINESS_19.md` (B1 partial, R1-R7) apply unchanged.
