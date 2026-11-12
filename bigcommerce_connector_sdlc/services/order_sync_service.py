# -*- coding: utf-8 -*-

import logging
from datetime import timedelta, timezone
from email.utils import parsedate_to_datetime

from odoo import Command, fields
from odoo.tools import float_is_zero

from .api_client import BigCommerceApiClient

_logger = logging.getLogger(__name__)


class BigCommerceOrderSyncService:
    """Service layer for manual BigCommerce order import."""

    def __init__(self, instance):
        self.instance = instance
        self.env = instance.env
        self.client = BigCommerceApiClient(instance)
        self.mapping_model = self.env["bigcommerce.field.mapping"]
        self._order_binding_cache = {}
        self._partner_by_remote_customer_cache = {}
        self._partner_by_email_cache = {}
        self._product_by_remote_variant_cache = {}
        self._product_by_remote_product_cache = {}
        self._product_by_sku_cache = {}

    def _reset_caches(self):
        self._order_binding_cache = {}
        self._partner_by_remote_customer_cache = {}
        self._partner_by_email_cache = {}
        self._product_by_remote_variant_cache = {}
        self._product_by_remote_product_cache = {}
        self._product_by_sku_cache = {}

    def import_orders(self, limit=20):
        """Import orders from BigCommerce into Odoo sale orders."""
        limit = int(limit or 20)
        if limit < 1:
            limit = 1
        if limit > 100:
            limit = 100

        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "order_import",
                "resource_type": "order",
                "request_method": "GET",
                "request_url": self.client._build_url("/v2/orders"),
                "status": "draft",
                "note": "Manual order import started (limit=%s)." % limit,
            }
        )

        checkpoint = self.instance.order_sync_checkpoint
        fetch_result = self._fetch_orders(limit=limit, modified_after=checkpoint)
        if not fetch_result.get("success"):
            message = fetch_result.get("message") or "Unable to fetch orders from BigCommerce."
            self._log_failure(
                message=message,
                request_url=fetch_result.get("url"),
                response_status=fetch_result.get("status_code"),
                response_body=fetch_result.get("response_body"),
                retry_count=fetch_result.get("retry_count"),
                duration_ms=fetch_result.get("duration_ms"),
                note="Order fetch failed before import started.",
            )
            return {
                "success": False,
                "message": message,
                "total": 0,
                "created": 0,
                "updated": 0,
                "failed": 0,
                "failed_items": [message],
            }

        orders = fetch_result.get("orders", [])
        if not orders:
            message = "Order import completed. No new or modified orders in BigCommerce."
            self._log_success(
                message=message,
                request_url=fetch_result.get("url"),
                response_status=fetch_result.get("status_code"),
                response_body=fetch_result.get("response_body"),
                retry_count=fetch_result.get("retry_count"),
                duration_ms=fetch_result.get("duration_ms"),
            )
            return {
                "success": True,
                "message": message,
                "total": 0,
                "created": 0,
                "updated": 0,
                "failed": 0,
                "failed_items": [],
            }

        created = 0
        updated = 0
        failed = 0
        failed_items = []

        batch_size = self._resolve_batch_size(limit)
        newest_modified = False
        for batch in self._iter_batches(orders, batch_size):
            for bc_order in batch:
                modified = self._parse_bc_datetime(bc_order.get("date_modified"))
                if modified and (not newest_modified or modified > newest_modified):
                    newest_modified = modified
                try:
                    # One savepoint per order: an order that fails (e.g. while being confirmed)
                    # leaves nothing half-imported behind.
                    with self.env.cr.savepoint():
                        sale_order, was_created = self._create_or_update_sale_order(bc_order)
                        self._create_or_update_binding(sale_order, bc_order)
                    if was_created:
                        created += 1
                    else:
                        updated += 1
                except Exception as err:
                    # The savepoint rolled back records the lookup caches may point to (a partner or
                    # product created for this record): forget them so the next record does not reuse them.
                    self._reset_caches()
                    failed += 1
                    remote_id = self._as_remote_id(bc_order)
                    order_number = self._order_number(bc_order)
                    error_text = str(err)
                    item_label = order_number or ("ID %s" % remote_id if remote_id else "Unknown Order")
                    failed_items.append("%s: %s" % (item_label, error_text))

                    _logger.warning(
                        "BigCommerce order import failed for instance_id=%s remote_id=%s order_number=%s: %s",
                        self.instance.id,
                        remote_id,
                        order_number,
                        error_text,
                    )
                    # The failed order is parked in the exception queue (error binding with a
                    # "Re-sync" button) so the checkpoint can move on without losing it.
                    self._mark_order_failed(bc_order, error_text)

                    self._log_failure(
                        message=error_text,
                        resource_remote_id=remote_id,
                        note="Order '%s' failed." % (order_number or "Unknown"),
                    )

        self._advance_checkpoint(checkpoint, newest_modified)

        total = len(orders)
        success = failed == 0
        message = (
            "Order import completed. Total: %(total)s, Created: %(created)s, "
            "Updated: %(updated)s, Failed: %(failed)s."
        ) % {
            "total": total,
            "created": created,
            "updated": updated,
            "failed": failed,
        }

        if success:
            self._log_success(
                message=message,
                request_url=fetch_result.get("url"),
                response_status=fetch_result.get("status_code"),
                response_body=fetch_result.get("response_body"),
                retry_count=fetch_result.get("retry_count"),
                duration_ms=fetch_result.get("duration_ms"),
            )
        else:
            failure_note = "First failures: %s" % " | ".join(failed_items[:3])
            self._log_failure(
                message=message,
                request_url=fetch_result.get("url"),
                response_status=fetch_result.get("status_code"),
                response_body=fetch_result.get("response_body"),
                retry_count=fetch_result.get("retry_count"),
                duration_ms=fetch_result.get("duration_ms"),
                note=failure_note,
            )

        return {
            "success": success,
            "message": message,
            "total": total,
            "created": created,
            "updated": updated,
            "failed": failed,
            "failed_items": failed_items[:5],
        }

    def _advance_checkpoint(self, previous, newest_modified):
        """Move the incremental checkpoint to the newest modification date processed.

        Failed orders do not hold it back (they are parked as error bindings). When the newest
        order carries the checkpoint timestamp itself, it moves one second forward so a burst of
        same-second modifications cannot pin the import on one page forever.
        """
        if not newest_modified:
            return
        newest = fields.Datetime.to_datetime(newest_modified)
        previous = fields.Datetime.to_datetime(previous) if previous else False
        if previous and newest <= previous:
            newest = previous + timedelta(seconds=1)
        self.instance.sudo().write({"order_sync_checkpoint": newest})

    def _mark_order_failed(self, bc_order, error_text):
        """Record a failed order as an error binding (exception queue, Re-sync button)."""
        remote_id = self._as_remote_id(bc_order)
        if not remote_id:
            return
        binding_model = self.env["bigcommerce.order.binding"].sudo()
        binding = binding_model.search(
            [("instance_id", "=", self.instance.id), ("bigcommerce_order_id", "=", remote_id)], limit=1
        )
        vals = {
            "sync_state": "error",
            "last_error": (error_text or "")[:2000],
            "status_on_bigcommerce": str(bc_order.get("status") or bc_order.get("status_id") or ""),
            "bigcommerce_total_amount": self._extract_order_total(bc_order),
            "bigcommerce_currency_code": self._extract_order_currency_code(bc_order),
        }
        if binding:
            binding.write(vals)
        else:
            vals.update(
                {
                    "instance_id": self.instance.id,
                    "bigcommerce_order_id": remote_id,
                    "bigcommerce_order_number": self._order_number(bc_order) or False,
                }
            )
            binding_model.create(vals)
        self._order_binding_cache.pop(remote_id, None)

    def import_order_by_id(self, order_id):
        """Import a single BigCommerce order by id (webhook-safe helper)."""
        result = self.client.get("/v2/orders/%s" % order_id, timeout=25)
        if not result.get("success"):
            return {"success": False, "message": result.get("message") or "Failed to fetch order by id."}

        body = result.get("response_body")
        if not isinstance(body, dict):
            return {"success": False, "message": "Unexpected order payload format."}

        sale_order, _created = self._create_or_update_sale_order(body)
        self._create_or_update_binding(sale_order, body)
        return {"success": True, "message": "Order %s synced." % order_id}

    def _fetch_orders(self, limit=20, modified_after=False):
        """Fetch orders modified after the checkpoint, oldest modification first.

        Without a date filter and an explicit sort, BigCommerce returns its oldest orders first, so
        a fixed limit kept re-importing the same first orders and never reached new ones (the
        behaviour before 19.0.2.1.0).
        """
        orders = []
        page_limit = max(20, min(100, int(limit or 20)))
        total_retry_count = 0
        total_duration_ms = 0
        last_result = {}
        params = {"sort": "date_modified:asc"}
        if modified_after:
            stamp = fields.Datetime.to_datetime(modified_after)
            params["min_date_modified"] = stamp.strftime("%Y-%m-%dT%H:%M:%S+00:00")

        for page_result in self.client.iter_paginated(
            "/v2/orders",
            params=params,
            limit=page_limit,
            max_pages=None,
            data_key="data",
        ):
            total_retry_count += int(page_result.get("retry_count") or 0)
            total_duration_ms += int(page_result.get("duration_ms") or 0)
            last_result = page_result
            if not page_result.get("success"):
                return {
                    "success": False,
                    "message": page_result.get("message") or "BigCommerce order request failed.",
                    "status_code": page_result.get("status_code"),
                    "url": page_result.get("url"),
                    "response_body": page_result.get("response_body"),
                    "retry_count": total_retry_count,
                    "duration_ms": total_duration_ms,
                    "orders": orders,
                }

            orders.extend(page_result.get("items", []))
            if len(orders) >= limit:
                orders = orders[:limit]
                break

        response_body = last_result.get("response_body")
        return {
            "success": True,
            "message": "Orders fetched successfully.",
            "status_code": last_result.get("status_code") if last_result else 200,
            "url": last_result.get("url") if last_result else self.client._build_url("/v2/orders"),
            "response_body": response_body,
            "retry_count": total_retry_count,
            "duration_ms": total_duration_ms,
            "orders": orders,
        }

    def _fetch_order_products(self, order_id):
        """Fetch every BigCommerce order product line for a given order id.

        GET /v2/orders/{id}/products returns 50 lines per page by default, so the lines are read
        page by page (limit=250) until BigCommerce answers with an empty page or HTTP 204.
        """
        result = self.client.get_paginated(
            "/v2/orders/%s/products" % order_id,
            limit=250,
        )
        if not result.get("success"):
            return {
                "success": False,
                "message": result.get("message") or "BigCommerce order products request failed.",
                "status_code": result.get("status_code"),
                "url": result.get("url"),
                "response_body": result.get("response_body"),
                "lines": [],
            }

        lines = result.get("items") or []
        return {
            "success": True,
            "message": "Order products fetched successfully.",
            "status_code": result.get("status_code"),
            "url": result.get("url"),
            "response_body": result.get("response_body"),
            "lines": lines,
        }

    def _find_existing_order_binding(self, bc_order):
        """Find an existing order binding for the BigCommerce order id."""
        remote_id = self._as_remote_id(bc_order)
        if not remote_id:
            return self.env["bigcommerce.order.binding"].sudo()

        if remote_id in self._order_binding_cache:
            return self._order_binding_cache[remote_id]

        binding = self.env["bigcommerce.order.binding"].sudo().search(
            [
                ("instance_id", "=", self.instance.id),
                ("bigcommerce_order_id", "=", remote_id),
            ],
            limit=1,
        )
        self._order_binding_cache[remote_id] = binding
        return binding

    def _find_or_create_customer(self, bc_order):
        """Resolve partner by customer binding, then email, otherwise create a safe contact."""
        partner_model = self.env["res.partner"].with_context(active_test=False)

        remote_customer_id = self._remote_customer_id(bc_order)
        if remote_customer_id:
            cached_partner = self._partner_by_remote_customer_cache.get(remote_customer_id)
            if remote_customer_id in self._partner_by_remote_customer_cache:
                return cached_partner if cached_partner else partner_model
            customer_binding = self.env["bigcommerce.customer.binding"].sudo().search(
                [
                    ("instance_id", "=", self.instance.id),
                    ("bigcommerce_customer_id", "=", remote_customer_id),
                ],
                limit=1,
            )
            if customer_binding and customer_binding.partner_id and customer_binding.partner_id.company_id in (
                self.env["res.company"], self.instance.company_id
            ):
                partner = customer_binding.partner_id.with_env(self.env)
                partner = self._refresh_partner_identity_from_order(partner, bc_order)
                self._partner_by_remote_customer_cache[remote_customer_id] = partner
                return partner
            self._partner_by_remote_customer_cache[remote_customer_id] = False

        email = self._extract_order_email(bc_order)
        if email:
            cached_partner = self._partner_by_email_cache.get(email)
            if email in self._partner_by_email_cache:
                return cached_partner if cached_partner else partner_model
            email_candidates = partner_model.search(
                [("email", "=ilike", email), ("company_id", "in", [False, self.instance.company_id.id])], limit=10
            )
            exact_email_matches = email_candidates.filtered(
                lambda partner: self._normalized_email(partner.email) == email
            )
            if len(exact_email_matches) == 1:
                partner = self._refresh_partner_identity_from_order(exact_email_matches[0], bc_order)
                self._partner_by_email_cache[email] = partner
                if remote_customer_id:
                    binding_model = self.env["bigcommerce.customer.binding"].sudo()
                    existing_binding = binding_model.search(
                        [
                            ("instance_id", "=", self.instance.id),
                            ("bigcommerce_customer_id", "=", remote_customer_id),
                        ],
                        limit=1,
                    )
                    binding_vals = {
                        "instance_id": self.instance.id,
                        "partner_id": partner.id,
                        "bigcommerce_customer_id": remote_customer_id,
                        "email": email,
                        "sync_state": "synced",
                        "last_synced_at": fields.Datetime.now(),
                        "last_error": False,
                    }
                    if existing_binding:
                        existing_binding.write(binding_vals)
                    else:
                        binding_model.create(binding_vals)
                    self._partner_by_remote_customer_cache[remote_customer_id] = partner
                return partner
            self._partner_by_email_cache[email] = False

        customer_name = self._customer_name_from_order(bc_order)
        phone = self._customer_phone_from_order(bc_order)
        partner_vals = {
            "name": customer_name or "BigCommerce Customer",
            "email": email or False,
            "phone": phone or False,
            "active": True,
        }
        partner = self.env["res.partner"].create(partner_vals)

        if remote_customer_id:
            binding_model = self.env["bigcommerce.customer.binding"].sudo()
            existing_binding = binding_model.search(
                [
                    ("instance_id", "=", self.instance.id),
                    ("bigcommerce_customer_id", "=", remote_customer_id),
                ],
                limit=1,
            )
            binding_vals = {
                "instance_id": self.instance.id,
                "partner_id": partner.id,
                "bigcommerce_customer_id": remote_customer_id,
                "email": email or False,
                "sync_state": "synced",
                "last_synced_at": fields.Datetime.now(),
                "last_error": False,
            }
            if existing_binding:
                existing_binding.write(binding_vals)
            else:
                binding_model.create(binding_vals)
            self._partner_by_remote_customer_cache[remote_customer_id] = partner
        if email:
            self._partner_by_email_cache[email] = partner

        return partner

    def _refresh_partner_identity_from_order(self, partner, bc_order):
        """Backfill partner name/phone from order payload when current values are placeholders."""
        if not partner:
            return partner

        customer_name = self._customer_name_from_order(bc_order)
        customer_phone = self._customer_phone_from_order(bc_order)

        current_name = (partner.name or "").strip()
        current_phone = (partner.phone or "").strip()
        normalized_email = self._normalized_email(partner.email)
        normalized_name_as_email = self._normalized_email(current_name)

        vals = {}
        if (
            customer_name
            and customer_name != "BigCommerce Customer"
            and (
                not current_name
                or (normalized_email and normalized_name_as_email == normalized_email)
            )
        ):
            vals["name"] = customer_name
        if customer_phone and not current_phone:
            vals["phone"] = customer_phone

        if vals:
            partner.sudo().write(vals)
            return partner.with_env(self.env)
        return partner

    def _find_product_for_line(self, bc_line):
        """Resolve product by product binding first, then by SKU default_code."""
        remote_product_id = self._line_remote_product_id(bc_line)
        remote_variant_id = self._line_remote_variant_id(bc_line)
        sku = (bc_line.get("sku") or "").strip()

        if remote_variant_id and remote_variant_id in self._product_by_remote_variant_cache:
            return self._product_by_remote_variant_cache[remote_variant_id]
        if remote_product_id and remote_product_id in self._product_by_remote_product_cache:
            return self._product_by_remote_product_cache[remote_product_id]
        if sku and sku in self._product_by_sku_cache:
            return self._product_by_sku_cache[sku]

        binding_model = self.env["bigcommerce.product.binding"].sudo()
        binding = self.env["bigcommerce.product.binding"]

        if remote_variant_id:
            binding = binding_model.search(
                [
                    ("instance_id", "=", self.instance.id),
                    ("bigcommerce_variant_id", "=", remote_variant_id),
                ],
                limit=1,
            )
        if not binding and remote_product_id:
            binding = binding_model.search(
                [
                    ("instance_id", "=", self.instance.id),
                    ("bigcommerce_product_id", "=", remote_product_id),
                ],
                limit=1,
            )

        if binding:
            if binding.product_id:
                product = binding.product_id.with_env(self.env)
                self._product_by_remote_product_cache[remote_product_id] = product
                if remote_variant_id:
                    self._product_by_remote_variant_cache[remote_variant_id] = product
                if sku:
                    self._product_by_sku_cache[sku] = product
                return product
            if binding.product_tmpl_id and binding.product_tmpl_id.product_variant_id:
                product = binding.product_tmpl_id.product_variant_id.with_env(self.env)
                self._product_by_remote_product_cache[remote_product_id] = product
                if remote_variant_id:
                    self._product_by_remote_variant_cache[remote_variant_id] = product
                if sku:
                    self._product_by_sku_cache[sku] = product
                return product

        if sku:
            product = self._find_product_by_sku(sku)
            if product:
                self._product_by_sku_cache[sku] = product
                if remote_variant_id:
                    self._product_by_remote_variant_cache[remote_variant_id] = product
                if remote_product_id:
                    self._product_by_remote_product_cache[remote_product_id] = product
                return product

        empty_product = self.env["product.product"]
        if remote_variant_id:
            self._product_by_remote_variant_cache[remote_variant_id] = empty_product
        if remote_product_id:
            self._product_by_remote_product_cache[remote_product_id] = empty_product
        if sku:
            self._product_by_sku_cache[sku] = empty_product
        return empty_product

    def _find_product_by_sku(self, sku):
        """Resolve product by SKU with case-insensitive and template fallback logic."""
        sku = (sku or "").strip()
        if not sku:
            return self.env["product.product"]

        product_model = self.env["product.product"].with_context(active_test=False)
        template_model = self.env["product.template"].with_context(active_test=False)

        candidates = product_model.search([("default_code", "ilike", sku)], limit=25)
        exact = candidates.filtered(lambda rec: (rec.default_code or "").strip().lower() == sku.lower())
        if len(exact) == 1:
            return exact[0]
        if len(candidates) == 1:
            return candidates[0]

        tmpl_candidates = template_model.search([("default_code", "ilike", sku)], limit=25)
        tmpl_exact = tmpl_candidates.filtered(lambda rec: (rec.default_code or "").strip().lower() == sku.lower())
        if len(tmpl_exact) == 1 and tmpl_exact[0].product_variant_id:
            return tmpl_exact[0].product_variant_id
        if len(tmpl_candidates) == 1 and tmpl_candidates[0].product_variant_id:
            return tmpl_candidates[0].product_variant_id

        return self.env["product.product"]

    def _prepare_sale_order_vals(self, bc_order, partner):
        """Prepare safe sale.order values from BigCommerce order payload."""
        remote_id = self._as_remote_id(bc_order)
        order_number = self._order_number(bc_order)
        bc_created_at = (
            bc_order.get("date_created")
            or bc_order.get("created_at")
            or bc_order.get("date_modified")
            or bc_order.get("updated_at")
        )

        note_parts = ["BigCommerce Order ID: %s" % (remote_id or "N/A")]
        customer_message = (bc_order.get("customer_message") or "").strip()
        if customer_message:
            note_parts.append("Customer Message: %s" % customer_message)

        vals = {
            "partner_id": partner.id,
            "partner_invoice_id": partner.id,
            "partner_shipping_id": partner.id,
            "company_id": self.instance.company_id.id,
            "client_order_ref": str(order_number or remote_id or ""),
            "origin": "BigCommerce #%s" % (order_number or remote_id or "N/A"),
            "note": "\n".join(note_parts),
        }

        parsed_date = self._parse_bc_datetime(bc_created_at)
        if parsed_date:
            vals["date_order"] = parsed_date
        pricelist = self._resolve_pricelist(bc_order)
        if pricelist:
            vals["pricelist_id"] = pricelist.id
        if self.instance.warehouse_id:
            vals["warehouse_id"] = self.instance.warehouse_id.id

        mapping_result = self.mapping_model._prepare_odoo_vals_from_mapping(
            payload=bc_order,
            mapping_type="order",
            connector=self.instance,
            direction="import",
            raise_on_required=False,
        )
        vals.update(mapping_result.get("vals") or {})

        return vals

    def _resolve_pricelist(self, bc_order):
        """Pricelist whose currency is the one BigCommerce charged in.

        The connector pricelist is used when it is in that currency (or the order states none);
        otherwise an active pricelist of the company in the order currency, if any. When nothing
        matches, Odoo's default applies and the order is flagged "Currency differs".
        """
        configured = self.instance.pricelist_id
        code = self._extract_order_currency_code(bc_order)
        if not code:
            return configured
        if configured and configured.currency_id.name == code:
            return configured
        return self.env["product.pricelist"].search(
            [
                ("currency_id.name", "=", code),
                ("company_id", "in", [False, self.instance.company_id.id]),
            ],
            order="company_id, sequence, id",
            limit=1,
        )

    def _order_charged_no_tax(self, bc_order):
        """True when BigCommerce charged no tax on the order.

        ``total_tax`` decides when present; otherwise the order is tax free when
        ``total_inc_tax`` equals ``total_ex_tax``. Unknown (neither available) counts as taxed.
        """
        if not isinstance(bc_order, dict):
            return False
        total_tax = self._safe_float(bc_order.get("total_tax"), default=None)
        if total_tax is not None:
            return float_is_zero(total_tax, precision_digits=4)
        total_inc = self._safe_float(bc_order.get("total_inc_tax"), default=None)
        total_ex = self._safe_float(bc_order.get("total_ex_tax"), default=None)
        if total_inc is None or total_ex is None:
            return False
        return float_is_zero(total_inc - total_ex, precision_digits=4)

    def _line_tax_vals(self, bc_order):
        """Tax values for every imported line (product, shipping, discount).

        BigCommerce is the source of truth for what the customer was charged:

        * no tax charged (see ``_order_charged_no_tax``): the lines get no taxes, so Odoo does
          not add its product default taxes (e.g. 15%) and the Odoo total equals the BigCommerce
          total (found in the live test: order #100, 104.85 became 120.58);
        * tax charged: Odoo's product taxes and the order fiscal position apply as before. Odoo
          cannot reproduce BigCommerce's tax engine line by line, so any difference is reported
          by the "Total differs" exception flag of the order binding (beyond the connector's
          order total tolerance) for review.
        """
        if self._order_charged_no_tax(bc_order):
            return {"tax_ids": [Command.clear()]}
        return {}

    def _prepare_sale_order_line_vals(self, bc_line, product, sale_order, bc_order=None):
        """Prepare safe sale.order.line values from a BigCommerce order line.

        ``bc_order`` (the order payload) decides the line taxes, see ``_line_tax_vals``.
        """
        quantity = self._safe_float(
            bc_line.get("quantity"),
            default=0.0,
        )
        if quantity <= 0:
            quantity = 1.0

        price_unit = self._safe_float(
            bc_line.get("price_ex_tax"),
            default=None,
        )
        if price_unit is None:
            price_unit = self._safe_float(bc_line.get("price_inc_tax"), default=None)
        if price_unit is None:
            price_unit = self._safe_float(bc_line.get("base_price"), default=None)
        if price_unit is None:
            price_unit = self._safe_float(bc_line.get("price"), default=0.0)

        line_name = (bc_line.get("name") or "").strip() or product.display_name

        vals = {
            "order_id": sale_order.id,
            "product_id": product.id,
            "name": line_name,
            "product_uom_qty": quantity,
            "price_unit": price_unit,
            "product_uom_id": product.uom_id.id,
        }
        vals.update(self._line_tax_vals(bc_order))
        mapping_result = self.mapping_model._prepare_odoo_vals_from_mapping(
            payload=bc_line,
            mapping_type="order_line",
            connector=self.instance,
            direction="import",
            raise_on_required=False,
        )
        vals.update(mapping_result.get("vals") or {})
        return vals

    def _create_or_update_sale_order(self, bc_order):
        """Create or update the sale order of a BigCommerce order, then follow its status.

        Quotations (draft/sent) are fully refreshed (header and lines), then brought to the
        state the BigCommerce status maps to (see _apply_bigcommerce_state). A confirmed or
        cancelled order keeps its lines: a later sync only follows the status (e.g. a
        cancellation) instead of failing.
        """
        remote_id = self._as_remote_id(bc_order)
        if not remote_id:
            raise ValueError("BigCommerce order id is missing in response payload.")
        self._placeholder_used = None

        binding = self._find_existing_order_binding(bc_order)
        partner = self._find_or_create_customer(bc_order)
        sale_order_vals = self._prepare_sale_order_vals(bc_order, partner)
        # The status mapping gives the target state; it is reached through the sale workflow.
        target_state = sale_order_vals.pop("state", False)
        order_number = self._order_number(bc_order)

        was_created = False
        if binding and binding.sale_order_id:
            sale_order = binding.sale_order_id.with_env(self.env)
        else:
            sale_order = self._find_existing_sale_order(order_number)
        if sale_order and sale_order.state not in ("draft", "sent"):
            _logger.info(
                "BigCommerce order %s: sale order %s is %s, lines not refreshed; following status %r.",
                remote_id, sale_order.name, sale_order.state, bc_order.get("status"),
            )
            self._apply_bigcommerce_state(sale_order, target_state)
            return sale_order, False
        if sale_order:
            sale_order.write(sale_order_vals)
        else:
            sale_order = self.env["sale.order"].create(sale_order_vals)
            was_created = True

        lines_result = self._fetch_order_products(remote_id)
        if not lines_result.get("success"):
            raise ValueError(lines_result.get("message") or "Unable to fetch order lines from BigCommerce.")

        bc_lines = lines_result.get("lines", [])
        if not bc_lines:
            raise ValueError("No order lines were returned for BigCommerce order %s." % remote_id)

        if sale_order.order_line:
            sale_order.order_line.unlink()

        self._placeholder_used = False
        for bc_line in bc_lines:
            product = self._find_product_for_line(bc_line)
            if not product:
                self._placeholder_used = True
                product = self._handle_missing_product(bc_line)
                if not product:
                    raise ValueError(
                        "Unable to resolve product for order line '%s' (product_id=%s, sku=%s)."
                        % (
                            (bc_line.get("name") or "Unknown"),
                            (bc_line.get("product_id") or "N/A"),
                            (bc_line.get("sku") or "N/A"),
                        )
                    )
            line_vals = self._prepare_sale_order_line_vals(bc_line, product, sale_order, bc_order=bc_order)
            self.env["sale.order.line"].create(line_vals)

        self._add_shipping_line(sale_order, bc_order)
        self._add_discount_line(sale_order, bc_order)
        self._apply_bigcommerce_state(sale_order, target_state, order_date=sale_order_vals.get("date_order"))

        return sale_order, was_created

    def _apply_bigcommerce_state(self, sale_order, target_state, order_date=False):
        """Bring the sale order to the state its BigCommerce status maps to.

        Only "sale" (action_confirm) and "cancel" (action_cancel) are applied; any other
        target, e.g. "draft" for orders still awaiting payment, changes nothing, and a
        confirmed order is never moved back to a quotation. Cancelling a locked order raises
        Odoo's UserError, which fails this order's sync with that message.
        """
        if target_state == "sale" and sale_order.state in ("draft", "sent"):
            sale_order.action_confirm()
            if order_date:
                # action_confirm() stamps the confirmation time; keep the BigCommerce order date.
                sale_order.write({"date_order": order_date})
        elif target_state == "cancel" and sale_order.state in ("draft", "sent", "sale"):
            sale_order.with_context(disable_cancel_warning=True).action_cancel()

    def _create_or_update_binding(self, sale_order, bc_order):
        """Create or update BigCommerce order binding for a sale order."""
        now = fields.Datetime.now()
        remote_id = self._as_remote_id(bc_order)
        if not remote_id:
            raise ValueError("BigCommerce order id is missing in response payload.")

        vals = {
            "instance_id": self.instance.id,
            "sale_order_id": sale_order.id,
            "bigcommerce_order_id": remote_id,
            "bigcommerce_order_number": self._order_number(bc_order) or False,
            "bigcommerce_total_amount": self._extract_order_total(bc_order),
            "bigcommerce_currency_code": self._extract_order_currency_code(bc_order),
            "status_on_bigcommerce": str(bc_order.get("status") or bc_order.get("status_id") or ""),
            "sync_state": "synced",
            "imported_at": now,
            "last_error": False,
        }
        if getattr(self, "_placeholder_used", None) is not None:
            vals["has_placeholder_lines"] = bool(self._placeholder_used)
            self._placeholder_used = None

        binding_model = self.env["bigcommerce.order.binding"].sudo()
        binding = binding_model.search(
            [
                ("instance_id", "=", self.instance.id),
                ("bigcommerce_order_id", "=", remote_id),
            ],
            limit=1,
        )
        if binding:
            binding.write(vals)
            self._order_binding_cache[remote_id] = binding
            return binding

        binding = binding_model.create(vals)
        self._order_binding_cache[remote_id] = binding
        return binding

    def _log_success(
        self,
        message,
        request_url=False,
        response_status=False,
        response_body=None,
        resource_remote_id=False,
        note=False,
        retry_count=0,
        duration_ms=0,
    ):
        """Create a success sync log record for order import."""
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "order_import",
                "resource_type": "order",
                "resource_remote_id": resource_remote_id,
                "request_url": request_url,
                "request_method": "GET",
                "response_status": self._as_response_status(response_status),
                "response_body": self._as_response_body(response_body),
                "retry_count": int(retry_count or 0),
                "duration_ms": float(duration_ms or 0.0),
                "severity": "info",
                "status": "success",
                "note": note or message,
            }
        )

    def _log_failure(
        self,
        message,
        request_url=False,
        response_status=False,
        response_body=None,
        resource_remote_id=False,
        note=False,
        retry_count=0,
        duration_ms=0,
    ):
        """Create a failure sync log record for order import."""
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "order_import",
                "resource_type": "order",
                "resource_remote_id": resource_remote_id,
                "request_url": request_url,
                "request_method": "GET",
                "response_status": self._as_response_status(response_status),
                "response_body": self._as_response_body(response_body),
                "retry_count": int(retry_count or 0),
                "duration_ms": float(duration_ms or 0.0),
                "severity": "error",
                "status": "failed",
                "error_message": message,
                "note": note,
            }
        )

    def _extract_order_email(self, bc_order):
        email = (
            bc_order.get("billing_address", {}).get("email")
            if isinstance(bc_order.get("billing_address"), dict)
            else False
        )
        if not email:
            email = (
                bc_order.get("shipping_address", {}).get("email")
                if isinstance(bc_order.get("shipping_address"), dict)
                else False
            )
        if not email:
            email = bc_order.get("email") or bc_order.get("customer_email")
        return self._normalized_email(email)

    def _customer_name_from_order(self, bc_order):
        billing = bc_order.get("billing_address")
        if isinstance(billing, dict):
            first_name = (billing.get("first_name") or "").strip()
            last_name = (billing.get("last_name") or "").strip()
            full_name = " ".join([value for value in [first_name, last_name] if value]).strip()
            if full_name:
                return full_name

        shipping = bc_order.get("shipping_address")
        if isinstance(shipping, dict):
            first_name = (shipping.get("first_name") or "").strip()
            last_name = (shipping.get("last_name") or "").strip()
            full_name = " ".join([value for value in [first_name, last_name] if value]).strip()
            if full_name:
                return full_name

        return "BigCommerce Customer"

    def _customer_phone_from_order(self, bc_order):
        billing = bc_order.get("billing_address")
        if isinstance(billing, dict) and billing.get("phone"):
            return (billing.get("phone") or "").strip()

        shipping = bc_order.get("shipping_address")
        if isinstance(shipping, dict) and shipping.get("phone"):
            return (shipping.get("phone") or "").strip()

        return (bc_order.get("phone") or "").strip() or False

    def _remote_customer_id(self, bc_order):
        value = bc_order.get("customer_id")
        if value in (None, False, "", 0):
            return False
        return str(value)

    def _line_remote_product_id(self, bc_line):
        value = bc_line.get("product_id")
        if value in (None, False, "", 0):
            return False
        return str(value)

    def _line_remote_variant_id(self, bc_line):
        value = bc_line.get("variant_id")
        if value in (None, False, "", 0):
            return False
        return str(value)

    def _add_shipping_line(self, sale_order, bc_order):
        """Add a simple shipping line when order-level shipping amount exists."""
        shipping_amount = self._safe_float(
            bc_order.get("shipping_cost_ex_tax"),
            default=None,
        )
        if shipping_amount is None:
            shipping_amount = self._safe_float(bc_order.get("shipping_cost_inc_tax"), default=None)
        if shipping_amount is None or shipping_amount == 0:
            return

        shipping_product = self._get_or_create_service_product(
            default_code="BIGCOMMERCE_SHIPPING",
            name="BigCommerce Shipping",
        )
        self.env["sale.order.line"].create(
            {
                "order_id": sale_order.id,
                "product_id": shipping_product.id,
                "name": "Shipping",
                "product_uom_qty": 1.0,
                "price_unit": shipping_amount,
                "product_uom_id": shipping_product.uom_id.id,
                **self._line_tax_vals(bc_order),
            }
        )

    def _add_discount_line(self, sale_order, bc_order):
        """Add a simple discount line with negative amount when provided."""
        # BigCommerce reports manual discounts and coupon discounts separately; both reduce the total.
        discount_amount = abs(self._safe_float(bc_order.get("discount_amount"), default=0.0)) + abs(
            self._safe_float(bc_order.get("coupon_discount"), default=0.0)
        )
        if not discount_amount:
            return

        discount_product = self._get_or_create_service_product(
            default_code="BIGCOMMERCE_DISCOUNT",
            name="BigCommerce Discount",
        )
        self.env["sale.order.line"].create(
            {
                "order_id": sale_order.id,
                "product_id": discount_product.id,
                "name": "Discount",
                "product_uom_qty": 1.0,
                "price_unit": -abs(discount_amount),
                "product_uom_id": discount_product.uom_id.id,
                **self._line_tax_vals(bc_order),
            }
        )

    def _get_or_create_service_product(self, default_code, name):
        product = self.env["product.product"].with_context(active_test=False).search(
            [
                ("default_code", "=", default_code),
                ("type", "=", "service"),
                ("company_id", "in", [False, self.instance.company_id.id]),
            ],
            limit=1,
        )
        if product:
            return product

        unit_uom = self.env.ref("uom.product_uom_unit")
        # Odoo 19 has neither the purchase UoM field (uom_po_id) nor the "All" product category
        # record (product.product_category_all) any more: referencing them made every order with
        # shipping, a discount or an unknown product fail before 19.0.2.1.0. The default category
        # applies.
        template = self.env["product.template"].create(
            {
                "name": name,
                "default_code": default_code,
                "type": "service",
                "sale_ok": True,
                "purchase_ok": False,
                "uom_id": unit_uom.id,
                "list_price": 0.0,
            }
        )
        return template.product_variant_id

    def _handle_missing_product(self, bc_line):
        """Create/reuse safe placeholder product so order import does not fail completely."""
        sku = (bc_line.get("sku") or "").strip()
        line_name = (bc_line.get("name") or "").strip()
        remote_product_id = self._line_remote_product_id(bc_line) or "N/A"

        # Never reuse the real SKU as the placeholder code: the placeholder would later be
        # mistaken for the real product by SKU matching.
        default_code = "BC-MISSING-%s" % (sku or remote_product_id)
        name = line_name or ("BigCommerce Item %s" % remote_product_id)
        placeholder_name = "[BigCommerce] %s" % name

        _logger.warning(
            "Order line product unresolved for instance_id=%s product_id=%s sku=%s; using placeholder product code=%s",
            self.instance.id,
            remote_product_id,
            sku or "N/A",
            default_code,
        )

        return self._get_or_create_service_product(default_code=default_code, name=placeholder_name)

    def _find_existing_sale_order(self, order_number):
        """Adopt a sale order created for this order number before its binding existed.

        Only an order that no BigCommerce binding claims is adopted: every store numbers its orders
        from 100, so with two stores in one company, the second store's order #100 used to be
        merged into the first store's sale order #100 (before 19.0.2.1.0).
        """
        if not order_number:
            return self.env["sale.order"]
        bound_ids = self.env["bigcommerce.order.binding"].sudo().search(
            [("sale_order_id", "!=", False), ("bigcommerce_order_number", "=", order_number)]
        ).sale_order_id.ids
        domain = [
            ("company_id", "=", self.instance.company_id.id),
            ("id", "not in", bound_ids),
            "|",
            ("client_order_ref", "=", order_number),
            ("origin", "=", "BigCommerce #%s" % order_number),
        ]
        return self.env["sale.order"].search(domain, limit=1)

    def _resolve_batch_size(self, limit):
        return max(20, min(100, int(limit or 20)))

    def _iter_batches(self, records, batch_size):
        records = records or []
        for index in range(0, len(records), batch_size):
            yield records[index : index + batch_size]

    def _order_number(self, bc_order):
        value = bc_order.get("order_number") or bc_order.get("id")
        if value in (None, False, ""):
            return False
        return str(value)

    def _extract_order_total(self, bc_order):
        if not isinstance(bc_order, dict):
            return 0.0
        total_candidates = (
            bc_order.get("total_inc_tax"),
            bc_order.get("total"),
            bc_order.get("total_ex_tax"),
            bc_order.get("subtotal_inc_tax"),
            bc_order.get("subtotal_ex_tax"),
            bc_order.get("subtotal"),
        )
        for candidate in total_candidates:
            value = self._safe_float(candidate, default=None)
            if value is not None:
                return value
        return 0.0

    def _extract_order_currency_code(self, bc_order):
        if not isinstance(bc_order, dict):
            return False
        candidates = (
            bc_order.get("currency_code"),
            bc_order.get("default_currency_code"),
            bc_order.get("store_default_currency_code"),
        )
        for candidate in candidates:
            value = (candidate or "").strip().upper()
            if value:
                return value
        return False

    def _parse_bc_datetime(self, value):
        if not value:
            return False
        try:
            dt = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            dt = False
        if not dt:
            return False
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return fields.Datetime.to_string(dt)

    def _safe_float(self, value, default=0.0):
        if value in (None, False, ""):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _normalized_email(self, email):
        if not email:
            return False
        value = str(email).strip().lower()
        return value or False

    def _as_response_status(self, response_status):
        if response_status in (None, False, ""):
            return False
        return str(response_status)

    def _as_response_body(self, response_body):
        if response_body in (None, False, ""):
            return False
        return str(response_body)[:3000]

    def _as_remote_id(self, bc_order):
        if not isinstance(bc_order, dict):
            return False
        value = bc_order.get("id")
        if value in (None, False, ""):
            return False
        return str(value)
