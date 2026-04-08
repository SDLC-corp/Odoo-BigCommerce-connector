# -*- coding: utf-8 -*-

import logging
from datetime import timezone
from email.utils import parsedate_to_datetime

from odoo import fields

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

        fetch_result = self._fetch_orders(limit=limit)
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
            message = "Order import completed. No orders returned from BigCommerce."
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
        for batch in self._iter_batches(orders, batch_size):
            for bc_order in batch:
                try:
                    sale_order, was_created = self._create_or_update_sale_order(bc_order)
                    self._create_or_update_binding(sale_order, bc_order)
                    if was_created:
                        created += 1
                    else:
                        updated += 1
                except Exception as err:
                    failed += 1
                    remote_id = self._as_remote_id(bc_order)
                    order_number = self._order_number(bc_order)
                    error_text = str(err)
                    item_label = order_number or ("ID %s" % remote_id if remote_id else "Unknown Order")
                    failed_items.append("%s: %s" % (item_label, error_text))

                    _logger.exception(
                        "BigCommerce order import failed for instance_id=%s remote_id=%s order_number=%s",
                        self.instance.id,
                        remote_id,
                        order_number,
                    )

                    self._log_failure(
                        message=error_text,
                        resource_remote_id=remote_id,
                        note="Order '%s' failed." % (order_number or "Unknown"),
                    )

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

    def import_order_by_id(self, order_id):
        """Import a single BigCommerce order by id (webhook-safe helper)."""
        result = self.client.get("/v2/orders/%s" % order_id, timeout=25)
        if not result.get("success"):
            return {"success": False, "message": result.get("message") or "Failed to fetch order by id."}

        body = result.get("response_body")
        if not isinstance(body, dict):
            return {"success": False, "message": "Unexpected order payload format."}

        sale_order, _ = self._create_or_update_sale_order(body)
        self._create_or_update_binding(sale_order, body)
        return {"success": True, "message": "Order %s synced." % order_id}

    def _fetch_orders(self, limit=20):
        """Fetch orders from BigCommerce orders API."""
        orders = []
        page_limit = max(20, min(100, int(limit or 20)))
        total_retry_count = 0
        total_duration_ms = 0
        last_result = {}

        for page_result in self.client.iter_paginated(
            "/v2/orders",
            params={},
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
        """Fetch BigCommerce order product lines for a given order id."""
        result = self.client.get(
            "/v2/orders/%s/products" % order_id,
            timeout=25,
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

        response_body = result.get("response_body")
        lines = []
        if isinstance(response_body, list):
            lines = response_body
        elif isinstance(response_body, dict) and isinstance(response_body.get("data"), list):
            lines = response_body.get("data")

        if not isinstance(lines, list):
            lines = []

        return {
            "success": True,
            "message": "Order products fetched successfully.",
            "status_code": result.get("status_code"),
            "url": result.get("url"),
            "response_body": response_body,
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
            if customer_binding and customer_binding.partner_id:
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
            email_candidates = partner_model.search([("email", "ilike", email)], limit=10)
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

        mapping_result = self.mapping_model._prepare_odoo_vals_from_mapping(
            payload=bc_order,
            mapping_type="order",
            connector=self.instance,
            direction="import",
            raise_on_required=False,
        )
        vals.update(mapping_result.get("vals") or {})

        return vals

    def _prepare_sale_order_line_vals(self, bc_line, product, sale_order):
        """Prepare safe sale.order.line values from a BigCommerce order line."""
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
            "product_uom": product.uom_id.id,
        }
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
        """Create or update sale.order in draft state and fully refresh order lines."""
        remote_id = self._as_remote_id(bc_order)
        if not remote_id:
            raise ValueError("BigCommerce order id is missing in response payload.")

        binding = self._find_existing_order_binding(bc_order)
        partner = self._find_or_create_customer(bc_order)
        sale_order_vals = self._prepare_sale_order_vals(bc_order, partner)
        order_number = self._order_number(bc_order)

        was_created = False
        if binding and binding.sale_order_id:
            sale_order = binding.sale_order_id.with_env(self.env)
            if sale_order.state not in ("draft", "sent"):
                raise ValueError(
                    "Sale order %s is in state '%s' and cannot be updated by sync."
                    % (sale_order.name, sale_order.state)
                )
            sale_order.write(sale_order_vals)
        else:
            sale_order = self._find_existing_sale_order(order_number)
            if sale_order:
                if sale_order.state not in ("draft", "sent"):
                    raise ValueError(
                        "Sale order %s is in state '%s' and cannot be updated by sync."
                        % (sale_order.name, sale_order.state)
                    )
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

        for bc_line in bc_lines:
            product = self._find_product_for_line(bc_line)
            if not product:
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
            line_vals = self._prepare_sale_order_line_vals(bc_line, product, sale_order)
            self.env["sale.order.line"].create(line_vals)

        self._add_shipping_line(sale_order, bc_order)
        self._add_discount_line(sale_order, bc_order)

        return sale_order, was_created

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
                "product_uom": shipping_product.uom_id.id,
            }
        )

    def _add_discount_line(self, sale_order, bc_order):
        """Add a simple discount line with negative amount when provided."""
        discount_amount = self._safe_float(bc_order.get("discount_amount"), default=0.0)
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
                "product_uom": discount_product.uom_id.id,
            }
        )

    def _get_or_create_service_product(self, default_code, name):
        product = self.env["product.product"].search([("default_code", "=", default_code)], limit=1)
        if product:
            return product

        unit_uom = self.env.ref("uom.product_uom_unit")
        category = self.env.ref("product.product_category_all")
        template = self.env["product.template"].create(
            {
                "name": name,
                "default_code": default_code,
                "type": "service",
                "sale_ok": True,
                "purchase_ok": False,
                "uom_id": unit_uom.id,
                "uom_po_id": unit_uom.id,
                "categ_id": category.id,
                "list_price": 0.0,
            }
        )
        return template.product_variant_id

    def _handle_missing_product(self, bc_line):
        """Create/reuse safe placeholder product so order import does not fail completely."""
        sku = (bc_line.get("sku") or "").strip()
        line_name = (bc_line.get("name") or "").strip()
        remote_product_id = self._line_remote_product_id(bc_line) or "N/A"

        default_code = sku or ("BC-MISSING-%s" % remote_product_id)
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
        if not order_number:
            return self.env["sale.order"]
        domain = [
            ("company_id", "=", self.instance.company_id.id),
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
