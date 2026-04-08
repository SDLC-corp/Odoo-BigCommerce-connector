# -*- coding: utf-8 -*-

import logging

from odoo import fields

from .api_client import BigCommerceApiClient

_logger = logging.getLogger(__name__)


class BigCommerceCustomerSyncService:
    """Service layer for manual BigCommerce customer import."""

    def __init__(self, instance):
        self.instance = instance
        self.env = instance.env
        self.client = BigCommerceApiClient(instance)
        self.mapping_model = self.env["bigcommerce.field.mapping"]
        self._binding_by_remote_id_cache = {}
        self._partner_by_email_cache = {}
        self._partner_by_name_cache = {}

    def import_customers(self, limit=50):
        """Import customers from BigCommerce into Odoo contacts."""
        limit = int(limit or 50)
        if limit < 1:
            limit = 1
        if limit > 250:
            limit = 250

        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "customer_import",
                "resource_type": "customer",
                "request_method": "GET",
                "request_url": self.client._build_url("/v3/customers"),
                "status": "draft",
                "note": "Manual customer import started (limit=%s)." % limit,
            }
        )

        fetch_result = self._fetch_customers(limit=limit)
        if not fetch_result.get("success"):
            message = fetch_result.get("message") or "Unable to fetch customers from BigCommerce."
            self._log_failure(
                message=message,
                request_url=fetch_result.get("url"),
                response_status=fetch_result.get("status_code"),
                response_body=fetch_result.get("response_body"),
                retry_count=fetch_result.get("retry_count"),
                duration_ms=fetch_result.get("duration_ms"),
                note="Customer fetch failed before import started.",
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

        customers = fetch_result.get("customers", [])
        if not customers:
            message = "Customer import completed. No customers returned from BigCommerce."
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
        for batch in self._iter_batches(customers, batch_size):
            self._prime_customer_caches(batch)
            for bc_customer in batch:
                try:
                    partner, was_created = self._create_or_update_partner(bc_customer)
                    self._create_or_update_binding(partner, bc_customer)
                    if was_created:
                        created += 1
                    else:
                        updated += 1
                except Exception as err:
                    failed += 1
                    remote_id = self._as_remote_id(bc_customer)
                    name = self._display_name(bc_customer)
                    email = self._normalized_email(bc_customer.get("email"))
                    error_text = str(err)
                    item_label = name or ("ID %s" % remote_id if remote_id else "Unknown Customer")
                    failed_items.append("%s: %s" % (item_label, error_text))

                    _logger.exception(
                        "BigCommerce customer import failed for instance_id=%s remote_id=%s email=%s",
                        self.instance.id,
                        remote_id,
                        email,
                    )

                    self._log_failure(
                        message=error_text,
                        resource_remote_id=remote_id,
                        note="Customer '%s' email '%s' failed." % (name or "Unknown", email or "N/A"),
                    )

        total = len(customers)
        success = failed == 0
        message = (
            "Customer import completed. Total: %(total)s, Created: %(created)s, "
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

    def export_customers(self, limit=50):
        """Export Odoo contacts to BigCommerce (simple optional push)."""
        limit = int(limit or 50)
        partners = self.env["res.partner"].search(
            [("type", "=", "contact"), ("email", "!=", False)],
            limit=max(1, limit),
            order="write_date desc,id desc",
        )
        exported = 0
        failed = 0
        failed_items = []
        for partner in partners:
            result = self._push_customer(partner)
            if result.get("success"):
                exported += 1
            else:
                failed += 1
                failed_items.append("%s: %s" % (partner.display_name, result.get("message")))
        return {
            "success": failed == 0,
            "message": "Customer export completed. Total: %s, Exported: %s, Failed: %s." % (len(partners), exported, failed),
            "total": len(partners),
            "exported": exported,
            "failed": failed,
            "failed_items": failed_items[:5],
        }

    def import_customer_by_id(self, customer_id):
        """Import or update a single BigCommerce customer by id (webhook-safe helper)."""
        result = self.client.get("/v3/customers/%s" % customer_id, timeout=25)
        if not result.get("success"):
            return {"success": False, "message": result.get("message") or "Failed to fetch customer by id."}

        response_body = result.get("response_body")
        customer = {}
        if isinstance(response_body, dict):
            data = response_body.get("data")
            if isinstance(data, list) and data:
                customer = data[0]
            elif isinstance(data, dict):
                customer = data
            elif isinstance(response_body.get("id"), (int, str)):
                customer = response_body

        if not isinstance(customer, dict) or not customer:
            return {"success": False, "message": "Unexpected customer payload format."}

        partner, _ = self._create_or_update_partner(customer)
        self._create_or_update_binding(partner, customer)
        return {"success": True, "message": "Customer %s synced." % customer_id}

    def _fetch_customers(self, limit=50):
        """Fetch customers from BigCommerce customers API."""
        customers = []
        page_limit = max(20, min(100, int(limit or 50)))
        total_retry_count = 0
        total_duration_ms = 0
        last_result = {}

        for page_result in self.client.iter_paginated(
            "/v3/customers",
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
                    "message": page_result.get("message") or "BigCommerce customer request failed.",
                    "status_code": page_result.get("status_code"),
                    "url": page_result.get("url"),
                    "response_body": page_result.get("response_body"),
                    "retry_count": total_retry_count,
                    "duration_ms": total_duration_ms,
                    "customers": customers,
                }

            customers.extend(page_result.get("items", []))
            if len(customers) >= limit:
                customers = customers[:limit]
                break

        response_body = last_result.get("response_body")
        if response_body and not isinstance(response_body, dict):
            return {
                "success": False,
                "message": "Unexpected BigCommerce response format while fetching customers.",
                "status_code": last_result.get("status_code"),
                "url": last_result.get("url"),
                "response_body": response_body,
                "retry_count": total_retry_count,
                "duration_ms": total_duration_ms,
                "customers": customers,
            }

        return {
            "success": True,
            "message": "Customers fetched successfully.",
            "status_code": last_result.get("status_code") if last_result else 200,
            "url": last_result.get("url") if last_result else self.client._build_url("/v3/customers"),
            "response_body": response_body,
            "retry_count": total_retry_count,
            "duration_ms": total_duration_ms,
            "customers": customers,
        }

    def _find_existing_partner(self, bc_customer):
        """Find an existing contact by binding, then email, then exact name fallback."""
        partner_model = self.env["res.partner"].with_context(active_test=False)
        remote_id = self._as_remote_id(bc_customer)
        email = self._normalized_email(bc_customer.get("email"))

        if remote_id:
            binding = self._binding_by_remote_id_cache.get(remote_id)
            if remote_id not in self._binding_by_remote_id_cache:
                binding = self.env["bigcommerce.customer.binding"].sudo().search(
                    [
                        ("instance_id", "=", self.instance.id),
                        ("bigcommerce_customer_id", "=", remote_id),
                    ],
                    limit=1,
                )
                self._binding_by_remote_id_cache[remote_id] = binding
            if binding and binding.partner_id:
                return binding.partner_id.with_env(self.env)

        if email:
            cached_partner = self._partner_by_email_cache.get(email)
            if email in self._partner_by_email_cache:
                return cached_partner or partner_model
            email_candidates = partner_model.search(
                [("email", "ilike", email)],
                limit=10,
            )
            exact_email_matches = email_candidates.filtered(
                lambda partner: self._normalized_email(partner.email) == email
            )
            if len(exact_email_matches) == 1:
                partner = exact_email_matches[0]
                self._partner_by_email_cache[email] = partner
                return partner
            if len(exact_email_matches) > 1:
                _logger.warning(
                    "Customer email match is ambiguous for instance_id=%s email=%s",
                    self.instance.id,
                    email,
                )
            self._partner_by_email_cache[email] = False

        if not email:
            name = self._display_name(bc_customer)
            if name:
                if name in self._partner_by_name_cache:
                    return self._partner_by_name_cache[name] or partner_model
                name_matches = partner_model.search([("name", "=", name)], limit=2)
                if len(name_matches) == 1:
                    self._partner_by_name_cache[name] = name_matches[0]
                    return name_matches[0]
                self._partner_by_name_cache[name] = False

        return partner_model

    def _prepare_partner_vals(self, bc_customer):
        """Prepare safe res.partner values from BigCommerce customer payload."""
        first_name = (bc_customer.get("first_name") or "").strip()
        last_name = (bc_customer.get("last_name") or "").strip()
        full_name = " ".join([value for value in [first_name, last_name] if value]).strip()
        email = self._normalized_email(bc_customer.get("email"))
        phone = (bc_customer.get("phone") or "").strip()
        name = self._display_name(bc_customer)

        vals = {
            "name": name,
        }
        if email:
            vals["email"] = email
        if phone:
            vals["phone"] = phone

        if "is_active" in bc_customer:
            vals["active"] = bool(bc_customer.get("is_active"))

        if not name:
            fallback_parts = [value for value in [first_name, last_name] if value]
            vals["name"] = " ".join(fallback_parts) if fallback_parts else "BigCommerce Customer"

        mapping_result = self.mapping_model._prepare_odoo_vals_from_mapping(
            payload=bc_customer,
            mapping_type="customer",
            connector=self.instance,
            direction="import",
            raise_on_required=False,
        )
        mapping_vals = mapping_result.get("vals") or {}

        # Keep canonical full name when first_name-only mapping would drop last name.
        mapped_name = mapping_vals.get("name")
        if full_name and mapped_name and str(mapped_name).strip() == first_name and first_name != full_name:
            mapping_vals["name"] = full_name
        vals.update(mapping_vals)

        # Preserve email <-> customer binding consistency.
        if email:
            vals["email"] = email
        if full_name and not vals.get("name"):
            vals["name"] = full_name

        return vals

    def _create_or_update_partner(self, bc_customer):
        """Create or update a res.partner and return (record, was_created)."""
        partner = self._find_existing_partner(bc_customer)
        vals = self._prepare_partner_vals(bc_customer)

        if partner:
            partner.write(vals)
            return partner, False

        created_partner = self.env["res.partner"].create(vals)
        return created_partner, True

    def _create_or_update_binding(self, partner, bc_customer):
        """Create or update BigCommerce customer binding for a partner."""
        now = fields.Datetime.now()
        remote_id = self._as_remote_id(bc_customer)
        if not remote_id:
            raise ValueError("BigCommerce customer id is missing in response payload.")

        vals = {
            "instance_id": self.instance.id,
            "partner_id": partner.id,
            "bigcommerce_customer_id": remote_id,
            "email": self._normalized_email(bc_customer.get("email")) or False,
            "sync_state": "synced",
            "last_synced_at": now,
            "last_error": False,
        }

        binding_model = self.env["bigcommerce.customer.binding"].sudo()
        binding = binding_model.search(
            [
                ("instance_id", "=", self.instance.id),
                ("bigcommerce_customer_id", "=", remote_id),
            ],
            limit=1,
        )
        if binding:
            binding.write(vals)
            self._binding_by_remote_id_cache[remote_id] = binding
            email = vals.get("email")
            if email:
                self._partner_by_email_cache[email] = partner
            return binding

        binding = binding_model.create(vals)
        self._binding_by_remote_id_cache[remote_id] = binding
        email = vals.get("email")
        if email:
            self._partner_by_email_cache[email] = partner
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
        """Create a success sync log record for customer import."""
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "customer_import",
                "resource_type": "customer",
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
        """Create a failure sync log record for customer import."""
        self.env["bigcommerce.sync.log"].sudo().create(
            {
                "instance_id": self.instance.id,
                "operation_type": "customer_import",
                "resource_type": "customer",
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

    def _resolve_batch_size(self, limit):
        return max(20, min(100, int(limit or 50)))

    def _iter_batches(self, records, batch_size):
        records = records or []
        for index in range(0, len(records), batch_size):
            yield records[index : index + batch_size]

    def _prime_customer_caches(self, customers):
        remote_ids = [self._as_remote_id(row) for row in customers or []]
        remote_ids = [rid for rid in remote_ids if rid]
        missing_remote_ids = [rid for rid in remote_ids if rid not in self._binding_by_remote_id_cache]
        if missing_remote_ids:
            bindings = self.env["bigcommerce.customer.binding"].sudo().search(
                [
                    ("instance_id", "=", self.instance.id),
                    ("bigcommerce_customer_id", "in", missing_remote_ids),
                ]
            )
            by_remote = {binding.bigcommerce_customer_id: binding for binding in bindings}
            for rid in missing_remote_ids:
                self._binding_by_remote_id_cache[rid] = by_remote.get(rid) or False

    def _display_name(self, bc_customer):
        first_name = (bc_customer.get("first_name") or "").strip()
        last_name = (bc_customer.get("last_name") or "").strip()
        full_name = " ".join([value for value in [first_name, last_name] if value]).strip()
        if full_name:
            return full_name

        name = (bc_customer.get("name") or "").strip()
        if name:
            return name

        email = self._normalized_email(bc_customer.get("email"))
        if email:
            return email

        remote_id = self._as_remote_id(bc_customer)
        if remote_id:
            return "BigCommerce Customer %s" % remote_id
        return "BigCommerce Customer"

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

    def _as_remote_id(self, bc_customer):
        if not isinstance(bc_customer, dict):
            return False
        value = bc_customer.get("id")
        if value in (None, False, ""):
            return False
        return str(value)

    def _push_customer(self, partner):
        binding = self.env["bigcommerce.customer.binding"].sudo().search(
            [
                ("instance_id", "=", self.instance.id),
                ("partner_id", "=", partner.id),
            ],
            limit=1,
        )
        payload = {
            "first_name": (partner.name or "Customer")[:50],
            "last_name": (partner.name or "Customer")[:50],
            "email": self._normalized_email(partner.email),
            "phone": (partner.phone or "").strip(),
        }
        mapping_payload = self.mapping_model._prepare_bigcommerce_payload_from_mapping(
            record=partner,
            mapping_type="customer",
            connector=self.instance,
            direction="export",
            raise_on_required=False,
        )
        payload = self.mapping_model._merge_payload_dict(payload, mapping_payload.get("payload") or {})
        if not payload["email"]:
            return {"success": False, "message": "Email is required for customer export."}

        if binding and binding.bigcommerce_customer_id:
            result = self.client.put("/v3/customers/%s" % binding.bigcommerce_customer_id, payload=payload)
            remote_id = binding.bigcommerce_customer_id
        else:
            result = self.client.post("/v3/customers", payload=payload)
            remote_id = self._extract_remote_customer_id(result.get("response_body"))

        if not result.get("success"):
            return {"success": False, "message": result.get("message"), "status_code": result.get("status_code")}

        if remote_id:
            vals = {
                "instance_id": self.instance.id,
                "partner_id": partner.id,
                "bigcommerce_customer_id": str(remote_id),
                "email": self._normalized_email(partner.email),
                "sync_state": "synced",
                "last_synced_at": fields.Datetime.now(),
                "last_error": False,
            }
            if binding:
                binding.write(vals)
            else:
                self.env["bigcommerce.customer.binding"].sudo().create(vals)
        return {"success": True}

    def _extract_remote_customer_id(self, response_body):
        if isinstance(response_body, dict):
            data = response_body.get("data")
            if isinstance(data, list) and data and data[0].get("id"):
                return data[0].get("id")
            if isinstance(data, dict) and data.get("id"):
                return data.get("id")
            if response_body.get("id"):
                return response_body.get("id")
        return False
