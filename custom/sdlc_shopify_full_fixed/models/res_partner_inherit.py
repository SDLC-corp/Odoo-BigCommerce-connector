from odoo import models, fields, api
from odoo.exceptions import UserError
import requests
import json
import logging
import re

_logger = logging.getLogger(__name__)


class ResPartner(models.Model):
    _inherit = "res.partner"

    shopify_customer_id = fields.Char("Shopify Customer ID")
    shopify_instance_id = fields.Many2one(
        "shopify.instance",
        string="Shopify Instance",
        readonly=False,
        copy=False,
    )

    # ---------------------------------------------------
    # SHOPIFY → ODOO (USING FIELD MAPPING)
    # ---------------------------------------------------
    @api.model
    def map_shopify_customer_to_odoo(self, data, instance=None):
        mapping_model = self.env["shopify.field.mapping"]
        payload = {}
        # Prefer Shopify's default address; fall back to first address if missing.
        addresses = data.get("addresses") or []
        address = data.get("default_address") or (addresses[0] if addresses else {}) or {}

        mappings = mapping_model.search([
            ("mapping_type", "=", "customer"),
            ("active", "=", True),
        ])

        for m in mappings:
            # Use the linked Shopify field record; the old attribute `shopify_field` does not exist
            shopify_key = m.shopify_field_id.code if m.shopify_field_id else False
            field_name = m.odoo_field.name if m.odoo_field else False

            if not shopify_key or not field_name:
                continue

            value = data.get(shopify_key)
            if value in (None, "", False):
                continue

            payload[field_name] = value

        # Only set fields when Shopify provides a value (avoid wiping Odoo data).
        def _set_if(val, key):
            if val not in (None, "", False):
                payload[key] = val

        fname = data.get("first_name") or ""
        lname = data.get("last_name") or ""
        full = f"{fname} {lname}".strip()
        name_from_shopify = full or data.get("email")
        _set_if(payload.get("name") or name_from_shopify, "name")
        _set_if(data.get("email"), "email")
        _set_if(data.get("phone") or address.get("phone"), "phone")
        if "mobile" in self._fields:
            _set_if(address.get("phone"), "mobile")
        _set_if(address.get("company") or data.get("company"), "company_name")
        _set_if(address.get("address1"), "street")
        _set_if(address.get("address2"), "street2")
        _set_if(address.get("city"), "city")
        _set_if(address.get("zip"), "zip")
        _set_if(data.get("note"), "comment")

        # Tags -> Partner Categories
        tag_ids = []
        tags_str = data.get("tags") or ""
        for tag in tags_str.split(","):
            tag = (tag or "").strip()
            if not tag:
                continue
            tag_rec = self.env["res.partner.category"].sudo().search([("name", "=", tag)], limit=1)
            if not tag_rec:
                tag_rec = self.env["res.partner.category"].sudo().create({"name": tag})
            tag_ids.append(tag_rec.id)
        if tag_ids:
            payload["category_id"] = [(6, 0, tag_ids)]

        # Country/state resolution
        country = False
        if address.get("country_code"):
            country = self.env["res.country"].sudo().search(
                [("code", "=", address.get("country_code"))], limit=1
            )
        if country:
            payload["country_id"] = country.id
            if address.get("province_code"):
                state = self.env["res.country.state"].sudo().search(
                    [("code", "=", address.get("province_code")), ("country_id", "=", country.id)],
                    limit=1,
                )
                if state:
                    payload["state_id"] = state.id

        _set_if(data.get("id"), "shopify_customer_id")
        if instance:
            payload["shopify_instance_id"] = instance.id
        return payload

    # ---------------------------------------------------
    # ODOO → SHOPIFY PAYLOAD 
    # ---------------------------------------------------
    def _prepare_shopify_customer_payload(self):
        self.ensure_one()

        if not self.email:
            raise UserError("Customer email is required to create Shopify customer.")

        mapping_model = self.env["shopify.field.mapping"]
        payload = {}

        mappings = mapping_model.search([
            ("mapping_type", "=", "customer"),
            ("active", "=", True),
        ])

        for m in mappings:
            field_name = m.odoo_field.name if m.odoo_field else False
            shopify_key = m.shopify_field_id.code if m.shopify_field_id else False

            if not field_name or not shopify_key:
                continue

            if field_name not in self._fields:
                continue

            value = self[field_name]
            if value in (None, False, ""):
                continue

            payload[shopify_key] = value

        def _format_shopify_phone(raw, country):
            """Return an E.164-ish phone for Shopify or False if unusable."""
            if not raw:
                return False
            digits = re.sub(r"\D", "", str(raw))
            if digits.startswith("00"):
                digits = digits[2:]
            if not digits:
                return False

            # If already has +, keep it with digits only
            if str(raw).strip().startswith("+") and len(digits) >= 8:
                return f"+{digits}"

            if country and country.phone_code:
                if digits.startswith(str(country.phone_code)):
                    candidate = f"+{digits}"
                else:
                    candidate = f"+{country.phone_code}{digits}"
                if len(re.sub(r"\D", "", candidate)) >= 8:
                    return candidate

            # Fallback: require at least 8 digits
            if len(digits) >= 8:
                return digits
            return False

        phone_clean = _format_shopify_phone(self.phone or self.mobile, self.country_id)

        payload.setdefault("email", self.email)
        payload.setdefault("first_name", (self.name or "").split(" ")[0])
        payload.setdefault("last_name", " ".join((self.name or "").split(" ")[1:]) or "Customer")
        if phone_clean:
            payload.setdefault("phone", phone_clean)
        payload.setdefault("note", self.comment)

        # Tags -> Shopify tags
        if self.category_id:
            payload.setdefault("tags", ", ".join(self.category_id.mapped("name")))

        # Address block
        address = {}
        if self.street:
            address["address1"] = self.street
        if self.street2:
            address["address2"] = self.street2
        if self.city:
            address["city"] = self.city
        if self.zip:
            address["zip"] = self.zip
        if phone_clean:
            address["phone"] = phone_clean
        if self.company_name:
            address["company"] = self.company_name
        if self.country_id:
            address["country_code"] = self.country_id.code
        if self.state_id:
            address["province"] = self.state_id.name
            if self.state_id.code:
                address["province_code"] = self.state_id.code

        addresses = []
        if address:
            address["default"] = True
            addresses.append(address)

        if addresses:
            payload.setdefault("addresses", addresses)

        return {"customer": payload}

    # ---------------------------------------------------
    # ✅ INTERNAL HELPER: CREATE SYNC REPORT
    # ---------------------------------------------------
    def _create_customer_sync_report(self, instance, payload, response_text, status, source_action):
        Report = self.env["shopify.sync.report"]
        Line = self.env["shopify.sync.report.line"]

        report = Report.create({
            "instance_id": instance.id,
            "sync_type": "customer",
            "total_records": 1,
            "success_count": 1 if status == "success" else 0,
            "error_count": 1 if status == "error" else 0,
        })

        Line.create({
            "report_id": report.id,
            "record_type": "customer",
            "source_action": source_action,
            "shopify_id": self.shopify_customer_id or "",
            "name": self.name,
            "status": status,
            "error_message": response_text if status == "error" else False,
        })

    # ---------------------------------------------------
    # CREATE CUSTOMER ON SHOPIFY 
    # ---------------------------------------------------
    def action_create_shopify_customer(self):
        self.ensure_one()

        instance = self.shopify_instance_id or self.env["shopify.instance"].search(
            [("active", "=", True)], limit=1
        )
        if not instance:
            raise UserError("Please configure Shopify Instance first.")
        self.shopify_instance_id = instance.id

        payload = self._prepare_shopify_customer_payload()
        mapped_payload, _ = self.env["shopify.field.mapping"].sudo().build_payload(
            "customer", self, instance
        )
        if mapped_payload and mapped_payload.get("customer"):
            # Merge mapped fields with prepared payload (addresses/tags/notes).
            base = dict(mapped_payload.get("customer") or {})
            for k, v in (payload.get("customer") or {}).items():
                base.setdefault(k, v)
            payload = {"customer": base}
        _logger.info("Shopify Customer CREATE Payload => %s", payload)

        url = f"{instance.shop_url.rstrip('/')}/admin/api/{instance.api_version}/customers.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

        response = requests.post(url, json=payload, headers=headers)

        if response.status_code not in (200, 201):
            self._create_customer_sync_report(
                instance, payload, response.text, "error", "customer_create"
            )
            raise UserError(f"Shopify Create Error: {response.text}")

        self.shopify_customer_id = response.json()["customer"]["id"]

        self._create_customer_sync_report(
            instance, payload, response.text, "success", "customer_create"
        )
        return {
            "effect": {
                "fadeout": "slow",
                "message": "Customer Created in Shopify",
                "type": "rainbow_man",
            }
        }

    # ---------------------------------------------------
    # UPDATE CUSTOMER ON SHOPIFY 
    # ---------------------------------------------------
    def action_update_shopify_customer(self):
        self.ensure_one()

        if not self.shopify_customer_id:
            raise UserError("Customer not linked with Shopify.")

        instance = self.shopify_instance_id or self.env["shopify.instance"].search(
            [("active", "=", True)], limit=1
        )
        if not instance:
            raise UserError("Please configure Shopify Instance first.")

        payload = self._prepare_shopify_customer_payload()
        mapped_payload, _ = self.env["shopify.field.mapping"].sudo().build_payload(
            "customer", self, instance
        )
        if mapped_payload and mapped_payload.get("customer"):
            base = dict(mapped_payload.get("customer") or {})
            for k, v in (payload.get("customer") or {}).items():
                base.setdefault(k, v)
            payload = {"customer": base}
        payload["customer"]["id"] = int(self.shopify_customer_id)

        _logger.info("Shopify Customer UPDATE Payload => %s", payload)

        url = (
            f"{instance.shop_url.rstrip('/')}/admin/api/"
            f"{instance.api_version}/customers/{self.shopify_customer_id}.json"
        )

        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": instance.access_token,
        }

        response = requests.put(url, json=payload, headers=headers)

        if response.status_code not in (200, 201):
            self._create_customer_sync_report(
                instance, payload, response.text, "error", "customer_update"
            )
            raise UserError(f"Shopify Update Error: {response.text}")

        self._create_customer_sync_report(
            instance, payload, response.text, "success", "customer_update"
        )
        return {
            "effect": {
                "fadeout": "slow",
                "message": "Customer updated in Shopify",
                "type": "rainbow_man",
            }
        }
