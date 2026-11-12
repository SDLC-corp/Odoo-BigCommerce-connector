# -*- coding: utf-8 -*-

import logging
import secrets
import ipaddress
from datetime import timedelta
from urllib.parse import parse_qs, urlparse

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError

from ..services.api_client import BigCommerceApiClient
from ..services.category_sync_service import BigCommerceCategorySyncService
from ..services.customer_sync_service import BigCommerceCustomerSyncService
from ..services.inventory_sync_service import BigCommerceInventorySyncService
from ..services.order_sync_service import BigCommerceOrderSyncService
from ..services.product_sync_service import BigCommerceProductSyncService
from ..services.shipment_sync_service import BigCommerceShipmentSyncService
from ..services.webhook_service import BigCommerceWebhookService

_logger = logging.getLogger(__name__)


class BigCommerceConnector(models.Model):
    _name = "bigcommerce.connector"
    _description = "BigCommerce Connector"
    _rec_name = "name"
    _order = "company_id, name, id"

    name = fields.Char(required=True)
    active = fields.Boolean(default=True)
    company_id = fields.Many2one(
        "res.company",
        required=True,
        default=lambda self: self.env.company,
        index=True,
    )
    store_hash = fields.Char(
        required=True,
        copy=False,
        help="BigCommerce store hash (the value after /stores/ in API endpoints).",
    )
    client_id = fields.Char(
        copy=False,
        help="Optional BigCommerce API client identifier.",
    )
    client_secret = fields.Char(
        copy=False,
        groups="bigcommerce_connector_sdlc.group_bigcommerce_admin",
        help="Optional BigCommerce API client secret.",
    )
    access_token = fields.Char(
        required=True,
        copy=False,
        groups="bigcommerce_connector_sdlc.group_bigcommerce_admin",
        help="API access token used for authenticated BigCommerce requests.",
    )
    api_base_url = fields.Char(
        required=True,
        default="https://api.bigcommerce.com/stores",
        help="Base URL prefix used for BigCommerce REST API calls.",
    )
    webhook_secret = fields.Char(
        copy=False,
        groups="bigcommerce_connector_sdlc.group_bigcommerce_admin",
        help="Shared secret used to validate incoming BigCommerce webhooks.",
    )
    webhook_callback_url = fields.Char(
        copy=False,
        help="Optional public callback URL override for webhook registration.",
    )
    webhook_enabled = fields.Boolean(
        default=True,
        help="Enable webhook callback acceptance and registration for this connector.",
    )
    warehouse_id = fields.Many2one(
        "stock.warehouse",
        string="Warehouse",
        domain="[('company_id', '=', company_id)]",
    )
    pricelist_id = fields.Many2one(
        "product.pricelist",
        string="Pricelist",
        domain="[('company_id', 'in', [False, company_id])]",
    )
    inventory_master = fields.Selection(
        selection=[
            ("odoo", "Odoo"),
            ("bigcommerce", "BigCommerce"),
        ],
        required=True,
        default="bigcommerce",
    )
    state = fields.Selection(
        selection=[
            ("draft", "Draft"),
            ("connected", "Connected"),
            ("error", "Error"),
        ],
        required=True,
        default="draft",
        copy=False,
    )
    debug_mode = fields.Boolean()
    ai_chat_enabled = fields.Boolean(default=False)
    gemini_api_key = fields.Char(copy=False, groups="bigcommerce_connector_sdlc.group_bigcommerce_admin")
    gemini_model = fields.Char(default="gemini-2.5-flash")
    ai_system_prompt = fields.Text()
    last_product_sync_at = fields.Datetime(readonly=True, copy=False)
    last_category_sync_at = fields.Datetime(readonly=True, copy=False)
    last_customer_sync_at = fields.Datetime(readonly=True, copy=False)
    last_order_sync_at = fields.Datetime(readonly=True, copy=False)
    last_inventory_export_at = fields.Datetime(
        string="Last Inventory Sync At",
        readonly=True,
        copy=False,
    )
    last_shipment_export_at = fields.Datetime(readonly=True, copy=False)
    last_webhook_process_at = fields.Datetime(readonly=True, copy=False)
    webhook_last_sync_at = fields.Datetime(readonly=True, copy=False)
    last_tested_at = fields.Datetime(readonly=True, copy=False)
    auto_sync_products = fields.Boolean(string="Auto-Import Products", related="auto_product_sync", readonly=False)
    auto_sync_orders = fields.Boolean(string="Auto-Import Orders", related="auto_order_sync", readonly=False)
    auto_sync_inventory = fields.Boolean(
        string="Auto-Sync Inventory",
        related="auto_inventory_export",
        readonly=False,
        help="Scheduled inventory sync in the direction of the Inventory Master: import from BigCommerce, "
        "or export Odoo quantities when Odoo is the master.",
    )
    auto_sync_shipments = fields.Boolean(string="Auto-Export Shipments", related="auto_shipment_export", readonly=False)
    auto_sync_customers = fields.Boolean(default=False)
    auto_product_sync = fields.Boolean(default=False)
    auto_order_sync = fields.Boolean(default=False)
    auto_inventory_export = fields.Boolean(default=False)
    auto_shipment_export = fields.Boolean(default=False)
    auto_webhook_process = fields.Boolean(default=True)
    sync_limit_product = fields.Integer(default=100)
    sync_limit_order = fields.Integer(default=50)
    sync_limit_customer = fields.Integer(default=100)
    sync_limit_inventory = fields.Integer(default=200)
    sync_limit_shipment = fields.Integer(default=100)
    sync_limit_webhook = fields.Integer(default=200)
    order_sync_checkpoint = fields.Datetime(
        string="Import Orders Modified After",
        copy=False,
        default=lambda self: fields.Datetime.now() - timedelta(days=30),
        help="Incremental order import: only orders modified in BigCommerce after this moment are "
        "requested, oldest first. It advances automatically after each run. A new connector starts "
        "30 days back so the first run does not import (and confirm) the whole order history; "
        "clear it to import everything.",
    )
    product_sync_checkpoint = fields.Datetime(
        string="Import Products Modified After",
        copy=False,
        help="Incremental product import checkpoint (advances automatically). Clear it for a full re-import.",
    )
    customer_sync_checkpoint = fields.Datetime(
        string="Import Customers Modified After",
        copy=False,
        help="Incremental customer import checkpoint (advances automatically). Clear it for a full re-import.",
    )
    webhook_max_retries = fields.Integer(
        default=5,
        help="A webhook event that failed this many times is no longer retried automatically; "
        "it stays in the exception queue until someone retries it by hand.",
    )
    log_retention_days = fields.Integer(
        default=30,
        help="Sync logs and processed/rejected webhook events older than this are purged daily. "
        "0 keeps everything.",
    )
    order_total_tolerance = fields.Float(
        default=0.05,
        help="An imported order whose Odoo total differs from the BigCommerce total by more than this "
        "amount is flagged in the exception queue.",
    )
    dashboard_exception_count = fields.Integer(
        string="Open Exceptions",
        compute="_compute_dashboard_exception_count",
    )
    dashboard_product_count = fields.Integer(compute="_compute_dashboard_metrics")
    dashboard_order_count = fields.Integer(compute="_compute_dashboard_metrics")
    dashboard_error_count = fields.Integer(compute="_compute_dashboard_metrics")
    dashboard_webhook_pending_count = fields.Integer(
        string="Dashboard Webhook Pending Count",
        compute="_compute_dashboard_metrics",
    )
    dashboard_webhook_active_count = fields.Integer(
        string="Dashboard Webhook Active Count",
        compute="_compute_dashboard_metrics",
    )
    dashboard_webhook_failed_count = fields.Integer(
        string="Dashboard Webhook Failed Count",
        compute="_compute_dashboard_metrics",
    )
    dashboard_webhook_last_received_at = fields.Datetime(
        string="Dashboard Last Webhook Event At",
        compute="_compute_dashboard_metrics",
    )
    dashboard_last_sync_at = fields.Datetime(compute="_compute_dashboard_metrics")
    dashboard_status = fields.Selection(
        selection=[("ok", "OK"), ("warning", "Warning"), ("error", "Error")],
        compute="_compute_dashboard_metrics",
    )
    connection_message = fields.Text(readonly=True, copy=False)
    is_ready_for_connection = fields.Boolean(
        compute="_compute_is_ready_for_connection",
    )
    note = fields.Text()
    mapping_ids = fields.One2many(
        "bigcommerce.field.mapping",
        "connector_id",
        string="Field Mappings",
    )

    _bigcommerce_instance_company_store_hash_uniq = models.Constraint(
        "unique(company_id, store_hash)",
        "Store hash must be unique per company.",
    )

    @staticmethod
    def _normalize_store_hash_value(value):
        normalized = (value or "").strip().strip("/")
        return normalized or False

    @staticmethod
    def _parse_api_base_and_store_hash(value):
        raw = (value or "").strip()
        if not raw:
            return False, False

        parse_target = raw if "://" in raw else "https://%s" % raw.lstrip("/")
        parsed = urlparse(parse_target)
        if not parsed.scheme or not parsed.netloc:
            return raw.rstrip("/"), False

        parts = [part for part in (parsed.path or "").split("/") if part]
        extracted_hash = False
        normalized_parts = parts

        if "stores" in parts:
            stores_idx = parts.index("stores")
            normalized_parts = parts[: stores_idx + 1]
            if len(parts) > stores_idx + 1:
                candidate = (parts[stores_idx + 1] or "").strip()
                if candidate and candidate.lower() not in ("v2", "v3"):
                    extracted_hash = candidate

        normalized_path = "/%s" % "/".join(normalized_parts) if normalized_parts else ""
        normalized_base = "%s://%s%s" % (
            parsed.scheme,
            parsed.netloc,
            normalized_path,
        )
        return normalized_base.rstrip("/"), BigCommerceConnector._normalize_store_hash_value(extracted_hash)

    @staticmethod
    def _extract_store_hash_from_callback_url(value):
        raw = (value or "").strip()
        if not raw:
            return False
        parsed = urlparse(raw)
        query = parse_qs(parsed.query or "")
        candidate = (query.get("store_hash") or [False])[0]
        return BigCommerceConnector._normalize_store_hash_value(candidate)

    def _build_default_webhook_callback_url(self, store_hash):
        store_hash = self._normalize_store_hash_value(store_hash)
        if not store_hash:
            return False
        base_url = (self.env["ir.config_parameter"].sudo().get_str("web.base.url") or "").strip()
        if not base_url:
            return False
        return "%s/bigcommerce/webhook?store_hash=%s" % (base_url.rstrip("/"), store_hash)

    def _prepare_autofill_vals(self, vals, current=False):
        prepared = dict(vals or {})
        current = current[:1] if current else self.env["bigcommerce.connector"]

        if "api_base_url" in prepared:
            parsed_base, parsed_hash = self._parse_api_base_and_store_hash(prepared.get("api_base_url"))
            prepared["api_base_url"] = parsed_base or False
            if parsed_hash and not self._normalize_store_hash_value(prepared.get("store_hash")):
                prepared["store_hash"] = parsed_hash

        if "store_hash" in prepared:
            prepared["store_hash"] = self._normalize_store_hash_value(prepared.get("store_hash"))

        if not self._normalize_store_hash_value(prepared.get("store_hash")):
            callback_hash = self._extract_store_hash_from_callback_url(prepared.get("webhook_callback_url"))
            if callback_hash:
                prepared["store_hash"] = callback_hash

        old_hash = self._normalize_store_hash_value(current.store_hash) if current else False
        new_hash = self._normalize_store_hash_value(prepared.get("store_hash")) or old_hash

        if "webhook_callback_url" in prepared:
            callback_url = (prepared.get("webhook_callback_url") or "").strip()
            prepared["webhook_callback_url"] = callback_url or False
            callback_hash = self._extract_store_hash_from_callback_url(callback_url)
            if callback_hash and not self._normalize_store_hash_value(prepared.get("store_hash")):
                prepared["store_hash"] = callback_hash
                new_hash = callback_hash
            if not callback_url and new_hash:
                prepared["webhook_callback_url"] = self._build_default_webhook_callback_url(new_hash) or False
        else:
            current_callback = (current.webhook_callback_url or "").strip() if current else ""
            default_old = self._build_default_webhook_callback_url(old_hash) if old_hash else False
            if new_hash and (not current_callback or (default_old and current_callback == default_old)):
                prepared["webhook_callback_url"] = self._build_default_webhook_callback_url(new_hash) or current_callback or False

        return prepared

    @api.onchange("api_base_url")
    def _onchange_api_base_url(self):
        for instance in self:
            parsed_base, parsed_hash = self._parse_api_base_and_store_hash(instance.api_base_url)
            if parsed_base:
                instance.api_base_url = parsed_base
            if parsed_hash and not instance.store_hash:
                instance.store_hash = parsed_hash

    @api.onchange("store_hash")
    def _onchange_store_hash(self):
        for instance in self:
            instance.store_hash = self._normalize_store_hash_value(instance.store_hash)
            if not instance.store_hash:
                continue
            callback_current = (instance.webhook_callback_url or "").strip()
            callback_default_current = instance._build_default_webhook_callback_url(instance.store_hash)
            origin_hash = self._normalize_store_hash_value(instance._origin.store_hash) if instance._origin else False
            callback_default_origin = instance._build_default_webhook_callback_url(origin_hash) if origin_hash else False
            if not callback_current or (callback_default_origin and callback_current == callback_default_origin):
                instance.webhook_callback_url = callback_default_current

    @api.onchange("webhook_callback_url")
    def _onchange_webhook_callback_url(self):
        for instance in self:
            callback_url = (instance.webhook_callback_url or "").strip()
            instance.webhook_callback_url = callback_url or False
            if callback_url and not instance.store_hash:
                parsed_hash = self._extract_store_hash_from_callback_url(callback_url)
                if parsed_hash:
                    instance.store_hash = parsed_hash

    @api.model_create_multi
    def create(self, vals_list):
        prepared_vals_list = [self._prepare_autofill_vals(vals) for vals in vals_list]
        records = super().create(prepared_vals_list)
        # Without the default mappings (notably the order status map) imported orders were never
        # confirmed: no delivery, no shipment export (found in the live test, 19.0.2.1.2).
        records._generate_default_mappings()
        return records

    def write(self, vals):
        keys = {"api_base_url", "store_hash", "webhook_callback_url"}
        if not (keys & set(vals.keys())):
            return super().write(vals)

        if len(self) == 1:
            prepared = self._prepare_autofill_vals(vals, current=self)
            return super().write(prepared)

        result = True
        for record in self:
            prepared = record._prepare_autofill_vals(vals, current=record)
            result = super(BigCommerceConnector, record).write(prepared) and result
        return result

    def _notification_action(self, message, notif_type="info"):
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("BigCommerce"),
                "message": message,
                "type": notif_type,
                "sticky": False,
            },
        }

    def _is_serialization_conflict(self, err):
        return "could not serialize access due to concurrent update" in str(err).lower()

    def _safe_write_sync_timestamp(self, instance, field_name):
        try:
            with self.env.cr.savepoint():
                instance.sudo().write({field_name: fields.Datetime.now()})
        except Exception as err:
            if self._is_serialization_conflict(err):
                _logger.warning(
                    "Skipped sync timestamp write due to concurrent update instance_id=%s field=%s",
                    instance.id,
                    field_name,
                )
                return
            raise

    @api.depends("store_hash", "api_base_url")
    def _compute_is_ready_for_connection(self):
        for instance in self:
            store_hash = (instance.store_hash or "").strip()
            api_base_url = (instance.api_base_url or "").strip()
            access_token = (instance.sudo().access_token or "").strip()
            instance.is_ready_for_connection = bool(
                store_hash and api_base_url and access_token
            )

    def _log_placeholder(self, action_name):
        for instance in self:
            _logger.info(
                "BigCommerce placeholder action '%s' triggered for instance id=%s name=%s company_id=%s",
                action_name,
                instance.id,
                instance.display_name,
                instance.company_id.id,
            )

    @api.depends(
        "last_product_sync_at",
        "last_category_sync_at",
        "last_customer_sync_at",
        "last_order_sync_at",
        "last_inventory_export_at",
        "last_shipment_export_at",
        "last_webhook_process_at",
        "webhook_last_sync_at",
    )
    def _compute_dashboard_metrics(self):
        product_model = self.env["bigcommerce.product.binding"].sudo()
        category_model = self.env["bigcommerce.category.binding"].sudo()
        customer_model = self.env["bigcommerce.customer.binding"].sudo()
        order_model = self.env["bigcommerce.order.binding"].sudo()
        log_model = self.env["bigcommerce.sync.log"].sudo()
        webhook_event_model = self.env["bigcommerce.webhook.event"].sudo()
        webhook_subscription_model = self.env["bigcommerce.webhook.subscription"].sudo()
        recent_failure_since = fields.Datetime.now() - timedelta(hours=24)
        for instance in self:
            instance.dashboard_product_count = product_model.search_count(
                [
                    ("instance_id", "=", instance.id),
                    ("bigcommerce_variant_id", "=", False),
                    "|",
                    ("sync_state", "=", "synced"),
                    ("last_synced_at", "!=", False),
                ]
            )
            instance.dashboard_order_count = order_model.search_count(
                [
                    ("instance_id", "=", instance.id),
                    "|",
                    ("sync_state", "=", "synced"),
                    ("imported_at", "!=", False),
                ]
            )

            open_binding_errors = (
                product_model.search_count([("instance_id", "=", instance.id), ("sync_state", "=", "error")])
                + category_model.search_count([("instance_id", "=", instance.id), ("sync_state", "=", "error")])
                + customer_model.search_count([("instance_id", "=", instance.id), ("sync_state", "=", "error")])
                + order_model.search_count([("instance_id", "=", instance.id), ("sync_state", "=", "error")])
            )
            recent_failed_sync_logs = log_model.search_count(
                [
                    ("instance_id", "=", instance.id),
                    ("status", "=", "failed"),
                    ("create_date", ">=", recent_failure_since),
                    (
                        "operation_type",
                        "in",
                        (
                            "connection_test",
                            "product_import",
                            "product_export",
                            "category_import",
                            "category_export",
                            "customer_import",
                            "customer_export",
                            "order_import",
                            "inventory_import",
                            "inventory_export",
                            "shipment_export",
                            "webhook_process",
                        ),
                    ),
                ]
            )
            connection_error_count = 1 if instance.state == "error" else 0
            instance.dashboard_error_count = open_binding_errors + recent_failed_sync_logs + connection_error_count
            instance.dashboard_webhook_pending_count = webhook_event_model.search_count(
                [("instance_id", "=", instance.id), ("status", "in", ("pending", "processing"))]
            )
            failed_webhook_events = webhook_event_model.search_count(
                [("instance_id", "=", instance.id), ("status", "=", "failed")]
            )
            failed_webhook_subscriptions = webhook_subscription_model.search_count(
                [("instance_id", "=", instance.id), ("status", "in", ("missing", "error"))]
            )
            instance.dashboard_webhook_failed_count = failed_webhook_events + failed_webhook_subscriptions
            instance.dashboard_webhook_active_count = webhook_subscription_model.search_count(
                [
                    ("instance_id", "=", instance.id),
                    ("status", "=", "active"),
                    ("is_active", "=", True),
                ]
            )
            last_webhook_event = webhook_event_model.search(
                [("instance_id", "=", instance.id)],
                order="received_at desc,id desc",
                limit=1,
            )
            instance.dashboard_webhook_last_received_at = last_webhook_event.received_at or False

            sync_dates = [
                instance.last_product_sync_at,
                instance.last_category_sync_at,
                instance.last_customer_sync_at,
                instance.last_order_sync_at,
                instance.last_inventory_export_at,
                instance.last_shipment_export_at,
                instance.last_webhook_process_at,
                instance.webhook_last_sync_at,
                instance.dashboard_webhook_last_received_at,
            ]
            sync_dates = [value for value in sync_dates if value]
            instance.dashboard_last_sync_at = max(sync_dates) if sync_dates else False

            if instance.state == "error" or open_binding_errors or instance.dashboard_webhook_failed_count:
                instance.dashboard_status = "error"
            elif recent_failed_sync_logs or instance.dashboard_webhook_pending_count:
                instance.dashboard_status = "warning"
            else:
                instance.dashboard_status = "ok"

    # ------------------------------------------------------------------
    # Access control helpers
    # ------------------------------------------------------------------
    _BC_ROLE_GROUPS = {
        "user": "bigcommerce_connector_sdlc.group_bigcommerce_user",
        "manager": "bigcommerce_connector_sdlc.group_bigcommerce_manager",
        "admin": "bigcommerce_connector_sdlc.group_bigcommerce_admin",
    }

    def _bc_check_access(self, role="manager"):
        """Raise AccessError unless the current user holds the BigCommerce role.

        Superuser environments (cron jobs, sudo() service calls) pass. The records themselves must
        also be visible to the user, which enforces the multi-company record rules.
        """
        if self.env.su:
            return
        group = self._BC_ROLE_GROUPS[role]
        if not self.env.user.has_group(group):
            raise AccessError(
                _("This BigCommerce operation requires the '%(role)s' role.")
                % {"role": {"user": _("User"), "manager": _("Manager"), "admin": _("Administrator")}[role]}
            )
        self.check_access("read")

    def _bc_service_instance(self):
        """The connector as seen by sync services: superuser, scoped to the connector company.

        Callers must have passed _bc_check_access() first. Services need the API token (restricted
        to administrators) and create sale orders, partners and stock moves that a BigCommerce
        manager may not be allowed to create directly.
        """
        self.ensure_one()
        return self.sudo().with_company(self.company_id)

    def _compute_dashboard_exception_count(self):
        order_model = self.env["bigcommerce.order.binding"].sudo()
        event_model = self.env["bigcommerce.webhook.event"].sudo()
        for instance in self:
            if not instance.id:
                instance.dashboard_exception_count = 0
                continue
            instance.dashboard_exception_count = order_model.search_count(
                [("instance_id", "=", instance.id), ("needs_attention", "=", True)]
            ) + event_model.search_count(
                [("instance_id", "=", instance.id), ("status", "=", "failed")]
            )

    def action_open_exceptions(self):
        self.ensure_one()
        self._bc_check_access("user")
        action = self.env["ir.actions.act_window"]._for_xml_id(
            "bigcommerce_connector_sdlc.action_bigcommerce_order_exceptions"
        )
        action["domain"] = [("instance_id", "=", self.id), ("needs_attention", "=", True)]
        return action

    def _ensure_connector_ready(self):
        missing = []
        if not (self.store_hash or "").strip():
            missing.append(_("Store Hash"))
        if not (self.api_base_url or "").strip():
            missing.append(_("API Base URL"))
        if not (self.sudo().access_token or "").strip():
            missing.append(_("Access Token"))
        return missing

    def _get_default_webhook_scopes(self):
        """Return default webhook scopes supported by this connector."""
        return [
            "store/order/created",
            "store/order/updated",
            "store/product/created",
            "store/product/updated",
            "store/product/deleted",
            "store/customer/created",
            "store/customer/updated",
        ]

    def _build_webhook_destination(self):
        """Build public callback URL for webhook registration."""
        self.ensure_one()
        if (self.webhook_callback_url or "").strip():
            return (self.webhook_callback_url or "").strip()

        base_url = self.env["ir.config_parameter"].sudo().get_str("web.base.url")
        if not (base_url or "").strip():
            return False
        return "%s/bigcommerce/webhook?store_hash=%s" % (
            (base_url or "").rstrip("/"),
            (self.store_hash or "").strip(),
        )

    def _get_webhook_destination_validation_error(self, destination):
        """Return a user-friendly validation message when destination is not public HTTPS."""
        destination = (destination or "").strip()
        if not destination:
            return _("Webhook callback URL is not configured. Set Webhook Callback URL or web.base.url.")

        parsed = urlparse(destination)
        host = (parsed.hostname or "").strip().lower()
        if parsed.scheme != "https":
            return _("Webhook callback URL must use HTTPS (current: %(scheme)s).") % {
                "scheme": parsed.scheme or "none"
            }
        if not host:
            return _("Webhook callback URL host is missing.")
        try:
            port = parsed.port
        except ValueError:
            return _("Webhook callback URL has an invalid port.")
        if port not in (None, 443):
            return _(
                "Webhook callback URL must use the standard HTTPS port 443 (current: %(port)s). "
                "BigCommerce only delivers webhooks to port 443; remove the port from the URL "
                "or put a reverse proxy on 443 in front of Odoo."
            ) % {"port": port}
        if host in ("localhost", "127.0.0.1", "0.0.0.0", "::1") or host.endswith(".local"):
            return _("Webhook callback URL must be publicly reachable. Localhost/.local addresses are not allowed.")

        try:
            ip = ipaddress.ip_address(host)
            if (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_reserved
                or ip.is_unspecified
            ):
                return _("Webhook callback URL must use a public IP or DNS host.")
        except ValueError:
            # Not an IP literal; DNS hostname is acceptable.
            pass
        return False

    def _is_public_https_url(self, url):
        """Check whether callback URL looks publicly reachable by BigCommerce."""
        return not bool(self._get_webhook_destination_validation_error(url))

    def _get_or_create_webhook_secret(self):
        """Return existing webhook secret or generate one securely."""
        self.ensure_one()
        secret = (self.sudo().webhook_secret or "").strip()
        if secret:
            return secret
        secret = secrets.token_hex(32)
        self.sudo().write({"webhook_secret": secret})
        return secret

    def _webhook_payload(self, scope, destination, webhook_secret):
        """Return BigCommerce webhook registration payload with custom header."""
        return {
            "scope": scope,
            "destination": destination,
            "is_active": True,
            "headers": {
                "X-Webhook-Secret": webhook_secret,
                # Lets Odoo route the callback to this database on servers hosting several
                # databases (no session cookie is sent by BigCommerce).
                "X-Odoo-Database": self.env.cr.dbname,
            },
        }

    def action_generate_webhook_secret(self):
        """Generate a webhook secret manually (system users only)."""
        self._bc_check_access("admin")
        self = self.sudo()
        for instance in self:
            instance.sudo().write({"webhook_secret": secrets.token_hex(32)})
        return self._notification_action(_("Webhook secret generated successfully."), notif_type="success")

    def _generate_default_mappings(self):
        """Create the missing default field mappings of each connector (idempotent).

        The definitions live in ``bigcommerce.field.mapping._default_mapping_definitions`` (the
        single source; they include the "Order Status" map that confirms or cancels imported
        orders). Rules that already exist - active or archived - are kept as they are. No access
        check, notification or sync log: used by ``create`` and by the admin button.

        Returns ``{connector_id: {"created": n, "skipped": n, "removed": 0}}``.
        """
        mapping_model = self.env["bigcommerce.field.mapping"].sudo()
        return {instance.id: mapping_model.generate_default_mappings(instance) for instance in self.sudo()}

    def action_generate_default_mappings(self):
        """Generate initial default mappings for each selected connector without duplicates."""
        self._bc_check_access("admin")
        self = self.sudo()
        total_created = 0
        total_skipped = 0
        summary_lines = []

        results = self._generate_default_mappings()
        for instance in self:
            created = results[instance.id]["created"]
            skipped = results[instance.id]["skipped"]
            total_created += created
            total_skipped += skipped
            summary_lines.append("%s: created=%s skipped=%s" % (instance.display_name, created, skipped))

            _logger.info(
                "BigCommerce default mappings generated instance_id=%s created=%s skipped=%s",
                instance.id,
                created,
                skipped,
            )
            self.env["bigcommerce.sync.log"].sudo().create(
                {
                    "instance_id": instance.id,
                    "operation_type": "manual_action",
                    "resource_type": "system",
                    "status": "success",
                    "note": "Default field mappings generated. created=%s skipped=%s" % (created, skipped),
                }
            )

        return self._notification_action(
            _("Default field mappings generated. Created: %(created)s, Skipped: %(skipped)s.\n%(details)s")
            % {
                "created": total_created,
                "skipped": total_skipped,
                "details": "\n".join(summary_lines),
            },
            notif_type="success" if total_created else "warning",
        )

    def action_generate_default_field_mappings(self):
        """Backward-compatible alias for older buttons/calls."""
        return self.action_generate_default_mappings()

    def action_validate_field_mappings(self):
        """Validate active field mapping rules and return user-friendly diagnostics."""
        self._bc_check_access("manager")
        self = self.sudo()
        mapping_model = self.env["bigcommerce.field.mapping"].sudo()
        invalid_total = 0
        summary_lines = []

        for instance in self:
            result = mapping_model.validate_connector_mappings(instance=instance)
            invalid = result.get("invalid", 0)
            valid = result.get("valid", 0)
            invalid_total += invalid
            if invalid:
                first_errors = " | ".join((result.get("errors") or [])[:3])
                summary_lines.append(
                    "%s: valid=%s invalid=%s errors=%s"
                    % (instance.display_name, valid, invalid, first_errors)
                )
            else:
                summary_lines.append(
                    "%s: valid=%s invalid=%s"
                    % (instance.display_name, valid, invalid)
                )

        return self._notification_action(
            _("Field mapping validation completed.\n%s") % "\n".join(summary_lines),
            notif_type="warning" if invalid_total else "success",
        )

    def _list_remote_webhooks(self, client):
        """Fetch registered webhooks from BigCommerce."""
        result = client.get_paginated("/v3/hooks", params={"limit": 250}, data_key="data")
        if not result.get("success"):
            return {"success": False, "message": result.get("message"), "hooks": []}
        hooks = result.get("items") or []
        if not isinstance(hooks, list):
            hooks = []
        return {"success": True, "hooks": hooks}

    def _upsert_webhook_subscription(self, hook, destination):
        """Create or update local webhook subscription tracking record."""
        self.ensure_one()
        remote_webhook_id = str(hook.get("id") or "")
        scope = (hook.get("scope") or "").strip()
        if not scope:
            return
        values = {
            "instance_id": self.id,
            "remote_webhook_id": remote_webhook_id or False,
            "scope": scope,
            "destination": destination,
            "is_active": bool(hook.get("is_active", True)),
            "last_sync_at": fields.Datetime.now(),
            "status": "active",
            "error_message": False,
        }

        model = self.env["bigcommerce.webhook.subscription"].sudo()
        existing = model.search(
            [
                ("instance_id", "=", self.id),
                ("scope", "=", scope),
                ("destination", "=", destination),
            ],
            limit=1,
        )
        if existing:
            existing.write(values)
        else:
            model.create(values)

    def action_test_connection(self):
        """Execute a real BigCommerce API connectivity test."""
        self._bc_check_access("manager")
        self = self.sudo()
        connected_count = 0
        error_count = 0
        for instance in self:
            now = fields.Datetime.now()
            store_hash = (instance.store_hash or "").strip()
            api_base_url = (instance.api_base_url or "").strip()
            access_token = (instance.access_token or "").strip()
            missing = []
            if not store_hash:
                missing.append(_("Store Hash"))
            if not access_token:
                missing.append(_("Access Token"))
            if not api_base_url:
                missing.append(_("API Base URL"))
            request_url = False
            if api_base_url and store_hash:
                request_url = "%s/%s/v2/store" % (
                    api_base_url.rstrip("/"),
                    store_hash.strip("/"),
                )

            if missing:
                message = _("Connection is not ready. Missing: %(fields)s.") % {
                    "fields": ", ".join(missing)
                }
                instance.state = "error"
                instance.last_tested_at = now
                instance.connection_message = message
                error_count += 1
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "connection_test",
                        "resource_type": "instance",
                        "request_url": request_url,
                        "request_method": "GET",
                        "status": "failed",
                        "error_message": message,
                    }
                )
                _logger.warning(
                    "BigCommerce instance id=%s failed connection test validation. Missing: %s",
                    instance.id,
                    ", ".join(missing),
                )
                continue

            client = BigCommerceApiClient(instance._bc_service_instance())
            result = client.test_connection()
            status_code = result.get("status_code")
            response_status = str(status_code) if status_code is not None else False
            message = result.get("message") or _("BigCommerce connection test failed.")
            request_url = result.get("url")
            response_body = result.get("response_body")
            response_body_text = False
            if isinstance(response_body, str):
                response_body_text = response_body[:3000]
            elif response_body is not None:
                response_body_text = str(response_body)[:3000]

            instance.last_tested_at = now
            if result.get("success"):
                instance.state = "connected"
                instance.connection_message = message
                connected_count += 1
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "connection_test",
                        "resource_type": "instance",
                        "request_url": request_url,
                        "request_method": "GET",
                        "response_status": response_status,
                        "response_body": response_body_text,
                        "status": "success",
                        "note": message,
                    }
                )
            else:
                instance.state = "error"
                instance.connection_message = message
                error_count += 1
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "connection_test",
                        "resource_type": "instance",
                        "request_url": request_url,
                        "request_method": "GET",
                        "response_status": response_status,
                        "response_body": response_body_text,
                        "status": "failed",
                        "error_message": message,
                    }
                )
                _logger.warning(
                    "BigCommerce real connection test failed for instance id=%s status=%s message=%s",
                    instance.id,
                    response_status,
                    message,
                )

        return self._notification_action(
            _("BigCommerce connection test completed. Connected: %(connected)s, Error: %(error)s.")
            % {"connected": connected_count, "error": error_count},
            notif_type="warning" if error_count else "success",
        )

    def action_sync_products(self):
        """Run manual product import from BigCommerce."""
        self._bc_check_access("manager")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = []
            if not (instance.store_hash or "").strip():
                missing.append(_("Store Hash"))
            if not (instance.api_base_url or "").strip():
                missing.append(_("API Base URL"))
            if not (instance.access_token or "").strip():
                missing.append(_("Access Token"))

            if missing:
                failure_count += 1
                message = _("Product sync not ready. Missing: %(fields)s.") % {
                    "fields": ", ".join(missing)
                }
                instance.connection_message = message
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "product_import",
                        "resource_type": "product",
                        "status": "failed",
                        "error_message": message,
                    }
                )
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": message,
                    }
                )
                continue

            service = BigCommerceProductSyncService(instance._bc_service_instance())
            result = service.import_products(limit=instance.sync_limit_product or 100)
            instance.last_product_sync_at = fields.Datetime.now()
            instance.connection_message = result.get("message")
            failed_items = result.get("failed_items") or []
            failure_hint = "; ".join(failed_items[:3])

            if result.get("success"):
                success_count += 1
                summary_lines.append(
                    _(
                        "%(name)s: success (created=%(created)s, updated=%(updated)s, failed=%(failed)s)"
                    )
                    % {
                        "name": instance.display_name,
                        "created": result.get("created", 0),
                        "updated": result.get("updated", 0),
                        "failed": result.get("failed", 0),
                    }
                )
            else:
                failure_count += 1
                reason = result.get("message")
                if failure_hint:
                    reason = "%s | %s" % (reason, failure_hint)
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": reason,
                    }
                )

        message = _("Product sync completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s") % {
            "ok": success_count,
            "fail": failure_count,
            "details": "\n".join(summary_lines),
        }
        return self._notification_action(
            message,
            notif_type="warning" if failure_count else "success",
        )

    def action_sync_customers(self):
        """Run manual customer import from BigCommerce."""
        self._bc_check_access("manager")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = []
            if not (instance.store_hash or "").strip():
                missing.append(_("Store Hash"))
            if not (instance.api_base_url or "").strip():
                missing.append(_("API Base URL"))
            if not (instance.access_token or "").strip():
                missing.append(_("Access Token"))

            if missing:
                failure_count += 1
                message = _("Customer sync not ready. Missing: %(fields)s.") % {
                    "fields": ", ".join(missing)
                }
                instance.connection_message = message
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "customer_import",
                        "resource_type": "customer",
                        "status": "failed",
                        "error_message": message,
                    }
                )
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": message,
                    }
                )
                continue

            service = BigCommerceCustomerSyncService(instance._bc_service_instance())
            result = service.import_customers(limit=instance.sync_limit_customer or 100)
            instance.last_customer_sync_at = fields.Datetime.now()
            instance.connection_message = result.get("message")
            failed_items = result.get("failed_items") or []
            failure_hint = "; ".join(failed_items[:3])

            if result.get("success"):
                success_count += 1
                summary_lines.append(
                    _(
                        "%(name)s: success (created=%(created)s, updated=%(updated)s, failed=%(failed)s)"
                    )
                    % {
                        "name": instance.display_name,
                        "created": result.get("created", 0),
                        "updated": result.get("updated", 0),
                        "failed": result.get("failed", 0),
                    }
                )
            else:
                failure_count += 1
                reason = result.get("message")
                if failure_hint:
                    reason = "%s | %s" % (reason, failure_hint)
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": reason,
                    }
                )

        message = _("Customer sync completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s") % {
            "ok": success_count,
            "fail": failure_count,
            "details": "\n".join(summary_lines),
        }
        return self._notification_action(
            message,
            notif_type="warning" if failure_count else "success",
        )

    def action_export_customers(self):
        """Run manual customer export from Odoo to BigCommerce."""
        self._bc_check_access("admin")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = instance._ensure_connector_ready()
            if missing:
                failure_count += 1
                summary_lines.append(
                    "%s: %s"
                    % (
                        instance.display_name,
                        _("Customer export not ready. Missing: %(fields)s.")
                        % {"fields": ", ".join(missing)},
                    )
                )
                continue

            service = BigCommerceCustomerSyncService(instance._bc_service_instance())
            result = service.export_customers(limit=instance.sync_limit_customer or 100)
            instance.connection_message = result.get("message")
            if result.get("success"):
                success_count += 1
            else:
                failure_count += 1
            summary_lines.append("%s: %s" % (instance.display_name, result.get("message")))

        return self._notification_action(
            _("Customer export completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s")
            % {"ok": success_count, "fail": failure_count, "details": "\n".join(summary_lines)},
            notif_type="warning" if failure_count else "success",
        )

    def action_sync_categories(self):
        """Run manual category import from BigCommerce."""
        self._bc_check_access("manager")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = instance._ensure_connector_ready()
            if missing:
                failure_count += 1
                message = _("Category sync not ready. Missing: %(fields)s.") % {
                    "fields": ", ".join(missing)
                }
                instance.connection_message = message
                summary_lines.append("%s: %s" % (instance.display_name, message))
                continue

            service = BigCommerceCategorySyncService(instance._bc_service_instance())
            result = service.import_categories(limit=instance.sync_limit_product or 200)
            instance.last_category_sync_at = fields.Datetime.now()
            instance.connection_message = result.get("message")
            if result.get("success"):
                success_count += 1
            else:
                failure_count += 1
            summary_lines.append("%s: %s" % (instance.display_name, result.get("message")))

        return self._notification_action(
            _("Category sync completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s")
            % {"ok": success_count, "fail": failure_count, "details": "\n".join(summary_lines)},
            notif_type="warning" if failure_count else "success",
        )

    def action_export_products(self):
        """Run manual product export from Odoo to BigCommerce."""
        self._bc_check_access("admin")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = instance._ensure_connector_ready()
            if missing:
                failure_count += 1
                message = _("Product export not ready. Missing: %(fields)s.") % {
                    "fields": ", ".join(missing)
                }
                instance.connection_message = message
                summary_lines.append("%s: %s" % (instance.display_name, message))
                continue

            service = BigCommerceProductSyncService(instance._bc_service_instance())
            result = service.export_products(limit=instance.sync_limit_product or 100)
            instance.connection_message = result.get("message")
            if result.get("success"):
                success_count += 1
            else:
                failure_count += 1
            summary_lines.append("%s: %s" % (instance.display_name, result.get("message")))

        return self._notification_action(
            _("Product export completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s")
            % {"ok": success_count, "fail": failure_count, "details": "\n".join(summary_lines)},
            notif_type="warning" if failure_count else "success",
        )

    def action_sync_orders(self):
        """Run manual order import from BigCommerce."""
        self._bc_check_access("manager")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = []
            if not (instance.store_hash or "").strip():
                missing.append(_("Store Hash"))
            if not (instance.api_base_url or "").strip():
                missing.append(_("API Base URL"))
            if not (instance.access_token or "").strip():
                missing.append(_("Access Token"))

            if missing:
                failure_count += 1
                message = _("Order sync not ready. Missing: %(fields)s.") % {
                    "fields": ", ".join(missing)
                }
                instance.connection_message = message
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "order_import",
                        "resource_type": "order",
                        "status": "failed",
                        "error_message": message,
                    }
                )
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": message,
                    }
                )
                continue

            service = BigCommerceOrderSyncService(instance._bc_service_instance())
            result = service.import_orders(limit=instance.sync_limit_order or 50)
            instance.last_order_sync_at = fields.Datetime.now()
            instance.connection_message = result.get("message")
            failed_items = result.get("failed_items") or []
            failure_hint = "; ".join(failed_items[:3])

            if result.get("success"):
                success_count += 1
                summary_lines.append(
                    _(
                        "%(name)s: success (created=%(created)s, updated=%(updated)s, failed=%(failed)s)"
                    )
                    % {
                        "name": instance.display_name,
                        "created": result.get("created", 0),
                        "updated": result.get("updated", 0),
                        "failed": result.get("failed", 0),
                    }
                )
            else:
                failure_count += 1
                reason = result.get("message")
                if failure_hint:
                    reason = "%s | %s" % (reason, failure_hint)
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": reason,
                    }
                )

        message = _("Order sync completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s") % {
            "ok": success_count,
            "fail": failure_count,
            "details": "\n".join(summary_lines),
        }
        return self._notification_action(
            message,
            notif_type="warning" if failure_count else "success",
        )

    def action_import_inventory(self):
        """Run manual inventory sync from BigCommerce to Odoo."""
        self._bc_check_access("manager")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = []
            if not (instance.store_hash or "").strip():
                missing.append(_("Store Hash"))
            if not (instance.api_base_url or "").strip():
                missing.append(_("API Base URL"))
            if not (instance.access_token or "").strip():
                missing.append(_("Access Token"))

            if missing:
                failure_count += 1
                message = _("Inventory sync not ready. Missing: %(fields)s.") % {
                    "fields": ", ".join(missing)
                }
                instance.connection_message = message
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "inventory_import",
                        "resource_type": "inventory",
                        "status": "failed",
                        "error_message": message,
                    }
                )
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": message,
                    }
                )
                continue

            if instance.inventory_master != "bigcommerce":
                failure_count += 1
                message = _(
                    "Inventory sync disabled because Inventory Master is '%(master)s'. "
                    "Set it to 'BigCommerce' to import stock."
                ) % {"master": instance.inventory_master}
                instance.connection_message = message
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "inventory_import",
                        "resource_type": "inventory",
                        "status": "failed",
                        "error_message": message,
                    }
                )
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": message,
                    }
                )
                continue

            service = BigCommerceInventorySyncService(instance._bc_service_instance())
            result = service.import_inventory(limit=instance.sync_limit_inventory or 200)
            instance.last_inventory_export_at = fields.Datetime.now()
            instance.connection_message = result.get("message")
            failed_items = result.get("failed_items") or []
            failure_hint = "; ".join(failed_items[:3])

            if result.get("success"):
                success_count += 1
                summary_lines.append(
                    _(
                        "%(name)s: success (updated=%(updated)s, failed=%(failed)s, skipped=%(skipped)s)"
                    )
                    % {
                        "name": instance.display_name,
                        "updated": result.get("updated", 0),
                        "failed": result.get("failed", 0),
                        "skipped": result.get("skipped", 0),
                    }
                )
            else:
                failure_count += 1
                reason = result.get("message")
                if failure_hint:
                    reason = "%s | %s" % (reason, failure_hint)
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": reason,
                    }
                )

        message = _("Inventory sync completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s") % {
            "ok": success_count,
            "fail": failure_count,
            "details": "\n".join(summary_lines),
        }
        return self._notification_action(
            message,
            notif_type="warning" if failure_count else "success",
        )

    def action_export_inventory(self):
        """Run manual inventory export from Odoo to BigCommerce."""
        self._bc_check_access("manager")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = []
            if not (instance.store_hash or "").strip():
                missing.append(_("Store Hash"))
            if not (instance.api_base_url or "").strip():
                missing.append(_("API Base URL"))
            if not (instance.access_token or "").strip():
                missing.append(_("Access Token"))

            if missing:
                failure_count += 1
                message = _("Inventory export not ready. Missing: %(fields)s.") % {
                    "fields": ", ".join(missing)
                }
                instance.connection_message = message
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "inventory_export",
                        "resource_type": "inventory",
                        "status": "failed",
                        "error_message": message,
                    }
                )
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": message,
                    }
                )
                continue

            service = BigCommerceInventorySyncService(instance._bc_service_instance())
            result = service.export_inventory(limit=instance.sync_limit_inventory or 200)
            instance.last_inventory_export_at = fields.Datetime.now()
            instance.connection_message = result.get("message")
            failed_items = result.get("failed_items") or []
            failure_hint = "; ".join(failed_items[:3])

            if result.get("success"):
                success_count += 1
                summary_lines.append(
                    _(
                        "%(name)s: success (exported=%(exported)s, failed=%(failed)s, skipped=%(skipped)s)"
                    )
                    % {
                        "name": instance.display_name,
                        "exported": result.get("exported", 0),
                        "failed": result.get("failed", 0),
                        "skipped": result.get("skipped", 0),
                    }
                )
            else:
                failure_count += 1
                reason = result.get("message")
                if failure_hint:
                    reason = "%s | %s" % (reason, failure_hint)
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": reason,
                    }
                )

        message = _("Inventory export completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s") % {
            "ok": success_count,
            "fail": failure_count,
            "details": "\n".join(summary_lines),
        }
        return self._notification_action(
            message,
            notif_type="warning" if failure_count else "success",
        )

    def action_sync_inventory(self):
        """UI action: sync inventory in the direction of each connector's Inventory Master.

        Before 19.0.2.1.0 the button always imported, so a connector whose master is Odoo had no way
        to export its quantities from the UI.
        """
        importing = self.filtered(lambda i: i.inventory_master == "bigcommerce")
        exporting = self - importing
        result = False
        if exporting:
            result = exporting.action_export_inventory()
        if importing:
            result = importing.action_import_inventory()
        return result

    def action_export_shipments(self):
        """Run manual shipment export from Odoo to BigCommerce."""
        self._bc_check_access("manager")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = []
            if not (instance.store_hash or "").strip():
                missing.append(_("Store Hash"))
            if not (instance.api_base_url or "").strip():
                missing.append(_("API Base URL"))
            if not (instance.access_token or "").strip():
                missing.append(_("Access Token"))

            if missing:
                failure_count += 1
                message = _("Shipment export not ready. Missing: %(fields)s.") % {
                    "fields": ", ".join(missing)
                }
                instance.connection_message = message
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "shipment_export",
                        "resource_type": "shipment",
                        "status": "failed",
                        "error_message": message,
                    }
                )
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": message,
                    }
                )
                continue

            service = BigCommerceShipmentSyncService(instance._bc_service_instance())
            result = service.export_shipments(limit=instance.sync_limit_shipment or 100)
            instance.last_shipment_export_at = fields.Datetime.now()
            instance.connection_message = result.get("message")
            failed_items = result.get("failed_items") or []
            failure_hint = "; ".join(failed_items[:3])

            if result.get("success"):
                success_count += 1
                summary_lines.append(
                    _(
                        "%(name)s: success (exported=%(exported)s, failed=%(failed)s, skipped=%(skipped)s)"
                    )
                    % {
                        "name": instance.display_name,
                        "exported": result.get("exported", 0),
                        "failed": result.get("failed", 0),
                        "skipped": result.get("skipped", 0),
                    }
                )
            else:
                failure_count += 1
                reason = result.get("message")
                if failure_hint:
                    reason = "%s | %s" % (reason, failure_hint)
                summary_lines.append(
                    _("%(name)s: failed (%(reason)s)") % {
                        "name": instance.display_name,
                        "reason": reason,
                    }
                )

        message = _("Shipment export completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s") % {
            "ok": success_count,
            "fail": failure_count,
            "details": "\n".join(summary_lines),
        }
        return self._notification_action(
            message,
            notif_type="warning" if failure_count else "success",
        )

    def action_register_webhooks(self):
        """Register standard BigCommerce webhooks for this instance."""
        self._bc_check_access("admin")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = instance._ensure_connector_ready()
            if missing:
                failure_count += 1
                summary_lines.append(
                    "%s: %s"
                    % (
                        instance.display_name,
                        _("Webhook registration not ready. Missing: %(fields)s.")
                        % {"fields": ", ".join(missing)},
                    )
                )
                continue
            if not instance.webhook_enabled:
                failure_count += 1
                summary_lines.append(
                    "%s: %s"
                    % (instance.display_name, _("Webhook registration skipped because webhooks are disabled."))
                )
                continue

            webhook_secret = instance._get_or_create_webhook_secret()
            destination = instance._build_webhook_destination()
            validation_error = instance._get_webhook_destination_validation_error(destination)
            _logger.info(
                "Webhook registration destination check instance_id=%s destination=%s valid=%s",
                instance.id,
                destination,
                not bool(validation_error),
            )
            if validation_error:
                _logger.warning(
                    "Webhook registration blocked for instance_id=%s destination=%s reason=%s",
                    instance.id,
                    destination,
                    validation_error,
                )
                raise UserError(
                    _(
                        "Webhook registration blocked for '%(name)s': %(reason)s\n\n"
                        "Use a public HTTPS callback URL. For development, use an HTTPS ngrok URL "
                        "(example: https://<subdomain>.ngrok-free.app/bigcommerce/webhook?store_hash=%(store_hash)s)."
                    )
                    % {
                        "name": instance.display_name,
                        "reason": validation_error,
                        "store_hash": (instance.store_hash or "").strip(),
                    }
                )
            scopes = instance._get_default_webhook_scopes()
            client = BigCommerceApiClient(instance._bc_service_instance())

            list_result = instance._list_remote_webhooks(client)
            existing_scope_map = {}
            if list_result.get("success"):
                for hook in list_result.get("hooks") or []:
                    if (hook.get("destination") or "").strip() != destination:
                        continue
                    scope = (hook.get("scope") or "").strip()
                    if scope and scope not in existing_scope_map:
                        existing_scope_map[scope] = hook
                        instance._upsert_webhook_subscription(hook=hook, destination=destination)

            ok = 0
            failed = 0
            skipped = 0
            for scope in scopes:
                if scope in existing_scope_map:
                    skipped += 1
                    continue

                payload = instance._webhook_payload(
                    scope=scope,
                    destination=destination,
                    webhook_secret=webhook_secret,
                )
                result = client.post("/v3/hooks", payload=payload)
                if result.get("success"):
                    ok += 1
                    response_body = result.get("response_body") or {}
                    hook_data = response_body.get("data") if isinstance(response_body, dict) else {}
                    if isinstance(hook_data, dict) and hook_data:
                        instance._upsert_webhook_subscription(hook=hook_data, destination=destination)
                else:
                    failed += 1
                    safe_payload = {
                        "scope": scope,
                        "destination": destination,
                        "is_active": True,
                        "headers": {"X-Webhook-Secret": "***"},
                    }
                    self.env["bigcommerce.sync.log"].sudo().create(
                        {
                            "instance_id": instance.id,
                            "operation_type": "manual_action",
                            "resource_type": "webhook",
                            "request_method": "POST",
                            "request_url": result.get("url"),
                            "request_payload": str(safe_payload),
                            "response_status": str(result.get("status_code")) if result.get("status_code") else False,
                            "response_body": str(result.get("response_body"))[:3000]
                            if result.get("response_body")
                            else False,
                            "status": "failed",
                            "error_message": result.get("message"),
                            "note": "Webhook registration failed for scope %s." % scope,
                        }
                    )
            if failed:
                failure_count += 1
            else:
                success_count += 1
            instance.webhook_last_sync_at = fields.Datetime.now()
            summary_lines.append(
                "%s: registered=%s skipped=%s failed=%s destination=%s"
                % (instance.display_name, ok, skipped, failed, destination)
            )

        return self._notification_action(
            _("Webhook registration completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s")
            % {"ok": success_count, "fail": failure_count, "details": "\n".join(summary_lines)},
            notif_type="warning" if failure_count else "success",
        )

    def action_delete_webhooks(self):
        """Delete registered webhooks for this connector destination."""
        self._bc_check_access("admin")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = instance._ensure_connector_ready()
            if missing:
                failure_count += 1
                summary_lines.append(
                    "%s: %s"
                    % (
                        instance.display_name,
                        _("Webhook delete not ready. Missing: %(fields)s.")
                        % {"fields": ", ".join(missing)},
                    )
                )
                continue
            if not instance.webhook_enabled:
                failure_count += 1
                summary_lines.append(
                    "%s: %s"
                    % (instance.display_name, _("Webhook delete skipped because webhooks are disabled."))
                )
                continue

            destination = instance._build_webhook_destination()
            if not destination:
                failure_count += 1
                summary_lines.append(
                    "%s: %s"
                    % (
                        instance.display_name,
                        _("Webhook callback URL is not configured. Set Webhook Callback URL or web.base.url."),
                    )
                )
                continue
            client = BigCommerceApiClient(instance._bc_service_instance())
            list_result = instance._list_remote_webhooks(client)
            if not list_result.get("success"):
                failure_count += 1
                summary_lines.append("%s: %s" % (instance.display_name, list_result.get("message")))
                continue

            delete_ok = 0
            delete_fail = 0
            sub_model = self.env["bigcommerce.webhook.subscription"].sudo()
            for hook in list_result.get("hooks") or []:
                if (hook.get("destination") or "").strip() != destination:
                    continue
                hook_id = hook.get("id")
                if not hook_id:
                    continue
                result = client.delete("/v3/hooks/%s" % hook_id)
                if result.get("success"):
                    delete_ok += 1
                    sub_model.search(
                        [
                            ("instance_id", "=", instance.id),
                            ("remote_webhook_id", "=", str(hook_id)),
                        ]
                    ).write(
                        {
                            "status": "deleted",
                            "is_active": False,
                            "last_sync_at": fields.Datetime.now(),
                            "error_message": False,
                        }
                    )
                else:
                    delete_fail += 1
                    sub_model.search(
                        [
                            ("instance_id", "=", instance.id),
                            ("remote_webhook_id", "=", str(hook_id)),
                        ]
                    ).write(
                        {
                            "status": "error",
                            "last_sync_at": fields.Datetime.now(),
                            "error_message": result.get("message"),
                        }
                    )

            if delete_fail:
                failure_count += 1
            else:
                success_count += 1
            instance.webhook_last_sync_at = fields.Datetime.now()
            summary_lines.append("%s: deleted=%s failed=%s" % (instance.display_name, delete_ok, delete_fail))

        return self._notification_action(
            _("Webhook delete completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s")
            % {"ok": success_count, "fail": failure_count, "details": "\n".join(summary_lines)},
            notif_type="warning" if failure_count else "success",
        )

    def action_sync_webhooks_status(self):
        """Fetch remote webhook state and log summary for diagnostics."""
        self._bc_check_access("admin")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []

        for instance in self:
            missing = instance._ensure_connector_ready()
            if missing:
                failure_count += 1
                summary_lines.append(
                    "%s: %s"
                    % (
                        instance.display_name,
                        _("Webhook status sync not ready. Missing: %(fields)s.")
                        % {"fields": ", ".join(missing)},
                    )
                )
                continue
            if not instance.webhook_enabled:
                failure_count += 1
                summary_lines.append(
                    "%s: %s"
                    % (instance.display_name, _("Webhook status sync skipped because webhooks are disabled."))
                )
                continue

            destination = instance._build_webhook_destination()
            if not destination:
                failure_count += 1
                summary_lines.append(
                    "%s: %s"
                    % (
                        instance.display_name,
                        _("Webhook callback URL is not configured. Set Webhook Callback URL or web.base.url."),
                    )
                )
                continue
            client = BigCommerceApiClient(instance._bc_service_instance())
            list_result = instance._list_remote_webhooks(client)
            if not list_result.get("success"):
                failure_count += 1
                summary_lines.append("%s: %s" % (instance.display_name, list_result.get("message")))
                continue

            hooks = [hook for hook in (list_result.get("hooks") or []) if (hook.get("destination") or "").strip() == destination]
            scope_names = sorted(set((hook.get("scope") or "").strip() for hook in hooks if hook.get("scope")))
            remote_ids = set(str(hook.get("id")) for hook in hooks if hook.get("id"))

            sub_model = self.env["bigcommerce.webhook.subscription"].sudo()
            for hook in hooks:
                instance._upsert_webhook_subscription(hook=hook, destination=destination)

            missing_subs = sub_model.search(
                [
                    ("instance_id", "=", instance.id),
                    ("destination", "=", destination),
                    ("remote_webhook_id", "!=", False),
                ]
            ).filtered(lambda sub: sub.remote_webhook_id not in remote_ids)
            if missing_subs:
                missing_subs.write(
                    {
                        "status": "missing",
                        "is_active": False,
                        "last_sync_at": fields.Datetime.now(),
                    }
                )

            self.env["bigcommerce.sync.log"].sudo().create(
                {
                    "instance_id": instance.id,
                    "operation_type": "manual_action",
                    "resource_type": "webhook",
                    "status": "success",
                    "note": "Webhook status sync: %s hooks for destination. Scopes: %s"
                    % (len(hooks), ", ".join(scope_names) or "none"),
                }
            )
            instance.webhook_last_sync_at = fields.Datetime.now()
            success_count += 1
            summary_lines.append("%s: hooks=%s scopes=%s" % (instance.display_name, len(hooks), len(scope_names)))

        return self._notification_action(
            _("Webhook status sync completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s")
            % {"ok": success_count, "fail": failure_count, "details": "\n".join(summary_lines)},
            notif_type="warning" if failure_count else "success",
        )

    def action_process_webhooks(self):
        """Run manual webhook processing for pending/failed events."""
        self._bc_check_access("manager")
        self = self.sudo()
        success_count = 0
        failure_count = 0
        summary_lines = []
        service = BigCommerceWebhookService(self.env)

        for instance in self:
            result = service.process_pending_events(instance=instance, limit=instance.sync_limit_webhook or 200)
            instance.last_webhook_process_at = fields.Datetime.now()
            instance.connection_message = result.get("message")
            if result.get("success"):
                success_count += 1
            else:
                failure_count += 1
            summary_lines.append("%s: %s" % (instance.display_name, result.get("message")))

        return self._notification_action(
            _("Webhook processing completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s")
            % {"ok": success_count, "fail": failure_count, "details": "\n".join(summary_lines)},
            notif_type="warning" if failure_count else "success",
        )

    def action_send_test_webhook_event(self):
        """Create and process a synthetic webhook event to verify local pipeline wiring."""
        self._bc_check_access("manager")
        self = self.sudo()
        service = BigCommerceWebhookService(self.env)
        created = 0
        failed = 0
        summary_lines = []

        for instance in self:
            if not instance.webhook_enabled:
                failed += 1
                summary_lines.append(
                    "%s: %s" % (instance.display_name, _("Webhook test skipped because webhooks are disabled."))
                )
                continue

            payload = {
                "type": "manual_test_event",
                "scope": "store/test/ping",
                "data": {"id": "manual-test-%s" % instance.id},
            }
            headers = {
                "X-BC-Scope": "store/test/ping",
                "X-BC-Webhook-Id": "manual-test-%s" % instance.id,
                "X-BC-Store-Hash": (instance.store_hash or "").strip(),
            }

            try:
                ingest = service.ingest_webhook(
                    instance=instance,
                    payload=payload,
                    headers=headers,
                    destination=instance._build_webhook_destination() or False,
                )
                event = self.env["bigcommerce.webhook.event"].sudo().browse(ingest.get("event_id"))
                process_result = service.process_event(event)
                if process_result.get("success"):
                    created += 1
                    summary_lines.append(
                        "%s: test event created and processed (event_id=%s)."
                        % (instance.display_name, event.id)
                    )
                else:
                    failed += 1
                    summary_lines.append(
                        "%s: test event created but processing failed (%s)."
                        % (instance.display_name, process_result.get("message"))
                    )
            except Exception as err:
                failed += 1
                summary_lines.append("%s: %s" % (instance.display_name, str(err)))

        return self._notification_action(
            _("Webhook test completed. Success: %(ok)s, Failed: %(fail)s.\n%(details)s")
            % {"ok": created, "fail": failed, "details": "\n".join(summary_lines)},
            notif_type="warning" if failed else "success",
        )

    @api.model
    def run_cron_sync_products(self):
        """Cron entrypoint: product import for enabled instances."""
        for instance in self.search([("active", "=", True), ("auto_product_sync", "=", True)]):
            try:
                BigCommerceProductSyncService(instance._bc_service_instance()).import_products(limit=instance.sync_limit_product or 100)
                self._safe_write_sync_timestamp(instance, "last_product_sync_at")
            except Exception as err:
                _logger.exception("Cron product sync failed for instance_id=%s", instance.id)
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "product_import",
                        "resource_type": "product",
                        "status": "failed",
                        "error_message": str(err),
                        "note": "Cron product sync failed.",
                    }
                )

    @api.model
    def run_cron_sync_orders(self):
        """Cron entrypoint: order import for enabled instances."""
        for instance in self.search([("active", "=", True), ("auto_order_sync", "=", True)]):
            try:
                BigCommerceOrderSyncService(instance._bc_service_instance()).import_orders(limit=instance.sync_limit_order or 50)
                self._safe_write_sync_timestamp(instance, "last_order_sync_at")
            except Exception as err:
                _logger.exception("Cron order sync failed for instance_id=%s", instance.id)
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "order_import",
                        "resource_type": "order",
                        "status": "failed",
                        "error_message": str(err),
                        "note": "Cron order sync failed.",
                    }
                )

    @api.model
    def run_cron_sync_customers(self):
        """Cron entrypoint: customer import for enabled instances."""
        for instance in self.search([("active", "=", True), ("auto_sync_customers", "=", True)]):
            try:
                BigCommerceCustomerSyncService(instance._bc_service_instance()).import_customers(limit=instance.sync_limit_customer or 100)
                self._safe_write_sync_timestamp(instance, "last_customer_sync_at")
            except Exception as err:
                _logger.exception("Cron customer sync failed for instance_id=%s", instance.id)
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "customer_import",
                        "resource_type": "customer",
                        "status": "failed",
                        "error_message": str(err),
                        "note": "Cron customer sync failed.",
                        "severity": "error",
                    }
                )

    @api.model
    def run_cron_sync_inventory(self):
        """Cron entrypoint: inventory sync in the direction of each connector's inventory master.

        BigCommerce master: stock levels are imported into Odoo. Odoo master: Odoo quantities are
        exported to BigCommerce. (Before 19.0.2.1.0 only the import ran, so stores whose master
        was Odoo were never updated automatically.)
        """
        for instance in self.search([("active", "=", True), ("auto_inventory_export", "=", True)]):
            operation = "inventory_import" if instance.inventory_master == "bigcommerce" else "inventory_export"
            try:
                service = BigCommerceInventorySyncService(instance._bc_service_instance())
                if instance.inventory_master == "bigcommerce":
                    service.import_inventory(limit=instance.sync_limit_inventory or 200)
                else:
                    service.export_inventory(limit=instance.sync_limit_inventory or 200)
                self._safe_write_sync_timestamp(instance, "last_inventory_export_at")
            except Exception as err:
                _logger.exception("Cron inventory sync failed for instance_id=%s", instance.id)
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": operation,
                        "resource_type": "inventory",
                        "status": "failed",
                        "error_message": str(err),
                        "note": "Cron inventory sync failed.",
                    }
                )

    @api.model
    def run_cron_export_inventory(self):
        """Cron entrypoint: inventory export for enabled instances."""
        for instance in self.search([("active", "=", True), ("auto_inventory_export", "=", True)]):
            try:
                if instance.inventory_master == "odoo":
                    BigCommerceInventorySyncService(instance._bc_service_instance()).export_inventory(
                        limit=instance.sync_limit_inventory or 200
                    )
                    self._safe_write_sync_timestamp(instance, "last_inventory_export_at")
            except Exception as err:
                _logger.exception("Cron inventory export failed for instance_id=%s", instance.id)
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "inventory_export",
                        "resource_type": "inventory",
                        "status": "failed",
                        "error_message": str(err),
                        "note": "Cron inventory export failed.",
                    }
                )

    @api.model
    def run_cron_export_shipments(self):
        """Cron entrypoint: shipment export for enabled instances."""
        for instance in self.search([("active", "=", True), ("auto_shipment_export", "=", True)]):
            try:
                BigCommerceShipmentSyncService(instance._bc_service_instance()).export_shipments(
                    limit=instance.sync_limit_shipment or 100
                )
                self._safe_write_sync_timestamp(instance, "last_shipment_export_at")
            except Exception as err:
                _logger.exception("Cron shipment export failed for instance_id=%s", instance.id)
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "shipment_export",
                        "resource_type": "shipment",
                        "status": "failed",
                        "error_message": str(err),
                        "note": "Cron shipment export failed.",
                    }
                )

    @api.model
    def run_cron_process_webhooks(self):
        """Cron entrypoint: webhook processing for enabled instances."""
        service = BigCommerceWebhookService(self.env)
        for instance in self.search([("active", "=", True), ("auto_webhook_process", "=", True)]):
            try:
                service.process_pending_events(instance=instance, limit=instance.sync_limit_webhook or 200)
                self._safe_write_sync_timestamp(instance, "last_webhook_process_at")
            except Exception as err:
                _logger.exception("Cron webhook processing failed for instance_id=%s", instance.id)
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "webhook_process",
                        "resource_type": "webhook",
                        "status": "failed",
                        "error_message": str(err),
                        "note": "Cron webhook processing failed.",
                    }
                )

    @api.model
    def run_cron_purge_history(self):
        """Cron entrypoint: purge old sync logs and finished/rejected webhook events.

        Pending, processing and failed webhook events are never purged: they are work or
        exceptions someone still has to look at.
        """
        log_model = self.env["bigcommerce.sync.log"].sudo()
        event_model = self.env["bigcommerce.webhook.event"].sudo()
        purged_logs = purged_events = 0
        for instance in self.with_context(active_test=False).search([]):
            days = int(instance.log_retention_days or 0)
            if days <= 0:
                continue
            limit_date = fields.Datetime.now() - timedelta(days=days)
            logs = log_model.search(
                [("instance_id", "=", instance.id), ("create_date", "<", limit_date)], limit=50000
            )
            events = event_model.search(
                [
                    ("instance_id", "=", instance.id),
                    ("status", "in", ("done", "rejected", "duplicate")),
                    ("received_at", "<", limit_date),
                ],
                limit=50000,
            )
            purged_logs += len(logs)
            purged_events += len(events)
            logs.unlink()
            events.unlink()
        _logger.info(
            "BigCommerce history purge removed %s sync logs and %s webhook events.", purged_logs, purged_events
        )
        return {"logs": purged_logs, "events": purged_events}
