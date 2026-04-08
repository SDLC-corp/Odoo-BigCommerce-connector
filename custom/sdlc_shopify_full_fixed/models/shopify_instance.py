from urllib.parse import urlparse, urlunparse

from odoo import api, fields, models
from odoo.exceptions import UserError
import requests
import base64
import logging
from datetime import timedelta
import calendar
from odoo import fields as odoo_fields

_logger = logging.getLogger(__name__)


class ShopifyInstance(models.Model):
    _name = "shopify.instance"
    _description = "Shopify Instance"
    _sql_constraints = [
        (
            "shop_url_unique",
            "unique(shop_url)",
            "A Shopify instance with this Shop URL already exists.",
        ),
    ]

    # ==================================================
    # BASIC CONFIG
    # ==================================================
    name = fields.Char(required=True)
    shop_url = fields.Char(required=True)
    api_version = fields.Char(
        default="2025-10",
        required=True,
        help="Shopify API version, e.g. 2025-10. Defaults to a supported release instead of a future version.",
    )
    access_token = fields.Char(required=True)
    active = fields.Boolean(default=True)
    notification_email = fields.Char("Notification Email")
    webhook_secret = fields.Char("Shopify Webhook Secret")

     # ---------------- PRODUCT WEBHOOK ----------------
    webhook_product_create = fields.Boolean("Product Create Sync", default=True)
    webhook_product_update = fields.Boolean("Product Update Sync", default=False)

    # ---------------- CUSTOMER WEBHOOK ----------------
    webhook_customer_create = fields.Boolean(default=True)
    webhook_customer_update = fields.Boolean(default=True)

    # ---------------- ORDER WEBHOOK ----------------
    webhook_order_create = fields.Boolean(default=True)
    webhook_order_update = fields.Boolean(default=True)

    # ---------------- CATEGORY WEBHOOK ----------------
    webhook_category_create = fields.Boolean(default=True)
    webhook_category_update = fields.Boolean(default=True)

    # ---------------- GIFTCARD WEBHOOK ----------------
    webhook_giftcard_create = fields.Boolean(default=True)
    webhook_giftcard_update = fields.Boolean(default=True)

    # ==================================================
    # AUTO SYNC CONFIG (BASE)
    # ==================================================
    auto_product_sync = fields.Boolean("Auto Product Sync")
    auto_product_interval = fields.Integer(default=1)
    product_cron_id = fields.Many2one("ir.cron", readonly=True)

    auto_customer_sync = fields.Boolean("Auto Customer Sync")
    auto_customer_interval = fields.Integer(default=1)
    customer_cron_id = fields.Many2one("ir.cron", readonly=True)

    auto_order_sync = fields.Boolean("Auto Order Sync")
    auto_order_interval = fields.Integer(default=1)
    order_cron_id = fields.Many2one("ir.cron", readonly=True)

    auto_category_sync = fields.Boolean("Auto Category Sync")
    auto_category_interval = fields.Integer(default=1)
    category_cron_id = fields.Many2one("ir.cron", readonly=True)

    auto_giftcard_sync = fields.Boolean("Auto Gift Card Sync")
    auto_giftcard_interval = fields.Integer(default=1)
    giftcard_cron_id = fields.Many2one("ir.cron", readonly=True)

    last_product_sync_at = fields.Datetime()
    last_customer_sync_at = fields.Datetime()
    last_order_sync_at = fields.Datetime()
    last_category_sync_at = fields.Datetime()
    last_giftcard_sync_at = fields.Datetime()

    # ==================================================
    # FREQUENCY DROPDOWN
    # ==================================================
    INTERVAL_TYPE = [
        ("hours", "Hourly"),
        ("days", "Daily"),
        ("weeks", "Weekly"),
        ("months", "Monthly"),
    ]

    auto_product_interval_type = fields.Selection(INTERVAL_TYPE, default="hours")
    auto_customer_interval_type = fields.Selection(INTERVAL_TYPE, default="hours")
    auto_order_interval_type = fields.Selection(INTERVAL_TYPE, default="hours")
    auto_category_interval_type = fields.Selection(INTERVAL_TYPE, default="days")
    auto_giftcard_interval_type = fields.Selection(INTERVAL_TYPE, default="days")

    # ==================================================
    # ADVANCED SCHEDULER OPTIONS
    # ==================================================
    auto_sync_hour = fields.Selection(
        [(str(i), f"{i:02d}") for i in range(0, 24)], string="Hour", default="0"
    )

    auto_sync_minute = fields.Selection(
        [(str(i), f"{i:02d}") for i in range(0, 60, 5)], string="Minute", default="0"
    )

    auto_sync_weekday = fields.Selection(
        [
            ("0", "Monday"),
            ("1", "Tuesday"),
            ("2", "Wednesday"),
            ("3", "Thursday"),
            ("4", "Friday"),
            ("5", "Saturday"),
            ("6", "Sunday"),
        ],
        string="Weekday",
        default="0",
    )

    auto_sync_month_day = fields.Selection(
        [(str(i), str(i)) for i in range(1, 32)], string="Day of Month", default="1"
    )

    # ==================================================
    # INVENTORY LOCATION CONFIG
    # ==================================================
    warehouse_id = fields.Many2one(
        "stock.warehouse",
        string="Odoo Warehouse",
        help="Warehouse whose stock movements should sync with this Shopify instance.",
    )
    shopify_location_id = fields.Char(
        string="Shopify Location ID",
        help="Shopify location to push inventory levels to. Configure this to avoid using a default location.",
    )

    # ==================================================
    # SHOP URL VALIDATION
    # ==================================================
    def _normalize_shop_url(self, url):
        """Return a canonical form for comparison (scheme + host, trimmed path)."""
        url = (url or "").strip()
        if not url:
            return ""
        if "://" not in url:
            url = f"https://{url}"
        parsed = urlparse(url)
        netloc = (parsed.netloc or "").strip().lower()
        path = (parsed.path or "").rstrip("/")
        if not netloc:
            raise UserError("Please provide a valid Shopify Shop URL.")
        return urlunparse((parsed.scheme or "https", netloc, path or "", "", "", ""))

    def _ensure_unique_shop_url(self, normalized_url):
        """Raise a friendly error before hitting the SQL unique constraint."""
        domain = [("shop_url", "=", normalized_url)]
        if self.ids:
            domain.append(("id", "not in", self.ids))
        if self.search_count(domain):
            raise UserError("A Shopify instance with this Shop URL already exists.")

    @api.model_create_multi
    def create(self, vals_list):
        sanitized = []
        for vals in vals_list:
            vals = dict(vals)
            if "shop_url" in vals:
                normalized = self._normalize_shop_url(vals.get("shop_url"))
                if not normalized:
                    raise UserError("Shop URL is required.")
                vals["shop_url"] = normalized
                self._ensure_unique_shop_url(normalized)
            sanitized.append(vals)
        return super().create(sanitized)

    def write(self, vals):
        vals = dict(vals)
        if "shop_url" in vals:
            normalized = self._normalize_shop_url(vals.get("shop_url"))
            if not normalized:
                raise UserError("Shop URL is required.")
            vals["shop_url"] = normalized
            self._ensure_unique_shop_url(normalized)
        return super().write(vals)

    # ==================================================
    # TEST CONNECTION
    # ==================================================
    def action_test_connection(self):
        self.ensure_one()
        token = self._clean_access_token()
        headers = {
            "X-Shopify-Access-Token": token,
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        configured_version = (self.api_version or "").strip()
        fallback_version = "2024-07"

        def _call(version):
            url = self._build_url("shop.json", version=version)
            return requests.get(url, headers=headers, timeout=20)

        try:
            res = _call(configured_version or fallback_version)
            if res.status_code == 401 and configured_version and configured_version != fallback_version:
                # Retry automatically with a supported fallback and persist it if it works.
                retry = _call(fallback_version)
                if retry.ok:
                    self.write({"api_version": fallback_version})
                    res = retry
                else:
                    retry.raise_for_status()
            res.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            # Surface Shopify's body to clarify invalid token vs version issues.
            body = res.text if "res" in locals() else str(exc)
            raise UserError(
                f"Failed to connect to Shopify ({res.status_code if 'res' in locals() else ''}). "
                f"Details: {body}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise UserError(f"Failed to connect to Shopify: {exc}") from exc

        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Success",
                "message": "Shopify Connected Successfully",
                "type": "success",
            },
        }

    # ==================================================
    # SHOPIFY HELPERS
    # ==================================================
    def _api_version(self):
        """Return the configured API version, or a safe default when empty."""
        version = (self.api_version or "").strip()
        return version or "2024-07"

    def _clean_access_token(self):
        """Strip accidental whitespace and ensure a token is present."""
        token = (self.access_token or "").strip()
        if not token:
            raise UserError("Shopify access token is missing on this instance.")
        return token

    def _build_url(self, endpoint, version=None):
        base = self.shop_url.rstrip("/")
        ver = (version or self._api_version()).strip() or "2024-07"
        return f"{base}/admin/api/{ver}/{endpoint}"

    def _headers(self):
        self.ensure_one()
        token = self._clean_access_token()
        return {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": token,
            # Some shops accept only one of these; send both defensively.
            "Authorization": f"Bearer {token}",
        }

    def _get(self, endpoint, params=None):
        self.ensure_one()
        configured_version = (self.api_version or "").strip()
        fallback_version = "2024-07"

        def _call(version):
            url = self._build_url(endpoint, version=version)
            return url, requests.get(
                url, headers=self._headers(), params=params or {}, timeout=30
            )

        try:
            url, res = _call(configured_version or fallback_version)
        except requests.exceptions.RequestException as exc:
            # Retry once with a known good version if the configured one is unsupported.
            if configured_version and configured_version != fallback_version:
                try:
                    url, res = _call(fallback_version)
                    self.write({"api_version": fallback_version})
                except requests.exceptions.RequestException:
                    _logger.exception("Shopify GET failed for %s", url)
                    raise UserError(
                        f"Failed to connect to Shopify at {url}. "
                        "Please verify the shop URL, internet connectivity, and DNS settings.\n\n"
                        f"Details: {exc}"
                    ) from exc
            else:
                _logger.exception("Shopify GET failed for %s", url)
                raise UserError(
                    f"Failed to connect to Shopify at {url}. "
                    "Please verify the shop URL, internet connectivity, and DNS settings.\n\n"
                    f"Details: {exc}"
                ) from exc
        try:
            res.raise_for_status()
        except Exception as exc:
            raise UserError(res.text or str(exc))
        return res.json()

    def _get_page(self, endpoint, params=None):
        """Return a raw response (for pagination) with raise_for_status applied."""
        self.ensure_one()
        configured_version = (self.api_version or "").strip()
        fallback_version = "2024-07"

        def _call(version):
            url = self._build_url(endpoint, version=version)
            return url, requests.get(
                url, headers=self._headers(), params=params or {}, timeout=30
            )

        try:
            url, res = _call(configured_version or fallback_version)
        except requests.exceptions.RequestException as exc:
            if configured_version and configured_version != fallback_version:
                try:
                    url, res = _call(fallback_version)
                    self.write({"api_version": fallback_version})
                except requests.exceptions.RequestException:
                    _logger.exception("Shopify GET failed for %s", url)
                    raise UserError(
                        f"Failed to connect to Shopify at {url}. Please verify the shop URL, internet connectivity, and DNS settings.\n\nDetails: {exc}"
                    ) from exc
            else:
                _logger.exception("Shopify GET failed for %s", url)
                raise UserError(
                    f"Failed to connect to Shopify at {url}. Please verify the shop URL, internet connectivity, and DNS settings.\n\nDetails: {exc}"
                ) from exc
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            message = res.text or str(exc)
            if res.status_code == 401:
                message = (
                    "Shopify returned 401 Unauthorized. "
                    "Check the access token and API version on this instance, "
                    "then run 'Test Connection'. The test will auto-fallback to 2024-07 if your version is unsupported."
                )
            raise UserError(message) from exc
        return res

    def _iterate(self, endpoint, params, root_key):
        """Iterate through Shopify cursor pages using Link headers."""
        records = []
        next_params = params or {}
        while True:
            res = self._get_page(endpoint, next_params)
            data = res.json() or {}
            chunk = data.get(root_key) or []
            records.extend(chunk)

            link = res.headers.get("Link") or res.headers.get("link")
            if not link or 'rel="next"' not in link:
                break
            # Extract page_info from the next rel
            page_info = None
            for part in link.split(","):
                if 'rel="next"' in part:
                    if "page_info=" in part:
                        page_info = part.split("page_info=")[1].split(">")[0]
                        page_info = page_info.strip("?").strip()
                    break
            if not page_info:
                break
            next_params = {"limit": next_params.get("limit", 250), "page_info": page_info}
        return records

    # ==================================================
    # REPORT HELPERS
    # ==================================================
    def _open_report(self, sync_type):
        return (
            self.env["shopify.sync.report"]
            .sudo()
            .create(
                {
                    "instance_id": self.id,
                    "sync_type": sync_type,
                    "start_time": fields.Datetime.now(),
                    "total_records": 0,
                    "success_count": 0,
                    "error_count": 0,
                }
            )
        )

    def _close_report(self, report, total, success, error):
        report.write(
            {
                "end_time": fields.Datetime.now(),
                "total_records": total,
                "success_count": success,
                "error_count": error,
            }
        )

    # ==================================================
    # NEXT CALL CALCULATION
    # ==================================================
    def _calculate_nextcall(self, interval_type, interval_number=1):
        now = fields.Datetime.now()

        hour = int(self.auto_sync_hour or 0)
        minute = int(self.auto_sync_minute or 0)

        if interval_type == "hours":
            return now + timedelta(hours=interval_number)

        if interval_type == "days":
            base = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if base <= now:
                base += timedelta(days=interval_number)
            return base

        if interval_type == "weeks":
            weekday = int(self.auto_sync_weekday or 0)
            base = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            days_ahead = weekday - base.weekday()
            if days_ahead <= 0:
                days_ahead += 7
            return base + timedelta(days=days_ahead)

        if interval_type == "months":
            year = now.year
            month = now.month
            day = int(self.auto_sync_month_day or 1)

            last_day = calendar.monthrange(year, month)[1]
            day = min(day, last_day)

            base = now.replace(
                year=year,
                month=month,
                day=day,
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
            )

            if base <= now:
                month += interval_number
                if month > 12:
                    month -= 12
                    year += 1

                last_day = calendar.monthrange(year, month)[1]
                day = min(day, last_day)
                base = base.replace(year=year, month=month, day=day)

            return base

        return now + timedelta(hours=1)

    # ==================================================
    # SYNC METHODS (WRAPPER for MULTI INSTANCES)
    # ==================================================
    def _sync_products(self, mode="manual"):
        for instance in self:
            instance._sync_products_single(mode=mode)

    def _sync_products_single(self, mode="manual"):
        self.ensure_one()
        ReportLine = self.env["shopify.sync.report.line"].sudo()
        report = self._open_report("product")

        total = success = error = 0
        products = self._iterate("products.json", {"limit": 250}, "products")
        Product = self.env["product.template"].sudo()

        for p in products:
            total += 1
            pid = str(p["id"])
            try:
                vals = {
                    "name": p.get("title") or f"Shopify Product {pid}",
                    "shopify_product_id": pid,
                    "type": "consu",
                    "description_sale": p.get("body_html") or False,
                }

                variants = p.get("variants") or []
                if variants:
                    vals["list_price"] = float(variants[0].get("price", 0.0))

                # Pull first product image from Shopify into Odoo
                images = p.get("images") or []
                if images:
                    src = (images[0] or {}).get("src")
                    if src:
                        try:
                            resp = requests.get(src, timeout=10)
                            if resp.status_code == 200:
                                vals["image_1920"] = base64.b64encode(resp.content)
                        except Exception:
                            pass

                if "shopify_instance_id" in Product._fields:
                    vals["shopify_instance_id"] = self.id

                prod = Product.search([("shopify_product_id", "=", pid), ("shopify_instance_id", "=", self.id)] if "shopify_instance_id" in Product._fields else [("shopify_product_id", "=", pid)], limit=1)
                if prod:
                    prod.write(vals)
                else:
                    Product.create(vals)

                success += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "product",
                        "shopify_id": pid,
                        "name": vals["name"],
                        "status": "success",
                    }
                )
            except Exception as e:
                error += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "product",
                        "shopify_id": pid,
                        "status": "error",
                        "error_message": str(e),
                    }
                )

        self._close_report(report, total, success, error)

    def _sync_customers(self, mode="manual"):
        for instance in self:
            instance._sync_customers_single(mode=mode)

    def _sync_customers_single(self, mode="manual"):
        self.ensure_one()

        ReportLine = self.env["shopify.sync.report.line"].sudo()
        Partner = self.env["res.partner"].sudo()
        report = self._open_report("customer")

        total = success = error = 0
        customers = self._iterate("customers.json", {"limit": 250}, "customers")

        for c in customers:
            total += 1
            cid = str(c.get("id"))

            try:
                with self.env.cr.savepoint():
                    vals = Partner.map_shopify_customer_to_odoo(c, instance=self) or {}
                    vals.setdefault("shopify_customer_id", cid)
                    vals.setdefault("shopify_instance_id", self.id)
                    vals.setdefault("customer_rank", 1)

                partner = Partner.search(
                    [
                        ("shopify_customer_id", "=", cid),
                        ("shopify_instance_id", "=", self.id),
                    ],
                    limit=1,
                )
                if not partner:
                    # Fallback for legacy records without instance linkage
                    partner = Partner.search([("shopify_customer_id", "=", cid)], limit=1)

                if not partner and not vals.get("name"):
                    vals["name"] = "Shopify Customer"

                if partner:
                    partner.write(vals)
                else:
                    Partner.create(vals)

                success += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "customer",
                        "shopify_id": cid,
                        "name": vals.get("name"),
                        "status": "success",
                    }
                )

            except Exception as e:
                error += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "customer",
                        "shopify_id": cid,
                        "status": "error",
                        "error_message": str(e),
                    }
                )

        self._close_report(report, total, success, error)

    def _sync_orders(self, mode="manual"):
        for instance in self:
            instance._sync_orders_single(mode=mode)

    def _sync_orders_single(self, mode="manual"):
        self.ensure_one()
        ReportLine = self.env["shopify.sync.report.line"].sudo()
        report = self._open_report("order")

        total = success = error = 0
        orders = self._iterate("orders.json", {"limit": 250, "status": "any"}, "orders")
        Order = self.env["sale.order"].sudo()

        for o in orders:
            total += 1
            oid = str(o["id"])
            try:
                # Isolate each order so one bad payload doesn't abort the whole sync
                with self.env.cr.savepoint():
                    order = Order.map_shopify_order_to_odoo(o, instance=self)

                success += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "order",
                        "shopify_id": oid,
                        "name": order.name if order else False,
                        "status": "success",
                    }
                )
            except Exception as e:
                _logger.exception("Shopify order import failed for %s", oid)
                error += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "order",
                        "shopify_id": oid,
                        "status": "error",
                        "error_message": str(e),
                    }
                )

        self._close_report(report, total, success, error)

    def _sync_categories(self, mode="manual"):
        for instance in self:
            instance._sync_categories_single(mode=mode)

    def _sync_categories_single(self, mode="manual"):
        self.ensure_one()

        ReportLine = self.env["shopify.sync.report.line"].sudo()
        Category = self.env["product.category"].sudo()

        report = self._open_report("category")

        total = success = error = 0
        categories = self._iterate(
            "custom_collections.json", {"limit": 250}, "custom_collections"
        )

        for c in categories:
            total += 1
            cid = str(c.get("id"))

            try:
                vals = {
                    "name": c.get("title") or f"Shopify Category {cid}",
                    "shopify_collection_id": cid,
                }

                domain = [("shopify_collection_id", "=", cid)]
                if "shopify_instance_id" in Category._fields:
                    vals["shopify_instance_id"] = self.id
                    domain.append(("shopify_instance_id", "=", self.id))
                category = Category.search(domain, limit=1)

                if category:
                    category.write(vals)
                else:
                    Category.create(vals)

                success += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "category",
                        "shopify_id": cid,
                        "name": vals["name"],
                        "status": "success",
                    }
                )

            except Exception as e:
                error += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "category",
                        "shopify_id": cid,
                        "status": "error",
                        "error_message": str(e),
                    }
                )

        self._close_report(report, total, success, error)

    def _sync_gift_cards(self, mode="manual"):
        for instance in self:
            instance._sync_gift_cards_single(mode=mode)

    def _sync_gift_cards_single(self, mode="manual"):
        self.ensure_one()

        ReportLine = self.env["shopify.sync.report.line"].sudo()
        GiftCard = self.env["shopify.gift.card"].sudo()

        report = self._open_report("gift_card")

        total = success = error = 0
        gift_cards = self._iterate("gift_cards.json", {"limit": 250}, "gift_cards")

        for g in gift_cards:
            total += 1
            gid = str(g.get("id"))

            try:
                vals = {
                    "instance_id": self.id,
                    "shopify_gift_card_id": gid,
                    "code": g.get("code"),
                    "value": float(g.get("initial_value") or 0.0),
                    "balance": float(g.get("balance") or 0.0),
                    "currency": g.get("currency") or False,
                    "active": not g.get("disabled", False),
                    "expiry_date": g.get("expires_on") or False,
                    "state": g.get("status") or g.get("state"),
                }

                gift = GiftCard.search(
                    [
                        ("shopify_gift_card_id", "=", gid),
                        ("instance_id", "=", self.id),
                    ],
                    limit=1,
                )

                if gift:
                    gift.write(vals)
                else:
                    GiftCard.create(vals)

                success += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "gift_card",
                        "shopify_id": gid,
                        "name": g.get("code"),
                        "status": "success",
                    }
                )

            except Exception as e:
                error += 1
                ReportLine.create(
                    {
                        "report_id": report.id,
                        "record_type": "gift_card",
                        "shopify_id": gid,
                        "status": "error",
                        "error_message": str(e),
                    }
                )

        self._close_report(report, total, success, error)

    # ==================================================
    # CHECK IF TIME TO SYNC
    # ==================================================
    def _is_time_to_sync(self, last_sync, interval_type, interval_number):
        if not last_sync:
            return True

        now = fields.Datetime.now()

        if interval_type == "hours":
            return now >= last_sync + timedelta(hours=interval_number)

        if interval_type == "days":
            return now >= last_sync + timedelta(days=interval_number)

        if interval_type == "weeks":
            return now >= last_sync + timedelta(weeks=interval_number)

        if interval_type == "months":
            return now >= last_sync + timedelta(days=30 * interval_number)

        return False

    # ==================================================
    # BUTTON ACTIONS
    # ==================================================
    def action_sync_products(self):
        self._sync_products()
        for instance in self:
            instance.action_sync_inventory_levels()
        now = fields.Datetime.now()
        self.write({"last_product_sync_at": now})
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Success",
                "message": "Product and inventory sync finished",
                "type": "success",
            },
        }

    def action_sync_customers(self):
        self._sync_customers()
        now = fields.Datetime.now()
        self.write({"last_customer_sync_at": now})
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Success",
                "message": "Customer sync finished",
                "type": "success",
            },
        }

    def action_sync_orders(self):
        self._sync_orders()
        now = fields.Datetime.now()
        self.write({"last_order_sync_at": now})
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Success",
                "message": "Order sync finished",
                "type": "success",
            },
        }

    def action_sync_categories(self):
        self._sync_categories()
        now = fields.Datetime.now()
        self.write({"last_category_sync_at": now})
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Success",
                "message": "Category sync finished",
                "type": "success",
            },
        }

    def action_sync_gift_cards(self):
        self._sync_gift_cards()
        now = fields.Datetime.now()
        self.write({"last_giftcard_sync_at": now})
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Success",
                "message": "Gift Card sync finished",
                "type": "success",
            },
        }

    def action_sync_inventory_levels(self):
        """Fetch Shopify product/variant quantities and refresh inventory report."""
        self.ensure_one()
        Product = self.env["product.template"].sudo()
        InventoryReport = self.env["shopify.inventory.report"].sudo()

        # Guard against concurrent inventory syncs on the same instance to avoid
        # serialization failures when writing the same product rows.
        lock_key = 922337 + self.id  # stable int key per instance
        self.env.cr.execute("SELECT pg_try_advisory_lock(%s)", [lock_key])
        locked = self.env.cr.fetchone()[0]
        if not locked:
            raise UserError(
                "Another Shopify inventory sync is already running for this instance. "
                "Please try again in a moment."
            )

        # Pull products (includes variants with inventory_quantity)
        products = self._iterate("products.json", {"limit": 250}, "products")

        try:
            updated = 0
            for p in products:
                pid = str(p.get("id"))
                tmpl = Product.search(
                    [("shopify_product_id", "=", pid), ("shopify_instance_id", "=", self.id)],
                    limit=1,
                )
                if not tmpl:
                    continue
                variants = p.get("variants") or []
                total_qty = 0.0
                for var in variants:
                    try:
                        total_qty += float(var.get("inventory_quantity") or 0.0)
                    except Exception:
                        continue
                tmpl.write({"shopify_available_qty": total_qty})
                updated += 1

            # Rebuild report with latest Shopify availability
            InventoryReport.rebuild_inventory_report(instance=self, include_in_stock=True)

            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": "Inventory Synced",
                    "message": f"Updated {updated} products from Shopify",
                    "type": "success",
                },
            }
        finally:
            self.env.cr.execute("SELECT pg_advisory_unlock(%s)", [lock_key])

    def action_inventory_report(self):
        """Rebuild and open the inventory report for this instance."""
        self.ensure_one()
        threshold = 0.0
        self.env["shopify.inventory.report"].sudo().rebuild_inventory_report(
            instance=self, threshold=threshold, include_in_stock=True
        )
        action = self.env.ref(
            "sdlc_shopify_connector.action_shopify_inventory_report"
        ).read()[0]
        action["domain"] = [("instance_id", "=", self.id)]
        ctx = action.get("context") or {}
        if not isinstance(ctx, dict):
            ctx = {}
        ctx.update({"default_instance_id": self.id})
        action["context"] = ctx
        return action

    # ==================================================
    # CRON : AUTO SYNC
    # ==================================================
    def cron_auto_sync(self):
        now = fields.Datetime.now()
        instances = self.search([("active", "=", True)])

        for instance in instances:
            if instance.auto_product_sync and instance._is_time_to_sync(
                instance.last_product_sync_at,
                instance.auto_product_interval_type,
                instance.auto_product_interval,
            ):
                instance._sync_products(mode="cron")
                instance.last_product_sync_at = now

            if instance.auto_customer_sync and instance._is_time_to_sync(
                instance.last_customer_sync_at,
                instance.auto_customer_interval_type,
                instance.auto_customer_interval,
            ):
                instance._sync_customers(mode="cron")
                instance.last_customer_sync_at = now

            if instance.auto_order_sync and instance._is_time_to_sync(
                instance.last_order_sync_at,
                instance.auto_order_interval_type,
                instance.auto_order_interval,
            ):
                instance._sync_orders(mode="cron")
                instance.last_order_sync_at = now

            if instance.auto_category_sync and instance._is_time_to_sync(
                instance.last_category_sync_at,
                instance.auto_category_interval_type,
                instance.auto_category_interval,
            ):
                instance._sync_categories(mode="cron")
                instance.last_category_sync_at = now

            if instance.auto_giftcard_sync and instance._is_time_to_sync(
                instance.last_giftcard_sync_at,
                instance.auto_giftcard_interval_type,
                instance.auto_giftcard_interval,
            ):
                instance._sync_gift_cards(mode="cron")
                instance.last_giftcard_sync_at = now

        return True
