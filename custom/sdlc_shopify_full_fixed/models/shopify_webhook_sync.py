from odoo import models, fields
import logging
import base64
import requests

_logger = logging.getLogger(__name__)


class ShopifyWebhookSync(models.AbstractModel):
    _name = "shopify.webhook.sync"
    _description = "Shopify Webhook Sync"

    # ==================================================
    # HELPER: INSTANCE FILTER
    # ==================================================
    def _with_instance(self, domain, instance):
        return domain + [("shopify_instance_id", "=", instance.id)] if instance else domain

    # ==================================================
    # HELPER: IMAGE DOWNLOAD
    # ==================================================
    def _get_image_base64(self, url):
        if not url:
            return False
        try:
            res = requests.get(url, timeout=15)
            if res.status_code == 200 and res.content:
                return base64.b64encode(res.content)  # bytes ok for Binary
        except Exception as e:
            _logger.info("Image download failed (%s): %s", url, e)
        return False

    # ==================================================
    # HELPER: GET BEST IMAGE URL FROM SHOPIFY PAYLOAD
    # ==================================================
    def _extract_image_url(self, data):
        """
        Shopify payload can be:
        - data["image"]["src"]
        - data["images"][0]["src"]
        - sometimes only images/update sends images list
        """
        # main image object
        if data.get("image") and isinstance(data["image"], dict) and data["image"].get("src"):
            return data["image"]["src"]

        # images list
        images = data.get("images") or []
        if isinstance(images, list) and images:
            first = images[0] or {}
            if isinstance(first, dict) and first.get("src"):
                return first["src"]

        return None

    # ==================================================
    # WEBHOOK REPORT LOGGER
    # ==================================================
    def _log_webhook(
        self, instance, record_type, shopify_id, name,
        status, source_action, error_message=False
    ):
        """Persist webhook activity into sync reports for visibility."""
        if not instance:
            _logger.info("Shopify webhook ignored because no active instance is configured.")
            return

        # Create a small webhook report with a single line.
        try:
            Report = self.env["shopify.sync.report"].sudo()
            Line = self.env["shopify.sync.report.line"].sudo()

            op = "create" if (source_action or "").endswith("create") else "update"

            report = Report.create({
                "instance_id": instance.id,
                "sync_type": record_type,
                "mode": "webhook",
                "operation": op,
                "reference": shopify_id or "",
                "start_time": fields.Datetime.now(),
                "end_time": fields.Datetime.now(),
                "total_records": 1,
                "success_count": 1 if status == "success" else 0,
                "error_count": 1 if status == "error" else 0,
            })

            Line.create({
                "report_id": report.id,
                "record_type": record_type,
                "source_action": source_action or "webhook",
                "shopify_id": shopify_id,
                "name": name,
                "status": status,
                "error_message": error_message or False,
            })
        except Exception as e:
            _logger.warning("Failed to log Shopify webhook: %s", e)

        _logger.info(
            "Shopify webhook processed | type=%s | id=%s | name=%s | status=%s",
            record_type,
            shopify_id,
            name,
            status,
        )

    # ==================================================
    # PRODUCT (ALL IMPORTANT FIELDS + IMAGE)
    # ==================================================
    def sync_product(self, data, instance=None, source_action=None):
        shopify_id = str(data.get("id") or "")
        variants = data.get("variants") or []
        v = variants[0] if variants else {}

        # Capture total available across variants/locations if present
        total_available = 0.0
        for var in variants:
            total_available += float(var.get("inventory_quantity") or 0.0)

        # ---------------- CATEGORY ----------------
        categ = False
        if data.get("product_type"):
            categ = self.env["product.category"].sudo().search(
                [("name", "=", data["product_type"])], limit=1
            )
            if not categ:
                categ = self.env["product.category"].sudo().create({"name": data["product_type"]})

        # ---------------- TAGS ----------------
        tag_ids = []
        tags_str = data.get("tags") or ""
        for tag in tags_str.split(","):
            tag = (tag or "").strip()
            if tag:
                tag_rec = self.env["product.tag"].sudo().search([("name", "=", tag)], limit=1)
                if not tag_rec:
                    tag_rec = self.env["product.tag"].sudo().create({"name": tag})
                tag_ids.append(tag_rec.id)

        # ---------------- IMAGE ----------------
        image_url = self._extract_image_url(data)
        _logger.info("Shopify image URL for %s = %s", shopify_id, image_url)

        image_1920 = self._get_image_base64(image_url)

        Product = self.env["product.template"].sudo()
        mapping_vals = Product.map_shopify_product_to_odoo(data, instance=instance) or {}

        variant_vals = mapping_vals.pop("_variant_vals", {}) if isinstance(mapping_vals, dict) else {}
        vals = mapping_vals.copy()
        # fallback assignments only if mapping didn't provide them
        vals.setdefault("name", data.get("title"))
        vals.setdefault("shopify_product_id", shopify_id)
        vals.setdefault("default_code", v.get("sku"))
        vals.setdefault("barcode", v.get("barcode"))
        vals.setdefault("list_price", float(v.get("price") or 0.0))
        vals.setdefault("weight", float(v.get("weight") or 0.0))
        vals.setdefault("description_sale", data.get("body_html"))
        vals.setdefault("active", (data.get("status") == "active"))
        # track Shopify availability on the template
        vals["shopify_available_qty"] = total_available
        if categ and not vals.get("categ_id"):
            vals["categ_id"] = categ.id
        if tag_ids and not vals.get("product_tag_ids"):
            vals["product_tag_ids"] = [(6, 0, tag_ids)]

        if image_1920:
            vals.setdefault("image_1920", image_1920)

        if instance:
            vals.setdefault("shopify_instance_id", instance.id)

        try:
            product = Product.search(
                self._with_instance([("shopify_product_id", "=", shopify_id)], instance),
                limit=1,
            )

            if product:
                product.write(vals)
                # Update inventory report for this product
                self.env["shopify.inventory.report"].sudo().rebuild_inventory_report(
                    instance=instance or product.shopify_instance_id,
                    threshold=0.0,
                    include_in_stock=False,
                )
            else:
                product = self.env["product.template"].sudo().create(vals)
                self.env["shopify.inventory.report"].sudo().rebuild_inventory_report(
                    instance=instance or product.shopify_instance_id,
                    threshold=0.0,
                    include_in_stock=False,
                )

            # Apply variant-level fields if provided by mapping (e.g., SKU)
            if variant_vals and product.product_variant_id:
                product.product_variant_id.write(variant_vals)

            self._log_webhook(instance, "product", shopify_id, product.name, "success", source_action)

        except Exception as e:
            self._log_webhook(instance, "product", shopify_id, vals.get("name"), "error", source_action, str(e))
            raise

    # ==================================================
    # CUSTOMER (FULL ADDRESS)
    # ==================================================
    def sync_customer(self, data, instance=None, source_action=None):
        shopify_id = str(data.get("id") or "")
        Partner = self.env["res.partner"].sudo()

        # Start with field-mapping aware values so custom mappings are honored.
        vals = Partner.map_shopify_customer_to_odoo(data, instance=instance) or {}

        # Ensure IDs and rank are present.
        vals["shopify_customer_id"] = shopify_id
        vals.setdefault("customer_rank", 1)
        if instance:
            vals.setdefault("shopify_instance_id", instance.id)

        try:
            partner = Partner.search(self._with_instance([("shopify_customer_id", "=", shopify_id)], instance), limit=1)
            if not partner:
                # Fallback for legacy records without instance linkage
                partner = Partner.search([("shopify_customer_id", "=", shopify_id)], limit=1)

            if partner:
                partner.write(vals)
            else:
                partner = Partner.create(vals)

            self._log_webhook(instance, "customer", shopify_id, partner.name, "success", source_action)

        except Exception as e:
            self._log_webhook(instance, "customer", shopify_id, vals.get("name"), "error", source_action, str(e))
            raise

    # ==================================================
    # ORDER (SAFE FIELDS)
    # ==================================================
    def sync_order(self, data, instance=None, source_action=None):
        shopify_id = str(data.get("id") or "")
        SaleOrder = self.env["sale.order"].sudo()

        try:
            # Use the detailed mapper so webhook orders include partners, lines, taxes, and totals.
            order = SaleOrder.map_shopify_order_to_odoo(data, instance=instance)
            name_for_log = order.name if order else data.get("name")
            self._log_webhook(instance, "order", shopify_id, name_for_log, "success", source_action)

        except Exception as e:
            name_for_log = data.get("name")
            self._log_webhook(instance, "order", shopify_id, name_for_log, "error", source_action, str(e))
            raise

    # ==================================================
    # CATEGORY (COLLECTION)
    # ==================================================
    def sync_category(self, data, instance=None, source_action=None):
        shopify_id = str(data.get("id") or "")

        vals = {
            "name": data.get("title"),
            "shopify_collection_id": shopify_id,
            "active": True,
        }

        if instance:
            vals["shopify_instance_id"] = instance.id

        try:
            category = self.env["product.category"].sudo().search(
                self._with_instance([("shopify_collection_id", "=", shopify_id)], instance),
                limit=1,
            )

            if category:
                category.write(vals)
            else:
                category = self.env["product.category"].sudo().create(vals)

            self._log_webhook(instance, "category", shopify_id, category.name, "success", source_action)

        except Exception as e:
            self._log_webhook(instance, "category", shopify_id, vals.get("name"), "error", source_action, str(e))
            raise
