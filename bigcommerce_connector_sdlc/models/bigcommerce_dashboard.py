# -*- coding: utf-8 -*-

import logging
from datetime import datetime, time, timedelta

from odoo import api, fields, models

from ..services.gemini_chat_service import GeminiChatService

_logger = logging.getLogger(__name__)


class BigCommerceDashboard(models.AbstractModel):
    """Backend data provider for the BigCommerce OWL dashboard."""

    _name = "bigcommerce.dashboard"
    _description = "BigCommerce Dashboard"
    _auto = False

    def _get_instances(self, instance_id=None):
        domain = [("company_id", "in", self.env.companies.ids)]
        if instance_id and str(instance_id).lower() != "all":
            try:
                instance_id_int = int(instance_id)
            except (TypeError, ValueError):
                return self.env["bigcommerce.connector"].browse()
            domain.append(("id", "=", instance_id_int))
        return self.env["bigcommerce.connector"].search(domain, order="name asc")

    def _max_datetime_as_string(self, records, field_name):
        values = [value for value in records.mapped(field_name) if value]
        return fields.Datetime.to_string(max(values)) if values else "Never"

    @api.model
    def get_instances(self):
        """Return active connector list for dashboard filter."""
        instances = self._get_instances(instance_id="all")
        return [
            {
                "id": instance.id,
                "name": instance.name,
                "company": instance.company_id.name,
                "active": bool(instance.active),
                "state": instance.state,
            }
            for instance in instances
        ]

    def _get_ai_instance(self, instance_id="all"):
        """Return connector instance used for AI call credentials and logging."""
        instances = self._get_instances(instance_id=instance_id)
        if not instances:
            return False
        if instance_id and str(instance_id).lower() != "all":
            return instances[:1]
        for instance in instances:
            instance_sudo = instance.sudo()
            if instance_sudo.ai_chat_enabled and (instance_sudo.gemini_api_key or "").strip():
                return instance
        return instances[:1]

    def _format_orders_context(self, instance_ids, today_start_str):
        """Return lightweight today/recent order context from binding model."""
        binding_model = self.env["bigcommerce.order.binding"]
        domain = [("instance_id", "in", instance_ids)] if instance_ids else [("id", "=", 0)]
        today_orders = binding_model.search_count(domain + [("imported_at", ">=", today_start_str)])
        recent_orders = binding_model.search(
            domain,
            order="imported_at desc, id desc",
            limit=8,
        )

        order_items = []
        for rec in recent_orders:
            order_items.append(
                {
                    "remote_id": rec.bigcommerce_order_id,
                    "order_number": rec.bigcommerce_order_number,
                    "status": rec.status_on_bigcommerce or rec.sync_state,
                    "customer": rec.sale_order_id.partner_id.name if rec.sale_order_id else "",
                    "total": rec.sale_order_id.amount_total if rec.sale_order_id else 0.0,
                }
            )

        lines = []
        for item in order_items[:5]:
            lines.append(
                "- #%s | %s | %.2f"
                % (
                    item.get("order_number") or item.get("remote_id") or "N/A",
                    item.get("status") or "unknown",
                    item.get("total") or 0.0,
                )
            )
        return {
            "today_orders": today_orders,
            "recent_orders": order_items,
            "text": "\n".join(lines) if lines else "No recent imported orders found.",
        }

    def _format_low_stock_context(self, instance_ids):
        """Return low stock products from bound products (fallback to company products)."""
        try:
            binding_domain = [("instance_id", "in", instance_ids)] if instance_ids else [("id", "=", 0)]
            bindings = self.env["bigcommerce.product.binding"].search(binding_domain + [("product_id", "!=", False)])
            bound_product_ids = list(set(bindings.mapped("product_id").ids))

            product_domain = [("type", "=", "product"), ("active", "=", True)]
            company_ids = self.env.companies.ids
            if company_ids:
                product_domain.append(("company_id", "in", [False] + company_ids))

            if bound_product_ids:
                product_domain.append(("id", "in", bound_product_ids))
                candidate_products = self.env["product.product"].search(product_domain)
            else:
                # Safe fallback to avoid expensive full-catalog scanning.
                candidate_products = self.env["product.product"].search(product_domain, limit=1000)

            if not candidate_products:
                return {
                    "count": 0,
                    "items": [],
                    "text": "No stockable products found in scope.",
                }

            quant_domain = [
                ("product_id", "in", candidate_products.ids),
                ("location_id.usage", "=", "internal"),
            ]
            if company_ids:
                quant_domain.append(("company_id", "in", company_ids))

            quant_groups = self.env["stock.quant"].read_group(
                quant_domain,
                ["product_id", "quantity:sum", "reserved_quantity:sum"],
                ["product_id"],
                lazy=False,
            )
            qty_map = {}
            for group in quant_groups:
                product_info = group.get("product_id")
                if not product_info:
                    continue
                product_id = product_info[0]
                quantity = float(group.get("quantity", 0.0) or 0.0)
                reserved = float(group.get("reserved_quantity", 0.0) or 0.0)
                qty_map[product_id] = quantity - reserved

            low_rows = []
            for product in candidate_products:
                qty = float(qty_map.get(product.id, 0.0))
                if qty <= 5.0:
                    low_rows.append(
                        {
                            "id": product.id,
                            "name": product.display_name,
                            "sku": product.default_code or "",
                            "qty": qty,
                        }
                    )

            low_rows.sort(key=lambda row: (row["qty"], row["name"] or ""))
            items = low_rows[:10]
            lines = ["- %s (SKU: %s, Qty: %.2f)" % (i["name"], i["sku"] or "-", i["qty"]) for i in items]
            return {
                "count": len(low_rows),
                "items": items,
                "text": "\n".join(lines) if lines else "No low stock products (<= 5) in scope.",
            }
        except Exception as err:
            _logger.exception("Low stock context build failed")
            return {
                "count": 0,
                "items": [],
                "text": "Low stock data unavailable: %s" % err,
            }

    def _format_top_selling_context(self, instance_ids, thirty_days_from):
        """Return top selling products from related sale order lines."""
        binding_domain = [("instance_id", "in", instance_ids)] if instance_ids else [("id", "=", 0)]
        order_bindings = self.env["bigcommerce.order.binding"].search(binding_domain + [("sale_order_id", "!=", False)])
        sale_order_ids = list(set(order_bindings.mapped("sale_order_id").ids))
        if not sale_order_ids:
            return {"items": [], "text": "No synced sale orders available for top-selling products."}

        line_domain = [
            ("order_id", "in", sale_order_ids),
            ("order_id.state", "in", ("sale", "done")),
            ("order_id.date_order", ">=", thirty_days_from),
            ("display_type", "=", False),
            ("product_id", "!=", False),
        ]
        groups = self.env["sale.order.line"].read_group(
            line_domain,
            ["product_id", "product_uom_qty:sum"],
            ["product_id"],
            limit=8,
            orderby="product_uom_qty desc",
        )

        items = []
        product_ids = [group["product_id"][0] for group in groups if group.get("product_id")]
        product_map = {product.id: product for product in self.env["product.product"].browse(product_ids)}
        for group in groups:
            product_info = group.get("product_id")
            if not product_info:
                continue
            product_id = product_info[0]
            product = product_map.get(product_id)
            items.append(
                {
                    "product_id": product_id,
                    "name": product.display_name if product else product_info[1],
                    "sku": product.default_code if product else "",
                    "qty": float(group.get("product_uom_qty", 0.0)),
                }
            )

        lines = ["- %s (SKU: %s, Qty: %.2f)" % (i["name"], i["sku"] or "-", i["qty"]) for i in items]
        return {
            "items": items,
            "text": "\n".join(lines) if lines else "No top-selling products found in last 30 days.",
        }

    def _format_sync_context(self, instance_id):
        """Reuse dashboard metrics for sync health context."""
        data = self.get_dashboard_data(range_days="7", instance_id=instance_id)
        totals = data.get("totals", {})
        health = data.get("health", {})
        queue = data.get("queue", {})
        summary = {
            "success_rate": health.get("success_rate", 0.0),
            "failed_sync_count": totals.get("failed_sync_count", 0),
            "failed_webhook_count": totals.get("failed_webhook_count", 0),
            "pending_webhook_queue": totals.get("pending_webhook_queue", 0),
            "pending_export_queue": totals.get("pending_export_queue", 0),
            "instances_with_errors": health.get("instances_with_errors", 0),
            "stale_instances": health.get("stale_instances", 0),
            "last_product_sync": health.get("last_product_sync"),
            "last_inventory_sync": health.get("last_inventory_sync"),
            "last_order_sync": health.get("last_order_sync"),
            "queue": queue,
        }
        text = (
            "Success rate: %(success_rate)s%%\n"
            "Failed syncs: %(failed_sync_count)s\n"
            "Failed webhooks: %(failed_webhook_count)s\n"
            "Pending exports: %(pending_export_queue)s\n"
            "Pending webhooks: %(pending_webhook_queue)s\n"
            "Instances with errors: %(instances_with_errors)s\n"
            "Stale instances: %(stale_instances)s"
        ) % summary
        return {"summary": summary, "text": text}

    def _build_business_context(self, instance_id="all"):
        """Build concise business context block consumed by the AI assistant."""
        instances = self._get_instances(instance_id=instance_id)
        instance_ids = instances.ids
        now = fields.Datetime.now()
        today = fields.Date.context_today(self)
        today_start_str = fields.Datetime.to_string(datetime.combine(today, time.min))
        thirty_days_from = fields.Datetime.to_string(now - timedelta(days=30))

        orders_ctx = self._format_orders_context(instance_ids, today_start_str)
        low_stock_ctx = self._format_low_stock_context(instance_ids)
        top_selling_ctx = self._format_top_selling_context(instance_ids, thirty_days_from)
        sync_ctx = self._format_sync_context(instance_id=instance_id)

        summary = {
            "instances_in_scope": len(instances),
            "today_orders": orders_ctx.get("today_orders", 0),
            "recent_order_count": len(orders_ctx.get("recent_orders", [])),
            "low_stock_count": low_stock_ctx.get("count", 0),
            "top_selling_count": len(top_selling_ctx.get("items", [])),
            "sync_success_rate": sync_ctx.get("summary", {}).get("success_rate", 0.0),
            "failed_sync_count": sync_ctx.get("summary", {}).get("failed_sync_count", 0),
            "pending_webhook_queue": sync_ctx.get("summary", {}).get("pending_webhook_queue", 0),
        }

        block = [
            "Instance scope count: %s" % summary["instances_in_scope"],
            "Today's imported orders: %s" % summary["today_orders"],
            "Recent orders:\n%s" % orders_ctx.get("text"),
            "Low stock products:\n%s" % low_stock_ctx.get("text"),
            "Top selling products (30d):\n%s" % top_selling_ctx.get("text"),
            "Sync health:\n%s" % sync_ctx.get("text"),
        ]
        return {
            "summary": summary,
            "orders": orders_ctx,
            "low_stock": low_stock_ctx,
            "top_selling": top_selling_ctx,
            "sync": sync_ctx,
            "text": "\n\n".join(block),
        }

    def _get_quick_actions_map(self):
        """Predefined chat quick actions used by frontend chips."""
        return [
            {
                "key": "today_orders",
                "label": "Today's Orders",
                "prompt": "Summarize today's imported BigCommerce orders.",
            },
            {
                "key": "recent_orders",
                "label": "Recent Orders",
                "prompt": "Show recent BigCommerce orders with status and amount.",
            },
            {
                "key": "low_stock",
                "label": "Low Stock Products",
                "prompt": "List low stock products and highlight urgent items.",
            },
            {
                "key": "top_selling",
                "label": "Top Selling Products",
                "prompt": "Show top selling products based on synced orders.",
            },
            {
                "key": "sync_status",
                "label": "Sync Status",
                "prompt": "Summarize connector sync health, failures, and pending queues.",
            },
        ]

    @api.model
    def get_chat_quick_actions(self, instance_id=None):
        """Return quick action chips for dashboard assistant."""
        instances = self._get_instances(instance_id=instance_id or "all")
        enabled = any(
            bool(instance.sudo().ai_chat_enabled and (instance.sudo().gemini_api_key or "").strip())
            for instance in instances
        )
        return {
            "enabled": enabled,
            "actions": self._get_quick_actions_map(),
        }

    def _resolve_quick_action(self, message):
        """Resolve quick action key from message text when possible."""
        text = (message or "").strip().lower()
        if not text:
            return ""

        # Fuzzy intent routing for manual natural-language prompts.
        if "order" in text and "today" in text:
            return "today_orders"
        if "order" in text and "recent" in text:
            return "recent_orders"
        if "low stock" in text or "stock low" in text or "out of stock" in text:
            return "low_stock"
        if "top selling" in text or "best selling" in text:
            return "top_selling"
        if "sync" in text and ("status" in text or "health" in text or "failed" in text):
            return "sync_status"

        for action in self._get_quick_actions_map():
            if text in ((action.get("label") or "").strip().lower(), (action.get("prompt") or "").strip().lower()):
                return action.get("key")
        return ""

    def _normalize_user_message(self, message):
        """Normalize incoming chat message from string/object payloads."""
        if isinstance(message, dict):
            candidate = message.get("prompt") or message.get("message") or message.get("label") or ""
        else:
            candidate = message or ""
        return str(candidate).strip()

    def _build_quick_answer(self, action_key, context_data):
        """Build deterministic quick summary hints for known quick actions."""
        if action_key == "today_orders":
            return "Today's imported orders: %s." % context_data.get("summary", {}).get("today_orders", 0)
        if action_key == "recent_orders":
            orders = context_data.get("orders", {}).get("recent_orders", [])[:5]
            if not orders:
                return "No recent imported orders found."
            return "Recent orders: %s." % ", ".join(
                [
                    "#%s (%s, %.2f)"
                    % (
                        order.get("order_number") or order.get("remote_id") or "N/A",
                        order.get("status") or "unknown",
                        order.get("total") or 0.0,
                    )
                    for order in orders
                ]
            )
        if action_key == "low_stock":
            low_stock = context_data.get("low_stock", {})
            return "Low stock products (<=5): %s." % low_stock.get("count", 0)
        if action_key == "top_selling":
            top_items = context_data.get("top_selling", {}).get("items", [])[:3]
            if not top_items:
                return "No top-selling products found in the recent period."
            return "Top sellers: %s." % ", ".join(
                ["%s (%.2f)" % (item.get("name"), item.get("qty")) for item in top_items]
            )
        if action_key == "sync_status":
            sync_summary = context_data.get("sync", {}).get("summary", {})
            return (
                "Sync status: success rate %(success_rate)s%%, failed syncs %(failed_sync_count)s, "
                "pending webhooks %(pending_webhook_queue)s."
            ) % {
                "success_rate": sync_summary.get("success_rate", 0),
                "failed_sync_count": sync_summary.get("failed_sync_count", 0),
                "pending_webhook_queue": sync_summary.get("pending_webhook_queue", 0),
            }
        return ""

    @api.model
    def ask_ai_assistant(self, message, instance_id="all", history=None):
        """Handle AI assistant request with secure backend Gemini call."""
        try:
            user_message = self._normalize_user_message(message)
            if not user_message:
                return {"ok": False, "error": "Please enter a message."}

            context_data = self._build_business_context(instance_id=instance_id)
            quick_action_key = self._resolve_quick_action(user_message)
            quick_hint = self._build_quick_answer(quick_action_key, context_data) if quick_action_key else ""
            if quick_action_key and quick_hint:
                return {
                    "ok": True,
                    "answer": quick_hint,
                    "context_summary": context_data.get("summary", {}),
                }

            instance = self._get_ai_instance(instance_id=instance_id)
            if not instance:
                return {"ok": False, "error": "No BigCommerce instance found for the selected scope."}

            instance_sudo = instance.sudo()
            if not instance_sudo.ai_chat_enabled:
                return {"ok": False, "error": "AI Assistant is disabled for this connector instance."}

            api_key = (instance_sudo.gemini_api_key or "").strip()
            if not api_key:
                return {"ok": False, "error": "Gemini API key is not configured for this instance."}

            safe_history = []
            history_list = history if isinstance(history, list) else []
            for item in history_list[-10:]:
                if not isinstance(item, dict):
                    continue
                role = (item.get("role") or "").strip().lower()
                content = self._normalize_user_message(item.get("content"))
                if role not in ("user", "assistant") or not content:
                    continue
                safe_history.append({"role": role, "content": content[:2000]})

            system_prompt = (
                (instance_sudo.ai_system_prompt or "").strip()
                or "You are an Odoo BigCommerce assistant. Use provided context, keep answers concise, and avoid inventing data."
            )

            context_text = context_data.get("text", "")
            if quick_hint:
                context_text = "%s\n\nQuick action hint: %s" % (context_text, quick_hint)

            service = GeminiChatService()
            result = service.ask(
                api_key=api_key,
                model=(instance_sudo.gemini_model or "gemini-1.5-flash"),
                system_prompt=system_prompt,
                context_text=context_text,
                history=safe_history,
                user_message=user_message[:2000],
            )

            if not result.get("ok"):
                self.env["bigcommerce.sync.log"].sudo().create(
                    {
                        "instance_id": instance.id,
                        "operation_type": "manual_action",
                        "resource_type": "system",
                        "status": "failed",
                        "request_method": "POST",
                        "request_url": "gemini://generateContent",
                        "request_payload": user_message[:500],
                        "response_status": str(result.get("status_code") or ""),
                        "error_message": result.get("error"),
                        "note": "AI assistant request failed.",
                    }
                )
                return {"ok": False, "error": result.get("error")}

            self.env["bigcommerce.sync.log"].sudo().create(
                {
                    "instance_id": instance.id,
                    "operation_type": "manual_action",
                    "resource_type": "system",
                    "status": "success",
                    "request_method": "POST",
                    "request_url": "gemini://generateContent",
                    "request_payload": user_message[:500],
                    "response_status": str(result.get("status_code") or ""),
                    "note": "AI assistant request completed.",
                }
            )
            _logger.info("BigCommerce AI assistant request completed for instance_id=%s", instance.id)
            return {
                "ok": True,
                "answer": result.get("answer"),
                "context_summary": context_data.get("summary", {}),
            }
        except Exception as err:
            _logger.exception("BigCommerce AI assistant unexpected error")
            return {"ok": False, "error": "AI assistant failed: %s" % err}

    @api.model
    def get_dashboard_data(self, range_days="7", instance_id="all"):
        """Return aggregated dashboard metrics for BigCommerce."""
        days = int(range_days or 7)
        if days < 0:
            days = 0

        instances = self._get_instances(instance_id=instance_id)
        instance_ids = instances.ids
        now = fields.Datetime.now()
        today = fields.Date.context_today(self)
        if days <= 1:
            date_from = datetime.combine(today, time.min)
        else:
            date_from = datetime.combine(today - timedelta(days=days - 1), time.min)
        date_from_str = fields.Datetime.to_string(date_from)

        binding_domain = [("instance_id", "in", instance_ids)] if instance_ids else [("id", "=", 0)]
        product_binding_domain = binding_domain + [("bigcommerce_variant_id", "=", False)]
        log_domain = binding_domain + [("create_date", ">=", date_from_str)]
        webhook_domain = binding_domain + [("received_at", ">=", date_from_str)]
        webhook_subscription_domain = (
            [("instance_id", "in", instance_ids)] if instance_ids else [("id", "=", 0)]
        )

        export_ops = [
            "product_export",
            "inventory_export",
            "shipment_export",
            "customer_export",
            "category_export",
        ]
        sync_ops = [
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
        ]
        sync_log_domain = log_domain + [("operation_type", "in", sync_ops)]
        strict_synced_product = self.env["bigcommerce.product.binding"].search_count(
            product_binding_domain + [("sync_state", "=", "synced")]
        )
        strict_synced_category = self.env["bigcommerce.category.binding"].search_count(
            binding_domain + [("sync_state", "=", "synced")]
        )
        strict_synced_customer = self.env["bigcommerce.customer.binding"].search_count(
            binding_domain + [("sync_state", "=", "synced")]
        )
        strict_synced_order = self.env["bigcommerce.order.binding"].search_count(
            binding_domain + [("sync_state", "=", "synced")]
        )
        binding_error_total = (
            self.env["bigcommerce.product.binding"].search_count(product_binding_domain + [("sync_state", "=", "error")])
            + self.env["bigcommerce.category.binding"].search_count(binding_domain + [("sync_state", "=", "error")])
            + self.env["bigcommerce.customer.binding"].search_count(binding_domain + [("sync_state", "=", "error")])
            + self.env["bigcommerce.order.binding"].search_count(binding_domain + [("sync_state", "=", "error")])
        )
        failed_sync_count_logs = self.env["bigcommerce.sync.log"].search_count(
            sync_log_domain + [("status", "=", "failed")]
        )

        totals = {
            "active_instances": len(instances.filtered("active")),
            "products_synced": self.env["bigcommerce.product.binding"].search_count(
                product_binding_domain + ["|", ("sync_state", "=", "synced"), ("last_synced_at", "!=", False)]
            ),
            "categories_synced": self.env["bigcommerce.category.binding"].search_count(
                binding_domain + ["|", ("sync_state", "=", "synced"), ("last_synced_at", "!=", False)]
            ),
            "customers_synced": self.env["bigcommerce.customer.binding"].search_count(
                binding_domain + ["|", ("sync_state", "=", "synced"), ("last_synced_at", "!=", False)]
            ),
            "orders_synced": self.env["bigcommerce.order.binding"].search_count(
                binding_domain + ["|", ("sync_state", "=", "synced"), ("imported_at", "!=", False)]
            ),
            "pending_export_queue": self.env["bigcommerce.sync.log"].search_count(
                binding_domain + [("status", "=", "draft"), ("operation_type", "in", export_ops)]
            ),
            "pending_webhook_queue": self.env["bigcommerce.webhook.event"].search_count(
                binding_domain + [("status", "in", ("pending", "processing"))]
            ),
            "webhook_subscriptions_active": self.env["bigcommerce.webhook.subscription"].search_count(
                webhook_subscription_domain + [("status", "=", "active"), ("is_active", "=", True)]
            ),
            "webhook_subscriptions_error": self.env["bigcommerce.webhook.subscription"].search_count(
                webhook_subscription_domain + [("status", "in", ("missing", "error"))]
            ),
            "webhook_events_received": self.env["bigcommerce.webhook.event"].search_count(webhook_domain),
            "webhook_events_done": self.env["bigcommerce.webhook.event"].search_count(
                webhook_domain + [("status", "=", "done")]
            ),
            "failed_sync_count": max(failed_sync_count_logs, binding_error_total),
            "failed_webhook_count": self.env["bigcommerce.webhook.event"].search_count(
                webhook_domain + [("status", "=", "failed")]
            ),
            "failed_export_count": self.env["bigcommerce.sync.log"].search_count(
                log_domain + [("status", "=", "failed"), ("operation_type", "in", export_ops)]
            ),
        }

        ops_domain = binding_domain + [("create_date", ">=", date_from_str)]
        operational = {
            "products_created": self.env["bigcommerce.product.binding"].search_count(
                product_binding_domain + [("create_date", ">=", date_from_str)]
            ),
            "products_updated": self.env["bigcommerce.product.binding"].search_count(
                product_binding_domain + [("last_synced_at", ">=", date_from_str)]
            ),
            "orders_imported": self.env["bigcommerce.order.binding"].search_count(
                binding_domain + [("imported_at", ">=", date_from_str)]
            ),
            "inventory_updates": self.env["bigcommerce.sync.log"].search_count(
                ops_domain + [("operation_type", "in", ("inventory_import", "inventory_export")), ("status", "=", "success")]
            ),
            "webhooks_processed": self.env["bigcommerce.webhook.event"].search_count(
                webhook_domain + [("status", "=", "done")]
            ),
            "failed_jobs": self.env["bigcommerce.sync.log"].search_count(
                ops_domain + [("status", "=", "failed"), ("operation_type", "in", sync_ops)]
            ),
        }

        queue = {
            "export_draft": self.env["bigcommerce.sync.log"].search_count(
                binding_domain + [("status", "=", "draft"), ("operation_type", "in", export_ops)]
            ),
            "export_failed": self.env["bigcommerce.sync.log"].search_count(
                binding_domain + [("status", "=", "failed"), ("operation_type", "in", export_ops)]
            ),
            "webhook_pending": self.env["bigcommerce.webhook.event"].search_count(
                binding_domain + [("status", "=", "pending")]
            ),
            "webhook_processing": self.env["bigcommerce.webhook.event"].search_count(
                binding_domain + [("status", "=", "processing")]
            ),
            "webhook_done": self.env["bigcommerce.webhook.event"].search_count(
                binding_domain + [("status", "=", "done")]
            ),
            "webhook_failed": self.env["bigcommerce.webhook.event"].search_count(
                binding_domain + [("status", "=", "failed")]
            ),
            "webhook_subscription_error": self.env["bigcommerce.webhook.subscription"].search_count(
                webhook_subscription_domain + [("status", "in", ("missing", "error"))]
            ),
        }

        recent_logs = self.env["bigcommerce.sync.log"].search(
            binding_domain,
            order="create_date desc, id desc",
            limit=8,
        )
        recent_activity = [
            {
                "time": fields.Datetime.to_string(log.create_date) if log.create_date else "",
                "instance": log.instance_id.name,
                "operation": log.operation_type,
                "status": log.status,
                "message": log.error_message or log.note or "",
            }
            for log in recent_logs
        ]

        sync_groups = self.env["bigcommerce.sync.log"].read_group(
            sync_log_domain + [("status", "in", ("success", "failed"))],
            ["status"],
            ["status"],
        )
        success_count = sum(group.get("status_count", 0) for group in sync_groups if group.get("status") == "success")
        failed_count = sum(group.get("status_count", 0) for group in sync_groups if group.get("status") == "failed")
        total_done = success_count + failed_count
        if total_done:
            success_rate = round((success_count * 100.0 / total_done), 1)
        else:
            binding_synced_total = (
                strict_synced_product
                + strict_synced_category
                + strict_synced_customer
                + strict_synced_order
            )
            binding_done = binding_synced_total + binding_error_total
            success_rate = round((binding_synced_total * 100.0 / binding_done), 1) if binding_done else 0.0

        stale_threshold = now - timedelta(hours=24)
        stale_instances = 0
        instance_rows = []
        for instance in instances:
            sync_dates = [
                instance.last_product_sync_at,
                instance.last_category_sync_at,
                instance.last_customer_sync_at,
                instance.last_order_sync_at,
                instance.last_inventory_export_at,
                instance.last_webhook_process_at,
            ]
            sync_dates = [value for value in sync_dates if value]
            last_sync = max(sync_dates) if sync_dates else False
            if instance.active and ((not last_sync) or (last_sync < stale_threshold)):
                stale_instances += 1

            failed_recent = self.env["bigcommerce.sync.log"].search_count(
                [
                    ("instance_id", "=", instance.id),
                    ("status", "=", "failed"),
                    ("create_date", ">=", date_from_str),
                    ("operation_type", "in", sync_ops),
                ]
            )
            if not failed_recent:
                failed_recent = (
                    self.env["bigcommerce.product.binding"].search_count(
                        [("instance_id", "=", instance.id), ("sync_state", "=", "error")]
                    )
                    + self.env["bigcommerce.category.binding"].search_count(
                        [("instance_id", "=", instance.id), ("sync_state", "=", "error")]
                    )
                    + self.env["bigcommerce.customer.binding"].search_count(
                        [("instance_id", "=", instance.id), ("sync_state", "=", "error")]
                    )
                    + self.env["bigcommerce.order.binding"].search_count(
                        [("instance_id", "=", instance.id), ("sync_state", "=", "error")]
                    )
                )
            pending_webhooks = self.env["bigcommerce.webhook.event"].search_count(
                [
                    ("instance_id", "=", instance.id),
                    ("status", "in", ("pending", "processing")),
                ]
            )
            health = "healthy"
            if failed_recent >= 10 or pending_webhooks >= 20 or instance.state == "error":
                health = "critical"
            elif failed_recent > 0 or pending_webhooks > 0:
                health = "warning"

            instance_rows.append(
                {
                    "id": instance.id,
                    "name": instance.name,
                    "company": instance.company_id.name,
                    "active": bool(instance.active),
                    "auto_sync": bool(
                        instance.auto_product_sync
                        or instance.auto_order_sync
                        or instance.auto_sync_customers
                        or instance.auto_inventory_export
                        or instance.auto_shipment_export
                    ),
                    "last_sync": fields.Datetime.to_string(last_sync) if last_sync else "Never",
                    "health": health,
                    "pending": pending_webhooks,
                    "failed": failed_recent,
                }
            )

        last_webhook_event = self.env["bigcommerce.webhook.event"].search(
            binding_domain,
            order="received_at desc,id desc",
            limit=1,
        )
        health = {
            "last_product_sync": self._max_datetime_as_string(instances, "last_product_sync_at"),
            "last_inventory_sync": self._max_datetime_as_string(instances, "last_inventory_export_at"),
            "last_order_sync": self._max_datetime_as_string(instances, "last_order_sync_at"),
            "last_webhook_sync": self._max_datetime_as_string(instances, "webhook_last_sync_at"),
            "last_webhook_process": self._max_datetime_as_string(instances, "last_webhook_process_at"),
            "last_webhook_event": (
                fields.Datetime.to_string(last_webhook_event.received_at) if last_webhook_event else "Never"
            ),
            "success_rate": success_rate,
            "instances_with_errors": len(instances.filtered(lambda i: i.state == "error")),
            "stale_instances": stale_instances,
        }

        day_groups = self.env["bigcommerce.sync.log"].read_group(
            sync_log_domain + [("status", "in", ("success", "failed"))],
            ["status"],
            ["create_date:day", "status"],
            orderby="create_date:day asc",
        )
        trend_map = {}
        for group in day_groups:
            date_key = group.get("create_date:day")
            if not date_key:
                continue
            bucket = trend_map.setdefault(
                date_key,
                {"date": str(date_key), "success": 0, "failed": 0},
            )
            if group.get("status") == "success":
                bucket["success"] = group.get("status_count", 0)
            if group.get("status") == "failed":
                bucket["failed"] = group.get("status_count", 0)

        trends = sorted(trend_map.values(), key=lambda row: row["date"])

        return {
            "filters": {
                "range_days": str(days),
                "instance_id": str(instance_id or "all"),
            },
            "totals": totals,
            "health": health,
            "operational": operational,
            "queue": queue,
            "instances": instance_rows,
            "recent_activity": recent_activity,
            "trends": trends,
        }
