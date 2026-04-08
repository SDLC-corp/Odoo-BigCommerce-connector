from datetime import timedelta, datetime

from odoo import api, fields, models


class ShopifyDashboard(models.AbstractModel):
    _name = "shopify.dashboard"
    _description = "Shopify Dashboard Data"

    @api.model
    def get_dashboard_data(
        self,
        date_range="30d",
        instance_id=None,
        date_check=None,
        date_from=None,
        date_to=None,
    ):
        SaleOrder = self.env["sale.order"]
        ProductTemplate = self.env["product.template"]
        ProductVariant = self.env["product.product"]
        Category = self.env["product.category"]
        Partner = self.env["res.partner"]
        Instance = self.env["shopify.instance"]
        GiftCard = self.env["shopify.gift.card"]
        SyncReport = self.env["shopify.sync.report"]
        SyncReportLine = self.env["shopify.sync.report.line"]

        current_instance_name = False
        if instance_id:
            inst = Instance.browse(instance_id)
            if inst.exists():
                current_instance_name = inst.name

        def _sum_sales(domain):
            res = SaleOrder.read_group(domain, ["amount_total:sum"], [])
            return res[0]["amount_total_sum"] if res and res[0].get("amount_total_sum") else 0.0

        def _instance_domain(model_field):
            return [(model_field, "=", instance_id)] if instance_id else []

        now = fields.Datetime.now()
        window_label_map = {
            "today": "Today",
            "yesterday": "Yesterday",
            "7d": "Last 7 days",
            "30d": "Last 30 days",
            "90d": "Last 90 days",
            "custom": "Custom",
        }
        window_label = window_label_map.get(date_range, date_range)
        window_days = {
            "today": 0,
            "yesterday": -1,  # special handling below
            "7d": 7,
            "30d": 30,
            "90d": 90,
        }.get(date_range, 30)

        if date_range == "custom" and date_from and date_to:
            date_from = fields.Datetime.from_string(date_from)
            date_to = fields.Datetime.from_string(date_to)
        elif date_range == "yesterday":
            base = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            date_from = base
            date_to = base + timedelta(days=1)
        elif window_days == 0:
            date_from = now.replace(hour=0, minute=0, second=0, microsecond=0)
            date_to = now
        else:
            date_from = now - timedelta(days=window_days)
            date_to = now

        if date_range == "custom" and date_from and date_to:
            try:
                window_label = "%s → %s" % (
                    fields.Date.to_string(date_from.date()),
                    fields.Date.to_string(date_to.date()),
                )
            except Exception:
                window_label = window_label_map.get(date_range, date_range)

        order_domain = [
            ("date_order", ">=", date_from),
            ("date_order", "<=", date_to),
        ]
        report_domain = [
            ("start_time", ">=", date_from),
            ("start_time", "<=", date_to),
        ]
        if instance_id:
            report_domain.append(("instance_id", "=", instance_id))
            order_domain.append(("shopify_instance_id", "=", instance_id))

        # Orders and sales
        orders = SaleOrder.search(order_domain)
        daily_sales = sum(orders.mapped("amount_total"))
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_orders = orders.filtered(lambda o: o.date_order and o.date_order >= today_start)
        today_products_sold = sum(today_orders.mapped("order_line").mapped("product_uom_qty"))
        pending_orders = SaleOrder.search_count(
            [
                ("state", "in", ["sale"]),
                ("invoice_status", "!=", "invoiced"),
            ]
            + order_domain
        )
        failed_orders = SaleOrder.search_count([("state", "=", "cancel")] + order_domain)

        # Entity counts
        total_instances = Instance.search_count([]) if not instance_id else 1
        total_products = ProductTemplate.search_count(_instance_domain("shopify_instance_id"))
        total_variants = ProductVariant.search_count(_instance_domain("shopify_instance_id"))
        total_categories = Category.search_count(_instance_domain("shopify_instance_id"))
        total_customers = Partner.search_count(_instance_domain("shopify_instance_id"))

        # Gift cards
        giftcard_domain = _instance_domain("instance_id")
        total_gift_cards = GiftCard.search_count(giftcard_domain)
        all_gift_cards = GiftCard.search(giftcard_domain)
        used_gift_cards = len(
            all_gift_cards.filtered(lambda gc: gc.value and gc.balance < gc.value)
        )
        pending_gift_cards = GiftCard.search_count(
            [("active", "=", True), ("balance", ">", 0)] + giftcard_domain
        )
        failed_gift_cards = 0  # state may not exist on older databases
        today_date = fields.Date.context_today(self)
        expired_gift_cards = GiftCard.search_count(
            [("expiry_date", "!=", False), ("expiry_date", "<", today_date)] + giftcard_domain
        )
        no_balance_gift_cards = GiftCard.search_count([("balance", "=", 0)] + giftcard_domain)
        gift_card_balance = sum(GiftCard.search(giftcard_domain).mapped("balance"))
        gift_card_revenue = sum(GiftCard.search(giftcard_domain).mapped("value"))

        # Payment breakdown within window
        cod_orders = 0
        online_orders = 0
        pending_amount = 0.0

        if "payment_transaction_id" in SaleOrder._fields:
            for order in orders:
                tx = order.payment_transaction_id
                provider = False
                if tx:
                    provider = getattr(tx, "provider", False) or getattr(
                        getattr(tx, "acquirer_id", False), "provider", False
                    )
                if provider and ("cash" in provider.lower() or "cod" in provider.lower()):
                    cod_orders += 1
                elif provider:
                    online_orders += 1

                if "invoice_ids" in order._fields:
                    for inv in order.invoice_ids:
                        if inv.state == "posted" and inv.payment_state not in ("paid", "in_payment"):
                            pending_amount += inv.amount_residual
        else:
            for order in orders:
                if order.invoice_status != "invoiced":
                    pending_amount += order.amount_total

        def _delivery_status(order):
            if "picking_ids" not in order._fields:
                return "unknown"
            pickings = order.picking_ids.filtered(lambda p: p.state == "done")
            return "delivered" if pickings else "pending"

        delivered_orders = len(orders.filtered(lambda o: _delivery_status(o) == "delivered"))
        cancelled_orders = len(orders.filtered(lambda o: o.state == "cancel"))

        # Per-instance order summary (always scoped to Shopify orders per instance)
        orders_by_instance = []
        for inst in Instance.search([]):
            inst_orders = SaleOrder.search(order_domain + [("shopify_instance_id", "=", inst.id)])
            orders_by_instance.append(
                {
                    "id": inst.id,
                    "name": inst.name,
                    "orders": len(inst_orders),
                    "sales": round(sum(inst_orders.mapped("amount_total")), 2),
                    "pending": inst_orders.filtered(
                        lambda so: so.state in ["sale"] and so.invoice_status != "invoiced"
                    ).__len__(),
                    "cancelled": inst_orders.filtered(lambda so: so.state == "cancel").__len__(),
                }
            )

        # Earnings snapshots
        earnings_7d = _sum_sales(
            [
                ("date_order", ">=", now - timedelta(days=7)),
                ("date_order", "<=", now),
            ]
        )
        earnings_30d = _sum_sales(
            [
                ("date_order", ">=", now - timedelta(days=30)),
                ("date_order", "<=", now),
            ]
        )
        total_earnings = _sum_sales([])

        # Recent orders (from window, latest first)
        recent_orders = []
        latest_orders = SaleOrder.search(order_domain, order="date_order desc")
        for o in latest_orders:
            recent_orders.append(
                {
                    "name": o.name,
                    "customer": o.partner_id.name or "",
                    "amount": round(o.amount_total, 2),
                    "instance": o.shopify_instance_id.name or "",
                    "state": o.state,
                    "invoice_status": o.invoice_status,
                    "delivery_status": _delivery_status(o),
                    "date": o.date_order,
                    "id": o.id,
                }
            )

        # Checked date sales (if provided)
        checked_range_result = {}
        if date_from and date_to:
            try:
                range_orders = SaleOrder.search(
                    [
                        ("date_order", ">=", date_from),
                        ("date_order", "<=", date_to),
                    ]
                )
                range_sales = sum(range_orders.mapped("amount_total"))
                pending_count = range_orders.filtered(
                    lambda so: so.state in ["sale"] and so.invoice_status != "invoiced"
                ).__len__()
                paid_count = range_orders.filtered(lambda so: so.invoice_status == "invoiced").__len__()
                delivered_count = range_orders.filtered(
                    lambda so: _delivery_status(so) == "delivered"
                ).__len__()
                checked_range_result = {
                    "date_from": fields.Datetime.to_string(date_from),
                    "date_to": fields.Datetime.to_string(date_to),
                    "orders": len(range_orders),
                    "sales": round(range_sales, 2),
                    "pending": pending_count,
                    "paid": paid_count,
                    "delivered": delivered_count,
                }
            except Exception:
                checked_range_result = {
                    "date_from": date_from,
                    "date_to": date_to,
                    "orders": 0,
                    "sales": 0,
                    "pending": 0,
                    "paid": 0,
                    "delivered": 0,
                }

        # Sync reports aggregated by type
        by_type = {}
        grouped_types = SyncReport.read_group(
            report_domain,
            ["success_count", "error_count", "total_records"],
            ["sync_type"],
        )
        for g in grouped_types:
            sync_type = g.get("sync_type") or "unknown"
            last_report = SyncReport.search(
                report_domain + [("sync_type", "=", sync_type)],
                order="start_time desc",
                limit=1,
            )
            by_type[sync_type] = {
                "success": int(g.get("success_count") or 0),
                "error": int(g.get("error_count") or 0),
                "total": int(g.get("total_records") or 0),
                "last_sync": last_report.start_time if last_report else False,
                "instance": last_report.instance_id.name if last_report else False,
            }

        # Sync trend by day
        trend = []
        grouped_days = SyncReport.read_group(
            report_domain,
            ["success_count", "error_count", "total_records"],
            ["start_time:day"],
            orderby="start_time:day",
        )
        for g in grouped_days:
            day_val = g.get("start_time:day")
            if isinstance(day_val, str):
                day_str = day_val
            else:
                day_str = fields.Date.to_string(day_val) if day_val else False

            trend.append(
                {
                    "date": day_str,
                    "success": int(g.get("success_count") or 0),
                    "error": int(g.get("error_count") or 0),
                    "total": int(g.get("total_records") or 0),
                }
            )

        # Recent errors
        recent_errors = []
        error_domain = [("status", "=", "error")]
        if instance_id:
            error_domain.append(("report_id.instance_id", "=", instance_id))
        for line in SyncReportLine.search(error_domain, order="create_date desc", limit=5):
            recent_errors.append(
                {
                    "record_type": line.record_type,
                    "shopify_id": line.shopify_id,
                    "message": line.error_message,
                    "at": line.create_date,
                    "instance": line.report_id.instance_id.name if line.report_id else False,
                }
            )

        # Instances detail
        instances_detail = []
        for inst in Instance.search([]):
            webhook_product = bool(
                getattr(inst, "webhook_product_create", False)
                or getattr(inst, "webhook_product_update", False)
            )
            webhook_customer = bool(
                getattr(inst, "webhook_customer_create", False)
                or getattr(inst, "webhook_customer_update", False)
            )
            webhook_order = bool(
                getattr(inst, "webhook_order_create", False)
                or getattr(inst, "webhook_order_update", False)
            )
            webhook_category = bool(
                getattr(inst, "webhook_category_create", False)
                or getattr(inst, "webhook_category_update", False)
            )
            webhook_giftcard = bool(
                getattr(inst, "webhook_giftcard_create", False)
                or getattr(inst, "webhook_giftcard_update", False)
            )
            instances_detail.append(
                {
                    "name": inst.name,
                    "active": inst.active,
                    "webhooks": {
                        "product": webhook_product,
                        "customer": webhook_customer,
                        "order": webhook_order,
                        "category": webhook_category,
                        "giftcard": webhook_giftcard,
                    },
                    "last_sync": {
                        "product": inst.last_product_sync_at,
                        "customer": inst.last_customer_sync_at,
                        "order": inst.last_order_sync_at,
                        "category": inst.last_category_sync_at,
                        "giftcard": inst.last_giftcard_sync_at,
                    },
                }
            )

        return {
            "instances": total_instances,
            "customers": total_customers,
            "products": total_products,
            "total_categories": total_categories,
            "total_variants": total_variants,
            "today_orders": len(today_orders),
            "today_products_sold": int(today_products_sold),
            "pending_orders": pending_orders,
            "failed_orders": failed_orders,
            "range_status": {
                "delivered": delivered_orders,
                "pending": pending_orders,
                "cancelled": cancelled_orders,
            },
            "daily_sales": round(daily_sales, 2),
            "window_sales": round(daily_sales, 2),
            "earnings_7d": round(earnings_7d, 2),
            "earnings_30d": round(earnings_30d, 2),
            "total_earnings": round(total_earnings, 2),
            "weekly_sales": 0,  # kept for compatibility; not computed in ranged view
            "monthly_sales": 0,  # kept for compatibility; not computed in ranged view
            "total_gift_cards": total_gift_cards,
            "used_gift_cards": used_gift_cards,
            "pending_gift_cards": pending_gift_cards,
            "failed_gift_cards": failed_gift_cards,
            "expired_gift_cards": expired_gift_cards,
            "no_balance_gift_cards": no_balance_gift_cards,
            "gift_card_balance": round(gift_card_balance, 2),
            "gift_card_revenue": round(gift_card_revenue, 2),
            "payments": {
                "cod": cod_orders,
                "online": online_orders,
                "pending_amount": round(pending_amount, 2),
            },
            "orders_by_instance": orders_by_instance,
            "sync": {
                "trend": trend,
                "recent_errors": recent_errors,
            },
            "instances_detail": instances_detail,
            "checked_range": checked_range_result,
            "recent_orders": recent_orders,
            "window": {
                "from": date_from,
                "to": date_to,
                "label": window_label,
                "key": date_range,
            },
            "current_instance": {
                "id": instance_id,
                "name": current_instance_name or ("All Instances" if not instance_id else False),
            },
        }
