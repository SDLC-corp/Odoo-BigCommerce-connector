/** @odoo-module **/

import { Component, onMounted, onWillStart, onWillUnmount, useState } from "@odoo/owl";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";

export class ShopifyDashboard extends Component {
    setup() {
        this.orm = useService("orm");
        this.action = useService("action");
        this.state = useState({
            data: {},
            charts: {
                instanceBars: [],
                statusShare: { delivered: 0, pending: 0, cancelled: 0 },
                paymentShare: { cod: 0, online: 0 },
                trendPoints: [],
            },
            loading: true,
            error: null,
            filters: {
                dateRange: "30d",
                instanceId: null,
                dateCheck: "",
                dateFrom: "",
                dateTo: "",
                showCustomDates: false,
            },
            instances: [],
        });

        // ensure handler keeps component context
        this.openOrder = this.openOrder.bind(this);

        onWillStart(async () => {
            await this.loadInstances();
            await this.fetchData();
        });

        onMounted(() => {
            this.intervalId = setInterval(() => this.fetchData(true), 60000);
        });

        onWillUnmount(() => {
            if (this.intervalId) {
                clearInterval(this.intervalId);
            }
        });
    }

    async loadInstances() {
        const records = await this.orm.searchRead("shopify.instance", [], ["name"]);
        this.state.instances = records || [];
    }

    async fetchData(isAuto = false) {
        if (!isAuto) {
            this.state.loading = true;
        }
        this.state.error = null;
        try {
            const result = await this.orm.call(
                "shopify.dashboard",
                "get_dashboard_data",
                [],
                {
                    date_range: this.state.filters.dateRange,
                    instance_id: this.state.filters.instanceId || false,
                    date_check: this.state.filters.dateCheck || false,
                    date_from: this.state.filters.dateFrom || false,
                    date_to: this.state.filters.dateTo || false,
                }
            );
            this.state.data = result || {};
            this.state.charts = this.computeCharts(this.state.data);
        } catch (e) {
            this.state.error = e && e.message ? e.message : "Failed to load dashboard";
        } finally {
            this.state.loading = false;
        }
    }

    onDateRangeChange(ev) {
        this.state.filters.dateRange = ev.target.value;
        this.state.filters.showCustomDates = ev.target.value === "custom";
        this.fetchData();
    }

    onInstanceChange(ev) {
        const val = ev.target.value;
        this.state.filters.instanceId = val ? parseInt(val) : null;
        this.fetchData();
    }

    onDateCheckChange(ev) {
        this.state.filters.dateCheck = ev.target.value;
        this.fetchData();
    }

    onDateFromChange(ev) {
        this.state.filters.dateFrom = ev.target.value;
        this.state.filters.dateRange = "custom";
        this.state.filters.showCustomDates = true;
        this.fetchData();
    }

    onDateToChange(ev) {
        this.state.filters.dateTo = ev.target.value;
        this.state.filters.dateRange = "custom";
        this.state.filters.showCustomDates = true;
        this.fetchData();
    }

    openOrder(orderId) {
        if (!orderId || !this.action) {
            return;
        }
        this.action.doAction({
            type: "ir.actions.act_window",
            res_model: "sale.order",
            res_id: orderId,
            views: [[false, "form"]],
            target: "current",
        });
    }

    manualRefresh() {
        this.fetchData();
    }

    computeCharts(data) {
        const ordersByInstance = data.orders_by_instance || [];
        const rangeStatus = data.range_status || {};
        const payments = data.payments || {};
        const trend = (data.sync && data.sync.trend) || [];

        const maxOrders = Math.max(
            1,
            ...ordersByInstance.map((o) => (typeof o.orders === "number" ? o.orders : 0))
        );
        const instanceBars = ordersByInstance.map((inst) => {
            const orders = inst.orders || 0;
            const pending = inst.pending || 0;
            const cancelled = inst.cancelled || 0;
            const delivered = Math.max(orders - pending - cancelled, 0);
            const base = Math.max(orders, 1);
            return {
                ...inst,
                deliveredHeight: Math.round((delivered / maxOrders) * 100),
                pendingHeight: Math.round((pending / maxOrders) * 100),
                cancelledHeight: Math.round((cancelled / maxOrders) * 100),
                deliveredShare: Math.round((delivered / base) * 100),
                pendingShare: Math.round((pending / base) * 100),
                cancelledShare: Math.round((cancelled / base) * 100),
            };
        });

        const statusTotal =
            (rangeStatus.delivered || 0) + (rangeStatus.pending || 0) + (rangeStatus.cancelled || 0);
        const statusShare = {
            delivered: this._percentage(rangeStatus.delivered, statusTotal),
            pending: this._percentage(rangeStatus.pending, statusTotal),
            cancelled: this._percentage(rangeStatus.cancelled, statusTotal),
        };

        const paymentTotal = (payments.cod || 0) + (payments.online || 0);
        const paymentShare = {
            cod: this._percentage(payments.cod, paymentTotal),
            online: this._percentage(payments.online, paymentTotal),
        };

        const maxTrend = Math.max(
            1,
            ...trend.map((t) =>
                Math.max(t.total || 0, t.success || 0, t.error || 0, t.success + t.error || 0)
            )
        );
        const trendPoints = trend.map((t) => ({
            label: t.date,
            successHeight: Math.round(((t.success || 0) / maxTrend) * 100),
            errorHeight: Math.round(((t.error || 0) / maxTrend) * 100),
            totalHeight: Math.round(((t.total || 0) / maxTrend) * 100),
            success: t.success || 0,
            error: t.error || 0,
            total: t.total || 0,
        }));

        return {
            instanceBars,
            statusShare,
            paymentShare,
            trendPoints,
        };
    }

    _percentage(value, total) {
        const safeTotal = total || 0;
        if (!safeTotal) {
            return 0;
        }
        return Math.round((value / safeTotal) * 100);
    }
}

ShopifyDashboard.template = "shopify_dashboard_template";

registry.category("actions").add("shopify_dashboard", ShopifyDashboard);
