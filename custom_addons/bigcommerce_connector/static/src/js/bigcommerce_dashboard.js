import { registry } from "@web/core/registry";
import { Component, onMounted, onWillUnmount, useState } from "@odoo/owl";
import { rpc } from "@web/core/network/rpc";

export class BigCommerceDashboard extends Component {
    setup() {
        this._dashboardRequestSeq = 0;
        this._quickActionRequestSeq = 0;
        this._autoRefreshTimer = null;
        this.state = useState({
            loading: false,
            rangeDays: "7",
            instanceId: "all",
            instances: [],
            data: this._emptyData(),
            isChatOpen: false,
            chatInput: "",
            sending: false,
            messages: [],
            quickActions: [],
            chatEnabled: false,
        });

        onMounted(async () => {
            await this.loadInstances();
            await this.loadData();
            await this.loadQuickActions();
            this._startAutoRefresh();
        });

        onWillUnmount(() => {
            this._stopAutoRefresh();
        });
    }

    _emptyData() {
        return {
            totals: {},
            health: {},
            operational: {},
            queue: {},
            instances: [],
            recent_activity: [],
            trends: [],
        };
    }

    async loadInstances() {
        const result = await rpc("/web/dataset/call_kw", {
            model: "bigcommerce.dashboard",
            method: "get_instances",
            args: [],
            kwargs: {},
        });
        this.state.instances = result || [];
    }

    async loadData() {
        const requestSeq = ++this._dashboardRequestSeq;
        this.state.loading = true;
        try {
            const result = await rpc("/web/dataset/call_kw", {
                model: "bigcommerce.dashboard",
                method: "get_dashboard_data",
                args: [],
                kwargs: {
                    range_days: this.state.rangeDays,
                    instance_id: this.state.instanceId,
                },
            });
            if (requestSeq === this._dashboardRequestSeq) {
                this.state.data = result || this._emptyData();
            }
        } finally {
            if (requestSeq === this._dashboardRequestSeq) {
                this.state.loading = false;
            }
        }
    }

    async loadQuickActions() {
        const requestSeq = ++this._quickActionRequestSeq;
        const result = await rpc("/web/dataset/call_kw", {
            model: "bigcommerce.dashboard",
            method: "get_chat_quick_actions",
            args: [],
            kwargs: {
                instance_id: this.state.instanceId,
            },
        });
        if (requestSeq === this._quickActionRequestSeq) {
            this.state.quickActions = (result && result.actions) || [];
            this.state.chatEnabled = !!(result && result.enabled);
        }
    }

    async onChangeRange(ev) {
        this.state.rangeDays = ev.target.value;
        await this.loadData();
    }

    async onChangeInstance(ev) {
        this.state.instanceId = ev.target.value;
        await this.loadData();
        await this.loadQuickActions();
    }

    async onRefreshClick(ev) {
        if (ev) {
            ev.preventDefault();
            ev.stopPropagation();
        }
        if (this.state.loading) {
            return;
        }
        await this.loadInstances();
        await this.loadData();
        await this.loadQuickActions();
    }

    _startAutoRefresh() {
        this._stopAutoRefresh();
        this._autoRefreshTimer = setInterval(async () => {
            if (!this.state.loading) {
                try {
                    await this.loadData();
                } catch (_error) {
                    // Keep dashboard usable even if one auto-refresh cycle fails.
                }
            }
        }, 30000);
    }

    _stopAutoRefresh() {
        if (this._autoRefreshTimer) {
            clearInterval(this._autoRefreshTimer);
            this._autoRefreshTimer = null;
        }
    }

    toggleChat() {
        this.state.isChatOpen = !this.state.isChatOpen;
        this._scrollChatToBottom();
    }

    chatToggleLabel() {
        return this.state.isChatOpen ? "Hide AI Assistant" : "AI Assistant";
    }

    onChatInput(ev) {
        this.state.chatInput = ev.target.value || "";
    }

    _normalizeMessageInput(rawInput = null) {
        let candidate = rawInput;
        if (candidate === null || candidate === undefined || candidate === false) {
            candidate = this.state.chatInput;
        }

        if (candidate && typeof candidate === "object") {
            const objectValue =
                candidate.prompt ||
                candidate.message ||
                candidate.label ||
                (candidate.currentTarget && candidate.currentTarget.dataset
                    ? candidate.currentTarget.dataset.prompt
                    : "");
            candidate = objectValue || this.state.chatInput;
        }

        return String(candidate || "").trim();
    }

    async onQuickActionClick(actionOrEvent) {
        const prompt = this._normalizeMessageInput(actionOrEvent);
        if (!prompt || this.state.sending) {
            return;
        }
        this.state.chatInput = prompt;
        await this.sendMessage(prompt);
    }

    async sendMessage(forcedMessage = null) {
        const outgoing = this._normalizeMessageInput(forcedMessage);
        if (!outgoing || this.state.sending) {
            return;
        }

        this.state.messages.push({
            role: "user",
            content: outgoing,
            ts: new Date().toISOString(),
        });
        this.state.chatInput = "";
        this.state.sending = true;
        this._scrollChatToBottom();

        try {
            const history = this.state.messages.slice(-10).map((item) => ({
                role: item.role,
                content: item.content,
            }));
            const result = await rpc("/web/dataset/call_kw", {
                model: "bigcommerce.dashboard",
                method: "ask_ai_assistant",
                args: [],
                kwargs: {
                    message: outgoing,
                    instance_id: this.state.instanceId || "all",
                    history: history,
                },
            });

            if (result && result.ok) {
                this.state.messages.push({
                    role: "assistant",
                    content: result.answer || "Assistant did not return a response.",
                    ts: new Date().toISOString(),
                });
            } else {
                this.state.messages.push({
                    role: "assistant",
                    isError: true,
                    content: (result && result.error) || "Unable to get a response from the AI assistant.",
                    ts: new Date().toISOString(),
                });
            }
        } catch (error) {
            this.state.messages.push({
                role: "assistant",
                isError: true,
                content: error && error.message ? error.message : "Unexpected dashboard assistant error.",
                ts: new Date().toISOString(),
            });
        } finally {
            this.state.sending = false;
            this._scrollChatToBottom();
        }
    }

    _scrollChatToBottom() {
        setTimeout(() => {
            if (!this.el) {
                return;
            }
            const container = this.el.querySelector(".bc-chat-messages");
            if (container) {
                container.scrollTop = container.scrollHeight;
            }
        }, 0);
    }

    trendWidth(value, row) {
        const max = Math.max(Number(row.success || 0), Number(row.failed || 0), 1);
        return `${Math.round((Number(value || 0) * 100) / max)}%`;
    }

    badgeClass(status) {
        if (status === "failed" || status === "critical") {
            return "bc-badge bc-badge-danger";
        }
        if (status === "warning" || status === "processing") {
            return "bc-badge bc-badge-warning";
        }
        if (status === "success" || status === "healthy" || status === "done") {
            return "bc-badge bc-badge-success";
        }
        return "bc-badge bc-badge-muted";
    }
}

BigCommerceDashboard.template = "bigcommerce_dashboard_template";
registry.category("actions").add("bigcommerce_dashboard", BigCommerceDashboard);
