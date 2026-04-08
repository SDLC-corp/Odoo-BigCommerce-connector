from odoo import models, fields, api, _
import logging

_logger = logging.getLogger(__name__)


class ShopifySyncReport(models.Model):
    _name = "shopify.sync.report"
    _description = "Shopify Sync Report"
    _order = "create_date desc"

    active = fields.Boolean(default=True)
    active = fields.Boolean(default=True)
    instance_id = fields.Many2one(
        "shopify.instance",
        required=True,
        string="Shopify Instance",
        ondelete="cascade",
    )

    sync_type = fields.Selection(
        [
            ("product", "Product"),
            ("customer", "Customer"),
            ("order", "Order"),
            ("category", "Category"),
            ("gift_card", "Gift Card"),
            ("all", "Full Sync"),
        ],
        required=True,
        string="Sync Type",
    )

    # ==================================================
    # WEBHOOK / MODE INFORMATION
    # ==================================================
    mode = fields.Selection(
        [
            ("manual", "Manual"),
            ("cron", "Cron"),
            ("webhook", "Webhook"),
        ],
        string="Mode",
        default="manual",
        index=True,
    )

    operation = fields.Selection(
        [
            ("create", "Create"),
            ("update", "Update"),
        ],
        string="Operation",
        index=True,
    )

    source_action = fields.Char(
        string="Source Action",
        compute="_compute_source_action",
        store=False,
    )

    reference = fields.Char(
        string="Shopify Reference ID",
        index=True,
    )

    # ==================================================
    # TIME + COUNTS
    # ==================================================
    start_time = fields.Datetime(string="Start Time")
    end_time = fields.Datetime(string="End Time")

    total_records = fields.Integer(string="Total Records")
    success_count = fields.Integer(string="Success")
    error_count = fields.Integer(string="Errors")

    # ==================================================
    # LINES
    # ==================================================
    line_ids = fields.One2many(
        "shopify.sync.report.line",
        "report_id",
        string="Details",
    )

    # ==================================================
    # COMPUTED FLAGS
    # ==================================================
    has_webhook = fields.Boolean(
        string="Webhook",
        compute="_compute_has_webhook",
        store=True,
    )

    # Nice label
    name = fields.Char(string="Name", compute="_compute_name", store=False)

    # -------------------------------------------------
    # NAME
    # -------------------------------------------------
    @api.depends("instance_id", "sync_type", "start_time", "mode", "operation")
    def _compute_name(self):
        for rec in self:
            synctype = dict(self._fields["sync_type"].selection).get(
                rec.sync_type, rec.sync_type
            )
            parts = []

            if rec.instance_id:
                parts.append(rec.instance_id.name)

            if synctype:
                parts.append(synctype)

            if rec.mode == "webhook" and rec.operation:
                parts.append(rec.operation.capitalize())

            if rec.start_time:
                parts.append(str(rec.start_time))

            rec.name = " - ".join(parts) if parts else "Sync Report"

    # -------------------------------------------------
    # WEBHOOK DETECTION (existing logic SAFE)
    # -------------------------------------------------
    @api.depends("line_ids.source_action", "mode")
    def _compute_has_webhook(self):
        """Mark reports that actually came from webhooks."""
        for rec in self:
            if rec.mode == "webhook":
                rec.has_webhook = True
                continue
            rec.has_webhook = any(
                (line.source_action or "") == "webhook" for line in rec.line_ids
            )

    @api.depends("line_ids.source_action")
    def _compute_source_action(self):
        for rec in self:
            rec.source_action = rec.line_ids[:1].source_action or False

    # -------------------------------------------------
    # PDF BUTTON
    # -------------------------------------------------
    def action_print_pdf(self):
        self.ensure_one()
        report_action = self.env.ref(
            "sdlc_shopify_full_fixed.action_report_shopify_sync",
            raise_if_not_found=False,
        )

        if not report_action:
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": "Report Missing",
                    "message": "PDF Report action not found. Please contact admin.",
                    "type": "danger",
                },
            }

        return report_action.report_action(self)

    # -------------------------------------------------
    # CSV / Excel BUTTON
    # -------------------------------------------------
    def action_export_csv(self):
        self.ensure_one()
        url = f"/shopify/sync_report/csv/{self.id}"
        return {
            "type": "ir.actions.act_url",
            "url": url,
            "target": "self",
        }

    # -------------------------------------------------
    # EMAIL SENDING (used by cron)
    # -------------------------------------------------
    def _send_email_notification(self):
        Mail = self.env["mail.mail"].sudo()

        for report in self:
            instance = report.instance_id
            if not instance or not instance.notification_email:
                continue

            synctype = dict(self._fields["sync_type"].selection).get(
                report.sync_type, report.sync_type
            )

            subject = _("Shopify Sync Report (%s) - %s") % (synctype, instance.name)

            body = """
                <p><b>%s</b></p>
                <p>%s</p>
                <ul>
                    <li><b>Mode:</b> %s</li>
                    <li><b>Operation:</b> %s</li>
                    <li><b>Total:</b> %s</li>
                    <li><b>Success:</b> %s</li>
                    <li><b>Errors:</b> %s</li>
                </ul>
            """ % (
                subject,
                _("Sync completed for Shopify instance: %s") % (instance.name,),
                report.mode or "-",
                report.operation or "-",
                report.total_records,
                report.success_count,
                report.error_count,
            )

            mail = Mail.create(
                {
                    "subject": subject,
                    "body_html": body,
                    "email_to": instance.notification_email,
                }
            )
            try:
                mail.send()
            except Exception as e:
                _logger.error("Failed to send Shopify sync email: %s", e)

    def unlink(self):
        # Archive instead of deleting to avoid FK errors on report lines.
        self.sudo().mapped("line_ids").write({"active": False})
        self.sudo().write({"active": False})
        return True
