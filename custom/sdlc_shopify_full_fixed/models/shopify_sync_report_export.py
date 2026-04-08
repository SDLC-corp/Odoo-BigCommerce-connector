from odoo import http
from odoo.http import request
from io import StringIO
import csv


class ShopifySyncReportExport(http.Controller):

    @http.route(
        "/shopify/sync_report/csv/<int:report_id>",
        type="http",
        auth="user",
    )
    def export_csv(self, report_id, **kwargs):
        Report = request.env["shopify.sync.report"].sudo()
        report = Report.browse(report_id)
        if not report.exists():
            return request.not_found()

        output = StringIO()
        writer = csv.writer(output)

        # Header row
        writer.writerow(["Record Type", "Shopify ID", "Name", "Status", "Error Message"])

        for line in report.line_ids:
            writer.writerow([
                line.record_type or "",
                line.shopify_id or "",
                line.name or "",
                line.status or "",
                (line.error_message or "").replace("\n", " "),
            ])

        csv_content = output.getvalue()
        output.close()

        filename = f"shopify_sync_report_{report.id}.csv"

        return request.make_response(
            csv_content,
            headers=[
                ("Content-Type", "text/csv; charset=utf-8"),
                ("Content-Disposition", f'attachment; filename="{filename}"'),
            ],
        )
