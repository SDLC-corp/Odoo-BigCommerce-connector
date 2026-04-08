from odoo import models, fields


class ShopifySyncReportLine(models.Model):
    _name = "shopify.sync.report.line"
    _description = "Shopify Sync Report Line"
    _order = "id desc"

    # LINK TO MAIN REPORT
    report_id = fields.Many2one(
        "shopify.sync.report",
        required=False,
        ondelete="set null",
    )
    active = fields.Boolean(default=True)

    record_type = fields.Selection([
        ("product", "Product"),
        ("customer", "Customer"),
        ("order", "Order"),
        ("category", "Category"),
        ("gift_card", "Gift Card"),
    ], required=True)

    source_action = fields.Selection([
        ("product_create", "Product - Push To Shopify"),
        ("product_update", "Product - Pull From Shopify"),
        ("customer_create", "Customer - Push To Shopify"),
        ("customer_update", "Customer - Pull From Shopify"),
        ("order_create", "Order - Push To Shopify"),
        ("order_update", "Order - Pull From Shopify"),
        ("category_create", "Category - Push To Shopify"),
        ("category_update", "Category - Pull From Shopify"),
        ("gift_card_create", "Gift Card - Pull From Shopify"),
        ("gift_card_update", "Gift Card - Pull From Shopify"),
        ("manual", "Manual Sync"),
        ("auto", "Auto Sync"),
        ("webhook", "Webhook Triggered"),
    ], default="manual")

    shopify_id = fields.Char(string="Shopify ID")
    name = fields.Char(string="Record Name")

    status = fields.Selection([
        ("success", "Success"),
        ("error", "Error"),
    ], required=True)

    error_message = fields.Text(string="Error Message")

    def unlink(self):
        # Archive instead of deleting to avoid FK issues from parent records.
        self.sudo().write({"active": False})
        return True
