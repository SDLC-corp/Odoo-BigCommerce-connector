from odoo import models, fields, api

class ShopifyMappingAudit(models.Model):
    _name = "shopify.mapping.audit"
    _description = "Shopify Mapping Audit"
    _order = "id desc"

    instance_id = fields.Many2one("shopify.instance", required=True, ondelete="cascade")
    mapping_type = fields.Selection([
        ("product", "Product"),
        ("customer", "Customer"),
        ("order", "Order"),
        ("category", "Category"),
    ], required=True)

    direction = fields.Selection([
        ("odoo_to_shopify", "Odoo to Shopify"),
        ("shopify_to_odoo", "Shopify to Odoo"),
    ], required=True, default="odoo_to_shopify")

    source_action = fields.Char()
    status = fields.Selection([("success", "Success"), ("error", "Error")], default="success")
    error_message = fields.Text()

    record_model = fields.Char()
    record_id = fields.Integer()
    record_name = fields.Char()

    payload_before = fields.Text()
    payload_after = fields.Text()
    response_text = fields.Text()

    line_ids = fields.One2many("shopify.mapping.audit.line", "audit_id", string="Changes")

    @api.model
    def create_audit(self, instance, mapping_type, direction, record, source_action,
                     payload_before=None, payload_after=None, response_text=None,
                     status="success", error_message=None, change_lines=None):

        audit = self.sudo().create({
            "instance_id": instance.id,
            "mapping_type": mapping_type,
            "direction": direction,
            "source_action": source_action,
            "status": status,
            "error_message": error_message or False,
            "record_model": record._name if record else False,
            "record_id": record.id if record else 0,
            "record_name": record.display_name if record else False,
            "payload_before": payload_before or False,
            "payload_after": payload_after or False,
            "response_text": response_text or False,
        })

        lines = []
        for ln in (change_lines or []):
            lines.append((0, 0, {
                "odoo_field": ln.get("odoo_field"),
                "shopify_field": ln.get("shopify_field"),
                "scope": ln.get("scope"),
                "old_value": ln.get("old_value"),
                "new_value": ln.get("new_value"),
                "applied": ln.get("applied", True),
                "note": ln.get("note"),
            }))
        if lines:
            audit.sudo().write({"line_ids": lines})

        return audit


class ShopifyMappingAuditLine(models.Model):
    _name = "shopify.mapping.audit.line"
    _description = "Shopify Mapping Audit Line"
    _order = "id"

    audit_id = fields.Many2one("shopify.mapping.audit", required=True, ondelete="cascade")
    odoo_field = fields.Char()
    shopify_field = fields.Char()
    scope = fields.Selection([("product", "Product"), ("variant", "Variant")])
    old_value = fields.Text()
    new_value = fields.Text()
    applied = fields.Boolean(default=True)
    note = fields.Char()
