from odoo import models


class QuotationDocument(models.Model):
    _inherit = "quotation.document"

    def _auto_init(self):
        """Ensure ir_attachment_id column exists before FK creation."""
        cr = self._cr
        cr.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'quotation_document' AND column_name = 'ir_attachment_id'
            """
        )
        if not cr.fetchone():
            cr.execute("ALTER TABLE quotation_document ADD COLUMN ir_attachment_id INTEGER")
        return super()._auto_init()
