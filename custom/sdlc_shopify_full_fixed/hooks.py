def post_init_hook(cr, registry):
    # Ensure report_id is nullable and FK uses SET NULL even after upgrades.
    cr.execute(
        "ALTER TABLE shopify_sync_report_line "
        "ALTER COLUMN report_id DROP NOT NULL"
    )
    cr.execute(
        "ALTER TABLE shopify_sync_report_line "
        "DROP CONSTRAINT IF EXISTS shopify_sync_report_line_report_id_fkey"
    )
    cr.execute(
        "ALTER TABLE shopify_sync_report_line "
        "ADD CONSTRAINT shopify_sync_report_line_report_id_fkey "
        "FOREIGN KEY (report_id) REFERENCES shopify_sync_report(id) ON DELETE SET NULL"
    )
    cr.execute(
        "UPDATE ir_model_fields "
        "SET required = FALSE, on_delete = 'set null' "
        "WHERE model = 'shopify.sync.report.line' AND name = 'report_id'"
    )
