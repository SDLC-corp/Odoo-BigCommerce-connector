{
    "name": "Shopify Connector",
    "version": "1.0.0",
    "summary": "Odoo ↔ Shopify Connector (Odoo 18 Compatible)",
    "author": "SDLC Corp",
    "website": 'https://sdlccorp.com/',
    "maintainer": 'SDLC Corp',
    "category": "Integration",
    "license": "LGPL-3",

    # =============================
    # DEPENDENCIES (MINIMAL & SAFE)
    # =============================
    "depends": [
        "base",
        "web",
        "website",
        "product",
        "sale_management",
        "stock",        
        "delivery",     
        "stock_delivery",  
        "contacts",
    ],

    # =============================
    # DATA FILES
    # =============================
    "data": [
        # SECURITY
        "security/ir.model.access.csv",

        # ACTIONS
        "views/shopify_dashboard_action.xml",
        "views/shopify_actions.xml",
        "views/shopify_sync_report_action.xml",

        # MENUS
        "views/shopify_menu.xml",
        "views/shopify_mapping_audit_menu.xml",

        # CORE BUSINESS VIEWS
        "views/shopify_instance_view.xml",
        "views/shopify_views.xml",
        "views/res_partner_view.xml",
        "views/product_category_view.xml",
        "views/sale_order_view.xml",
        "views/stock_picking_view.xml",
        "views/shopify_order_report.xml",
        "views/shopify_sales_report.xml",

        # FIELD MAPPING
        "views/shopify_field_mapping_views.xml",
        "views/shopify_mapping_audit_views.xml",
        "views/shopify_mapping_test_popup.xml",
        "views/shopify_mapping_test_wizard_views.xml",
        "views/shopify_webhook_report_view.xml",

        # GIFT CARD
        "views/shopify_gift_card_view.xml",

        # REPORTS
        "views/shopify_sync_report_view.xml",
        "views/shopify_sync_report_template.xml",
        "views/shopify_inventory_report_views.xml",

        # CRON
        "data/shopify_cron.xml",

        # SHIPPING (NO res.config.settings HERE)
        "views/shopify_shipping_menu.xml",
    ],

    # =============================
    # FRONTEND ASSETS (ODOO 17/18)
    # =============================
    "assets": {
        "web.assets_backend": [
            # JS
            "sdlc_shopify_connector/static/src/js/shopify_dashboard.js",
            "sdlc_shopify_connector/static/src/js/custom_widget_placeholder.js",

            # CSS
            "sdlc_shopify_connector/static/src/css/dashboard.css",

            # QWEB (backend load)
            "sdlc_shopify_connector/static/src/xml/shopify_dashboard.xml",
            "sdlc_shopify_connector/static/src/xml/custom_widget_placeholder.xml",
        ],
    },

    # =============================
    # APP CONFIG
    # =============================
    "installable": True,
    "application": True,
    "post_init_hook": "post_init_hook",
    "price": 300,
    "currency": "USD",
    "live_test_url": "http://159.65.145.19:8070/sdlc",
    "icon": "sdlc_shopify_connector/static/description/banner.png",
    "images":["static/description/banner.png"],
}
