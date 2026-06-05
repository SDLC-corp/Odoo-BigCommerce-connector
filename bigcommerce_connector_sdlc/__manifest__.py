# -*- coding: utf-8 -*-
{
    "version": "18.0.2.0.0",
    "name": "BigCommerce Connector for Odoo",
    "summary": "Connect your BigCommerce store with Odoo ERP. Sync products, orders, inventory, and customers in real time.",
    "description": """
    BigCommerce Connector
    =====================
    
    This module connects your BigCommerce store to Odoo, letting you manage all your 
    eCommerce operations from your ERP system. No more switching between platforms for 
    orders, inventory, and customer data.
    
    What This Does
    ---------------
    • Import and export products with variants and images
    • Sync customer records automatically
    • Pull orders from BigCommerce into Odoo as sales orders
    • Keep inventory levels in sync between both systems
    • Track shipments and update order status
    • Monitor all sync activities with detailed logs
    • Receive real-time updates via BigCommerce webhooks
    
    How It Works
    -----------
    Setup is straightforward. Enter your BigCommerce API credentials, and the connector 
    handles the rest. Products, customers, and orders sync automatically on a schedule, 
    and you can trigger manual syncs whenever you need them. Webhooks let BigCommerce 
    notify Odoo about changes instantly.
    
    Field Mapping
    -----------
    Map your custom BigCommerce fields to Odoo fields however you need. The built-in 
    mapping wizard lets you test your mappings before running live syncs.
    
    Webhooks & Real-Time Updates
    ---------------------------
    Set up webhooks to receive updates the moment something changes in BigCommerce. 
    No more waiting for scheduled syncs.
    
    Operation Logs
    -----------
    Every sync operation gets logged with full details. See what was imported, what 
    failed, and why. Makes troubleshooting fast and easy.
    
    Multi-Store Support
    ----------------
    Run multiple BigCommerce stores? Manage them all from one Odoo instance. Each 
    store gets its own configuration.
    """,
    "category": "eCommerce",
    "author": "SDLC Corp",
    "maintainer": "SDLC Corp",
    "website": "https://sdlccorp.com/products/bigcommerce-odoo-connector/",
    "support": "sales@sdlccorp.com",
    "license": "OPL-1",
    "price": 19.99,
    "currency": "USD",
    "depends": [
        "base",
        "product",
        "sale",
        "stock",
        "contacts",
        "delivery",
        "mail",
        "account",
    ],
    "data": [
        "security/security.xml",
        "security/ir.model.access.csv",
        "data/bigcommerce_cron_data.xml",
        "views/bigcommerce_dashboard_action.xml",
        "views/bigcommerce_connector_views.xml",
        "views/field_mapping_views.xml",
        "views/field_mapping_test_wizard_views.xml",
        "views/bigcommerce_mapping_message_wizard_views.xml",
        "views/category_binding_views.xml",
        "views/product_binding_views.xml",
        "views/customer_binding_views.xml",
        "views/order_binding_views.xml",
        "views/bigcommerce_log_views.xml",
        "views/bigcommerce_webhook_views.xml",
        "views/menu_views.xml",
    ],
    "assets": {
        "web.assets_backend": [
            "bigcommerce_connector_sdlc/static/src/xml/bigcommerce_dashboard_templates.xml",
            "bigcommerce_connector_sdlc/static/src/js/bigcommerce_dashboard.js",
            "bigcommerce_connector_sdlc/static/src/css/bigcommerce_dashboard.css",
        ],
    },
    "images": ["static/description/banner.gif"],
    "installable": True,
    "application": True,
    "auto_install": False,
    "external_dependencies": {
        "python": ["requests"],
    },
}
