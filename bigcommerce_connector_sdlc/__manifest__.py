# -*- coding: utf-8 -*-
{
    "version": "19.0.2.1.1",
    "name": "BigCommerce Connector for Odoo",
    "summary": "Import BigCommerce orders, products and customers into Odoo, sync stock one way, export shipments with tracking, with webhooks and an exception queue.",
    "description": """
BigCommerce Connector
=====================

* Incremental, idempotent order import (orders modified since the last run) as Odoo sales orders,
  with shipping, discounts, coupons and status-based confirmation/cancellation.
* Product import with BigCommerce options as Odoo attributes and variants; export of new or changed
  goods; category import; customer import and export (customers of the connector company only).
* Inventory sync in the direction of the chosen inventory master (traceable inventory adjustments on
  import; only changed quantities on export).
* Shipment export per validated delivery (partial deliveries included, exactly once).
* Authenticated webhooks with re-delivery protection, back-off retries and a rejected-callback trace.
* Exception queue (failed imports/exports, total or currency mismatch, unmatched products) with
  one-click re-sync; sync logs with automatic purge; dashboard.
* Three roles (read-only user, manager, administrator), credentials visible to administrators only,
  multi-company record rules.

Tested on Odoo 19 Community against mocked BigCommerce API responses only; see the README.
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
        "views/stock_picking_views.xml",
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
