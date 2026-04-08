/** @odoo-module **/

import { Component } from "@odoo/owl";
import { registry } from "@web/core/registry";

// Minimal placeholder widget for views expecting "sdlcCustomContentKanbanLikeWidget"
class sdlcCustomContentKanbanLikeWidget extends Component {}
sdlcCustomContentKanbanLikeWidget.template = "sdlc_shopify_full_fixed.sdlcCustomContentKanbanLikeWidget";

registry
    .category("view_widgets")
    .add("sdlcCustomContentKanbanLikeWidget", { component: sdlcCustomContentKanbanLikeWidget });
