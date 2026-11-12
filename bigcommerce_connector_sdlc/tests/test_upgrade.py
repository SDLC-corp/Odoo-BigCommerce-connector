"""Upgrade logic of migrations/20.0.2.1.0 (role grant for existing internal users)."""
import importlib.util
from pathlib import Path

from odoo.tests.common import new_test_user, tagged

from .common import GROUP_ADMIN, GROUP_MANAGER, GROUP_USER, BigCommerceCommon


def _post_migration():
    path = Path(__file__).resolve().parent.parent / "migrations" / "20.0.2.1.0" / "post-migration.py"
    spec = importlib.util.spec_from_file_location("bc_post_migration_20_0_2_1_0", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@tagged("post_install", "-at_install", "bigcommerce")
class TestUpgradeRoleGrant(BigCommerceCommon):

    def test_internal_users_without_role_get_read_only_role(self):
        portal = new_test_user(self.env, login="bc_portal_user", groups="base.group_portal")
        archived = new_test_user(self.env, login="bc_archived_user", groups="base.group_user")
        archived.active = False
        manager_groups = set(self.user_manager.group_ids.ids)
        admin_groups = set(self.user_admin.group_ids.ids)

        granted = _post_migration().grant_read_only_role(self.env)

        self.assertIn(self.user_plain, granted)
        self.assertTrue(self.user_plain.has_group(GROUP_USER))
        self.assertFalse(self.user_plain.has_group(GROUP_MANAGER), "read-only role only")
        for skipped in (self.user_manager, self.user_admin, self.user_reader, portal, archived):
            self.assertNotIn(skipped, granted, skipped.login)
        self.assertFalse(portal.has_group(GROUP_USER))
        self.assertFalse(archived.has_group(GROUP_USER))
        self.assertEqual(set(self.user_manager.group_ids.ids), manager_groups)
        self.assertEqual(set(self.user_admin.group_ids.ids), admin_groups)
        self.assertTrue(self.user_admin.has_group(GROUP_ADMIN))
        # Settings administrators already hold the Administrator role through base.group_system.
        self.assertNotIn(self.env.ref("base.user_admin"), granted)
        # Idempotent: a second run grants nothing.
        self.assertFalse(_post_migration().grant_read_only_role(self.env))
