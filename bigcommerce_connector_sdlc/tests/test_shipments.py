"""Shipment export: BigCommerce's required payload, partial shipments, idempotency."""
from odoo.tests.common import tagged

from ..services.order_sync_service import BigCommerceOrderSyncService
from ..services.shipment_sync_service import BigCommerceShipmentSyncService
from .common import STORE_PATH, BigCommerceCommon, v2_pages


@tagged("post_install", "-at_install", "bigcommerce")
class TestShipmentExport(BigCommerceCommon):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.product = cls.env["product.product"].create({
            "name": "Ship Widget", "default_code": "BC-SKU-1", "type": "consu", "is_storable": True,
            "list_price": 10.0, "taxes_id": [(5, 0, 0)],
        })
        cls.env["stock.quant"]._update_available_quantity(cls.product, cls.warehouse.lot_stock_id, 100)

    def setUp(self):
        super().setUp()
        self.bind_product(self.product)
        self.mock_order(self.order_payload(), [self.order_line(qty=3)])
        BigCommerceOrderSyncService(self.instance._bc_service_instance()).import_order_by_id(101)
        self.http.reset()
        self.binding = self.env["bigcommerce.order.binding"].search(
            [("instance_id", "=", self.instance.id), ("bigcommerce_order_id", "=", "101")])
        self.sale = self.binding.sale_order_id
        # The default "Order Status" mapping (created with the connector since 19.0.2.1.2)
        # confirms an "Awaiting Fulfillment" order on import.
        self.assertEqual(self.sale.state, "sale")
        self.picking = self.sale.picking_ids

    def deliver(self, picking, qty, tracking="TRK-1"):
        picking.move_ids.quantity = qty
        picking.carrier_tracking_ref = tracking
        res = picking.with_context(skip_backorder=False).button_validate()
        if isinstance(res, dict) and res.get("res_model") == "stock.backorder.confirmation":
            self.env["stock.backorder.confirmation"].with_context(res["context"]).create({}).process()
        return picking

    def mock_store(self, remaining_shipped=0, existing=None):
        self.http.add("GET", STORE_PATH + "/v2/orders/101/shipments", callback=v2_pages(existing))
        self.http.add("GET", STORE_PATH + "/v2/orders/101/shipping_addresses", [{"id": 9, "order_id": 101}])
        self.http.add("GET", STORE_PATH + "/v2/orders/101/products",
                      callback=v2_pages([self.order_line(qty=3, shipped=remaining_shipped)]))
        self.http.add("POST", STORE_PATH + "/v2/orders/101/shipments", {"id": 3001}, status=201)

    def service(self):
        return BigCommerceShipmentSyncService(self.instance._bc_service_instance())

    def test_payload_has_required_address_and_items(self):
        """Regression: order_address_id and items were missing, so BigCommerce refused every shipment."""
        self.deliver(self.picking, 3)
        self.mock_store()
        result = self.service().export_shipments()
        self.assertEqual(result["exported"], 1, result)
        payload = self.http.calls_to("POST", STORE_PATH + "/v2/orders/101/shipments")[0]["json"]
        self.assertEqual(payload["order_address_id"], 9)
        self.assertEqual(payload["items"], [{"order_product_id": 501, "quantity": 3}])
        self.assertEqual(payload["tracking_number"], "TRK-1")
        self.assertEqual(self.picking.bigcommerce_shipment_id, "3001")

    def test_exported_once_only(self):
        self.deliver(self.picking, 3)
        self.mock_store()
        self.service().export_shipments()
        self.http.calls.clear()
        result = self.service().export_shipments()
        self.assertEqual(result["total"], 0)
        self.assertFalse(self.http.calls_to("POST", STORE_PATH + "/v2/orders/101/shipments"))

    def test_partial_deliveries_become_separate_shipments(self):
        self.deliver(self.picking, 1, tracking="TRK-A")
        backorder = self.sale.picking_ids - self.picking
        self.assertTrue(backorder)
        self.mock_store()
        self.service().export_shipments()
        first = self.http.calls_to("POST", STORE_PATH + "/v2/orders/101/shipments")[0]["json"]
        self.assertEqual(first["items"], [{"order_product_id": 501, "quantity": 1}])
        self.deliver(backorder, 2, tracking="TRK-B")
        self.http.reset()
        self.mock_store(remaining_shipped=1)
        self.http.add("POST", STORE_PATH + "/v2/orders/101/shipments", {"id": 3002}, status=201)
        self.service().export_shipments()
        second = self.http.calls_to("POST", STORE_PATH + "/v2/orders/101/shipments")[0]["json"]
        self.assertEqual(second["items"], [{"order_product_id": 501, "quantity": 2}])
        self.assertEqual(backorder.bigcommerce_shipment_id, "3002")

    def test_existing_remote_shipment_is_linked_not_duplicated(self):
        self.deliver(self.picking, 3)
        self.mock_store(existing=[{"id": 2999, "tracking_number": "TRK-1"}])
        self.service().export_shipments()
        self.assertFalse(self.http.calls_to("POST", STORE_PATH + "/v2/orders/101/shipments"))
        self.assertEqual(self.picking.bigcommerce_shipment_id, "2999")

    def test_order_lines_beyond_the_first_page_are_matched(self):
        """Regression: /v2/orders/{id}/products was read without paging (50 per page), so a delivered
        line on a later page was never matched and the shipment could not be exported."""
        self.deliver(self.picking, 3)
        self.mock_store()
        others = [self.order_line(line_id=1000 + i, product_id=9000 + i, variant_id=9500 + i, sku="OTHER-%s" % i)
                  for i in range(59)]
        lines = others[:55] + [self.order_line(line_id=555, qty=3)] + others[55:]
        self.http.add("GET", STORE_PATH + "/v2/orders/101/products", callback=v2_pages(lines, max_page_size=50))
        result = self.service().export_shipments()
        self.assertEqual(result["exported"], 1, result)
        payload = self.http.calls_to("POST", STORE_PATH + "/v2/orders/101/shipments")[0]["json"]
        self.assertEqual(payload["items"], [{"order_product_id": 555, "quantity": 3}])
        pages = [c["params"]["page"] for c in self.http.calls_to("GET", STORE_PATH + "/v2/orders/101/products")]
        self.assertEqual(pages, [1, 2, 3])

    def test_existing_shipment_on_a_later_page_is_linked_not_duplicated(self):
        """Regression: /v2/orders/{id}/shipments was read without paging, so a shipment past the first
        50 was not found and a duplicate was created."""
        self.deliver(self.picking, 3)
        self.mock_store()
        shipments = [{"id": 2000 + i, "tracking_number": "OLD-%s" % i} for i in range(50)]
        shipments.append({"id": 2999, "tracking_number": "TRK-1"})
        self.http.add("GET", STORE_PATH + "/v2/orders/101/shipments", callback=v2_pages(shipments, max_page_size=50))
        self.service().export_shipments()
        self.assertFalse(self.http.calls_to("POST", STORE_PATH + "/v2/orders/101/shipments"))
        self.assertEqual(self.picking.bigcommerce_shipment_id, "2999")

    def test_refusal_goes_to_exception_queue(self):
        self.deliver(self.picking, 3)
        self.mock_store()
        self.http.add("POST", STORE_PATH + "/v2/orders/101/shipments", {"title": "Bad"}, status=400)
        result = self.service().export_shipments()
        self.assertEqual(result["failed"], 1)
        self.assertTrue(self.picking.bigcommerce_shipment_error)
        self.assertTrue(self.binding.needs_attention)
        self.assertFalse(self.picking.bigcommerce_shipment_id)

    def test_deliveries_of_other_orders_are_ignored(self):
        partner = self.env["res.partner"].create({"name": "Walk-in"})
        other = self.env["sale.order"].create({
            "partner_id": partner.id,
            "order_line": [(0, 0, {"product_id": self.product.id, "product_uom_qty": 1, "price_unit": 1})]})
        other.action_confirm()
        self.deliver(other.picking_ids, 1, tracking="OTHER")
        result = self.service().export_shipments()
        self.assertEqual(result["total"], 0)
        self.assertFalse(self.env["bigcommerce.sync.log"].search(
            [("instance_id", "=", self.instance.id), ("status", "=", "skipped")]))

    def test_known_carrier_maps_to_documented_provider(self):
        service = self.service()
        carrier = self.env["delivery.carrier"].create({
            "name": "FedEx Ground", "product_id": self.env["product.product"].create(
                {"name": "Fee", "type": "service"}).id})
        self.picking.carrier_id = carrier
        self.assertEqual(service._shipping_provider(self.picking), "fedex")
        carrier.name = "Local courier"
        self.assertFalse(service._shipping_provider(self.picking))
