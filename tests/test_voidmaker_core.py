import unittest

from voidmaker import (
    ArbOpportunity,
    BookTop,
    ExchangeClient,
    LandmineConfig,
    LandmineFill,
    MarketHub,
    PlannedOrder,
    build_landmine_orders,
    can_submit_order,
    estimate_landmine_profit_cents,
    find_cross_venue_arbs,
    select_close_exchanges,
    unprotected_position,
)


class RecordingExchangeClient(ExchangeClient):
    def __init__(self, exchange="LSE", cfg=None):
        super().__init__(exchange, cfg or LandmineConfig())
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)


class VoidmakerCoreTest(unittest.IsolatedAsyncioTestCase):
    def test_select_close_exchanges_prefers_current_location_cluster(self):
        self.assertEqual(
            select_close_exchanges("ZSE", max_rtt_ms=30),
            ["ZSE", "Euronext", "LSE"],
        )
        self.assertEqual(
            select_close_exchanges("HKEX", max_rtt_ms=60),
            ["HKEX", "SSE", "JPX", "NSE"],
        )

    def test_landmine_orders_are_passive_and_include_extreme_free_options(self):
        cfg = LandmineConfig(extreme_qty=7, wide_qty=3)
        orders = build_landmine_orders(
            "ZSE",
            "CARD",
            BookTop(best_bid=10_000, best_ask=10_004),
            cfg,
        )

        bids = [order for order in orders if order.side == "bid"]
        asks = [order for order in orders if order.side == "ask"]

        self.assertIn(1, [order.price for order in bids])
        self.assertIn(999_999, [order.price for order in asks])
        self.assertTrue(all(order.price < 10_004 for order in bids))
        self.assertTrue(all(order.price > 10_000 for order in asks))
        self.assertTrue(all(order.quantity > 0 for order in orders))

    def test_landmine_profit_estimate_uses_only_favorable_extreme_fills(self):
        fills = [
            LandmineFill(exchange="SSE", ticker="ZABA", side="ask", price=999_999, quantity=4),
            LandmineFill(exchange="NYSE", ticker="ZABA", side="bid", price=1, quantity=10),
            LandmineFill(exchange="JPX", ticker="HT", side="bid", price=5, quantity=100),
            LandmineFill(exchange="Euronext", ticker="KOTD", side="ask", price=35_667, quantity=5),
        ]

        profit = estimate_landmine_profit_cents(fills, conservative_cover_cents=10_000)

        expected = (
            (999_999 - 10_000) * 4
            + (10_000 - 1) * 10
            + (10_000 - 5) * 100
            + (35_667 - 10_000) * 5
        )
        self.assertEqual(profit, expected)

    def test_cross_venue_arb_finds_best_close_exchange_spread(self):
        books = {
            "ZSE": {
                "CARD": BookTop(best_bid=10_000, best_ask=10_010, best_bid_qty=50, best_ask_qty=40),
                "SIMP": BookTop(best_bid=10_000, best_ask=10_002, best_bid_qty=50, best_ask_qty=50),
            },
            "LSE": {
                "CARD": BookTop(best_bid=10_200, best_ask=10_220, best_bid_qty=30, best_ask_qty=50),
                "SIMP": BookTop(best_bid=10_001, best_ask=10_003, best_bid_qty=50, best_ask_qty=50),
            },
            "Euronext": {
                "CARD": BookTop(best_bid=10_090, best_ask=10_110, best_bid_qty=50, best_ask_qty=50),
            },
        }

        arbs = find_cross_venue_arbs(books, ["ZSE", "LSE", "Euronext"], min_spread_cents=20, max_qty=50)

        self.assertEqual(
            arbs[0],
            ArbOpportunity(
                ticker="CARD",
                buy_exchange="ZSE",
                sell_exchange="LSE",
                buy_price=10_010,
                sell_price=10_200,
                quantity=30,
            ),
        )

    def test_unprotected_position_keeps_tracked_arb_inventory_out_of_liquidation(self):
        self.assertEqual(unprotected_position(actual=40, protected=25), 15)
        self.assertEqual(unprotected_position(actual=-40, protected=-25), -15)
        self.assertEqual(unprotected_position(actual=25, protected=40), 0)
        self.assertEqual(unprotected_position(actual=-25, protected=-40), 0)
        self.assertEqual(unprotected_position(actual=20, protected=-20), 20)

    def test_can_submit_order_blocks_asks_without_available_inventory(self):
        order = PlannedOrder("LSE", "CARD", "ask", 10_100, 25, "ioc")

        self.assertFalse(
            can_submit_order(
                order,
                cash_total=10_000_000,
                cash_reserved=0,
                position_total=0,
                position_reserved=0,
                allow_short=False,
            )
        )
        self.assertTrue(
            can_submit_order(
                order,
                cash_total=10_000_000,
                cash_reserved=0,
                position_total=30,
                position_reserved=5,
                allow_short=False,
            )
        )

    def test_can_submit_order_blocks_bids_without_cash_headroom(self):
        order = PlannedOrder("ZSE", "CARD", "bid", 10_000, 100, "ioc")

        self.assertFalse(
            can_submit_order(
                order,
                cash_total=-4_950_000,
                cash_reserved=0,
                position_total=0,
                position_reserved=0,
                allow_short=False,
                min_cash=-5_000_000,
                cash_buffer=100_000,
            )
        )

    async def test_local_shadow_reservation_prevents_overasking_between_inventory_ticks(self):
        client = RecordingExchangeClient()
        client.state.positions["LSE-CARD"] = 10

        await client.add_order(PlannedOrder("LSE", "CARD", "ask", 999_999, 7), tag="landmine")
        await client.add_order(PlannedOrder("LSE", "CARD", "ask", 250_000, 7), tag="landmine")

        self.assertEqual(len(client.sent), 1)

    async def test_immediate_fill_updates_local_inventory_before_next_inventory_tick(self):
        client = RecordingExchangeClient()
        client.state.positions["LSE-CARD"] = 25
        await client.add_order(PlannedOrder("LSE", "CARD", "ask", 10_100, 25, "ioc"), tag="arb")

        req_id = client.sent[0]["user_request_id"]
        client.on_add_order_response(
            {
                "type": "add_order_response",
                "user_request_id": req_id,
                "success": True,
                "data": {"immediate_inventory_change": -25, "immediate_balance_change": 252_500},
            }
        )

        self.assertEqual(client.state.positions["LSE-CARD"], 0)

    async def test_seed_inventory_buys_when_below_target(self):
        client = RecordingExchangeClient()
        client.cfg = LandmineConfig(seed_inventory_qty=20, seed_clip_qty=7)
        client.state.books["LSE-CARD"] = BookTop(best_bid=9_998, best_ask=10_002, best_bid_qty=50, best_ask_qty=11)

        await client.seed_inventory()

        self.assertEqual(len(client.sent), 1)
        self.assertEqual(client.sent[0]["side"], "bid")
        self.assertEqual(client.sent[0]["order_type"], "ioc")
        self.assertEqual(client.sent[0]["price"], 10_002)
        self.assertEqual(client.sent[0]["quantity"], 7)

    async def test_seed_inventory_does_not_rebuy_intentional_arb_sale(self):
        client = RecordingExchangeClient()
        client.cfg = LandmineConfig(seed_inventory_qty=20, seed_clip_qty=10)
        client.state.positions["LSE-CARD"] = 0
        client.state.arb_inventory["LSE-CARD"] = -20
        client.state.books["LSE-CARD"] = BookTop(best_bid=10_500, best_ask=10_502, best_bid_qty=50, best_ask_qty=50)

        await client.seed_inventory()

        self.assertEqual(client.sent, [])

    async def test_liquidation_preserves_seed_inventory_target(self):
        client = RecordingExchangeClient()
        client.cfg = LandmineConfig(seed_inventory_qty=20)
        client.state.positions["LSE-CARD"] = 20
        client.state.books["LSE-CARD"] = BookTop(best_bid=10_000, best_ask=10_002, best_bid_qty=50, best_ask_qty=50)

        await client.liquidate_inventory()

        self.assertEqual(client.sent, [])

    async def test_landmine_asks_do_not_reserve_seed_inventory(self):
        client = RecordingExchangeClient()
        client.cfg = LandmineConfig(seed_inventory_qty=20)
        client.state.positions["LSE-CARD"] = 20
        client.state.books["LSE-CARD"] = BookTop(best_bid=10_000, best_ask=10_002, best_bid_qty=50, best_ask_qty=50)

        await client.seed_landmines()

        self.assertTrue(client.sent)
        self.assertTrue(all(payload["side"] == "bid" for payload in client.sent))

    async def test_market_hub_downsizes_arb_to_sellable_inventory(self):
        cfg = LandmineConfig(arb_min_spread_cents=20, arb_clip_qty=25, seed_inventory_qty=20)
        buy_client = RecordingExchangeClient("ZSE", cfg)
        sell_client = RecordingExchangeClient("LSE", cfg)
        buy_client.state.books["ZSE-CARD"] = BookTop(best_bid=10_000, best_ask=10_000, best_bid_qty=50, best_ask_qty=50)
        sell_client.state.books["LSE-CARD"] = BookTop(best_bid=10_100, best_ask=10_110, best_bid_qty=50, best_ask_qty=50)
        sell_client.state.positions["LSE-CARD"] = 20

        await MarketHub({"ZSE": buy_client, "LSE": sell_client}, cfg).scan_once()

        self.assertEqual(len(buy_client.sent), 1)
        self.assertEqual(len(sell_client.sent), 1)
        self.assertEqual(buy_client.sent[0]["quantity"], 20)
        self.assertEqual(sell_client.sent[0]["quantity"], 20)


if __name__ == "__main__":
    unittest.main()
