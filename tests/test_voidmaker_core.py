import unittest

from voidmaker import (
    ArbOpportunity,
    BookTop,
    LandmineConfig,
    LandmineFill,
    build_landmine_orders,
    estimate_landmine_profit_cents,
    find_cross_venue_arbs,
    select_close_exchanges,
    unprotected_position,
)


class VoidmakerCoreTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
