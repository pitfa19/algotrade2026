import time
import unittest

from simp_card_cross_median_bot import (
    Book,
    EdgeConfig,
    ExchangeRunner,
    Inventory,
    StrategyCore,
    median_int,
)


class StrategyCoreTests(unittest.TestCase):
    def test_median_int_keeps_half_cent_precision_as_doubled_cents(self):
        self.assertEqual(median_int([20001, 19999, 20005]), 20001)
        self.assertEqual(median_int([20000, 20002]), 20001)

    def test_simp_sweeps_visible_asks_below_fixed_fair_with_cash_and_position_caps(self):
        core = StrategyCore(EdgeConfig(home="ZSE", targets=("ZSE",)))
        now = time.monotonic()
        book = Book(
            exchange="ZSE",
            ticker="SIMP",
            server_time_ms=123_400,
            bids=[(9998, 50)],
            asks=[(9994, 80), (9995, 100), (9996, 100)],
            received_at=now,
        )
        inv = Inventory(cash=10_000_000, positions={"ZSE-SIMP": 1_750})

        orders = core.plan_orders("ZSE", book, inv, now)

        self.assertEqual(len(orders), 1)
        order = orders[0]
        self.assertEqual(order.side, "bid")
        self.assertEqual(order.price, 9995)
        self.assertEqual(order.quantity, 50)
        self.assertEqual(order.reason, "SIMP_FIXED_FAIR")

    def test_simp_sells_visible_bids_above_fixed_fair_without_breaching_short_floor(self):
        core = StrategyCore(EdgeConfig(home="NYSE", targets=("NYSE",)))
        now = time.monotonic()
        book = Book(
            exchange="NYSE",
            ticker="SIMP",
            server_time_ms=222_000,
            bids=[(10004, 80), (10003, 80), (10002, 80)],
            asks=[(10006, 50)],
            received_at=now,
        )
        inv = Inventory(cash=10_000_000, positions={"NYSE-SIMP": -175})

        orders = core.plan_orders("NYSE", book, inv, now)

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].side, "ask")
        self.assertEqual(orders[0].price, 10003)
        self.assertEqual(orders[0].quantity, 25)

    def test_card_uses_fresh_cross_median_and_home_threshold(self):
        core = StrategyCore(EdgeConfig(home="ZSE", targets=("ZSE",), card_min_fresh=3))
        now = time.monotonic()
        for exchange, bid, ask in [
            ("NYSE", 10100, 10104),
            ("NASDAQ", 10102, 10106),
            ("LSE", 10104, 10108),
            ("Euronext", 10106, 10110),
            ("HKEX", 10108, 10112),
        ]:
            core.update_book(
                Book(
                    exchange=exchange,
                    ticker="CARD",
                    server_time_ms=100_000,
                    bids=[(bid, 50)],
                    asks=[(ask, 50)],
                    received_at=now,
                )
            )
        target = Book(
            exchange="ZSE",
            ticker="CARD",
            server_time_ms=100_010,
            bids=[(9900, 50)],
            asks=[(9980, 120), (9981, 120)],
            received_at=now,
        )
        inv = Inventory(cash=10_000_000, positions={"ZSE-CARD": 0})

        orders = core.plan_orders("ZSE", target, inv, now)

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].side, "bid")
        self.assertEqual(orders[0].price, 9981)
        self.assertEqual(orders[0].quantity, 240)
        self.assertEqual(orders[0].reason, "CARD_CROSS_MEDIAN")

    def test_card_does_not_trade_without_enough_fresh_median_inputs(self):
        core = StrategyCore(EdgeConfig(home="HKEX", targets=("HKEX",), card_min_fresh=5))
        now = time.monotonic()
        for exchange in ["HKEX", "SSE", "JPX", "NSE"]:
            core.update_book(
                Book(
                    exchange=exchange,
                    ticker="CARD",
                    server_time_ms=100_000,
                    bids=[(10200, 50)],
                    asks=[(10204, 50)],
                    received_at=now - 5.0,
                )
            )
        target = Book(
            exchange="HKEX",
            ticker="CARD",
            server_time_ms=100_100,
            bids=[(9800, 100)],
            asks=[(9801, 100)],
            received_at=now,
        )
        inv = Inventory(cash=10_000_000, positions={"HKEX-CARD": 0})

        self.assertEqual(core.plan_orders("HKEX", target, inv, now), [])

    def test_add_order_response_updates_inventory_from_remembered_request_id(self):
        core = StrategyCore(EdgeConfig(home="NYSE", targets=("NYSE",)))
        inv = Inventory(cash=10_000_000)
        runner = ExchangeRunner("NYSE", core, inv, dry_run=True)
        request_id = runner.next_request_id("SIMP_FIXED_FAIR", "NYSE-SIMP")

        runner.handle_add_order_response(
            {
                "type": "add_order_response",
                "user_request_id": request_id,
                "success": True,
                "data": {
                    "immediate_inventory_change": 50,
                    "immediate_balance_change": -499_750,
                },
            }
        )

        self.assertEqual(inv.position("NYSE-SIMP"), 50)
        self.assertEqual(inv.cash, 9_500_250)


if __name__ == "__main__":
    unittest.main()
