import unittest

from edge_max import AggressiveConfig, AggressiveEdgeEngine, Book, OrderIntent


class EdgeMaxCoreTests(unittest.TestCase):
    def make_engine(self, location="ZSE"):
        return AggressiveEdgeEngine(AggressiveConfig(location=location, trade_all=False))

    def test_opening_window_places_one_dollar_bid_trap(self):
        engine = self.make_engine("ZSE")
        book = Book.from_lists(
            exchange="ZSE",
            instrument_id="ZSE-CARD",
            time_ms=1000,
            bids=[(10000, 50), (9999, 100), (9998, 150)],
            asks=[(10003, 50), (10004, 100), (10005, 150)],
        )

        intents = engine.on_book(book, server_time_ms=1000)

        self.assertTrue(
            any(
                i.side == "bid"
                and i.order_type == "limit"
                and i.price == 100
                and i.quantity == 2000
                and "opening-dollar" in i.reason
                for i in intents
            )
        )

    def test_passive_bid_fill_unwinds_with_profit_protected_ask_ioc(self):
        engine = self.make_engine("ZSE")
        book = Book.from_lists(
            exchange="ZSE",
            instrument_id="ZSE-CARD",
            time_ms=5000,
            bids=[(10000, 50), (9999, 100), (9998, 150)],
            asks=[(10003, 50), (10004, 100), (10005, 150)],
        )
        engine.on_book(book, server_time_ms=5000)

        intents = engine.unwind_after_fill("ZSE-CARD", "bid", 100, 175)

        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].side, "ask")
        self.assertEqual(intents[0].order_type, "ioc")
        self.assertEqual(intents[0].price, 101)
        self.assertEqual(intents[0].quantity, 175)

    def test_cross_median_sweeper_buys_stale_ask_on_close_exchange(self):
        engine = self.make_engine("NYSE")
        engine.on_book(
            Book.from_lists(
                exchange="NASDAQ",
                instrument_id="NASDAQ-CARD",
                time_ms=10000,
                bids=[(10000, 100)],
                asks=[(10004, 100)],
            ),
            server_time_ms=10000,
        )
        stale = Book.from_lists(
            exchange="NYSE",
            instrument_id="NYSE-CARD",
            time_ms=10001,
            bids=[(9470, 100)],
            asks=[(9500, 40), (9510, 60), (9520, 100)],
        )

        intents = engine.on_book(stale, server_time_ms=10001)

        sweep = [i for i in intents if i.order_type == "ioc" and i.side == "bid"]
        self.assertTrue(sweep)
        self.assertEqual(sweep[0].instrument_id, "NYSE-CARD")
        self.assertGreaterEqual(sweep[0].quantity, 40)
        self.assertLessEqual(sweep[0].price, 9900)

    def test_non_close_exchange_only_gets_opening_dollar_trap_by_default(self):
        engine = self.make_engine("ZSE")
        book = Book.from_lists(
            exchange="HKEX",
            instrument_id="HKEX-CARD",
            time_ms=1000,
            bids=[(10000, 50)],
            asks=[(10003, 50)],
        )

        opening = engine.on_book(book, server_time_ms=1000)
        later = engine.on_book(book, server_time_ms=20_000)

        self.assertEqual([intent.price for intent in opening], [100])
        self.assertEqual(later, [])

    def test_profitable_long_position_gets_flattened_with_ioc(self):
        engine = self.make_engine("ZSE")
        buy = OrderIntent("ZSE", "ZSE-CARD", "bid", 100, "ioc", "test-buy", 9000)
        engine.note_request("buy-1", buy)
        engine.on_order_response(
            "buy-1",
            True,
            {"immediate_inventory_change": 100, "immediate_balance_change": -900000},
        )
        book = Book.from_lists(
            exchange="ZSE",
            instrument_id="ZSE-CARD",
            time_ms=20_000,
            bids=[(9100, 80), (9099, 80)],
            asks=[(9110, 50)],
        )

        intents = engine.on_book(book, server_time_ms=20_000)

        exits = [intent for intent in intents if intent.reason.startswith("exit-long")]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0].side, "ask")
        self.assertEqual(exits[0].price, 9001)
        self.assertEqual(exits[0].quantity, 100)

    def test_bid_intent_is_clamped_to_remaining_position_room(self):
        engine = self.make_engine("ZSE")
        engine.positions["ZSE-CARD"] = 1990
        intent = OrderIntent("ZSE", "ZSE-CARD", "bid", 100, "ioc", "test-buy", 10000)

        clamped = engine.clamp_intent(intent)

        self.assertIsNotNone(clamped)
        self.assertEqual(clamped.quantity, 10)

    def test_bid_intent_accounts_for_cash_reserved_by_pending_bids(self):
        engine = self.make_engine("ZSE")
        expensive = OrderIntent("ZSE", "ZSE-CARD", "bid", 1000, "limit", "reserve", 10000)
        engine.note_request("reserve-1", expensive)
        engine.on_order_response("reserve-1", True, {"order_id": 123})
        intent = OrderIntent("ZSE", "ZSE-SIMP", "bid", 1000, "ioc", "test-buy", 10000)

        clamped = engine.clamp_intent(intent)

        self.assertIsNotNone(clamped)
        self.assertEqual(clamped.quantity, 500)


if __name__ == "__main__":
    unittest.main()
