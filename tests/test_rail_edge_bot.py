import time
import unittest

from rail_edge_bot import (
    ExchangeState,
    PendingOrder,
    RailConfig,
    RailEdgeStrategy,
    TokenBucket,
    build_add_order,
    default_rail_configs,
)


class RailEdgeStrategyTests(unittest.TestCase):
    def test_flat_account_places_capped_bid_and_short_ask(self):
        config = RailConfig(
            exchange="NASDAQ",
            symbol="CARD",
            low_bid_price=7001,
            high_ask_price=11000,
            close_bid_min=10100,
            close_ask_max=10050,
        )
        state = ExchangeState(exchange="NASDAQ")
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(state, depth=None, now_ms=1_000)
        bids = [o for o in orders if o["side"] == "bid"]
        asks = [o for o in orders if o["side"] == "ask"]

        # bid capped at lot_size=200
        self.assertEqual(len(bids), 1)
        self.assertEqual((bids[0]["price"], bids[0]["quantity"], bids[0]["order_type"]),
                         (7001, 200, "limit"))
        # ask exists even at flat position because allow_short_rail is on
        self.assertEqual(len(asks), 1)
        self.assertEqual((asks[0]["price"], asks[0]["quantity"], asks[0]["order_type"]),
                         (11000, 200, "limit"))

    def test_flat_account_with_short_rail_disabled_places_no_ask(self):
        config = RailConfig(
            exchange="NASDAQ",
            symbol="CARD",
            low_bid_price=7001,
            high_ask_price=11000,
            close_bid_min=10100,
            close_ask_max=10050,
            allow_short_rail=False,
        )
        state = ExchangeState(exchange="NASDAQ")
        strategy = RailEdgeStrategy(config)
        orders = strategy.plan_orders(state, depth=None, now_ms=1_000)
        self.assertEqual([o for o in orders if o["side"] == "ask"], [])

    def test_long_inventory_places_high_rail_ask_capped_at_lot_size(self):
        config = default_rail_configs()["NASDAQ"]
        state = ExchangeState(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = 350
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(state, depth=None, now_ms=1_500)
        asks = [order for order in orders if order["side"] == "ask"]

        # cap at lot_size; one ticket per tick (planner re-arms on next tick)
        self.assertEqual(len(asks), 1)
        self.assertEqual(asks[0]["price"], 11000)
        self.assertEqual(asks[0]["quantity"], config.lot_size)

    def test_passive_rail_fill_updates_position_cash_and_remaining_order(self):
        config = default_rail_configs()["NASDAQ"]
        state = ExchangeState(exchange="NASDAQ")
        state.track_order(
            local_id="rail-bid-1",
            order_id=42,
            instrument="NASDAQ-CARD",
            side="bid",
            price=7001,
            quantity=500,
            role="rail",
        )
        strategy = RailEdgeStrategy(config)

        strategy.apply_trade_event(
            state,
            {
                "passiveOrderID": 42,
                "activeOrderID": 99,
                "quantity": 158,
                "price": 7001,
                "instrumentID": "NASDAQ-CARD",
            },
        )

        self.assertEqual(state.position("NASDAQ-CARD"), 158)
        self.assertEqual(state.cash, 10_000_000 - 158 * 7001)
        self.assertEqual(state.live_orders[42].remaining, 342)

    def test_long_position_closes_only_against_visible_bids_above_threshold(self):
        config = default_rail_configs()["NASDAQ"]
        state = ExchangeState(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = 620
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(
            state,
            depth={
                "bids": {"10220": 100, "10100": 250, "10099": 900},
                "asks": {"10240": 50},
            },
            now_ms=2_000,
        )

        close_orders = [order for order in orders if order["order_type"] == "ioc"]
        limit_asks = [
            order
            for order in orders
            if order["side"] == "ask" and order["order_type"] == "limit"
        ]
        self.assertEqual(len(close_orders), 1)
        self.assertEqual(close_orders[0]["side"], "ask")
        self.assertEqual(close_orders[0]["price"], config.close_bid_min)
        # close fills against bids at or above close_bid_min, capped at pos
        expected_close_qty = min(
            620,
            sum(int(q) for p, q in {"10220": 100, "10100": 250, "10099": 900}.items()
                if int(p) >= config.close_bid_min),
        )
        self.assertEqual(close_orders[0]["quantity"], expected_close_qty)
        # one ticket per tick, capped at lot_size; the planner re-arms next tick
        self.assertEqual(sum(order["quantity"] for order in limit_asks), config.lot_size)

    def test_short_position_closes_only_against_visible_asks_below_threshold(self):
        config = default_rail_configs()["NASDAQ"]
        state = ExchangeState(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = -120
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(
            state,
            depth={
                "bids": {"10000": 50},
                "asks": {"10040": 25, "10050": 40, "10051": 500},
            },
            now_ms=3_000,
        )

        close_orders = [order for order in orders if order["order_type"] == "ioc"]
        self.assertEqual(len(close_orders), 1)
        self.assertEqual(close_orders[0]["side"], "bid")
        self.assertEqual(close_orders[0]["price"], config.close_ask_max)
        expected_close_qty = min(
            120,
            sum(int(q) for p, q in {"10040": 25, "10050": 40, "10051": 500}.items()
                if int(p) <= config.close_ask_max),
        )
        self.assertEqual(close_orders[0]["quantity"], expected_close_qty)

    def test_cash_and_pending_bids_limit_new_rail_bid_quantity_to_positive_available_cash(self):
        config = default_rail_configs()["NASDAQ"]
        state = ExchangeState(exchange="NASDAQ", cash=10_000_000)
        state.track_order(
            local_id="old-bid",
            order_id=7,
            instrument="NASDAQ-CARD",
            side="bid",
            price=7001,
            quantity=100,
            role="rail",
        )
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(state, depth=None, now_ms=4_000)
        bids = [order for order in orders if order["side"] == "bid"]

        # cash floor is -50k; available = cash - pending - CASH_FLOOR
        self.assertEqual(len(bids), 1)
        self.assertLessEqual((100 + bids[0]["quantity"]) * 7001,
                             10_000_000 - (-5_000_000))

    def test_cash_at_floor_does_not_place_more_rail_bids(self):
        # the bot deploys capital down to CASH_FLOOR (-$50k); below that the
        # exchange would reject the bid for insufficient balance.
        config = default_rail_configs()["NASDAQ"]
        state = ExchangeState(exchange="NASDAQ", cash=-5_000_000)
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(state, depth=None, now_ms=4_500)
        bids = [order for order in orders if order["side"] == "bid"]

        self.assertEqual(bids, [])

    def test_inflight_rail_orders_count_as_reserved_capacity(self):
        config = default_rail_configs()["NASDAQ"]
        state = ExchangeState(exchange="NASDAQ")
        state.track_inflight(
            PendingOrder(
                local_id="pending-rail",
                instrument="NASDAQ-CARD",
                side="bid",
                price=7001,
                quantity=2000,
                order_type="limit",
                role="rail",
            )
        )
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(state, depth=None, now_ms=5_000)
        bids = [order for order in orders if order["side"] == "bid"]

        self.assertEqual(bids, [])


class ForceCloseTests(unittest.TestCase):
    def test_long_inventory_force_closes_with_market_after_timeout(self):
        config = default_rail_configs()["NASDAQ"]
        state = ExchangeState(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = 200
        state.last_flat_ms["NASDAQ-CARD"] = 1_000
        strategy = RailEdgeStrategy(config)

        # before timeout: no force_close (only the polite IOC may fire)
        orders = strategy.plan_orders(state, depth=None,
                                      now_ms=1_000 + config.force_close_after_ms - 1)
        self.assertFalse(any(o["role"] == "force_close" for o in orders))

        # past timeout: market sell fires
        orders = strategy.plan_orders(state, depth=None,
                                      now_ms=1_000 + config.force_close_after_ms + 1)
        force = [o for o in orders if o["role"] == "force_close"]
        self.assertEqual(len(force), 1)
        self.assertEqual((force[0]["side"], force[0]["order_type"], force[0]["quantity"]),
                         ("ask", "market", 200))

    def test_short_inventory_force_closes_with_market_after_timeout(self):
        config = default_rail_configs()["NASDAQ"]
        state = ExchangeState(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = -150
        state.last_flat_ms["NASDAQ-CARD"] = 0
        strategy = RailEdgeStrategy(config)
        orders = strategy.plan_orders(state, depth=None,
                                      now_ms=config.force_close_after_ms + 10)
        force = [o for o in orders if o["role"] == "force_close"]
        self.assertEqual(len(force), 1)
        self.assertEqual((force[0]["side"], force[0]["order_type"], force[0]["quantity"]),
                         ("bid", "market", 150))


class ProtocolTests(unittest.TestCase):
    def test_build_add_order_uses_integer_protocol_fields_and_future_expiry(self):
        before = int(time.time() * 1000)

        message = build_add_order(
            request_id="x-1",
            instrument_id="NASDAQ-CARD",
            side="bid",
            price=7001,
            quantity=12,
            order_type="limit",
            ttl_ms=10_000,
        )

        self.assertEqual(message["type"], "add_order")
        self.assertEqual(message["order_type"], "limit")
        self.assertEqual(message["price"], 7001)
        self.assertIsInstance(message["expiry"], int)
        self.assertGreaterEqual(message["expiry"], before + 9_000)

    def test_token_bucket_allows_burst_then_refills(self):
        bucket = TokenBucket(rate_per_second=2, burst=2)

        self.assertTrue(bucket.try_take(now=0.0))
        self.assertTrue(bucket.try_take(now=0.0))
        self.assertFalse(bucket.try_take(now=0.0))
        self.assertTrue(bucket.try_take(now=0.5))
        self.assertFalse(bucket.try_take(now=0.5))


if __name__ == "__main__":
    unittest.main()
