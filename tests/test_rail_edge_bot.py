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


def synced_state(exchange: str, **kwargs) -> ExchangeState:
    """Tests construct ExchangeState directly without going through the
    inventory/pending-orders message path. Production code skips planning
    until is_synced()==True, so flip both flags here so the tests can
    exercise the planner."""
    state = ExchangeState(exchange=exchange, **kwargs)
    state.inventory_synced = True
    state.pending_orders_synced = True
    return state


class RailEdgeStrategyTests(unittest.TestCase):
    def test_flat_account_places_only_cash_backed_bid_at_lot_size(self):
        # Default config has allow_short_rail=False — server rejects resting
        # asks beyond owned shares with "Insufficient inventory".
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(state, depth=None, now_ms=1_000)
        bids = [o for o in orders if o["side"] == "bid"]
        asks = [o for o in orders if o["side"] == "ask"]
        self.assertEqual(len(bids), 1)
        self.assertEqual((bids[0]["price"], bids[0]["quantity"]),
                         (config.low_bid_price, config.lot_size))
        self.assertEqual(asks, [])

    def test_with_high_rail_enabled_an_ask_is_planned(self):
        # both flags must flip on: enable_high_rail to put the rail up at all,
        # and allow_short_rail to size against MAX_SHORT when flat.
        config = default_rail_configs()["NASDAQ"]
        config = RailConfig(
            exchange=config.exchange, symbol=config.symbol,
            low_bid_price=config.low_bid_price, high_ask_price=config.high_ask_price,
            close_bid_min=config.close_bid_min, close_ask_max=config.close_ask_max,
            lot_size=config.lot_size,
            allow_short_rail=True, enable_high_rail=True,
            force_close_after_ms=config.force_close_after_ms,
        )
        state = synced_state(exchange="NASDAQ")
        strategy = RailEdgeStrategy(config)
        orders = strategy.plan_orders(state, depth=None, now_ms=1_000)
        asks = [o for o in orders if o["side"] == "ask"]
        self.assertEqual(len(asks), 1)
        self.assertEqual(asks[0]["price"], config.high_ask_price)

    def test_bid_does_not_stack_above_lot_size(self):
        # The previous-tick bid (still inflight or resting) must NOT trigger
        # a duplicate bid. lot_size is a target, not a per-tick add.
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
        state.track_order(
            local_id="resting", order_id=1,
            instrument="NASDAQ-CARD", side="bid",
            price=config.low_bid_price, quantity=config.lot_size,
            role="rail",
        )
        strategy = RailEdgeStrategy(config)
        orders = strategy.plan_orders(state, depth=None, now_ms=2_000)
        bids = [o for o in orders if o["side"] == "bid"]
        self.assertEqual(bids, [])

    def test_long_inventory_places_high_rail_ask_when_explicitly_enabled(self):
        # default leaves enable_high_rail=False — the rail ask would
        # block close/force_close from putting up exit orders. Test the
        # opt-in path here.
        base = default_rail_configs()["NASDAQ"]
        config = RailConfig(
            exchange=base.exchange, symbol=base.symbol,
            low_bid_price=base.low_bid_price, high_ask_price=base.high_ask_price,
            close_bid_min=base.close_bid_min, close_ask_max=base.close_ask_max,
            lot_size=base.lot_size,
            allow_short_rail=base.allow_short_rail, enable_high_rail=True,
            force_close_after_ms=base.force_close_after_ms,
        )
        state = synced_state(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = 350
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(state, depth=None, now_ms=1_500)
        asks = [order for order in orders if order["side"] == "ask"]
        self.assertEqual(len(asks), 1)
        self.assertEqual(asks[0]["price"], config.high_ask_price)
        self.assertEqual(asks[0]["quantity"], config.lot_size)

    def test_passive_rail_fill_updates_position_cash_and_remaining_order(self):
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
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
        state = synced_state(exchange="NASDAQ")
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
        # With allow_short_rail=False (default — server rejects resting
        # shorts), the close reserves 620 against the 620 owned shares;
        # that leaves zero owned capacity for an additional rail ask.
        self.assertEqual(sum(order["quantity"] for order in limit_asks), 0)

    def test_short_position_closes_only_against_visible_asks_below_threshold(self):
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
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
        state = synced_state(exchange="NASDAQ", cash=10_000_000)
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
        state = synced_state(exchange="NASDAQ", cash=-5_000_000)
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(state, depth=None, now_ms=4_500)
        bids = [order for order in orders if order["side"] == "bid"]

        self.assertEqual(bids, [])

    def test_inflight_rail_orders_count_as_reserved_capacity(self):
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
        state.track_inflight(
            PendingOrder(
                local_id="pending-rail",
                instrument="NASDAQ-CARD",
                side="bid",
                price=default_rail_configs()["NASDAQ"].low_bid_price,
                quantity=2000,
                order_type="limit",
                role="rail",
            )
        )
        strategy = RailEdgeStrategy(config)

        orders = strategy.plan_orders(state, depth=None, now_ms=5_000)
        bids = [order for order in orders if order["side"] == "bid"]

        self.assertEqual(bids, [])


class CashReservationTests(unittest.TestCase):
    def test_server_reserved_cash_blocks_new_bids_against_hard_floor(self):
        # When the server tells us cash=$100k of which $140k is reserved
        # (impossible in practice but mathematically equivalent to "we're
        # already past the floor"), we must NOT issue another bid. The
        # exact-sizing path subtracts reserved_cash directly from free_cash.
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
        # server reserved == total + |hard floor| → free cash exactly at floor
        state.apply_inventory({"$": [15_000_000, 10_000_000]})
        strategy = RailEdgeStrategy(config)
        orders = strategy.plan_orders(state, depth=None, now_ms=1_000)
        bids = [o for o in orders if o["side"] == "bid"]
        self.assertEqual(bids, [])

    def test_inventory_total_field_is_what_drives_cash(self):
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
        # data["$"] = [reserved, total]; we trust total
        state.apply_inventory({"$": [3_000_000, 8_000_000]})
        self.assertEqual(state.cash, 8_000_000)


class ForceCloseTests(unittest.TestCase):
    def test_long_inventory_force_closes_with_market_after_timeout(self):
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = 200
        state.last_flat_ms["NASDAQ-CARD"] = 1_000
        strategy = RailEdgeStrategy(config)

        # before timeout: no force_close (only the polite IOC may fire)
        orders = strategy.plan_orders(state, depth=None,
                                      now_ms=1_000 + config.force_close_after_ms - 1)
        self.assertFalse(any(o["role"] == "force_close" for o in orders))

        # past timeout: aggressive IOC fires, priced just above our rail
        # bid so the unwind cannot self-trade against it.
        orders = strategy.plan_orders(state, depth=None,
                                      now_ms=1_000 + config.force_close_after_ms + 1)
        force = [o for o in orders if o.get("role") == "force_close"]
        self.assertEqual(len(force), 1)
        self.assertEqual((force[0]["side"], force[0]["order_type"], force[0]["quantity"]),
                         ("ask", "ioc", 200))
        self.assertEqual(force[0]["price"], config.low_bid_price + 1)

    def test_short_inventory_force_closes_with_market_after_timeout(self):
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = -150
        state.last_flat_ms["NASDAQ-CARD"] = 0
        strategy = RailEdgeStrategy(config)
        orders = strategy.plan_orders(state, depth=None,
                                      now_ms=config.force_close_after_ms + 10)
        force = [o for o in orders if o.get("role") == "force_close"]
        self.assertEqual(len(force), 1)
        self.assertEqual((force[0]["side"], force[0]["order_type"], force[0]["quantity"]),
                         ("bid", "ioc", 150))
        self.assertEqual(force[0]["price"], config.high_ask_price - 1)

    def test_force_close_ioc_price_cannot_self_trade_with_rail_bid(self):
        # Rail bid is at low_bid_price (e.g. $50). The force-close IOC
        # ASK is set at low_bid_price + 1 ($50.01) so it never crosses
        # our own bid; only MM/other-team bids at higher prices match.
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = 200
        state.last_flat_ms["NASDAQ-CARD"] = 0
        state.track_order(
            local_id="rail-b", order_id=77,
            instrument="NASDAQ-CARD", side="bid",
            price=config.low_bid_price, quantity=200, role="rail",
        )
        strategy = RailEdgeStrategy(config)
        orders = strategy.plan_orders(state, depth=None,
                                      now_ms=config.force_close_after_ms + 1)
        force = [o for o in orders if o.get("role") == "force_close"]
        self.assertEqual(len(force), 1)
        self.assertEqual(force[0]["side"], "ask")
        # ask price must be STRICTLY above our rail bid so the order
        # cannot match against ourselves.
        self.assertGreater(force[0]["price"], config.low_bid_price)

    def test_force_close_ioc_price_cannot_self_trade_with_rail_ask(self):
        config = default_rail_configs()["NASDAQ"]
        state = synced_state(exchange="NASDAQ")
        state.positions["NASDAQ-CARD"] = -100
        state.last_flat_ms["NASDAQ-CARD"] = 0
        state.track_order(
            local_id="rail-a", order_id=99,
            instrument="NASDAQ-CARD", side="ask",
            price=config.high_ask_price, quantity=100, role="rail",
        )
        strategy = RailEdgeStrategy(config)
        orders = strategy.plan_orders(state, depth=None,
                                      now_ms=config.force_close_after_ms + 1)
        force = [o for o in orders if o.get("role") == "force_close"]
        self.assertEqual(len(force), 1)
        self.assertEqual(force[0]["side"], "bid")
        self.assertLess(force[0]["price"], config.high_ask_price)


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
