import time
import unittest

from novel_edge_bot import (
    BotConfig,
    FairValueEngine,
    MarketState,
    RiskManager,
    Side,
    StrategyEngine,
    TokenBucket,
    build_order_message,
    parse_orderbook_depth,
)


class OrderBookTests(unittest.TestCase):
    def test_parse_orderbook_depth_sorts_string_prices_and_computes_mid(self):
        book = parse_orderbook_depth(
            {
                "bids": {"10001": 25, "10003": 10, "9999": 50},
                "asks": {"10009": 30, "10007": 40, "10012": 10},
            }
        )

        self.assertEqual(book.best_bid, 10003)
        self.assertEqual(book.best_ask, 10007)
        self.assertEqual(book.spread, 4)
        self.assertEqual(book.mid, 10005.0)
        self.assertEqual(book.bid_quantity_at_or_better(10001), 35)
        self.assertEqual(book.ask_quantity_at_or_better(10009), 70)


class FairValueTests(unittest.TestCase):
    def test_etf_fair_value_prefers_fresh_zse_constituent_mids(self):
        state = MarketState()
        state.apply_market_data(
            "ZSE",
            {
                "time": 1200,
                "orderbook_depths": {
                    "ZSE-NGUP": quote(9998, 10002),
                    "ZSE-KTST": quote(10098, 10102),
                    "ZSE-XFR": quote(9898, 9902),
                },
            },
            received_monotonic=10.0,
        )

        fair = FairValueEngine(state, BotConfig()).fair_value("ETFA3", "NYSE", now=10.1)

        self.assertEqual(fair, 10000.0)

    def test_lead_lag_adjusts_same_ticker_value_with_recent_remote_move(self):
        state = MarketState()
        state.apply_market_data(
            "ZSE",
            {"time": 1000, "orderbook_depths": {"ZSE-CARD": quote(10000, 10010)}},
            received_monotonic=20.0,
        )
        state.apply_market_data(
            "ZSE",
            {"time": 1100, "orderbook_depths": {"ZSE-CARD": quote(10100, 10110)}},
            received_monotonic=20.1,
        )
        state.apply_market_data(
            "NYSE",
            {"time": 1100, "orderbook_depths": {"NYSE-CARD": quote(10000, 10010)}},
            received_monotonic=20.1,
        )

        fair = FairValueEngine(
            state, BotConfig(lead_lag_weight=0.35, max_book_age_seconds=2.0)
        ).fair_value("CARD", "NYSE", now=20.15)

        self.assertGreater(fair, 10005.0)


class StrategyTests(unittest.TestCase):
    def test_zse_basket_arb_uses_integer_hedge_ratio_for_overpriced_etf(self):
        state = MarketState()
        state.apply_market_data(
            "ZSE",
            {
                "time": 2500,
                "orderbook_depths": {
                    "ZSE-ETFA3": depth(10050, 30, 10060, 30),
                    "ZSE-NGUP": depth(9998, 50, 10002, 50),
                    "ZSE-KTST": depth(9998, 50, 10002, 50),
                    "ZSE-XFR": depth(9998, 50, 10002, 50),
                },
            },
            received_monotonic=30.0,
        )

        groups = StrategyEngine(
            BotConfig(min_basket_edge_cents=20, max_basket_units=2)
        ).find_zse_basket_arbs(state, now=30.1)

        best = groups[0]
        self.assertEqual(best.kind, "zse_basket")
        self.assertGreaterEqual(best.edge_cents, 48)
        self.assertEqual(best.legs[0].instrument_id, "ZSE-ETFA3")
        self.assertEqual(best.legs[0].side, Side.ASK)
        self.assertEqual(best.legs[0].quantity, 6)
        component_legs = best.legs[1:]
        self.assertEqual({leg.side for leg in component_legs}, {Side.BID})
        self.assertEqual({leg.quantity for leg in component_legs}, {2})

    def test_cross_venue_pair_buys_cheap_venue_and_sells_rich_venue(self):
        state = MarketState()
        state.apply_market_data(
            "NYSE",
            {"time": 5000, "orderbook_depths": {"NYSE-CARD": depth(9980, 50, 9990, 50)}},
            received_monotonic=40.0,
        )
        state.apply_market_data(
            "HKEX",
            {"time": 5000, "orderbook_depths": {"HKEX-CARD": depth(10080, 50, 10090, 50)}},
            received_monotonic=40.0,
        )

        groups = StrategyEngine(
            BotConfig(min_pair_edge_cents=60, pair_quantity=7)
        ).find_cross_venue_pairs(state, now=40.1)

        best = groups[0]
        self.assertEqual(best.kind, "cross_pair")
        self.assertEqual([(leg.exchange, leg.side, leg.price) for leg in best.legs], [
            ("NYSE", Side.BID, 9990),
            ("HKEX", Side.ASK, 10080),
        ])
        self.assertEqual({leg.quantity for leg in best.legs}, {7})

    def test_single_leg_dislocation_emits_ioc_order_with_reasonable_quantity(self):
        state = MarketState()
        state.apply_market_data(
            "ZSE",
            {
                "time": 7000,
                "orderbook_depths": {
                    "ZSE-NGUP": quote(10000, 10000),
                    "ZSE-KTST": quote(10000, 10000),
                    "ZSE-XFR": quote(10000, 10000),
                },
            },
            received_monotonic=50.0,
        )
        state.apply_market_data(
            "NYSE",
            {"time": 7000, "orderbook_depths": {"NYSE-ETFA3": depth(9950, 50, 9960, 12)}},
            received_monotonic=50.0,
        )

        groups = StrategyEngine(
            BotConfig(min_single_edge_cents=25, single_leg_quantity=20)
        ).find_single_leg_dislocations(state, "NYSE", now=50.1)

        leg = groups[0].legs[0]
        self.assertEqual(leg.instrument_id, "NYSE-ETFA3")
        self.assertEqual(leg.side, Side.BID)
        self.assertEqual(leg.price, 9960)
        self.assertEqual(leg.quantity, 12)


class RiskAndProtocolTests(unittest.TestCase):
    def test_risk_blocks_groups_near_segment_end_and_short_soft_floor(self):
        group = StrategyEngine(BotConfig()).make_single_leg_group(
            exchange="NYSE",
            instrument_id="NYSE-CARD",
            side=Side.ASK,
            price=10000,
            quantity=10,
            edge_cents=50,
            reason="test",
        )
        risk = RiskManager(BotConfig(max_short=-20, no_new_risk_after_ms=590_000))

        allowed, reason = risk.check_group(group, exchange_time_ms=590_001)
        self.assertFalse(allowed)
        self.assertIn("segment end", reason)

        risk.positions["NYSE-CARD"] = -15
        allowed, reason = risk.check_group(group, exchange_time_ms=1000)
        self.assertFalse(allowed)
        self.assertIn("short", reason)

    def test_token_bucket_limits_bursts_and_refills(self):
        bucket = TokenBucket(rate_per_second=2, capacity=2, now=100.0)

        self.assertTrue(bucket.try_acquire(now=100.0))
        self.assertTrue(bucket.try_acquire(now=100.0))
        self.assertFalse(bucket.try_acquire(now=100.0))
        self.assertTrue(bucket.try_acquire(now=100.5))

    def test_build_order_message_uses_api_side_ioc_and_unix_expiry(self):
        now_ms = int(time.time() * 1000)
        msg = build_order_message(
            request_id="r1",
            instrument_id="NYSE-CARD",
            side=Side.BID,
            price=10001,
            quantity=3,
            expiry_ms=now_ms + 5000,
        )

        self.assertEqual(msg["type"], "add_order")
        self.assertEqual(msg["order_type"], "ioc")
        self.assertEqual(msg["side"], "bid")
        self.assertEqual(msg["price"], 10001)
        self.assertGreater(msg["expiry"], now_ms)


def quote(bid: int, ask: int) -> dict[str, dict[str, int]]:
    return depth(bid, 50, ask, 50)


def depth(bid: int, bid_qty: int, ask: int, ask_qty: int) -> dict[str, dict[str, int]]:
    return {
        "bids": {str(bid): bid_qty, str(bid - 10): 50, str(bid - 20): 50},
        "asks": {str(ask): ask_qty, str(ask + 10): 50, str(ask + 20): 50},
    }


if __name__ == "__main__":
    unittest.main()
