import time
import unittest

from codex_bot import (
    BotConfig,
    FairValueEngine,
    MarketState,
    Opportunity,
    RiskManager,
    Side,
    StrategyEngine,
    TokenBucket,
    parse_exchanges,
    parse_orderbook_depth,
)


class MarketParsingTests(unittest.TestCase):
    def test_order_book_sorts_levels_and_computes_mid(self):
        book = parse_orderbook_depth(
            {"bids": {"10000": 20, "10002": 5}, "asks": {"10008": 5, "10005": 20}}
        )

        self.assertEqual(book.best_bid, 10002)
        self.assertEqual(book.best_ask, 10005)
        self.assertEqual(book.spread, 3)
        self.assertEqual(book.mid, 10003.5)

    def test_market_state_tracks_books_by_exchange_and_ticker(self):
        state = MarketState()
        state.apply_market_data(
            "NYSE",
            {
                "type": "market_data_update",
                "time": 1234,
                "orderbook_depths": {
                    "NYSE-CARD": {"bids": {"10000": 10}, "asks": {"10010": 10}}
                },
            },
            received_monotonic=10.0,
        )

        self.assertEqual(state.book("NYSE", "NYSE-CARD").best_bid, 10000)
        self.assertEqual(state.ticker_mids("CARD"), {"NYSE": 10005.0})


class FairValueTests(unittest.TestCase):
    def test_etf_fair_value_uses_constituent_mids_from_zse(self):
        state = MarketState()
        state.apply_market_data(
            "ZSE",
            {
                "time": 1000,
                "orderbook_depths": {
                    "ZSE-NGUP": {"bids": {"9998": 10}, "asks": {"10002": 10}},
                    "ZSE-KTST": {"bids": {"10098": 10}, "asks": {"10102": 10}},
                    "ZSE-XFR": {"bids": {"9898": 10}, "asks": {"9902": 10}},
                },
            },
            received_monotonic=10.0,
        )

        engine = FairValueEngine(state)

        self.assertEqual(engine.fair_value("ETFA3", "NYSE"), 10000.0)

    def test_same_ticker_fair_value_uses_median_across_fresh_venues(self):
        state = MarketState()
        state.apply_market_data(
            "NYSE",
            {
                "time": 1000,
                "orderbook_depths": {
                    "NYSE-CARD": {"bids": {"9990": 10}, "asks": {"10010": 10}},
                },
            },
            received_monotonic=10.0,
        )
        state.apply_market_data(
            "NASDAQ",
            {
                "time": 1000,
                "orderbook_depths": {
                    "NASDAQ-CARD": {"bids": {"10010": 10}, "asks": {"10030": 10}},
                },
            },
            received_monotonic=10.0,
        )
        state.apply_market_data(
            "HKEX",
            {
                "time": 1000,
                "orderbook_depths": {
                    "HKEX-CARD": {"bids": {"11000": 10}, "asks": {"11020": 10}},
                },
            },
            received_monotonic=10.0,
        )

        engine = FairValueEngine(state)

        self.assertEqual(engine.fair_value("CARD", "LSE"), 10020.0)


class StrategyTests(unittest.TestCase):
    def test_strategy_buys_underpriced_etf_with_ioc_order(self):
        state = MarketState()
        state.apply_market_data(
            "ZSE",
            {
                "time": 3000,
                "orderbook_depths": {
                    "ZSE-NGUP": {"bids": {"10000": 10}, "asks": {"10000": 10}},
                    "ZSE-KTST": {"bids": {"10000": 10}, "asks": {"10000": 10}},
                    "ZSE-XFR": {"bids": {"10000": 10}, "asks": {"10000": 10}},
                },
            },
            received_monotonic=20.0,
        )
        state.apply_market_data(
            "NYSE",
            {
                "time": 3000,
                "orderbook_depths": {
                    "NYSE-ETFA3": {"bids": {"9960": 50}, "asks": {"9970": 50}},
                },
            },
            received_monotonic=20.0,
        )

        strategy = StrategyEngine(BotConfig(live_trading=False, min_edge_cents=15))
        opportunities = strategy.find_opportunities(state, "NYSE", now_monotonic=20.1)

        best = opportunities[0]
        self.assertEqual(best.instrument_id, "NYSE-ETFA3")
        self.assertEqual(best.side, Side.BID)
        self.assertEqual(best.price, 9970)
        self.assertEqual(best.order_type, "ioc")
        self.assertGreater(best.score, 0)

    def test_strategy_sells_overpriced_cross_venue_ticker(self):
        state = MarketState()
        state.apply_market_data(
            "ZSE",
            {
                "time": 3000,
                "orderbook_depths": {
                    "ZSE-SIMP": {"bids": {"9998": 10}, "asks": {"10002": 10}},
                    "ZSE-CARD": {"bids": {"9998": 10}, "asks": {"10002": 10}},
                },
            },
            received_monotonic=20.0,
        )
        state.apply_market_data(
            "HKEX",
            {
                "time": 3000,
                "orderbook_depths": {
                    "HKEX-SIMP": {"bids": {"10050": 50}, "asks": {"10060": 50}},
                },
            },
            received_monotonic=20.0,
        )

        strategy = StrategyEngine(BotConfig(live_trading=False, min_edge_cents=20))
        opportunities = strategy.find_opportunities(state, "HKEX", now_monotonic=20.1)

        self.assertTrue(any(o.instrument_id == "HKEX-SIMP" and o.side == Side.ASK for o in opportunities))


class RiskTests(unittest.TestCase):
    def test_risk_blocks_new_orders_near_segment_end(self):
        config = BotConfig(no_new_risk_after_ms=590_000)
        risk = RiskManager(config)
        opportunity = Opportunity(
            exchange="NYSE",
            instrument_id="NYSE-CARD",
            side=Side.BID,
            price=10000,
            quantity=10,
            order_type="ioc",
            reason="test",
            edge_cents=30.0,
            score=30.0,
        )

        allowed, reason = risk.check(opportunity, exchange_time_ms=590_001)

        self.assertFalse(allowed)
        self.assertIn("segment end", reason)

    def test_risk_blocks_short_beyond_soft_floor(self):
        config = BotConfig(max_short=-50)
        risk = RiskManager(config)
        risk.positions["NYSE-CARD"] = -45
        opportunity = Opportunity(
            exchange="NYSE",
            instrument_id="NYSE-CARD",
            side=Side.ASK,
            price=10000,
            quantity=10,
            order_type="ioc",
            reason="test",
            edge_cents=30.0,
            score=30.0,
        )

        allowed, reason = risk.check(opportunity, exchange_time_ms=1000)

        self.assertFalse(allowed)
        self.assertIn("short", reason)


class UtilityTests(unittest.TestCase):
    def test_token_bucket_limits_burst_and_refills(self):
        bucket = TokenBucket(rate_per_second=2, capacity=2, now=100.0)

        self.assertTrue(bucket.try_acquire(now=100.0))
        self.assertTrue(bucket.try_acquire(now=100.0))
        self.assertFalse(bucket.try_acquire(now=100.0))
        self.assertTrue(bucket.try_acquire(now=100.5))

    def test_parse_exchanges_keeps_known_names_only(self):
        self.assertEqual(parse_exchanges("NYSE, nope, ZSE"), ["NYSE", "ZSE"])


if __name__ == "__main__":
    unittest.main()
