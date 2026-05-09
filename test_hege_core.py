import unittest

import hege


class HegeCoreTests(unittest.TestCase):
    def test_book_parsing_microprice_and_imbalance_are_integer_safe(self):
        book = hege.Book.from_depth(
            "NYSE-CARD",
            {
                "bids": {"10000": 25, "9990": 50},
                "asks": {"10010": 75, "10020": 50},
            },
            now_ms=123,
        )

        self.assertEqual(book.best_bid, 10000)
        self.assertEqual(book.best_ask, 10010)
        self.assertEqual(book.mid, 10005)
        self.assertEqual(book.microprice, 10002)
        self.assertEqual(book.imbalance_bps, -5000)
        self.assertEqual(book.spread, 10)

    def test_etf_fair_value_uses_constituent_mids_and_floor_integer_average(self):
        state = hege.MarketState()
        for ticker, mid in {"NGUP": 10001, "KTST": 10004, "XFR": 10006}.items():
            state.update_book(
                "ZSE",
                f"ZSE-{ticker}",
                {
                    "bids": {str(mid - 1): 50},
                    "asks": {str(mid + 1): 50},
                },
                now_ms=100,
            )

        fv = hege.compute_basket_fair_value(state, "ZSE", hege.ETF_BASKETS["ETFA3"])

        self.assertEqual(fv, 10003)

    def test_risk_manager_blocks_orders_that_cross_position_or_cash_limits(self):
        cfg = hege.Config.from_env(
            {
                "DRY_RUN": "1",
                "MAX_SYMBOL_POSITION": "120",
                "MAX_SYMBOL_SHORT": "50",
                "CASH_BUFFER_CENTS": "1000",
            }
        )
        risk = hege.RiskManager(cfg)
        risk.sync_inventory("ZSE", {"$": [0, -4_999_500], "ZSE-CARD": [0, 119]})

        self.assertFalse(risk.approve("ZSE", "ZSE-CARD", "bid", 10000, 2, reduce_only=False).ok)
        self.assertFalse(risk.approve("ZSE", "ZSE-CARD", "ask", 10000, 170, reduce_only=False).ok)
        self.assertTrue(risk.approve("ZSE", "ZSE-CARD", "ask", 10000, 10, reduce_only=True).ok)

    def test_candidate_scores_do_not_penalize_far_venues_when_physical_latency_removed(self):
        cfg = hege.Config.from_env({"BOT_HOME": "NYSE"})
        signal = hege.Signal(
            strategy="unit",
            exchange="HKEX",
            instrument_id="HKEX-CARD",
            side="bid",
            price=10020,
            quantity=5,
            edge_cents=40,
            confidence_bps=8000,
            reduce_only=False,
            reason="far venue",
        )
        near = hege.Signal(
            strategy="unit",
            exchange="NASDAQ",
            instrument_id="NASDAQ-CARD",
            side="bid",
            price=10020,
            quantity=5,
            edge_cents=40,
            confidence_bps=8000,
            reduce_only=False,
            reason="near venue",
        )

        self.assertEqual(hege.score_signal(near, cfg), hege.score_signal(signal, cfg))

    def test_legacy_latency_mode_can_still_penalize_far_venues(self):
        cfg = hege.Config.from_env({"BOT_HOME": "NYSE", "PHYSICAL_LATENCY_REMOVED": "0"})
        signal = hege.Signal(
            strategy="unit",
            exchange="HKEX",
            instrument_id="HKEX-CARD",
            side="bid",
            price=10020,
            quantity=5,
            edge_cents=40,
            confidence_bps=8000,
            reduce_only=False,
            reason="far venue",
        )
        near = hege.Signal(
            strategy="unit",
            exchange="NASDAQ",
            instrument_id="NASDAQ-CARD",
            side="bid",
            price=10020,
            quantity=5,
            edge_cents=40,
            confidence_bps=8000,
            reduce_only=False,
            reason="near venue",
        )

        self.assertGreater(hege.score_signal(near, cfg), hege.score_signal(signal, cfg))


if __name__ == "__main__":
    unittest.main()
