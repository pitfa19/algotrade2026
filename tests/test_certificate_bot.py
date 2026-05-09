import unittest

from certificate_bot import (
    ETF_BASKETS,
    Side,
    build_certificate_orders,
    find_certificates,
    parse_orderbook_depth,
)


class CertificateMathTests(unittest.TestCase):
    def test_finds_depth_aware_buy_etf_sell_basket_certificate(self):
        books = {
            "ZSE-ETFA": parse_orderbook_depth(
                depth(
                    bids=[(12503, 3), (12501, 30), (12499, 5)],
                    asks=[(12507, 2), (12510, 67), (12513, 150)],
                )
            ),
            "ZSE-NGUP": parse_orderbook_depth(depth(bids=[(11658, 41)], asks=[(11664, 50)])),
            "ZSE-OIT": parse_orderbook_depth(depth(bids=[(10573, 50)], asks=[(10579, 73)])),
            "ZSE-KTST": parse_orderbook_depth(depth(bids=[(12787, 50)], asks=[(12790, 2)])),
            "ZSE-FSR": parse_orderbook_depth(depth(bids=[(12160, 50)], asks=[(12166, 40)])),
            "ZSE-JZRO": parse_orderbook_depth(depth(bids=[(18636, 32)], asks=[(18644, 50)])),
            "ZSE-XFR": parse_orderbook_depth(depth(bids=[(11017, 22)], asks=[(11021, 50)])),
        }

        certificates = find_certificates("ZSE", books, min_credit_cents=1)

        self.assertEqual(len(certificates), 1)
        cert = certificates[0]
        self.assertEqual(cert.exchange, "ZSE")
        self.assertEqual(cert.etf, "ETFA")
        self.assertEqual(cert.side, Side.BUY_ETF_SELL_BASKET)
        self.assertEqual(cert.credit_cents, 1777)
        self.assertEqual(cert.etf_quantity, len(ETF_BASKETS["ETFA"]))
        self.assertEqual(cert.component_quantity, 1)
        self.assertEqual(cert.entry_notional_cents, 1777)

    def test_rejects_certificate_when_depth_cannot_fill_integer_package(self):
        books = {
            "NYSE-ETFA3": parse_orderbook_depth(depth(bids=[(10050, 2)], asks=[(10060, 2)])),
            "NYSE-NGUP": parse_orderbook_depth(depth(bids=[(10100, 50)], asks=[(10110, 50)])),
            "NYSE-KTST": parse_orderbook_depth(depth(bids=[(10100, 50)], asks=[(10110, 50)])),
            "NYSE-XFR": parse_orderbook_depth(depth(bids=[(10100, 50)], asks=[(10110, 50)])),
        }

        certificates = find_certificates("NYSE", books, min_credit_cents=1)

        self.assertEqual(certificates, [])

    def test_builds_ioc_order_package_at_exact_limit_prices(self):
        books = {
            "JPX-ETFSH": parse_orderbook_depth(depth(bids=[(11010, 10)], asks=[(11020, 10)])),
            "JPX-GOLD": parse_orderbook_depth(depth(bids=[(11200, 10)], asks=[(11210, 10)])),
            "JPX-XAG": parse_orderbook_depth(depth(bids=[(11200, 10)], asks=[(11210, 10)])),
        }
        cert = find_certificates("JPX", books, min_credit_cents=1)[0]

        orders = build_certificate_orders(cert, request_prefix="proof-1", expiry_ms=123456)

        self.assertEqual(
            [(order["instrument_id"], order["side"], order["price"], order["quantity"]) for order in orders],
            [
                ("JPX-ETFSH", "bid", 11020, 2),
                ("JPX-GOLD", "ask", 11200, 1),
                ("JPX-XAG", "ask", 11200, 1),
            ],
        )
        self.assertTrue(all(order["order_type"] == "ioc" for order in orders))
        self.assertTrue(all(order["expiry"] == 123456 for order in orders))


def depth(
    *,
    bids: list[tuple[int, int]],
    asks: list[tuple[int, int]],
) -> dict[str, dict[str, int]]:
    return {
        "bids": {str(price): qty for price, qty in bids},
        "asks": {str(price): qty for price, qty in asks},
    }


if __name__ == "__main__":
    unittest.main()
