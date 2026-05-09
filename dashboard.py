"""AlgoTrade 2026 self-dashboard.

Connects one WebSocket per exchange (10 total), tracks our own
inventory / cash / order-book / events, computes per-exchange NAV
and PnL, and serves a small web UI at http://localhost:8080.

There is no public leaderboard API — trade events are anonymous and
the venue doesn't expose other teams' positions. The "score" shown
here is our own NAV minus initial capital, summed across exchanges,
which is what the official scoring formula is computed from.

Run:
    pip install -r requirements.txt
    python dashboard.py            # binds 0.0.0.0:8080
    PORT=9000 python dashboard.py  # override port
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from pathlib import Path

import aiohttp
from aiohttp import web
import websockets

EXCHANGES = [
    "nyse", "nasdaq", "sse", "jpx", "euronext",
    "lse", "hkex", "nse", "tmx", "zse",
]
WS_URL = "ws://{ex}.algotrade.hr:9001/trade"
HEALTH_URL = "http://{ex}.algotrade.hr:9001/health"
INITIAL_CASH_CENTS = 10_000_000  # $100k per exchange

INV_POLL_SEC = float(os.environ.get("INV_POLL_SEC", "1.0"))
HEALTH_POLL_SEC = float(os.environ.get("HEALTH_POLL_SEC", "2.0"))
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("dashboard")


class ExchangeState:
    def __init__(self, name: str) -> None:
        self.name = name
        self.connected = False
        self.error: str | None = None
        self.last_msg_ts = 0.0
        # server-time / round
        self.server_time_ms = 0
        self.round_length_ms = 0
        self.last_health_ts = 0.0
        # inventory
        self.cash = INITIAL_CASH_CENTS
        self.reserved_cash = 0
        self.positions: dict[str, int] = {}
        self.reserved_positions: dict[str, int] = {}
        # market
        self.orderbooks: dict[str, dict] = {}
        self.last_trade_price: dict[str, int] = {}
        self.recent_events: deque = deque(maxlen=200)
        # orders
        self.pending_orders: dict[str, list] = {}

    # --- pricing helpers ---------------------------------------------------
    def best_bid(self, instrument: str) -> int | None:
        ob = self.orderbooks.get(instrument)
        if not ob:
            return None
        bids = ob.get("bids") or {}
        if not bids:
            return None
        return max(int(p) for p in bids.keys())

    def best_ask(self, instrument: str) -> int | None:
        ob = self.orderbooks.get(instrument)
        if not ob:
            return None
        asks = ob.get("asks") or {}
        if not asks:
            return None
        return min(int(p) for p in asks.keys())

    def mid(self, instrument: str) -> int | None:
        bb = self.best_bid(instrument)
        ba = self.best_ask(instrument)
        if bb is not None and ba is not None:
            return (bb + ba) // 2
        if bb is not None:
            return bb
        if ba is not None:
            return ba
        return self.last_trade_price.get(instrument)

    def nav_cents(self) -> int:
        nav = self.cash
        for inst, qty in self.positions.items():
            if qty == 0:
                continue
            m = self.mid(inst)
            if m is None:
                continue
            nav += qty * m
        return nav

    def pnl_cents(self) -> int:
        return self.nav_cents() - INITIAL_CASH_CENTS


STATE: dict[str, ExchangeState] = {ex: ExchangeState(ex) for ex in EXCHANGES}


# ----------------------------------------------------------------------------
# Message handling
# ----------------------------------------------------------------------------
def handle_msg(es: ExchangeState, msg: dict) -> None:
    t = msg.get("type")
    if t == "market_data_update":
        if isinstance(msg.get("time"), int):
            es.server_time_ms = msg["time"]
        for inst, ob in (msg.get("orderbook_depths") or {}).items():
            es.orderbooks[inst] = ob
        for ev in (msg.get("events") or []):
            data = ev.get("data") or {}
            if ev.get("event_type") == "trade":
                inst = data.get("instrumentID")
                price = data.get("price")
                if inst and isinstance(price, int):
                    es.last_trade_price[inst] = price
            es.recent_events.append(ev)
    elif t == "get_inventory_response":
        data = msg.get("data") or {}
        new_pos: dict[str, int] = {}
        new_res: dict[str, int] = {}
        for k, v in data.items():
            if not (isinstance(v, list) and len(v) == 2):
                continue
            reserved, total = v
            if k == "$":
                es.reserved_cash = int(reserved)
                es.cash = int(total)
            else:
                new_pos[k] = int(total)
                new_res[k] = int(reserved)
        es.positions = new_pos
        es.reserved_positions = new_res
    elif t == "get_pending_orders_response":
        es.pending_orders = msg.get("data") or {}
    elif t == "end_of_round":
        es.recent_events.append({"event_type": "end_of_round", "data": {}})
    elif t == "welcome":
        pass
    elif t in ("add_order_response", "cancel_order_response"):
        pass  # not initiated by dashboard
    else:
        log.debug("[%s] unhandled msg type %s", es.name, t)


# ----------------------------------------------------------------------------
# Per-exchange tasks
# ----------------------------------------------------------------------------
async def exchange_ws_loop(es: ExchangeState) -> None:
    url = WS_URL.format(ex=es.name)
    backoff = 1.0
    inv_id = 0
    while True:
        try:
            log.info("[%s] connecting %s", es.name, url)
            async with websockets.connect(
                url,
                max_size=20 * 1024 * 1024,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                es.connected = True
                es.error = None
                backoff = 1.0
                last_inv_request = 0.0

                async def poll_inventory():
                    nonlocal inv_id, last_inv_request
                    while True:
                        await asyncio.sleep(INV_POLL_SEC)
                        inv_id += 1
                        try:
                            await ws.send(json.dumps({
                                "type": "get_inventory",
                                "user_request_id": f"inv-{inv_id}",
                            }))
                            await ws.send(json.dumps({
                                "type": "get_pending_orders",
                                "user_request_id": f"po-{inv_id}",
                            }))
                            last_inv_request = time.time()
                        except Exception:
                            return

                poller = asyncio.create_task(poll_inventory())
                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            log.warning("[%s] non-JSON frame: %r", es.name, raw[:120])
                            continue
                        handle_msg(es, msg)
                        es.last_msg_ts = time.time()
                finally:
                    poller.cancel()
                    with __import__("contextlib").suppress(BaseException):
                        await poller
        except Exception as e:
            es.error = f"{type(e).__name__}: {e}"
            log.warning("[%s] disconnected: %s", es.name, es.error)
        es.connected = False
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, 5.0)


async def health_loop(es: ExchangeState, http: aiohttp.ClientSession) -> None:
    url = HEALTH_URL.format(ex=es.name)
    while True:
        try:
            async with http.get(url, timeout=aiohttp.ClientTimeout(total=2.0)) as r:
                if r.status == 200:
                    j = await r.json()
                    if isinstance(j.get("time"), int):
                        es.server_time_ms = max(es.server_time_ms, j["time"])
                    if isinstance(j.get("round_length"), int):
                        es.round_length_ms = j["round_length"]
                    es.last_health_ts = time.time()
        except Exception:
            pass
        await asyncio.sleep(HEALTH_POLL_SEC)


# ----------------------------------------------------------------------------
# HTTP API
# ----------------------------------------------------------------------------
def snapshot() -> dict:
    out: dict = {
        "ts_ms": int(time.time() * 1000),
        "exchanges": [],
        "totals": {
            "nav_cents": 0,
            "pnl_cents": 0,
            "cash_cents": 0,
            "initial_cents": 0,
            "connected": 0,
            "exchange_count": len(EXCHANGES),
        },
    }
    for name in EXCHANGES:
        es = STATE[name]
        nav = es.nav_cents()
        pnl = es.pnl_cents()
        positions = []
        for inst in sorted(es.positions.keys()):
            qty = es.positions[inst]
            res = es.reserved_positions.get(inst, 0)
            if qty == 0 and res == 0:
                continue
            mid = es.mid(inst)
            positions.append({
                "instrument": inst,
                "qty": qty,
                "reserved": res,
                "mid_cents": mid,
                "value_cents": qty * mid if (qty and mid is not None) else 0,
            })

        pending_count = 0
        for inst, sides in (es.pending_orders or {}).items():
            if isinstance(sides, list) and len(sides) == 2:
                pending_count += len(sides[0]) + len(sides[1])

        time_remaining = None
        if es.round_length_ms:
            time_remaining = max(0, es.round_length_ms - es.server_time_ms)

        out["exchanges"].append({
            "name": name,
            "connected": es.connected,
            "error": es.error,
            "cash_cents": es.cash,
            "reserved_cash_cents": es.reserved_cash,
            "nav_cents": nav,
            "pnl_cents": pnl,
            "server_time_ms": es.server_time_ms,
            "round_length_ms": es.round_length_ms,
            "time_remaining_ms": time_remaining,
            "last_msg_age_sec": (time.time() - es.last_msg_ts) if es.last_msg_ts else None,
            "positions": positions,
            "instruments_seen": sorted(es.orderbooks.keys()),
            "pending_orders_count": pending_count,
            "recent_events": list(es.recent_events)[-25:],
        })
        out["totals"]["nav_cents"] += nav
        out["totals"]["pnl_cents"] += pnl
        out["totals"]["cash_cents"] += es.cash
        out["totals"]["initial_cents"] += INITIAL_CASH_CENTS
        if es.connected:
            out["totals"]["connected"] += 1
    return out


async def api_state(_req: web.Request) -> web.Response:
    return web.json_response(snapshot())


async def api_orderbook(req: web.Request) -> web.Response:
    ex = req.match_info["ex"]
    inst = req.match_info["instrument"]
    es = STATE.get(ex)
    if not es:
        return web.json_response({"error": "unknown exchange"}, status=404)
    ob = es.orderbooks.get(inst)
    if ob is None:
        return web.json_response({"error": "no book yet"}, status=404)
    bids = sorted(((int(p), q) for p, q in (ob.get("bids") or {}).items()),
                  key=lambda x: -x[0])
    asks = sorted(((int(p), q) for p, q in (ob.get("asks") or {}).items()),
                  key=lambda x: x[0])
    return web.json_response({
        "exchange": ex,
        "instrument": inst,
        "bids": bids,
        "asks": asks,
        "mid_cents": es.mid(inst),
        "last_trade_cents": es.last_trade_price.get(inst),
    })


async def api_events(req: web.Request) -> web.Response:
    ex = req.match_info["ex"]
    es = STATE.get(ex)
    if not es:
        return web.json_response({"error": "unknown exchange"}, status=404)
    return web.json_response({"events": list(es.recent_events)})


async def index(_req: web.Request) -> web.Response:
    html_path = Path(__file__).parent / "dashboard.html"
    return web.Response(text=html_path.read_text(encoding="utf-8"),
                        content_type="text/html")


# ----------------------------------------------------------------------------
# Bootstrap
# ----------------------------------------------------------------------------
async def main() -> None:
    http = aiohttp.ClientSession()
    tasks = []
    for ex in EXCHANGES:
        tasks.append(asyncio.create_task(exchange_ws_loop(STATE[ex])))
        tasks.append(asyncio.create_task(health_loop(STATE[ex], http)))

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/api/state", api_state)
    app.router.add_get("/api/orderbook/{ex}/{instrument}", api_orderbook)
    app.router.add_get("/api/events/{ex}", api_events)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    log.info("dashboard listening on http://%s:%d", HOST, PORT)

    try:
        await asyncio.Event().wait()
    finally:
        for t in tasks:
            t.cancel()
        await http.close()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
