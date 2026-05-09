#!/usr/bin/env python3
"""
flatten.py — emergency cash-out across AlgoTrade exchanges.

Connects to every requested exchange in parallel, cancels any pending orders,
then issues market-cross orders to close every non-zero position.  Loops up
to MAX_ROUNDS times per venue so partial fills get retried.

This is a one-shot tool — it exits when every venue is flat (or it gives up
after MAX_ROUNDS).

Run
---
    python3 flatten.py --dry-run                # print intended orders only
    python3 flatten.py --venues ZSE,NYSE        # subset, live
    python3 flatten.py                          # all 10 venues, live

Exits 0 when every venue ends flat, 1 if any venue has residual positions.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from typing import Optional

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:                                    # websockets <12 fallback
    from websockets.client import connect as ws_connect  # type: ignore


VENUES = ["NYSE", "NASDAQ", "SSE", "JPX", "EURONEXT", "LSE", "HKEX", "NSE", "TMX", "ZSE"]
WS_HOSTS = {
    "NYSE":     "nyse.algotrade.hr",
    "NASDAQ":   "nasdaq.algotrade.hr",
    "SSE":      "sse.algotrade.hr",
    "JPX":      "jpx.algotrade.hr",
    "EURONEXT": "euronext.algotrade.hr",
    "LSE":      "lse.algotrade.hr",
    "HKEX":     "hkex.algotrade.hr",
    "NSE":      "nse.algotrade.hr",
    "TMX":      "tmx.algotrade.hr",
    "ZSE":      "zse.algotrade.hr",
}

MAX_ROUNDS    = 5       # iterations of (get_inventory → market-close) per venue
ROUND_DELAY_S = 0.6     # let fills settle between rounds
TIMEOUT_S     = 3.0     # per-message recv timeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("flatten")


class _RID:
    """Per-venue request-id sequencer; passed in instead of a closure so the
    flatten coroutine stays flat and easy to read."""
    def __init__(self, venue: str) -> None:
        self.venue = venue
        self.seq = 0

    def next(self, tag: str) -> str:
        self.seq += 1
        return f"f-{self.venue}-{self.seq:04d}-{tag}"


async def _recv_until(ws, msg_type: str, rid: Optional[str] = None,
                      timeout: float = TIMEOUT_S) -> Optional[dict]:
    """Drain market-data broadcasts until we see the matching response."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        except asyncio.TimeoutError:
            return None
        try:
            msg = json.loads(raw)
        except Exception:
            continue
        if msg.get("type") == msg_type and (rid is None or msg.get("user_request_id") == rid):
            return msg


def _parse_inventory(data: dict) -> tuple[int, dict[str, int]]:
    """(cash_cents, {instrument_id: signed_position_total})."""
    cash = 0
    positions: dict[str, int] = {}
    for inst, pair in (data or {}).items():
        try:
            total = int(pair[1])
        except (TypeError, ValueError, IndexError):
            continue
        if inst == "$":
            cash = total
        elif total != 0:
            positions[inst] = total
    return cash, positions


async def flatten_venue(venue: str, dry_run: bool) -> tuple[str, dict[str, int]]:
    """Connect, cancel pending, flatten positions. Returns (venue, residual)."""
    url = f"ws://{WS_HOSTS[venue]}:9001/trade"
    rid = _RID(venue)

    try:
        async with ws_connect(url, open_timeout=5, max_size=2**24) as ws:
            welcome_raw = await asyncio.wait_for(ws.recv(), timeout=5)
            try:
                welcome = json.loads(welcome_raw)
            except Exception:
                welcome = {}
            if welcome.get("type") != "welcome":
                log.warning("[%s] unexpected first frame: %r", venue, welcome_raw[:80])
            log.info("[%s] connected", venue)

            # ── Phase 1: cancel any pending orders so reserved inventory is released
            r = rid.next("pend")
            await ws.send(json.dumps({
                "type": "get_pending_orders",
                "user_request_id": r,
            }))
            resp = await _recv_until(ws, "get_pending_orders_response", r)
            cancel_count = 0
            if resp:
                data = resp.get("data") or {}
                for inst, sides in data.items():
                    if not isinstance(sides, list) or len(sides) < 2:
                        continue
                    for side_orders in sides:
                        for od in side_orders or []:
                            try:
                                oid = int(od["orderID"])
                            except (KeyError, TypeError, ValueError):
                                continue
                            if dry_run:
                                log.info("[%s] DRY: would cancel order %d on %s", venue, oid, inst)
                            else:
                                await ws.send(json.dumps({
                                    "type": "cancel_order",
                                    "user_request_id": rid.next("cxl"),
                                    "order_id": oid,
                                    "instrument_id": inst,
                                }))
                            cancel_count += 1
                if cancel_count and not dry_run:
                    await asyncio.sleep(0.4)
            if cancel_count:
                log.info("[%s] %s %d pending orders",
                         venue, "would cancel" if dry_run else "cancelled", cancel_count)

            # ── Phase 2: iterate inventory → market-close until flat or out of rounds
            residual: dict[str, int] = {}
            for round_n in range(MAX_ROUNDS):
                r = rid.next(f"inv{round_n}")
                await ws.send(json.dumps({
                    "type": "get_inventory",
                    "user_request_id": r,
                }))
                inv = await _recv_until(ws, "get_inventory_response", r)
                if not inv:
                    log.warning("[%s] inventory request timed out (round %d)", venue, round_n)
                    return venue, {"_error": "inventory_timeout"}

                cash, positions = _parse_inventory(inv.get("data") or {})
                if not positions:
                    log.info("[%s] FLAT: cash=$%.2f", venue, cash / 100)
                    return venue, {}

                log.info("[%s] round %d: %d non-zero positions, cash=$%.2f",
                         venue, round_n, len(positions), cash / 100)

                for inst, pos in positions.items():
                    side = "ask" if pos > 0 else "bid"
                    qty = abs(pos)
                    if dry_run:
                        log.info("[%s] DRY: would %s %d %s (market)", venue, side, qty, inst)
                    else:
                        await ws.send(json.dumps({
                            "type": "add_order",
                            "user_request_id": rid.next(f"flat-{inst}"),
                            "instrument_id": inst,
                            "side": side,
                            "quantity": qty,
                            "order_type": "market",
                        }))
                residual = positions
                if dry_run:
                    # In dry-run we never send the close orders so positions won't change;
                    # one report is enough.
                    return venue, residual
                await asyncio.sleep(ROUND_DELAY_S)

            # Out of rounds — log final state.
            log.warning("[%s] RESIDUAL after %d rounds: %s",
                        venue, MAX_ROUNDS, residual)
            return venue, residual
    except Exception as e:
        log.error("[%s] failed: %s", venue, e)
        return venue, {"_error": str(e)}


async def amain(active: list[str], dry_run: bool) -> int:
    if dry_run:
        log.info("DRY RUN — no orders will be sent")
    log.info("flattening venues: %s", active)
    results = await asyncio.gather(
        *[flatten_venue(v, dry_run) for v in active],
        return_exceptions=False,
    )
    log.info("=" * 60)
    any_residual = False
    for venue, residual in results:
        if not residual:
            log.info("%-10s: FLAT", venue)
        elif "_error" in residual:
            log.warning("%-10s: ERROR %s", venue, residual["_error"])
            any_residual = True
        else:
            log.warning("%-10s: %s", venue, residual)
            any_residual = True
    return 1 if any_residual else 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="emergency flatten across AlgoTrade exchanges")
    p.add_argument(
        "--venues",
        default=",".join(VENUES),
        help="Comma-separated venue subset (default: all 10)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print intended actions without sending any orders",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    active = [v.strip().upper() for v in args.venues.split(",") if v.strip()]
    bad = [v for v in active if v not in VENUES]
    if bad:
        raise SystemExit(f"unknown venue(s): {bad} (valid: {VENUES})")
    rc = asyncio.run(amain(active, args.dry_run))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
