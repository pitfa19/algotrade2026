# Edge Max Proof Note

## What can and cannot be proved

No finite historical market-data cache can unconditionally prove a future profit edge. For any deterministic bot and any finite cache prefix, construct a valid future segment in which no market order ever reaches the bot's passive traps, and every stale quote disappears before our order arrives. The bot then earns zero or loses on inventory. This future is compatible with the protocol and with the observed cache prefix. Therefore an unconditional future-profit theorem is false.

The cache also cannot prove the reported "buy at $1 at the very beginning" exploit, because the provided CSV data starts around server time 1,545,000 ms, not at segment open. The bot treats the $1 opening bid as a high-upside live hypothesis, not as a fact proven by this cache.

## Mechanical edge that is provable

The matching engine executes trades at the passive resting order's price. Suppose our resting bid at price `p` is filled for quantity `q`, and a later visible bid exists at price at least `p + 1`. Sending an IOC ask with limit `p + 1` for quantity `q` cannot execute below `p + 1`; any unfilled remainder is cancelled. Every filled share therefore realizes at least 1 cent profit, and no additional loss is introduced by the IOC itself.

The symmetric short version is identical: if our resting ask at `p` is filled, and a later visible ask exists at price at most `p - 1`, an IOC bid with limit `p - 1` cannot execute above `p - 1`. Every covered share realizes at least 1 cent profit.

This is an unconditional statement about the protocol, not a statistical claim.

## Historical evidence in the cache

The replay scanner found historical bursts matching the mechanical condition. Examples:

- `NYSE-DDJH` at `t=1621041`: bid trap around `13338`, next bid `18311`, replay PnL `845410` cents.
- `LSE-ETFB` at `t=1557898`: ask trap `18000`, next ask `11762`, replay PnL `623800` cents.
- `ZSE-INA` at `t=1620597`: fixed bid `10000`, next bid `10756`, replay PnL `120960` cents.

With aggressive 20-cent median and 30-cent ETF thresholds:

- Close ZSE cluster replay: `2,842,841.37` dollars.
- All-exchange replay: `9,319,833.60` dollars.

These replay numbers are edge-ranking evidence, not guaranteed deploy PnL. They do not fully model queue priority, cash contention across simultaneous signals, remote latency, or all settlement details. They do show that the threshold class is large enough to plausibly explain a multi-million winning result when combined with the segment-open trap.

## Stricter cash/position backtest

`tools/backtest_edge_max.py` is a more conservative simulator. It enforces cash floors, position limits, pending bid cash reservation, visible top-3 IOC depth, order expiry, passive fills only when a recorded active-order burst swept through our resting price, and final mark-to-market from the cache.

On the available cache, after adding position-aware exits and per-send room clamps:

- Close ZSE cluster strict backtest: `77,726.97` dollars.
- All-exchange strict backtest: `568,784.31` dollars.

This is the number to compare with existing `$500k`-class strategies. It still excludes the opening `$1` exploit because the cache begins far after segment open.
