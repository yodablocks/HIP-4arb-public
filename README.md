# HIP-4arb

Cross-venue arbitrage bot for [Hyperliquid HIP-4](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/hip-4-outcome-markets) binary outcome markets and [Polymarket](https://polymarket.com).

Monitors both venues in real time via WebSocket. When the same binary event is priced differently across venues, the bot sizes a position on HIP-4 (zero open fee) using fractional Kelly and logs it to SQLite.

---

## How it works

HIP-4 outcome markets and Polymarket both price binary events as probabilities between 0 and 1. When the same event trades at meaningfully different prices across venues, there is an exploitable edge.

```
edge = abs(hip4_price - poly_price) - polymarket_taker_fee

if edge > min_edge and time_to_expiry > min_ttl:
    direction = "YES" if hip4_price < poly_price else "NO"
    size = kelly_size(edge, hip4_price, bankroll, max_position_usdh)
    executor.execute(coin, direction, size)   # always on HIP-4
```

HIP-4 has no open fee. Max loss per trade is the entry premium — binary markets have no liquidation risk.

---

## Structure

```
src/
├── hip4_client.py      # HyperCore outcomeMeta + allMids REST and WebSocket
├── poly_client.py      # Polymarket CLOB WebSocket + orderbook state
├── signal_engine.py    # Cross-venue divergence detection
├── sizer.py            # Fractional Kelly position sizing
├── executor.py         # Order submission (paper mode included, live stub)
├── database.py         # SQLite trade and signal log
└── main.py             # CLI entry point
```

---

## Quickstart

```bash
pip install -e .

# Configure markets in config.yaml, then:
python src/main.py paper     # paper trading, no execution
python src/main.py monitor   # print daily stats from SQLite
```

---

## Configuration

Edit `config.yaml`:

```yaml
trading:
  min_edge: 0.005           # minimum edge after fees
  max_position_usdh: 10     # hard cap per trade
  bankroll_usdh: 100.0      # total capital for Kelly sizing
  kelly_fraction: 0.25      # fractional Kelly multiplier

markets:
  - id: "my-market"
    polymarket_asset_id: "..."   # YES token from Polymarket CLOB (clobTokenIds[0])
    hip4_outcome_id: 0           # outcome ID from HyperCore outcomeMeta API

mode: paper
```

**Finding market IDs:**
- HIP-4 outcome IDs: `POST https://api.hyperliquid.xyz/info {"type": "outcomeMeta"}`
- Polymarket YES token: `https://gamma-api.polymarket.com/events?slug=<event-slug>` → `clobTokenIds[0]`

---

## Live trading

Live execution requires:

1. `pip install hyperliquid-python-sdk eth-account`
2. `export HYPERLIQUID_PRIVATE_KEY=0x...`
3. Set `mode: live` in `config.yaml`
4. Implement `_live_order` and `_init_exchange` in `src/executor.py` using the [Hyperliquid Python SDK](https://github.com/hyperliquid-dex/hyperliquid-python-sdk)

> Run paper mode first and validate signals before going live.

---

## Notes

- Collateral is **USDH**, not USDC. Swap once at startup via HyperCore spot.
- HIP-4 merged book: buying YES at `p` is equivalent to selling NO at `1-p`.
- Settlement is binary: YES = 1 USDH, NO = 0 USDH.
- Use `accepting_orders: true` (not `active`) when filtering Polymarket markets — closed markets also return `active: true`.

---

## License

MIT
