"""
HIP-4arb — CLI entry point.

Modes:
  paper    Both WS feeds running, signals logged, no execution.
  live     Live execution via HyperCore /exchange. Requires HYPERLIQUID_PRIVATE_KEY.
  monitor  Read-only: print latest prices and detected signals from SQLite.

Usage:
  python src/main.py paper
  python src/main.py live
  python src/main.py monitor
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import yaml
import structlog

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Lazy local imports (src/ is on path)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))

from database import Database
from hip4_client import HIP4Client, HIP4PriceManager, HIP4WebSocketClient
from poly_client import PolymarketWebSocketClient, OrderbookManager
from signal_engine import SignalEngine, MarketPair
from sizer import kelly_size
from executor import Executor


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def build_market_pairs(config: dict, outcome_meta: dict) -> List[MarketPair]:
    """
    Match config market entries against outcomeMeta from HyperCore.

    outcomeMeta shape: {outcomes: [{outcome: int, name, ...}], questions: [...]}
    Config entry needs: id, polymarket_asset_id (YES token), hip4_outcome_id (int).

    Asset ID formula: YES coin = "#<id*10>", NO coin = "#<id*10+1>"
    No expiry field in outcomeMeta — omit for now; TTL gate uses None (always passes).
    """
    known_ids = {o["outcome"] for o in outcome_meta.get("outcomes", [])}
    pairs: List[MarketPair] = []

    for entry in config.get("markets", []):
        outcome_id: int = entry.get("hip4_outcome_id")
        if outcome_id not in known_ids:
            logger.warning("market_not_in_outcome_meta", id=entry.get("id"), outcome_id=outcome_id)
            continue

        yes_coin = f"#{outcome_id * 10}"
        no_coin  = f"#{outcome_id * 10 + 1}"

        pairs.append(MarketPair(
            id=entry["id"],
            polymarket_asset_id=entry["polymarket_asset_id"],
            hip4_yes_coin=yes_coin,
            hip4_no_coin=no_coin,
            expiry=None,
        ))
        logger.info("market_pair_registered", id=entry["id"], yes=yes_coin, no=no_coin)

    return pairs


# ---------------------------------------------------------------------------
# Paper / live run loop
# ---------------------------------------------------------------------------

async def run(mode: str, config_path: str) -> None:
    cfg = load_config(config_path)
    trading = cfg.get("trading", {})

    paper = mode != "live"
    if not paper and cfg.get("mode") == "paper":
        print("ERROR: config.yaml mode=paper but CLI mode=live. Set mode: live in config.yaml first.")
        sys.exit(1)

    Path("data").mkdir(exist_ok=True)
    db = Database("data/hip4arb.db")
    await db.connect()

    # --- Discover HIP-4 markets -----------------------------------------
    async with HIP4Client() as hip4_rest:
        outcome_meta = await hip4_rest.get_outcome_meta()

    market_pairs = build_market_pairs(cfg, outcome_meta)
    if not market_pairs:
        logger.error("no_market_pairs_configured")
        await db.close()
        sys.exit(1)

    poly_asset_ids = [m.polymarket_asset_id for m in market_pairs]

    # --- Shared price state ---------------------------------------------
    hip4_prices   = HIP4PriceManager()
    poly_books    = OrderbookManager()

    # --- Signal engine --------------------------------------------------
    engine = SignalEngine(
        markets=market_pairs,
        hip4_prices=hip4_prices,
        poly_books=poly_books,
        db=db,
        min_edge=trading.get("min_edge", 0.005),
        fee_cost=trading.get("polymarket_taker_fee", 0.018),
        min_ttl_seconds=trading.get("min_ttl_seconds", 120),
        paper=paper,
    )

    bankroll        = trading.get("bankroll_usdh", 100.0)
    max_pos         = trading.get("max_position_usdh", 10.0)
    kelly_fraction  = trading.get("kelly_fraction", 0.25)

    executor = Executor(paper=paper)

    # --- WS clients -----------------------------------------------------
    hip4_ws = HIP4WebSocketClient(price_manager=hip4_prices)
    poly_ws = PolymarketWebSocketClient()
    poly_books_ref = poly_books  # closure

    hip4_ws.register_callback(engine.on_hip4_update)

    async def poly_book_callback(msg: dict) -> None:
        poly_books_ref.update(msg)
        engine.on_poly_update(msg)

    poly_ws.register_callback("book", poly_book_callback)

    # --- Signal consumer ------------------------------------------------
    async def consume_signals() -> None:
        while True:
            signal = await engine.next_signal()

            size = kelly_size(
                edge=signal.edge,
                entry_price=signal.hip4_price,
                bankroll=bankroll,
                max_position_usdh=max_pos,
                kelly_fraction=kelly_fraction,
            )

            result = await executor.execute(
                market_id=signal.market_id,
                coin=signal.hip4_coin,
                direction=signal.direction,
                size_usdh=size,
                price=signal.hip4_price,
            )

            if result.success:
                logger.info(
                    "order_result",
                    market_id=result.market_id,
                    direction=result.direction,
                    size_usdh=result.size_usdh,
                    price=result.price,
                    paper=result.paper,
                )

    # --- Run all tasks concurrently -------------------------------------
    logger.info("starting", mode=mode, markets=[m.id for m in market_pairs])

    try:
        await asyncio.gather(
            hip4_ws.start(),
            poly_ws.start(poly_asset_ids),
            consume_signals(),
        )
    except KeyboardInterrupt:
        logger.info("interrupted")
    finally:
        await hip4_ws.close()
        await poly_ws.close()
        await db.close()


# ---------------------------------------------------------------------------
# Monitor mode
# ---------------------------------------------------------------------------

async def run_monitor(config_path: str) -> None:
    db = Database("data/hip4arb.db")
    await db.connect()
    stats = await db.get_daily_stats()
    print("\n--- HIP-4arb monitor ---")
    print(f"Date:                  {stats['date']}")
    print(f"Signals detected:      {stats['opportunities_detected']}")
    print(f"Trades executed:       {stats['total_trades']}")
    print(f"Win rate:              {stats['win_rate']:.1f}%")
    print(f"Total P&L:             {stats['total_pnl']:.4f} USDH")
    print(f"Avg P&L per trade:     {stats['avg_pnl_per_trade']:.4f} USDH")
    await db.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="HIP-4arb cross-venue arbitrage bot")
    parser.add_argument("mode", choices=["paper", "live", "monitor"])
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    if args.mode == "monitor":
        asyncio.run(run_monitor(args.config))
    else:
        asyncio.run(run(args.mode, args.config))


if __name__ == "__main__":
    main()
