"""
Cross-venue divergence signal engine.

Every tick (driven by either WS feed pushing new prices):
  1. Look up the latest mid from each venue for a configured market pair
  2. Compute edge = abs(hip4_price - poly_price) - fee_cost
  3. Gate on minimum edge, liquidity placeholder, and time-to-expiry
  4. Emit a TradeSignal if all gates pass

Paper mode: signals are logged and yielded but never executed.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import structlog

from database import Database
from hip4_client import HIP4PriceManager
from poly_client import OrderbookManager

logger = structlog.get_logger()


@dataclass
class MarketPair:
    """One matched market across both venues."""
    id: str                        # stable key, e.g. "btc-daily-binary"
    polymarket_asset_id: str       # Polymarket token ID (YES side)
    hip4_yes_coin: str             # HIP-4 coin for YES side, e.g. "#20"
    hip4_no_coin: str              # HIP-4 coin for NO side, e.g. "#21"
    expiry: Optional[datetime] = None


@dataclass
class TradeSignal:
    market_id: str
    direction: str                 # "YES" | "NO"
    hip4_coin: str                 # which coin to buy on HIP-4
    hip4_price: float
    poly_price: float
    divergence: float
    edge: float                    # divergence - fee_cost
    size_usdh: float               # from sizer; 0.0 in paper mode until sizer wired
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    paper: bool = True


class SignalEngine:
    """
    Computes cross-venue divergence signals on every price tick.

    Consumers call on_hip4_update() or on_poly_update() when their
    respective WS feed delivers new prices. Both methods are synchronous
    callbacks safe to call from the WS listener threads.
    """

    def __init__(
        self,
        markets: List[MarketPair],
        hip4_prices: HIP4PriceManager,
        poly_books: OrderbookManager,
        db: Database,
        min_edge: float = 0.005,
        fee_cost: float = 0.018,
        min_ttl_seconds: int = 120,
        paper: bool = True,
    ) -> None:
        self.markets = {m.id: m for m in markets}
        self.hip4_prices = hip4_prices
        self.poly_books = poly_books
        self.db = db
        self.min_edge = min_edge
        self.fee_cost = fee_cost
        self.min_ttl_seconds = min_ttl_seconds
        self.paper = paper

        # Pending signals waiting to be consumed by the executor / logger
        self._signal_queue: asyncio.Queue[TradeSignal] = asyncio.Queue()

        # Rate-limit: don't re-signal the same market within this many seconds
        self._last_signal: Dict[str, datetime] = {}
        self._signal_cooldown = 5.0

    # ------------------------------------------------------------------
    # WS callbacks — called by feed listeners
    # ------------------------------------------------------------------

    def on_hip4_update(self, mids: dict[str, float]) -> None:
        """Called by HIP4WebSocketClient callback on every allMids push."""
        for market in self.markets.values():
            self._evaluate(market)

    def on_poly_update(self, message: dict) -> None:
        """Called by PolymarketWebSocketClient callback on every book event."""
        asset_id = message.get("asset_id")
        for market in self.markets.values():
            if market.polymarket_asset_id == asset_id:
                self._evaluate(market)

    # ------------------------------------------------------------------
    # Core evaluation
    # ------------------------------------------------------------------

    def _evaluate(self, market: MarketPair) -> None:
        hip4_yes = self.hip4_prices.get_mid(market.hip4_yes_coin)
        hip4_no = self.hip4_prices.get_mid(market.hip4_no_coin)
        poly_bid, poly_ask = self.poly_books.get_best_bid_ask(market.polymarket_asset_id)

        if hip4_yes is None or hip4_no is None or poly_bid is None or poly_ask is None:
            return

        # Polymarket mid for YES side
        poly_price = (poly_bid + poly_ask) / 2.0

        # HIP-4 merged book: buying YES at p == selling NO at 1-p
        # Use the YES mid as our canonical HIP-4 price
        hip4_price = hip4_yes

        divergence = abs(hip4_price - poly_price)
        edge = divergence - self.fee_cost

        if edge < self.min_edge:
            return

        if not self._ttl_ok(market):
            return

        if not self._cooldown_ok(market.id):
            return

        # Direction: buy the cheaper venue's YES
        # HIP-4 has zero open fee, so we always execute there
        direction = "YES" if hip4_price < poly_price else "NO"
        hip4_coin = market.hip4_yes_coin if direction == "YES" else market.hip4_no_coin

        signal = TradeSignal(
            market_id=market.id,
            direction=direction,
            hip4_coin=hip4_coin,
            hip4_price=hip4_price,
            poly_price=poly_price,
            divergence=divergence,
            edge=edge,
            size_usdh=0.0,  # sizer.py wires this in next
            paper=self.paper,
        )

        self._last_signal[market.id] = signal.timestamp

        logger.info(
            "trade_signal",
            market_id=market.id,
            direction=direction,
            hip4_price=hip4_price,
            poly_price=poly_price,
            divergence=round(divergence, 4),
            edge=round(edge, 4),
            paper=self.paper,
        )

        # Fire-and-forget: schedule DB log and queue push on the running loop
        asyncio.get_event_loop().call_soon_threadsafe(
            lambda s=signal: asyncio.ensure_future(self._emit(s))
        )

    async def _emit(self, signal: TradeSignal) -> None:
        await self._signal_queue.put(signal)
        await self.db.log_opportunity(
            market_id=signal.market_id,
            market_type="hip4arb",
            yes_price=signal.hip4_price,
            no_price=1.0 - signal.hip4_price,
            time_until_resolution=0,
            traded=False,
        )

    # ------------------------------------------------------------------
    # Gates
    # ------------------------------------------------------------------

    def _ttl_ok(self, market: MarketPair) -> bool:
        if market.expiry is None:
            return True
        now = datetime.now(timezone.utc)
        expiry = market.expiry
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        ttl = (expiry - now).total_seconds()
        return ttl >= self.min_ttl_seconds

    def _cooldown_ok(self, market_id: str) -> bool:
        last = self._last_signal.get(market_id)
        if last is None:
            return True
        elapsed = (datetime.now(timezone.utc) - last).total_seconds()
        return elapsed >= self._signal_cooldown

    # ------------------------------------------------------------------
    # Signal consumption
    # ------------------------------------------------------------------

    async def next_signal(self) -> TradeSignal:
        """Block until the next signal is available."""
        return await self._signal_queue.get()

    async def drain_signals(self) -> List[TradeSignal]:
        """Return all signals currently in the queue without blocking."""
        signals = []
        while not self._signal_queue.empty():
            signals.append(self._signal_queue.get_nowait())
        return signals
