"""
HyperCore order executor.

Paper mode (config mode=paper): logs the order, never POSTs to /exchange.
Live mode: signs and submits via the Hyperliquid Python SDK Exchange class.

Requires:
  pip install hyperliquid-python-sdk
  HYPERLIQUID_PRIVATE_KEY env var set to EVM private key (0x-prefixed)

SDK reference: https://github.com/hyperliquid-dex/hyperliquid-python-sdk
Exchange.order() signature (as of SDK 0.9):
  exchange.order(coin, is_buy, sz, limit_px, order_type, reduce_only=False)
  order_type = {"limit": {"tif": "Gtc"}} | {"limit": {"tif": "Ioc"}}
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import structlog

logger = structlog.get_logger()

EXCHANGE_URL = "https://api.hyperliquid.xyz/exchange"
MIN_ORDER_USDH = 10.0


@dataclass
class OrderResult:
    success: bool
    market_id: str
    coin: str
    direction: str
    size_usdh: float
    price: float
    paper: bool
    raw: Optional[dict] = None
    error: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


class Executor:
    """
    Signs and submits orders to HyperCore /exchange.

    In paper mode, all calls return a successful OrderResult without
    touching the network. The paper guard is checked at construction
    time so no live key is needed for paper runs.
    """

    def __init__(self, paper: bool = True) -> None:
        self.paper = paper
        self._exchange = None

        if not paper:
            self._exchange = self._init_exchange()

    def _init_exchange(self):
        """Initialise the SDK Exchange instance for live trading."""
        raise NotImplementedError(
            "Live execution is not included in this public release. "
            "Implement using the Hyperliquid Python SDK: "
            "https://github.com/hyperliquid-dex/hyperliquid-python-sdk"
        )

    async def execute(
        self,
        market_id: str,
        coin: str,
        direction: str,
        size_usdh: float,
        price: float,
    ) -> OrderResult:
        """
        Place a limit IOC order on HIP-4.

        Args:
            market_id:  Human-readable market key for logging.
            coin:       HIP-4 coin string, e.g. "#20".
            direction:  "YES" or "NO".
            size_usdh:  Position size in USDH (must be >= 10).
            price:      Limit price (mid from hip4_client). Used as IOC limit.
        """
        if size_usdh < MIN_ORDER_USDH:
            logger.warning(
                "order_below_minimum",
                market_id=market_id,
                size_usdh=size_usdh,
                minimum=MIN_ORDER_USDH,
            )
            return OrderResult(
                success=False,
                market_id=market_id,
                coin=coin,
                direction=direction,
                size_usdh=size_usdh,
                price=price,
                paper=self.paper,
                error=f"size {size_usdh} below minimum {MIN_ORDER_USDH} USDH",
            )

        if self.paper:
            return self._paper_order(market_id, coin, direction, size_usdh, price)

        return await self._live_order(market_id, coin, direction, size_usdh, price)

    def _paper_order(
        self,
        market_id: str,
        coin: str,
        direction: str,
        size_usdh: float,
        price: float,
    ) -> OrderResult:
        logger.info(
            "paper_order",
            market_id=market_id,
            coin=coin,
            direction=direction,
            size_usdh=size_usdh,
            price=price,
        )
        return OrderResult(
            success=True,
            market_id=market_id,
            coin=coin,
            direction=direction,
            size_usdh=size_usdh,
            price=price,
            paper=True,
        )

    async def _live_order(
        self,
        market_id: str,
        coin: str,
        direction: str,
        size_usdh: float,
        price: float,
    ) -> OrderResult:
        raise NotImplementedError(
            "Live execution is not included in this public release."
        )
