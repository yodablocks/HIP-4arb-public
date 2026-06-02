"""
HyperCore outcome markets client.

Endpoints:
  POST https://api.hyperliquid.xyz/info  {"type": "outcomeMeta"}      -> discover HIP-4 markets
  POST https://api.hyperliquid.xyz/info  {"type": "allMids"}           -> live mid prices
  WS   wss://api.hyperliquid.xyz/ws      subscribe allMids             -> streaming mid prices

HIP-4 coins appear as "#N" in allMids. Filter: k.startswith("#").
Asset ID: N = 10 * outcomeIndex + sideIndex. YES = even N, NO = odd N.

Note: outcomeMeta payload shape is documented but unverified against a live response
(HIP-4 launched May 2026). Adjust field names if they differ from the Hyperliquid docs.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import httpx
import websockets

import structlog

logger = structlog.get_logger()

REST_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"

OUTCOME_META_CACHE_TTL = 30.0  # markets don't churn; 30s is fine


def decode_asset_id(n: int) -> tuple[int, str]:
    """Return (outcome_index, side) for a HIP-4 asset integer N."""
    outcome_index = n // 10
    side = "YES" if n % 2 == 0 else "NO"
    return outcome_index, side


class HIP4Client:
    """
    REST client for HyperCore outcome markets.

    Wraps outcomeMeta (market discovery) and allMids (snapshot prices)
    with a short TTL cache on outcomeMeta to avoid hammering the endpoint.
    """

    def __init__(self, client: httpx.AsyncClient | None = None):
        self._owns_client = client is None
        self.http = client or httpx.AsyncClient(timeout=10.0)
        self._meta_cache: tuple[datetime, dict] | None = None
        self._meta_lock = asyncio.Lock()

    async def __aenter__(self) -> HIP4Client:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_client:
            await self.http.aclose()

    # ------------------------------------------------------------------
    # outcomeMeta — market discovery
    # ------------------------------------------------------------------

    async def get_outcome_meta(self) -> dict:
        """
        Fetch all active HIP-4 outcome markets.

        Returns the raw response dict with keys:
          outcomes: list of {outcome: int, name, description, sideSpecs, quoteToken}
          questions: list of {question: int, name, description, fallbackOutcome,
                               namedOutcomes: [int, ...], settledNamedOutcomes: [int]}

        Outcome integer IDs map to #N coins in allMids:
          YES coin = "#<outcome_id * 10>"
          NO  coin = "#<outcome_id * 10 + 1>"
        """
        async with self._meta_lock:
            now = datetime.now(timezone.utc)
            if (
                self._meta_cache is not None
                and (now - self._meta_cache[0]).total_seconds() < OUTCOME_META_CACHE_TTL
            ):
                return self._meta_cache[1]

            resp = await self.http.post(REST_URL, json={"type": "outcomeMeta"})
            resp.raise_for_status()
            data = resp.json()

            self._meta_cache = (now, data)
            logger.info(
                "outcome_meta_fetched",
                outcomes=len(data.get("outcomes", [])),
                questions=len(data.get("questions", [])),
            )
            return data

    # ------------------------------------------------------------------
    # allMids — snapshot prices filtered to HIP-4 (#N) coins
    # ------------------------------------------------------------------

    async def get_hip4_mids(self) -> dict[str, float]:
        """
        Fetch current mid prices for all HIP-4 outcome coins.

        Returns {coin: mid_price} for every "#N" key in allMids,
        where mid_price is a float in [0.0, 1.0].
        """
        resp = await self.http.post(REST_URL, json={"type": "allMids"})
        resp.raise_for_status()
        data: dict[str, str] = resp.json()

        return {
            k: float(v)
            for k, v in data.items()
            if k.startswith("#")
        }

    async def get_mid(self, coin: str) -> float | None:
        """Return current mid price for a single HIP-4 coin (e.g. '#20')."""
        mids = await self.get_hip4_mids()
        return mids.get(coin)


class HIP4PriceManager:
    """
    Maintains latest mid prices for HIP-4 coins from WebSocket updates.

    Analogous to OrderbookManager in poly_client.py: stores the last
    allMids snapshot and exposes per-coin lookup.
    """

    def __init__(self) -> None:
        self._prices: dict[str, float] = {}
        self._updated_at: datetime | None = None

    def update(self, mids: dict[str, str | float]) -> None:
        """Apply an allMids payload, keeping only #N coins."""
        for k, v in mids.items():
            if k.startswith("#"):
                self._prices[k] = float(v)
        self._updated_at = datetime.now(timezone.utc)

    def get_mid(self, coin: str) -> float | None:
        return self._prices.get(coin)

    def all_mids(self) -> dict[str, float]:
        return dict(self._prices)

    @property
    def last_updated(self) -> datetime | None:
        return self._updated_at


class HIP4WebSocketClient:
    """
    WebSocket client for streaming HyperCore allMids prices.

    Mirrors the structure of PolymarketWebSocketClient in poly_client.py:
    connect → subscribe → listen, with exponential-backoff reconnect and
    callback dispatch.
    """

    RECONNECT_DELAY = 5
    MAX_RECONNECT_ATTEMPTS = 10

    def __init__(self, price_manager: HIP4PriceManager | None = None) -> None:
        self.prices = price_manager or HIP4PriceManager()
        self._ws = None
        self.running = False
        self._reconnect_attempts = 0
        self._callbacks: list[Callable[[dict[str, float]], None]] = []

    def register_callback(self, cb: Callable[[dict[str, float]], None]) -> None:
        """Register a function called on every allMids update with the filtered price dict."""
        self._callbacks.append(cb)

    async def connect(self) -> None:
        logger.info("hip4_ws_connecting", url=WS_URL)
        self._ws = await websockets.connect(WS_URL, ping_interval=20)
        self._reconnect_attempts = 0
        self.running = True
        logger.info("hip4_ws_connected")

    async def _subscribe(self) -> None:
        msg = json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}})
        await self._ws.send(msg)
        logger.info("hip4_ws_subscribed", channel="allMids")

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error("hip4_ws_json_error", error=str(e))
            return

        if msg.get("channel") != "allMids":
            return

        data = msg.get("data", {})
        mids_raw: dict = data.get("mids", data) if isinstance(data, dict) else {}
        if not mids_raw:
            return

        self.prices.update(mids_raw)
        filtered = self.prices.all_mids()

        for cb in self._callbacks:
            try:
                cb(filtered)
            except Exception as e:
                logger.error("hip4_ws_callback_error", error=str(e))

    async def listen(self) -> None:
        if not self._ws:
            raise RuntimeError("Not connected. Call connect() first.")

        logger.info("hip4_ws_listening")
        try:
            async for raw in self._ws:
                await self._handle_message(raw)
        except websockets.exceptions.ConnectionClosed:
            logger.warning("hip4_ws_closed")
            await self._reconnect()
        except Exception as e:
            logger.error("hip4_ws_error", error=str(e))
            raise

    async def _reconnect(self) -> None:
        if not self.running:
            return

        self._reconnect_attempts += 1
        if self._reconnect_attempts > self.MAX_RECONNECT_ATTEMPTS:
            logger.error("hip4_ws_max_reconnects_reached", attempts=self._reconnect_attempts)
            self.running = False
            return

        delay = min(self.RECONNECT_DELAY * (2 ** (self._reconnect_attempts - 1)), 60)
        logger.info("hip4_ws_reconnecting", attempt=self._reconnect_attempts, delay=delay)
        await asyncio.sleep(delay)

        try:
            await self.connect()
            await self._subscribe()
            await self.listen()
        except Exception as e:
            logger.error("hip4_ws_reconnect_failed", error=str(e))
            await self._reconnect()

    async def close(self) -> None:
        self.running = False
        if self._ws:
            await self._ws.close()
            self._ws = None
            logger.info("hip4_ws_closed_cleanly")

    async def start(self) -> None:
        """Connect, subscribe, and listen in one call."""
        await self.connect()
        await self._subscribe()
        await self.listen()
