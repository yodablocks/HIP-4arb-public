"""
WebSocket client for real-time Polymarket CLOB orderbook monitoring.

This module connects to Polymarket's WebSocket API to receive real-time
orderbook updates, enabling millisecond-level spread detection.
"""
import asyncio
import json
from typing import Callable, Dict, List, Optional
from datetime import datetime
import structlog
import websockets
from websockets.client import WebSocketClientProtocol

logger = structlog.get_logger()

class PolymarketWebSocketClient:
    """
    WebSocket client for Polymarket CLOB orderbook streams.

    Connects to wss://ws-subscriptions-clob.polymarket.com and subscribes
    to market channels for real-time orderbook updates.
    """

    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    RECONNECT_DELAY = 5  # seconds
    PING_INTERVAL = 30  # seconds

    def __init__(self):
        """Initialize WebSocket client."""
        self.ws: Optional[WebSocketClientProtocol] = None
        self.subscribed_assets: List[str] = []
        self.callbacks: Dict[str, List[Callable]] = {}
        self.running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10

    async def connect(self):
        """
        Connect to Polymarket WebSocket API.

        Raises:
            websockets.exceptions.WebSocketException: If connection fails
        """
        try:
            logger.info("connecting_to_websocket", url=self.WS_URL)
            self.ws = await websockets.connect(
                self.WS_URL,
                ping_interval=self.PING_INTERVAL,
                ping_timeout=10
            )
            logger.info("websocket_connected")
            self.reconnect_attempts = 0
            self.running = True

        except Exception as e:
            logger.error("websocket_connection_failed", error=str(e))
            raise

    async def subscribe_to_markets(self, asset_ids: List[str]):
        """
        Subscribe to market orderbook updates for given token IDs.

        Args:
            asset_ids: List of token IDs to subscribe to

        Message format:
        {
            "auth": {},
            "assets_ids": ["token_id_1", "token_id_2"],
            "type": "market",
            "custom_feature_enabled": false
        }
        """
        if not self.ws:
            raise RuntimeError("WebSocket not connected. Call connect() first.")

        subscribe_msg = {
            "auth": {},  # Empty for public data
            "assets_ids": asset_ids,
            "type": "market",
            "custom_feature_enabled": False
        }

        logger.info("subscribing_to_markets", asset_count=len(asset_ids))
        await self.ws.send(json.dumps(subscribe_msg))
        self.subscribed_assets.extend(asset_ids)
        logger.info("subscription_sent", assets=asset_ids)

    def register_callback(self, event_type: str, callback: Callable):
        """
        Register a callback function for a specific event type.

        Args:
            event_type: Type of event (e.g., "book", "trade")
            callback: Async function to call when event is received
                      Signature: async def callback(message: dict)
        """
        if event_type not in self.callbacks:
            self.callbacks[event_type] = []
        self.callbacks[event_type].append(callback)
        logger.info("callback_registered", event_type=event_type)

    async def _handle_message(self, message: dict):
        """
        Process incoming WebSocket message.

        Expected message format:
        {
            "event_type": "book",
            "asset_id": "token_id",
            "timestamp": 1234567890,
            "market": "condition_id",
            "hash": "0x...",
            "price": "0.45",
            "size": "100.0",
            "side": "BUY"  // or "SELL"
        }
        """
        event_type = message.get("event_type")

        if not event_type:
            logger.warning("message_missing_event_type", message=message)
            return

        # Call registered callbacks for this event type
        if event_type in self.callbacks:
            for callback in self.callbacks[event_type]:
                try:
                    await callback(message)
                except Exception as e:
                    logger.error(
                        "callback_error",
                        event_type=event_type,
                        error=str(e)
                    )

    async def listen(self):
        """
        Listen for incoming WebSocket messages and dispatch to callbacks.

        This runs in a loop until stopped or disconnected.
        """
        if not self.ws:
            raise RuntimeError("WebSocket not connected. Call connect() first.")

        logger.info("listening_for_messages")

        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)

                    # Log raw message for debugging
                    logger.debug("received_message", message_type=type(data).__name__, data=data)

                    # Handle both single messages and arrays of messages
                    if isinstance(data, list):
                        for item in data:
                            await self._handle_message(item)
                    else:
                        await self._handle_message(data)

                except json.JSONDecodeError as e:
                    logger.error("json_decode_error", error=str(e), message=message)
                except Exception as e:
                    logger.error("message_processing_error", error=str(e), message_sample=str(message)[:200])

        except websockets.exceptions.ConnectionClosed:
            logger.warning("websocket_connection_closed")
            await self._reconnect()
        except Exception as e:
            logger.error("websocket_error", error=str(e))
            raise

    async def _reconnect(self):
        """
        Attempt to reconnect to WebSocket with exponential backoff.
        """
        if not self.running:
            return

        self.reconnect_attempts += 1

        if self.reconnect_attempts > self.max_reconnect_attempts:
            logger.error(
                "max_reconnect_attempts_reached",
                attempts=self.reconnect_attempts
            )
            self.running = False
            return

        delay = min(self.RECONNECT_DELAY * (2 ** (self.reconnect_attempts - 1)), 60)
        logger.info(
            "reconnecting",
            attempt=self.reconnect_attempts,
            delay_seconds=delay
        )

        await asyncio.sleep(delay)

        try:
            await self.connect()

            # Re-subscribe to markets
            if self.subscribed_assets:
                await self.subscribe_to_markets(self.subscribed_assets)

            # Resume listening
            await self.listen()

        except Exception as e:
            logger.error("reconnect_failed", error=str(e))
            await self._reconnect()

    async def close(self):
        """Close WebSocket connection gracefully."""
        self.running = False

        if self.ws:
            logger.info("closing_websocket")
            await self.ws.close()
            self.ws = None
            logger.info("websocket_closed")

    async def start(self, asset_ids: List[str]):
        """
        Connect, subscribe, and start listening in one call.

        Args:
            asset_ids: List of token IDs to monitor

        This is a convenience method that handles the full lifecycle.
        """
        await self.connect()
        await self.subscribe_to_markets(asset_ids)
        await self.listen()


class OrderbookManager:
    """
    Maintains in-memory orderbook state from WebSocket updates.

    Polymarket WebSocket sends full orderbook snapshots (not deltas),
    so we simply store the latest snapshot for each asset.
    """

    def __init__(self):
        """Initialize orderbook manager."""
        self.orderbooks: Dict[str, Dict] = {}
        # Structure: {asset_id: {"bids": [...], "asks": [...], "timestamp": ...}}

    def update(self, message: dict):
        """
        Update orderbook with snapshot from WebSocket.

        The message format is:
        {
            "asset_id": "...",
            "bids": [{"price": "0.5", "size": "100"}, ...],
            "asks": [{"price": "0.51", "size": "200"}, ...],
            "timestamp": "1767961394134",
            "event_type": "book",
            ...
        }

        Args:
            message: WebSocket message with full orderbook snapshot
        """
        asset_id = message.get("asset_id")
        bids = message.get("bids", [])
        asks = message.get("asks", [])

        if not asset_id:
            return

        # Store the full snapshot
        self.orderbooks[asset_id] = {
            "bids": bids,
            "asks": asks,
            "timestamp": message.get("timestamp"),
            "last_trade_price": message.get("last_trade_price")
        }

    def get_best_bid_ask(self, asset_id: str) -> tuple[Optional[float], Optional[float]]:
        """
        Get current best bid and ask for an asset.

        Args:
            asset_id: Token ID

        Returns:
            Tuple of (best_bid, best_ask) or (None, None) if no data
        """
        if asset_id not in self.orderbooks:
            return None, None

        book = self.orderbooks[asset_id]

        # Best bid is the highest price (first in bids array)
        # Best ask is the lowest price (first in asks array)
        best_bid = float(book["bids"][0]["price"]) if book["bids"] else None
        best_ask = float(book["asks"][0]["price"]) if book["asks"] else None

        return best_bid, best_ask

    def get_orderbook(self, asset_id: str) -> Optional[dict]:
        """
        Get full orderbook for an asset.

        Args:
            asset_id: Token ID

        Returns:
            Dictionary with "bids" and "asks" lists, or None
        """
        return self.orderbooks.get(asset_id)
