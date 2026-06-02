"""
Spread detection algorithm - First Milestone.

Connects to Polymarket markets, calculates YES+NO sum, 
and logs when sum deviates >1% from $1.00.
"""
import asyncio
from datetime import datetime
from typing import Dict, Optional
import structlog
from database import Database
from utils import (
    calculate_spread, 
    get_seconds_until, 
    parse_iso_timestamp,
    should_enter_trade
)

logger = structlog.get_logger()


class SpreadDetector:
    """
    Detects spread opportunities in Polymarket markets.
    
    Key insight: YES + NO must equal $1.00 at resolution.
    When sum deviates >1%, an arbitrage opportunity exists.
    """
    
    def __init__(self, db: Database, spread_threshold: float = 0.01,
                 entry_window_start: int = 60, entry_window_end: int = 90):
        self.db = db
        self.spread_threshold = spread_threshold
        self.entry_window_start = entry_window_start
        self.entry_window_end = entry_window_end
        
        # Track last detection time to avoid spam
        self.last_detection: Dict[str, datetime] = {}
    
    async def analyze_market(self, market_id: str, market_type: str,
                            yes_price: float, no_price: float,
                            resolution_time: Optional[datetime]) -> bool:
        """
        Analyze a market for spread opportunities.
        
        Returns True if opportunity detected, False otherwise.
        """
        spread = calculate_spread(yes_price, no_price)
        
        # Calculate time until resolution
        if resolution_time:
            seconds_until = get_seconds_until(resolution_time)
        else:
            seconds_until = 0
        
        # Check if this is an opportunity
        is_opportunity = should_enter_trade(
            spread, 
            self.spread_threshold,
            seconds_until,
            self.entry_window_start,
            self.entry_window_end
        )
        
        if is_opportunity:
            await self._log_opportunity(
                market_id, market_type, yes_price, no_price, 
                spread, seconds_until
            )
            return True
        
        return False
    
    async def _log_opportunity(self, market_id: str, market_type: str,
                              yes_price: float, no_price: float,
                              spread: float, seconds_until: int):
        """Log detected opportunity to database and console."""
        
        # Rate limit logging (don't spam for same market)
        now = datetime.utcnow()
        if market_id in self.last_detection:
            delta = (now - self.last_detection[market_id]).total_seconds()
            if delta < 5:  # Only log every 5 seconds per market
                return
        
        self.last_detection[market_id] = now
        
        # Log to database
        await self.db.log_opportunity(
            market_id=market_id,
            market_type=market_type,
            yes_price=yes_price,
            no_price=no_price,
            time_until_resolution=seconds_until,
            traded=False
        )
        
        # Log to console
        logger.info(
            "spread_opportunity_detected",
            market_id=market_id,
            market_type=market_type,
            yes_price=yes_price,
            no_price=no_price,
            sum=yes_price + no_price,
            spread=spread,
            spread_pct=spread * 100,
            seconds_until_resolution=seconds_until,
            opportunity_type="OVERPRICED" if spread > 0 else "UNDERPRICED"
        )
    
    async def get_today_summary(self) -> Dict:
        """Get summary of opportunities detected today."""
        stats = await self.db.get_daily_stats()
        return stats


# Example usage for testing
async def test_spread_detector():
    """Test the spread detector with sample data."""
    db = Database("data/test_polybot.db")
    await db.connect()
    
    detector = SpreadDetector(db, spread_threshold=0.01)
    
    # Test case 1: Overpriced market (sum > $1.00)
    print("\nTest 1: Overpriced market")
    resolution_time = datetime.utcnow()
    resolution_time = resolution_time.replace(second=resolution_time.second + 75)
    
    await detector.analyze_market(
        market_id="test_btc_1",
        market_type="BTC_15min",
        yes_price=0.54,
        no_price=0.49,
        resolution_time=resolution_time
    )
    
    # Test case 2: Underpriced market (sum < $1.00)
    print("\nTest 2: Underpriced market")
    await detector.analyze_market(
        market_id="test_btc_2",
        market_type="BTC_15min",
        yes_price=0.47,
        no_price=0.52,
        resolution_time=resolution_time
    )
    
    # Test case 3: Fair price (sum ≈ $1.00)
    print("\nTest 3: Fair price - should not trigger")
    await detector.analyze_market(
        market_id="test_btc_3",
        market_type="BTC_15min",
        yes_price=0.50,
        no_price=0.50,
        resolution_time=resolution_time
    )
    
    # Get summary
    summary = await detector.get_today_summary()
    print(f"\nToday's Summary: {summary}")
    
    await db.close()


if __name__ == "__main__":
    asyncio.run(test_spread_detector())
