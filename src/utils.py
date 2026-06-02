"""
Utility functions and helpers.
"""
from datetime import datetime, timezone
from dateutil import parser
import structlog

logger = structlog.get_logger()


def parse_iso_timestamp(iso_string: str) -> datetime:
    """Parse ISO 8601 timestamp to datetime object."""
    return parser.isoparse(iso_string)


def get_seconds_until(target_time: datetime) -> int:
    """Calculate seconds until target time."""
    now = datetime.now(timezone.utc)
    if target_time.tzinfo is None:
        target_time = target_time.replace(tzinfo=timezone.utc)
    delta = target_time - now
    return max(0, int(delta.total_seconds()))


def is_within_trading_hours(start_hour: int, end_hour: int) -> bool:
    """Check if current time is within trading hours (EST)."""
    # For simplicity in prototype, using UTC
    # In production, should convert to EST properly
    current_hour = datetime.utcnow().hour
    return start_hour <= current_hour < end_hour


def calculate_position_size(capital_available: float, 
                           max_position: float,
                           liquidity_available: float) -> float:
    """
    Calculate position size using conservative formula:
    Min(max_position, capital × 0.05, available_liquidity)
    """
    capital_limit = capital_available * 0.05
    return min(max_position, capital_limit, liquidity_available)


def calculate_net_price(gross_price: float, fee_rate: float, 
                       is_buy: bool = True) -> float:
    """
    Convert gross price to net price after fees.
    Fees are applied on the winning side at resolution.
    """
    # Simplified fee model for prototype
    # In practice, Polymarket fees are more complex
    if is_buy:
        return gross_price * (1 + fee_rate)
    else:
        return gross_price * (1 - fee_rate)


def calculate_spread(yes_price: float, no_price: float) -> float:
    """Calculate spread deviation from $1.00."""
    return (yes_price + no_price) - 1.0


def should_enter_trade(spread: float, threshold: float, 
                      seconds_until_resolution: int,
                      entry_window_start: int, entry_window_end: int) -> bool:
    """
    Determine if we should enter a trade based on entry conditions.
    """
    # Check spread threshold
    if abs(spread) < threshold:
        return False
    
    # Check time window (60-90 seconds before resolution)
    if not (entry_window_start <= seconds_until_resolution <= entry_window_end):
        return False
    
    return True


def get_cheaper_side(yes_price: float, no_price: float) -> str:
    """Determine which side is cheaper to buy."""
    return "YES" if yes_price < no_price else "NO"


def calculate_expected_profit(entry_price: float, exit_price: float,
                             position_size: float, fee_rate: float) -> float:
    """
    Calculate expected profit from spread compression.
    Simple spread capture: (exit_price - entry_price) × position_size × (1 - fee_rate)
    """
    shares = position_size / entry_price
    gross_profit = (exit_price - entry_price) * shares
    net_profit = gross_profit * (1 - fee_rate)
    return net_profit
