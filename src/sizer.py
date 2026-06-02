"""
Position sizer.

Kelly fraction on edge, hard-capped at max_position_usdh.
Binary outcome: p = implied win probability, b = net odds (1:1 payout minus entry cost).
"""

from __future__ import annotations


def kelly_size(
    edge: float,
    entry_price: float,
    bankroll: float,
    max_position_usdh: float,
    kelly_fraction: float = 0.25,
) -> float:
    """
    Return position size in USDH.

    Binary Kelly: f = (p*b - (1-p)) / b
      p = win probability ≈ entry_price adjusted by edge
      b = net payout per unit wagered = (1 - entry_price) / entry_price

    kelly_fraction < 1.0 applies fractional Kelly for variance reduction.
    Result is clamped to [0, max_position_usdh].
    """
    if entry_price <= 0.0 or entry_price >= 1.0:
        return 0.0
    if bankroll <= 0.0 or edge <= 0.0:
        return 0.0

    # Implied win prob from the price we're paying; edge shifts it in our favour
    p = entry_price + edge
    p = min(p, 0.999)

    # Net odds: win (1 - entry_price) per unit staked, lose entry_price
    b = (1.0 - entry_price) / entry_price

    full_kelly = (p * b - (1.0 - p)) / b
    if full_kelly <= 0.0:
        return 0.0

    raw = bankroll * kelly_fraction * full_kelly
    return min(raw, max_position_usdh)
