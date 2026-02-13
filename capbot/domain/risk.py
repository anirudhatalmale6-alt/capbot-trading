import math
from typing import Optional


def _isfinite(x: float) -> bool:
    try:
        return math.isfinite(float(x))
    except Exception:
        return False


def calc_position_size(
    bot_equity: float,
    risk_pct: float,
    r_points: float,
    value_per_point_per_size: float,
    *,
    min_size: float = 1.0,
    max_size: Optional[float] = None,
) -> float:
    """
    Position sizing by fixed fractional risk.

    risk_cash = bot_equity * risk_pct
    denom = r_points * value_per_point_per_size
    size = floor(risk_cash / denom)

    Robustness:
      - clamps risk_pct to [0, 1]
      - guards NaN/inf/<=0 inputs
      - optional min_size / max_size
    """
    # sanitize inputs
    bot_equity = float(bot_equity) if _isfinite(bot_equity) else 0.0
    risk_pct = float(risk_pct) if _isfinite(risk_pct) else 0.0
    r_points = float(r_points) if _isfinite(r_points) else 0.0
    vpps = float(value_per_point_per_size) if _isfinite(value_per_point_per_size) else 0.0

    risk_pct = max(0.0, min(1.0, risk_pct))
    min_size = float(min_size) if _isfinite(min_size) else 1.0
    min_size = max(0.0, min_size)

    if max_size is not None:
        max_size = float(max_size) if _isfinite(max_size) else None
        if max_size is not None and max_size < min_size:
            max_size = min_size

    if bot_equity <= 0 or risk_pct <= 0 or r_points <= 0 or vpps <= 0:
        return float(min_size)

    risk_cash = bot_equity * risk_pct
    denom = r_points * vpps
    if denom <= 0:
        return float(min_size)

    raw = risk_cash / denom
    if not _isfinite(raw) or raw <= 0:
        return float(min_size)

    size = math.floor(raw)
    size = max(min_size, float(size))

    if max_size is not None:
        size = min(float(max_size), float(size))

    return float(size)
