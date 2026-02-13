from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

Direction = Literal["BUY", "SELL"]

@dataclass(frozen=True)
class Signal:
    direction: Direction
    entry_price_est: float
    meta: Dict[str, Any]

@dataclass
class PositionState:
    deal_id: str
    direction: Direction
    size: float
    entry_time_iso: str
    entry_price_est: float
    entry_price_api: Optional[float]
    initial_sl: float
    r_points: float
    sl_local: float
    tp_local: float
    planned_exit_iso: str
    trail_1r_done: bool = False
    trail_2r_done: bool = False
    meta: Dict[str, Any] = None
