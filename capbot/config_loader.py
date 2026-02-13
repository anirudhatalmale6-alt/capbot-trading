from __future__ import annotations

import os
from capbot.config import BotConfig


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def load_bot_config(symbol: str, timeframe: str) -> BotConfig:
    """Minimal loader via env vars."""
    return BotConfig(
        symbol=symbol,
        timeframe=timeframe,
        rth_only=_env_bool("CAPBOT_RTH_ONLY", True),
        session_tz=os.getenv("CAPBOT_SESSION_TZ", "America/New_York"),
        session_start_hhmm=os.getenv("CAPBOT_SESSION_START", "0930"),
        session_end_hhmm=os.getenv("CAPBOT_SESSION_END", "1600"),
        evaluate_signals_outside_rth=_env_bool("CAPBOT_EVAL_SIGNALS_OUTSIDE_RTH", False),
    )
