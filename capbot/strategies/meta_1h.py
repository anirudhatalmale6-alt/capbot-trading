"""
META 1h strategy - Long-only intraday mean reversion.

Entry: close < BB_lower(20,2) AND RSI14 < 30.
Exit: close >= BB_mid(20,2) OR time stop (6 candles).
Hard filters: no Wednesday (UTC), no earnings day (configurable).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


@dataclass
class Signal:
    direction: str
    entry_price_est: float
    meta: Dict[str, Any]


def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()


def _rsi_sma(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    up_avg = _sma(up, n)
    down_avg = _sma(down, n)
    rs = up_avg / down_avg.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


class META_1H:
    """META 1h mean reversion strategy (long-only, Bollinger + RSI)."""

    def enrich(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        d = df.copy()
        c = d["close"].astype(float)

        # Bollinger Bands (20, 2)
        d["bb_mid_20"] = _sma(c, 20)
        bb_std = c.rolling(20, min_periods=20).std()
        d["bb_low_20"] = d["bb_mid_20"] - 2 * bb_std
        d["bb_up_20"] = d["bb_mid_20"] + 2 * bb_std

        # RSI(14) SMA
        d["rsi14"] = _rsi_sma(c, 14)

        # EMA72 (informational)
        d["ema72"] = c.ewm(span=72, adjust=False).mean()

        return d

    def signal_on_bar_close(self, df: pd.DataFrame, params: Dict[str, Any]) -> Optional[Signal]:
        if df is None or len(df) < 30:
            return None

        i = -2
        row = df.iloc[i]
        ts_utc = df.index[i]

        ts_utc = pd.to_datetime(ts_utc, utc=True, errors="coerce")
        if ts_utc is pd.NaT:
            return None

        # Hard filter: no Wednesday (UTC)
        if ts_utc.weekday() == 2:
            return None

        # Hard filter: no earnings days (configurable list of dates)
        earnings_dates = params.get("earnings_blackout_dates", [])
        today_str = ts_utc.strftime("%Y-%m-%d")
        if today_str in earnings_dates:
            return None

        # RTH check: US market hours
        ts_et = ts_utc.tz_convert("America/New_York")
        if ts_et.weekday() >= 5:
            return None
        hhmm = (ts_et.hour, ts_et.minute)
        if not ((hhmm >= (9, 30)) and (hhmm <= (16, 0))):
            return None

        # Validate indicators
        need = ["bb_low_20", "bb_mid_20", "rsi14"]
        for k in need:
            v = row.get(k)
            if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
                return None

        c = float(row["close"])
        bb_low = float(row["bb_low_20"])
        rsi = float(row["rsi14"])

        # Entry: close < BB lower AND RSI < 30
        if c < bb_low and rsi < 30:
            meta = {
                "ts_signal_utc": ts_utc.isoformat(),
                "ts_signal_et": ts_et.isoformat(),
                "bb_low_20": bb_low,
                "bb_mid_20": float(row["bb_mid_20"]),
                "rsi14": rsi,
            }
            return Signal(direction="BUY", entry_price_est=c, meta=meta)

        return None

    def initial_risk(self, entry_price: float, atr_signal: float, sig: Signal, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        META uses 100% capital, exit at BB mid or 6-candle time stop.
        No fixed SL - set a wide SL as safety net (5% below entry).
        TP is BB_mid (handled by engine via tp_local).
        """
        bb_mid = sig.meta.get("bb_mid_20", entry_price * 1.01)
        safety_sl = entry_price * 0.95

        return {
            "r_points": abs(entry_price - safety_sl),
            "sl_local": float(safety_sl),
            "tp_local": float(bb_mid),
            "tp_r_multiple": 1.0,
            "exit_bars": 6,
        }
