"""
NVDA 1h strategy - Mean reversion long + short with EMA72 regime filter.

Entry LONG: close < BB_lower(20,2) AND RSI14 < 30 AND regime OK.
Entry SHORT: close > BB_upper(20,2) AND RSI14 > 70 AND regime OK.
Regime filter: |EMA72(t)/EMA72(t-12h) - 1| <= 1%.
Exit: close crosses BB_mid (mean reversion) OR time stop (6 candles).
Hard filters: no Monday (UTC), no earnings days.
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


class NVDA_1H:
    """NVDA 1h mean reversion strategy (long + short, with EMA72 regime filter)."""

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

        # EMA72
        d["ema72"] = c.ewm(span=72, adjust=False).mean()

        # EMA72 12h ago (12 bars on 1h timeframe)
        d["ema72_12h_ago"] = d["ema72"].shift(12)

        # Regime filter: |EMA72(t)/EMA72(t-12h) - 1| <= 0.01
        ratio = d["ema72"] / d["ema72_12h_ago"].replace(0, np.nan)
        d["regime_ok"] = ((ratio - 1.0).abs() <= 0.01).astype(int)

        return d

    def signal_on_bar_close(self, df: pd.DataFrame, params: Dict[str, Any]) -> Optional[Signal]:
        if df is None or len(df) < 80:
            return None

        i = -2
        row = df.iloc[i]
        ts_utc = df.index[i]

        ts_utc = pd.to_datetime(ts_utc, utc=True, errors="coerce")
        if ts_utc is pd.NaT:
            return None

        # Hard filter: no Monday (UTC)
        if ts_utc.weekday() == 0:
            return None

        # Hard filter: no earnings days (configurable)
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
        need = ["bb_low_20", "bb_mid_20", "bb_up_20", "rsi14", "regime_ok"]
        for k in need:
            v = row.get(k)
            if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
                return None

        # Regime filter must pass
        if int(row["regime_ok"]) != 1:
            return None

        c = float(row["close"])
        bb_low = float(row["bb_low_20"])
        bb_up = float(row["bb_up_20"])
        bb_mid = float(row["bb_mid_20"])
        rsi = float(row["rsi14"])

        meta = {
            "ts_signal_utc": ts_utc.isoformat(),
            "ts_signal_et": ts_et.isoformat(),
            "bb_low_20": bb_low,
            "bb_mid_20": bb_mid,
            "bb_up_20": bb_up,
            "rsi14": rsi,
            "regime_ok": True,
        }

        # LONG: close < BB lower AND RSI < 30
        if c < bb_low and rsi < 30:
            meta["tp_target"] = bb_mid
            return Signal(direction="BUY", entry_price_est=c, meta=meta)

        # SHORT: close > BB upper AND RSI > 70
        if c > bb_up and rsi > 70:
            meta["tp_target"] = bb_mid
            return Signal(direction="SELL", entry_price_est=c, meta=meta)

        return None

    def initial_risk(self, entry_price: float, atr_signal: float, sig: Signal, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        NVDA uses 100% capital, exit at BB mid or 6-candle time stop.
        No fixed SL - set a wide safety SL (5% from entry).
        TP is BB_mid.
        """
        bb_mid = sig.meta.get("bb_mid_20", entry_price)

        if sig.direction == "BUY":
            safety_sl = entry_price * 0.95
            tp = float(bb_mid)
        else:
            safety_sl = entry_price * 1.05
            tp = float(bb_mid)

        return {
            "r_points": abs(entry_price - safety_sl),
            "sl_local": float(safety_sl),
            "tp_local": tp,
            "tp_r_multiple": 1.0,
            "exit_bars": 6,
        }
