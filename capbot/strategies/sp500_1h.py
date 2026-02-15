"""
SP500 1h strategy - Long-only trend following.

Entry A) SMA20 pullback: close crosses back above SMA20 after 2 bars below.
Entry B) 10-bar breakout with ATR%/BB_width/dist_SMA200/RSI filters.

Trend filter: close > SMA50 AND SMA50 > SMA200.
Exit: stop (0.8*ATR), TP (2.5R), trend break (close < SMA50), time (80 bars).
"""
from __future__ import annotations

import math
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


def _atr_sma(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    prev_c = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return _sma(tr, n)


class SP500_1H:
    """SP500 1h trend-following strategy (long-only)."""

    def enrich(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        d = df.copy()
        c = d["close"].astype(float)
        h = d["high"].astype(float)

        d["sma20"] = _sma(c, 20)
        d["sma50"] = _sma(c, 50)
        d["sma200"] = _sma(c, 200)
        d["atr14"] = _atr_sma(d, 14)
        d["rsi14"] = _rsi_sma(c, 14)

        # Bollinger Bands (20, 2)
        bb_mid = d["sma20"]
        bb_std = c.rolling(20, min_periods=20).std()
        d["bb_upper"] = bb_mid + 2 * bb_std
        d["bb_lower"] = bb_mid - 2 * bb_std
        d["bb_width"] = (d["bb_upper"] - d["bb_lower"]) / bb_mid.replace(0, np.nan)

        # ATR%
        d["atr_pct"] = d["atr14"] / c.replace(0, np.nan)

        # Distance from SMA200
        d["dist_sma200_pct"] = (c - d["sma200"]) / d["sma200"].replace(0, np.nan)

        # 10-bar breakout: close > max(high[i-10 .. i-1])
        d["high_10"] = h.shift(1).rolling(10, min_periods=10).max()
        d["breakout_10"] = (c > d["high_10"]).astype(int)

        return d

    def signal_on_bar_close(self, df: pd.DataFrame, params: Dict[str, Any]) -> Optional[Signal]:
        if df is None or len(df) < 210:
            return None

        i = -2
        row = df.iloc[i]
        ts_utc = df.index[i]

        # Robust tz handling
        ts_utc = pd.to_datetime(ts_utc, utc=True, errors="coerce")
        if ts_utc is pd.NaT:
            return None

        # US/Eastern RTH: 09:30-16:00 (inclusive), no weekends
        ts_et = ts_utc.tz_convert("America/New_York")
        if ts_et.weekday() >= 5:
            return None
        hhmm = (ts_et.hour, ts_et.minute)
        if not ((hhmm >= (9, 30)) and (hhmm <= (16, 0))):
            return None

        # Validate indicators
        need = ["sma20", "sma50", "sma200", "atr14", "rsi14", "bb_width", "atr_pct", "dist_sma200_pct", "high_10"]
        for k in need:
            v = row.get(k)
            if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
                return None

        c = float(row["close"])
        sma50 = float(row["sma50"])
        sma200 = float(row["sma200"])

        # Trend filter (mandatory): close > SMA50 AND SMA50 > SMA200
        if not (c > sma50 and sma50 > sma200):
            return None

        sma20 = float(row["sma20"])

        # Entry A: SMA20 pullback
        # close[i-1] < sma20[i-1] AND close[i-2] < sma20[i-2] AND close[i] > sma20[i]
        entry_a = False
        if len(df) >= 4:
            prev1 = df.iloc[i - 1]
            prev2 = df.iloc[i - 2]
            p1_ok = float(prev1.get("close", np.nan)) < float(prev1.get("sma20", np.nan)) if not np.isnan(float(prev1.get("sma20", np.nan))) else False
            p2_ok = float(prev2.get("close", np.nan)) < float(prev2.get("sma20", np.nan)) if not np.isnan(float(prev2.get("sma20", np.nan))) else False
            if p1_ok and p2_ok and c > sma20:
                entry_a = True

        # Entry B: 10-bar breakout with filters
        entry_b = False
        if int(row.get("breakout_10", 0)) == 1:
            atr_pct = float(row["atr_pct"])
            bb_w = float(row["bb_width"])
            dist_200 = float(row["dist_sma200_pct"])
            rsi = float(row["rsi14"])
            if atr_pct <= 0.0045 and bb_w <= 0.03 and dist_200 <= 0.05 and rsi <= 70:
                entry_b = True

        if not (entry_a or entry_b):
            return None

        meta = {
            "entry_type": "SMA20_PULLBACK" if entry_a else "BREAKOUT_10",
            "ts_signal_utc": ts_utc.isoformat(),
            "ts_signal_et": ts_et.isoformat(),
            "atr14": float(row["atr14"]),
            "rsi14": float(row["rsi14"]),
            "sma20": sma20,
            "sma50": sma50,
        }

        return Signal(direction="BUY", entry_price_est=c, meta=meta)

    def initial_risk(self, entry_price: float, atr_signal: float, sig: Signal, params: Dict[str, Any]) -> Dict[str, Any]:
        """Stop = entry - 0.8*ATR, TP = entry + 2.5R."""
        stop_dist = 0.8 * float(atr_signal)
        r_points = stop_dist
        sl = entry_price - stop_dist
        tp = entry_price + 2.5 * r_points

        return {
            "atr_entry": float(atr_signal),
            "r_points": r_points,
            "sl_local": float(sl),
            "tp_local": float(tp),
            "tp_r_multiple": 2.5,
            "exit_bars": 80,
        }
