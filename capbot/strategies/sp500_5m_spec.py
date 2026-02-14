from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


@dataclass
class Signal:
    direction: str                 # "BUY" or "SELL"
    entry_price_est: float         # close[i]
    meta: Dict[str, Any]


class SP5005MSpec:
    """
    Canonical SP500 5m spec (NO VWAP, NO extra gates, NO Wilder RSI).
    - time is UTC and is the OPEN time of the candle.
    - signal uses CLOSED candle i == df.iloc[-2]
    """

    def enrich(self, d: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        d = d.copy()

        # Ensure numeric
        for c in ("open", "high", "low", "close", "volume"):
            if c in d.columns:
                d[c] = pd.to_numeric(d[c], errors="coerce")

        # Body ratio
        rng = d["high"] - d["low"]
        d["range"] = rng
        d["body_ratio"] = (d["close"] - d["open"]).abs() / rng

        # Relative volume (SMA20 including signal bar, no shift)
        vol_ma20 = d["volume"].rolling(20).mean()
        d["vol_ma20"] = vol_ma20
        d["vol_rel"] = d["volume"] / vol_ma20

        # RSI(14) SMA (not Wilder)
        delta = d["close"].diff()
        up = delta.clip(lower=0).rolling(14).mean()
        down = (-delta).clip(lower=0).rolling(14).mean()

        rs = up / down
        rsi = 100.0 - (100.0 / (1.0 + rs))

        # down==0 => RSI invalid (no signal)
        rsi = rsi.where(down != 0, np.nan)
        d["rsi14"] = rsi

        # ATR(14) (classic TR + SMA14)
        prev_close = d["close"].shift(1)
        tr1 = (d["high"] - d["low"]).abs()
        tr2 = (d["high"] - prev_close).abs()
        tr3 = (d["low"] - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        d["tr"] = tr
        d["atr14"] = tr.rolling(14).mean()

        # Previous bar count (excludes signal bar)
        bear = (d["close"] < d["open"]).astype(int)
        bull = (d["close"] > d["open"]).astype(int)
        d["bear_prev3"] = bear.shift(1).rolling(3).sum()
        d["bull_prev3"] = bull.shift(1).rolling(3).sum()

        return d

    def signal_on_bar_close(self, df: pd.DataFrame, params: Dict[str, Any]) -> Optional[Signal]:
        if df is None or getattr(df, "empty", True) or len(df) < 30:
            return None

        # Signal bar = last closed candle
        i = -2
        row = df.iloc[i]

        # time[i] is UTC open time
        ts_utc = pd.Timestamp(df.index[i])
        if ts_utc.tzinfo is None:
            ts_utc = ts_utc.tz_localize("UTC")
        else:
            ts_utc = ts_utc.tz_convert("UTC")

        # No Thursday UTC
        if int(ts_utc.weekday()) == 3:
            return None

        # RTH NY 09:30..16:00 inclusive
        ts_ny = ts_utc.tz_convert(ZoneInfo("America/New_York"))
        mins = int(ts_ny.hour) * 60 + int(ts_ny.minute)
        if not (mins >= (9 * 60 + 30) and mins <= (16 * 60 + 0)):
            return None

        # Params
        BODY_MIN = float(params.get("BODY_MIN", 0.70))
        VOL_REL_MIN = float(params.get("VOL_REL_MIN", 0.70))
        RSI_LONG_MAX = float(params.get("RSI_LONG_MAX", 75))
        RSI_SHORT_MIN = float(params.get("RSI_SHORT_MIN", 40))
        BEAR_PREV3_LONG = int(params.get("BEAR_PREV3_LONG", 2))
        BULL_PREV3_SHORT = int(params.get("BULL_PREV3_SHORT", 2))

        # Validate indicators
        need = ["range", "body_ratio", "vol_ma20", "vol_rel", "rsi14", "atr14", "bear_prev3", "bull_prev3"]
        for k in need:
            v = row.get(k)
            if v is None:
                return None
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                return None

        if float(row["range"]) <= 0:
            return None

        br = float(row["body_ratio"])
        vr = float(row["vol_rel"])
        if br < BODY_MIN or vr < VOL_REL_MIN:
            return None

        close_px = float(row["close"])
        open_px = float(row["open"])
        rsi = float(row["rsi14"])
        atr = float(row["atr14"])
        bear3 = float(row["bear_prev3"])
        bull3 = float(row["bull_prev3"])

        # Entry at close[i]
        meta = {
            "ts_signal_utc": ts_utc.isoformat(),
            "ts_signal_ny": ts_ny.isoformat(),
            "atr_entry": atr,
        }

        # BUY
        if (close_px > open_px) and (bear3 >= BEAR_PREV3_LONG) and (rsi < RSI_LONG_MAX):
            return Signal(direction="BUY", entry_price_est=close_px, meta=meta)

        # SELL
        if (close_px < open_px) and (bull3 >= BULL_PREV3_SHORT) and (rsi > RSI_SHORT_MIN):
            return Signal(direction="SELL", entry_price_est=close_px, meta=meta)

        return None


# Loader expects a class named like this in some setups
Strategy = SP5005MSpec
