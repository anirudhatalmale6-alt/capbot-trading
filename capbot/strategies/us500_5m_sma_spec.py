from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

@dataclass
class Signal:
    direction: str        # "BUY" | "SELL"
    entry_price_est: float
    meta: Dict[str, Any]

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def rsi_sma(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    up_sma = _sma(up, n)
    down_sma = _sma(down, n)

    # down==0 => RSI invalid (no signal)
    rs = up_sma / down_sma.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi

def atr_sma(df: pd.DataFrame, n: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)

    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    return _sma(tr, n)

class US500_5m_SMA_SPEC:
    """
    SP500 5m strategy (SMA-based indicators):
    - RTH gate NY 09:30-16:00, no Thursday UTC
    - RSI/ATR with SMA (not Wilder)
    - Entry at CLOSE of signal bar
    - Management: TP first, then SL; trailing end-of-bar + BE lock; time-exit 24 bars
    """

    def enrich(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        d = df.copy()

        # Body ratio
        rng = (d["high"] - d["low"]).astype(float)
        d["range"] = rng
        d["body_ratio"] = (d["close"] - d["open"]).abs() / rng.replace(0, np.nan)

        # Relative volume (SMA20 including current bar, no shift)
        vol = d["volume"].astype(float)
        d["vol_ma20"] = vol.rolling(20).mean()
        d["vol_rel"] = vol / d["vol_ma20"].replace(0, np.nan)

        # Prev3 excludes signal bar: shift(1).rolling(3).sum()
        bear = (d["close"] < d["open"]).astype(int)
        bull = (d["close"] > d["open"]).astype(int)
        d["bear_prev3"] = bear.shift(1).rolling(3).sum()
        d["bull_prev3"] = bull.shift(1).rolling(3).sum()

        # RSI/ATR SMA
        d["rsi14"] = rsi_sma(d["close"].astype(float), 14)
        d["atr14"] = atr_sma(d, 14)

        return d

    def signal_on_bar_close(self, df: pd.DataFrame, params: Dict[str, Any]) -> Optional[Signal]:
        if df is None or len(df) < 50:
            return None

        # Signal bar = last closed candle
        i = -2
        row = df.iloc[i]
        ts_utc = df.index[i]

        # Robust tz handling (fix int/naive index)
        if isinstance(ts_utc, (int, float)):
            v = int(ts_utc)
            unit = 'ms' if v > 10_000_000_000 else 's'
            ts_utc = pd.to_datetime(v, unit=unit, utc=True, errors='coerce')
        else:
            ts_utc = pd.to_datetime(ts_utc, utc=True, errors='coerce')
        if ts_utc is pd.NaT:
            return None
        ts_ny = ts_utc.tz_convert('America/New_York')

        # RTH NY: 09:30 <= hh:mm <= 16:00 (inclusive)
        hhmm = (ts_ny.hour, ts_ny.minute)
        in_rth = (hhmm >= (9, 30)) and (hhmm <= (16, 0))
        if not in_rth:
            return None

        # No Thursday UTC
        if ts_utc.weekday() == 3:
            return None

        # Validate indicators
        need = ["body_ratio", "vol_ma20", "vol_rel", "rsi14", "atr14", "bear_prev3", "bull_prev3"]
        for k in need:
            v = row.get(k)
            if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
                return None

        if float(row["range"]) <= 0:
            return None

        br = float(row["body_ratio"])
        vr = float(row["vol_rel"])
        if br < 0.70 or vr < 0.70:
            return None

        close_px = float(row["close"])
        open_px  = float(row["open"])
        rsi = float(row["rsi14"])
        bear3 = float(row["bear_prev3"])
        bull3 = float(row["bull_prev3"])

        meta = {
            "ts_signal_utc": ts_utc.isoformat(),
            "ts_signal_ny": ts_ny.isoformat(),
            "close": close_px, "open": open_px,
            "body_ratio": br, "vol_rel": vr,
            "rsi14": rsi, "atr14": float(row["atr14"]),
        }

        # LONG
        if (close_px > open_px) and (bear3 >= 2) and (rsi < 75):
            meta["bear_prev3"] = bear3
            return Signal(direction="BUY", entry_price_est=close_px, meta=meta)

        # SHORT
        if (close_px < open_px) and (bull3 >= 2) and (rsi > 40):
            meta["bull_prev3"] = bull3
            return Signal(direction="SELL", entry_price_est=close_px, meta=meta)

        return None

    def initial_risk(self, entry_price: float, atr_signal: float, sig: Signal, params: Dict[str, Any]) -> Dict[str, Any]:
        """SL/TP with ATR_entry: BUY SL=entry-1*ATR, TP=entry+3*ATR; SELL reversed."""
        atr_entry = float(atr_signal)
        r_points = atr_entry
        if sig.direction == "BUY":
            sl = entry_price - atr_entry
            tp = entry_price + 3.0 * atr_entry
        else:
            sl = entry_price + atr_entry
            tp = entry_price - 3.0 * atr_entry

        return {
            "atr_entry": atr_entry,
            "r_points": r_points,
            "sl_local": float(sl),
            "tp_local": float(tp),
            "exit_bars": 24,
        }
