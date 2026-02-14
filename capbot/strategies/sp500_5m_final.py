from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Optional

import pandas as pd


# SP500 5m Final Strategy (per spec)
# - Signal on closed bar i (engine evaluates df.iloc[-2])
# - Filters on signal bar i:
#     * RTH NY 09:30-16:00 (inclusive), weekdays only
#     * Thursday UTC disabled only if disable_thursday_utc=True
# - RSI(14): SMA14(up)/SMA14(down). If down==0 => RSI invalid (NaN)
# - ATR(14): SMA14(TR)
# - vol_ma20 includes volume[i] (no shift)
# - prev3 excludes signal bar (shift(1))


@dataclass
class _Sig:
    direction: str        # "BUY" | "SELL"
    ts: pd.Timestamp      # signal bar timestamp (UTC)
    entry_price: float
    atr_entry: float


class SP500_5M_FINAL:
    name = "sp500_5m_final"

    def __init__(self, **params):
        self.params = params or {}

    @staticmethod
    def _ensure_time_index(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if isinstance(df.index, pd.DatetimeIndex):
            idx = df.index
            if idx.tz is None:
                df.index = idx.tz_localize("UTC")
            else:
                df.index = idx.tz_convert("UTC")
            return df

        if "time" in df.columns:
            t = pd.to_datetime(df["time"], utc=True, errors="coerce")
            if t.notna().any():
                df.index = t
                return df

        # Last resort: try converting numeric index (epoch s/ms)
        try:
            v = int(df.index[-1])
            unit = "ms" if v > 10**11 else "s"
            df.index = pd.to_datetime(df.index.astype("int64"), unit=unit, utc=True, errors="coerce")
        except Exception:
            raise ValueError("Need df['time'] or DatetimeIndex (UTC)")

        return df

    @staticmethod
    def _isnum(x: Any) -> bool:
        try:
            return (x == x) and (not math.isinf(float(x)))
        except Exception:
            return False

    @staticmethod
    def enrich(df: pd.DataFrame, params: dict | None = None) -> pd.DataFrame:
        params = params or {}
        df = df.copy()

        for c in ("open", "high", "low", "close", "volume"):
            if c not in df.columns:
                raise ValueError(f"Missing column: {c}")

        df = SP500_5M_FINAL._ensure_time_index(df)

        o = df["open"].astype("float64")
        h = df["high"].astype("float64")
        l = df["low"].astype("float64")
        c = df["close"].astype("float64")
        v = df["volume"].astype("float64")

        # 3.1 body_ratio
        rng = (h - l)
        df["range"] = rng
        body = (c - o).abs() / rng.replace(0, pd.NA)
        body = body.where(rng > 0)
        df["body_ratio"] = body

        # 3.2 vol_rel (SMA20 includes volume[i])
        vol_ma20 = v.rolling(20, min_periods=20).mean()
        df["vol_ma20"] = vol_ma20
        df["vol_rel"] = v / vol_ma20.replace(0, pd.NA)

        # 3.3 RSI(14) SMA14(up)/SMA14(down)
        delta = c.diff()
        up = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        up_sma = up.rolling(14, min_periods=14).mean()
        down_sma = down.rolling(14, min_periods=14).mean()

        # down==0 => RSI invalid
        rs = up_sma / down_sma.replace(0, pd.NA)
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.where(down_sma != 0)
        df["rsi14"] = rsi

        # 3.4 ATR(14) SMA14(TR)
        prev_close = c.shift(1)
        tr = pd.concat(
            [(h - l), (h - prev_close).abs(), (l - prev_close).abs()],
            axis=1
        ).max(axis=1)
        df["tr"] = tr
        df["atr14"] = tr.rolling(14, min_periods=14).mean()

        # 3.5 prev3 excludes signal bar
        bear = (c < o).astype("int64")
        bull = (c > o).astype("int64")
        df["bear_prev3"] = bear.shift(1).rolling(3, min_periods=3).sum()
        df["bull_prev3"] = bull.shift(1).rolling(3, min_periods=3).sum()

        return df

    @staticmethod
    def signal_on_row(df: pd.DataFrame, i: int, params: dict | None = None) -> Optional[_Sig]:
        params = params or {}
        disable_thursday_utc = bool(params.get("disable_thursday_utc", False))

        # thresholds spec
        BODY_MIN = 0.70
        VOL_REL_MIN = 0.70
        RSI_LONG_MAX = 75.0
        RSI_SHORT_MIN = 40.0

        row = df.iloc[i]
        ts_utc = pd.Timestamp(df.index[i]).tz_convert("UTC")

        # Session filters (signal bar i)
        ts_ny = ts_utc.tz_convert("America/New_York")
        if ts_ny.weekday() >= 5:
            return None
        hhmm = (ts_ny.hour, ts_ny.minute)
        rth_ok = (hhmm >= (9, 30)) and (hhmm <= (16, 0))

        thu_ok = True if (not disable_thursday_utc) else (ts_utc.weekday() != 3)

        # Validate indicators
        o = float(row["open"]); c = float(row["close"]); h = float(row["high"]); l = float(row["low"])
        rng = float(row.get("range", h - l))
        body = float(row.get("body_ratio", float("nan")))
        vol_rel = float(row.get("vol_rel", float("nan")))
        rsi = float(row.get("rsi14", float("nan")))
        atr = float(row.get("atr14", float("nan")))
        bear3 = row.get("bear_prev3", None)
        bull3 = row.get("bull_prev3", None)

        valid = (
            SP500_5M_FINAL._isnum(o) and SP500_5M_FINAL._isnum(c) and SP500_5M_FINAL._isnum(h) and SP500_5M_FINAL._isnum(l)
            and SP500_5M_FINAL._isnum(rng) and rng > 0
            and SP500_5M_FINAL._isnum(body)
            and SP500_5M_FINAL._isnum(vol_rel)
            and SP500_5M_FINAL._isnum(rsi)
            and SP500_5M_FINAL._isnum(atr)
            and (bear3 is not None) and (bull3 is not None)
        )
        if not valid:
            return None

        if not (rth_ok and thu_ok):
            return None

        if not (body >= BODY_MIN and vol_rel >= VOL_REL_MIN):
            return None

        # Entry conditions
        long_ok = (c > o) and (float(bear3) >= 2) and (rsi < RSI_LONG_MAX)
        short_ok = (c < o) and (float(bull3) >= 2) and (rsi > RSI_SHORT_MIN)

        if long_ok:
            return _Sig(direction="BUY", ts=ts_utc, entry_price=c, atr_entry=atr)
        if short_ok:
            return _Sig(direction="SELL", ts=ts_utc, entry_price=c, atr_entry=atr)

        return None

    @staticmethod
    def signal(df: pd.DataFrame, params: dict | None = None) -> Optional[_Sig]:
        # If df already has enriched columns (from engine), skip recalculation.
        params = params or {}

        needed = {"body_ratio", "vol_rel", "rsi14", "atr14", "bear_prev3", "bull_prev3"}
        df2 = df if needed.issubset(set(df.columns)) else SP500_5M_FINAL.enrich(df, params)

        if df2 is None or df2.empty or len(df2) < 3:
            return None

        return SP500_5M_FINAL.signal_on_row(df2, -2, params)
