from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from capbot.strategies.vwap_pullback_rsi import (
    Signal,
    rsi_wilder,
    atr_wilder,
    vwap_intraday_reset_berlin,
)


def _get_ts_utc(df: pd.DataFrame, i: int) -> pd.Timestamp:
    """Return tz-aware UTC timestamp for bar at index i."""
    if isinstance(df.index, pd.DatetimeIndex):
        ts = df.index[i]
        if df.index.tz is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    if "time" in df.columns:
        ts = pd.to_datetime(df["time"].iloc[i], utc=True, errors="coerce")
        if ts is pd.NaT:
            raise ValueError("Cannot parse df['time'] to datetime")
        return ts

    raise ValueError("Requires df.index DatetimeIndex or 'time' column")


class DE40VWAPK020:
    """
    DE40/GER40 5m strategy: Daily VWAP + VWAP distance filter (k=0.20).
    Signal on closed bar; entry on next bar open (handled by engine).
    """

    def enrich(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        out = df.copy()

        vol_window = int(params.get("VOL_WINDOW", 20))
        rsi_len = int(params.get("RSI_LEN", params.get("RSI_PERIOD", 14)))
        atr_len = int(params.get("ATR_LEN", params.get("ATR_PERIOD", 14)))
        tz = str(params.get("VWAP_TZ", "Europe/Berlin"))

        # Body ratio
        rng = (out["high"] - out["low"]).replace(0, np.nan)
        out["body_ratio"] = (out["close"] - out["open"]).abs() / rng
        out["body_ratio"] = out["body_ratio"].replace([np.inf, -np.inf], np.nan).fillna(0.0)

        # Volume relative to SMA
        out["vol_sma"] = out["volume"].rolling(vol_window).mean()
        out["vol_rel"] = out["volume"] / out["vol_sma"].replace(0, np.nan)

        # Prev 3 bars: count of bears/bulls (shifted by 1)
        is_bear = (out["close"] < out["open"]).astype(int)
        is_bull = (out["close"] > out["open"]).astype(int)
        out["bear_prev3"] = is_bear.shift(1).rolling(3).sum()
        out["bull_prev3"] = is_bull.shift(1).rolling(3).sum()

        # RSI / ATR (Wilder smoothing)
        out["rsi14"] = rsi_wilder(out["close"], rsi_len)
        out["atr14"] = atr_wilder(out, atr_len)

        # Intraday VWAP (resets at 00:00 local time)
        out["vwap"] = vwap_intraday_reset_berlin(out, tz)

        return out

    def signal_on_bar_close(self, df: pd.DataFrame, params: Dict[str, Any]) -> Optional[Signal]:
        if df is None or len(df) < 50:
            return None

        # Use the last CLOSED bar (iloc[-2])
        i = -2
        row = df.iloc[i]

        tz = str(params.get("VWAP_TZ", "Europe/Berlin"))
        ts_utc = _get_ts_utc(df, i)
        if ts_utc is None or pd.isna(ts_utc):
            ts_utc = pd.Timestamp.utcnow().tz_localize("UTC")
        ts_local = ts_utc.tz_convert(tz)

        # Strategy parameters
        BODY_MIN = float(params.get("BODY_MIN", 0.70))
        VOL_REL_MIN = float(params.get("VOL_REL_MIN", 0.70))
        RSI_LONG_MAX = float(params.get("RSI_LONG_MAX", 75))
        RSI_SHORT_MIN = float(params.get("RSI_SHORT_MIN", 40))
        BEAR_PREV3_LONG = int(params.get("BEAR_PREV3_LONG", 2))
        BULL_PREV3_SHORT = int(params.get("BULL_PREV3_SHORT", 2))
        VWAP_DISTANCE_K = float(params.get("VWAP_DISTANCE_K", 0.20))

        # Schedule gates (strategy-level, optional overrides)
        disable_thursday_utc = bool(params.get("DISABLE_THURSDAY_UTC", True))
        no_trade_hours = set(params.get("NO_TRADE_HOURS_BERLIN", []))
        rth_start = str(params.get("RTH_START", "09:30"))
        rth_end = str(params.get("RTH_END", "17:30"))

        sh, sm = map(int, rth_start.split(":"))
        eh, em = map(int, rth_end.split(":"))
        start_ok = (ts_local.hour > sh) or (ts_local.hour == sh and ts_local.minute >= sm)
        end_ok = (ts_local.hour < eh) or (ts_local.hour == eh and ts_local.minute <= em)
        in_rth = start_ok and end_ok

        thu_ok = not (disable_thursday_utc and ts_utc.weekday() == 3)
        nth_ok = (ts_local.hour not in no_trade_hours)

        if not thu_ok or not in_rth or not nth_ok:
            return None

        # Validate required columns
        need_cols = ["body_ratio", "vol_rel", "rsi14", "atr14", "vwap", "bear_prev3", "bull_prev3"]
        for c in need_cols:
            if c not in df.columns:
                return None
            v = row.get(c)
            if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
                return None

        # Extract values
        body_ratio = float(row["body_ratio"])
        vol_rel = float(row["vol_rel"])
        rsi = float(row["rsi14"])
        atr = float(row["atr14"])
        vwap = float(row["vwap"])
        close = float(row["close"])
        open_ = float(row["open"])
        bear_prev3 = float(row["bear_prev3"])
        bull_prev3 = float(row["bull_prev3"])

        # Filter checks
        if body_ratio < BODY_MIN:
            return None
        if vol_rel < VOL_REL_MIN:
            return None
        if abs(close - vwap) < (VWAP_DISTANCE_K * atr):
            return None

        # Signal logic
        # BUY: bullish candle, bear_prev3 >= threshold, RSI not overbought, close > VWAP
        # SELL: bearish candle, bull_prev3 >= threshold, RSI not oversold, close < VWAP
        buy_ok = bool((close > open_) and bear_prev3 >= BEAR_PREV3_LONG and rsi <= RSI_LONG_MAX and close > vwap)
        sell_ok = bool((close < open_) and bull_prev3 >= BULL_PREV3_SHORT and rsi >= RSI_SHORT_MIN and close < vwap)

        if not buy_ok and not sell_ok:
            return None

        direction = "BUY" if buy_ok else "SELL"

        # Entry price estimate (next bar open)
        try:
            entry_price_est = float(df["open"].iloc[-1])
        except Exception:
            entry_price_est = float(df["close"].iloc[-2])

        return Signal(direction=direction, entry_price_est=entry_price_est, meta={"ts": ts_utc.isoformat()})

    def initial_risk(self, entry_price: float, atr_v: float, sig: Signal, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Risk levels:
          R_pts = SL_ATR * ATR14 (signal bar)
          Long:  SL = entry - 1R,  TP = entry + TP_R_MULTIPLE * R
          Short: SL = entry + 1R,  TP = entry - TP_R_MULTIPLE * R
        """
        SL_ATR = float(params.get("SL_ATR", 1.0))
        TP_R_MULTIPLE = float(params.get("TP_R_MULTIPLE", 3.0))

        r_points = SL_ATR * float(atr_v)

        if sig.direction == "BUY":
            sl = float(entry_price) - r_points
            tp = float(entry_price) + (TP_R_MULTIPLE * r_points)
        else:
            sl = float(entry_price) + r_points
            tp = float(entry_price) - (TP_R_MULTIPLE * r_points)

        return {
            "r_points": float(r_points),
            "sl_local": float(sl),
            "tp_local": float(tp),
            "tp_r_multiple": float(TP_R_MULTIPLE),
            "exit_bars": 24,
        }
