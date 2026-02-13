from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd


@dataclass
class Signal:
    direction: str               # "BUY" | "SELL"
    entry_price_est: float       # para logging; el engine usa open del siguiente bar para entrar
    meta: Dict[str, Any]


def _rma(series: pd.Series, length: int) -> pd.Series:
    # Wilder RMA = EMA(alpha=1/length, adjust=False)
    return series.ewm(alpha=1.0 / float(length), adjust=False).mean()


def rsi_wilder(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    avg_up = _rma(up, length)
    avg_down = _rma(down, length)
    rs = avg_up / avg_down.replace(0, pd.NA)
    out = 100.0 - (100.0 / (1.0 + rs))
    return out


def atr_wilder(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)

    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    return _rma(tr, length)


def vwap_intraday_reset_berlin(df: pd.DataFrame, tz_name: str = "Europe/Berlin") -> pd.Series:
    """
    VWAP intradía reseteando a las 00:00 Berlin (o tz_name).
    Soporta:
      - df.index DatetimeIndex (tz-aware o naive)
      - o columna df["time"] con timestamps (ISO/datetime)
    """
    # 1) Obtener timestamps
    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index
    elif "time" in df.columns:
        # ojo: esto devuelve Series -> lo convertimos abajo a DatetimeIndex
        idx = pd.to_datetime(df["time"], errors="coerce")
    else:
        raise ValueError("VWAP requiere df.index DatetimeIndex o columna 'time'")

    # 2) Forzar DatetimeIndex siempre
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.DatetimeIndex(idx)

    # 3) Asegurar tz-aware en UTC (sobre el INDEX, no sobre valores)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    else:
        idx = idx.tz_convert("UTC")

    # 4) Día de reseteo en tz local
    local = idx.tz_convert(tz_name)
    day_key = local.normalize()

    # 5) Typical price y acumulados intradía
    tp = (df["high"].astype(float) + df["low"].astype(float) + df["close"].astype(float)) / 3.0
    vol = df["volume"].astype(float).fillna(0.0)

    cum_pv = (tp * vol).groupby(day_key).cumsum()
    cum_v = vol.groupby(day_key).cumsum().replace(0.0, float("nan"))
    return cum_pv / cum_v

class VWAPPullbackRSI:
    """
    Estrategia DE40/GER40 5m — VWAP diario + filtro distancia VWAP (k=0.20)
    - Señal con vela cerrada (signal_bar = df.iloc[-2])
    - Entrada en open de la vela siguiente (entry_bar = df.iloc[-1]) -> lo hace el engine
    """

    name = "vwap_pullback_rsi"

    def enrich(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        vol_window = int(params.get("VOL_WINDOW", 20))
        rsi_len = int(params.get("RSI_PERIOD", 14))
        atr_len = int(params.get("ATR_PERIOD", 14))
        vwap_tz = str(params.get("VWAP_TZ", "Europe/Berlin"))

        d = df.copy()

        # Body ratio
        rng = (d["high"] - d["low"]).astype(float)
        d["range"] = rng
        d["body_ratio"] = (d["close"] - d["open"]).abs() / rng.replace(0, pd.NA)
        d["body_ratio"] = d["body_ratio"].fillna(0.0)

        # Volumen relativo
        d["vol_sma20"] = d["volume"].rolling(vol_window, min_periods=vol_window).mean()
        d["vol_rel"] = d["volume"] / d["vol_sma20"].replace(0, pd.NA)

        # Prev3 bulls/bears (shift(1).rolling(3).sum())
        d["bear"] = (d["close"] < d["open"]).astype(int)
        d["bull"] = (d["close"] > d["open"]).astype(int)
        d["bear_prev3"] = d["bear"].shift(1).rolling(3).sum()
        d["bull_prev3"] = d["bull"].shift(1).rolling(3).sum()

        # RSI/ATR Wilder
        d["rsi14"] = rsi_wilder(d["close"].astype(float), rsi_len)
        d["atr14"] = atr_wilder(d, atr_len)

        # VWAP intradía reset 00:00 Berlin
        d["vwap"] = vwap_intraday_reset_berlin(d, vwap_tz)

        return d

    def signal_on_bar_close(self, df: pd.DataFrame, params: Dict[str, Any]) -> Optional[Signal]:
        if df is None or df.empty or len(df) < 50:
            return None

        # signal_bar = última vela CERRADA
        last = df.iloc[-2]

        close_px = float(last["close"])
        open_px = float(last["open"])

        BODY_MIN = float(params.get("BODY_MIN", 0.70))
        VOL_REL_MIN = float(params.get("VOL_REL_MIN", 0.70))

        RSI_LONG_MAX = float(params.get("RSI_LONG_MAX", 75))
        RSI_SHORT_MIN = float(params.get("RSI_SHORT_MIN", 40))
        BEAR_PREV3_LONG = int(params.get("BEAR_PREV3_LONG", 2))
        BULL_PREV3_SHORT = int(params.get("BULL_PREV3_SHORT", 2))

        VWAP_DISTANCE_K = float(params.get("VWAP_DISTANCE_K", 0.20))

        need = ["body_ratio", "vol_rel", "rsi14", "atr14", "vwap", "bear_prev3", "bull_prev3"]
        if any(pd.isna(last[k]) for k in need):
            return None

        br = float(last["body_ratio"])
        vr = float(last["vol_rel"])
        if br < BODY_MIN or vr < VOL_REL_MIN:
            return None

        rsi_v = float(last["rsi14"])
        atr_v = float(last["atr14"])
        vwap_px = float(last["vwap"])
        bear3 = int(last["bear_prev3"])
        bull3 = int(last["bull_prev3"])

        # GATE NUEVO: distancia a VWAP
        if abs(close_px - vwap_px) < (VWAP_DISTANCE_K * atr_v):
            return None

        # Señales
        cond_long = (close_px > vwap_px) and (bear3 >= BEAR_PREV3_LONG) and (rsi_v <= RSI_LONG_MAX) and (close_px > open_px)
        cond_short = (close_px < vwap_px) and (bull3 >= BULL_PREV3_SHORT) and (rsi_v >= RSI_SHORT_MIN) and (close_px < open_px)

        meta = {
            "body_ratio": br,
            "vol_rel": vr,
            "rsi14": rsi_v,
            "atr14": atr_v,
            "vwap": vwap_px,
            "bear_prev3": bear3,
            "bull_prev3": bull3,
            "vwap_distance_k": VWAP_DISTANCE_K,
        }

        if cond_long:
            return Signal(direction="BUY", entry_price_est=close_px, meta=meta)
        if cond_short:
            return Signal(direction="SELL", entry_price_est=close_px, meta=meta)
        return None

    def initial_risk(self, entry_price: float, atr_signal_bar: float, sig: Signal, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        100% spec:
          R_pts = SL_ATR * ATR14 (signal_bar)
          Long: SL = entry - 1R ; TP = entry + 3R
          Short: SL = entry + 1R ; TP = entry - 3R
        """
        SL_ATR = float(params.get("SL_ATR", 1.0))
        TP_R_MULTIPLE = float(params.get("TP_R_MULTIPLE", 3.0))

        r_points = float(SL_ATR * atr_signal_bar)

        if str(sig.direction).upper() == "BUY":
            sl_local = float(entry_price - 1.0 * r_points)
            tp_local = float(entry_price + TP_R_MULTIPLE * r_points)
        else:
            sl_local = float(entry_price + 1.0 * r_points)
            tp_local = float(entry_price - TP_R_MULTIPLE * r_points)

        return {
            "r_points": r_points,
            "sl_local": sl_local,
            "tp_local": tp_local,
            "tp_r_multiple": TP_R_MULTIPLE,
        }

# --- Backward compat: DE40 strategy expects this name ---
def vwap_daily_berlin(*args, **kwargs):
    """
    Compat wrapper. Kept to avoid breaking de40_vwap_k020 import.
    """
    # TODO: change the call below to the actual function name found in this module.
    return vwap_intraday_reset_berlin(*args, **kwargs)
