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

    # spec: si down==0 => RSI inválido (no señal)
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
    SP500 5m EXACT SPEC:
    - RTH gate NY + no jueves UTC evaluado en vela señal
    - RSI/ATR con SMA (no Wilder)
    - Entry al CLOSE de la vela señal
    - Gestión: TP primero, luego SL; trailing end-of-bar + BE lock; time-exit 24 velas
    """

    def enrich(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        d = df.copy()

        # Body ratio
        rng = (d["high"] - d["low"]).astype(float)
        d["range"] = rng
        d["body_ratio"] = (d["close"] - d["open"]).abs() / rng.replace(0, np.nan)

        # Vol rel (SMA20 incluyendo volume[i] sin shift)
        vol = d["volume"].astype(float)
        d["vol_ma20"] = vol.rolling(20).mean()
        d["vol_rel"] = vol / d["vol_ma20"].replace(0, np.nan)

        # Prev3 excluye vela señal: shift(1).rolling(3).sum()
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

        # signal bar = última cerrada
        i = -2
        row = df.iloc[i]
        ts_utc = df.index[i]  # tz-aware UTC en engine
        # --- robust tz handling (fix int/naive index) ---
        import pandas as pd
        if isinstance(ts_utc, (int, float)):
            v = int(ts_utc)
            # heurística: seconds vs ms
            unit = 'ms' if v > 10_000_000_000 else 's'
            ts_utc = pd.to_datetime(v, unit=unit, utc=True, errors='coerce')
        else:
            ts_utc = pd.to_datetime(ts_utc, utc=True, errors='coerce')
        if ts_utc is pd.NaT:
            return None
        ts_ny = ts_utc.tz_convert('America/New_York')

        # Gates (evaluados en vela señal)
        # RTH NY: 09:30 <= hh:mm <= 16:00 (incluidos)
        hhmm = (ts_ny.hour, ts_ny.minute)
        in_rth = (hhmm > (9,30) or hhmm == (9,30)) and (hhmm < (16,0) or hhmm == (16,0))
        if not in_rth:
            return None

        # No jueves UTC
        if ts_utc.weekday() == 3:
            return None

                # --- SP500 SPEC: calcular indicadores EXACTOS aquí (no depender del pipeline global) ---
        import pandas as pd
        import numpy as np

        df2 = df.copy()

        # asegurar columna de volumen
        vol_col = "volume" if "volume" in df2.columns else ("vol" if "vol" in df2.columns else None)
        if vol_col is None:
            return None

        o2 = df2["open"].astype(float)
        h2 = df2["high"].astype(float)
        l2 = df2["low"].astype(float)
        c2 = df2["close"].astype(float)
        v2 = df2[vol_col].astype(float)

        # 3.1 body_ratio (range<=0 invalida)
        rng = (h2 - l2)
        df2["range"] = rng
        df2["body_ratio"] = (c2 - o2).abs() / rng.replace(0, np.nan)

        # 3.2 vol_rel con SMA20(volume) incluyendo i (sin shift)
        df2["vol_ma20"] = v2.rolling(window=20, min_periods=20).mean()
        df2["vol_rel"] = v2 / df2["vol_ma20"]

        # 3.3 RSI(14) EXACTO por SMA (no Wilder)
        delta = c2.diff()
        up = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        up_sma = up.rolling(window=14, min_periods=14).mean()
        down_sma = down.rolling(window=14, min_periods=14).mean()

        # down==0 => RSI inválido (NaN)
        rs = up_sma / down_sma.replace(0, np.nan)
        df2["rsi14"] = 100 - (100 / (1 + rs))

        # 3.4 ATR(14) EXACTO por SMA(TR)
        prev_close = c2.shift(1)
        tr = pd.concat([
            (h2 - l2),
            (h2 - prev_close).abs(),
            (l2 - prev_close).abs(),
        ], axis=1).max(axis=1)
        df2["atr14"] = tr.rolling(window=14, min_periods=14).mean()

        # 3.5 prev3 (excluye vela señal): rolling(3) sobre i-1..i-3
        bear = (c2 < o2).astype(int)
        bull = (c2 > o2).astype(int)
        df2["bear_prev3"] = bear.shift(1).rolling(window=3, min_periods=3).sum()
        df2["bull_prev3"] = bull.shift(1).rolling(window=3, min_periods=3).sum()

        # ahora la vela señal i=-2 debe salir de df2 (con indicadores exactos)
        row = df2.iloc[i]
        # --- fin bloque indicadores SP500 SPEC ---

# Validaciones de indicadores disponibles
        need = ["body_ratio","vol_ma20","vol_rel","rsi14","atr14","bear_prev3","bull_prev3"]
        for k in need:
            v = row.get(k)
            if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
                return None

        # Spec: range<=0 invalida
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

        # LONG
        if (close_px > open_px) and (bear3 >= 2) and (rsi < 75):
            return Signal(direction="BUY", entry_price_est=close_px, meta={
                "ts_signal_utc": ts_utc.isoformat(),
                "ts_signal_ny": ts_ny.isoformat(),
                "close": close_px, "open": open_px,
                "body_ratio": br, "vol_rel": vr,
                "rsi14": rsi, "atr14": float(row["atr14"]),
                "bear_prev3": bear3,
            })

        # SHORT
        if (close_px < open_px) and (bull3 >= 2) and (rsi > 40):
            return Signal(direction="SELL", entry_price_est=close_px, meta={
                "ts_signal_utc": ts_utc.isoformat(),
                "ts_signal_ny": ts_ny.isoformat(),
                "close": close_px, "open": open_px,
                "body_ratio": br, "vol_rel": vr,
                "rsi14": rsi, "atr14": float(row["atr14"]),
                "bull_prev3": bull3,
            })

        return None

    def initial_risk(self, entry_price: float, atr_signal: float, sig: Signal, params: Dict[str, Any]) -> Dict[str, Any]:
        # SL/TP iniciales con ATR_entry constante:
        # BUY: SL = entry - 1*ATR_entry ; TP = entry + 3*ATR_entry
        # SELL: SL = entry + 1*ATR_entry ; TP = entry - 3*ATR_entry
        atr_entry = float(atr_signal)
        r_points = atr_entry  # 1*ATR
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
        }
