import pandas as pd
import pytz

def rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0).rolling(period).mean()
    dn = (-delta).clip(lower=0).rolling(period).mean()
    rs = up / dn.replace(0, pd.NA)
    return 100 - (100 / (1 + rs))

def atr(df: pd.DataFrame, period: int) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [(df["high"] - df["low"]).abs(),
         (df["high"] - prev_close).abs(),
         (df["low"] - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()

def vwap_intraday(df: pd.DataFrame, tz_name: str) -> pd.Series:
    """
    Intraday VWAP (resets each local day in tz_name).

    Guarantees:
    - Output index == df.index (prevents alignment->NaN on assignment)
    - Works when volume missing/zero: falls back to vol=1 per bar
    - Works when high/low are present but NaN (common outside RTH): falls back to close per-row
    """
    if df is None or df.empty:
        return pd.Series(dtype="float64")

    orig_idx = df.index

    # tz-aware index for day grouping, but output stays on orig_idx
    if getattr(orig_idx, "tz", None) is None:
        idx_utc = orig_idx.tz_localize("UTC")
    else:
        idx_utc = orig_idx

    try:
        idx_local = idx_utc.tz_convert(tz_name)
    except Exception:
        idx_local = idx_utc

    # Price proxy
    if "close" not in df.columns:
        return pd.Series([float("nan")] * len(df), index=orig_idx, dtype="float64")

    close = df["close"].astype("float64")

    if "high" in df.columns and "low" in df.columns:
        high = df["high"].astype("float64")
        low  = df["low"].astype("float64")
        tp = (high + low + close) / 3.0
        # per-row fallback: if tp is NaN (high/low missing), use close
        tp = tp.where(tp == tp, close)
    else:
        tp = close

    # Volume proxy
    vol_col = None
    for c in ("volume", "vol", "tick_volume", "real_volume", "volume_traded"):
        if c in df.columns:
            vol_col = c
            break

    if vol_col is None:
        vol = tp * 0.0 + 1.0
    else:
        vol = df[vol_col].astype("float64").fillna(0.0)
        if float(vol.sum()) == 0.0:
            vol = tp * 0.0 + 1.0

    pv = (tp * vol).astype("float64")

    out = [float("nan")] * len(df)
    day_keys = idx_local.date

    start = 0
    n = len(df)
    while start < n:
        d = day_keys[start]
        end = start + 1
        while end < n and day_keys[end] == d:
            end += 1

        pv_seg = pv.iloc[start:end].to_numpy()
        v_seg  = vol.iloc[start:end].to_numpy()

        c_pv = pv_seg.cumsum()
        c_v  = v_seg.cumsum()

        for i in range(end - start):
            out[start + i] = (c_pv[i] / c_v[i]) if c_v[i] != 0 else float("nan")

        start = end

    return pd.Series(out, index=orig_idx, dtype="float64")

