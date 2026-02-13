from typing import Any, Dict, Optional, List

import pandas as pd


def mid(px: Dict[str, Any]) -> Optional[float]:
    b = (px or {}).get("bid")
    a = (px or {}).get("ask")
    try:
        if b is not None and a is not None:
            return (float(b) + float(a)) / 2.0
        if b is not None:
            return float(b)
        if a is not None:
            return float(a)
    except Exception:
        return None
    return None


def prices_to_df(j: Dict[str, Any]) -> pd.DataFrame:
    """
    Capital /prices response -> DataFrame with columns:
      time (UTC tz-aware), open, high, low, close, volume

    Robustness:
      - parse timestamps safely
      - filter rows with invalid OHLC
      - keep last row per timestamp if duplicates
      - ensure numeric types
    """
    prices: List[Dict[str, Any]] = (j or {}).get("prices", []) or []
    rows = []

    for p in prices:
        t = (p or {}).get("snapshotTimeUTC")
        if not t:
            continue

        o = mid((p or {}).get("openPrice") or {})
        h = mid((p or {}).get("highPrice") or {})
        l = mid((p or {}).get("lowPrice") or {})
        c = mid((p or {}).get("closePrice") or {})
        v = (p or {}).get("lastTradedVolume")

        if None in (o, h, l, c):
            continue

        rows.append(
            {
                "time": t,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"])

    # numeric cleanup
    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])

    # flag suspicious bars (but don't necessarily drop unless totally broken)
    df["is_suspicious"] = False
    bad_hl = df["high"] < df["low"]
    bad_range = (df["high"] - df["low"]) <= 0
    # close/open wildly outside range -> suspicious
    bad_oc = (df["close"] < df["low"]) | (df["close"] > df["high"]) | (df["open"] < df["low"]) | (df["open"] > df["high"])
    df.loc[bad_hl | bad_oc, "is_suspicious"] = True

    # if high<low or range<=0, drop (can't compute body/atr reliably)
    df = df.loc[~(bad_hl | bad_range)].copy()

    # keep last version per timestamp (better than drop_duplicates)
    df = df.sort_values("time").groupby("time", as_index=False).tail(1)

    df = df.sort_values("time").reset_index(drop=True)

    # volume: default 0 if missing
    df["volume"] = df["volume"].fillna(0.0)

    return df
