from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

try:
    # Prefer stdlib zoneinfo (py3.9+)
    from zoneinfo import ZoneInfo  # type: ignore
    _HAS_ZONEINFO = True
except Exception:
    _HAS_ZONEINFO = False
    import pytz  # type: ignore


def _to_utc_ts(x: Any) -> pd.Timestamp:
    """
    Best-effort conversion to tz-aware UTC Timestamp.
    Accepts: pd.Timestamp, datetime, str, int/float epoch (s/ms).
    Returns pd.NaT on failure.
    """
    try:
        if x is None:
            return pd.NaT

        if isinstance(x, pd.Timestamp):
            ts = x
        else:
            if isinstance(x, (int, float)):
                unit = "ms" if x > 10_000_000_000 else "s"
                ts = pd.to_datetime(x, unit=unit, utc=True, errors="coerce")
            else:
                ts = pd.to_datetime(x, utc=True, errors="coerce")

        if ts is pd.NaT:
            return pd.NaT

        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        # anti-junk epoch
        if ts.year < 2000:
            return pd.NaT

        return ts
    except Exception:
        return pd.NaT


@dataclass(frozen=True)
class RTH:
    tz_name: str
    start_hh: int
    start_mm: int
    end_hh: int
    end_mm: int

    def in_rth(self, ts_utc: Any) -> bool:
        """
        Returns True if timestamp is inside RTH window (inclusive end).
        Robust: never raises, returns False on bad input.
        Blocks weekends by default.
        """
        tsu = _to_utc_ts(ts_utc)
        if tsu is pd.NaT:
            return False

        try:
            tz = ZoneInfo(self.tz_name) if _HAS_ZONEINFO else pytz.timezone(self.tz_name)
            local = tsu.tz_convert(tz)
        except Exception:
            return False

        # Weekend guard
        try:
            if local.weekday() >= 5:
                return False
        except Exception:
            return False

        hh, mm = int(local.hour), int(local.minute)

        start_ok = (hh > int(self.start_hh)) or (hh == int(self.start_hh) and mm >= int(self.start_mm))
        end_ok = (hh < int(self.end_hh)) or (hh == int(self.end_hh) and mm <= int(self.end_mm))

        return bool(start_ok and end_ok)
