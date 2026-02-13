from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class SessionSpec:
    tz: str = "America/New_York"
    start_hhmm: str = "0930"
    end_hhmm: str = "1600"
    # Monday=0 ... Sunday=6
    weekdays: tuple[int, ...] = (0, 1, 2, 3, 4)


def _hhmm_to_time(hhmm: str) -> dtime:
    hhmm = hhmm.strip()
    if len(hhmm) != 4 or not hhmm.isdigit():
        raise ValueError(f"Invalid HHMM: {hhmm!r}")
    h = int(hhmm[:2])
    m = int(hhmm[2:])
    return dtime(h, m)


def in_session(now_utc: datetime, spec: SessionSpec) -> bool:
    """Returns True iff now_utc is inside [start, end) in spec.tz and weekday allowed."""
    if now_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware (UTC).")

    tz = ZoneInfo(spec.tz)
    local = now_utc.astimezone(tz)

    if local.weekday() not in spec.weekdays:
        return False

    start = _hhmm_to_time(spec.start_hhmm)
    end = _hhmm_to_time(spec.end_hhmm)
    t = local.time()

    return start <= t < end


def in_rth(now_utc: datetime, tz: str = "America/New_York") -> bool:
    return in_session(now_utc, SessionSpec(tz=tz, start_hhmm="0930", end_hhmm="1600"))
