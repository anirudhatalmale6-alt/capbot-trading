from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")
UTC = ZoneInfo("UTC")

@dataclass
class TradeState:
    side: str | None = None          # "LONG" / "SHORT" / None
    entry_price: float | None = None
    entry_time_utc: datetime | None = None
    r_pts: float | None = None
    stop_level: float | None = None
    limit_level: float | None = None
    planned_exit_utc: datetime | None = None
    sl_moved_1r: bool = False
    sl_moved_2r: bool = False
    size: int = 0

@dataclass
class CBState:
    consec_losses: int = 0
    cooldown_until_utc: datetime | None = None

def _rma(prev: float | None, x: float, n: int) -> float:
    # Wilder RMA: alpha=1/n
    a = 1.0 / float(n)
    if prev is None:
        return x
    return prev + a * (x - prev)

def _parse_hhmm(s: str) -> time:
    hh, mm = s.split(":")
    return time(int(hh), int(mm))

def gates_ok(now_utc: datetime, candle, ind, cfg, trade_open: bool, cb: CBState) -> tuple[bool, str]:
    # time rules
    now_berlin = now_utc.astimezone(BERLIN)

    if cfg.get("disable_thursday_utc", True):
        if now_utc.weekday() == 3:  # Thu
            return False, "GATE: thursday_utc"

    rth = cfg.get("rth_berlin", {"start":"09:30", "end":"17:30"})
    start_t = _parse_hhmm(rth["start"])
    end_t = _parse_hhmm(rth["end"])
    t = now_berlin.timetz().replace(tzinfo=None)

    # inclusive 09:30..17:30
    if not (t >= start_t and t <= end_t):
        return False, "GATE: outside_rth_berlin"

    if int(now_berlin.hour) in set(cfg.get("no_trade_hours_berlin", [9,14,15])):
        return False, "GATE: no_trade_hour_berlin"

    # indicators valid
    needed = ["body_ratio","vol_rel","rsi","atr","vwap","bear_prev3","bull_prev3"]
    for k in needed:
        v = ind.get(k)
        if v is None or not math.isfinite(float(v)):
            return False, f"GATE: ind_invalid:{k}"

    if ind["body_ratio"] < float(cfg.get("body_min", 0.70)):
        return False, "GATE: body_min"

    if ind["vol_rel"] < float(cfg.get("vol_rel_min", 0.70)):
        return False, "GATE: vol_rel_min"

    k = float(cfg.get("vwap_distance_k", 0.20))
    r = float(ind["atr"])
    if abs(float(candle["close"]) - float(ind["vwap"])) < k * r:
        return False, "GATE: vwap_distance"

    # circuit breaker (solo cuando estás flat)
    if (not trade_open) and cb.cooldown_until_utc is not None:
        if now_utc < cb.cooldown_until_utc:
            return False, "GATE: cooldown"

    return True, "OK"

def signal(candle, ind, gates: bool) -> str | None:
    if not gates:
        return None

    close_ = float(candle["close"])
    open_ = float(candle["open"])
    vwap = float(ind["vwap"])
    rsi = float(ind["rsi"])
    bear_prev3 = int(ind["bear_prev3"])
    bull_prev3 = int(ind["bull_prev3"])

    # LONG
    if (close_ > vwap and bear_prev3 >= 2 and rsi <= 75 and close_ > open_):
        return "LONG"

    # SHORT
    if (close_ < vwap and bull_prev3 >= 2 and rsi >= 40 and close_ < open_):
        return "SHORT"

    return None

def size_from_risk(equity: float, atr_pts: float, cfg) -> int:
    risk_pct = float(cfg.get("risk_pct", 0.02))
    vpp = float(cfg.get("value_per_point_per_size", 1))
    sl_atr = float(cfg.get("sl_atr", 1.0))
    risk_cash = equity * risk_pct
    r_pts = sl_atr * atr_pts
    if r_pts <= 0:
        return 1
    size_raw = risk_cash / (r_pts * vpp)
    size_ = int(math.floor(size_raw))
    return max(size_, 1)

def init_levels(side: str, entry_price: float, r_pts: float, entry_time_utc: datetime, cfg) -> TradeState:
    tp_atr = float(cfg.get("tp_atr", 3.0))
    st = TradeState(side=side)
    st.entry_price = entry_price
    st.entry_time_utc = entry_time_utc
    st.r_pts = r_pts
    st.planned_exit_utc = entry_time_utc + timedelta(minutes=5*int(cfg.get("exit_bars",24)))  # 24 bars -> 120min
    if side == "LONG":
        st.stop_level = entry_price - 1.0 * r_pts
        st.limit_level = entry_price + tp_atr * r_pts
    else:
        st.stop_level = entry_price + 1.0 * r_pts
        st.limit_level = entry_price - tp_atr * r_pts
    return st

def apply_trailing(st: TradeState, close_price: float, cfg) -> None:
    if not cfg.get("trailing_sl", True):
        return
    r = float(st.r_pts or 0.0)
    if r <= 0:
        return
    buf = float(cfg.get("trail_buffer_r", 0.10)) * r

    if st.side == "LONG":
        profit_pts = close_price - float(st.entry_price)
        if (profit_pts >= 1*r) and (not st.sl_moved_1r):
            new_sl = float(st.entry_price) + buf
            st.stop_level = max(float(st.stop_level), new_sl)
            st.sl_moved_1r = True
        if (profit_pts >= 2*r) and (not st.sl_moved_2r):
            new_sl = float(st.entry_price) + r + buf
            st.stop_level = max(float(st.stop_level), new_sl)
            st.sl_moved_2r = True
    else:
        profit_pts = float(st.entry_price) - close_price
        if (profit_pts >= 1*r) and (not st.sl_moved_1r):
            new_sl = float(st.entry_price) - buf
            st.stop_level = min(float(st.stop_level), new_sl)
            st.sl_moved_1r = True
        if (profit_pts >= 2*r) and (not st.sl_moved_2r):
            new_sl = float(st.entry_price) - r - buf
            st.stop_level = min(float(st.stop_level), new_sl)
            st.sl_moved_2r = True

def check_exit(st: TradeState, candle, now_utc: datetime, cfg) -> tuple[bool, str, float | None]:
    """
    Orden determinista:
      1 OUTSIDE_RTH (Berlin)
      2 TIME_EXIT
      3 STOP
      4 TAKE_PROFIT
    Convención conservadora: STOP primero si ambos.
    Devuelve (exit?, reason, exit_price)
    """
    now_berlin = now_utc.astimezone(BERLIN)

    rth = cfg.get("rth_berlin", {"start":"09:30", "end":"17:30"})
    start_t = _parse_hhmm(rth["start"])
    end_t = _parse_hhmm(rth["end"])
    t = now_berlin.timetz().replace(tzinfo=None)
    if not (t >= start_t and t <= end_t):
        return True, "OUTSIDE_RTH", float(candle["close"])

    if st.planned_exit_utc and now_utc >= st.planned_exit_utc:
        return True, "TIME_EXIT", float(candle["close"])

    lo = float(candle["low"])
    hi = float(candle["high"])

    if st.side == "LONG":
        if lo <= float(st.stop_level):
            return True, "STOP", float(st.stop_level)
        if hi >= float(st.limit_level):
            return True, "TAKE_PROFIT", float(st.limit_level)
    else:
        if hi >= float(st.stop_level):
            return True, "STOP", float(st.stop_level)
        if lo <= float(st.limit_level):
            return True, "TAKE_PROFIT", float(st.limit_level)

    return False, "HOLD", None

def update_circuit_breaker(cb: CBState, profit_cash: float, now_utc: datetime, cfg) -> None:
    if profit_cash < 0:
        cb.consec_losses += 1
    else:
        cb.consec_losses = 0

    if cb.consec_losses >= int(cfg.get("cb_losses", 3)):
        cb.cooldown_until_utc = now_utc + timedelta(minutes=int(cfg.get("cb_cooldown_min", 60)))

