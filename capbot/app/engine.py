from typing import Any, Dict, Optional

import pandas as pd
import numpy as np
import time
import os
import math
import inspect
import json
import signal
import threading

ENGINE_BUILD_TAG = "20260215_engine_v3"

# ──────────────────────────────────────────────────
# Imports
# ──────────────────────────────────────────────────
try:
    from capbot.app.notifier import email_event, email_startup
except Exception:
    def email_event(*args, **kwargs):
        return None
    def email_startup(*args, **kwargs):
        return None

try:
    from capbot.app.telegram_notifier import telegram_event
except Exception:
    def telegram_event(*args, **kwargs):
        return None

from capbot.broker.capital_client import CapitalClient, pick_position_dealid_from_confirm
from capbot.data.prices import prices_to_df
from capbot.domain.lock import InstanceLock
from capbot.domain.logger import log_line
from capbot.domain.paths import bot_paths
from capbot.domain.risk import calc_position_size
from capbot.domain.schedule import RTH
from capbot.domain.state_store import load_state, save_state_atomic
from capbot.domain.trade_log import append_row, ensure_header
from capbot.domain.trailing import maybe_trail_option_a
from capbot.strategies.loader import load_strategy


# ──────────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────────

def utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _as_ts(x: Optional[str]) -> Optional[pd.Timestamp]:
    if not x:
        return None
    try:
        return pd.to_datetime(x, utc=True)
    except Exception:
        return None


def _resolution_to_minutes(resolution: str) -> int:
    r = (resolution or "").upper().strip()
    if r.startswith("MINUTE_"):
        try:
            return int(r.split("_", 1)[1])
        except Exception:
            return 5
    if r in ("HOUR", "HOUR_1"):
        return 60
    if r == "HOUR_4":
        return 240
    if r == "DAY":
        return 1440
    return 5


def _rth_is_open(rth: RTH, ts: pd.Timestamp) -> bool:
    import pytz
    tz = pytz.timezone(getattr(rth, "tz_name", "UTC"))
    local = ts.tz_convert(tz)
    if local.weekday() >= 5:
        return False
    sh = int(getattr(rth, "start_hh", 0))
    sm = int(getattr(rth, "start_mm", 0))
    eh = int(getattr(rth, "end_hh", 23))
    em = int(getattr(rth, "end_mm", 59))
    start = local.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = local.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= local <= end


def _to_utc_ts(x):
    """Best-effort: convert x to tz-aware UTC pandas Timestamp."""
    try:
        ts = pd.Timestamp(x)
    except Exception:
        try:
            xv = float(x)
            ts = pd.to_datetime(xv, unit="s", utc=True)
        except Exception:
            ts = pd.to_datetime(x, utc=True, errors="coerce")
    if ts is pd.NaT:
        return pd.Timestamp.utcnow().tz_localize("UTC")
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _normalize_positions_payload(payload):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("positions", "data", "items"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
        return [payload]
    return []


def _extract_open_position_for_epic(client, epic: str):
    """Return dict with deal_id/direction/size/sl/tp/entry if open position exists for epic, else None."""
    for meth in ("get_positions", "positions", "list_positions", "get_open_positions"):
        if not hasattr(client, meth):
            continue
        try:
            payload = getattr(client, meth)()
            items = _normalize_positions_payload(payload)
            for it in items:
                m = it.get("market") if isinstance(it, dict) else None
                it_epic = None
                if isinstance(m, dict):
                    it_epic = m.get("epic")
                it_epic = it_epic or (it.get("epic") if isinstance(it, dict) else None)

                if str(it_epic or "").strip() != str(epic).strip():
                    continue

                deal_id = None
                if isinstance(it, dict):
                    deal_id = it.get("dealId") or it.get("deal_id")
                    pos = it.get("position")
                    if isinstance(pos, dict):
                        deal_id = deal_id or pos.get("dealId") or pos.get("deal_id")

                direction = None
                size = None
                sl = None
                tp = None
                entry = None

                if isinstance(it, dict):
                    direction = it.get("direction")
                    size = it.get("size")
                    pos = it.get("position") if isinstance(it.get("position"), dict) else {}
                    direction = direction or pos.get("direction")
                    size = size or pos.get("size")
                    sl = pos.get("stopLevel") or pos.get("stop_level") or it.get("stopLevel")
                    tp = pos.get("limitLevel") or pos.get("limit_level") or it.get("limitLevel")
                    entry = pos.get("level") or pos.get("openLevel") or it.get("level")

                if deal_id:
                    return {
                        "deal_id": str(deal_id),
                        "direction": direction,
                        "size": size,
                        "sl_local": sl,
                        "tp_local": tp,
                        "entry_price_est": entry,
                    }
            return None
        except Exception:
            continue

    return None


def _ensure_utc_datetime_index_safe(df):
    """Normalize df to have UTC DatetimeIndex. Never returns None."""
    if df is None or getattr(df, "empty", True):
        return df

    try:
        if isinstance(df.index, pd.DatetimeIndex):
            d = df.copy()
            if d.index.tz is None:
                d.index = d.index.tz_localize("UTC")
            else:
                d.index = d.index.tz_convert("UTC")
            return d.sort_index()
    except Exception:
        return df

    try:
        if "time" in getattr(df, "columns", []):
            d = df.copy()
            t = pd.to_datetime(d["time"], utc=True, errors="coerce")
            try:
                all_bad = t.isna().all()
            except Exception:
                all_bad = False
            if all_bad:
                return df
            d["time"] = t
            d = d.dropna(subset=["time"]).set_index("time").sort_index()
            return d if not getattr(d, "empty", True) else df
    except Exception:
        return df

    return df


def _fetch_broker_open_snap(client, epic: str, deal_id: str):
    """Best-effort broker snapshot for this position."""
    snap = {"epic": str(epic), "deal_id": str(deal_id)}
    try:
        fn = getattr(client, "get_position_by_deal_id", None)
        if callable(fn) and deal_id:
            one = fn(str(deal_id))
            if one:
                snap["position_item"] = one
                return snap
    except Exception:
        pass
    try:
        snap["positions_payload"] = client.get_positions()
    except Exception:
        pass
    return snap


def _fetch_broker_history_snap(client):
    """Best-effort history snapshot. Never raises."""
    try:
        now = pd.Timestamp.now(tz="UTC")
        frm = (now - pd.Timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S")
        to = now.strftime("%Y-%m-%dT%H:%M:%S")
        out = {"frm": frm, "to": to}
        try:
            out["activity"] = client.get_history_activity(frm=frm, to=to, max_items=200)
        except Exception as e:
            out["activity_err"] = repr(e)
        try:
            out["transactions"] = client.get_history_transactions(frm=frm, to=to, max_items=200)
        except Exception as e:
            out["transactions_err"] = repr(e)
        return out
    except Exception as e:
        return {"err": repr(e)}


def safe_close_position(client, deal_id):
    """Fail-safe close wrapper. Never raises."""
    try:
        fn = getattr(client, "close_position", None)
        if callable(fn):
            return fn(deal_id)
        fn2 = getattr(client, "close", None)
        if callable(fn2):
            return fn2(deal_id)
        return None
    except Exception:
        return None


def safe_append_row(csv_path, ts, bot_id, epic, direction, size, reason, exit_price, conf,
                    entry_price=None, entry_time=None, r_points=None, sl_local=None, tp_local=None,
                    deal_id=None, profit_pts=None, profit_cash=None):
    """Never crash the bot for trade logging. Maps to trade_log.py HEADER."""
    try:
        close_ref = ""
        if isinstance(conf, dict):
            close_ref = conf.get("dealReference") or conf.get("deal_reference") or ""

        row = {
            "entry_time": entry_time or "",
            "exit_time": ts,
            "direction": direction,
            "size": size,
            "entry_price_est": entry_price or "",
            "entry_price_api": entry_price or "",
            "exit_price_est": exit_price,
            "exit_price_api": exit_price,
            "profit_api": profit_pts or "",
            "profit_ccy": profit_cash or "",
            "r_points": r_points or "",
            "initial_sl": sl_local or "",
            "sl_local": sl_local or "",
            "tp_local": tp_local or "",
            "exit_reason": reason,
            "position_deal_id": deal_id or "",
            "close_dealReference": close_ref,
            "meta_json": json.dumps({"bot_id": bot_id, "epic": epic}, default=str),
        }
        return append_row(csv_path, row)
    except Exception:
        try:
            return append_row(csv_path, {
                "exit_time": ts, "direction": direction, "size": size,
                "exit_price_est": exit_price, "exit_reason": reason,
                "meta_json": json.dumps({"bot_id": bot_id, "epic": epic, "error": "fallback"}, default=str),
            })
        except Exception:
            return None


def _trail_sp500_spec(direction, entry, atr_entry, sl, be_armed, max_fav, min_fav, bar_high, bar_low):
    """SP500 spec end-of-bar trailing + BE lock."""
    direction = str(direction).upper()
    if atr_entry <= 0:
        return sl, be_armed, max_fav, min_fav

    if direction == "BUY":
        max_fav = max(float(max_fav), float(bar_high))
        if float(bar_high) > float(entry):
            be_armed = True
        sl_cand = float(max_fav) - 1.0 * float(atr_entry)
        sl = max(float(sl), float(sl_cand))
        if be_armed:
            sl = max(float(sl), float(entry))
    else:
        min_fav = min(float(min_fav), float(bar_low))
        if float(bar_low) < float(entry):
            be_armed = True
        sl_cand = float(min_fav) + 1.0 * float(atr_entry)
        sl = min(float(sl), float(sl_cand))
        if be_armed:
            sl = min(float(sl), float(entry))

    return sl, be_armed, max_fav, min_fav


def _isnum(x):
    """Check if x is a valid finite number."""
    try:
        return (x == x) and (not math.isinf(x))
    except Exception:
        return False


def _ok(x):
    return "\u2705" if x else "\u274c"


def _compute_vis_checks(df, strat_params, rth, rth_enabled, tz_name, now,
                        disable_thursday_utc, no_trade_hours, st, strat=None):
    """
    Compute visual CHECK line from enriched df. Strategy-agnostic.
    Shows: time | gates (check/X) | key indicators | LONG/SHORT | ENTRY
    """
    if df is None or getattr(df, "empty", True) or len(df) < 2:
        return "CHECK | (no data)"

    i = -2
    row = df.iloc[i]

    try:
        t_closed = _to_utc_ts(df.index[i]).strftime("%H:%M")
    except Exception:
        t_closed = "?"

    close = float(row.get("close", float("nan")))
    open_ = float(row.get("open", float("nan")))
    rsi = float(row.get("rsi14", float("nan")))

    # Gates
    thu_block = bool(disable_thursday_utc and now.weekday() == 3)
    rth_ok = (not rth_enabled) or _rth_is_open(rth, now)
    try:
        now_local = now.tz_convert(tz_name)
        nth_block = int(now_local.hour) in set(int(x) for x in no_trade_hours)
    except Exception:
        nth_block = False
    cooldown_until = _as_ts((st or {}).get("cooldown_until_iso"))
    cooldown_block = bool(cooldown_until and now < cooldown_until)

    gates_ok = (not thu_block) and rth_ok and (not nth_block) and (not cooldown_block)
    gates = f"rth={_ok(rth_ok)} day={_ok(not thu_block)} cd={_ok(not cooldown_block)}"

    # Strategy-specific indicators (show what's available)
    indicators = []
    indicators.append(f"close={close:.2f}")
    if _isnum(rsi):
        indicators.append(f"rsi={rsi:.1f}")

    # VWAP-based (DE40, SP500 5m)
    vwap = row.get("vwap")
    if vwap is not None and _isnum(float(vwap)):
        indicators.append(f"vwap={float(vwap):.1f}")

    # Bollinger-based (META, NVDA)
    bb_low = row.get("bb_low_20")
    bb_up = row.get("bb_up_20")
    bb_mid = row.get("bb_mid_20")
    if bb_low is not None and _isnum(float(bb_low)):
        indicators.append(f"bb=[{float(bb_low):.1f}|{float(bb_mid):.1f}|{float(bb_up):.1f}]")

    # Regime filter (NVDA)
    regime = row.get("regime_ok")
    if regime is not None:
        indicators.append(f"regime={_ok(bool(int(regime)))}")

    # SMA trend (SP500 1h)
    sma50 = row.get("sma50")
    sma200 = row.get("sma200")
    if sma50 is not None and sma200 is not None and _isnum(float(sma50)):
        trend_ok = close > float(sma50) and float(sma50) > float(sma200)
        indicators.append(f"trend={_ok(trend_ok)}")

    # Body/vol (DE40, SP500 5m)
    body = row.get("body_ratio")
    volr = row.get("vol_rel")
    if body is not None and _isnum(float(body)):
        p_ = strat_params or {}
        body_ok = float(body) >= float(p_.get("BODY_MIN", 0.70))
        indicators.append(f"body={_ok(body_ok)}")
    if volr is not None and _isnum(float(volr)):
        p_ = strat_params or {}
        vol_ok = float(volr) >= float(p_.get("VOL_REL_MIN", 0.70))
        indicators.append(f"vol={_ok(vol_ok)}")

    ind_str = " ".join(indicators)

    # LONG/SHORT/ENTRY - use the actual strategy signal check
    long_ok = False
    short_ok = False
    if strat is not None and gates_ok:
        try:
            sig = strat.signal_on_bar_close(df, strat_params or {})
            if sig is not None:
                if sig.direction == "BUY":
                    long_ok = True
                else:
                    short_ok = True
        except Exception:
            pass

    entry_ok = long_ok or short_ok

    return (
        f"CHECK {t_closed} | {gates} | {ind_str} | "
        f"LONG={_ok(long_ok)} SHORT={_ok(short_ok)} | ENTRY {_ok(entry_ok)}"
    )


def _handle_position_exit(client, st, pos, deal_id, direction, reason, exit_price,
                          csv_path, bot_id, epic, vpp, cb_losses, cb_cooldown,
                          email_enabled, logfile, now, mode_sp500=False,
                          currency_symbol="$", account_currency="USD"):
    """Common exit handler: close, log, circuit breaker, state cleanup, notify."""
    conf = safe_close_position(client, str(deal_id))

    # Try to get broker's actual exit price and profit from confirm
    broker_profit = None
    broker_exit_price = None
    try:
        close_ref = (conf or {}).get("dealReference") or (conf or {}).get("deal_reference")
        if close_ref:
            time.sleep(0.5)
            close_conf = client.confirm(str(close_ref), timeout_sec=10)
            if close_conf:
                if close_conf.get("profit") is not None:
                    broker_profit = float(close_conf["profit"])
                if close_conf.get("level") is not None:
                    broker_exit_price = float(close_conf["level"])
                log_line(logfile, f"BROKER_CLOSE_CONFIRM: profit={broker_profit} level={broker_exit_price}")
    except Exception as e:
        log_line(logfile, f"BROKER_CLOSE_CONFIRM warning: {repr(e)}")

    # Fallback: fetch from transaction history if confirm didn't have profit
    if broker_profit is None:
        try:
            time.sleep(1.5)
            history = client.get_history_transactions(max_items=5)
            transactions = history.get("transactions") or []
            for tx in transactions:
                ref = tx.get("reference") or ""
                tx_type = (tx.get("type") or tx.get("transactionType") or "").upper()
                if str(deal_id) in str(ref) or "TRADE" in tx_type:
                    for field in ("profitAndLoss", "profit", "cashTransaction", "amount"):
                        val = tx.get(field)
                        if val is not None:
                            try:
                                broker_profit = float(str(val).replace(",", ""))
                                log_line(logfile, f"BROKER_TX_HISTORY profit ({field}): {broker_profit}")
                                break
                            except (ValueError, TypeError):
                                pass
                    if broker_profit is not None:
                        break
        except Exception as e:
            log_line(logfile, f"BROKER_TX_HISTORY warning: {repr(e)}")

    try:
        st["last_broker_close"] = {"close_resp": conf}
        st["last_broker_snap"] = {"positions_payload": client.get_positions()}
        try:
            st["last_broker_history_close"] = _fetch_broker_history_snap(client)
        except Exception:
            pass
    except Exception as e:
        log_line(logfile, f"EXIT_SNAP warning: {repr(e)}")

    # Use broker exit price if available
    if broker_exit_price is not None:
        exit_price = broker_exit_price

    # Circuit breaker
    entry_price = 0.0
    size_pos = 0.0
    profit_pts = 0.0
    profit_cash = 0.0
    try:
        entry_price = float(pos.get("entry_price_est", 0))
        size_pos = float(pos.get("size", 0))
        profit_pts = (exit_price - entry_price) if direction == "BUY" else (entry_price - exit_price)
        # Prefer broker profit if available
        if broker_profit is not None:
            profit_cash = broker_profit
        else:
            profit_cash = profit_pts * size_pos * vpp
        consec = int(st.get("consec_losses", 0))
        consec = (consec + 1) if profit_cash < 0 else 0
        st["consec_losses"] = consec
        if consec >= cb_losses:
            st["cooldown_until_iso"] = (now + pd.Timedelta(minutes=cb_cooldown)).isoformat()
            log_line(logfile, f"CIRCUIT_BREAKER: {consec} consecutive losses, cooldown {cb_cooldown}min")
    except Exception as e:
        log_line(logfile, f"CIRCUIT_BREAKER calc warning: {repr(e)}")

    safe_append_row(csv_path, now.isoformat(), bot_id, epic, direction, size_pos, reason, exit_price, conf,
                    entry_price=entry_price, entry_time=pos.get("entry_bar_time_utc") or pos.get("ts_signal_utc"),
                    r_points=pos.get("r_points"), sl_local=pos.get("sl_local"), tp_local=pos.get("tp_local"),
                    deal_id=deal_id, profit_pts=round(profit_pts, 2), profit_cash=round(profit_cash, 2))

    st["pos"] = {}
    if not mode_sp500:
        st["last_closed_time"] = now.isoformat()

    log_line(logfile, f"EXIT {reason} deal_id={deal_id} exit_price={exit_price:.2f} profit={profit_pts:.2f}pts {currency_symbol}{profit_cash:.2f}")

    email_event(email_enabled, bot_id, reason, {
        "epic": epic, "deal_id": deal_id, "exit_price": round(exit_price, 2),
        "direction": direction, "entry_price": round(entry_price, 2),
        "size": size_pos, "vpp": vpp,
        "profit_points": round(profit_pts, 2), "profit_cash": round(profit_cash, 2),
        "currency": account_currency, "currency_symbol": currency_symbol,
    }, logfile)

    telegram_event(bot_id, reason, {
        "epic": epic, "deal_id": deal_id, "exit_price": round(exit_price, 2),
        "direction": direction, "entry_price": round(entry_price, 2),
        "size": size_pos, "profit_points": round(profit_pts, 2),
        "profit_cash": round(profit_cash, 2),
        "currency": account_currency, "currency_symbol": currency_symbol,
    })

    return conf


# ──────────────────────────────────────────────────
# Graceful shutdown
# ──────────────────────────────────────────────────

class _ShutdownRequested(Exception):
    """Raised inside the main loop when SIGTERM/SIGINT is received."""

_shutdown_flag = threading.Event()

def _install_signal_handlers(logfile: str):
    """Install SIGTERM/SIGINT handlers that set the shutdown flag."""
    def _handler(signum, frame):
        name = signal.Signals(signum).name
        log_line(logfile, f"SHUTDOWN: received {name}, shutting down gracefully...")
        _shutdown_flag.set()
    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


# ──────────────────────────────────────────────────
# Heartbeat watchdog
# ──────────────────────────────────────────────────

class _Watchdog:
    """Background thread that alerts if the main loop hasn't ticked recently."""

    def __init__(self, timeout_sec: int, bot_id: str, logfile: str, email_enabled: bool, epic: str = ""):
        self._timeout = max(60, timeout_sec)
        self._bot_id = bot_id
        self._logfile = logfile
        self._email_enabled = email_enabled
        self._epic = epic
        self._last_tick = time.monotonic()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._alerted = False
        self._thread = threading.Thread(target=self._run, daemon=True, name="watchdog")

    def start(self):
        self._thread.start()

    def tick(self):
        with self._lock:
            self._last_tick = time.monotonic()
            self._alerted = False

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.wait(30):
            with self._lock:
                elapsed = time.monotonic() - self._last_tick
                already_alerted = self._alerted
            if elapsed > self._timeout and not already_alerted:
                with self._lock:
                    self._alerted = True
                msg = f"WATCHDOG: main loop stalled for {int(elapsed)}s (threshold {self._timeout}s)"
                log_line(self._logfile, msg)
                email_event(self._email_enabled, self._bot_id, "WATCHDOG_ALERT", {
                    "elapsed_sec": int(elapsed), "threshold_sec": self._timeout,
                    "epic": self._epic,
                }, self._logfile)
                telegram_event(self._bot_id, "WATCHDOG_ALERT", {
                    "elapsed_sec": int(elapsed), "threshold_sec": self._timeout,
                    "epic": self._epic,
                })


# ──────────────────────────────────────────────────
# Config hot-reload
# ──────────────────────────────────────────────────

def _check_config_reload(config_path: str, last_mtime: float, logfile: str):
    """Check if config file changed. Returns (new_cfg_dict_or_None, new_mtime)."""
    try:
        mtime = os.path.getmtime(config_path)
        if mtime > last_mtime:
            from capbot.app.config import load_config
            cfg = load_config(config_path).raw
            log_line(logfile, f"CONFIG_RELOAD: detected change in {config_path}")
            return cfg, mtime
    except Exception as e:
        log_line(logfile, f"CONFIG_RELOAD warning: {repr(e)}")
    return None, last_mtime


def _apply_hot_config(cfg: Dict[str, Any], logfile: str):
    """Extract hot-reloadable params from config. Returns dict of updated values."""
    reloaded = {}

    risk = cfg.get("risk") or {}
    reloaded["bot_equity"] = float(risk.get("bot_equity", 25000.0))
    reloaded["risk_pct"] = float(risk.get("risk_pct", 0.02))

    trail = cfg.get("trailing") or {}
    reloaded["trailing_on"] = bool(trail.get("enabled", True))
    reloaded["trail_buffer_r"] = float(trail.get("buffer_r", 0.10))

    cb = cfg.get("circuit_breaker") or {}
    reloaded["cb_losses"] = int(cb.get("losses", 3))
    reloaded["cb_cooldown"] = int(cb.get("cooldown_min", 60))

    strat = cfg.get("strategy") or {}
    reloaded["strat_params"] = strat.get("params") or {}

    reloaded["poll"] = int(cfg.get("poll_seconds", 30))

    log_line(logfile, f"CONFIG_RELOAD: applied new params equity={reloaded['bot_equity']} risk={reloaded['risk_pct']} poll={reloaded['poll']}s")
    return reloaded


# ──────────────────────────────────────────────────
# Daily trade summary
# ──────────────────────────────────────────────────

def _send_daily_summary(csv_path, bot_id, epic, email_enabled, logfile):
    """Parse today's trades from CSV and send summary via email/telegram."""
    import csv as _csv
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    trades = []

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                exit_time = row.get("exit_time") or ""
                if today in exit_time:
                    trades.append(row)
    except Exception:
        trades = []

    total = len(trades)
    if total == 0:
        summary = {"trades": 0, "message": "No trades today", "epic": epic, "date": today}
    else:
        wins = 0
        total_pnl = 0.0
        for t in trades:
            try:
                pnl = float(t.get("profit_ccy") or t.get("profit_api") or 0)
                total_pnl += pnl
                if pnl > 0:
                    wins += 1
            except Exception:
                pass
        win_rate = (wins / total * 100) if total > 0 else 0
        summary = {
            "trades": total,
            "wins": wins,
            "losses": total - wins,
            "win_rate": f"{win_rate:.0f}%",
            "total_pnl": f"${total_pnl:.2f}",
            "epic": epic,
            "date": today,
        }

    log_line(logfile, f"DAILY_SUMMARY: {summary}")
    email_event(email_enabled, bot_id, "DAILY_SUMMARY", summary, logfile)
    telegram_event(bot_id, "DAILY_SUMMARY", summary)


def _wait_seconds_until_next_bar(bar_minutes: int) -> float:
    """Calculate seconds to wait until next bar close + small buffer."""
    now = pd.Timestamp.now(tz="UTC")
    minute_of_day = now.hour * 60 + now.minute
    next_bar_minute = ((minute_of_day // bar_minutes) + 1) * bar_minutes
    next_bar = now.replace(hour=0, minute=0, second=0, microsecond=0) + pd.Timedelta(minutes=next_bar_minute)
    wait = (next_bar - now).total_seconds() + 2  # 2s buffer for bar to finalize
    return max(1, min(wait, bar_minutes * 60))


# ──────────────────────────────────────────────────
# Main engine loop
# ──────────────────────────────────────────────────

def run_bot(cfg: Dict[str, Any], once: bool = False):
    bot_id = str(cfg.get("bot_id") or (cfg.get("market") or {}).get("epic") or "capbot")
    paths = bot_paths(bot_id)
    state_path, csv_path, logfile, lock_path = paths

    log_line(logfile, f"ENGINE_BUILD_TAG={ENGINE_BUILD_TAG}")

    lock = InstanceLock(lock_path, 1800)

    def save_state(st: dict):
        save_state_atomic(state_path, st)

    lock.acquire()
    ensure_header(csv_path)
    st = load_state(state_path) or {}

    poll = int(cfg.get("poll_seconds", 30))

    # ── Market / data ──
    market = cfg.get("market") or {}
    epic = str(market.get("epic") or "").strip()
    resolution = str(market.get("resolution") or "MINUTE_5").strip()
    warmup = int(market.get("warmup_bars", 200))
    if not epic:
        raise RuntimeError("Config error: market.epic is missing")
    bar_minutes = _resolution_to_minutes(resolution)

    # ── Schedule ──
    schedule_cfg = cfg.get("schedule") or {}
    tz_name = str(schedule_cfg.get("timezone") or schedule_cfg.get("tz_name") or "Europe/Berlin")
    rth_enabled = bool(schedule_cfg.get("rth_enabled", True))
    disable_thursday_utc = bool(schedule_cfg.get("disable_thursday_utc", True))
    rth_start = str(schedule_cfg.get("rth_start", "09:30"))
    rth_end = str(schedule_cfg.get("rth_end", "17:30"))
    rth = RTH(
        tz_name=tz_name,
        start_hh=int(rth_start.split(":")[0]),
        start_mm=int(rth_start.split(":")[1]),
        end_hh=int(rth_end.split(":")[0]),
        end_mm=int(rth_end.split(":")[1]),
    )
    no_trade_hours = (
        (cfg.get("strategy") or {}).get("no_trade_hours_berlin")
        or schedule_cfg.get("no_trade_hours_berlin")
        or [9, 14, 15]
    )

    # ── Risk ──
    risk_cfg = cfg.get("risk") or {}
    bot_equity = float(risk_cfg.get("bot_equity", 25000.0))
    risk_pct = float(risk_cfg.get("risk_pct", 0.02))
    vpp = float(risk_cfg.get("value_per_point_per_size", 1.0))

    # ── Trailing ──
    trailing_cfg = cfg.get("trailing") or {}
    trailing_on = bool(trailing_cfg.get("enabled", True))
    trail_buffer_r = float(trailing_cfg.get("buffer_r", 0.10))

    # ── Circuit breaker ──
    cb_cfg = cfg.get("circuit_breaker") or {}
    cb_losses = int(cb_cfg.get("losses", 3))
    cb_cooldown = int(cb_cfg.get("cooldown_min", 60))

    # ── Notifications ──
    notif_cfg = cfg.get("notifications") or {}
    email_enabled = bool(notif_cfg.get("email_enabled", True))

    # ── Strategy ──
    strategy_cfg = cfg.get("strategy") or {}
    strat = load_strategy(strategy_cfg.get("module"))
    strat_params = strategy_cfg.get("params") or {}

    # ── Account ──
    account_cfg = cfg.get("account") or {}
    account_id = account_cfg.get("account_id") or os.environ.get("CAPITAL_ACCOUNT_ID")

    # ── Engine mode ──
    overrides = cfg.get("engine_overrides") or {}
    mode = overrides.get("mode")
    is_sp500_spec = (mode == "sp500_5m_spec")
    trailing_mode = overrides.get("trailing_mode", "sp500_spec" if is_sp500_spec else "option_a")
    rth_exit_enabled = overrides.get("rth_exit", not is_sp500_spec)
    tp_first = is_sp500_spec or (overrides.get("exit_priority") == "TP_FIRST")

    # ── Align poll to bar close ──
    align_poll = bool(cfg.get("align_poll_to_bar", True))

    # ── Config hot-reload tracking ──
    config_path = cfg.get("_config_path") or ""
    config_mtime = os.path.getmtime(config_path) if config_path and os.path.exists(config_path) else 0.0

    # ── Graceful shutdown ──
    _shutdown_flag.clear()
    _install_signal_handlers(logfile)

    client = CapitalClient()

    # ── Account currency ──
    _CURRENCY_SYMBOLS = {
        "USD": "$", "USDD": "$",
        "EUR": "€", "EURD": "€",
        "GBP": "£", "GBPD": "£",
        "CHF": "CHF ", "CHFD": "CHF ",
        "JPY": "¥", "JPYD": "¥",
        "AUD": "A$", "AUDD": "A$",
        "CAD": "C$", "CADD": "C$",
    }
    account_currency = "USD"
    currency_symbol = "$"
    try:
        sess_info = client.get_session()
        raw_ccy = (sess_info.get("currency") or "USD").upper()
        account_currency = raw_ccy.rstrip("D") if len(raw_ccy) == 4 and raw_ccy.endswith("D") else raw_ccy
        currency_symbol = _CURRENCY_SYMBOLS.get(raw_ccy, _CURRENCY_SYMBOLS.get(account_currency, account_currency + " "))
        log_line(logfile, f"ACCOUNT_CURRENCY: {raw_ccy} -> {account_currency} ({currency_symbol.strip()})")
    except Exception as e:
        log_line(logfile, f"ACCOUNT_CURRENCY warning (defaulting to USD): {repr(e)}")

    email_startup(email_enabled, bot_id, cfg, logfile)
    telegram_event(bot_id, "STARTUP", {"epic": epic, "resolution": resolution})

    last_closed_time = _as_ts(st.get("last_closed_time"))

    # ── Heartbeat watchdog ──
    watchdog_timeout = int(cfg.get("watchdog_timeout_sec", bar_minutes * 60 * 3))
    watchdog = _Watchdog(watchdog_timeout, bot_id, logfile, email_enabled, epic=epic)
    if not once:
        watchdog.start()

    log_line(logfile, f"BOT_START epic={epic} res={resolution} warmup={warmup} poll={poll}s align={align_poll} watchdog={watchdog_timeout}s")

    # ══════════════════════════════════════════════
    # MAIN LOOP
    # ══════════════════════════════════════════════
    while True:
        now = utc_now()

        # ── Graceful shutdown check ──
        if _shutdown_flag.is_set():
            log_line(logfile, "SHUTDOWN: graceful shutdown initiated")
            pos = st.get("pos") or {}
            deal_id = pos.get("deal_id")
            if deal_id:
                direction = str(pos.get("direction", "")).upper()
                log_line(logfile, f"SHUTDOWN: closing open position deal_id={deal_id}")
                try:
                    px = client.get_prices(epic, resolution, max_points=10)
                    df_shut = prices_to_df(px)
                    exit_price = float(df_shut["close"].iloc[-1]) if df_shut is not None and len(df_shut) > 0 else 0.0
                except Exception:
                    exit_price = float(pos.get("entry_price_est", 0))
                _handle_position_exit(
                    client, st, pos, deal_id, direction, "EXIT_SHUTDOWN", exit_price,
                    csv_path, bot_id, epic, vpp, cb_losses, cb_cooldown,
                    email_enabled, logfile, now,
                    currency_symbol=currency_symbol, account_currency=account_currency,
                )
                save_state(st)
            else:
                log_line(logfile, "SHUTDOWN: no open position, clean exit")
            watchdog.stop()
            lock.release()
            log_line(logfile, "SHUTDOWN: complete")
            return

        # ── Watchdog tick ──
        watchdog.tick()

        # ── Config hot-reload ──
        if config_path:
            new_cfg, config_mtime = _check_config_reload(config_path, config_mtime, logfile)
            if new_cfg:
                hot = _apply_hot_config(new_cfg, logfile)
                bot_equity = hot["bot_equity"]
                risk_pct = hot["risk_pct"]
                trailing_on = hot["trailing_on"]
                trail_buffer_r = hot["trail_buffer_r"]
                cb_losses = hot["cb_losses"]
                cb_cooldown = hot["cb_cooldown"]
                strat_params = hot["strat_params"]
                poll = hot["poll"]

        # ── Heartbeat at RTH open ──
        try:
            now_local = now.tz_convert(tz_name)
            if now_local.weekday() < 5:
                sh, sm = map(int, rth_start.split(":"))
                if now_local.hour == sh and now_local.minute == sm:
                    sent_date = st.get("heartbeat_sent_date")
                    today = now_local.date().isoformat()
                    if sent_date != today:
                        hb_payload = {
                            "time_local": now_local.isoformat(),
                            "timezone": tz_name,
                            "account_id": account_id,
                            "epic": epic,
                            "resolution": resolution,
                            "poll_seconds": poll,
                        }
                        email_event(email_enabled, bot_id, "HEARTBEAT_RTH_OPEN", hb_payload, logfile)
                        telegram_event(bot_id, "HEARTBEAT_RTH_OPEN", hb_payload)
                        st["heartbeat_sent_date"] = today
                        save_state(st)
        except Exception as e:
            log_line(logfile, f"HEARTBEAT warning: {repr(e)}")

        # ── Daily summary at RTH close ──
        try:
            now_local = now.tz_convert(tz_name)
            if now_local.weekday() < 5:
                eh, em = map(int, rth_end.split(":"))
                if now_local.hour == eh and now_local.minute == em:
                    summary_date = st.get("daily_summary_sent_date")
                    today = now_local.date().isoformat()
                    if summary_date != today:
                        _send_daily_summary(csv_path, bot_id, epic, email_enabled, logfile)
                        st["daily_summary_sent_date"] = today
                        save_state(st)
        except Exception as e:
            log_line(logfile, f"DAILY_SUMMARY warning: {repr(e)}")

        # ── Login + account ──
        try:
            client.login()
            client.ensure_account(account_id)
        except Exception as e:
            log_line(logfile, f"LOGIN error: {repr(e)}")
            if once:
                return
            time.sleep(poll)
            continue

        # ── Reconcile broker vs local state ──
        try:
            broker_pos = _extract_open_position_for_epic(client, epic)
            state_pos = st.get("pos") or {}
            state_deal = state_pos.get("deal_id")

            if broker_pos and not state_deal:
                st["pos"] = {k: v for k, v in broker_pos.items() if v is not None}
                st["pos"]["recovered_from_broker"] = True
                save_state(st)
                log_line(logfile, f"RECONCILE: recovered open position deal_id={st['pos'].get('deal_id')}")
            elif (not broker_pos) and state_deal:
                st["pos"] = {}
                save_state(st)
                log_line(logfile, f"RECONCILE: cleared stale local position deal_id={state_deal}")
        except Exception as e:
            log_line(logfile, f"RECONCILE warning: {repr(e)}")

        pos = st.get("pos") or {}
        deal_id = pos.get("deal_id")
        in_position = bool(deal_id)

        # ══════════════════════════════════════
        # POSITION MANAGEMENT (if in position)
        # ══════════════════════════════════════
        if in_position:
            px = client.get_prices(epic, resolution, max_points=max(warmup, 200))
            df = prices_to_df(px)
            df = _ensure_utc_datetime_index_safe(df)

            if df is None or getattr(df, "empty", True) or len(df) < 3:
                log_line(logfile, "WARN: no prices returned (in_position)")
                if once:
                    return
                time.sleep(0 if is_sp500_spec else poll)
                continue

            bar_t = _to_utc_ts(df.index[-2])
            last_mgmt = st.get("last_mgmt_bar_iso")
            if last_mgmt and str(last_mgmt) == str(_to_utc_ts(bar_t).isoformat()):
                if once:
                    return
                if not is_sp500_spec:
                    time.sleep(poll)
                    continue

            st["last_mgmt_bar_iso"] = _to_utc_ts(bar_t).isoformat()
            save_state(st)

            bar = df.iloc[-2]
            close_px = float(bar["close"])
            bar_high = float(bar["high"])
            bar_low = float(bar["low"])
            direction = str(pos.get("direction")).upper()

            # ── Exit outside RTH ──
            if rth_exit_enabled and rth_enabled and (not _rth_is_open(rth, now)):
                _handle_position_exit(
                    client, st, pos, deal_id, direction, "EXIT_RTH", close_px,
                    csv_path, bot_id, epic, vpp, cb_losses, cb_cooldown,
                    email_enabled, logfile, now,
                    currency_symbol=currency_symbol, account_currency=account_currency,
                )
                save_state(st)
                if once:
                    return
                time.sleep(0 if is_sp500_spec else poll)
                continue

            # ── Check SL/TP levels exist ──
            _tp = pos.get("tp_local")
            _sl = pos.get("sl_local")
            _rp = pos.get("r_points")
            if _tp is None or _sl is None or _rp is None:
                log_line(logfile, f"WARN: missing levels tp={_tp} sl={_sl} r_points={_rp} -> skip manage")
                if once:
                    return
                time.sleep(poll)
                continue
            tp_local = float(_tp)
            sl_local = float(_sl)

            # ── SL/TP hit check ──
            hit_sl = (direction == "BUY" and bar_low <= sl_local) or (direction == "SELL" and bar_high >= sl_local)
            hit_tp = (direction == "BUY" and bar_high >= tp_local) or (direction == "SELL" and bar_low <= tp_local)

            if hit_sl or hit_tp:
                if tp_first:
                    if hit_tp:
                        reason, exit_price = "EXIT_TP", float(tp_local)
                    else:
                        reason, exit_price = "EXIT_SL", float(sl_local)
                else:
                    if hit_sl:
                        reason, exit_price = "EXIT_SL", float(sl_local)
                    else:
                        reason, exit_price = "EXIT_TP", float(tp_local)

                _handle_position_exit(
                    client, st, pos, deal_id, direction, reason, exit_price,
                    csv_path, bot_id, epic, vpp, cb_losses, cb_cooldown,
                    email_enabled, logfile, now, mode_sp500=is_sp500_spec,
                    currency_symbol=currency_symbol, account_currency=account_currency,
                )
                save_state(st)
                if once:
                    return
                time.sleep(0 if is_sp500_spec else poll)
                continue

            # ── TIME_EXIT (generic: uses exit_bars from position state) ──
            pos_exit_bars = int(pos.get("exit_bars", 0))
            if is_sp500_spec and pos_exit_bars == 0:
                pos_exit_bars = 24  # backward compat: SP500 5m = 24 bars (2h)
            if pos_exit_bars > 0:
                try:
                    entry_bar_iso = pos.get("entry_bar_time_utc") or pos.get("ts_signal_utc") or pos.get("entry_time_utc")
                    if entry_bar_iso:
                        entry_bar = _to_utc_ts(entry_bar_iso)
                        bar_t_ts = _to_utc_ts(bar_t)
                        exit_minutes = pos_exit_bars * bar_minutes
                        if bar_t_ts >= entry_bar + pd.Timedelta(minutes=exit_minutes):
                            _handle_position_exit(
                                client, st, pos, deal_id, direction, "TIME_EXIT", close_px,
                                csv_path, bot_id, epic, vpp, cb_losses, cb_cooldown,
                                email_enabled, logfile, now, mode_sp500=is_sp500_spec,
                                currency_symbol=currency_symbol, account_currency=account_currency,
                            )
                            save_state(st)
                            if once:
                                return
                            time.sleep(0 if is_sp500_spec else poll)
                            continue
                except Exception as e:
                    log_line(logfile, f"TIME_EXIT warning: {repr(e)}")

            # ── Trailing stop ──
            if trailing_mode == "sp500_spec":
                try:
                    atr_entry = float(pos.get("atr_entry_const") or pos.get("atr_entry") or pos.get("atr_signal") or 0.0)
                    entry_est = float(pos.get("entry_price_est"))
                    sl_cur = float(pos.get("sl_local") or 0.0)
                    be_armed = bool(pos.get("be_armed", False))
                    max_fav = float(pos.get("max_fav", entry_est))
                    min_fav = float(pos.get("min_fav", entry_est))

                    new_sl, be_armed, max_fav, min_fav = _trail_sp500_spec(
                        direction=direction, entry=entry_est, atr_entry=atr_entry,
                        sl=sl_cur, be_armed=be_armed, max_fav=max_fav, min_fav=min_fav,
                        bar_high=bar_high, bar_low=bar_low,
                    )

                    if float(new_sl) != float(sl_cur) or bool(be_armed) != bool(pos.get("be_armed", False)):
                        pos["sl_local"] = float(new_sl)
                        pos["be_armed"] = bool(be_armed)
                        pos["max_fav"] = float(max_fav)
                        pos["min_fav"] = float(min_fav)
                        st["pos"] = pos
                        save_state(st)
                        email_event(email_enabled, bot_id, "TRAIL_SL", {"epic": epic, "deal_id": deal_id, "sl_local": round(new_sl, 2), "be_armed": be_armed}, logfile)
                        telegram_event(bot_id, "TRAIL_SL", {"epic": epic, "deal_id": deal_id, "sl_local": round(new_sl, 2), "be_armed": be_armed})
                except Exception as e:
                    log_line(logfile, f"TRAIL warning (sp500): {repr(e)}")
            elif trailing_mode == "option_a" and trailing_on:
                try:
                    moved, new_sl, flags = maybe_trail_option_a(
                        direction=direction,
                        entry=float(pos.get("entry_price_est")),
                        live=float(close_px),
                        r_points=float(pos.get("r_points")),
                        current_sl=float(pos.get("sl_local")),
                        trail_1r_done=bool(pos.get("trail_1r_done")),
                        trail_2r_done=bool(pos.get("trail_2r_done")),
                        buffer_r=float(trail_buffer_r),
                    )
                    if moved:
                        prev_1r = bool(pos.get("trail_1r_done"))
                        prev_2r = bool(pos.get("trail_2r_done"))

                        pos["sl_local"] = float(new_sl)
                        pos["trail_1r_done"] = bool(flags.get("trail_1r_done"))
                        pos["trail_2r_done"] = bool(flags.get("trail_2r_done"))
                        st["pos"] = pos
                        save_state(st)

                        if (not prev_1r) and flags.get("trail_1r_done"):
                            email_event(email_enabled, bot_id, "TRAIL_1R", {"epic": epic, "deal_id": deal_id, "sl_local": round(new_sl, 2)}, logfile)
                            telegram_event(bot_id, "TRAIL_1R", {"epic": epic, "deal_id": deal_id, "sl_local": round(new_sl, 2)})
                        if (not prev_2r) and flags.get("trail_2r_done"):
                            email_event(email_enabled, bot_id, "TRAIL_2R", {"epic": epic, "deal_id": deal_id, "sl_local": round(new_sl, 2)}, logfile)
                            telegram_event(bot_id, "TRAIL_2R", {"epic": epic, "deal_id": deal_id, "sl_local": round(new_sl, 2)})

                        email_event(email_enabled, bot_id, "TRAIL_SL", {"epic": epic, "deal_id": deal_id, "sl_local": round(new_sl, 2)}, logfile)
                        telegram_event(bot_id, "TRAIL_SL", {"epic": epic, "deal_id": deal_id, "sl_local": round(new_sl, 2)})
                except Exception as e:
                    log_line(logfile, f"TRAIL warning: {repr(e)}")

            if once:
                return
            time.sleep(poll)
            continue

        # ══════════════════════════════════════
        # ENTRY GATES (no position open)
        # ══════════════════════════════════════

        # Fetch prices ONCE for both gates check and signal detection
        px = client.get_prices(epic, resolution, max_points=max(warmup, 200))
        df = prices_to_df(px)
        if df is None or df.empty or len(df) < 3:
            log_line(logfile, "WARN: no prices returned")
            if once:
                return
            time.sleep(poll)
            continue

        df = _ensure_utc_datetime_index_safe(df)
        if df is None or getattr(df, "empty", True):
            log_line(logfile, "WARN: df empty after index normalization")
            if once:
                return
            time.sleep(poll)
            continue

        # Enrich ONCE — reused for VIS output and signal detection
        try:
            df = strat.enrich(df, strat_params)
        except TypeError:
            df = strat.enrich(df)

        if df is None or getattr(df, "empty", True):
            log_line(logfile, "WARN: df empty after enrich")
            if once:
                return
            time.sleep(poll)
            continue

        # ── Visual CHECK output (always, using cached enriched df) ──
        try:
            vis_line = _compute_vis_checks(
                df, strat_params, rth, rth_enabled, tz_name, now,
                disable_thursday_utc, no_trade_hours, st, strat=strat,
            )
            log_line(logfile, vis_line)
        except Exception as e:
            log_line(logfile, f"CHECK_VIS warning: {repr(e)}")

        # ── Gate 1: Thursday UTC ──
        if disable_thursday_utc and now.weekday() == 3:
            log_line(logfile, "GATE: Thursday UTC disabled")
            if once:
                return
            time.sleep(poll)
            continue

        # ── Gate 2: RTH ──
        if rth_enabled and (not _rth_is_open(rth, now)):
            log_line(logfile, f"GATE: outside RTH ({rth_start}-{rth_end} {tz_name})")
            if once:
                return
            time.sleep(poll)
            continue

        # ── Gate 3: No-trade hours ──
        try:
            now_local = now.tz_convert(tz_name)
            if int(now_local.hour) in set(int(x) for x in no_trade_hours):
                log_line(logfile, f"GATE: NO_TRADE_HOURS hour={now_local.hour}")
                if once:
                    return
                if not is_sp500_spec:
                    time.sleep(poll)
                    continue
        except Exception:
            pass

        # ── Gate 4: Circuit breaker ──
        cooldown_until = _as_ts(st.get("cooldown_until_iso"))
        if cooldown_until and now < cooldown_until:
            log_line(logfile, f"GATE: circuit breaker until {cooldown_until.isoformat()}")
            if once:
                return
            time.sleep(poll)
            continue

        # ══════════════════════════════════════
        # SIGNAL DETECTION (uses cached enriched df)
        # ══════════════════════════════════════
        sig = strat.signal_on_bar_close(df, strat_params)

        if sig is None:
            log_line(logfile, "SIGNAL: none")
            if once:
                return
            # Align poll to next bar close for faster detection
            if align_poll:
                wait = _wait_seconds_until_next_bar(bar_minutes)
                time.sleep(wait)
            else:
                time.sleep(poll)
            continue

        # ── Dedupe: skip if same signal bar already processed ──
        signal_bar_time = df.index[-2]
        if last_closed_time and signal_bar_time <= last_closed_time:
            if once:
                return
            time.sleep(poll)
            continue

        # ══════════════════════════════════════
        # POSITION SIZING + RISK LEVELS
        # ══════════════════════════════════════
        if (cfg.get("engine_overrides") or {}).get("entry_mode") == "SIGNAL_CLOSE":
            entry_price = float(df["close"].iloc[-2])
            entry_time = df.index[-2]
        else:
            entry_price = float(df["open"].iloc[-1])
            entry_time = df.index[-1]

        atr_signal = float(df["atr14"].iloc[-2])
        init = strat.initial_risk(entry_price, atr_signal, sig, strat_params)
        r_points = float(init["r_points"])
        sl_local = float(init["sl_local"])
        tp_local = float(init["tp_local"])
        tp_r_multiple = float(init.get("tp_r_multiple", 3.0))

        size = calc_position_size(
            bot_equity=bot_equity,
            risk_pct=risk_pct,
            r_points=r_points,
            value_per_point_per_size=vpp,
        )

        log_line(logfile, f"SIGNAL {sig.direction} entry={entry_price:.2f} size={size} R={r_points:.2f} SL={sl_local:.2f} TP({tp_r_multiple}R)={tp_local:.2f}")

        # ── Guard: already have a position on this epic ──
        try:
            existing = _extract_open_position_for_epic(client, epic)
            if existing:
                log_line(logfile, f"GATE: OPEN_POSITION_GUARD epic={epic} -> skip")
                log_line(logfile, "ENTRY \u274c")
                last_closed_time = signal_bar_time
                st["last_closed_time"] = signal_bar_time.isoformat()
                save_state(st)
                if once:
                    return
                time.sleep(poll)
                continue
        except Exception as e:
            log_line(logfile, f"OPEN_POSITION_GUARD warning: {repr(e)}")

        # ══════════════════════════════════════
        # OPEN POSITION
        # ══════════════════════════════════════
        resp = client.open_market(epic, sig.direction, size)
        log_line(logfile, f"OPEN_MARKET resp={str(resp)[:300]}")

        # ── Confirm position opened (with hard timeout) ──
        deal_ref = (resp or {}).get("dealReference") or (resp or {}).get("deal_reference")
        fill_timeout = int((cfg.get("engine_overrides") or {}).get("fill_timeout_sec", 45))
        fill_t0 = time.monotonic()

        # Method 1: Poll broker positions (fast, ~3s)
        confirmed_via_broker = False
        broker_deal_id = None
        for _i in range(10):
            if time.monotonic() - fill_t0 > fill_timeout:
                break
            time.sleep(0.3)
            bp = _extract_open_position_for_epic(client, epic)
            if bp:
                confirmed_via_broker = True
                broker_deal_id = bp.get("deal_id")
                break

        # Method 2: Confirm via deal reference
        deal_id = broker_deal_id
        conf = {}
        if deal_ref and (time.monotonic() - fill_t0 < fill_timeout):
            for _i in range(3):
                if time.monotonic() - fill_t0 > fill_timeout:
                    break
                conf = client.confirm(str(deal_ref), timeout_sec=min(15, fill_timeout)) or {}
                did = pick_position_dealid_from_confirm(conf)
                if did:
                    deal_id = did
                    break
                time.sleep(1)

        # CRITICAL: Always track position if it exists on broker
        if not deal_id and confirmed_via_broker:
            deal_id = broker_deal_id
            log_line(logfile, f"CONFIRM_FALLBACK: using broker-detected deal_id={deal_id}")

        if not deal_id:
            # Last resort: check broker one more time
            bp = _extract_open_position_for_epic(client, epic)
            if bp:
                deal_id = bp.get("deal_id")
                log_line(logfile, f"CONFIRM_LAST_RESORT: found deal_id={deal_id}")

        # Hard timeout: if still no deal_id, check if position exists and force-close
        fill_elapsed = time.monotonic() - fill_t0
        if not deal_id and fill_elapsed >= fill_timeout:
            log_line(logfile, f"FILL_TIMEOUT: {int(fill_elapsed)}s elapsed, checking for orphan position")
            orphan = _extract_open_position_for_epic(client, epic)
            if orphan:
                orphan_id = orphan.get("deal_id")
                log_line(logfile, f"FILL_TIMEOUT: force-closing orphan deal_id={orphan_id}")
                safe_close_position(client, orphan_id)
                email_event(email_enabled, bot_id, "FILL_TIMEOUT", {
                    "epic": epic, "action": "force_closed", "deal_id": orphan_id,
                    "elapsed_sec": int(fill_elapsed),
                }, logfile)
                telegram_event(bot_id, "FILL_TIMEOUT", {
                    "epic": epic, "action": "force_closed", "deal_id": orphan_id,
                    "elapsed_sec": int(fill_elapsed),
                })

        if not deal_id:
            log_line(logfile, f"ENTRY ERROR: no deal_id after {int(fill_elapsed)}s. resp={str(resp)[:250]} conf={str(conf)[:250]}")
            last_closed_time = signal_bar_time
            st["last_closed_time"] = signal_bar_time.isoformat()
            save_state(st)
            if once:
                return
            time.sleep(poll)
            continue

        # ── Position confirmed: save full state ──
        last_closed_time = signal_bar_time
        st["last_closed_time"] = signal_bar_time.isoformat()

        broker_snap_open = _fetch_broker_open_snap(client, epic, str(deal_id))

        exit_bars = int(init.get("exit_bars", 0))

        st["pos"] = {
            "deal_id": str(deal_id),
            "direction": sig.direction,
            "size": float(size),
            "entry_price_est": float(entry_price),
            "r_points": float(r_points),
            "sl_local": float(sl_local),
            "tp_local": float(tp_local),
            "tp_r_multiple": float(tp_r_multiple),
            "atr_signal": float(atr_signal),
            "atr_entry_const": float(atr_signal),
            "trail_1r_done": False,
            "trail_2r_done": False,
            "entry_bar_time_utc": str(entry_time),
            "ts_signal_utc": str(signal_bar_time),
            "exit_bars": exit_bars,
            "broker_snap_open": broker_snap_open,
        }
        save_state(st)

        log_line(logfile, f"ENTRY \u2705 deal_id={deal_id} {sig.direction} size={size} entry={entry_price:.2f} SL={sl_local:.2f} TP={tp_local:.2f}")

        # Sync SL/TP to broker (so Capital.com also protects the position)
        try:
            client.update_position(str(deal_id), stop_level=round(sl_local, 2), profit_level=round(tp_local, 2))
            log_line(logfile, f"BROKER_SL_TP_SYNC OK deal_id={deal_id} SL={sl_local:.2f} TP={tp_local:.2f}")
        except Exception as e:
            log_line(logfile, f"BROKER_SL_TP_SYNC warning: {repr(e)}")

        email_event(email_enabled, bot_id, "TRADE_OPEN", {
            "epic": epic, "resolution": resolution, "direction": sig.direction, "size": size,
            "entry_price": round(entry_price, 2), "sl": round(sl_local, 2), "tp": round(tp_local, 2),
            "deal_id": deal_id, "account_id": account_id,
        }, logfile)

        telegram_event(bot_id, "TRADE_OPEN", {
            "epic": epic, "resolution": resolution, "direction": sig.direction, "size": size,
            "entry_price": round(entry_price, 2), "sl": round(sl_local, 2), "tp": round(tp_local, 2),
            "deal_id": deal_id,
        })

        if once:
            return
        time.sleep(poll)
