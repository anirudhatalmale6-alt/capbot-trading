from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any, Dict, Tuple

ICON = {
    "STARTED": "🚀",
    "HEALTH": "✅",
    "MARKET_OPEN": "🔔",
    "MARKET_CLOSE": "🌙",
    "ENTRY_OPENED": "🟢",
    "TRADE_OPEN": "🟢",
    "EXIT_TP": "🎯",
    "EXIT_SL": "🛑",
    "EXIT_TIME": "⏱️",
    "EXIT_RTH": "🌙",
    "TRADE_CLOSE": "🏁",
    "TRAIL_SL": "🧷",
    "ERROR": "⚠️",
}

EVENT_LABEL = {
    "STARTED": "STARTED",
    "HEALTH": "HEALTH",
    "MARKET_OPEN": "MARKET OPEN",
    "MARKET_CLOSE": "MARKET CLOSE",
    "ENTRY_OPENED": "ENTRY",
    "TRADE_OPEN": "ENTRY",
    "EXIT_TP": "EXIT (TP)",
    "EXIT_SL": "EXIT (SL)",
    "EXIT_TIME": "EXIT (TIME)",
    "EXIT_RTH": "EXIT (RTH)",
    "TRADE_CLOSE": "EXIT",
    "TRAIL_SL": "TRAIL SL",
    "ERROR": "ERROR",
}


def _fmt(v: Any, nd: int = 2) -> str:
    try:
        if v is None:
            return "-"
        return f"{float(v):.{nd}f}"
    except Exception:
        return str(v) if v is not None else "-"


def _pick(*vals: Any, default: str = "") -> str:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return default


def _market_and_tf(payload: Dict[str, Any], meta: Dict[str, Any]) -> Tuple[str, str]:
    raw_market = (
        payload.get("market")
        or payload.get("symbol")
        or payload.get("epic")
        or meta.get("market")
        or "Germany 40"
    )

    market_dict = raw_market if isinstance(raw_market, dict) else None

    # If market is a dict, extract a human-readable identifier
    if isinstance(raw_market, dict):
        raw_market = (
            raw_market.get("label")
            or raw_market.get("name")
            or raw_market.get("market")
            or raw_market.get("symbol")
            or raw_market.get("epic")
            or "Germany 40"
        )

    market = str(raw_market).replace("_", " ").strip()

    # Normalize epic -> display name
    upper = market.upper().replace(" ", "")
    if upper in ("DE40", "GER40", "GERMANY40", "GERMANY40CASH"):
        market = "Germany 40"

    # Timeframe/resolution
    tf = (
        payload.get("timeframe")
        or payload.get("tf")
        or payload.get("resolution")
        or meta.get("timeframe")
        or (market_dict.get("resolution") if isinstance(market_dict, dict) else None)
        or "5m"
    )

    tf = str(tf).strip()

    # Normalize MINUTE_5 / MINUTE 5 / etc -> 5m
    tfx = tf.upper().replace(" ", "_")
    if tfx.startswith("MINUTE_"):
        try:
            n = int(tfx.split("_")[-1])
            tf = f"{n}m"
        except Exception:
            pass

    return market, tf


def subject(event: str, payload: Dict[str, Any], meta: Dict[str, Any]) -> str:
    market, tf = _market_and_tf(payload, meta)
    ico = ICON.get(event, "📣")
    label = EVENT_LABEL.get(event, event)

    # Show pass/fail icon for HEALTH events
    if event == "HEALTH":
        ok = payload.get("ok")
        if ok is True:
            ico = "✅"
        elif ok is False:
            ico = "❌"

    return f"{market} {tf} | {label} {ico}"


def render_email(event: str, bot_id: str, payload: Dict[str, Any], meta: Dict[str, Any]) -> Tuple[str, str]:
    """Return (text_body, html_body) for an email notification."""
    market, tf = _market_and_tf(payload, meta)
    ico = ICON.get(event, "📣")
    label = EVENT_LABEL.get(event, event)

    # headline
    headline = f"{market} {tf} · {label} {ico}"

    # summary (short line above details)
    summary_bits = []
    direction = payload.get("direction")
    if direction:
        summary_bits.append(str(direction).upper())

    deal_id = payload.get("deal_id") or payload.get("id")
    if deal_id:
        summary_bits.append(f"deal {deal_id}")

    entry_px = payload.get("entry_price") or payload.get("entry")
    exit_px = payload.get("exit_price") or payload.get("exit")
    if entry_px is not None and exit_px is not None:
        summary_bits.append(f"{_fmt(entry_px)} → {_fmt(exit_px)}")

    pnl_cash = payload.get("profit_cash") or payload.get("pnl_cash")
    pnl_pts = payload.get("profit_points") or payload.get("pnl_points")
    if pnl_cash is not None:
        summary_bits.append(f"PnL {_fmt(pnl_cash)}")
    elif pnl_pts is not None:
        summary_bits.append(f"PnL {_fmt(pnl_pts)} pts")

    summary = " · ".join(summary_bits) if summary_bits else ""

    # detail table (only useful fields)
    rows = []
    def add(k: str, v: Any):
        if v is None or v == "" or v == "-":
            return
        rows.append((k, v))

    add("Bot", bot_id)
    add("Market", f"{market} {tf}")
    add("Time (UTC)", meta.get("ts_utc"))
    add("Time (Local)", meta.get("ts_local"))
    add("Host", meta.get("host"))
    add("Service", meta.get("service"))
    add("Config", meta.get("config_path"))
    add("Log", meta.get("logfile"))

    # trade bits
    add("Direction", direction)
    add("Size", payload.get("size"))
    add("Entry", entry_px)
    add("SL", payload.get("sl_local") or payload.get("sl"))
    add("TP", payload.get("tp_local") or payload.get("tp"))
    add("Exit", exit_px)

    if pnl_cash is not None:
        add("PnL (cash)", pnl_cash)
    if pnl_pts is not None:
        add("PnL (pts)", pnl_pts)

    # health bits
    add("State", payload.get("state"))
    add("Log age (min)", payload.get("log_age_min"))
    add("Last", payload.get("last"))

    # error
    add("Error", payload.get("error"))

    # footer
    build = meta.get("build") or ""
    footer = _pick(build, default="")

    # TEXT
    text_lines = [headline]
    if summary:
        text_lines.append(summary)
    text_lines.append("")
    for k, v in rows:
        text_lines.append(f"{k}: {v}")
    if footer:
        text_lines.append("")
        text_lines.append(footer)
    text_body = "\n".join(text_lines).strip() + "\n"

    # HTML
    def tr(k: str, v: Any) -> str:
        return (
            "<tr>"
            f"<td style='padding:8px 10px;color:#6b7280;white-space:nowrap;border-bottom:1px solid #eef0f6'>{escape(str(k))}</td>"
            f"<td style='padding:8px 10px;border-bottom:1px solid #eef0f6'>{escape(str(v))}</td>"
            "</tr>"
        )

    table_html = "".join(tr(k, v) for k, v in rows) if rows else ""

    html = f"""<!doctype html>
<html>
  <body style="margin:0;padding:20px;background:#f6f7fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
    <div style="max-width:720px;margin:0 auto;">
      <div style="background:#ffffff;border:1px solid #e6e8f0;border-radius:14px;overflow:hidden;box-shadow:0 6px 18px rgba(17,24,39,0.06);">
        <div style="padding:18px 18px 10px 18px;border-bottom:1px solid #eef0f6;">
          <div style="font-size:18px;font-weight:700;color:#111827;line-height:1.2;">{escape(headline)}</div>
          {"<div style='margin-top:6px;font-size:13px;color:#6b7280;line-height:1.4;'>" + escape(summary) + "</div>" if summary else ""}
        </div>

        <div style="padding:0 18px 16px 18px;">
          <table style="width:100%;border-collapse:collapse;font-size:13px;color:#111827;margin-top:10px;">
            {table_html}
          </table>

          {"<div style='margin-top:12px;font-size:12px;color:#6b7280;'>" + escape(footer) + "</div>" if footer else ""}
        </div>
      </div>
    </div>
  </body>
</html>
"""
    return text_body, html
