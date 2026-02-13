"""
Telegram notification module for capbot.

Setup:
  1. Create a bot via @BotFather on Telegram -> get BOT_TOKEN
  2. Send /start to your bot, then get your chat_id via https://api.telegram.org/bot<TOKEN>/getUpdates
  3. Set environment variables:
       TELEGRAM_BOT_TOKEN=<your_bot_token>
       TELEGRAM_CHAT_ID=<your_chat_id>

All methods are fail-safe (never raise to caller).
"""
import os
import logging

log = logging.getLogger(__name__)


def _send_telegram(text: str) -> bool:
    """Send a message via Telegram Bot API. Returns True if sent."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        return False

    try:
        import requests
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=10)
        if resp.status_code == 200:
            log.info("TELEGRAM_SENT chat_id=%s", chat_id)
            return True
        else:
            log.warning("TELEGRAM_FAILED status=%s body=%s", resp.status_code, resp.text[:200])
            return False
    except Exception as e:
        log.warning("TELEGRAM_ERROR: %r", e)
        return False


def _format_event(bot_id: str, event: str, payload: dict) -> str:
    """Format event into a readable Telegram message."""
    lines = [f"<b>[{bot_id}] {event}</b>"]

    if event == "TRADE_OPEN":
        lines.append(f"\u2705 {payload.get('direction', '?')} {payload.get('epic', '?')}")
        lines.append(f"Entry: {payload.get('entry_price', '?')}")
        lines.append(f"Size: {payload.get('size', '?')}")
        lines.append(f"SL: {payload.get('sl', '?')} | TP: {payload.get('tp', '?')}")

    elif event in ("EXIT_TP", "EXIT_SL", "EXIT_RTH", "TIME_EXIT"):
        emoji = "\U0001f4b0" if event == "EXIT_TP" else "\U0001f6d1" if event == "EXIT_SL" else "\u23f0"
        pnl = payload.get("profit_cash", 0)
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        lines.append(f"{emoji} {payload.get('direction', '?')} closed @ {payload.get('exit_price', '?')}")
        lines.append(f"P&L: {pnl_str} ({payload.get('profit_points', 0):.1f} pts)")

    elif event in ("TRAIL_1R", "TRAIL_2R", "TRAIL_SL"):
        lines.append(f"\U0001f4c8 SL moved to {payload.get('sl_local', '?')}")

    elif event == "STARTUP":
        lines.append(f"Bot started: {payload.get('epic', '?')} {payload.get('resolution', '?')}")

    elif event == "HEALTH":
        for k, v in (payload or {}).items():
            lines.append(f"  {k}: {v}")

    else:
        for k, v in list((payload or {}).items())[:5]:
            lines.append(f"  {k}: {v}")

    return "\n".join(lines)


def telegram_event(bot_id: str, event: str, payload: dict = None) -> bool:
    """Send a trading event notification via Telegram. Never raises."""
    try:
        text = _format_event(str(bot_id), str(event), payload or {})
        return _send_telegram(text)
    except Exception as e:
        log.warning("telegram_event error: %r", e)
        return False
