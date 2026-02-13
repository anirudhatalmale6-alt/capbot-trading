from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from typing import Dict, Any

from capbot.app.notifier import email_event


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _run(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True).strip()


def _is_active(service: str) -> bool:
    try:
        out = _run(["systemctl", "is-active", service])
        return out.strip() == "active"
    except Exception:
        return False


def _tail(path: str, n: int = 1) -> str:
    try:
        out = _run(["tail", "-n", str(n), path])
        return out.splitlines()[-1] if out else ""
    except Exception:
        return ""


def main() -> int:
    bot_id = _env("HEALTH_BOT_ID", "germany40_5m_vwap")
    market = _env("HEALTH_MARKET", "Germany 40 5m").replace("_", " ").strip()
    tz = _env("HEALTH_TZ", "Europe/Berlin")
    service = _env("HEALTH_SERVICE", "capbot-germany40.service")
    logfile = _env("HEALTH_LOGFILE", "/home/ubuntu/capbot_events_germany40_5m_vwap.log")
    max_age_min = float(_env("HEALTH_MAX_AGE_MIN", "10") or "10")

    now = datetime.now(timezone.utc)
    ts_utc = now.strftime("%Y-%m-%d %H:%M:%SZ")

    active = _is_active(service)

    # log age
    log_age_min = 9999.0
    try:
        mtime = os.path.getmtime(logfile)
        log_age_min = max(0.0, (now.timestamp() - mtime) / 60.0)
    except Exception:
        pass

    last = _tail(logfile, 1)

    ok = bool(active and (log_age_min <= max_age_min))

    payload: Dict[str, Any] = {
        "event": "HEALTH",
        "bot_id": bot_id,
        "market": market.replace("  ", " "),
        "timeframe": "5m" if "5m" in market.lower() else "",
        "tz": tz,
        "service": service,
        "logfile": logfile,
        "state": "active" if active else "inactive",
        "log_age_min": round(log_age_min, 2),
        "last": last,
        "ok": ok,
        "ts_utc": ts_utc,
        # local lo dejamos vacío si no tenemos tz convert aquí; igual se ve bien
        "ts_local": "",
    }

    email_event(True, bot_id, "HEALTH", payload, logfile=logfile, cfg=None)
    print(f"SENT: {market} {'✅' if ok else '❌'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
