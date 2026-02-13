import os, time, socket, subprocess
from datetime import datetime, timezone

from capbot.app.notifier import email_event

def sh(cmd: str) -> str:
    return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT).strip()

def main():
    market  = os.getenv("HEALTH_MARKET", "Germany_40_5m")
    tz      = os.getenv("HEALTH_TZ", "Europe/Berlin")
    service = os.getenv("HEALTH_SERVICE", "capbot-germany40.service")
    logfile = os.getenv("HEALTH_LOGFILE", "/home/ubuntu/capbot_events_germany40_5m_vwap.log")
    max_age = float(os.getenv("HEALTH_MAX_AGE_MIN", "10"))

    ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    host = socket.gethostname()

    state = "unknown"
    try:
        state = sh(f"systemctl is-active {service}")
    except Exception:
        state = "unknown"

    log_age_min = None
    last_line = ""
    try:
        st = os.stat(logfile)
        age_sec = time.time() - st.st_mtime
        log_age_min = age_sec / 60.0
        last_line = sh(f"tail -n 1 {logfile}")
    except Exception as e:
        last_line = f"LOG_ERR: {repr(e)}"

    ok = (state == "active") and (log_age_min is not None) and (log_age_min <= max_age)

    payload = {
        "event": "HEALTH",
        "bot_id": os.getenv("HEALTH_BOT_ID", "germany40_5m_vwap"),
        "market": market,
        "timeframe": "5m",
        "tz": tz,
        "state": state,
        "logfile": logfile,
        "log_age_min": (None if log_age_min is None else round(log_age_min, 2)),
        "last": last_line,
        "result": ("OK ✅" if ok else "NOT OK ❌"),
        "host": host,
        "service": service,
        "ts_utc": ts_utc,
    }

    # usa tu pipeline bonito
    email_event(True, payload["bot_id"], "HEALTH", payload, logfile=logfile, cfg=None)

if __name__ == "__main__":
    main()
