#!/usr/bin/env python3
import os, sys, subprocess
from datetime import datetime, timezone
from pathlib import Path
import smtplib
from email.message import EmailMessage

SECRETS = "/etc/capbot/germany40.secrets"
SERVICE = "capbot-germany40.service"
LOGFILE = "/home/ubuntu/capbot_events_germany40_5m_vwap.log"
MAX_AGE_MIN = 10

def load_env(path):
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"ERROR: secrets not found: {path}")
    for line in p.read_text().splitlines():
        line=line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k,v = line.split("=",1)
        k=k.strip(); v=v.strip()
        # remove optional quotes
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        os.environ.setdefault(k, v)

def sh(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)

def service_state():
    r = sh(["systemctl","is-active",SERVICE])
    return r.stdout.strip() if r.stdout else "unknown"

def minutes_since_mtime(path):
    p=Path(path)
    if not p.exists():
        return None
    mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - mtime).total_seconds()/60.0

def tail_last_line(path):
    p=Path(path)
    if not p.exists():
        return "(log missing)"
    try:
        data = p.read_text(errors="ignore").splitlines()
        return data[-1] if data else "(log empty)"
    except Exception as e:
        return f"(tail failed: {e!r})"

def send_email(subject, body):
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT","587"))
    user = os.environ.get("SMTP_USER")
    pw   = os.environ.get("SMTP_PASS")
    to_  = os.environ.get("EMAIL_TO")
    frm  = os.environ.get("EMAIL_FROM")

    missing = [k for k in ("SMTP_HOST","SMTP_PORT","SMTP_USER","SMTP_PASS","EMAIL_TO","EMAIL_FROM") if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"ERROR: missing in secrets: {', '.join(missing)}")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = frm
    msg["To"] = to_
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=20) as s:
        s.ehlo()
        try:
            s.starttls()
            s.ehlo()
        except Exception:
            # some servers may be implicit TLS or no TLS; keep going
            pass
        if user and pw:
            s.login(user, pw)
        s.send_message(msg)

def main():
    load_env(SECRETS)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    st = service_state()
    age = minutes_since_mtime(LOGFILE)
    age_str = "NA" if age is None else f"{age:.1f}m"
    recent_ok = (age is not None and age <= MAX_AGE_MIN)
    last = tail_last_line(LOGFILE)

    ok = (st == "active") and recent_ok

    subject = f"[CapBot] Germany40 health @ {now} | service={st} log_age={age_str}"
    body = "\n".join([
        f"CapBot Healthcheck (Germany40) @ {now}",
        f"SERVICE: {SERVICE}",
        f"STATE:   {st}",
        f"LOG:     {LOGFILE}",
        f"LOG_AGE: {age_str} (<= {MAX_AGE_MIN}m is OK)",
        f"LOG_RECENT_OK: {recent_ok}",
        "",
        "LAST_LOG_LINE:",
        last,
        "",
        f"RESULT: {'OK ✅' if ok else 'NOT OK ❌'}",
    ])

    send_email(subject, body)
    print(body)

if __name__ == "__main__":
    main()
