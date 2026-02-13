#!/usr/bin/env bash
set -euo pipefail

SERVICE="capbot-germany40.service"
LOG="/home/ubuntu/capbot_events_germany40_5m_vwap.log"

# ✅ pon aquí tu email receptor
TO="pedro@andyapp.io"

# “vivo” = ha escrito algo en los últimos N minutos
MAX_AGE_MIN=10

now_utc="$(date -u +'%Y-%m-%d %H:%M:%SZ')"

svc_state="$(systemctl is-active "$SERVICE" 2>/dev/null || true)"

# Última línea del log (si existe)
last_line=""
last_mtime_ok="UNKNOWN"
if [[ -f "$LOG" ]]; then
  last_line="$(tail -n 1 "$LOG" 2>/dev/null || true)"

  # edad del archivo en minutos (Linux stat -c %Y)
  last_epoch="$(stat -c %Y "$LOG" 2>/dev/null || echo 0)"
  now_epoch="$(date +%s)"
  age_min="$(( (now_epoch - last_epoch) / 60 ))"
  if (( age_min <= MAX_AGE_MIN )); then
    last_mtime_ok="YES (age=${age_min}m)"
  else
    last_mtime_ok="NO (age=${age_min}m)"
  fi
fi

# Construye asunto + body
subject=""
body="CapBot Healthcheck (Germany40) @ ${now_utc}\n\n"
body+="SERVICE: ${SERVICE}\n"
body+="STATE:   ${svc_state}\n"
body+="LOG:     ${LOG}\n"
body+="LOG_RECENT(${MAX_AGE_MIN}m): ${last_mtime_ok}\n\n"
body+="LAST_LOG_LINE:\n${last_line}\n"

ok=true
if [[ "$svc_state" != "active" ]]; then ok=false; fi
if [[ "$last_mtime_ok" == NO* ]]; then ok=false; fi

if $ok; then
  subject="✅ CapBot Germany40 OK (09:30 Berlin)"
else
  subject="🚨 CapBot Germany40 ALERT (09:30 Berlin)"
fi

send_mail() {
  if command -v mail >/dev/null 2>&1; then
    printf "%b" "$body" | mail -s "$subject" "$TO"
    return 0
  fi
  if command -v sendmail >/dev/null 2>&1; then
    {
      echo "To: $TO"
      echo "Subject: $subject"
      echo
      printf "%b" "$body"
    } | sendmail -t
    return 0
  fi
  return 1
}

if ! send_mail; then
  echo "ERROR: No mail/sendmail found. Install: sudo apt-get update && sudo apt-get install -y mailutils"
  echo -e "$body"
  exit 2
fi
