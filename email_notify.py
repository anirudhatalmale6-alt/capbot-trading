import os
import smtplib
from email.message import EmailMessage
from typing import Optional

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default

def send_trade_email(subject: str, body: str) -> bool:
    host = _env("SMTP_HOST")
    port = int(_env("SMTP_PORT", "587"))
    user = _env("SMTP_USER")
    password = _env("SMTP_PASS")
    mail_from = _env("EMAIL_FROM", user)
    mail_to = _env("EMAIL_TO")

    if not host or not user or not password or not mail_to or not mail_from:
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg.set_content(body or "")

    try:
        with smtplib.SMTP(host, port, timeout=20) as s:
            s.ehlo()
            try:
                s.starttls()
                s.ehlo()
            except Exception:
                pass
            s.login(user, password)
            s.send_message(msg)
        return True
    except Exception:
        return False
