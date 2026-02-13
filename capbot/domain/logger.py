from pathlib import Path


def log_line(logfile, msg: str) -> str:
    """Log with Berlin timezone prefix. Writes to stdout + logfile."""
    try:
        import pandas as pd
        ts_local = pd.Timestamp.utcnow().tz_localize("UTC").tz_convert("Europe/Berlin")
        prefix = "[" + ts_local.isoformat() + "] "
    except Exception:
        try:
            from datetime import datetime
            import pytz
            prefix = "[" + datetime.now(pytz.timezone("Europe/Berlin")).isoformat() + "] "
        except Exception:
            from datetime import datetime, timezone
            prefix = "[" + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z] "

    line = prefix + str(msg)

    try:
        print(line, flush=True)
    except Exception:
        pass

    try:
        if logfile:
            _p = logfile if hasattr(logfile, "write_text") else Path(str(logfile))
            _p.parent.mkdir(parents=True, exist_ok=True)
            with open(str(_p), "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
    except Exception:
        pass

    return line
