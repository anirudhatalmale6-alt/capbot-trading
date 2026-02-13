import argparse
from pathlib import Path

from capbot.app.config import load_config
from capbot.app.engine_sp500_rth import run_bot
from capbot.domain.logger import log_line
from capbot.domain.paths import bot_paths


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    runp = sub.add_parser("run")
    runp.add_argument("--config", required=True)
    # Keep a default, but systemd passes the real one anyway.
    runp.add_argument("--secrets", default=str(Path.home() / ".capital_secrets.md"))
    runp.add_argument("--once", action="store_true")

    args = ap.parse_args()

    if args.cmd == "run":
        cfg = load_config(args.config).raw

        bot_id = str(cfg.get("bot_id") or "sp500_5m_rth")
        _, _, logfile, _ = bot_paths(bot_id)

        # Best-effort secrets loader: don't hard-crash on missing file/module.
        try:
            from capbot.domain.secrets import load_secrets  # type: ignore
            load_secrets(args.secrets)
            log_line(logfile, f"SECRETS loaded from {args.secrets}")
        except Exception as e:
            # If secrets are already in env (systemd EnvironmentFile), we can still run.
            log_line(logfile, f"WARN load_secrets skipped file={args.secrets} err={repr(e)}")

        run_bot(cfg, once=bool(args.once))


if __name__ == "__main__":
    main()
