import os
import logging

# Respect systemd LOG_LEVEL (default INFO)
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(message)s"
)

import argparse
from pathlib import Path
from capbot.app.config import load_config
from capbot.app.engine import run_bot
from capbot.domain.secrets import load_secrets

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    runp = sub.add_parser("run")
    runp.add_argument("--config", required=True)
    runp.add_argument("--secrets", default=str(Path.home()/".capital_secrets.md"))
    runp.add_argument("--once", action="store_true")

    args = ap.parse_args()

    if args.cmd == "run":
        load_secrets(args.secrets)
        cfg = load_config(args.config).raw
        run_bot(cfg, once=bool(args.once))

if __name__ == "__main__":
    main()
