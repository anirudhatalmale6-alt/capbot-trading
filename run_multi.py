#!/usr/bin/env python3
"""
Multi-bot launcher for capbot.

Usage:
  python run_multi.py configs/de40_5m_vwap.json configs/sp500_strategy.json

Each config runs as an independent subprocess with its own:
  - Capital.com credentials (via per-bot secrets file)
  - Strategy and parameters
  - State file, log file, trade CSV, lock file
  - PID (can be killed independently)

The launcher monitors all bots and restarts crashed ones automatically.
"""
import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def _launch_bot(config_path: str, secrets_path: str = None) -> subprocess.Popen:
    """Launch a single bot subprocess."""
    cmd = [sys.executable, "run_bot.py", "run", "--config", config_path]
    if secrets_path:
        cmd.extend(["--secrets", secrets_path])

    env = os.environ.copy()

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        bufsize=1,
        universal_newlines=True,
    )
    return proc


def _config_name(path: str) -> str:
    """Extract a short name from config path for display."""
    return Path(path).stem


def main():
    ap = argparse.ArgumentParser(description="Launch multiple capbot instances")
    ap.add_argument("configs", nargs="+", help="Config JSON files for each bot")
    ap.add_argument("--secrets", default=None, help="Shared secrets file (default: ~/.capital_secrets.md)")
    ap.add_argument("--restart-delay", type=int, default=10, help="Seconds to wait before restarting crashed bot")
    ap.add_argument("--no-restart", action="store_true", help="Don't restart crashed bots")
    args = ap.parse_args()

    procs = {}  # config_path -> (Popen, name)
    running = True

    def _signal_handler(sig, frame):
        nonlocal running
        running = False
        print(f"\n[MULTI] Received signal {sig}, shutting down all bots...")
        for cfg, (proc, name) in procs.items():
            try:
                proc.terminate()
                print(f"[MULTI] Terminated {name} (PID {proc.pid})")
            except Exception:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Launch all bots
    for cfg_path in args.configs:
        if not Path(cfg_path).exists():
            print(f"[MULTI] ERROR: config not found: {cfg_path}")
            continue

        name = _config_name(cfg_path)
        proc = _launch_bot(cfg_path, args.secrets)
        procs[cfg_path] = (proc, name)
        print(f"[MULTI] Started {name} (PID {proc.pid}) from {cfg_path}")

    if not procs:
        print("[MULTI] No bots started. Exiting.")
        return

    print(f"[MULTI] {len(procs)} bot(s) running. Press Ctrl+C to stop all.")

    # Monitor loop
    while running:
        time.sleep(5)

        for cfg_path in list(procs.keys()):
            proc, name = procs[cfg_path]
            ret = proc.poll()

            if ret is not None:
                # Bot exited
                # Drain remaining output
                try:
                    remaining = proc.stdout.read()
                    if remaining:
                        for line in remaining.strip().split("\n"):
                            print(f"[{name}] {line}")
                except Exception:
                    pass

                if ret == 0:
                    print(f"[MULTI] {name} exited normally (code 0)")
                else:
                    print(f"[MULTI] {name} CRASHED (code {ret})")

                if not args.no_restart and running:
                    print(f"[MULTI] Restarting {name} in {args.restart_delay}s...")
                    time.sleep(args.restart_delay)
                    new_proc = _launch_bot(cfg_path, args.secrets)
                    procs[cfg_path] = (new_proc, name)
                    print(f"[MULTI] Restarted {name} (new PID {new_proc.pid})")
                else:
                    del procs[cfg_path]

            else:
                # Bot still running - print any new output
                try:
                    import select
                    if hasattr(select, "select"):
                        readable, _, _ = select.select([proc.stdout], [], [], 0)
                        if readable:
                            line = proc.stdout.readline()
                            if line:
                                print(f"[{name}] {line.rstrip()}")
                except Exception:
                    pass

        if not procs:
            print("[MULTI] All bots have exited. Shutting down.")
            break


if __name__ == "__main__":
    main()
