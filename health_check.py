#!/usr/bin/env python3
"""
Health check for all running capbot instances.

Usage:
  python health_check.py
  python health_check.py --json
  python health_check.py --telegram  # send health report via Telegram

Checks:
  - Lock file exists (bot is running)
  - PID in lock file is alive
  - State file freshness (last update < 10 minutes)
  - Log file freshness (last log < 10 minutes)
  - Current position status
  - Consecutive losses / circuit breaker
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError, OSError):
        return False


def _check_bot(bot_id: str, base_dir: Path = None) -> dict:
    """Check health of a single bot. Returns status dict."""
    if base_dir:
        state_dir = base_dir / "state"
        log_dir = base_dir / "log"
        lock_dir = base_dir / "lock"
    else:
        home = Path.home()
        basedir = os.environ.get("CAPBOT_BASEDIR", "").strip()
        if basedir:
            b = Path(basedir)
            state_dir = b / "state"
            log_dir = b / "log"
            lock_dir = b / "lock"
        else:
            state_dir = log_dir = lock_dir = home

    safe = "".join(ch for ch in bot_id if ch.isalnum() or ch in "-_").strip() or "bot"

    state_path = state_dir / f".capbot_state_{safe}.json"
    log_path = log_dir / f"capbot_events_{safe}.log"
    lock_path = lock_dir / f".capbot_lock_{safe}.lock"

    status = {
        "bot_id": bot_id,
        "running": False,
        "pid": None,
        "state_age_sec": None,
        "log_age_sec": None,
        "in_position": False,
        "deal_id": None,
        "direction": None,
        "entry_price": None,
        "consec_losses": 0,
        "cooldown_active": False,
        "healthy": False,
        "issues": [],
    }

    # Check lock file
    if lock_path.exists():
        try:
            pid = int(lock_path.read_text().strip())
            status["pid"] = pid
            status["running"] = _pid_is_alive(pid)
            if not status["running"]:
                status["issues"].append(f"PID {pid} not alive (stale lock)")
        except Exception:
            status["issues"].append("Cannot read lock file PID")
    else:
        status["issues"].append("No lock file (bot not running)")

    # Check state file
    now = time.time()
    if state_path.exists():
        try:
            age = now - state_path.stat().st_mtime
            status["state_age_sec"] = int(age)
            if age > 600:
                status["issues"].append(f"State file stale ({int(age)}s old)")

            st = json.loads(state_path.read_text())
            pos = st.get("pos") or {}
            if pos.get("deal_id"):
                status["in_position"] = True
                status["deal_id"] = pos.get("deal_id")
                status["direction"] = pos.get("direction")
                status["entry_price"] = pos.get("entry_price_est")

            status["consec_losses"] = int(st.get("consec_losses", 0))

            cd = st.get("cooldown_until_iso")
            if cd:
                try:
                    cd_ts = datetime.fromisoformat(cd.replace("Z", "+00:00"))
                    if cd_ts > datetime.now(timezone.utc):
                        status["cooldown_active"] = True
                        status["issues"].append(f"Circuit breaker active until {cd}")
                except Exception:
                    pass
        except Exception as e:
            status["issues"].append(f"Cannot read state: {e}")
    else:
        status["issues"].append("No state file")

    # Check log file
    if log_path.exists():
        try:
            age = now - log_path.stat().st_mtime
            status["log_age_sec"] = int(age)
            if age > 600:
                status["issues"].append(f"Log file stale ({int(age)}s old)")
        except Exception:
            pass

    status["healthy"] = status["running"] and len(status["issues"]) == 0

    return status


def _discover_bots() -> list:
    """Find all bot IDs from lock/state files."""
    bot_ids = set()
    basedir = os.environ.get("CAPBOT_BASEDIR", "").strip()

    if basedir:
        dirs = [Path(basedir) / "lock", Path(basedir) / "state"]
    else:
        dirs = [Path.home()]

    for d in dirs:
        if not d.exists():
            continue
        for f in d.iterdir():
            name = f.name
            if name.startswith(".capbot_lock_") and name.endswith(".lock"):
                bot_ids.add(name[len(".capbot_lock_"):-len(".lock")])
            elif name.startswith(".capbot_state_") and name.endswith(".json"):
                bot_ids.add(name[len(".capbot_state_"):-len(".json")])

    return sorted(bot_ids)


def main():
    ap = argparse.ArgumentParser(description="Check health of capbot instances")
    ap.add_argument("--bot-id", nargs="*", help="Specific bot IDs to check (default: auto-discover)")
    ap.add_argument("--json", action="store_true", help="Output as JSON")
    ap.add_argument("--telegram", action="store_true", help="Send report via Telegram")
    args = ap.parse_args()

    bot_ids = args.bot_id or _discover_bots()

    if not bot_ids:
        print("No bots found.")
        return

    results = []
    for bid in bot_ids:
        results.append(_check_bot(bid))

    if args.json:
        print(json.dumps(results, indent=2, default=str))
        return

    # Pretty print
    print(f"\n{'='*60}")
    print(f"  CAPBOT HEALTH CHECK  ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})")
    print(f"{'='*60}\n")

    for r in results:
        icon = "\u2705" if r["healthy"] else "\u274c"
        pid_str = f"PID {r['pid']}" if r["pid"] else "not running"
        print(f"  {icon} {r['bot_id']} ({pid_str})")

        if r["in_position"]:
            print(f"     Position: {r['direction']} @ {r['entry_price']} (deal {r['deal_id']})")
        else:
            print(f"     Position: flat")

        if r["state_age_sec"] is not None:
            print(f"     State: {r['state_age_sec']}s ago | Log: {r.get('log_age_sec', '?')}s ago")

        if r["consec_losses"] > 0:
            print(f"     Consecutive losses: {r['consec_losses']}")

        if r["issues"]:
            for issue in r["issues"]:
                print(f"     \u26a0\ufe0f  {issue}")

        print()

    healthy = sum(1 for r in results if r["healthy"])
    total = len(results)
    print(f"  Summary: {healthy}/{total} healthy\n")

    if args.telegram:
        try:
            from capbot.app.telegram_notifier import telegram_event
            summary = {
                "total": total,
                "healthy": healthy,
                "bots": {r["bot_id"]: ("OK" if r["healthy"] else "; ".join(r["issues"])) for r in results},
            }
            telegram_event("health_check", "HEALTH", summary)
            print("  Telegram report sent.")
        except Exception as e:
            print(f"  Telegram failed: {e}")


if __name__ == "__main__":
    main()
