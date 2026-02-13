from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple


def _safe_bot_id(bot_id: str) -> str:
    safe = "".join(ch for ch in str(bot_id) if ch.isalnum() or ch in ("-", "_")).strip()
    return safe or "bot"


def _dir_from_env(var: str, fallback: Path) -> Path:
    v = (os.environ.get(var) or "").strip()
    if v:
        return Path(v).expanduser()
    return fallback


def bot_paths(bot_id: str) -> Tuple[Path, Path, Path, Path]:
    """
    Returns: (state_path, trades_csv_path, logfile_path, lock_path)

    Defaults:
      - all under ~ (Path.home())

    Optional env overrides:
      - CAPBOT_BASEDIR: base dir for everything (subfolders will be used)
      - CAPBOT_DIR_STATE, CAPBOT_DIR_TRADES, CAPBOT_DIR_LOG, CAPBOT_DIR_LOCK: override each directory directly
    """
    safe = _safe_bot_id(bot_id)

    home = Path.home()
    basedir = (os.environ.get("CAPBOT_BASEDIR") or "").strip()
    if basedir:
        base = Path(basedir).expanduser()
        default_state_dir = base / "state"
        default_trades_dir = base / "trades"
        default_log_dir = base / "log"
        default_lock_dir = base / "lock"
    else:
        # keep exact previous behavior
        default_state_dir = home
        default_trades_dir = home
        default_log_dir = home
        default_lock_dir = home

    state_dir = _dir_from_env("CAPBOT_DIR_STATE", default_state_dir)
    trades_dir = _dir_from_env("CAPBOT_DIR_TRADES", default_trades_dir)
    log_dir = _dir_from_env("CAPBOT_DIR_LOG", default_log_dir)
    lock_dir = _dir_from_env("CAPBOT_DIR_LOCK", default_lock_dir)

    # ensure dirs exist (best-effort)
    for d in (state_dir, trades_dir, log_dir, lock_dir):
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    state = state_dir / f".capbot_state_{safe}.json"
    trades = trades_dir / f"capbot_trades_{safe}.csv"
    log = log_dir / f"capbot_events_{safe}.log"
    lock = lock_dir / f".capbot_lock_{safe}.lock"
    return state, trades, log, lock
