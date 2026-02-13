from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        txt = path.read_text(encoding="utf-8")
        obj = json.loads(txt)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def load_state(path: Path) -> Dict[str, Any]:
    """
    Load state dict. If main file is corrupt, try .bak.
    Never raises.
    """
    path = Path(path)
    if not path.exists():
        return {}

    st = _read_json(path)
    if st:
        return st

    # fallback to backup if exists
    bak = path.with_suffix(path.suffix + ".bak")
    if bak.exists():
        st2 = _read_json(bak)
        if st2:
            return st2

    return {}


def save_state_atomic(path: Path, st: Dict[str, Any]) -> None:
    """
    Atomic write with best-effort durability:
      - ensure parent dir exists
      - write to tmp, flush
      - rotate previous to .bak (best-effort)
      - os.replace(tmp, path)
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")
    bak = path.with_suffix(path.suffix + ".bak")

    payload = json.dumps(st or {}, indent=2, sort_keys=True)

    # write tmp
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(payload)
        f.flush()

    # rotate old -> .bak (best-effort)
    try:
        if path.exists():
            os.replace(path, bak)
    except Exception:
        pass

    # atomic replace tmp -> path
    os.replace(tmp, path)
