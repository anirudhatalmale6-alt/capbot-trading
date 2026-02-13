import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass(frozen=True)
class BotConfig:
    raw: Dict[str, Any]


_ENV_RE = re.compile(r"^\$\{([A-Z0-9_]+)\}$")


def _expand_env_value(v: Any) -> Any:
    """
    Conservative env expansion:
      - only replaces strings that are EXACTLY '${VAR}'
      - leaves everything else unchanged
    """
    if isinstance(v, str):
        m = _ENV_RE.fullmatch(v.strip())
        if m:
            key = m.group(1)
            return os.environ.get(key, v)
    return v


def _expand_env_tree(x: Any) -> Any:
    if isinstance(x, dict):
        return {k: _expand_env_tree(_expand_env_value(v)) for k, v in x.items()}
    if isinstance(x, list):
        return [_expand_env_tree(_expand_env_value(v)) for v in x]
    return _expand_env_value(x)


def load_config(path: str) -> BotConfig:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")

    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config {p}: {e}") from e

    if not isinstance(raw, dict):
        raise ValueError(f"Config {p} must be a JSON object at top-level")

    raw = _expand_env_tree(raw)
    return BotConfig(raw=raw)
