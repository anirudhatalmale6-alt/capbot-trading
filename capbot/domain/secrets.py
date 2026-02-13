import os
from pathlib import Path
from typing import Optional


def _strip_quotes(v: str) -> str:
    v = v.strip()
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        return v[1:-1]
    return v


def _strip_inline_comment(s: str) -> str:
    """
    Remove inline comments starting with #, but only when # is not inside quotes.
    """
    out = []
    q = None  # quote char if inside quote
    for ch in s:
        if ch in ("'", '"'):
            if q is None:
                q = ch
            elif q == ch:
                q = None
            out.append(ch)
            continue
        if ch == "#" and q is None:
            break
        out.append(ch)
    return "".join(out).strip()


def load_secrets(path: Optional[str], *, override: bool = True) -> None:
    """
    Load KEY=VALUE lines into os.environ.

    Supports:
      - KEY=VALUE
      - export KEY=VALUE
    Ignores:
      - empty lines
      - lines starting with '#'
      - malformed lines without '='

    If override=False, existing env vars are not overwritten.
    """
    if not path:
        return

    p = Path(path)
    if not p.exists():
        return

    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        # allow: export KEY=VALUE
        if line.startswith("export "):
            line = line[len("export "):].strip()

        if "=" not in line:
            continue

        k, v = line.split("=", 1)
        k = k.strip()
        if not k:
            continue

        v = _strip_inline_comment(v)
        v = _strip_quotes(v)

        if (not override) and (k in os.environ):
            continue

        os.environ[k] = v
