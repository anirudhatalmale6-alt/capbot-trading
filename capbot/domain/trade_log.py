import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

HEADER: List[str] = [
    "entry_time","exit_time","direction","size",
    "entry_price_est","entry_price_api","exit_price_est","exit_price_api",
    "profit_api","profit_ccy",
    "r_points","initial_sl","sl_local","tp_local",
    "exit_reason","position_deal_id","close_dealReference",
    "meta_json"
]

def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def ensure_header(csv_path: Path) -> None:
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)
        return

    # If empty -> write header
    if csv_path.stat().st_size == 0:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)
        return

    # Validate existing header row
    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            first = next(r, None)
    except Exception:
        first = None

    if first != HEADER:
        # Preserve the bad file and create a fresh correct one
        bad = csv_path.with_name(csv_path.stem + f".bad.{_utc_stamp()}" + csv_path.suffix)
        try:
            csv_path.replace(bad)
        except Exception:
            # If replace fails, don't brick the process—just proceed to recreate
            pass
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)

def append_row(csv_path: Path, row: Dict[str, Any]) -> None:
    csv_path = Path(csv_path)
    ensure_header(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([row.get(h) for h in HEADER])
