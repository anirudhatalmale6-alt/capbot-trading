def profit_points(direction: str, entry_px: float, live_px: float) -> float:
    return (live_px - entry_px) if direction == "BUY" else (entry_px - live_px)


def maybe_trail_option_a(
    direction: str,
    entry: float,
    live: float,
    r_points: float,
    current_sl: float,
    trail_1r_done: bool = False,
    trail_2r_done: bool = False,
    buffer_r: float = 0.10,
):
    """
    Trailing stop Option A:
      +1R profit => SL moves to BE + buffer
      +2R profit => SL moves to +1R + buffer
    Never worsens SL. Returns (moved: bool, new_sl: float, flags: dict).
    """
    if r_points <= 0:
        return False, current_sl, {"trail_1r_done": trail_1r_done, "trail_2r_done": trail_2r_done}

    ppts = profit_points(direction, entry, live)
    if ppts <= 0:
        return False, current_sl, {"trail_1r_done": trail_1r_done, "trail_2r_done": trail_2r_done}

    buffer_pts = float(buffer_r) * r_points
    sl = current_sl
    changed = False

    if direction == "BUY":
        sl_be = entry + buffer_pts
        sl_1r = entry + r_points + buffer_pts
        better = lambda new, old: new > old
    else:
        sl_be = entry - buffer_pts
        sl_1r = entry - r_points - buffer_pts
        better = lambda new, old: new < old

    if (not trail_2r_done) and ppts >= 2.0 * r_points:
        if better(sl_1r, sl):
            sl = float(sl_1r)
            changed = True
        trail_2r_done = True
        trail_1r_done = True

    if (not trail_1r_done) and ppts >= 1.0 * r_points:
        if better(sl_be, sl):
            sl = float(sl_be)
            changed = True
        trail_1r_done = True

    return changed, sl, {"trail_1r_done": trail_1r_done, "trail_2r_done": trail_2r_done}
