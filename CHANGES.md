# Change Report - capbot Refactor v2

## Critical Bug Fixes

### 1. Trailing Stop Crash (trailing.py)
**Problem**: `maybe_trail_option_a()` was defined with signature `(pos: dict, live_px, buffer_r)` but engine.py called it with keyword args `(direction=, entry=, live=, r_points=, current_sl=, trail_1r_done=, trail_2r_done=, buffer_r=)`.
**Impact**: TypeError crash at runtime whenever trailing stop should activate.
**Fix**: Rewrote function to accept keyword args and return `(moved, new_sl, flags)` tuple matching engine expectations.

### 2. Order Confirmation Race Condition (engine.py)
**Problem**: After opening a position, if `confirm()` API returned no `dealId` after 3 retries, the position was LEFT OPEN ON BROKER but UNTRACKED in local state.
**Impact**: Ghost positions - money at risk with no management (no SL/TP checking, no trailing).
**Fix**: Triple-fallback confirmation:
  1. Poll broker positions (fast, ~3s)
  2. Confirm via deal reference (standard API)
  3. Last-resort broker position check
  Position is now ALWAYS tracked if it exists on broker.

### 3. InstanceLock Constructor Mismatch (lock.py)
**Problem**: Engine called `InstanceLock(path, 1800)` but constructor only accepted 1 argument.
**Impact**: TypeError crash on startup (caught by try/except, fell back to no timeout).
**Fix**: Added `stale_timeout_sec` parameter to constructor.

### 4. Duplicate Logger Function (logger.py)
**Problem**: Two `log_line()` definitions in same file. Second one (Berlin timezone) overwrote first. Code relied on Python's last-definition-wins behavior.
**Fix**: Kept single Berlin-timezone version, added UTC fallback if pytz unavailable.

## Performance Improvements

### 5. Indicator Caching (engine.py)
**Before**: `strat.enrich()` called 4+ times per cycle (once for each VIS check block + once for signal).
Each call computes RSI, ATR, VWAP on 600 bars.
**After**: Called ONCE per cycle. Reused for all VIS output and signal detection.
**Impact**: ~75% reduction in CPU usage per cycle.

### 6. Bar-Close Aligned Polling
**Before**: Fixed 30-second polling regardless of where in the bar cycle.
**After**: Calculates time until next bar close and sleeps until then (+2s buffer).
**Impact**: Signals detected within 2-3 seconds of bar close instead of up to 30 seconds.
**Config**: `"align_poll_to_bar": true` (default on, set false for old behavior).

### 7. Consolidated VIS Output
**Before**: ~300 lines of duplicated CHECK/VIS code across Thursday/RTH/NTH gate blocks.
**After**: Single `_compute_vis_checks()` function, called once per cycle.
**Impact**: Cleaner logs, same ✅/❌ output format everywhere.

## New Features

### 8. Telegram Notifications
- New module: `capbot/app/telegram_notifier.py`
- Events: STARTUP, TRADE_OPEN, EXIT_TP, EXIT_SL, EXIT_RTH, TRAIL_1R, TRAIL_2R, HEALTH
- Setup: Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in secrets file
- Formatted messages with emojis and P&L

### 9. Multi-Bot Launcher
- New file: `run_multi.py`
- Runs multiple configs as independent subprocesses
- Auto-restarts crashed bots (configurable delay)
- Ctrl+C gracefully stops all
- Usage: `python run_multi.py configs/de40.json configs/sp500.json`

### 10. Health Monitor
- New file: `health_check.py`
- Checks: lock file, PID alive, state/log freshness, position status, circuit breaker
- Auto-discovers all running bots
- JSON output mode for monitoring tools
- Telegram reporting: `python health_check.py --telegram`

### 11. VPS Quick Install
- New file: `install.sh` - one-command setup
- New file: `capbot@.service` - systemd template for auto-start
- Creates venv, installs deps, sets up directories, creates secrets template

### 12. Strategy Guide
- New file: `STRATEGY_GUIDE.md`
- Step-by-step instructions to create new strategies
- Code template with all 3 required methods
- Available indicators reference

### 13. Example SP500 Config
- New file: `configs/sp500_example.json`
- Pre-configured for US500 with NY timezone
- Uses existing VWAPPullbackRSI strategy

## Structural Improvements

### 14. Engine Rewrite (engine.py: 1369 lines -> ~1050 lines)
- Extracted common exit logic into `_handle_position_exit()`
- Extracted VIS checks into `_compute_vis_checks()`
- Fixed indentation issues (mixed tabs/spaces)
- Proper code organization: helpers, then main loop
- All gates/checks use consistent structure

### 15. Position State Completeness
After opening a position, state now includes ALL fields:
- `deal_id`, `direction`, `size`, `entry_price_est`
- `r_points`, `sl_local`, `tp_local`, `tp_r_multiple`
- `atr_signal`, `atr_entry_const`
- `trail_1r_done`, `trail_2r_done`
- `entry_bar_time_utc`, `ts_signal_utc`
- `broker_snap_open`

### 16. Error Logging
Silent `except: pass` blocks replaced with proper error logging where critical.

## Files Changed
- `capbot/app/engine.py` - Complete rewrite
- `capbot/domain/trailing.py` - Signature fix + return value change
- `capbot/domain/lock.py` - Constructor parameter added
- `capbot/domain/logger.py` - Deduplicated
- `configs/de40_5m_vwap.json` - Added align_poll_to_bar

## Files Added
- `capbot/app/telegram_notifier.py` - Telegram notifications
- `run_multi.py` - Multi-bot launcher
- `health_check.py` - Health monitor
- `install.sh` - VPS installer
- `capbot@.service` - systemd template
- `configs/sp500_example.json` - SP500 example config
- `STRATEGY_GUIDE.md` - How to add strategies
- `VPS_SETUP.md` - VPS deployment guide
- `CHANGES.md` - This file
