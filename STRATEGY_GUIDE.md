# How to Add a New Strategy

## Overview

Each strategy is a Python class with 3 methods. The bot loads it dynamically from the config file.

## Step 1: Create Your Strategy File

Create a new file in `capbot/strategies/`, e.g. `capbot/strategies/my_strategy.py`:

```python
from typing import Any, Dict, Optional
import pandas as pd
from capbot.strategies.vwap_pullback_rsi import Signal

class MyStrategy:

    def enrich(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Add indicator columns to the DataFrame.

        INPUT:  df with columns: open, high, low, close, volume (DatetimeIndex UTC)
        OUTPUT: df with your custom indicator columns added

        This is called ONCE per cycle. Add all indicators you need here.
        """
        d = df.copy()

        # Example: Simple moving averages
        d["sma_fast"] = d["close"].rolling(int(params.get("SMA_FAST", 10))).mean()
        d["sma_slow"] = d["close"].rolling(int(params.get("SMA_SLOW", 50))).mean()

        # ATR (required for risk calculation)
        from capbot.strategies.vwap_pullback_rsi import atr_wilder
        d["atr14"] = atr_wilder(d, int(params.get("ATR_PERIOD", 14)))

        return d

    def signal_on_bar_close(self, df: pd.DataFrame, params: Dict[str, Any]) -> Optional[Signal]:
        """
        Check if there's a BUY or SELL signal on the LAST CLOSED bar.

        IMPORTANT: Use df.iloc[-2] (the closed bar), NOT df.iloc[-1] (still forming).

        Return Signal(direction="BUY"/"SELL", entry_price_est=..., meta={...})
        or None if no signal.
        """
        if df is None or len(df) < 50:
            return None

        bar = df.iloc[-2]  # CLOSED bar

        # Example: SMA crossover
        fast = float(bar["sma_fast"])
        slow = float(bar["sma_slow"])
        close = float(bar["close"])

        if pd.isna(fast) or pd.isna(slow):
            return None

        if fast > slow and close > fast:
            return Signal(direction="BUY", entry_price_est=close, meta={"fast": fast, "slow": slow})

        if fast < slow and close < fast:
            return Signal(direction="SELL", entry_price_est=close, meta={"fast": fast, "slow": slow})

        return None

    def initial_risk(self, entry_price: float, atr_v: float, sig: Signal, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate SL and TP levels.

        MUST return dict with: r_points, sl_local, tp_local, tp_r_multiple

        Standard formula:
          R = SL_ATR * ATR
          BUY:  SL = entry - R, TP = entry + (TP_R * R)
          SELL: SL = entry + R, TP = entry - (TP_R * R)
        """
        SL_ATR = float(params.get("SL_ATR", 1.0))
        TP_R = float(params.get("TP_R_MULTIPLE", 3.0))

        r_points = SL_ATR * float(atr_v)

        if sig.direction == "BUY":
            sl = entry_price - r_points
            tp = entry_price + (TP_R * r_points)
        else:
            sl = entry_price + r_points
            tp = entry_price - (TP_R * r_points)

        return {
            "r_points": r_points,
            "sl_local": sl,
            "tp_local": tp,
            "tp_r_multiple": TP_R,
        }
```

## Step 2: Create a Config File

Copy `configs/template.json` and customize:

```json
{
  "bot_id": "my_strategy_bot",
  "market": {
    "epic": "US500",
    "resolution": "MINUTE_5",
    "warmup_bars": 600
  },
  "strategy": {
    "module": "capbot.strategies.my_strategy:MyStrategy",
    "params": {
      "SMA_FAST": 10,
      "SMA_SLOW": 50,
      "ATR_PERIOD": 14,
      "SL_ATR": 1.0,
      "TP_R_MULTIPLE": 3.0
    }
  },
  ...
}
```

Key points:
- `module` format: `python.module.path:ClassName`
- `params` are passed to all 3 strategy methods
- `bot_id` must be unique per running instance

## Step 3: Set Up Credentials

Each bot can use the same or different Capital.com accounts.

Same account for all bots (default):
```
# ~/.capital_secrets.md
CAPITAL_API_KEY=your_key
CAPITAL_IDENTIFIER=your_email
CAPITAL_API_PASSWORD=your_pass
```

Different accounts per bot: Use `account.account_id` in config, or create separate secrets files.

## Step 4: Run

Single bot:
```bash
python run_bot.py run --config configs/my_strategy.json
```

Multiple bots simultaneously:
```bash
python run_multi.py configs/de40_5m_vwap.json configs/my_strategy.json
```

## Available Indicators (in capbot.strategies.vwap_pullback_rsi)

- `rsi_wilder(close_series, length)` - Wilder RSI
- `atr_wilder(df, length)` - Wilder ATR (requires high/low/close)
- `vwap_intraday_reset_berlin(df, tz_name)` - Intraday VWAP with daily reset

## Tips

- Always use `df.iloc[-2]` for the closed bar, never `df.iloc[-1]`
- Your `enrich()` MUST add an `atr14` column (used for position sizing)
- Test with `--once` flag: `python run_bot.py run --config configs/test.json --once`
- Set `CAPITAL_ENV=demo` to test on paper account first
- Use `DEBUG_CHECKS: true` in params to see VIS output
