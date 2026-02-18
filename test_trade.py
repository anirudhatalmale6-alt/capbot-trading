#!/usr/bin/env python3
"""
Force a test transaction: BUY, wait, then SELL (close).

Usage:
  python test_trade.py --config configs/de40_5m_vwap.json
  python test_trade.py --config configs/meta_1h.json --wait 300
  python test_trade.py --config configs/sp500_5m.json --size 1 --wait 60

Options:
  --config   Config file (required - uses epic + account from it)
  --wait     Seconds to hold position (default: 300 = 5 min)
  --size     Trade size (default: 1)
  --direction  BUY or SELL (default: BUY)
"""
import argparse
import json
import os
import sys
import time

from capbot.broker.capital_client import CapitalClient

try:
    from capbot.app.notifier import email_event
except ImportError:
    def email_event(*args, **kwargs): pass

try:
    from capbot.app.telegram_notifier import telegram_event
except ImportError:
    def telegram_event(*args, **kwargs): pass


def main():
    parser = argparse.ArgumentParser(description="Force a test trade: open, wait, close")
    parser.add_argument("--config", required=True, help="Config JSON file")
    parser.add_argument("--wait", type=int, default=300, help="Seconds to hold (default 300 = 5 min)")
    parser.add_argument("--size", type=float, default=1.0, help="Trade size (default 1)")
    parser.add_argument("--direction", default="BUY", choices=["BUY", "SELL"], help="Direction (default BUY)")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    epic = cfg["market"]["epic"]
    account_id = cfg.get("account", {}).get("account_id")
    bot_id = cfg.get("bot_id", "test")

    print(f"Bot: {bot_id} | Epic: {epic} | Account: {account_id}")
    print(f"Direction: {args.direction} | Size: {args.size} | Wait: {args.wait}s")
    print()

    # Login
    print("Logging in to Capital.com...")
    client = CapitalClient()
    client.login()
    client.ensure_account(account_id)
    print("Login OK")

    # Get account currency
    _CURRENCY_SYMBOLS = {
        "USD": "$", "USDD": "$",
        "EUR": "€", "EURD": "€",
        "GBP": "£", "GBPD": "£",
        "CHF": "CHF ", "CHFD": "CHF ",
        "JPY": "¥", "JPYD": "¥",
        "AUD": "A$", "AUDD": "A$",
        "CAD": "C$", "CADD": "C$",
    }
    account_currency = "USD"
    currency_symbol = "$"
    try:
        sess_info = client.get_session()
        raw_ccy = (sess_info.get("currency") or "USD").upper()
        account_currency = raw_ccy.rstrip("D") if len(raw_ccy) == 4 and raw_ccy.endswith("D") else raw_ccy
        currency_symbol = _CURRENCY_SYMBOLS.get(raw_ccy, _CURRENCY_SYMBOLS.get(account_currency, account_currency + " "))
        print(f"Account currency: {raw_ccy} -> {account_currency} ({currency_symbol.strip()})")
    except Exception:
        print("Could not detect account currency, defaulting to USD")

    # Check no existing position
    positions = client.get_positions()
    pos_list = (positions or {}).get("positions", [])
    for p in pos_list:
        mkt = p.get("market", {})
        if mkt.get("epic") == epic:
            deal_id = p.get("position", {}).get("dealId")
            print(f"WARNING: Already have open position on {epic} (deal_id={deal_id})")
            print("Close it first or use a different epic.")
            sys.exit(1)

    # Open position
    print(f"\nOpening {args.direction} {args.size} on {epic}...")
    resp = client.open_market(epic, args.direction, args.size)
    deal_ref = resp.get("dealReference")
    print(f"Order response: dealReference={deal_ref}")

    # Wait for confirm
    for attempt in range(10):
        time.sleep(1)
        conf = client.confirm(deal_ref, timeout_sec=10)
        if conf and conf.get("dealId"):
            status = conf.get("dealStatus", "?")
            level = conf.get("level", "?")
            print(f"Confirmed: status={status} entry={level}")
            break
    else:
        print("WARNING: Could not get confirm response, checking positions...")

    # Get the REAL deal_id from positions (confirm dealId differs from position dealId)
    deal_id = None
    for attempt in range(5):
        time.sleep(1)
        positions = client.get_positions()
        for p in (positions or {}).get("positions", []):
            mkt = p.get("market", {})
            if mkt.get("epic") == epic:
                deal_id = p.get("position", {}).get("dealId")
                level = p.get("position", {}).get("level")
                print(f"Position found: deal_id={deal_id} entry={level}")
                break
        if deal_id:
            break

    if not deal_id:
        print("ERROR: Could not find position. Check Capital.com manually.")
        sys.exit(1)

    # Notify: test trade opened
    entry_price = float(level) if level else 0
    notify_payload = {
        "epic": epic, "direction": args.direction, "size": args.size,
        "deal_id": deal_id, "entry_price": entry_price, "account_id": account_id,
        "sl": "N/A (test)", "tp": "N/A (test)",
        "currency": account_currency, "currency_symbol": currency_symbol,
    }
    email_event(True, bot_id, "TRADE_OPEN", notify_payload)
    telegram_event(bot_id, "TRADE_OPEN", notify_payload)
    print("Notifications sent (TRADE_OPEN)")

    # Wait
    print(f"\nPosition open. Waiting {args.wait} seconds...")
    for remaining in range(args.wait, 0, -10):
        print(f"  {remaining}s remaining...")
        time.sleep(min(10, remaining))

    # Close position using the real deal_id from positions
    print(f"\nClosing position {deal_id}...")
    close_resp = client.close_position(deal_id)
    print(f"Close response: {close_resp}")

    # Verify closed
    time.sleep(2)
    positions = client.get_positions()
    still_open = False
    for p in (positions or {}).get("positions", []):
        mkt = p.get("market", {})
        if mkt.get("epic") == epic:
            still_open = True

    if still_open:
        print("WARNING: Position may still be open. Check Capital.com.")
    else:
        # Get actual close details from confirm
        close_deal_ref = (close_resp or {}).get("dealReference")
        exit_price = entry_price
        broker_profit = None
        if close_deal_ref:
            time.sleep(1)
            close_conf = client.confirm(str(close_deal_ref), timeout_sec=10)
            if close_conf:
                print(f"Broker confirm response: {close_conf}")
                if close_conf.get("level"):
                    exit_price = float(close_conf["level"])
                if close_conf.get("profit") is not None:
                    broker_profit = float(close_conf["profit"])
                    print(f"Broker confirm profit: {broker_profit}")

        # Fallback: fetch from transaction history if confirm didn't have profit
        if broker_profit is None:
            try:
                time.sleep(2)
                history = client.get_history_transactions(max_items=5)
                transactions = history.get("transactions") or []
                for tx in transactions:
                    ref = tx.get("reference") or ""
                    tx_type = (tx.get("type") or tx.get("transactionType") or "").upper()
                    if str(deal_id) in str(ref) or "TRADE" in tx_type:
                        # Check for profit/cashTransaction/profitAndLoss fields
                        for field in ("profitAndLoss", "profit", "cashTransaction", "amount"):
                            val = tx.get(field)
                            if val is not None:
                                try:
                                    broker_profit = float(str(val).replace(",", ""))
                                    print(f"Transaction history profit ({field}): {broker_profit}")
                                    break
                                except (ValueError, TypeError):
                                    pass
                        if broker_profit is not None:
                            break
            except Exception as e:
                print(f"Transaction history lookup warning: {e}")

        if args.direction == "BUY":
            profit_pts = round(exit_price - entry_price, 2)
        else:
            profit_pts = round(entry_price - exit_price, 2)

        # Use broker's actual profit (includes spread + currency conversion) if available
        if broker_profit is not None:
            profit_cash = round(broker_profit, 2)
        else:
            profit_cash = round(profit_pts * args.size, 2)
            print("WARNING: Could not get broker profit, using local calculation")

        print(f"Position closed! Entry={entry_price} Exit={exit_price} PnL={profit_pts}pts {currency_symbol}{profit_cash}")
        close_payload = {
            "deal_id": deal_id, "direction": args.direction,
            "epic": epic, "exit_price": exit_price,
            "entry_price": entry_price, "size": args.size,
            "profit_points": profit_pts, "profit_cash": profit_cash,
            "currency": account_currency, "currency_symbol": currency_symbol,
        }
        email_event(True, bot_id, "EXIT_TP", close_payload)
        telegram_event(bot_id, "EXIT_TP", close_payload)
        print("Notifications sent (EXIT)")

    print("\nTest trade complete.")


if __name__ == "__main__":
    main()
