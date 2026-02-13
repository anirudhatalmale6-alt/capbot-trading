# VPS Setup Guide

## Requirements

- Ubuntu 22.04+ (or Debian 11+)
- Python 3.9+
- 1 GB RAM minimum (2 GB recommended for multiple bots)
- 10 GB disk space

## Quick Install

```bash
# 1. Upload the bot files to your VPS
scp capbot.zip user@your-vps-ip:~/

# 2. SSH into VPS
ssh user@your-vps-ip

# 3. Extract and install
unzip capbot.zip -d ~/capbot
cd ~/capbot
bash install.sh

# 4. Edit credentials
nano ~/.capital_secrets.md
```

## Manual Install

```bash
# System packages
sudo apt update && sudo apt install -y python3 python3-pip python3-venv

# Create venv
cd ~/capbot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create data directory
mkdir -p ~/capbot_data/{state,trades,log,lock}
echo 'export CAPBOT_BASEDIR=~/capbot_data' >> ~/.bashrc
source ~/.bashrc
```

## Credentials Setup

```bash
nano ~/.capital_secrets.md
```

Required:
```
CAPITAL_API_KEY=your_api_key
CAPITAL_IDENTIFIER=your_email
CAPITAL_API_PASSWORD=your_password
CAPITAL_ENV=demo
```

Optional (for live trading):
```
CAPITAL_ENV=live
CAPITAL_ACCOUNT_ID=your_account_id
```

Optional (email notifications):
```
EMAIL_TO=your@email.com
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your@gmail.com
SMTP_PASS=your_app_password
```

Optional (Telegram notifications):
```
TELEGRAM_BOT_TOKEN=123456:ABC-DEF
TELEGRAM_CHAT_ID=your_chat_id
```

## Running Bots

### Test run (single iteration)
```bash
source venv/bin/activate
python run_bot.py run --config configs/de40_5m_vwap.json --once
```

### Run single bot (foreground)
```bash
python run_bot.py run --config configs/de40_5m_vwap.json
```

### Run multiple bots
```bash
python run_multi.py configs/de40_5m_vwap.json configs/sp500_example.json
```

### Run as systemd service (auto-start on boot)

```bash
# Copy service template
sudo cp capbot@.service /etc/systemd/system/

# Enable and start a bot
sudo systemctl enable capbot@de40_5m_vwap
sudo systemctl start capbot@de40_5m_vwap

# View logs
sudo journalctl -u capbot@de40_5m_vwap -f

# Stop
sudo systemctl stop capbot@de40_5m_vwap
```

## Monitoring

### Health check
```bash
python health_check.py
```

### Health check with Telegram alert
```bash
python health_check.py --telegram
```

### View bot logs
```bash
tail -f ~/capbot_data/log/capbot_events_de40_5m_vwap.log
```

### View trades
```bash
cat ~/capbot_data/trades/capbot_trades_de40_5m_vwap.csv
```

## File Structure

```
~/capbot/
├── run_bot.py          # Single bot launcher
├── run_multi.py        # Multi-bot launcher
├── health_check.py     # Health checker
├── install.sh          # Quick installer
├── capbot@.service     # systemd template
├── configs/
│   ├── de40_5m_vwap.json       # DE40 strategy config
│   └── sp500_example.json      # SP500 example config
├── capbot/
│   ├── app/
│   │   ├── engine.py           # Main engine loop
│   │   ├── notifier.py         # Email notifications
│   │   └── telegram_notifier.py # Telegram notifications
│   ├── broker/
│   │   └── capital_client.py   # Capital.com API client
│   ├── strategies/
│   │   ├── de40_vwap_k020.py   # DE40 VWAP strategy
│   │   └── vwap_pullback_rsi.py # Base VWAP strategy
│   └── domain/                  # Core logic (state, risk, etc.)
└── venv/                        # Python virtual environment

~/capbot_data/
├── state/    # Bot state files (JSON)
├── trades/   # Trade CSV logs
├── log/      # Event logs
└── lock/     # PID lock files
```

## Troubleshooting

**Bot won't start - "Lock activo"**
```bash
# Check if another instance is running
ps aux | grep run_bot
# If not, remove stale lock
rm ~/capbot_data/lock/.capbot_lock_*.lock
```

**No trades happening**
- Check the CHECK line in logs - it shows all conditions with ✅/❌
- Make sure CAPITAL_ENV matches your account (demo vs live)
- Verify RTH hours match your market

**Email notifications not working**
- Gmail requires an "App Password" (not your regular password)
- Check SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS

**Telegram notifications not working**
- Create bot via @BotFather, copy the token
- Send /start to your bot first
- Get chat_id from: https://api.telegram.org/bot<TOKEN>/getUpdates
