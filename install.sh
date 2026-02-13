#!/bin/bash
# ============================================
# capbot VPS Quick Install
# ============================================
# Usage: bash install.sh
#
# Requirements:
#   - Ubuntu 20.04+ / Debian 11+
#   - Python 3.9+
#   - Internet access

set -e

echo "=== capbot installer ==="

# 1. System packages
echo "[1/5] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y python3 python3-pip python3-venv -qq

# 2. Create venv
echo "[2/5] Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
echo "[3/5] Installing Python dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q

# 4. Create secrets file template
SECRETS="$HOME/.capital_secrets.md"
if [ ! -f "$SECRETS" ]; then
    echo "[4/5] Creating secrets file template at $SECRETS"
    cat > "$SECRETS" << 'SECRETS_EOF'
# Capital.com API credentials
CAPITAL_API_KEY=your_api_key_here
CAPITAL_IDENTIFIER=your_email_or_identifier
CAPITAL_API_PASSWORD=your_password
CAPITAL_ENV=demo
# CAPITAL_ACCOUNT_ID=optional_account_id

# Email notifications (optional)
# EMAIL_TO=your@email.com
# SMTP_HOST=smtp.gmail.com
# SMTP_PORT=587
# SMTP_USER=your@email.com
# SMTP_PASS=your_app_password

# Telegram notifications (optional)
# TELEGRAM_BOT_TOKEN=your_bot_token
# TELEGRAM_CHAT_ID=your_chat_id
SECRETS_EOF
    echo "    -> EDIT THIS FILE with your real credentials!"
else
    echo "[4/5] Secrets file already exists at $SECRETS"
fi

# 5. Create data directory
echo "[5/5] Setting up data directory..."
BASEDIR="$HOME/capbot_data"
mkdir -p "$BASEDIR"/{state,trades,log,lock}
echo "export CAPBOT_BASEDIR=$BASEDIR" >> "$HOME/.bashrc"

echo ""
echo "=== Installation complete! ==="
echo ""
echo "Next steps:"
echo "  1. Edit your secrets:  nano $SECRETS"
echo "  2. Activate venv:      source venv/bin/activate"
echo "  3. Run single bot:     python run_bot.py run --config configs/de40_5m_vwap.json"
echo "  4. Run multiple bots:  python run_multi.py configs/de40_5m_vwap.json configs/sp500_example.json"
echo "  5. Check health:       python health_check.py"
echo ""
echo "For systemd service (auto-start on boot):"
echo "  sudo cp capbot@.service /etc/systemd/system/"
echo "  sudo systemctl enable capbot@de40_5m_vwap"
echo "  sudo systemctl start capbot@de40_5m_vwap"
