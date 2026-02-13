# capbot_full_pack

Bot modular para Capital.com con:
- Engine robusto + locks
- Estado JSON + CSV + logs
- Gestión local SL/TP
- Trailing Option A (+1R => BE+buffer, +2R => +1R+buffer)
- TP por defecto a 3R (configurable)
- Emails via SMTP (opcional, pero incluido)

## Secrets Capital
Crea: `~/.capital_secrets.md`

```
CAPITAL_ENV=demo
CAPITAL_API_KEY=...
CAPITAL_IDENTIFIER=...
CAPITAL_API_PASSWORD=...
CAPITAL_ACCOUNT_ID=...   # recomendado si tienes varias cuentas
```

## Email SMTP
Variables de entorno:
```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=tu_usuario@gmail.com
SMTP_PASS=tu_app_password
EMAIL_FROM=tu_usuario@gmail.com
EMAIL_TO=tu_correo@dominio.com
```

Si no las defines, el bot corre igual pero no envía emails.

## Ejecutar
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m capbot.app.main run --config configs/germany40_5m_vwap.json
```

Run único (test):
```
python -m capbot.app.main run --config configs/germany40_5m_vwap.json --once
```
