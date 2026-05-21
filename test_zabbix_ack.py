#!/usr/bin/env python3
"""
test_zabbix_ack.py — Testa a chamada event.acknowledge na API do Zabbix.

Uso:
    python3 test_zabbix_ack.py <event_id>

Exemplo:
    python3 test_zabbix_ack.py 1234

O event_id pode ser obtido em: Monitoring > Problems > coluna EventID
(ou ativando a coluna na interface do Zabbix).
"""

import json
import sys
from urllib import error, request
from dotenv import load_dotenv
import os

load_dotenv()

ZABBIX_URL       = os.getenv("ZABBIX_URL", "").rstrip("/")
ZABBIX_API_TOKEN = os.getenv("ZABBIX_API_TOKEN", "")

if not ZABBIX_URL or not ZABBIX_API_TOKEN:
    print("[ERRO] ZABBIX_URL ou ZABBIX_API_TOKEN nao configurados no .env")
    sys.exit(1)

if len(sys.argv) < 2:
    print(f"Uso: python3 {sys.argv[0]} <event_id>")
    sys.exit(1)

event_id     = sys.argv[1]
ticket_label = f"TESTE-TICKET-{event_id}"

print(f"Zabbix URL:   {ZABBIX_URL}")
print(f"Event ID:     {event_id}")
print(f"Mensagem:     {ticket_label}")
print("-" * 50)

# ---- 1. Verificar versao da API ----
ver_body = {"jsonrpc": "2.0", "method": "apiinfo.version", "params": [], "id": 1}
req = request.Request(
    f"{ZABBIX_URL}/api_jsonrpc.php",
    data=json.dumps(ver_body).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with request.urlopen(req, timeout=5) as resp:
        ver_data = json.loads(resp.read().decode("utf-8"))
        print(f"Versao da API Zabbix: {ver_data.get('result', '?')}")
except Exception as exc:
    print(f"[ERRO] Nao foi possivel conectar ao Zabbix: {exc}")
    sys.exit(1)

# ---- 2. Testar event.acknowledge ----
ack_body = {
    "jsonrpc": "2.0",
    "method":  "event.acknowledge",
    "params":  {
        "eventids": [event_id],
        "action":   4,
        "message":  ticket_label,
    },
    "auth": ZABBIX_API_TOKEN,
    "id":   2,
}

print(f"\nEnviando event.acknowledge...")
print(f"Payload: {json.dumps(ack_body, indent=2)}")

req = request.Request(
    f"{ZABBIX_URL}/api_jsonrpc.php",
    data=json.dumps(ack_body).encode("utf-8"),
    headers={
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {ZABBIX_API_TOKEN}",
    },
    method="POST",
)

try:
    with request.urlopen(req, timeout=10) as resp:
        raw  = resp.read().decode("utf-8")
        data = json.loads(raw)
        print(f"\nResposta Zabbix:\n{json.dumps(data, indent=2)}")

        if "error" in data:
            print(f"\n[FALHOU] Zabbix retornou erro: {data['error']}")
            sys.exit(1)
        else:
            print(f"\n[OK] Acknowledge enviado com sucesso.")
            print("Verifique em: Monitoring > Problems > coluna Ack ou Timeline do evento.")
except error.HTTPError as exc:
    detail = exc.read().decode("utf-8", errors="ignore")
    print(f"\n[ERRO] HTTP {exc.code}: {detail}")
    sys.exit(1)
except Exception as exc:
    print(f"\n[ERRO] {exc}")
    sys.exit(1)
