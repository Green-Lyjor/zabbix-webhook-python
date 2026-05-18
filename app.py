import json
import os
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from redis import Redis
from redis.exceptions import RedisError

load_dotenv()

app = Flask(__name__)

ALERT_TTL_SECONDS = 600


def create_redis_client() -> Redis | None:
    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))
    password = os.getenv("REDIS_PASSWORD") or None

    try:
        client = Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        client.ping()
        return client
    except RedisError as exc:
        print(f"[WARN] Redis indisponivel: {exc}")
        return None


redis_client: Redis | None = None


def get_redis_client() -> Redis | None:
    global redis_client
    if redis_client is None:
        redis_client = create_redis_client()
    return redis_client


def normalize_payload(payload: dict) -> dict:
    return {
        k: v.replace("\\r\\n", "\n").replace("\r\n", "\n").strip()
        if isinstance(v, str)
        else v
        for k, v in payload.items()
    }


def get_host_key(payload: dict, fallback_ip: str | None) -> str:
    host_fields = ["host", "host_name", "hostname", "host_ip"]
    for field in host_fields:
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if fallback_ip:
        return fallback_ip
    return "host_desconhecido"


def classify_alert_by_host(host_key: str) -> str:
    client = get_redis_client()
    if client is None:
        return "sem redis: alerta tratado como novo"

    cache_key = f"zbx:host_alert:{host_key}"
    now = datetime.now().isoformat(timespec="seconds")

    try:
        previous = client.get(cache_key)
        client.setex(cache_key, ALERT_TTL_SECONDS, now)

        if previous:
            return "mesmo host (alerta nos ultimos 10 minutos)"
        return "novo host (sem alerta recente)"
    except RedisError as exc:
        global redis_client
        redis_client = None
        return f"erro redis: {exc}"


@app.post("/webhook/zabbix")
def zabbix_webhook() -> tuple:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = request.get_json(silent=True)

    print("\n" + "=" * 60)
    print(f"[{now}] Webhook do Zabbix recebido")
    print(f"IP origem : {request.remote_addr}")
    print("-" * 60)

    if payload is not None:
        clean = normalize_payload(payload)
        host_key = get_host_key(clean, request.remote_addr)
        status = classify_alert_by_host(host_key)

        print(f"CLASSIFICACAO: {status}")
        print(f"HOST CHAVE  : {host_key}")
        print("-" * 60)
        print("PAYLOAD COMPLETO:")
        print(json.dumps(clean, indent=2, ensure_ascii=False))

        response_data = {
            "status": "ok",
            "message": "webhook recebido",
            "classification": status,
            "host_key": host_key,
        }
    else:
        raw_body = request.get_data(as_text=True)
        print(f"PAYLOAD BRUTO (nao-JSON):\n{raw_body}")

        response_data = {
            "status": "ok",
            "message": "webhook recebido sem JSON",
            "classification": "nao classificado",
        }

    print("=" * 60 + "\n")

    return jsonify(response_data), 200


if __name__ == "__main__":
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))

    print(f"Iniciando servidor em http://{host}:{port}")
    app.run(host=host, port=port, debug=True)
