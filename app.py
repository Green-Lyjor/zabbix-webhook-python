import json
import os
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from redis import Redis
from redis.exceptions import RedisError

load_dotenv()

app = Flask(__name__)

ALERT_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "600"))
CACHE_BACKEND = os.getenv("CACHE_BACKEND", "valkey").lower()  # valkey ou redis


def create_cache_client() -> Redis | None:
    """Conecta ao Valkey/Redis para cache de alertas.
    
    Suporta tanto Valkey (fork do Redis) quanto Redis.
    A API é idêntica, diferença está apenas na instalação.
    """
    host = os.getenv("VALKEY_HOST", os.getenv("REDIS_HOST", "127.0.0.1"))
    port = int(os.getenv("VALKEY_PORT", os.getenv("REDIS_PORT", "6379")))
    db = int(os.getenv("VALKEY_DB", os.getenv("REDIS_DB", "0")))
    password = os.getenv("VALKEY_PASSWORD", os.getenv("REDIS_PASSWORD")) or None

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
        backend_name = "Valkey" if CACHE_BACKEND == "valkey" else "Redis"
        print(f"[INFO] {backend_name} conectado em {host}:{port}")
        return client
    except RedisError as exc:
        backend_name = "Valkey" if CACHE_BACKEND == "valkey" else "Redis"
        print(f"[WARN] {backend_name} indisponivel: {exc}")
        return None


cache_client: Redis | None = None


def get_cache_client() -> Redis | None:
    """Obtém conexão com Valkey/Redis, reconectando se necessário."""
    global cache_client
    if cache_client is None:
        cache_client = create_cache_client()
    return cache_client


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

    # Fallback: extrai "Host: <nome>" da linha do campo message
    message = payload.get("message", "")
    if isinstance(message, str):
        for line in message.splitlines():
            line = line.strip()
            if line.lower().startswith("host:"):
                host_from_msg = line.split(":", 1)[1].strip()
                if host_from_msg:
                    return host_from_msg

    if fallback_ip:
        return fallback_ip
    return "host_desconhecido"


def classify_alert_by_host(host_key: str) -> str:
    """Classifica alerta como novo ou repetido (em 10 min) usando Valkey/Redis."""
    client = get_cache_client()
    if client is None:
        backend = "Valkey" if CACHE_BACKEND == "valkey" else "Redis"
        return f"sem {backend.lower()}: alerta tratado como novo"

    cache_key = f"zbx:host_alert:{host_key}"
    now = datetime.now().isoformat(timespec="seconds")

    try:
        previous = client.get(cache_key)
        client.setex(cache_key, ALERT_TTL_SECONDS, now)

        if previous:
            return "mesmo host (alerta nos ultimos 10 minutos)"
        return "novo host (sem alerta recente)"
    except RedisError as exc:
        global cache_client
        cache_client = None
        return f"erro cache: {exc}"


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
