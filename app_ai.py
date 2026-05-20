import json
import os
import re
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from urllib import error, request

from dotenv import load_dotenv
from flask import Flask, jsonify, request as flask_request
from redis import Redis
from redis.exceptions import RedisError

load_dotenv()

app = Flask(__name__)

CACHE_BACKEND = os.getenv("CACHE_BACKEND", "valkey").lower()
WINDOW_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "600"))
EVENTS_DIR = Path(os.getenv("EVENTS_DIR", "eventos_hosts"))
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
FINALIZER_INTERVAL_SECONDS = int(os.getenv("FINALIZER_INTERVAL_SECONDS", "20"))
SUMMARY_MARKER = "================ RESUMO IA ================"

EVENTS_DIR.mkdir(parents=True, exist_ok=True)

cache_client: Redis | None = None
in_memory_sessions: dict[str, dict] = {}


def now_dt() -> datetime:
    return datetime.now()


def ts_epoch() -> int:
    return int(time.time())


def sanitize_host(host: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9._-]", "_", host.strip())
    return value or "host_desconhecido"


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


def create_cache_client() -> Redis | None:
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
        print(f"[WARN] {backend_name} indisponivel, usando memoria local: {exc}")
        return None


def get_cache_client() -> Redis | None:
    global cache_client
    if cache_client is None:
        cache_client = create_cache_client()
    return cache_client


def build_filename(host_key: str, started_at: datetime) -> Path:
    safe_host = sanitize_host(host_key)
    stamp = started_at.strftime("%Y%m%d%H%M%S")
    return EVENTS_DIR / f"{safe_host}.{stamp}.txt"


def openrouter_summarize(host_key: str, events_text: str) -> str:
    if not OPENROUTER_API_KEY:
        return "[ERRO] OPENROUTER_API_KEY nao configurada."

    prompt = (
        "Voce e um analista SRE. Recebera eventos do Zabbix de um unico host dentro de 10 minutos. "
        "Responda em pt-BR com: 1) resumo descritivo dos eventos, 2) hipoteses provaveis de causa raiz, "
        "3) acoes recomendadas em ordem de prioridade. Seja objetivo.\n\n"
        f"Host: {host_key}\n\nEventos:\n{events_text}"
    )

    body = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": "Voce resume alertas Zabbix e sugere resolucao tecnica."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }

    req = request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            return data["choices"][0]["message"]["content"].strip()
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        return f"[ERRO] OpenRouter HTTP {exc.code}: {detail}"
    except Exception as exc:
        return f"[ERRO] Falha ao chamar OpenRouter: {exc}"


def append_event_to_file(file_path: Path, clean_payload: dict) -> None:
    event_time = now_dt().strftime("%Y-%m-%d %H:%M:%S")
    with file_path.open("a", encoding="utf-8") as f:
        f.write(f"\n--- EVENTO {event_time} ---\n")
        f.write(json.dumps(clean_payload, indent=2, ensure_ascii=False))
        f.write("\n")


def append_summary_to_file(file_path: Path, summary: str) -> None:
    finished_at = now_dt().strftime("%Y-%m-%d %H:%M:%S")
    with file_path.open("a", encoding="utf-8") as f:
        f.write(f"\n{SUMMARY_MARKER}\n")
        f.write(f"Finalizado em: {finished_at}\n")
        f.write(summary)
        f.write("\n")


def has_summary(file_path: Path) -> bool:
    if not file_path.exists():
        return False
    return SUMMARY_MARKER in file_path.read_text(encoding="utf-8")


def acquire_finalize_lock(file_path: Path) -> bool:
    lock_path = Path(str(file_path) + ".lock")
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False


def release_finalize_lock(file_path: Path) -> None:
    lock_path = Path(str(file_path) + ".lock")
    if lock_path.exists():
        lock_path.unlink()


def _session_key(host_key: str) -> str:
    return f"zbx:ai:active:{host_key}"


def _session_data_key(session_id: str) -> str:
    return f"zbx:ai:session:{session_id}"


def finalize_session(host_key: str, session: dict) -> None:
    if str(session.get("finalized", "0")) == "1":
        return

    file_path = Path(session["file_path"])
    if not file_path.exists():
        return

    if has_summary(file_path):
        return

    if not acquire_finalize_lock(file_path):
        return

    try:
        if has_summary(file_path):
            return

        events_text = file_path.read_text(encoding="utf-8")
        summary = openrouter_summarize(host_key, events_text)
        append_summary_to_file(file_path, summary)

        client = get_cache_client()
        if client is not None:
            try:
                client.hset(_session_data_key(session["session_id"]), mapping={"finalized": "1"})
                client.delete(_session_key(host_key))
            except RedisError:
                pass
        else:
            in_memory = in_memory_sessions.get(host_key)
            if in_memory:
                in_memory["finalized"] = "1"
                in_memory_sessions.pop(host_key, None)
    finally:
        release_finalize_lock(file_path)


def get_or_create_session(host_key: str) -> tuple[dict, bool]:
    now = ts_epoch()
    client = get_cache_client()

    if client is not None:
        try:
            session_id = client.get(_session_key(host_key))
            if session_id:
                data_key = _session_data_key(session_id)
                session = client.hgetall(data_key)
                if session:
                    started_at = int(session["started_at"])
                    if now - started_at < WINDOW_SECONDS and session.get("finalized", "0") != "1":
                        return session, False
                    finalize_session(host_key, session)

            started = now_dt()
            session_id = str(uuid.uuid4())
            file_path = build_filename(host_key, started)
            file_path.write_text(
                f"Host: {host_key}\nInicio da janela: {started.strftime('%Y-%m-%d %H:%M:%S')}\n",
                encoding="utf-8",
            )

            session = {
                "session_id": session_id,
                "host_key": host_key,
                "started_at": str(now),
                "last_event_at": str(now),
                "file_path": str(file_path),
                "finalized": "0",
            }
            client.hset(_session_data_key(session_id), mapping=session)
            client.set(_session_key(host_key), session_id)
            return session, True
        except RedisError:
            global cache_client
            cache_client = None

    existing = in_memory_sessions.get(host_key)
    if existing and now - int(existing["started_at"]) < WINDOW_SECONDS and existing.get("finalized", "0") != "1":
        return existing, False

    if existing and existing.get("finalized", "0") != "1":
        finalize_session(host_key, existing)

    started = now_dt()
    session_id = str(uuid.uuid4())
    file_path = build_filename(host_key, started)
    file_path.write_text(
        f"Host: {host_key}\nInicio da janela: {started.strftime('%Y-%m-%d %H:%M:%S')}\n",
        encoding="utf-8",
    )

    session = {
        "session_id": session_id,
        "host_key": host_key,
        "started_at": str(now),
        "last_event_at": str(now),
        "file_path": str(file_path),
        "finalized": "0",
    }
    in_memory_sessions[host_key] = session
    return session, True


def touch_session(session: dict) -> None:
    now = str(ts_epoch())
    session["last_event_at"] = now

    client = get_cache_client()
    if client is not None:
        try:
            client.hset(_session_data_key(session["session_id"]), mapping={"last_event_at": now})
            return
        except RedisError:
            pass


def finalize_due_sessions_loop() -> None:
    while True:
        time.sleep(FINALIZER_INTERVAL_SECONDS)
        now = ts_epoch()
        client = get_cache_client()

        if client is not None:
            try:
                for key in client.scan_iter(match="zbx:ai:active:*"):
                    host_key = key.split("zbx:ai:active:", 1)[1]
                    session_id = client.get(key)
                    if not session_id:
                        continue
                    session = client.hgetall(_session_data_key(session_id))
                    if not session:
                        continue
                    started_at = int(session.get("started_at", now))
                    if now - started_at >= WINDOW_SECONDS:
                        finalize_session(host_key, session)
            except RedisError:
                pass
            continue

        for host_key, session in list(in_memory_sessions.items()):
            started_at = int(session.get("started_at", now))
            if now - started_at >= WINDOW_SECONDS and session.get("finalized", "0") != "1":
                finalize_session(host_key, session)


@app.post("/webhook/zabbix")
def zabbix_webhook() -> tuple:
    payload = flask_request.get_json(silent=True)

    if payload is None:
        raw_body = flask_request.get_data(as_text=True)
        return jsonify({"status": "erro", "message": f"payload nao-JSON: {raw_body}"}), 400

    clean = normalize_payload(payload)
    host_key = get_host_key(clean, flask_request.remote_addr)

    session, created_new_file = get_or_create_session(host_key)
    file_path = Path(session["file_path"])
    append_event_to_file(file_path, clean)
    touch_session(session)

    print("\n" + "=" * 60)
    print(f"Webhook recebido para host: {host_key}")
    print(f"Arquivo ativo: {file_path.name}")
    print("Status janela: " + ("nova" if created_new_file else "existente"))
    print("=" * 60 + "\n")

    return (
        jsonify(
            {
                "status": "ok",
                "host": host_key,
                "window_seconds": WINDOW_SECONDS,
                "file": file_path.name,
                "file_path": str(file_path),
                "session_status": "novo_arquivo" if created_new_file else "arquivo_atualizado",
            }
        ),
        200,
    )


if __name__ == "__main__":
    debug_mode = os.getenv("APP_DEBUG", "true").lower() == "true"
    should_start_finalizer = (not debug_mode) or (os.environ.get("WERKZEUG_RUN_MAIN") == "true")
    if should_start_finalizer:
        threading.Thread(target=finalize_due_sessions_loop, daemon=True).start()

    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))

    print(f"Iniciando servidor IA em http://{host}:{port}")
    print(f"Diretorio de eventos: {EVENTS_DIR.resolve()}")
    app.run(host=host, port=port, debug=debug_mode)
