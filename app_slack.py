# =============================================================
# app_slack.py — Webhook Zabbix + resumo IA + notificacao Slack
#
# Evolucao do app_ai.py. Alem de todas as funcionalidades de
# agrupamento de eventos e resumo via LLM, esta versao:
#
# - Atribui um numero de ticket (nome do arquivo sem extensao)
#   a cada janela aberta, registrando-o no Redis desde o primeiro
#   evento e retornando-o em todas as respostas do webhook.
#
# - Ao fechar o chamado, envia o resumo da IA para um canal Slack
#   via Incoming Webhook configurado em SLACK_WEBHOOK_URL.
#
# Configuracao necessaria no .env:
#   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../...
# =============================================================

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

# --- Configuracoes ---
CACHE_BACKEND              = os.getenv("CACHE_BACKEND", "valkey").lower()
WINDOW_SECONDS             = int(os.getenv("CACHE_TTL_SECONDS", "600"))
EVENTS_DIR                 = Path(os.getenv("EVENTS_DIR", "eventos_hosts"))
OPENROUTER_API_KEY         = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL           = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
FINALIZER_INTERVAL_SECONDS = int(os.getenv("FINALIZER_INTERVAL_SECONDS", "20"))
SLACK_WEBHOOK_URL          = os.getenv("SLACK_WEBHOOK_URL", "")
ZABBIX_URL                 = os.getenv("ZABBIX_URL", "").rstrip("/")
ZABBIX_API_TOKEN           = os.getenv("ZABBIX_API_TOKEN", "")

SUMMARY_MARKER = "================ RESUMO IA ================"

EVENTS_DIR.mkdir(parents=True, exist_ok=True)

cache_client: Redis | None = None
in_memory_sessions: dict[str, dict] = {}
in_memory_event_ids: dict[str, set] = {}  # fallback quando Redis indisponivel


# ------------------------------------------------------------------ utilidades

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


# ------------------------------------------------------------------ cache

def create_cache_client() -> Redis | None:
    host     = os.getenv("VALKEY_HOST", os.getenv("REDIS_HOST", "127.0.0.1"))
    port     = int(os.getenv("VALKEY_PORT", os.getenv("REDIS_PORT", "6379")))
    db       = int(os.getenv("VALKEY_DB", os.getenv("REDIS_DB", "0")))
    password = os.getenv("VALKEY_PASSWORD", os.getenv("REDIS_PASSWORD")) or None

    try:
        client = Redis(
            host=host, port=port, db=db, password=password,
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


# ------------------------------------------------------------------ eventos Zabbix

def _event_ids_key(session_id: str) -> str:
    """Chave do Set Redis que acumula os event_ids da sessao."""
    return f"zbx:ai:eventids:{session_id}"


def extract_event_id(payload: dict) -> str | None:
    """Extrai o event_id Zabbix do payload recebido.

    Tenta os campos mais comuns usados em configuracoes de webhook
    Zabbix: event_id, eventid, EVENT.ID.
    Retorna None se nenhum campo for encontrado ou o valor for zero.
    """
    for field in ("event_id", "eventid", "EVENT.ID", "event"):
        value = payload.get(field)
        if value and str(value).strip() not in ("", "0"):
            return str(value).strip()
    return None


def collect_event_id(session_id: str, payload: dict) -> None:
    """Armazena o event_id do payload no Set Redis da sessao."""
    event_id = extract_event_id(payload)

    # Log sempre — ajuda a diagnosticar se o campo chega ou nao
    raw_val = payload.get("event_id") or payload.get("eventid") or payload.get("EVENT.ID")
    print(f"[DEBUG] collect_event_id: raw='{raw_val}' -> extraido='{event_id}'")

    if not event_id:
        return

    client = get_cache_client()
    if client is not None:
        try:
            key = _event_ids_key(session_id)
            client.sadd(key, event_id)
            client.expire(key, 86400)  # TTL 24h
            print(f"[DEBUG] event_id {event_id} salvo no Redis (sessao {session_id})")
            return
        except RedisError:
            pass

    # Fallback em memoria
    if session_id not in in_memory_event_ids:
        in_memory_event_ids[session_id] = set()
    in_memory_event_ids[session_id].add(event_id)
    print(f"[DEBUG] event_id {event_id} salvo em memoria (sessao {session_id})")


def zabbix_acknowledge_events(session_id: str, ticket_number: str) -> None:
    """Chama event.acknowledge no Zabbix para todos os eventos do ticket."""
    if not ZABBIX_URL or not ZABBIX_API_TOKEN:
        print("[WARN] ZABBIX_URL ou ZABBIX_API_TOKEN nao configurados. Atualizacao ignorada.")
        return

    event_ids: list[str] = []
    client = get_cache_client()
    if client is not None:
        try:
            event_ids = list(client.smembers(_event_ids_key(session_id)))
        except RedisError:
            pass

    if not event_ids:
        event_ids = list(in_memory_event_ids.get(session_id, set()))

    print(f"[DEBUG] zabbix_acknowledge: session={session_id} event_ids={event_ids}")

    if not event_ids:
        print(f"[WARN] Nenhum event_id coletado para sessao {session_id}. Atualizacao ignorada.")
        return

    body = {
        "jsonrpc": "2.0",
        "method":  "event.acknowledge",
        "params":  {
            "eventids": event_ids,
            "action":   4,  # 4 = adicionar mensagem
            "message":  f"Ticket: {ticket_number}",
        },
        "auth": ZABBIX_API_TOKEN,
        "id":   1,
    }

    api_url = f"{ZABBIX_URL}/api_jsonrpc.php"
    print(f"[DEBUG] Chamando Zabbix API: {api_url}")
    print(f"[DEBUG] Payload: {json.dumps(body)}")

    req = request.Request(
        api_url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {ZABBIX_API_TOKEN}",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            print(f"[DEBUG] Zabbix resposta: {raw}")
            data = json.loads(raw)
            if "error" in data:
                print(f"[WARN] Zabbix event.acknowledge erro: {data['error']}")
            else:
                print(f"[INFO] Zabbix: {len(event_ids)} evento(s) atualizado(s) com ticket={ticket_number}")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        print(f"[WARN] Zabbix API HTTP {exc.code}: {detail}")
    except Exception as exc:
        print(f"[WARN] Falha ao chamar Zabbix API: {exc}")


# ------------------------------------------------------------------ sessao / arquivo

def build_filename(host_key: str, started_at: datetime) -> Path:
    safe_host = sanitize_host(host_key)
    stamp = started_at.strftime("%Y%m%d%H%M%S")
    return EVENTS_DIR / f"{safe_host}.{stamp}.txt"


def ticket_from_path(file_path: Path) -> str:
    """Retorna o numero do ticket: nome do arquivo sem extensao.

    Exemplo: eventos_hosts/Zabbix_server.20260521142454.txt
             -> Zabbix_server.20260521142454
    """
    stem = file_path.stem
    # Remove sufixo _fechado caso o path ja seja o arquivo final
    if stem.endswith("_fechado"):
        stem = stem[: -len("_fechado")]
    return stem


def _session_key(host_key: str) -> str:
    return f"zbx:ai:active:{host_key}"


def _session_data_key(session_id: str) -> str:
    return f"zbx:ai:session:{session_id}"


def mark_session_finalized(host_key: str, session: dict, final_file_path: Path) -> None:
    client = get_cache_client()
    if client is not None:
        try:
            client.hset(
                _session_data_key(session["session_id"]),
                mapping={"finalized": "1", "file_path": str(final_file_path)},
            )
            client.delete(_session_key(host_key))
            return
        except RedisError:
            pass

    in_memory = in_memory_sessions.get(host_key)
    if in_memory:
        in_memory["finalized"] = "1"
        in_memory["file_path"] = str(final_file_path)
        in_memory_sessions.pop(host_key, None)


def get_or_create_session(host_key: str) -> tuple[dict, bool]:
    """Retorna a sessao ativa ou cria uma nova.

    Ao criar, registra ticket_number no hash do Redis e no cabecalho
    do arquivo, para que todos os eventos da janela sejam associados
    ao mesmo numero de chamado.
    """
    now    = ts_epoch()
    client = get_cache_client()

    if client is not None:
        try:
            session_id = client.get(_session_key(host_key))
            if session_id:
                data_key = _session_data_key(session_id)
                session  = client.hgetall(data_key)
                if session:
                    started_at = int(session["started_at"])
                    if now - started_at < WINDOW_SECONDS and session.get("finalized", "0") != "1":
                        return session, False
                    finalize_session(host_key, session)

            started       = now_dt()
            session_id    = str(uuid.uuid4())
            file_path     = build_filename(host_key, started)
            ticket_number = ticket_from_path(file_path)

            # Cabecalho do arquivo inclui o numero do ticket
            file_path.write_text(
                f"Host:            {host_key}\n"
                f"Ticket:          {ticket_number}\n"
                f"Inicio da janela: {started.strftime('%Y-%m-%d %H:%M:%S')}\n",
                encoding="utf-8",
            )

            session = {
                "session_id":    session_id,
                "host_key":      host_key,
                "started_at":    str(now),
                "last_event_at": str(now),
                "file_path":     str(file_path),
                "ticket_number": ticket_number,
                "finalized":     "0",
            }
            client.hset(_session_data_key(session_id), mapping=session)
            client.set(_session_key(host_key), session_id)
            return session, True
        except RedisError:
            global cache_client
            cache_client = None

    # --- Fallback em memoria ---
    existing = in_memory_sessions.get(host_key)
    if existing and now - int(existing["started_at"]) < WINDOW_SECONDS and existing.get("finalized", "0") != "1":
        return existing, False

    if existing and existing.get("finalized", "0") != "1":
        finalize_session(host_key, existing)

    started       = now_dt()
    session_id    = str(uuid.uuid4())
    file_path     = build_filename(host_key, started)
    ticket_number = ticket_from_path(file_path)

    file_path.write_text(
        f"Host:            {host_key}\n"
        f"Ticket:          {ticket_number}\n"
        f"Inicio da janela: {started.strftime('%Y-%m-%d %H:%M:%S')}\n",
        encoding="utf-8",
    )

    session = {
        "session_id":    session_id,
        "host_key":      host_key,
        "started_at":    str(now),
        "last_event_at": str(now),
        "file_path":     str(file_path),
        "ticket_number": ticket_number,
        "finalized":     "0",
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
        except RedisError:
            pass


# ------------------------------------------------------------------ arquivo de eventos

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


def build_closed_file_path(file_path: Path) -> Path:
    if file_path.stem.endswith("_fechado"):
        return file_path
    return file_path.with_name(f"{file_path.stem}_fechado{file_path.suffix}")


def ensure_closed_filename(file_path: Path) -> Path:
    target = build_closed_file_path(file_path)
    if target == file_path:
        return file_path
    if target.exists():
        stamp  = datetime.now().strftime("%Y%m%d%H%M%S")
        target = file_path.with_name(f"{file_path.stem}_fechado_{stamp}{file_path.suffix}")
    file_path.rename(target)
    return target


# ------------------------------------------------------------------ trava de finalizacao

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


# ------------------------------------------------------------------ OpenRouter

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
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.2,
    }

    req = request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data["choices"][0]["message"]["content"].strip()
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        return f"[ERRO] OpenRouter HTTP {exc.code}: {detail}"
    except Exception as exc:
        return f"[ERRO] Falha ao chamar OpenRouter: {exc}"


# ------------------------------------------------------------------ Slack

def send_slack_notification(host_key: str, ticket_number: str, summary: str, finished_at: str) -> None:
    """Envia o resumo do chamado fechado para o canal Slack via Incoming Webhook.

    O Slack tem limite de 3000 caracteres por bloco de texto; o resumo
    e truncado se necessario para evitar rejeicao da mensagem.
    Silencioso em caso de falha (nao bloqueia o fechamento do chamado).
    """
    if not SLACK_WEBHOOK_URL:
        print("[WARN] SLACK_WEBHOOK_URL nao configurada. Notificacao Slack ignorada.")
        return

    # Slack limita blocos de texto a 3000 caracteres
    MAX_SUMMARY = 2900
    summary_display = summary if len(summary) <= MAX_SUMMARY else summary[:MAX_SUMMARY] + "\n_(resumo truncado)_"

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"Chamado Fechado: {ticket_number}", "emoji": True},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Host:*\n{host_key}"},
                    {"type": "mrkdwn", "text": f"*Fechado em:*\n{finished_at}"},
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Resumo IA:*\n{summary_display}"},
            },
            {"type": "divider"},
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"Ticket: `{ticket_number}` | Modelo: `{OPENROUTER_MODEL}`"},
                ],
            },
        ]
    }

    req = request.Request(
        SLACK_WEBHOOK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=10) as resp:
            status = resp.status
            if status != 200:
                print(f"[WARN] Slack retornou HTTP {status}")
            else:
                print(f"[INFO] Slack notificado: ticket={ticket_number}")
    except Exception as exc:
        print(f"[WARN] Falha ao notificar Slack: {exc}")


# ------------------------------------------------------------------ finalizacao

def finalize_session(host_key: str, session: dict) -> None:
    """Finaliza a janela: gera resumo IA, renomeia o arquivo, notifica o Slack.

    Fluxo:
    1. Idempotencia: sai se ja finalizado
    2. Trava exclusiva via .lock
    3. Le eventos acumulados e chama o LLM
    4. Grava resumo no arquivo
    5. Renomeia para _fechado
    6. Envia notificacao ao Slack
    7. Marca sessao como finalizada no cache
    8. Libera trava
    """
    if str(session.get("finalized", "0")) == "1":
        return

    original_file_path = Path(session["file_path"])
    if not original_file_path.exists():
        return

    ticket_number = session.get("ticket_number") or ticket_from_path(original_file_path)

    if has_summary(original_file_path):
        final_file_path = ensure_closed_filename(original_file_path)
        mark_session_finalized(host_key, session, final_file_path)
        return

    if not acquire_finalize_lock(original_file_path):
        return

    try:
        if has_summary(original_file_path):
            final_file_path = ensure_closed_filename(original_file_path)
            mark_session_finalized(host_key, session, final_file_path)
            return

        events_text = original_file_path.read_text(encoding="utf-8")
        summary     = openrouter_summarize(host_key, events_text)
        finished_at = now_dt().strftime("%Y-%m-%d %H:%M:%S")

        append_summary_to_file(original_file_path, summary)

        final_file_path = ensure_closed_filename(original_file_path)
        mark_session_finalized(host_key, session, final_file_path)

        # Atualiza os eventos Zabbix envolvidos com o numero do ticket
        zabbix_acknowledge_events(session["session_id"], ticket_number)

        # Notifica o Slack apos gravar e renomear o arquivo
        send_slack_notification(host_key, ticket_number, summary, finished_at)
    finally:
        release_finalize_lock(original_file_path)


# ------------------------------------------------------------------ thread de finalizacao

def finalize_due_sessions_loop() -> None:
    """Thread em background que verifica periodicamente janelas expiradas."""
    while True:
        time.sleep(FINALIZER_INTERVAL_SECONDS)
        now    = ts_epoch()
        client = get_cache_client()

        if client is not None:
            try:
                for key in client.scan_iter(match="zbx:ai:active:*"):
                    host_key   = key.split("zbx:ai:active:", 1)[1]
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


# ------------------------------------------------------------------ rota Flask

@app.post("/webhook/zabbix")
def zabbix_webhook() -> tuple:
    payload = flask_request.get_json(silent=True)

    if payload is None:
        raw_body = flask_request.get_data(as_text=True)
        return jsonify({"status": "erro", "message": f"payload nao-JSON: {raw_body}"}), 400

    clean    = normalize_payload(payload)
    host_key = get_host_key(clean, flask_request.remote_addr)

    session, created_new_file = get_or_create_session(host_key)
    file_path     = Path(session["file_path"])
    ticket_number = session.get("ticket_number") or ticket_from_path(file_path)

    append_event_to_file(file_path, clean)
    collect_event_id(session["session_id"], clean)
    touch_session(session)

    print("\n" + "=" * 60)
    print(f"Webhook recebido para host:  {host_key}")
    print(f"Ticket:                      {ticket_number}")
    print(f"Arquivo ativo:               {file_path.name}")
    print("Status janela: " + ("nova" if created_new_file else "existente"))
    print("=" * 60 + "\n")

    return (
        jsonify(
            {
                "status":         "ok",
                "host":           host_key,
                "ticket_number":  ticket_number,
                "window_seconds": WINDOW_SECONDS,
                "file":           file_path.name,
                "file_path":      str(file_path),
                "session_status": "novo_ticket" if created_new_file else "ticket_em_aberto",
            }
        ),
        200,
    )


# ------------------------------------------------------------------ entrypoint

if __name__ == "__main__":
    debug_mode = os.getenv("APP_DEBUG", "true").lower() == "true"

    should_start_finalizer = (not debug_mode) or (os.environ.get("WERKZEUG_RUN_MAIN") == "true")
    if should_start_finalizer:
        threading.Thread(target=finalize_due_sessions_loop, daemon=True).start()

    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))

    print(f"Iniciando app_slack em http://{host}:{port}")
    print(f"Diretorio de eventos: {EVENTS_DIR.resolve()}")
    print(f"Slack configurado:    {'sim' if SLACK_WEBHOOK_URL else 'NAO (SLACK_WEBHOOK_URL vazia)'}")
    app.run(host=host, port=port, debug=debug_mode)
