# =============================================================
# app_ai.py — Webhook Zabbix com janela de eventos + resumo IA
#
# Evolucao do app.py. Alem de receber eventos do Zabbix via
# HTTP POST, agrupa todos os eventos de um mesmo host em uma
# janela de tempo (padrao: 10 minutos). Ao fechar a janela,
# envia os eventos ao OpenRouter (LLM) para gerar um resumo
# descritivo com hipoteses e acoes recomendadas.
#
# Cada janela de host gera um arquivo .txt na pasta eventos_hosts/
# Ao finalizar, o arquivo e renomeado com sufixo _fechado.
# =============================================================

# Bibliotecas da biblioteca padrao do Python
import json        # Serializar/desserializar JSON
import os          # Ler variaveis de ambiente e criar arquivos com flags atomicas
import re          # Expressoes regulares (sanitizacao do nome do host)
import threading   # Thread em background para verificar janelas expiradas
import time        # Timestamp em segundos (epoch) para controle da janela
import uuid        # Gerar IDs unicos para cada sessao de host
from datetime import datetime  # Formatar timestamps legíveis
from pathlib import Path        # Manipulacao de caminhos de arquivo
from urllib import error, request  # HTTP nativo do Python (sem dependencia extra)

# Bibliotecas de terceiros instaladas via pip
from dotenv import load_dotenv                    # Carregar variaveis do .env
from flask import Flask, jsonify, request as flask_request  # Framework web
from redis import Redis                            # Cliente Redis/Valkey
from redis.exceptions import RedisError           # Excecoes de conexao

# Carrega as variaveis do arquivo .env para o ambiente do processo
load_dotenv()

# Cria a instancia da aplicacao Flask
app = Flask(__name__)

# --- Configuracoes lidas do .env com valores padrao como fallback ---

# Backend de cache: valkey (Rocky Linux) ou redis
CACHE_BACKEND = os.getenv("CACHE_BACKEND", "valkey").lower()

# Duracao da janela de agrupamento por host em segundos (padrao: 10 min)
WINDOW_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "600"))

# Pasta onde os arquivos de eventos serao salvos
EVENTS_DIR = Path(os.getenv("EVENTS_DIR", "eventos_hosts"))

# Chave de API do OpenRouter para chamadas ao modelo de linguagem (LLM)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")

# Modelo LLM usado para gerar o resumo (ex: openai/gpt-4o-mini)
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")

# Intervalo em segundos em que a thread de background verifica janelas expiradas
FINALIZER_INTERVAL_SECONDS = int(os.getenv("FINALIZER_INTERVAL_SECONDS", "20"))

# Marcador textual que identifica o bloco de resumo IA dentro do arquivo
# Usado para detectar se o resumo ja foi escrito e evitar duplicacao
SUMMARY_MARKER = "================ RESUMO IA ================"

# Cria a pasta de eventos se nao existir (parents=True cria diretorios pai)
EVENTS_DIR.mkdir(parents=True, exist_ok=True)

# Cliente de cache global (Valkey/Redis) -- inicia como None (lazy init)
cache_client: Redis | None = None

# Fallback em memoria: dicionario host_key -> dados da sessao
# Usado quando o Valkey nao esta disponivel
in_memory_sessions: dict[str, dict] = {}


def now_dt() -> datetime:
    """Retorna o datetime atual. Centralizado para facilitar testes."""
    return datetime.now()


def ts_epoch() -> int:
    """Retorna o timestamp atual em segundos (Unix epoch).

    Usado para calcular se a janela de 10 minutos expirou:
    now - started_at >= WINDOW_SECONDS
    """
    return int(time.time())


def sanitize_host(host: str) -> str:
    """Remove caracteres invalidos do nome do host para usar como nome de arquivo.

    Substitui qualquer caractere que nao seja letra, numero, ponto, traco ou
    underscore por underscore, evitando erros ao criar arquivos no sistema.
    """
    value = re.sub(r"[^a-zA-Z0-9._-]", "_", host.strip())
    return value or "host_desconhecido"


def normalize_payload(payload: dict) -> dict:
    """Limpa os valores de string do payload recebido do Zabbix.

    O Zabbix envia quebras de linha como \\r\\n (literal) ou \r\n.
    Converte ambos para \n real e remove espacos extras nas bordas.
    """
    return {
        k: v.replace("\\r\\n", "\n").replace("\r\n", "\n").strip()
        if isinstance(v, str)
        else v
        for k, v in payload.items()
    }


def get_host_key(payload: dict, fallback_ip: str | None) -> str:
    """Determina a chave de identificacao do host a partir do payload.

    Estrategia em cascata:
    1. Procura campos dedicados de host no JSON (host, host_name, etc.)
    2. Tenta extrair a linha 'Host: <nome>' do campo message do Zabbix
    3. Usa o IP de origem da requisicao como ultimo recurso
    """
    # 1. Campos padrao presentes no JSON
    host_fields = ["host", "host_name", "hostname", "host_ip"]
    for field in host_fields:
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()

    # 2. Extrai da mensagem do Zabbix (ex: "Host: Zabbix server")
    message = payload.get("message", "")
    if isinstance(message, str):
        for line in message.splitlines():
            line = line.strip()
            if line.lower().startswith("host:"):
                host_from_msg = line.split(":", 1)[1].strip()
                if host_from_msg:
                    return host_from_msg

    # 3. Fallback final: IP de quem fez a requisicao HTTP
    if fallback_ip:
        return fallback_ip
    return "host_desconhecido"


def create_cache_client() -> Redis | None:
    """Cria e testa a conexao com o Valkey/Redis.

    Le as configuracoes do .env priorizando VALKEY_* com fallback para REDIS_*.
    Retorna o cliente conectado ou None se o servidor nao estiver disponivel.
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
            decode_responses=True,   # Retorna strings, nao bytes
            socket_connect_timeout=2, # Falha rapido se o servidor nao responder
            socket_timeout=2,
        )
        client.ping()  # Valida a conexao antes de retornar
        backend_name = "Valkey" if CACHE_BACKEND == "valkey" else "Redis"
        print(f"[INFO] {backend_name} conectado em {host}:{port}")
        return client
    except RedisError as exc:
        backend_name = "Valkey" if CACHE_BACKEND == "valkey" else "Redis"
        # Modo degradado: sem cache, sessoes ficam somente em memoria local
        print(f"[WARN] {backend_name} indisponivel, usando memoria local: {exc}")
        return None


def get_cache_client() -> Redis | None:
    """Retorna o cliente de cache existente ou tenta criar um novo (lazy init)."""
    global cache_client
    if cache_client is None:
        cache_client = create_cache_client()
    return cache_client


def build_filename(host_key: str, started_at: datetime) -> Path:
    """Gera o caminho do arquivo de eventos para a janela aberta.

    Formato: <pasta>/<host_sanitizado>.<YYYYmmddHHMMSS>.txt
    Exemplo: eventos_hosts/Zabbix_server.20260520144820.txt
    """
    safe_host = sanitize_host(host_key)
    stamp = started_at.strftime("%Y%m%d%H%M%S")
    return EVENTS_DIR / f"{safe_host}.{stamp}.txt"


def openrouter_summarize(host_key: str, events_text: str) -> str:
    """Envia os eventos da janela ao OpenRouter e retorna o resumo da IA.

    O prompt instrui o modelo a responder em pt-BR com:
    1) Resumo descritivo dos eventos
    2) Hipoteses de causa raiz
    3) Acoes recomendadas em ordem de prioridade

    Usa a urllib padrao do Python para evitar dependencia de httpx/requests.
    """
    if not OPENROUTER_API_KEY:
        return "[ERRO] OPENROUTER_API_KEY nao configurada."

    # Prompt do usuario: contexto do host e todos os eventos da janela
    prompt = (
        "Voce e um analista SRE. Recebera eventos do Zabbix de um unico host dentro de 10 minutos. "
        "Responda em pt-BR com: 1) resumo descritivo dos eventos, 2) hipoteses provaveis de causa raiz, "
        "3) acoes recomendadas em ordem de prioridade. Seja objetivo.\n\n"
        f"Host: {host_key}\n\nEventos:\n{events_text}"
    )

    # Corpo da requisicao no formato esperado pela API OpenAI/OpenRouter
    body = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": "Voce resume alertas Zabbix e sugere resolucao tecnica."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,  # Valor baixo = respostas mais deterministicas e tecnicas
    }

    # Monta a requisicao HTTP com autenticacao via Bearer token
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
        # Envia a requisicao e aguarda resposta (timeout de 60s para o LLM processar)
        with request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            # Extrai o texto gerado pelo modelo da estrutura de resposta
            return data["choices"][0]["message"]["content"].strip()
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        return f"[ERRO] OpenRouter HTTP {exc.code}: {detail}"
    except Exception as exc:
        return f"[ERRO] Falha ao chamar OpenRouter: {exc}"


def append_event_to_file(file_path: Path, clean_payload: dict) -> None:
    """Adiciona um novo evento ao arquivo da janela aberta.

    Cada evento e separado por um cabecalho com o timestamp,
    seguido pelo JSON indentado do payload limpo.
    O arquivo e aberto em modo 'append' (a) para nao sobrescrever eventos anteriores.
    """
    event_time = now_dt().strftime("%Y-%m-%d %H:%M:%S")
    with file_path.open("a", encoding="utf-8") as f:
        f.write(f"\n--- EVENTO {event_time} ---\n")
        f.write(json.dumps(clean_payload, indent=2, ensure_ascii=False))
        f.write("\n")


def append_summary_to_file(file_path: Path, summary: str) -> None:
    """Adiciona o resumo gerado pela IA ao final do arquivo.

    O marcador SUMMARY_MARKER e usado para detectar se o resumo ja foi
    escrito, impedindo que a IA seja chamada mais de uma vez para o
    mesmo arquivo (ver funcao has_summary).
    """
    finished_at = now_dt().strftime("%Y-%m-%d %H:%M:%S")
    with file_path.open("a", encoding="utf-8") as f:
        f.write(f"\n{SUMMARY_MARKER}\n")
        f.write(f"Finalizado em: {finished_at}\n")
        f.write(summary)
        f.write("\n")


def has_summary(file_path: Path) -> bool:
    """Verifica se o arquivo ja contem o bloco de resumo da IA.

    Leitura simples do conteudo procurando pelo marcador fixo.
    Evita chamar o LLM e gravar resumo duplicado.
    """
    if not file_path.exists():
        return False
    return SUMMARY_MARKER in file_path.read_text(encoding="utf-8")


def build_closed_file_path(file_path: Path) -> Path:
    """Retorna o caminho do arquivo com sufixo _fechado no nome.

    Exemplo: Zabbix_server.20260520144820.txt
             -> Zabbix_server.20260520144820_fechado.txt
    """
    if file_path.stem.endswith("_fechado"):
        return file_path  # Ja esta com o sufixo correto
    return file_path.with_name(f"{file_path.stem}_fechado{file_path.suffix}")


def ensure_closed_filename(file_path: Path) -> Path:
    """Renomeia o arquivo adicionando o sufixo _fechado.

    Se ja existir um arquivo com esse nome (raro, mas possivel),
    adiciona um timestamp extra para garantir unicidade.
    Retorna o novo caminho do arquivo apos o rename.
    """
    target = build_closed_file_path(file_path)
    if target == file_path:
        return file_path  # Nada a fazer, ja esta fechado

    if target.exists():
        # Colisao de nome: adiciona timestamp para evitar sobrescrita
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        target = file_path.with_name(f"{file_path.stem}_fechado_{stamp}{file_path.suffix}")

    file_path.rename(target)
    return target


def acquire_finalize_lock(file_path: Path) -> bool:
    """Tenta adquirir uma trava exclusiva para finalizar o arquivo.

    Usa criacao atomica de arquivo (.lock) com O_EXCL para garantir que
    apenas uma thread/processo finalize o arquivo por vez.
    Retorna True se conseguiu a trava, False se outra thread ja esta finalizando.

    Este mecanismo e necessario porque o Flask em modo debug inicia dois
    processos (reloader + worker), o que poderia gerar dois resumos.
    """
    lock_path = Path(str(file_path) + ".lock")
    try:
        # O_CREAT | O_EXCL falha se o arquivo ja existir (operacao atomica)
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False  # Outra thread ja esta processando


def release_finalize_lock(file_path: Path) -> None:
    """Remove o arquivo de trava apos a finalizacao (sucesso ou erro)."""
    lock_path = Path(str(file_path) + ".lock")
    if lock_path.exists():
        lock_path.unlink()


def mark_session_finalized(host_key: str, session: dict, final_file_path: Path) -> None:
    """Marca a sessao como finalizada no cache e remove a chave ativa do host.

    Atualiza o campo 'finalized' para '1' e registra o novo caminho do arquivo
    (que agora tem sufixo _fechado). Remove a chave ativa para que o proximo
    evento do mesmo host abra uma nova sessao.
    """
    client = get_cache_client()
    if client is not None:
        try:
            # Atualiza o hash da sessao e remove a referencia ativa do host
            client.hset(
                _session_data_key(session["session_id"]),
                mapping={"finalized": "1", "file_path": str(final_file_path)},
            )
            client.delete(_session_key(host_key))
            return
        except RedisError:
            pass  # Cai para o fallback em memoria

    # Fallback: atualiza o dicionario em memoria
    in_memory = in_memory_sessions.get(host_key)
    if in_memory:
        in_memory["finalized"] = "1"
        in_memory["file_path"] = str(final_file_path)
        in_memory_sessions.pop(host_key, None)


def _session_key(host_key: str) -> str:
    """Chave no cache que aponta para o session_id ativo de um host.

    Exemplo: zbx:ai:active:Zabbix server
    Quando a janela expira, esta chave e deletada.
    """
    return f"zbx:ai:active:{host_key}"


def _session_data_key(session_id: str) -> str:
    """Chave no cache que armazena o hash de dados da sessao.

    Exemplo: zbx:ai:session:uuid4-gerado
    Contem: session_id, host_key, started_at, last_event_at, file_path, finalized
    """
    return f"zbx:ai:session:{session_id}"


def finalize_session(host_key: str, session: dict) -> None:
    """Finaliza a janela de um host: gera resumo IA, renomeia o arquivo e marca como fechado.

    Fluxo de finalizacao (executado somente uma vez por arquivo):
    1. Verifica se ja foi finalizado (flag ou resumo existente no arquivo)
    2. Adquire trava exclusiva via arquivo .lock (evita corrida entre threads)
    3. Le todo o conteudo do arquivo (todos os eventos da janela)
    4. Chama o OpenRouter para gerar o resumo
    5. Grava o resumo no final do arquivo
    6. Renomeia o arquivo para sufixo _fechado
    7. Marca a sessao como finalizada no cache
    8. Libera a trava
    """
    # Idempotencia: se ja foi marcado como finalizado, nao faz nada
    if str(session.get("finalized", "0")) == "1":
        return

    original_file_path = Path(session["file_path"])
    if not original_file_path.exists():
        return

    # Se o resumo ja esta no arquivo (ex: restart do servidor), garante apenas o rename
    if has_summary(original_file_path):
        final_file_path = ensure_closed_filename(original_file_path)
        mark_session_finalized(host_key, session, final_file_path)
        return

    # Trava atomica para evitar que dois processos finalizem ao mesmo tempo
    if not acquire_finalize_lock(original_file_path):
        return  # Outra thread ja esta finalizando este arquivo

    try:
        # Verifica novamente apos adquirir a trava (double-check)
        if has_summary(original_file_path):
            final_file_path = ensure_closed_filename(original_file_path)
            mark_session_finalized(host_key, session, final_file_path)
            return

        # Le todo o conteudo acumulado na janela para enviar ao LLM
        events_text = original_file_path.read_text(encoding="utf-8")

        # Chama o OpenRouter e obtem o resumo em pt-BR
        summary = openrouter_summarize(host_key, events_text)

        # Adiciona o resumo ao final do arquivo
        append_summary_to_file(original_file_path, summary)

        # Renomeia para _fechado, sinalizando que nao aceita mais eventos
        final_file_path = ensure_closed_filename(original_file_path)

        # Atualiza o cache com o status finalizado e novo caminho do arquivo
        mark_session_finalized(host_key, session, final_file_path)
    finally:
        # A trava e sempre liberada, mesmo que ocorra uma excecao
        release_finalize_lock(original_file_path)


def get_or_create_session(host_key: str) -> tuple[dict, bool]:
    """Retorna a sessao ativa do host ou cria uma nova se necessario.

    Logica:
    - Se existe sessao ativa e dentro da janela: retorna ela (False = existente)
    - Se existe sessao expirada: finaliza ela e cria nova (True = nova)
    - Se nao existe sessao: cria nova (True = nova)

    A sessao e um hash com: session_id, host_key, started_at,
    last_event_at, file_path e finalized.
    Usa Valkey/Redis como armazenamento principal com fallback em memoria.
    """
    now = ts_epoch()
    client = get_cache_client()

    if client is not None:
        try:
            # Busca o session_id ativo para este host no cache
            session_id = client.get(_session_key(host_key))
            if session_id:
                data_key = _session_data_key(session_id)
                session = client.hgetall(data_key)  # Le todos os campos do hash
                if session:
                    started_at = int(session["started_at"])
                    # Verifica se a janela ainda esta aberta
                    if now - started_at < WINDOW_SECONDS and session.get("finalized", "0") != "1":
                        return session, False  # Sessao ativa, reutiliza
                    # Janela expirou: finaliza antes de criar nova
                    finalize_session(host_key, session)

            # Cria nova sessao: gera ID unico e cria o arquivo inicial
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
            # Grava os dados da sessao como hash no Valkey
            client.hset(_session_data_key(session_id), mapping=session)
            # Grava o ponteiro host -> session_id
            client.set(_session_key(host_key), session_id)
            return session, True
        except RedisError:
            # Cache falhou: zera para reconexao e cai para memoria
            global cache_client
            cache_client = None

    # --- Fallback: gerenciamento em memoria local ---
    existing = in_memory_sessions.get(host_key)
    if existing and now - int(existing["started_at"]) < WINDOW_SECONDS and existing.get("finalized", "0") != "1":
        return existing, False  # Sessao ativa em memoria

    if existing and existing.get("finalized", "0") != "1":
        finalize_session(host_key, existing)  # Expirou: finaliza antes de criar nova

    # Cria nova sessao em memoria
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
    """Atualiza o timestamp do ultimo evento recebido na sessao.

    Registra quando foi o ultimo evento, util para monitoramento e logs.
    Nao afeta o calculo da janela (que usa started_at, nao last_event_at).
    """
    now = str(ts_epoch())
    session["last_event_at"] = now

    client = get_cache_client()
    if client is not None:
        try:
            client.hset(_session_data_key(session["session_id"]), mapping={"last_event_at": now})
            return
        except RedisError:
            pass  # Silencioso: nao e critico se falhar


def finalize_due_sessions_loop() -> None:
    """Thread em background que verifica periodicamente janelas expiradas.

    Roda em loop infinito com intervalo FINALIZER_INTERVAL_SECONDS.
    Ao encontrar uma sessao cujo started_at ultrapassou WINDOW_SECONDS,
    chama finalize_session para fechar a janela e gerar o resumo IA.

    Sem esta thread, a finalizacao so ocorreria quando chegasse um novo
    evento do mesmo host, o que pode demorar ou nunca acontecer.
    """
    while True:
        # Aguarda o intervalo antes de verificar (nao ocupa CPU)
        time.sleep(FINALIZER_INTERVAL_SECONDS)
        now = ts_epoch()
        client = get_cache_client()

        if client is not None:
            try:
                # scan_iter percorre todas as chaves ativas sem bloquear o servidor
                for key in client.scan_iter(match="zbx:ai:active:*"):
                    host_key = key.split("zbx:ai:active:", 1)[1]
                    session_id = client.get(key)
                    if not session_id:
                        continue
                    session = client.hgetall(_session_data_key(session_id))
                    if not session:
                        continue
                    started_at = int(session.get("started_at", now))
                    # Se a janela expirou, finaliza
                    if now - started_at >= WINDOW_SECONDS:
                        finalize_session(host_key, session)
            except RedisError:
                pass
            continue  # Pula o fallback em memoria se o cache funcionou

        # Fallback: verifica sessoes em memoria
        for host_key, session in list(in_memory_sessions.items()):
            started_at = int(session.get("started_at", now))
            if now - started_at >= WINDOW_SECONDS and session.get("finalized", "0") != "1":
                finalize_session(host_key, session)


# Rota principal: recebe POST do Zabbix em /webhook/zabbix
@app.post("/webhook/zabbix")
def zabbix_webhook() -> tuple:
    # Tenta interpretar o corpo como JSON (silent=True evita excecao)
    payload = flask_request.get_json(silent=True)

    if payload is None:
        # Payload invalido: retorna erro com o corpo bruto para diagnostico
        raw_body = flask_request.get_data(as_text=True)
        return jsonify({"status": "erro", "message": f"payload nao-JSON: {raw_body}"}), 400

    # Limpa o payload (normaliza quebras de linha vindas do Zabbix)
    clean = normalize_payload(payload)

    # Descobre o nome do host que originou o alerta
    host_key = get_host_key(clean, flask_request.remote_addr)

    # Busca ou cria a sessao/janela deste host no cache
    session, created_new_file = get_or_create_session(host_key)
    file_path = Path(session["file_path"])

    # Adiciona este evento ao arquivo da janela
    append_event_to_file(file_path, clean)

    # Atualiza o timestamp do ultimo evento na sessao
    touch_session(session)

    print("\n" + "=" * 60)
    print(f"Webhook recebido para host: {host_key}")
    print(f"Arquivo ativo: {file_path.name}")
    print("Status janela: " + ("nova" if created_new_file else "existente"))
    print("=" * 60 + "\n")

    # Retorna confirmacao com metadados uteis para debug
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


# Ponto de entrada: executado apenas quando rodado diretamente (python app_ai.py)
if __name__ == "__main__":
    debug_mode = os.getenv("APP_DEBUG", "true").lower() == "true"

    # O Flask em modo debug cria dois processos (reloader + worker).
    # WERKZEUG_RUN_MAIN='true' identifica o processo worker real.
    # Isso evita que a thread de finalizacao seja iniciada duas vezes,
    # o que causaria o problema de dois resumos sendo gerados.
    should_start_finalizer = (not debug_mode) or (os.environ.get("WERKZEUG_RUN_MAIN") == "true")
    if should_start_finalizer:
        # Thread daemon: encerra automaticamente quando o processo principal terminar
        threading.Thread(target=finalize_due_sessions_loop, daemon=True).start()

    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))

    print(f"Iniciando servidor IA em http://{host}:{port}")
    print(f"Diretorio de eventos: {EVENTS_DIR.resolve()}")
    app.run(host=host, port=port, debug=debug_mode)
