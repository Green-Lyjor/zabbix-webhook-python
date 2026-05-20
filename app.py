# =============================================================
# app.py — Webhook Zabbix com cache Valkey/Redis
#
# Recebe eventos do Zabbix via HTTP POST, exibe o payload no
# terminal e classifica se o alerta veio de um host ja visto
# nos ultimos N minutos (usando Valkey/Redis como cache TTL).
# =============================================================

# Bibliotecas da biblioteca padrao do Python
import json          # Serializar/desserializar JSON
import os            # Ler variaveis de ambiente
from datetime import datetime  # Formatar timestamps

# Bibliotecas de terceiros instaladas via pip
from dotenv import load_dotenv       # Carregar variaveis do arquivo .env
from flask import Flask, jsonify, request  # Framework web leve
from redis import Redis              # Cliente Redis/Valkey (protocolo RESP)
from redis.exceptions import RedisError  # Excecoes especificas do Redis

# Carrega as variaveis definidas no arquivo .env para os.environ
# Isso evita escrever senhas e configs diretamente no codigo
load_dotenv()

# Cria a instancia da aplicacao Flask
# __name__ diz ao Flask onde procurar recursos relativos a este modulo
app = Flask(__name__)

# Tempo de vida (TTL) do cache por host, em segundos
# Configuravel via .env: CACHE_TTL_SECONDS=600 (padrao = 10 minutos)
ALERT_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "600"))

# Backend de cache: valkey (recomendado para Rocky Linux 10) ou redis
# Ambos usam o mesmo protocolo RESP, entao a biblioteca redis-py funciona
CACHE_BACKEND = os.getenv("CACHE_BACKEND", "valkey").lower()  # valkey ou redis


def create_cache_client() -> Redis | None:
    """Cria e testa a conexao com o Valkey/Redis.

    Le as configuracoes do .env com fallback para Redis caso
    as variaveis VALKEY_* nao estejam definidas.
    Retorna o cliente conectado ou None se falhar.
    """
    # Le host, porta, banco e senha das variaveis de ambiente
    # Prioriza VALKEY_*, mas aceita REDIS_* como fallback
    host = os.getenv("VALKEY_HOST", os.getenv("REDIS_HOST", "127.0.0.1"))
    port = int(os.getenv("VALKEY_PORT", os.getenv("REDIS_PORT", "6379")))
    db = int(os.getenv("VALKEY_DB", os.getenv("REDIS_DB", "0")))
    password = os.getenv("VALKEY_PASSWORD", os.getenv("REDIS_PASSWORD")) or None

    try:
        # Cria o cliente com timeout curto para falhar rapido se o servidor nao responder
        # decode_responses=True faz o cliente retornar strings em vez de bytes
        client = Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        # PING valida que a conexao esta ativa antes de retornar o cliente
        client.ping()
        backend_name = "Valkey" if CACHE_BACKEND == "valkey" else "Redis"
        print(f"[INFO] {backend_name} conectado em {host}:{port}")
        return client
    except RedisError as exc:
        # Se o servidor nao estiver disponivel, o app continua sem cache
        # Os alertas serao tratados como "novo" ate o cache voltar
        backend_name = "Valkey" if CACHE_BACKEND == "valkey" else "Redis"
        print(f"[WARN] {backend_name} indisponivel: {exc}")
        return None


# Variavel global que guarda o cliente ativo entre requisicoes
# Inicia como None e e preenchida na primeira chamada
cache_client: Redis | None = None


def get_cache_client() -> Redis | None:
    """Retorna o cliente de cache existente ou tenta criar um novo.

    Padrao Lazy Initialization: so conecta quando necessario.
    Se o cliente anterior caiu (RedisError), a variavel global e
    zerada para que a proxima chamada tente reconectar.
    """
    global cache_client
    if cache_client is None:
        cache_client = create_cache_client()
    return cache_client


def normalize_payload(payload: dict) -> dict:
    """Limpa os valores de string do payload recebido.

    O Zabbix envia quebras de linha como \\r\\n (literal) ou \r\n.
    Esta funcao converte ambos para \n real e remove espacos extras,
    deixando o payload legivel para exibir e processar.
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
    2. Se nao encontrar, tenta extrair a linha 'Host: <nome>' do campo message
    3. Como ultimo recurso usa o IP de origem da requisicao
    """
    # 1. Verifica campos padrao de host presentes no payload JSON
    host_fields = ["host", "host_name", "hostname", "host_ip"]
    for field in host_fields:
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()

    # 2. Fallback: o Zabbix pode enviar o host apenas dentro do campo 'message'
    #    Exemplo de linha: "Host: Zabbix server"
    message = payload.get("message", "")
    if isinstance(message, str):
        for line in message.splitlines():
            line = line.strip()
            if line.lower().startswith("host:"):
                host_from_msg = line.split(":", 1)[1].strip()
                if host_from_msg:
                    return host_from_msg

    # 3. Ultimo recurso: usa o IP de origem da requisicao HTTP
    if fallback_ip:
        return fallback_ip
    return "host_desconhecido"


def classify_alert_by_host(host_key: str) -> str:
    """Classifica o alerta como novo ou repetido usando cache com TTL.

    Logica:
    - Monta uma chave unica no cache para o host (zbx:host_alert:<host>)
    - Verifica se essa chave ja existe (alerta anterior nos ultimos N min)
    - Grava/atualiza a chave com TTL para expirar apos ALERT_TTL_SECONDS
    - Retorna uma string descritiva da classificacao
    """
    client = get_cache_client()
    # Se o cache nao estiver disponivel, trata como novo para nao bloquear
    if client is None:
        backend = "Valkey" if CACHE_BACKEND == "valkey" else "Redis"
        return f"sem {backend.lower()}: alerta tratado como novo"

    # Chave no cache: prefixo zbx: evita conflito com outras aplicacoes
    cache_key = f"zbx:host_alert:{host_key}"
    now = datetime.now().isoformat(timespec="seconds")

    try:
        # GET verifica se ja existe registro para este host
        previous = client.get(cache_key)
        # SETEX grava (ou renova) a chave com tempo de expiracao automatico
        client.setex(cache_key, ALERT_TTL_SECONDS, now)

        if previous:
            return "mesmo host (alerta nos ultimos 10 minutos)"
        return "novo host (sem alerta recente)"
    except RedisError as exc:
        # Zera o cliente para forcar reconexao na proxima requisicao
        global cache_client
        cache_client = None
        return f"erro cache: {exc}"


# Rota principal: recebe POST do Zabbix em /webhook/zabbix
# O Zabbix e configurado para enviar um HTTP POST com JSON para esta URL
@app.post("/webhook/zabbix")
def zabbix_webhook() -> tuple:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Tenta interpretar o corpo da requisicao como JSON
    # silent=True evita excecao se o corpo nao for JSON valido
    payload = request.get_json(silent=True)

    print("\n" + "=" * 60)
    print(f"[{now}] Webhook do Zabbix recebido")
    print(f"IP origem : {request.remote_addr}")
    print("-" * 60)

    if payload is not None:
        # Limpa o payload (normaliza quebras de linha do Zabbix)
        clean = normalize_payload(payload)

        # Descobre o nome do host que gerou o alerta
        host_key = get_host_key(clean, request.remote_addr)

        # Consulta o cache para saber se e alerta novo ou repetido
        status = classify_alert_by_host(host_key)

        # Exibe no terminal de forma legivel
        print(f"CLASSIFICACAO: {status}")
        print(f"HOST CHAVE  : {host_key}")
        print("-" * 60)
        print("PAYLOAD COMPLETO:")
        print(json.dumps(clean, indent=2, ensure_ascii=False))

        # Monta resposta JSON para o Zabbix confirmar recebimento
        response_data = {
            "status": "ok",
            "message": "webhook recebido",
            "classification": status,
            "host_key": host_key,
        }
    else:
        # Payload nao era JSON: exibe o corpo bruto para diagnostico
        raw_body = request.get_data(as_text=True)
        print(f"PAYLOAD BRUTO (nao-JSON):\n{raw_body}")

        response_data = {
            "status": "ok",
            "message": "webhook recebido sem JSON",
            "classification": "nao classificado",
        }

    print("=" * 60 + "\n")

    # HTTP 200 confirma para o Zabbix que o webhook foi processado com sucesso
    return jsonify(response_data), 200


# Ponto de entrada: executado apenas quando rodado diretamente (python app.py)
# Se importado como modulo (ex: testes), este bloco e ignorado
if __name__ == "__main__":
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))

    print(f"Iniciando servidor em http://{host}:{port}")
    # debug=True ativa reloader automatico e mensagens detalhadas de erro
    # Nao usar debug=True em producao
    app.run(host=host, port=port, debug=True)
