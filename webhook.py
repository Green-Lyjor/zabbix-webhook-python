import json
import os
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv()

app = Flask(__name__)


@app.post("/webhook/zabbix")
def zabbix_webhook() -> tuple:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = request.get_json(silent=True)

    print("\n" + "=" * 60)
    print(f"[{now}] Webhook do Zabbix recebido")
    print(f"IP origem : {request.remote_addr}")
    print("-" * 60)

    if payload is not None:
        clean = {
            k: v.replace("\\r\\n", "\n").replace("\r\n", "\n").strip()
            if isinstance(v, str)
            else v
            for k, v in payload.items()
        }
        print("PAYLOAD COMPLETO:")
        print(json.dumps(clean, indent=2, ensure_ascii=False))

        response_data = {
            "status": "ok",
            "message": "webhook recebido",
        }
    else:
        raw_body = request.get_data(as_text=True)
        print(f"PAYLOAD BRUTO (nao-JSON):\n{raw_body}")

        response_data = {
            "status": "ok",
            "message": "webhook recebido sem JSON",
        }

    print("=" * 60 + "\n")

    return jsonify(response_data), 200


if __name__ == "__main__":
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))

    print(f"Iniciando servidor em http://{host}:{port}")
    app.run(host=host, port=port, debug=True)
