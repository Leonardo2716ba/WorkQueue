import os
import json
import uuid
from datetime import datetime

import pika
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = os.environ.get("RABBITMQ_QUEUE", "submissoes")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/data/results")

os.makedirs(RESULTS_DIR, exist_ok=True)


def get_connection():
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(params)


def publicar_submissao(mensagem: dict):
    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=RABBITMQ_QUEUE,
        body=json.dumps(mensagem),
        properties=pika.BasicProperties(delivery_mode=2),  # mensagem persistente
    )
    connection.close()


def _caminho_resultado(submissao_id: str) -> str:
    return os.path.join(RESULTS_DIR, f"{submissao_id}.json")


def salvar_resultado_inicial(submissao_id: str, payload: dict):
    resultado = {
        "id": submissao_id,
        "status": "pendente",
        "id_questao": payload["id_questao"],
        "id_aluno": payload["id_aluno"],
        "criado_em": datetime.utcnow().isoformat() + "Z",
    }
    with open(_caminho_resultado(submissao_id), "w") as f:
        json.dump(resultado, f, indent=2)


def ler_resultado(submissao_id: str):
    caminho = _caminho_resultado(submissao_id)
    if not os.path.exists(caminho):
        return None
    with open(caminho) as f:
        return json.load(f)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/submissoes", methods=["POST"])
def criar_submissao():
    """
    Recebe: codigo (string), id_questao, id_aluno
    Publica a submissao na fila e retorna o id gerado (202 Accepted).
    """
    dados = request.get_json(silent=True) or {}

    campos_obrigatorios = ["codigo", "id_questao", "id_aluno"]
    faltando = [c for c in campos_obrigatorios if c not in dados]
    if faltando:
        return jsonify({
            "erro": "Campos obrigatorios ausentes",
            "campos": faltando,
        }), 400

    submissao_id = str(uuid.uuid4())
    mensagem = {
        "id": submissao_id,
        "codigo": dados["codigo"],
        "id_questao": dados["id_questao"],
        "id_aluno": dados["id_aluno"],
    }

    try:
        salvar_resultado_inicial(submissao_id, mensagem)
        publicar_submissao(mensagem)
    except Exception as exc:
        return jsonify({"erro": "Falha ao publicar submissao no broker", "detalhe": str(exc)}), 502

    return jsonify({"id": submissao_id, "status": "pendente"}), 202


@app.route("/submissoes/<submissao_id>", methods=["GET"])
def consultar_submissao(submissao_id):
    resultado = ler_resultado(submissao_id)
    if resultado is None:
        return jsonify({"erro": "Submissao nao encontrada"}), 404
    return jsonify(resultado)


@app.route("/painel", methods=["GET"])
def painel():
    return render_template("painel.html")


@app.route("/submissoes", methods=["GET"])
def listar_submissoes():
    """Lista todas as submissoes conhecidas, mais recentes primeiro."""
    submissoes = []
    for nome_arquivo in os.listdir(RESULTS_DIR):
        if not nome_arquivo.endswith(".json"):
            continue
        with open(os.path.join(RESULTS_DIR, nome_arquivo)) as f:
            submissoes.append(json.load(f))

    submissoes.sort(key=lambda s: s.get("criado_em", ""), reverse=True)
    return jsonify({"total": len(submissoes), "submissoes": submissoes})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
