import os
import io
import json
import time
import tarfile

import pika
import docker

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = os.environ.get("RABBITMQ_QUEUE", "submissoes")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/data/results")

TIMEOUT_SEGUNDOS = int(os.environ.get("TIMEOUT_SEGUNDOS", "5"))
MEM_LIMIT = os.environ.get("MEM_LIMIT", "64m")
CPU_QUOTA = int(os.environ.get("CPU_QUOTA", "50000"))  # cpu_period padrao = 100000

os.makedirs(RESULTS_DIR, exist_ok=True)

docker_client = docker.from_env()


def _caminho_resultado(submissao_id: str) -> str:
    return os.path.join(RESULTS_DIR, f"{submissao_id}.json")


def atualizar_resultado(submissao_id: str, **campos):
    caminho = _caminho_resultado(submissao_id)
    resultado = {}
    if os.path.exists(caminho):
        with open(caminho) as f:
            resultado = json.load(f)
    resultado.update(campos)
    with open(caminho, "w") as f:
        json.dump(resultado, f, indent=2)


def _tar_com_codigo(codigo: str) -> io.BytesIO:
    """Empacota o codigo do aluno em um tar em memoria, pronto para
    docker put_archive. Evita depender de bind mount (que nao funciona
    quando o corretor roda dentro de outro container falando com o
    socket do host -- os caminhos ali sao relativos ao corretor, nao
    ao host, entao o Docker do host nao acha o diretorio)."""
    dados = codigo.encode("utf-8")
    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode="w") as tar:
        info = tarfile.TarInfo(name="solucao.py")
        info.size = len(dados)
        tar.addfile(info, io.BytesIO(dados))
    tar_stream.seek(0)
    return tar_stream


def executar_em_container(codigo: str) -> dict:
    inicio = time.time()
    container = None
    try:
        container = docker_client.containers.create(
            image="python:3.11-slim",
            command=["python", "/solucao.py"],
            mem_limit=MEM_LIMIT,
            memswap_limit=MEM_LIMIT,
            cpu_period=100000,
            cpu_quota=CPU_QUOTA,
            network_disabled=True,
        )
        # injeta o codigo do aluno diretamente na camada do container
        container.put_archive("/", _tar_com_codigo(codigo))
        container.start()
    except docker.errors.APIError as exc:
        if container is not None:
            try:
                container.remove(force=True)
            except Exception:
                pass
        return {
            "veredito": "erro_infraestrutura",
            "saida": str(exc),
            "tempo_execucao_s": round(time.time() - inicio, 3),
        }

    try:
        resultado_wait = container.wait(timeout=TIMEOUT_SEGUNDOS)
        status_code = resultado_wait.get("StatusCode", -1)
        logs = container.logs().decode("utf-8", errors="replace")

        container.reload()  # atualiza container.attrs com o estado final
        oom_killed = container.attrs.get("State", {}).get("OOMKilled", False)

        if status_code == 0:
            veredito = "aceito"
        elif oom_killed:
            veredito = "limite_memoria_excedido"
        else:
            veredito = "erro_execucao"
    except Exception:
        container.kill()
        logs = "Tempo limite de execucao excedido."
        veredito = "tempo_limite_excedido"
    finally:
        tempo_execucao = round(time.time() - inicio, 3)
        try:
            container.remove(force=True)
        except Exception:
            pass

    return {
        "veredito": veredito,
        "saida": logs,
        "tempo_execucao_s": tempo_execucao,
    }


def processar_mensagem(channel, method, properties, body):
    payload = json.loads(body)
    submissao_id = payload["id"]
    print(f" [x] Processando submissao {submissao_id}")

    try:
        atualizar_resultado(submissao_id, status="em_execucao")

        resultado = executar_em_container(payload["codigo"])

        atualizar_resultado(submissao_id, status="concluido", **resultado)

        # confirma o processamento -> mensagem removida da fila
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f" [x] Submissao {submissao_id} concluida: {resultado['veredito']}")

    except Exception as exc:
        atualizar_resultado(submissao_id, status="falha", erro=str(exc))
        # falha inesperada: nao confirma -> mensagem volta para a fila
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        print(f" [!] Falha ao processar {submissao_id}: {exc}")


def main():
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    # ACK automatico desabilitado + prefetch=1: pega uma submissao por vez
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=processar_mensagem)

    print(" [*] Corretor aguardando submissoes. CTRL+C para sair.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()


if __name__ == "__main__":
    main()
