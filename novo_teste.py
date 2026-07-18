#!/usr/bin/env python3
"""
Teste de carga para a fila de submissoes do opCoders Judge (ambiente de teste).

Envia N submissoes concorrentes para a API, espera todas terminarem
(polling) e imprime um resumo: distribuicao por veredito/status, tempo
medio de execucao, vazao de processamento e (se aplicavel) speedup/eficiencia.

Dependencias: apenas biblioteca padrao do Python 3 (urllib, json, csv, etc.)

Uso interativo (recomendado):
    python3 teste_carga.py
    -> o script pergunta o que precisar (URL, total, concorrencia, timeout,
       numero de corretores) e usa valores padrao se voce so apertar ENTER.

Uso nao interativo (para automatizar/scriptar):
    python3 teste_carga.py [url_base] [total_requisicoes] [concorrencia] [timeout_poll_s] [n_corretores]
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from statistics import mean
from typing import Optional


# --------------------------------------------------------------------------
# Helper: pergunta com valor padrao. So pergunta se o argumento posicional
# correspondente nao foi passado na chamada do script.
# --------------------------------------------------------------------------
def perguntar(prompt: str, default: str, valor_arg: Optional[str] = None) -> str:
    if valor_arg:
        return valor_arg
    try:
        resposta = input(f"{prompt} [{default}]: ").strip()
    except EOFError:
        resposta = ""
    return resposta or default


def detectar_corretores() -> Optional[int]:
    """Tenta detectar o numero de corretores ativos via docker compose."""
    try:
        subprocess.run(
            ["docker", "compose", "ps", "-q", "corretor"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

    resultado = subprocess.run(
        ["docker", "compose", "ps", "-q", "corretor"],
        capture_output=True,
        text=True,
    )
    ids = [linha for linha in resultado.stdout.splitlines() if linha.strip()]
    return len(ids) if ids else None


# --------------------------------------------------------------------------
# Payloads de teste
# --------------------------------------------------------------------------
def payload_normal(i: int) -> dict:
    codigo = (
        "import random\n"
        "n = 1000\n"
        "vetor = [random.randint(1, 1000) for _ in range(n)]\n"
        "trocas = 0\n"
        "for i in range(len(vetor)):\n"
        "    for j in range(len(vetor) - i - 1):\n"
        "        if vetor[j] > vetor[j + 1]:\n"
        "            vetor[j], vetor[j + 1] = vetor[j + 1], vetor[j]\n"
        "            trocas += 1\n"
        "print(f'ordenado n={n} trocas={trocas} ok={vetor == sorted(vetor)}')"
    )
    return {"codigo": codigo, "id_questao": "carga", "id_aluno": f"carga_{i}"}


def payload_lento(i: int) -> dict:
    codigo = f"import time\ntime.sleep(1)\nprint('lento {i}')"
    return {"codigo": codigo, "id_questao": "carga", "id_aluno": f"carga_{i}"}


def payload_erro(i: int) -> dict:
    codigo = f"raise ValueError('erro proposital {i}')"
    return {"codigo": codigo, "id_questao": "carga", "id_aluno": f"carga_{i}"}


def payload_loop_infinito(i: int) -> dict:
    codigo = "while True: pass"
    return {"codigo": codigo, "id_questao": "carga", "id_aluno": f"carga_{i}"}


def payload_memoria(i: int) -> dict:
    codigo = (
        "x = []\n"
        "while True:\n"
        "    x.append(bytearray(10**6))\n"
        "    print(len(x), 'MB alocados')"
    )
    return {"codigo": codigo, "id_questao": "carga", "id_aluno": f"carga_{i}"}


def gerar_payload(i: int) -> dict:
    # Mesma escolha do script original: apenas payload "normal".
    # Para exercitar timeout/erro/latencia, troque por uma distribuicao,
    # ex.: usando i % 10 para escolher entre payload_normal, payload_lento,
    # payload_erro, payload_loop_infinito e payload_memoria.
    return payload_normal(i)


# --------------------------------------------------------------------------
# HTTP helpers (apenas biblioteca padrao)
# --------------------------------------------------------------------------
def http_get(url: str, timeout: float = 10.0):
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return resp.status, body


def http_post_json(url: str, payload: dict, timeout: float = 30.0):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        return e.code, body


# --------------------------------------------------------------------------
# Estruturas de resultado
# --------------------------------------------------------------------------
@dataclass
class ResultadoEnvio:
    indice: int
    ok: bool
    id_submissao: Optional[str] = None
    erro: Optional[str] = None


@dataclass
class ResultadoPoll:
    id_submissao: str
    status: str
    veredito: str = "-"
    tempo_execucao_s: Optional[float] = None
    ts_conclusao: Optional[float] = None


def enviar_submissao(url_base: str, i: int) -> ResultadoEnvio:
    payload = gerar_payload(i)
    try:
        status, body = http_post_json(f"{url_base}/submissoes", payload)
    except Exception as e:  # falha de rede/timeout etc.
        return ResultadoEnvio(indice=i, ok=False, erro=f"ERRO_ENVIO {e}")

    if status != 202:
        return ResultadoEnvio(indice=i, ok=False, erro=f"ERRO_ENVIO {status}")

    try:
        id_submissao = json.loads(body)["id"]
    except Exception:
        return ResultadoEnvio(indice=i, ok=False, erro="ERRO_PARSE")

    return ResultadoEnvio(indice=i, ok=True, id_submissao=id_submissao)


def consultar_id(url_base: str, id_submissao: str, timeout_poll: int) -> ResultadoPoll:
    esperado = 0
    while esperado < timeout_poll:
        try:
            _, body = http_get(f"{url_base}/submissoes/{id_submissao}")
            dados = json.loads(body)
        except Exception:
            dados = {}

        status = dados.get("status", "")
        if status in ("concluido", "falha"):
            veredito = dados.get("veredito", "-")
            tempo = dados.get("tempo_execucao_s", "-")
            tempo_execucao = float(tempo) if tempo not in ("-", None) else None
            return ResultadoPoll(
                id_submissao=id_submissao,
                status=status,
                veredito=str(veredito),
                tempo_execucao_s=tempo_execucao,
                ts_conclusao=time.time(),
            )

        time.sleep(1)
        esperado += 1

    return ResultadoPoll(id_submissao=id_submissao, status="timeout_poll")


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main() -> int:
    args = sys.argv[1:]
    arg = lambda idx: args[idx] if idx < len(args) and args[idx] else None

    url_base = perguntar("URL base da API", "http://localhost:5000", arg(0))
    total = int(perguntar("Total de requisicoes", "1000", arg(1)))
    concorrencia = int(perguntar("Concorrencia de envio", "20", arg(2)))
    timeout_poll = int(perguntar("Timeout de polling (s)", "30", arg(3)))

    n_detectado = detectar_corretores()
    if arg(4):
        n_corretores = int(arg(4))
    elif n_detectado:
        n_corretores = int(
            perguntar(
                "Corretores ativos (detectado via docker compose)",
                str(n_detectado),
                None,
            )
        )
    else:
        n_corretores = int(perguntar("Quantos corretores estao ativos nesta execucao?", "1", None))

    resumo_csv = "./resumo_testes_carga.csv"

    print()
    print("== Teste de carga - opCoders Judge ==")
    print(f"URL base:      {url_base}")
    print(f"Requisicoes:   {total}")
    print(f"Concorrencia:  {concorrencia}")
    print(f"Timeout poll:  {timeout_poll}s por submissao")
    print(f"Corretores:    {n_corretores}")
    print()

    try:
        status, _ = http_get(f"{url_base}/health")
        if status >= 400:
            raise RuntimeError(f"status {status}")
    except Exception:
        print(f"Erro: API nao respondeu em {url_base}/health. Ela esta no ar?")
        return 1

    # ---------------- Envio concorrente ----------------
    print(f"Enviando {total} submissoes (concorrencia {concorrencia})...")
    inicio_envio = time.time()

    resultados_envio: list[ResultadoEnvio] = []
    with ThreadPoolExecutor(max_workers=concorrencia) as pool:
        futuros = [pool.submit(enviar_submissao, url_base, i) for i in range(1, total + 1)]
        for fut in as_completed(futuros):
            resultados_envio.append(fut.result())

    fim_envio = time.time()
    tempo_envio = fim_envio - inicio_envio

    ids_ok = [r.id_submissao for r in resultados_envio if r.ok]
    total_enviadas = len(ids_ok)
    total_erros_envio = len(resultados_envio) - total_enviadas

    print(f"Envio concluido em {tempo_envio:.2f}s")
    print(f"  OK: {total_enviadas}   Falhas de envio: {total_erros_envio}")
    print()

    if total_enviadas == 0:
        print("Nenhuma submissao foi enviada com sucesso. Abortando.")
        return 1

    # ---------------- Polling concorrente ----------------
    print(f"Aguardando processamento (polling a cada 1s, timeout {timeout_poll}s por submissao)...")
    inicio_poll = time.time()

    resultados_poll: list[ResultadoPoll] = []
    with ThreadPoolExecutor(max_workers=concorrencia) as pool:
        futuros = [pool.submit(consultar_id, url_base, id_, timeout_poll) for id_ in ids_ok]
        for fut in as_completed(futuros):
            resultados_poll.append(fut.result())

    fim_poll = time.time()
    tempo_poll = fim_poll - inicio_poll

    print(f"Polling concluido em {tempo_poll:.2f}s")
    print()

    # ---------------- Resumo ----------------
    print("== Resumo ==")
    print(f"Total enviado:        {total_enviadas}")
    print(f"Falhas no envio:      {total_erros_envio}")
    print()

    print("Distribuicao por veredito:")
    contagem_veredito: dict[str, int] = {}
    for r in resultados_poll:
        contagem_veredito[r.veredito] = contagem_veredito.get(r.veredito, 0) + 1
    for veredito, qtd in sorted(contagem_veredito.items(), key=lambda x: -x[1]):
        print(f"{qtd:>6} {veredito}")

    print()
    print("Distribuicao por status:")
    contagem_status: dict[str, int] = {}
    for r in resultados_poll:
        contagem_status[r.status] = contagem_status.get(r.status, 0) + 1
    for status, qtd in sorted(contagem_status.items(), key=lambda x: -x[1]):
        print(f"{qtd:>6} {status}")

    print()
    tempos_validos = [r.tempo_execucao_s for r in resultados_poll if r.tempo_execucao_s is not None]
    tempo_medio = mean(tempos_validos) if tempos_validos else 0.0
    n_concluidas = len(tempos_validos)
    print("Tempo medio de execucao (apenas concluidas):")
    print(f"  {tempo_medio:.3f}s (n={n_concluidas})")

    print()
    tempo_total = tempo_envio + tempo_poll
    vazao_envio = total_enviadas / tempo_envio if tempo_envio > 0 else float("nan")
    print(f"Tempo total do teste:      {tempo_total:.2f}s")
    print(
        f"Vazao no envio (req/s):    {vazao_envio:.2f}   "
        "(velocidade de publicacao na fila, NAO e a vazao de processamento)"
    )

    # Vazao de PROCESSAMENTO: concluidas / tempo ate a ULTIMA concluir.
    ts_conclusoes = [r.ts_conclusao for r in resultados_poll if r.status != "timeout_poll" and r.ts_conclusao]
    if ts_conclusoes:
        ultimo_ts = max(ts_conclusoes)
        tempo_processamento = ultimo_ts - inicio_envio
        vazao_processamento = n_concluidas / tempo_processamento if tempo_processamento > 0 else float("nan")
        tempo_processamento_str = f"{tempo_processamento:.3f}"
        vazao_processamento_str = f"{vazao_processamento:.4f}"
    else:
        tempo_processamento_str = "n/a"
        vazao_processamento_str = "n/a"
        vazao_processamento = None

    print(f"Tempo de processamento:    {tempo_processamento_str}s (inicio do envio ate a ultima conclusao)")
    print(f"Vazao de processamento (req/s): {vazao_processamento_str}")

    # ---------------- Speedup / eficiencia ----------------
    speedup_str = "n/a"
    eficiencia_str = "n/a"
    vazao_baseline: Optional[float] = None

    if n_corretores != 1 and os.path.isfile(resumo_csv):
        with open(resumo_csv, newline="", encoding="utf-8") as f:
            leitor = csv.DictReader(f)
            for linha in leitor:
                if linha.get("corretores") == "1":
                    try:
                        vazao_baseline = float(linha["vazao_processamento_req_s"])
                    except (KeyError, ValueError):
                        pass  # mantem a ultima ocorrencia valida encontrada

    if n_corretores != 1 and vazao_baseline is None:
        print()
        print(f"Nao encontrei uma execucao com N=1 no {resumo_csv}.")
        try:
            entrada = input("Informe a vazao de processamento medida com N=1 (ou ENTER para pular): ").strip()
        except EOFError:
            entrada = ""
        if entrada:
            vazao_baseline = float(entrada)

    if vazao_processamento is not None and vazao_baseline:
        speedup = vazao_processamento / vazao_baseline
        eficiencia = speedup / n_corretores
        speedup_str = f"{speedup:.3f}"
        eficiencia_str = f"{eficiencia:.3f}"
        print(f"Speedup (N={n_corretores}):        {speedup_str}")
        print(f"Eficiencia (N={n_corretores}):      {eficiencia_str}")
    elif n_corretores == 1:
        print()
        print(">> Esta e a execucao com N=1: ela vira a baseline automatica para as proximas.")

    # ---------------- Persistencia (CSV + arquivo detalhado) ----------------
    novo_arquivo = not os.path.isfile(resumo_csv)
    with open(resumo_csv, "a", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        if novo_arquivo:
            escritor.writerow(
                [
                    "corretores",
                    "submissoes_concluidas",
                    "vazao_processamento_req_s",
                    "speedup",
                    "eficiencia",
                    "tempo_medio_s",
                ]
            )
        escritor.writerow(
            [
                n_corretores,
                n_concluidas,
                vazao_processamento_str,
                speedup_str,
                eficiencia_str,
                f"{tempo_medio:.3f}",
            ]
        )

    saida_final = f"resultado_teste_carga_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(saida_final, "w", encoding="utf-8") as f:
        for r in resultados_poll:
            tempo_str = f"{r.tempo_execucao_s:.3f}" if r.tempo_execucao_s is not None else "-"
            ts_str = f"{r.ts_conclusao:.3f}" if r.ts_conclusao is not None else "-"
            f.write(f"{r.id_submissao} {r.status} {r.veredito} {tempo_str} {ts_str}\n")

    print()
    print(f"Resultados detalhados salvos em: ./{saida_final}")
    print(f"Linha adicionada em:             {resumo_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())