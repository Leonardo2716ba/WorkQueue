#!/usr/bin/env python3
#
# Teste de carga para a fila de submissoes do opCoders Judge (ambiente de teste).
#
# Envia N submissoes concorrentes para a API, espera todas terminarem
# (polling) e imprime um resumo: distribuicao por veredito/status, tempo
# medio de execucao e vazao.
#
# Dependencias: apenas Python 3 (usa bibliotecas padrao: urllib, concurrent.futures)
#
# Uso:
#   python teste_carga.py [url_base] [total_requisicoes] [concorrencia] [timeout_poll_s]
#
# Exemplos:
#   python teste_carga.py
#   python teste_carga.py http://localhost:5000 100 20
#   python teste_carga.py http://localhost:5000 200 30 60
#

import argparse
import sys
import time
import json
import urllib.request
import urllib.error
import concurrent.futures
import datetime
from collections import Counter

# --------------------------------------------------------------------------
# Payloads de teste: mistura de casos "normais" e "extremos", para exercitar
# tanto o caminho feliz quanto timeout/erro/latencia do corretor.
# --------------------------------------------------------------------------

def payload_normal(i):
    return {
        "codigo": (
            "import random\n"
            "n = 1000\n"
            "vetor = list(range(1, n + 1))\n"
            "random.shuffle(vetor)\n"
            "trocas = 0\n"
            "for i in range(len(vetor)):\n"
            "    for j in range(len(vetor) - i - 1):\n"
            "        if vetor[j] > vetor[j + 1]:\n"
            "            vetor[j], vetor[j + 1] = vetor[j + 1], vetor[j]\n"
            "            trocas += 1\n"
            "print(f'ordenado n={n} trocas={trocas} ok={vetor == sorted(vetor)}')"
        ),
        "id_questao": "carga",
        "id_aluno": f"carga_{i}"
    }

def payload_lento(i):
    return {
        "codigo": f"import time\ntime.sleep(1)\nprint('lento {i}')",
        "id_questao": "carga",
        "id_aluno": f"carga_{i}"
    }

def payload_erro(i):
    return {
        "codigo": f"raise ValueError('erro proposital {i}')",
        "id_questao": "carga",
        "id_aluno": f"carga_{i}"
    }

def payload_loop_infinito(i):
    return {
        "codigo": "while True: pass",
        "id_questao": "carga",
        "id_aluno": f"carga_{i}"
    }

def payload_memoria(i):
    return {
        "codigo": (
            "x = []\n"
            "while True:\n"
            "    x.append(bytearray(10**6))\n"
            "    print(len(x), 'MB alocados')"
        ),
        "id_questao": "carga",
        "id_aluno": f"carga_{i}"
    }

def gerar_payload(i):
    return payload_normal(i)

def enviar_submissao(url_base, i):
    payload = gerar_payload(i)
    url = f"{url_base.rstrip('/')}/submissoes"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            status = response.status
            body_bytes = response.read()
            body_str = body_bytes.decode("utf-8")
            if status != 202:
                return f"ERRO_ENVIO {i} {status}"
            
            try:
                data = json.loads(body_str)
                sub_id = data.get("id")
                if sub_id:
                    return sub_id
                else:
                    return f"ERRO_PARSE {i}"
            except Exception:
                return f"ERRO_PARSE {i}"
    except urllib.error.HTTPError as e:
        return f"ERRO_ENVIO {i} {e.code}"
    except Exception as e:
        return f"ERRO_ENVIO {i} {str(e)}"

def consultar_id(url_base, sub_id, timeout_poll):
    url = f"{url_base.rstrip('/')}/submissoes/{sub_id}"
    for esperado in range(timeout_poll):
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    body = response.read().decode("utf-8")
                    data = json.loads(body)
                    status = data.get("status", "")
                    if status in ("concluido", "falha"):
                        veredito = data.get("veredito", "-")
                        tempo = data.get("tempo_execucao_s", "-")
                        return (sub_id, status, veredito, tempo)
        except Exception:
            pass  # Ignora erros temporarios de conexao ou da API
        time.sleep(1)
    return (sub_id, "timeout_poll", "-", "-")

def gerar_relatorio_markdown(resultados, total_enviadas, total_erros_envio, tempo_envio, tempo_poll, tempos_execucao, vereditos, status_list):
    tempo_total = tempo_envio + tempo_poll
    vazao = total_enviadas / tempo_envio if tempo_envio > 0 else 0.0
    
    total_total = total_enviadas + total_erros_envio
    taxa_sucesso_envio = (total_enviadas / total_total * 100) if total_total > 0 else 0.0
    
    p50 = p90 = p95 = p99 = min_t = max_t = avg_t = 0.0
    if tempos_execucao:
        sorted_t = sorted(tempos_execucao)
        n = len(sorted_t)
        min_t = sorted_t[0]
        max_t = sorted_t[-1]
        avg_t = sum(sorted_t) / n
        
        def pct(p):
            k = (n - 1) * (p / 100.0)
            idx = int(k)
            frac = k - idx
            if idx + 1 < n:
                return sorted_t[idx] + frac * (sorted_t[idx + 1] - sorted_t[idx])
            return sorted_t[idx]
            
        p50 = pct(50)
        p90 = pct(90)
        p95 = pct(95)
        p99 = pct(99)
        
    veredito_counts = Counter(vereditos)
    veredito_rows = []
    for val, count in veredito_counts.most_common():
        pct_val = (count / len(resultados) * 100) if resultados else 0.0
        veredito_rows.append(f"| `{val}` | **{count}** | {pct_val:.1f}% |")
        
    status_counts = Counter(status_list)
    status_rows = []
    for val, count in status_counts.most_common():
        pct_val = (count / len(resultados) * 100) if resultados else 0.0
        status_rows.append(f"| `{val}` | **{count}** | {pct_val:.1f}% |")

    details_rows = []
    for sub_id, status, veredito, tempo in resultados[:100]:
        details_rows.append(f"| `{sub_id}` | `{status}` | `{veredito}` | `{tempo}` |")
    if len(resultados) > 100:
        details_rows.append(f"| ... | ... | ... | ... |")
        details_rows.append(f"| *({len(resultados) - 100} mais submissoes ocultadas)* | | | |")
        
    md = f"""# Relatório de Teste de Carga - opCoders Judge

Gerado em: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 📊 Métricas Gerais

| Métrica | Valor | Descrição |
| :--- | :--- | :--- |
| **Total de Submissões** | {total_total} | Total de requisições solicitadas no teste |
| **Sucesso no Envio (HTTP 202)** | {total_enviadas} ({taxa_sucesso_envio:.1f}%) | Submissões aceitas com sucesso pela fila |
| **Falhas de Envio** | {total_erros_envio} | Falhas HTTP ou erros de conexão/parse durante o envio |
| **Tempo de Envio** | {tempo_envio:.2f}s | Tempo gasto para despachar todas as requisições |
| **Tempo de Polling** | {tempo_poll:.2f}s | Tempo gasto aguardando o processamento terminar |
| **Tempo Total do Teste** | {tempo_total:.2f}s | Duração total do ciclo (Envio + Polling) |
| **Vazão de Envio** | {vazao:.2f} req/s | Taxa de envio de requisições por segundo |

---

## ⏱️ Tempos de Execução (Apenas Concluídas)

"""
    if tempos_execucao:
        md += f"""| Métrica / Percentil | Tempo (s) |
| :--- | :--- |
| **Mínimo** | {min_t:.3f}s |
| **Média (Avg)** | {avg_t:.3f}s |
| **Mediana (p50)** | {p50:.3f}s |
| **p90** | {p90:.3f}s |
| **p95** | {p95:.3f}s |
| **p99** | {p99:.3f}s |
| **Máximo** | {max_t:.3f}s |
| **Total de Amostras (n)** | {len(tempos_execucao)} |
"""
    else:
        md += "*Nenhum caso concluído com sucesso para extração de tempos.*\n"

    md += f"""
---

## 🏆 Distribuição por Veredito

| Veredito | Quantidade | Percentual |
| :--- | :--- | :--- |
"""
    if veredito_rows:
        md += "\n".join(veredito_rows) + "\n"
    else:
        md += "| - | - | - |\n"

    md += f"""
---

## 🔄 Distribuição por Status Final

| Status | Quantidade | Percentual |
| :--- | :--- | :--- |
"""
    if status_rows:
        md += "\n".join(status_rows) + "\n"
    else:
        md += "| - | - | - |\n"

    md += f"""
---

## 📝 Detalhes das Submissões (Amostra - Primeiras 100)

| ID Submissão | Status | Veredito | Tempo Execução |
| :--- | :--- | :--- | :--- |
"""
    if details_rows:
        md += "\n".join(details_rows) + "\n"
    else:
        md += "| - | - | - | - |\n"

    return md


def main():
    parser = argparse.ArgumentParser(
        description="Teste de carga para a fila de submissoes do opCoders Judge (ambiente de teste)."
    )
    parser.add_argument("url_base", nargs="?", default="http://localhost:5000", help="URL base da API (default: http://localhost:5000)")
    parser.add_argument("total", nargs="?", type=int, default=10000, help="Total de submissões a enviar (default: 50)")
    parser.add_argument("concorrencia", nargs="?", type=int, default=50, help="Número de threads simultâneas (default: 10)")
    parser.add_argument("timeout_poll", nargs="?", type=int, default=30, help="Tempo máximo de polling em segundos por submissão (default: 30)")

    args = parser.parse_args()

    url_base = args.url_base
    total = args.total
    concorrencia = args.concorrencia
    timeout_poll = args.timeout_poll

    print("== Teste de carga - opCoders Judge ==")
    print(f"URL base:      {url_base}")
    print(f"Requisicoes:   {total}")
    print(f"Concorrencia:  {concorrencia}")
    print(f"Timeout poll:  {timeout_poll}s por submissao")
    print()

    # Health check
    health_url = f"{url_base.rstrip('/')}/health"
    try:
        # Aumentamos timeout do health check para 5 segundos
        with urllib.request.urlopen(health_url, timeout=5) as response:
            if response.status != 200:
                raise Exception(f"HTTP status {response.status}")
    except Exception as e:
        print(f"Erro: API nao respondeu em {health_url}. Ela esta no ar?")
        sys.exit(1)

    print(f"Enviando {total} submissoes (concorrencia {concorrencia})...")
    inicio_envio = time.time()

    ids_resultados = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concorrencia) as executor:
        futures = {executor.submit(enviar_submissao, url_base, i): i for i in range(1, total + 1)}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            ids_resultados.append(res)

    fim_envio = time.time()
    tempo_envio = fim_envio - inicio_envio

    total_enviadas = 0
    total_erros_envio = 0
    ids_com_sucesso = []

    for item in ids_resultados:
        if item.startswith("ERRO"):
            total_erros_envio += 1
        else:
            ids_com_sucesso.append(item)
            total_enviadas += 1

    print(f"Envio concluido em {tempo_envio:.2f}s")
    print(f"  OK: {total_enviadas}   Falhas de envio: {total_erros_envio}")
    print()

    if total_enviadas == 0:
        print("Nenhuma submissao foi enviada com sucesso. Abortando.")
        sys.exit(1)

    print(f"Aguardando processamento (polling a cada 1s, timeout {timeout_poll}s por submissao)...")
    inicio_poll = time.time()

    resultados = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=concorrencia) as executor:
        futures = {executor.submit(consultar_id, url_base, sub_id, timeout_poll): sub_id for sub_id in ids_com_sucesso}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            resultados.append(res)

    fim_poll = time.time()
    tempo_poll = fim_poll - inicio_poll

    print(f"Polling concluido em {tempo_poll:.2f}s")
    print()

    # Estatisticas
    vereditos = []
    status_list = []
    tempos_execucao = []

    for sub_id, status, veredito, tempo in resultados:
        vereditos.append(veredito)
        status_list.append(status)
        if tempo != "-":
            try:
                tempos_execucao.append(float(tempo))
            except ValueError:
                pass

    print("== Resumo ==")
    print(f"Total enviado:        {total_enviadas}")
    print(f"Falhas no envio:      {total_erros_envio}")
    print()

    tempo_total = tempo_envio + tempo_poll
    vazao = total_enviadas / tempo_envio if tempo_envio > 0 else 0.0
    avg_time_str = "n/a"
    if tempos_execucao:
        avg_time = sum(tempos_execucao) / len(tempos_execucao)
        avg_time_str = f"{avg_time:.3f}s (n={len(tempos_execucao)})"

    veredito_counts = Counter(vereditos)
    veredito_lines = []
    for val, count in veredito_counts.most_common():
        veredito_lines.append(f"   {count} {val}")
    veredito_dist = "\n".join(veredito_lines)

    status_counts = Counter(status_list)
    status_lines = []
    for val, count in status_counts.most_common():
        status_lines.append(f"   {count} {val}")
    status_dist = "\n".join(status_lines)

    resumo = (
        "== Resumo ==\n"
        f"Total enviado:        {total_enviadas}\n"
        f"Falhas no envio:      {total_erros_envio}\n\n"
        "Distribuicao por veredito:\n"
        f"{veredito_dist}\n\n"
        "Distribuicao por status:\n"
        f"{status_dist}\n\n"
        "Tempo medio de execucao (apenas concluidas):\n"
        f"  {avg_time_str}\n\n"
        f"Tempo total do teste:      {tempo_total:.2f}s\n"
        f"Vazao no envio (req/s):    {vazao:.2f}\n"
    )

    print(resumo)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    saida_final = f"resultado_teste_carga_{timestamp}.txt"
    saida_stats = f"estatisticas_teste_carga_{timestamp}.txt"
    saida_relatorio = f"relatorio_teste_carga_{timestamp}.md"

    with open(saida_final, "w", encoding="utf-8") as f:
        for sub_id, status, veredito, tempo in resultados:
            f.write(f"{sub_id} {status} {veredito} {tempo}\n")

    with open(saida_stats, "w", encoding="utf-8") as f:
        f.write(resumo)

    relatorio_md = gerar_relatorio_markdown(
        resultados, total_enviadas, total_erros_envio, tempo_envio, tempo_poll, tempos_execucao, vereditos, status_list
    )
    with open(saida_relatorio, "w", encoding="utf-8") as f:
        f.write(relatorio_md)

    print(f"Resultados detalhados salvos em: ./{saida_final}")
    print(f"Estatisticas do teste salvas em: ./{saida_stats}")
    print(f"Relatorio formatado (Markdown) salvo em: ./{saida_relatorio}")

if __name__ == "__main__":
    main()