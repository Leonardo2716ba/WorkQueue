#!/usr/bin/env bash
#
# Teste de carga para a fila de submissoes do opCoders Judge (ambiente de teste).
#
# Envia N submissoes concorrentes para a API, espera todas terminarem
# (polling) e imprime um resumo: distribuicao por veredito/status, tempo
# medio de execucao, vazao de processamento e (se aplicavel) speedup/eficiencia.
#
# Dependencias: curl, python3, xargs, awk (padrao em qualquer distro Linux)
#
# Uso interativo (recomendado):
#   ./teste_carga.sh
#   -> o script pergunta o que precisar (URL, total, concorrencia, timeout,
#      numero de corretores) e usa valores padrao se voce so apertar ENTER.
#
# Uso nao interativo (para automatizar/scriptar):
#   ./teste_carga.sh [url_base] [total_requisicoes] [concorrencia] [timeout_poll_s] [n_corretores]
#
set -euo pipefail

# --------------------------------------------------------------------------
# Helper: pergunta com valor padrao. So pergunta se o argumento posicional
# correspondente nao foi passado na chamada do script.
# --------------------------------------------------------------------------
perguntar() {
  local prompt="$1" default="$2" valor_arg="${3:-}"
  if [ -n "$valor_arg" ]; then
    echo "$valor_arg"
    return
  fi
  local resposta
  read -r -p "$prompt [$default]: " resposta </dev/tty
  echo "${resposta:-$default}"
}

URL_BASE=$(perguntar "URL base da API" "http://localhost:5000" "${1:-}")
TOTAL=$(perguntar "Total de requisicoes" "1000" "${2:-}")
CONCORRENCIA=$(perguntar "Concorrencia de envio" "20" "${3:-}")
TIMEOUT_POLL=$(perguntar "Timeout de polling (s)" "30" "${4:-}")

# --------------------------------------------------------------------------
# Numero de corretores: tenta detectar via docker compose; se nao conseguir,
# ou se o valor detectado parecer errado, pergunta e deixa o usuario confirmar.
# --------------------------------------------------------------------------
N_DETECTADO=""
if command -v docker &> /dev/null && docker compose ps -q corretor &> /dev/null; then
  N_DETECTADO=$(docker compose ps -q corretor 2>/dev/null | wc -l | tr -d ' ')
fi

if [ -n "${5:-}" ]; then
  N_CORRETORES="$5"
elif [ -n "$N_DETECTADO" ] && [ "$N_DETECTADO" -gt 0 ]; then
  N_CORRETORES=$(perguntar "Corretores ativos (detectado via docker compose)" "$N_DETECTADO" "")
else
  N_CORRETORES=$(perguntar "Quantos corretores estao ativos nesta execucao?" "1" "")
fi

TMP_DIR=$(mktemp -d)
IDS_FILE="$TMP_DIR/ids.txt"
RESULTADOS_FILE="$TMP_DIR/resultados.txt"
RESUMO_CSV="./resumo_testes_carga.csv"
trap 'rm -rf "$TMP_DIR"' EXIT

echo
echo "== Teste de carga - opCoders Judge =="
echo "URL base:      $URL_BASE"
echo "Requisicoes:   $TOTAL"
echo "Concorrencia:  $CONCORRENCIA"
echo "Timeout poll:  ${TIMEOUT_POLL}s por submissao"
echo "Corretores:    $N_CORRETORES"
echo

if ! curl -sf "$URL_BASE/health" > /dev/null; then
  echo "Erro: API nao respondeu em $URL_BASE/health. Ela esta no ar?"
  exit 1
fi

# --------------------------------------------------------------------------
# Payloads de teste: mistura de casos "normais" e "extremos", para exercitar
# tanto o caminho feliz quanto timeout/erro/latencia do corretor.
# --------------------------------------------------------------------------

payload_normal() {
  printf '{"codigo": "import random\nn = 1000\nvetor = [random.randint(1, 1000) for _ in range(n)]\ntrocas = 0\nfor i in range(len(vetor)):\n    for j in range(len(vetor) - i - 1):\n        if vetor[j] > vetor[j + 1]:\n            vetor[j], vetor[j + 1] = vetor[j + 1], vetor[j]\n            trocas += 1\nprint(f%s)", "id_questao": "carga", "id_aluno": "carga_%s"}' \
    "'ordenado n={n} trocas={trocas} ok={vetor == sorted(vetor)}'" "$1"
}

payload_lento() {
  printf '{"codigo": "import time\\ntime.sleep(1)\\nprint(%s)", "id_questao": "carga", "id_aluno": "carga_%s"}' \
    "'lento $1'" "$1"
}

payload_erro() {
  printf '{"codigo": "raise ValueError(%s)", "id_questao": "carga", "id_aluno": "carga_%s"}' \
    "'erro proposital $1'" "$1"
}

payload_loop_infinito() {
  printf '{"codigo": "while True: pass", "id_questao": "carga", "id_aluno": "carga_%s"}' "$1"
}

payload_memoria() {
  printf '{"codigo": "x = []\\nwhile True:\\n    x.append(bytearray(10**6))\\n    print(len(x), %s)", "id_questao": "carga", "id_aluno": "carga_%s"}' \
    "'MB alocados'" "$1"
}

# distribuicao aproximada a cada 10 submissoes:
#   1x loop infinito | 1x estoura memoria | 2x erro | 2x lento | 4x normal
gerar_payload() {
  payload_normal "$1"
}

enviar_submissao() {
  local i=$1
  local payload
  payload=$(gerar_payload "$i")

  local resposta http_code body id
  resposta=$(curl -s -w "\n%{http_code}" -X POST "$URL_BASE/submissoes" \
    -H "Content-Type: application/json" \
    -d "$payload")

  http_code=$(echo "$resposta" | tail -n1)
  body=$(echo "$resposta" | sed '$d')

  if [ "$http_code" != "202" ]; then
    echo "ERRO_ENVIO $i $http_code" >> "$IDS_FILE"
    return
  fi

  id=$(echo "$body" | python3 -c "import sys, json; print(json.load(sys.stdin)['id'])" 2>/dev/null || echo "")

  if [ -z "$id" ]; then
    echo "ERRO_PARSE $i" >> "$IDS_FILE"
  else
    echo "$id" >> "$IDS_FILE"
  fi
}

export -f enviar_submissao gerar_payload payload_normal payload_lento payload_erro payload_loop_infinito payload_memoria
export URL_BASE IDS_FILE

echo "Enviando $TOTAL submissoes (concorrencia $CONCORRENCIA)..."
inicio_envio=$(date +%s.%N)

seq 1 "$TOTAL" | xargs -P "$CONCORRENCIA" -I{} bash -c 'enviar_submissao "$@"' _ {}

fim_envio=$(date +%s.%N)
tempo_envio=$(python3 -c "print(f'{$fim_envio - $inicio_envio:.2f}')")

total_enviadas=$(grep -vc "^ERRO" "$IDS_FILE" || true)
total_erros_envio=$(grep -c "^ERRO" "$IDS_FILE" || true)

echo "Envio concluido em ${tempo_envio}s"
echo "  OK: $total_enviadas   Falhas de envio: $total_erros_envio"
echo

if [ "$total_enviadas" -eq 0 ]; then
  echo "Nenhuma submissao foi enviada com sucesso. Abortando."
  exit 1
fi

echo "Aguardando processamento (polling a cada 1s, timeout ${TIMEOUT_POLL}s por submissao)..."
inicio_poll=$(date +%s.%N)

consultar_id() {
  local id=$1
  local esperado=0
  local body status veredito tempo ts_conclusao
  while [ "$esperado" -lt "$TIMEOUT_POLL" ]; do
    body=$(curl -s "$URL_BASE/submissoes/$id")
    status=$(echo "$body" | python3 -c "import sys, json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
    if [ "$status" = "concluido" ] || [ "$status" = "falha" ]; then
      veredito=$(echo "$body" | python3 -c "import sys, json; print(json.load(sys.stdin).get('veredito','-'))" 2>/dev/null || echo "-")
      tempo=$(echo "$body" | python3 -c "import sys, json; print(json.load(sys.stdin).get('tempo_execucao_s','-'))" 2>/dev/null || echo "-")
      ts_conclusao=$(date +%s.%N)
      echo "$id $status $veredito $tempo $ts_conclusao" >> "$RESULTADOS_FILE"
      return
    fi
    sleep 1
    esperado=$((esperado + 1))
  done
  echo "$id timeout_poll - - -" >> "$RESULTADOS_FILE"
}

export -f consultar_id
export URL_BASE TIMEOUT_POLL RESULTADOS_FILE

grep -v "^ERRO" "$IDS_FILE" | xargs -P "$CONCORRENCIA" -I{} bash -c 'consultar_id "$@"' _ {}

fim_poll=$(date +%s.%N)
tempo_poll=$(python3 -c "print(f'{$fim_poll - $inicio_poll:.2f}')")

echo "Polling concluido em ${tempo_poll}s"
echo

echo "== Resumo =="
echo "Total enviado:        $total_enviadas"
echo "Falhas no envio:      $total_erros_envio"
echo

echo "Distribuicao por veredito:"
awk '{print $3}' "$RESULTADOS_FILE" | sort | uniq -c | sort -rn

echo
echo "Distribuicao por status:"
awk '{print $2}' "$RESULTADOS_FILE" | sort | uniq -c | sort -rn

echo
tempo_medio=$(awk '$4 != "-" {sum+=$4; n++} END {if (n>0) printf "%.3f", sum/n; else print "0"}' "$RESULTADOS_FILE")
n_concluidas=$(awk '$4 != "-" {n++} END {print n+0}' "$RESULTADOS_FILE")
echo "Tempo medio de execucao (apenas concluidas):"
echo "  ${tempo_medio}s (n=$n_concluidas)"

echo
tempo_total=$(python3 -c "print(f'{$tempo_envio + $tempo_poll:.2f}')")
vazao_envio=$(python3 -c "print(f'{$total_enviadas / $tempo_envio:.2f}')" 2>/dev/null || echo "n/a")
echo "Tempo total do teste:      ${tempo_total}s"
echo "Vazao no envio (req/s):    ${vazao_envio}   (velocidade de publicacao na fila, NAO e a vazao de processamento)"

# --------------------------------------------------------------------------
# Vazao de PROCESSAMENTO: submissoes concluidas / tempo ate a ULTIMA concluir.
# --------------------------------------------------------------------------
ultimo_ts=$(awk '$2 != "timeout_poll" {print $5}' "$RESULTADOS_FILE" | sort -n | tail -1)

if [ -n "$ultimo_ts" ]; then
  tempo_processamento=$(python3 -c "print(f'{$ultimo_ts - $inicio_envio:.3f}')")
  vazao_processamento=$(python3 -c "print(f'{$n_concluidas / $tempo_processamento:.4f}')")
else
  tempo_processamento="n/a"
  vazao_processamento="n/a"
fi

echo "Tempo de processamento:    ${tempo_processamento}s (inicio do envio ate a ultima conclusao)"
echo "Vazao de processamento (req/s): ${vazao_processamento}"

# --------------------------------------------------------------------------
# Speedup e eficiencia: busca a baseline (N=1) automaticamente no
# resumo_testes_carga.csv. So pergunta se nao achar e N > 1.
# --------------------------------------------------------------------------
speedup="n/a"
eficiencia="n/a"
VAZAO_BASELINE=""

if [ "$N_CORRETORES" != "1" ] && [ -f "$RESUMO_CSV" ]; then
  VAZAO_BASELINE=$(awk -F',' '$1 == "1" {print $3}' "$RESUMO_CSV" | tail -1)
fi

if [ "$N_CORRETORES" != "1" ] && [ -z "$VAZAO_BASELINE" ]; then
  echo
  echo "Nao encontrei uma execucao com N=1 no $RESUMO_CSV."
  read -r -p "Informe a vazao de processamento medida com N=1 (ou ENTER para pular): " VAZAO_BASELINE </dev/tty
fi

if [ "$vazao_processamento" != "n/a" ] && [ -n "$VAZAO_BASELINE" ]; then
  speedup=$(python3 -c "print(f'{$vazao_processamento / $VAZAO_BASELINE:.3f}')")
  eficiencia=$(python3 -c "print(f'{($vazao_processamento / $VAZAO_BASELINE) / $N_CORRETORES:.3f}')")
  echo "Speedup (N=$N_CORRETORES):        ${speedup}"
  echo "Eficiencia (N=$N_CORRETORES):      ${eficiencia}"
elif [ "$N_CORRETORES" = "1" ]; then
  echo
  echo ">> Esta e a execucao com N=1: ela vira a baseline automatica para as proximas."
fi

# --------------------------------------------------------------------------
# Acumula uma linha em resumo_testes_carga.csv (usado tanto para o calculo
# automatico da baseline quanto para montar a Tabela do Cap. 4).
# --------------------------------------------------------------------------
if [ ! -f "$RESUMO_CSV" ]; then
  echo "corretores,submissoes_concluidas,vazao_processamento_req_s,speedup,eficiencia,tempo_medio_s" > "$RESUMO_CSV"
fi
echo "${N_CORRETORES},${n_concluidas},${vazao_processamento},${speedup},${eficiencia},${tempo_medio}" >> "$RESUMO_CSV"

saida_final="resultado_teste_carga_$(date +%Y%m%d_%H%M%S).txt"
cp "$RESULTADOS_FILE" "./$saida_final"
echo
echo "Resultados detalhados salvos em: ./$saida_final"
echo "Linha adicionada em:             $RESUMO_CSV"