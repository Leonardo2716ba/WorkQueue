#!/usr/bin/env bash
#
# Teste de carga para a fila de submissoes do opCoders Judge (ambiente de teste).
#
# Envia N submissoes concorrentes para a API, espera todas terminarem
# (polling) e imprime um resumo: distribuicao por veredito/status, tempo
# medio de execucao e vazao.
#
# Dependencias: curl, python3, xargs, awk (padrao em qualquer distro Linux)
#
# Uso:
#   ./teste_carga.sh [url_base] [total_requisicoes] [concorrencia] [timeout_poll_s]
#
# Exemplos:
#   ./teste_carga.sh
#   ./teste_carga.sh http://localhost:5000 100 20
#   ./teste_carga.sh http://localhost:5000 200 30 60
#
set -euo pipefail

URL_BASE="${1:-http://localhost:5000}"
TOTAL="${2:-50}"
CONCORRENCIA="${3:-10}"
TIMEOUT_POLL="${4:-30}"   # segundos maximos esperando cada submissao terminar

TMP_DIR=$(mktemp -d)
IDS_FILE="$TMP_DIR/ids.txt"
RESULTADOS_FILE="$TMP_DIR/resultados.txt"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "== Teste de carga - opCoders Judge =="
echo "URL base:      $URL_BASE"
echo "Requisicoes:   $TOTAL"
echo "Concorrencia:  $CONCORRENCIA"
echo "Timeout poll:  ${TIMEOUT_POLL}s por submissao"
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
  printf '{"codigo": "import random\\nn = 1000\\nvetor = list(range(n, 0, -1))\\ntrocas = 0\\nfor i in range(len(vetor)):\\n    for j in range(len(vetor) - i - 1):\\n        if vetor[j] > vetor[j + 1]:\\n            vetor[j], vetor[j + 1] = vetor[j + 1], vetor[j]\\n            trocas += 1\\nprint(f%s)", "id_questao": "carga", "id_aluno": "carga_%s"}' \
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
  local i=$1
  local resto=$(( i % 10 ))
  if   [ "$resto" -eq 0 ]; then payload_loop_infinito "$i"
  elif [ "$resto" -eq 1 ]; then payload_memoria "$i"
  elif [ "$resto" -le 3 ]; then payload_erro "$i"
  elif [ "$resto" -le 5 ]; then payload_lento "$i"
  else payload_normal "$i"
  fi
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
  local body status veredito tempo
  while [ "$esperado" -lt "$TIMEOUT_POLL" ]; do
    body=$(curl -s "$URL_BASE/submissoes/$id")
    status=$(echo "$body" | python3 -c "import sys, json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
    if [ "$status" = "concluido" ] || [ "$status" = "falha" ]; then
      veredito=$(echo "$body" | python3 -c "import sys, json; print(json.load(sys.stdin).get('veredito','-'))" 2>/dev/null || echo "-")
      tempo=$(echo "$body" | python3 -c "import sys, json; print(json.load(sys.stdin).get('tempo_execucao_s','-'))" 2>/dev/null || echo "-")
      echo "$id $status $veredito $tempo" >> "$RESULTADOS_FILE"
      return
    fi
    sleep 1
    esperado=$((esperado + 1))
  done
  echo "$id timeout_poll - -" >> "$RESULTADOS_FILE"
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
echo "Tempo medio de execucao (apenas concluidas):"
awk '$4 != "-" {sum+=$4; n++} END {if (n>0) printf "  %.3fs (n=%d)\n", sum/n, n; else print "  n/a"}' "$RESULTADOS_FILE"

echo
tempo_total=$(python3 -c "print(f'{$tempo_envio + $tempo_poll:.2f}')")
vazao=$(python3 -c "print(f'{$total_enviadas / $tempo_envio:.2f}')" 2>/dev/null || echo "n/a")
echo "Tempo total do teste:      ${tempo_total}s"
echo "Vazao no envio (req/s):    ${vazao}"

saida_final="resultado_teste_carga_$(date +%Y%m%d_%H%M%S).txt"
cp "$RESULTADOS_FILE" "./$saida_final"
echo
echo "Resultados detalhados salvos em: ./$saida_final"
