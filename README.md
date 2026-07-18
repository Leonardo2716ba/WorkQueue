# Ambiente de teste — workqueue opCoders Judge (sem MongoDB)

Ambiente mínimo com **3 componentes**, seguindo o fluxo do seu TCC, mas sem a etapa de
banco de dados de questões (MongoDB entra depois):

- **rabbitmq** — broker de mensagens (fila `submissoes`)
- **flask-api** — recebe submissões (`POST /submissoes`) e publica na fila; consulta
  resultados (`GET /submissoes/<id>`)
- **corretor** — consome a fila com ACK manual, executa o código em um container
  Docker isolado (limite de memória/CPU, sem rede) e grava o resultado

## O que está mockado (por não ter Mongo ainda)

- Não há casos de teste reais sendo comparados — o corretor apenas roda o código
  enviado e captura `stdout`/`stderr`/`exit code`.
- Os resultados são persistidos como arquivos `.json` num **volume Docker
  compartilhado** (`resultados`), montado tanto na API quanto no corretor. Isso
  imita o contrato de leitura/escrita que o MongoDB vai assumir depois — trocar
  essa camada por chamadas ao Mongo deve ser localizado (só nas funções
  `salvar_resultado_inicial` / `ler_resultado` / `atualizar_resultado`).

## Como rodar

```bash
cd workqueue-test
docker compose up --build
```

Isso sobe RabbitMQ (com painel em http://localhost:15672, usuário/senha
`guest`/`guest`), a API em `http://localhost:5000` e um corretor.

Para simular múltiplos corretores concorrentes (como no diagrama do TCC):

```bash
docker compose up --build --scale corretor=3

./teste_carga.sh http://localhost:5000 800 50
docker compose exec flask-api sh -c "rm -f /data/results/*.json"

docker compose kill -s SIGKILL corretor
docker compose kill --index=2 -s SIGKILL corretor

docker compose up -d corretor

```

## Testando o fluxo

**1. Enviar uma submissão:**

```bash
curl -X POST http://localhost:5000/submissoes \
  -H "Content-Type: application/json" \
  -d '{
    "codigo": "print(\"ola mundo\")",
    "id_questao": "q1",
    "id_aluno": "aluno123"
  }'
```

```bash
curl -X POST http://localhost:5000/submissoes \
  -H "Content-Type: application/json" \
  -d '{
    "codigo": "import random\nn = 1000\nvetor = list(range(n, 0, -1))\ntrocas = 0\nfor i in range(len(vetor)):\n    for j in range(len(vetor) - i - 1):\n        if vetor[j] > vetor[j + 1]:\n            vetor[j], vetor[j + 1] = vetor[j + 1], vetor[j]\n            trocas += 1\nprint(f\"ordenado n={n} trocas={trocas} ok={vetor == sorted(vetor)}\")",
    "id_questao": "q1",
    "id_aluno": "aluno123"
  }'
```

Resposta (`202 Accepted`):

```json
{ "id": "3f9a...", "status": "pendente" }
```

**2. Consultar o resultado (use o `id` retornado acima):**

```bash
curl http://localhost:5000/submissoes/3f9a...
```

O status evolui: `pendente` → `em_execucao` → `concluido` (ou `falha` /
`tempo_limite_excedido` / `erro_infraestrutura`).

**3. Testar o caso de falha (loop infinito, deve estourar o timeout):**

```bash
curl -X POST http://localhost:5000/submissoes \
  -H "Content-Type: application/json" \
  -d '{
    "codigo": "while True: pass",
    "id_questao": "q1",
    "id_aluno": "aluno123"
  }'
```

Depois de `TIMEOUT_SEGUNDOS` (padrão 5s), o resultado deve vir com
`"veredito": "tempo_limite_excedido"`.

## Observações técnicas

- O corretor monta `/var/run/docker.sock` do host, então ele cria containers
  **irmãos** (sibling containers) via `docker-py`, sem precisar de
  Docker-in-Docker.
- Ele usa `python:3.11-slim` para rodar o código do aluno — na primeira
  execução o Docker vai baixar essa imagem, o que pode levar alguns segundos.
- Limites configuráveis via variáveis de ambiente no `docker-compose.yml`:
  `TIMEOUT_SEGUNDOS`, `MEM_LIMIT`, `CPU_QUOTA`.
- Consumo com ACK manual (`prefetch_count=1`): se o corretor cair no meio do
  processamento, a mensagem não é confirmada e volta para a fila — igual ao
  que está descrito na Seção 3.1 do seu texto.

## Próximo passo (quando for integrar o Mongo)

Trocar as funções de leitura/escrita de resultado por chamadas ao MongoDB, e
fazer o corretor buscar os casos de teste/critérios de aceitação da questão
antes de rodar o código do aluno (hoje ele só executa e devolve a saída bruta).
