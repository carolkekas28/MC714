# MC714 — Sistemas Distribuídos

## Alunos

| Aluno | RA | 
| -------- | -------- | 
| Ana Carolina de Almeida Cardoso | 246914  | 
| Pedro Damasceno Vasconcellos | 260640  | 

## Objetivo

Cluster distribuído em Python com troca real de mensagens TCP, implementando:

- **Relógio lógico de Lamport**
- **Exclusão mútua Ricart-Agrawala**
- **Eleição de líder Bully**

## Requisitos

- [uv](https://docs.astral.sh/uv/) — desenvolvimento e execução local
- [Docker Desktop](https://docs.docker.com/get-docker/) — recomendado para a demonstração final (cluster em contêineres)

## Estrutura do projeto

```
MC714/
├── src/                  # código dos nós
├── docker/Dockerfile     # imagem do nó
├── docker-compose.yml    # cluster com 4 nós (node0..node3)
├── scripts/              # scripts de demonstração
└── report/               # relatório (entrega)
```

## Executar com Docker Compose

Subir o cluster com 4 nós:

```bash
docker compose up --build
```

Os nós sobem em sequência (`node0` → `node1` → `node2` → `node3`) com healthcheck de processo.

Ver logs de um nó específico:

```bash
docker compose logs -f node2
```

Parar o cluster:

```bash
docker compose down
```

## Desenvolvimento local (sem Docker)

Útil enquanto o Docker não está instalado ou para depuração rápida.

Subir os 4 nós de uma vez:

```bash
uv sync
chmod +x scripts/run_local_cluster.sh
./scripts/run_local_cluster.sh
```

Ver logs agregados:

```bash
./scripts/run_local_cluster.sh logs
```

Parar todos os nós:

```bash
./scripts/run_local_cluster.sh stop
```

Rodar um único nó manualmente:

```bash
NODE_ID=0 NODE_COUNT=4 BASE_PORT=8000 \
  PEERS="localhost:8000,localhost:8001,localhost:8002,localhost:8003" \
  uv run python src/main.py
```

## Topologia

| Nó    | Hostname (Docker) | Porta TCP |
|-------|-------------------|-----------|
| node0 | node0             | 8000      |
| node1 | node1             | 8001      |
| node2 | node2             | 8002      |
| node3 | node3             | 8003      |

> **Nota (macOS):** a porta 5000 costuma ser usada pelo AirPlay Receiver. Por isso o cluster usa a faixa **8000–8003**.

Conexões em malha completa: o nó de menor ID inicia conexão com IDs maiores (implementado no passo de transporte).

O volume `shared_data` é montado em `/app/shared` para o log da seção crítica (Ricart-Agrawala).

## Status da implementação

- [x] Estrutura do projeto, Docker e Compose
- [x] Camada de transporte TCP e relógio de Lamport
- [ ] Ricart-Agrawala
- [ ] Bully
- [ ] Integração e scripts de demo