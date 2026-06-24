# MC714 — Sistemas Distribuídos

Cluster distribuído em Python com troca real de mensagens TCP, implementando:

- **Relógio lógico de Lamport**
- **Exclusão mútua Ricart-Agrawala**
- **Eleição de líder Bully**

## Requisitos

- [Docker](https://docs.docker.com/get-docker/) e Docker Compose
- [uv](https://docs.astral.sh/uv/) (opcional, para desenvolvimento local)

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

Criar ambiente e rodar um nó manualmente:

```bash
uv sync
NODE_ID=0 NODE_COUNT=4 BASE_PORT=5000 \
  PEERS="localhost:5000,localhost:5001,localhost:5002,localhost:5003" \
  uv run python src/main.py
```

## Topologia

| Nó    | Hostname (Docker) | Porta TCP |
|-------|-------------------|-----------|
| node0 | node0             | 5000      |
| node1 | node1             | 5001      |
| node2 | node2             | 5002      |
| node3 | node3             | 5003      |

Conexões em malha completa: o nó de menor ID inicia conexão com IDs maiores (implementado no passo de transporte).

O volume `shared_data` é montado em `/app/shared` para o log da seção crítica (Ricart-Agrawala).

## Status da implementação

- [x] Estrutura do projeto, Docker e Compose
- [ ] Camada de transporte TCP e relógio de Lamport
- [ ] Ricart-Agrawala
- [ ] Bully
- [ ] Integração e scripts de demo