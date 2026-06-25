# MC714 (Sistemas Distribuídos) - Trabalho 2

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

## Arquitetura

### Diagrama 1 — O cluster como um todo

Quatro nós rodam em containers Docker separados e se comunicam via **sockets TCP**. A pasta `./shared` do projeto é montada como volume em todos os containers — não é usada para comunicação entre nós, apenas para expor o log da seção crítica e receber comandos do script `command.sh`.

```
┌────────────────────────── Máquina hospedeira ──────────────────────────┐
│                                                                        │
│  ./shared/                    ┌─── Docker Compose ───────────────────┐ │
│  ├── critical.log  ◄──────────┤  (volume: pasta espelhada nos 4 nós) │ │
│  └── commands/     ──────────►│                                      │ │
│                               │  ┌─────────┐     ┌─────────┐         │ │
│                               │  │  node0  │─────│  node1  │         │ │
│                               │  │  :8000  │╲   ╱│  :8001  │         │ │
│                               │  └─────────┘ ╲ ╱ └─────────┘         │ │
│                               │               ╳                      │ │
│                               │  ┌─────────┐ ╱ ╲ ┌─────────┐         │ │
│                               │  │  node2  │╱   ╲│  node3  │         │ │
│                               │  │  :8002  │─────│  :8003  │         │ │
│                               │  └─────────┘     └─────────┘         │ │
│                               │                                      │ │
│                               │  Todos os pares conectados via TCP   │ │
│                               └──────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────┘
```

Cada linha entre nós é **uma conexão TCP bidirecional**. O nó de **menor ID sempre inicia** a conexão (evita duplicatas). Exemplo: `node0` conecta em `node1`, `node2` e `node3`; `node2` conecta apenas em `node3`.

---

### Diagrama 2 — O que roda dentro de cada nó

Todos os quatro containers são idênticos. Dentro de cada um há um único processo Python estruturado assim:

```
                         ┌──────── Container nodeN ────--────┐
                         │                                   │
                         │   main.py ──► Node (orquestrador) │
                         │                    │              │
                         │         ┌──────────┼─────────┐    │
                         │         │          │         │    │
                         │   ┌─────▼──┐ ┌────▼───┐ ┌────▼──┐ │
                         │   │Lamport │ │Ricart- │ │ Bully │ │
                         │   │ Clock  │ │Agrawala│ │       │ │
                         │   └───┬────┘ └────┬───┘ └───┬───┘ │
                         │       │           │         │     │  
                         │       └─────┬─────┘         │     │ 
                         │             │               │     │ 
                         │      ┌──────▼───────────────▼-──┐ │  
                         │      │    Transporte TCP        │ │  
                         │      │  (envia e recebe JSON)   │ │  
                         │      └──────────────┬───────────┘ │  
                         │                     │             │  
                         └─────────────────────┼─────────────┘  
                                               │
                              ◄────────────────▼────────────────►
                                  mensagens TCP para/de outros nós
```

**Fluxo de uma mensagem recebida:**
1. `Transporte TCP` recebe os bytes, monta o JSON
2. Atualiza o `LamportClock` (`max(local, msg.ts) + 1`)
3. Roteia pela tipo: `REQUEST/REPLY` → Ricart-Agrawala; `ELECTION/OK/COORDINATOR/HEARTBEAT` → Bully

**Fluxo de uma mensagem enviada:**
1. Algoritmo chama `transport.send(msg)`
2. `LamportClock` incrementa e carimba o timestamp na mensagem
3. `Transporte TCP` serializa e envia pelo socket

---

### Diagrama 3 — Sequência: exclusão mútua (Ricart-Agrawala)

```
node0 quer entrar na seção crítica:

  node0           node1           node2           node3
    │                │               │               │
    │──── REQUEST ──►│               │               │
    │──── REQUEST ──────────────────►│               │
    │──── REQUEST ──────────────────────────────────►│
    │                │               │               │
    │◄─── REPLY ─────│               │               │   (node1 concede)
    │◄─── REPLY ─────────────────────│               │   (node2 concede)
    │◄─── REPLY ─────────────────────────────────────│   (node3 concede)
    │                │               │               │
  [entra na seção crítica]           │               │
  [escreve em critical.log]          │               │
    │                │               │               │
  [sai]              │               │               │
    │──── REPLY ────►│  (libera node1 se estava esperando)
    │──── REPLY ─────────────────────►  (libera node2 ...)
    │──── REPLY ──────────────────────────────────────►
```

---

### Diagrama 4 — Sequência: eleição Bully

Cenário: o líder (`node3`, maior ID) para de responder. `node0` detecta a falha e inicia eleição.

```
  node0           node1           node2           node3
    │                │               │           [morto]
    │                │               │               │
    │  (timeout do heartbeat de node3)               │
    │                │               │               │
    │──── ELECTION ─►│               │               │
    │──── ELECTION ──────────────────►               │
    │                │               │               │
    │◄─── OK ────────│  (node1 tem ID maior, assume) │
    │◄─── OK ─────────────────────────  (node2 também)
    │                │               │               │
    │                │──── ELECTION ─►               │
    │                │◄─── OK ────────               │
    │                │               │               │
    │                │               │               │
    │                │   (node2 não recebeu OK de ninguém acima)
    │                │               │               │
    │◄─── COORDINATOR────────────────│  node2 anuncia vitória
    │────────────────◄───COORDINATOR─│
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

### Demo de exclusão mútua (Ricart-Agrawala)

```bash
chmod +x scripts/demo_mutex.sh
./scripts/demo_mutex.sh local 45    # cluster local
./scripts/demo_mutex.sh docker 45   # via Docker
```

O log compartilhado da seção crítica fica em `shared/critical.log` (volume `/app/shared` no Docker).

### Demo de eleição de líder (Bully)

```bash
chmod +x scripts/demo_election.sh
./scripts/demo_election.sh docker    # para o líder (node3) e observa reeleição
./scripts/demo_election.sh local     # equivalente com processos locais
```

Para rodar só eleição (sem demo de mutex):

```bash
RUN_MUTEX_DEMO=false docker compose up --build
# ou
DEMO_MODE=none docker compose up --build
```

### Modos de demonstração (`DEMO_MODE`)

| Valor | Comportamento |
|-------|----------------|
| `mutex` (padrão) | Demo automática Ricart-Agrawala |
| `lamport` | Demo automática de eventos Lamport |
| `none` | Sem demo automática; Bully sempre ativo |

### CLI e comandos remotos

Com um nó em foreground (terminal interativo):

```
status
event 2
request-cs
help
quit
```

Com cluster em background (Docker ou `run_local_cluster.sh`), use:

```bash
chmod +x scripts/command.sh
./scripts/command.sh node0 status
./scripts/command.sh node1 request-cs
./scripts/command.sh node2 event 3
```

## Testes

Instalar dependências de desenvolvimento e rodar a suíte:

```bash
uv sync --extra dev
uv run pytest -v
```

A suíte inclui:

- **Unitários:** relógio de Lamport, serialização de mensagens, regras de Ricart-Agrawala e Bully
- **Integração leve:** troca de eventos Lamport entre 3 nós, serialização da seção crítica e anúncio de coordenador

## Status da implementação

- [x] Estrutura do projeto, Docker e Compose
- [x] Camada de transporte TCP e relógio de Lamport
- [x] Ricart-Agrawala
- [x] Bully
- [x] Integração e scripts de demo
- [x] Testes unitários e de integração (`tests/`)
