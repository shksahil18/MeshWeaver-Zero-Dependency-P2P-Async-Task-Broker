# MeshWeaver --- Zero-Dependency P2P Async Task Broker

MeshWeaver is a decentralized peer-to-peer task broker designed for
distributed and edge-computing environments.

Traditional distributed task queues often depend on centralized
components such as message brokers, worker infrastructure, or shared
databases. MeshWeaver explores a lightweight peer-to-peer approach where
independent Python nodes communicate directly and maintain distributed
network state.

The project is built around asynchronous networking, lightweight
Kademlia-based peer discovery, gossip-based resource sharing, remote
task serialization, CPU-load-aware task routing, heartbeat-based fault
tolerance, cryptographic message signing, and a live Rich CLI dashboard.

------------------------------------------------------------------------

## 📌 Project Domain

**Distributed Systems & Edge Computing**

------------------------------------------------------------------------

## 🎯 Problem

Traditional distributed task queues commonly depend on centralized
infrastructure such as:

-   Redis
-   RabbitMQ
-   Central worker infrastructure

This introduces configuration overhead and potential central points of
failure.

------------------------------------------------------------------------

## 🚀 Goal

MeshWeaver aims to provide a decentralized task execution mesh using
Python asynchronous networking and distributed protocols.

------------------------------------------------------------------------

## 🛠️ Technology Stack

-   **Python**
-   **asyncio**
-   **UDP sockets**
-   **JSON protocol messages**
-   **Kademlia-based DHT concepts**
-   **Gossip protocol**
-   **cloudpickle**
-   **HMAC-SHA256** (Week 4 signing)
-   **rich** (Week 4 CLI dashboard)
-   **pytest**
-   **Standard Python libraries**

------------------------------------------------------------------------

# 📅 Week 1 --- Networking & Task Serialization

## Objective

The first week establishes the basic communication foundation of
MeshWeaver.

The Week 1 implementation focuses on:

-   Asynchronous UDP peer communication
-   Task serialization and remote execution

### 1. Asynchronous UDP Networking

Basic communication flow:

``` text
Node A
  │
  │ UDP message
  ▼
Node B
  │
  │ response
  ▼
Node A
```

### 2. Peer Messaging

Network messages are exchanged between nodes using a structured message
format.

The protocol layer is responsible for:

-   Encoding messages
-   Decoding messages
-   Identifying message types
-   Passing messages to the appropriate node handler

### 3. Task Serialization

Week 1 also introduces task serialization using `cloudpickle`.

The purpose is to package a Python function together with its required
arguments so that the serialized representation can be transferred
between peer nodes.

Conceptually:

``` text
Python Function
      │
      ▼
  cloudpickle
      │
      ▼
Serialized Task
      │
      │ UDP Network
      ▼
  Remote Node
      │
      ▼
 Deserialize
      │
      ▼
Execute Function
```

This establishes the foundation for future distributed task execution.

------------------------------------------------------------------------

# 📅 Week 2 --- DHT & Gossip Protocol

## Objective

Week 2 extends the Week 1 networking layer with decentralized peer
discovery and resource-state propagation.

The two primary objectives are:

-   Lightweight Kademlia-based node discovery
-   Periodic CPU/RAM gossip between neighboring nodes

### 1. Kademlia-Based Node Discovery

MeshWeaver uses a lightweight Kademlia-inspired Distributed Hash Table
(DHT) to organize and discover peers.

Each node has a **160-bit node identifier**.

The node identifier is used to calculate **XOR distance** between peers.

``` text
Node A ID
    │
    │ XOR distance
    ▼
Node B ID
```

### 2. Bootstrap Process

A new node can join the mesh through a known bootstrap peer.

``` text
Node B
  │
  │ Bootstrap
  ▼
Node A
```

### 3. Gossip Protocol

Every node periodically collects its current resource information and
shares it with known neighbors.

The Week 2 implementation uses a **five-second gossip interval**.

The information includes:

-   CPU utilization
-   RAM utilization
-   Node identifier
-   Timestamp

------------------------------------------------------------------------

# 📅 Week 3 --- Task Routing & Fault Tolerance

## Objective

Week 3 introduces intelligent task routing and automatic recovery from
node failures.

### 1. Least-Loaded Task Routing

When a task is submitted via `node.route_task()`, the `TaskRouter`
selects the peer with the **lowest reported CPU load** from the live
gossip metrics.

Routing algorithm:

```
1. Collect DHT peers
2. Filter: drop self, exclude already-tried nodes, drop OFFLINE peers
3. Attach gossip metrics (CPU %, RAM %)
4. Sort: fresh metrics first → lowest CPU → lowest RAM → deterministic tie-break
5. Dispatch to first candidate
```

``` text
   Gossip Metrics
   CPU: 72% │ 18% │ 41%
      ┌─────┴──┬───┘
      │        │
    Node      Node ◄── selected (lowest CPU)
    9002      9003
```

### 2. Heartbeat Monitor

The `HeartbeatMonitor` sends a lightweight PING every **2 seconds** to
every known peer. If a peer has not been heard from for **6 seconds** it
is declared **OFFLINE**.

``` text
t=0   PING → peer     (PONG received, last_seen updated)
t=2   PING → peer     (no reply)
t=4   PING → peer     (no reply)
t=6   PING → peer     (no reply)
t=6+  now - last_seen > 6 s  →  OFFLINE
```

### 3. Automatic Re-routing on Failure

When a peer goes OFFLINE the `MeshNode._on_peer_offline` callback:

1.  Finds every `DISPATCHED` task assigned to that peer.
2.  Marks them `FAILED` (non-final).
3.  Re-dispatches to the next best available node.
4.  Repeats up to `max_attempts` (default 3).

``` text
Task submitted
     │
     ▼
Node A ← selected (CPU 18%)
     │
  [crashes]
     │
Heartbeat detects: OFFLINE
     │
Task re-routed →  Node B (next lowest CPU)
```

### 4. Task Tracker

The `TaskTracker` maintains a complete ledger of every submitted task:

-   Status: `PENDING` → `DISPATCHED` → `COMPLETED` / `FAILED`
-   Attempt count and target address
-   Full history log
-   Set of already-tried addresses (excluded from re-routing)

------------------------------------------------------------------------

# 📅 Week 4 --- Security & Live CLI Dashboard

## Objective

Week 4 adds message authentication and a real-time terminal dashboard.

### 1. HMAC-SHA256 Message Signing

All `TASK` messages can be signed with a shared secret key using
**HMAC-SHA256**.

> **Note on TLS**: Standard TLS operates over TCP only. MeshWeaver uses
> UDP. The UDP equivalent (DTLS, RFC 6347) requires unstable platform
> bindings. MeshWeaver therefore implements application-layer message
> authentication which provides equivalent integrity guarantees.

Key management:

```bash
# Generate and export a key
export MESHWEAVER_KEY=$(python -c "import secrets; print(secrets.token_hex(32))")
```

Signing flow:

``` text
Sender                           Receiver
  │                                  │
  │  sign_message(key, task_msg)     │
  │  → attaches "sig": "<hmac_hex>"  │
  │                                  │
  │ ──── UDP TASK message ──────────►│
  │                                  │
  │                    verify_message(key, msg)
  │                    OK → execute task
  │                    FAIL → drop silently
```

### 2. Live CLI Dashboard

The `rich`-powered dashboard shows real-time mesh state:

``` text
┌─────────────────────────────────────────────────────────────────┐
│  ⬡ MeshWeaver Live Dashboard  │  Node: abc123...  │  127.0.0.1:9001 │
├──────────────────────────┬──────────────────────────────────────┤
│  🌐  Mesh Topology       │  📋  Task Ledger                     │
│                          │                                      │
│  Address    Status CPU   │  Task ID  Fn      Status  Node       │
│  9002:●     ONLINE 18%   │  abc12345 add     DONE    9002       │
│  9003:◌    OFFLINE --    │  def67890 slow    RUN     9003       │
│                          │                                      │
├──────────────────────────┴──────────────────────────────────────┤
│  Nodes: 1 online / 2 known   Tasks: 2 total  1 done  1 running  │
└─────────────────────────────────────────────────────────────────┘
```

------------------------------------------------------------------------

# 📁 Project Structure

``` text
MeshWeaver-Zero-Dependency-P2P-Async-Task-Broker/
│
├── meshweaver/
│   ├── __init__.py
│   ├── node.py          ← Core node (Weeks 1-4 integrated)
│   ├── network.py       ← Async UDP
│   ├── protocol.py      ← JSON encode/decode
│   ├── dht.py           ← Kademlia DHT
│   ├── gossip.py        ← CPU/RAM gossip
│   ├── metrics.py       ← System metrics (CPU/RAM)
│   ├── heartbeat.py     ← Heartbeat monitor (Week 3)
│   ├── router.py        ← Task router (Week 3)
│   ├── security.py      ← HMAC signing (Week 4)
│   ├── dashboard.py     ← Rich CLI dashboard (Week 4)
│   └── tasks/
│       ├── serializer.py
│       └── tracker.py   ← Task ledger (Week 3)
│
├── examples/
│   ├── receiver.py
│   ├── sender.py
│   ├── mid_review_network_audit.py
│   ├── ml_serialization_demo.py
│   ├── week3_routing_fault_tolerance_demo.py  ← Week 3
│   └── week4_dashboard.py                     ← Week 4
│
└── tests/
    ├── test_dht.py
    ├── test_metrics.py
    ├── test_protocol.py
    ├── test_serializer.py
    └── test_router.py   ← Week 3
```

------------------------------------------------------------------------

# ▶️ Running the Mesh

## Basic Mesh (Week 1 & 2)

``` bash
# Terminal 1
python examples/receiver.py --port 9001

# Terminal 2
python examples/sender.py --port 9002 --bootstrap 127.0.0.1:9001

# Terminal 3
python examples/sender.py --port 9003 --bootstrap 127.0.0.1:9001
```

## Week 3 — Routing & Fault Tolerance

``` bash
# Start two worker nodes
python examples/receiver.py --port 9002
python examples/receiver.py --port 9003

# Run the demo (auto-routes to lowest CPU)
python examples/week3_routing_fault_tolerance_demo.py

# Fault tolerance demo (kill a worker after dispatch)
python examples/week3_routing_fault_tolerance_demo.py --failure-demo
```

## Week 4 — Dashboard + Security

``` bash
# Start a worker
python examples/receiver.py --port 9002

# Run the live dashboard
python examples/week4_dashboard.py --port 9001 --worker 127.0.0.1:9002

# With HMAC signing + submit tasks
python examples/week4_dashboard.py --port 9001 --worker 127.0.0.1:9002 --sign --tasks 5
```

## Running Tests

``` bash
pip install -r requirements.txt
pytest tests/ -v
```

------------------------------------------------------------------------

# 🗺️ Development Roadmap

``` text
Week 1
Async UDP + Task Serialization
        │
        ▼
Week 2
Kademlia DHT + Gossip
        │
        ▼
Week 3
Task Routing + Fault Tolerance
        │
        ▼
Week 4
Security + CLI Dashboard
```

------------------------------------------------------------------------

## 📌 Current Project Progress

  Week     Focus                            Status
  -------- -------------------------------- ----------------
  Week 1   Async UDP + Task Serialization   ✅ Completed
  Week 2   Kademlia DHT + Gossip            ✅ Completed
  Week 3   Task Routing + Fault Tolerance   ✅ Completed
  Week 4   Security + CLI Dashboard         ✅ Completed

------------------------------------------------------------------------

## ✅ Week 3 Completed Tasks

-   [x] Least-loaded CPU task routing algorithm (`router.py`)
-   [x] `TaskRouter.select_worker()` — picks lowest-CPU peer from a list
-   [x] `TaskRouter.select_node()` — DHT-aware routing with heartbeat filter
-   [x] `TaskRouter` route lifecycle: `create_route`, `mark_dispatched`, `mark_failed`, `mark_completed`
-   [x] `HeartbeatMonitor` — sends PING every 2 s, timeout 6 s
-   [x] `HeartbeatMonitor.mark_alive()` — refreshed by every inbound message
-   [x] `_on_peer_offline` callback — marks tasks FAILED and re-routes
-   [x] `TaskTracker` — full task ledger with history
-   [x] `MeshNode.route_task()` — routing-aware task submission
-   [x] `MeshNode.wait_for_result()` — awaitable result future
-   [x] `MeshNode.send_to_address()` — raw address send for HeartbeatMonitor
-   [x] `test_router.py` — unit tests for routing and route lifecycle

------------------------------------------------------------------------

## ✅ Week 4 Completed Tasks

-   [x] `security.py` — HMAC-SHA256 message signing
-   [x] `sign_message()` / `verify_message()` / `verify_and_strip()`
-   [x] `generate_key()` / `key_from_env()` / `key_from_file()`
-   [x] Signature verification on incoming TASK messages in `MeshNode`
-   [x] Signing applied in `route_task()` and `send_task()` when key configured
-   [x] `dashboard.py` — Rich live CLI dashboard
-   [x] Mesh Topology panel (address, status, CPU, RAM, last seen)
-   [x] Task Ledger panel (ID, function, status, attempts, node, result)
-   [x] Footer summary bar (online nodes, task counts, security status)
-   [x] `examples/week4_dashboard.py` — dashboard + security demo

------------------------------------------------------------------------

## 💡 Project Summary

MeshWeaver explores how a distributed task execution system can operate
without relying on a centralized task broker.

Across all four weeks, the project establishes:

1.  **Asynchronous peer communication** using UDP and `asyncio`
2.  **Task serialization** for transferring executable Python tasks
3.  **Kademlia-inspired peer discovery** using node IDs and XOR distance
4.  **Bootstrap and iterative peer lookup** for decentralized discovery
5.  **Gossip-based resource sharing** for CPU/RAM state propagation
6.  **Least-loaded task routing** via live gossip CPU metrics
7.  **Heartbeat fault tolerance** — failed nodes trigger automatic re-routing
8.  **HMAC-SHA256 message signing** for task integrity verification
9.  **Rich live CLI dashboard** showing real-time mesh topology and task state
