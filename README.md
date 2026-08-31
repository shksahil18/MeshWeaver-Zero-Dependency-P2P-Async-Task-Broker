# MeshWeaver --- Zero-Dependency P2P Async Task Broker

MeshWeaver is a decentralized peer-to-peer task broker designed for
distributed and edge-computing environments.

Traditional distributed task queues often depend on centralized
components such as message brokers, worker infrastructure, or shared
databases. MeshWeaver explores a lightweight peer-to-peer approach where
independent Python nodes communicate directly and maintain distributed
network state.

The project is built around asynchronous networking, lightweight
Kademlia-based peer discovery, gossip-based resource sharing, and remote
task serialization.

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

Example:

``` text
Node B
  │
  │ Bootstrap
  ▼
Node A
```

The joining node contacts Node A and requests information about other
known peers.

Conceptually:

``` text
New Node
    │
    │ FIND_NODE
    ▼
Bootstrap Peer
    │
    │ Known peer information
    ▼
New Node
```

The new node can then add discovered peers to its local routing table.

### 3. Iterative Peer Discovery

The lightweight discovery mechanism queries known peers for nodes that
are closer to a target node ID.

Conceptually:

``` text
Node A
  │
  │ FIND_NODE
  ▼
Node B
  │
  │ discovered peers
  ▼
Node C
```

### 4. Gossip Protocol

MeshWeaver also introduces a background gossip engine.

Every node periodically collects its current resource information and
shares it with known neighbors.

The Week 2 implementation uses a **five-second gossip interval**.

The information includes:

-   CPU utilization
-   RAM utilization
-   Node identifier
-   Timestamp

#### Gossip Flow

``` text
Node A
CPU: 25%
RAM: 52%
   │
   │ GOSSIP
   ▼
Node B
```

Node B stores the received resource information locally.

After the next interval:

``` text
Node A
CPU: 31%
RAM: 53%
   │
   │ GOSSIP
   ▼
Node B
```

The remote resource information is updated.

------------------------------------------------------------------------

# 🔗 Week 1 + Week 2 System Flow

``` text
                    MeshWeaver Node
                           │
                           ▼
                  Async UDP Network
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
        Kademlia DHT              Gossip Engine
              │                         │
              ▼                         ▼
       Peer Discovery            CPU / RAM Sharing
              │                         │
              └────────────┬────────────┘
                           │
                           ▼
                     P2P Mesh State
```

------------------------------------------------------------------------

# ✅ Week 1 Completed Tasks

-   [x] Asyncio UDP networking foundation
-   [x] Peer-to-peer message exchange
-   [x] Network protocol layer
-   [x] Task serialization foundation
-   [x] Remote task execution foundation

------------------------------------------------------------------------

# ✅ Week 2 Completed Tasks

-   [x] Lightweight Kademlia node identifiers
-   [x] XOR distance calculation
-   [x] Routing table
-   [x] Bootstrap-based peer discovery
-   [x] `FIND_NODE` discovery
-   [x] Iterative peer lookup
-   [x] CPU monitoring
-   [x] RAM monitoring
-   [x] Background gossip engine
-   [x] Five-second resource broadcasting
-   [x] Remote peer metric storage
-   [x] DHT tests
-   [x] Metrics tests

------------------------------------------------------------------------

# ▶️ Running the Mesh

Start the first node:

``` bash
python examples/receiver.py --port 9001
```

Start another node:

``` bash
python examples/sender.py --port 9002 --bootstrap 127.0.0.1:9001
```

Start a third node:

``` bash
python examples/sender.py --port 9003 --bootstrap 127.0.0.1:9001
```

### Example Mesh

``` text
Node 9001
    │
    ├──────── Node 9002
    │
    └──────── Node 9003
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
  -------- -------------------------------- --------------
  Week 1   Async UDP + Task Serialization   ✅ Completed
  Week 2   Kademlia DHT + Gossip            ✅ Completed
  Week 3   Task Routing + Fault Tolerance   🔜 Planned
  Week 4   Security + CLI Dashboard         🔜 Planned

------------------------------------------------------------------------

## 📁 Project Structure

``` text
MeshWeaver-Zero-Dependency-P2P-Async-Task-Broker/
│
├── examples/
│   ├── receiver.py
│   └── sender.py
│
├── meshweaver/
│   ├── ...
│   └── ...
│
├── tests/
│   └── ...
│
├── README.md
└── requirements.txt
```

> The structure above represents the main project areas; individual
> implementation files may evolve as development continues.

------------------------------------------------------------------------

## 💡 Project Summary

MeshWeaver explores how a distributed task execution system can operate
without relying on a centralized task broker.

Across the first two weeks, the project establishes:

1.  **Asynchronous peer communication** using UDP and `asyncio`
2.  **Task serialization** for transferring executable Python tasks
3.  **Kademlia-inspired peer discovery** using node IDs and XOR distance
4.  **Bootstrap and iterative peer lookup** for decentralized discovery
5.  **Gossip-based resource sharing** for CPU/RAM state propagation
6.  **Distributed mesh state** maintained across independent Python
    nodes

The Week 1 and Week 2 foundations prepare MeshWeaver for the next
development stages: task routing, fault tolerance, security, and a CLI
dashboard.
