# MeshWeaver-Zero-Dependency-P2P-Async-Task-Broker

MeshWeaver is a decentralized peer-to-peer task broker designed for
distributed and edge-computing environments.

Traditional distributed task queues often depend on centralized
components such as message brokers, worker infrastructure, or shared
databases. MeshWeaver explores a lightweight peer-to-peer approach
where independent Python nodes communicate directly and maintain
distributed network state.

The project is built around asynchronous networking, lightweight
Kademlia-based peer discovery, gossip-based resource sharing, and
remote task serialization.

---

## Project Domain

**Distributed Systems & Edge Computing**

---

## Problem

Traditional distributed task queues commonly depend on central
infrastructure such as:

- Redis
- RabbitMQ
- Central worker infrastructure

This introduces configuration overhead and potential central
points of failure.

## Goal

MeshWeaver aims to provide a decentralized task execution mesh
using Python asynchronous networking and distributed protocols.

# Technology Stack

- Python
- asyncio
- UDP sockets
- JSON protocol messages
- Kademlia-based DHT concepts
- Gossip protocol
- cloudpickle
- pytest
- Standard Python libraries

---

# Week 1 — Networking & Task Serialization
## Objective

The first week establishes the basic communication foundation of
MeshWeaver.

The Week 1 implementation focuses on two areas:
- Asynchronous UDP peer communication
- Task serialization and remote execution

1. Asynchronous UDP Networking 
## Basic communication flow:

    Node A
    │
    │ UDP message
    ▼
    Node B
    │
    │ response
    ▼
    Node A

2. Peer Messaging
Network messages are exchanged between nodes using a structured
message format.

The protocol layer is responsible for:
- Encoding messages
- Decoding messages
- Identifying message types
- Passing messages to the appropriate node handler

3. Task Serialization
Week 1 also introduces task serialization using cloudpickle.

The purpose is to package a Python function together with its required
arguments so that the serialized representation can be transferred
between peer nodes.

Conceptually:

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

- This establishes the foundation for future distributed task execution.

# Week 2 — DHT & Gossip Protocol
## Objective

Week 2 extends the Week 1 networking layer with decentralized peer
discovery and resource-state propagation.

The two primary objectives are:
- Lightweight Kademlia-based node discovery
- Periodic CPU/RAM gossip between neighboring nodes

1. Kademlia-Based Node Discovery
MeshWeaver uses a lightweight Kademlia-inspired Distributed Hash Table
to organize and discover peers.
Each node has a 160-bit node identifier.

The node identifier is used to calculate XOR distance between peers.
    Node A ID
        │
        │ XOR distance
        ▼
    Node B ID

2. Bootstrap Process
A new node can join the mesh through a known bootstrap peer.
Example:
    Node B
    │
    │ Bootstrap
    ▼
    Node A
- The joining node contacts Node A and requests information about
  other known peers.

Conceptually:
    New Node
        │
        │ FIND_NODE
        ▼
    Bootstrap Peer
        │
        │ Known peer information
        ▼
    New Node
- The new node can then add discovered peers to its local routing table.

3. Iterative Peer Discovery
The lightweight discovery mechanism queries known peers for nodes that
are closer to a target node ID.

Conceptually:
    Node A
    │
    │ FIND_NODE
    ▼
    Node B
    │
    │ discovered peers
    ▼
    Node C

4. Gossip Protocol
MeshWeaver also introduces a background gossip engine.
Every node periodically collects its current resource information and
shares it with known neighbors.

The Week 2 implementation uses a five-second gossip interval.

The information includes:
- CPU utilization
- RAM utilization
- Node identifier
- Timestamp

Gossip Flow
    Node A
    CPU: 25%
    RAM: 52%
        │
        │ GOSSIP
        ▼
    Node B
- Node B stores the received resource information locally.

After the next interval:
    Node A
    CPU: 31%
    RAM: 53%
        │
        │ GOSSIP
        ▼
    Node B
- The remote resource information is updated.

---


# Week 1 + Week 2 System Flow :
                    MeshWeaver Node
                          │
                          ▼
                 Async UDP Network
                          │
              ┌───────────┴───────────┐
              │                       │
              ▼                       ▼
        Kademlia DHT             Gossip Engine
              │                       │
              ▼                       ▼
       Peer Discovery          CPU / RAM Sharing
              │                       │
              └───────────┬───────────┘
                          │
                          ▼
                    P2P Mesh State

---

## Week 1 Completed Task
✅ Asyncio UDP networking foundation
✅ Peer-to-peer message exchange
✅ Network protocol layer
✅ Task serialization foundation
✅ Remote task execution foundation

## Week 2 Completed Task
✅ Lightweight Kademlia node identifiers
✅ XOR distance calculation
✅ Routing table
✅ Bootstrap-based peer discovery
✅ FIND_NODE discovery
✅ Iterative peer lookup
✅ CPU monitoring
✅ RAM monitoring
✅ Background gossip engine
✅ Five-second resource broadcasting
✅ Remote peer metric storage
✅ DHT tests
✅ Metrics tests

---

# Running the Mesh

## Start the first node:
- python examples/receiver.py --port 9001

## Start another node:
- python examples/sender.py \
    --port 9002 \
    --bootstrap 127.0.0.1:9001

## Start a third node:
- python examples/sender.py \
    --port 9003 \
    --bootstrap 127.0.0.1:9001

---

# Development Roadmap :
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

---