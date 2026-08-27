# MeshWeaver-Zero-Dependency-P2P-Async-Task-Broker

MeshWeaver is a decentralized peer-to-peer task broker designed
for distributed and edge-computing environments.

Instead of depending on a central broker such as Redis or
RabbitMQ, MeshWeaver is designed around independent peer nodes.

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

## Technology

- Python
- asyncio
- UDP sockets
- Kademlia DHT
- cloudpickle

## Week 1

### Mesh Setup

Two independent Python instances communicate through
asynchronous UDP networking.

Implemented:

- UDP server
- UDP client communication
- PING/PONG protocol
- Peer node identity

### Task Serialization

Python functions can be serialized using cloudpickle,
transmitted over UDP, deserialized and executed on the
receiving node.

Example:

```python
def add_numbers(a, b):
    return a + b