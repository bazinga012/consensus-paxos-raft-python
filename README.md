# Replicated Log Consensus: Paxos and Raft in Python

This project contains lightweight, educational, yet fully-functional implementations of two of the most popular distributed consensus algorithms: **Multi-Paxos** and **Raft**. 

Both algorithms are containerized with Docker, configured with dynamic chaos injection endpoints, and can be queried or tested in real-time.

---

## Architecture Overview

We run two independent 3-node clusters using a shared Docker network. 

1. **Multi-Paxos Cluster** (ports `8001`, `8002`, `8003`)
   - Employs basic Single-Decree Paxos instances per log slot.
   - Nodes act as combined Proposers, Acceptors, and Learners.
   - If a slot contains an unresolved value from a concurrent proposer, the proposer helps resolve the conflict before proceeding with its own proposal in the next slot.

2. **Raft Cluster** (ports `9001`, `9002`, `9003`)
   - Fully implements Leader Election, Heartbeats, Log Replication, and Safety Commits.
   - Nodes transition dynamically between **Follower**, **Candidate**, and **Leader** states.
   - If a client writes to a Follower, the client is redirected to the active Leader.

---

## Directory Structure

```
.
├── paxos/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── node.py             # Multi-Paxos implementation
│   └── client.py           # CLI tool for Paxos
├── raft/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── node.py             # Raft implementation
│   └── client.py           # CLI tool for Raft (with redirect handling)
├── docker-compose.yml       # Orchestration for the 6 containers
├── simulate_chaos.py       # Automated test & partition simulator
└── README.md               # Documentation
```

---

## Getting Started

### 1. Build and Start the Clusters
Ensure Docker Desktop is running. In the root directory, run:
```bash
docker-compose up --build
```
You will see colored logs scrolling in the terminal, where each node has its own distinct color:
- **Green**: `node-1`
- **Blue**: `node-2`
- **Magenta**: `node-3`

*Keep this window open to observe the real-time voting, heartbeats, and write replication messages!*

### 2. Querying State or Writing Values Manually
Open a new terminal window to interact with the clusters using the provided client CLIs.

#### Paxos Cluster
- **Check current logs and cluster state**:
  ```bash
  python paxos/client.py state
  ```
- **Write a value to the cluster (propose through `node-1`)**:
  ```bash
  python paxos/client.py write "apple" node-1
  ```

#### Raft Cluster
- **Check current roles, terms, and logs**:
  ```bash
  python raft/client.py state
  ```
- **Write a value to the cluster (it will automatically follow redirects to the leader)**:
  ```bash
  python raft/client.py write "banana" node-2
  ```

---

## Dynamic Chaos & Failure Simulation

To see the algorithms "in action", we simulate network partitions and node crashes without needing complex system network calls. Instead, nodes support `/chaos` control endpoints to simulate dropped packets.

### Automated Simulation Script
Run the automated test suite on your host machine (requires no third-party libraries, using Python's standard `urllib` library):
```bash
python simulate_chaos.py
```

The script automatically executes the following scenarios and prints color-coded validation results:

1. **Happy Path Replication**: Validates that writes to any active node are replicated, committed, and logs are identical.
2. **Network Partition (Split Brain)**:
   - Partitions the cluster into a majority group (`node-1`, `node-2`) and a minority group (`node-3`).
   - Writes to the majority group succeed.
   - Writes to the minority group fail/timeout (since it cannot reach consensus with a minority).
   - Heals the partition and validates that `node-3` catches up and matches the majority, while discarding its uncommitted entries.
3. **Leader Crash and Recovery (Raft)**:
   - Queries the cluster to find the active Leader.
   - Triggers `/chaos/down` to simulate a node crash on the Leader.
   - Monitors the remaining nodes as they time out and elect a new Leader.
   - Submits writes to the new Leader.
   - Recovers the crashed leader (`/chaos/up`) and validates that it transitions back to a Follower and catches up.
