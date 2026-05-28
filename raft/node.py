import asyncio
import os
import sys
import random
import time
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uvicorn

# Configuration from environment variables
NODE_ID = os.environ.get("NODE_ID", "node-1")
PORT = int(os.environ.get("PORT", "8000"))

# Parse peers environment variable
PEERS_ENV = os.environ.get("PEERS", "")
peers: Dict[str, str] = {}
if PEERS_ENV:
    for item in PEERS_ENV.split(","):
        if "=" in item:
            name, url = item.split("=", 1)
            peers[name] = url

# Terminal colors for visual logs
COLORS = {
    "node-1": "\033[92m",  # Green
    "node-2": "\033[94m",  # Blue
    "node-3": "\033[95m",  # Magenta
    "node-4": "\033[96m",  # Cyan
    "node-5": "\033[93m",  # Yellow
}
RESET = "\033[0m"

def log(msg: str, component: str = "INFO"):
    color = COLORS.get(NODE_ID, "\033[97m")
    comp_color = "\033[1m"
    if component == "LEADER":
        comp_color += "\033[92m"  # Green
    elif component == "CANDIDATE":
        comp_color += "\033[93m"  # Yellow
    elif component == "FOLLOWER":
        comp_color += "\033[94m"  # Blue
    elif component == "CHAOS":
        comp_color += "\033[91m"  # Red
    print(f"{color}[{NODE_ID}]{RESET} {comp_color}[{component}]{RESET} {msg}", flush=True)

# Chaos variables
is_down = False
blocked_peers = set()

# Raft State Variables
role = "FOLLOWER"  # FOLLOWER, CANDIDATE, LEADER
current_term = 0
voted_for: Optional[str] = None

# Log starts with a sentinel at index 0 (1-based index support)
log_data: List[dict] = [{"term": 0, "command": "SENTINEL"}]

commit_index = 0
last_applied = 0

# Volatile state on leaders
next_index: Dict[str, int] = {}
match_index: Dict[str, int] = {}

# Keep track of the leader to redirect clients
last_known_leader: Optional[str] = None

# Timers
last_rpc_time = time.time()
election_timeout = random.uniform(1.5, 3.0)

app = FastAPI()
client = httpx.AsyncClient(timeout=2.0)

class RequestVoteRequest(BaseModel):
    sender_id: str
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

class AppendEntriesRequest(BaseModel):
    sender_id: str
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[dict]
    leader_commit: int

def check_status(sender_id: Optional[str] = None):
    if is_down:
        raise HTTPException(status_code=503, detail="Node is down")
    if sender_id and sender_id in blocked_peers:
        raise HTTPException(status_code=503, detail="Sender partitioned")

def check_term(term: int):
    global current_term, role, voted_for
    if term > current_term:
        log(f"Saw term {term} > current_term {current_term}. Converting to FOLLOWER.", role)
        current_term = term
        voted_for = None
        role = "FOLLOWER"

# -----------------
# Raft RPC Endpoints
# -----------------

@app.post("/request_vote")
async def request_vote(req: RequestVoteRequest):
    check_status(req.sender_id)
    check_term(req.term)

    global current_term, voted_for, last_rpc_time, election_timeout

    # 1. Reply false if term < currentTerm
    if req.term < current_term:
        log(f"Rejecting vote to {req.candidate_id}: term {req.term} < {current_term}", "FOLLOWER")
        return {"term": current_term, "vote_granted": False}

    # Check if candidate's log is up-to-date
    last_log_idx = len(log_data) - 1
    last_log_term = log_data[last_log_idx]["term"]
    
    up_to_date = False
    if req.last_log_term > last_log_term:
        up_to_date = True
    elif req.last_log_term == last_log_term and req.last_log_index >= last_log_idx:
        up_to_date = True

    # 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
    if (voted_for is None or voted_for == req.candidate_id) and up_to_date:
        voted_for = req.candidate_id
        # Reset election timer
        last_rpc_time = time.time()
        election_timeout = random.uniform(1.5, 3.0)
        log(f"Granting vote to candidate {req.candidate_id} for term {current_term}", "FOLLOWER")
        return {"term": current_term, "vote_granted": True}
    else:
        reason = "already voted" if voted_for is not None else "candidate log not up-to-date"
        log(f"Rejecting vote to {req.candidate_id} for term {current_term} (Reason: {reason})", "FOLLOWER")
        return {"term": current_term, "vote_granted": False}

@app.post("/append_entries")
async def append_entries(req: AppendEntriesRequest):
    check_status(req.sender_id)
    check_term(req.term)

    global current_term, role, last_rpc_time, election_timeout, last_known_leader, commit_index

    # 1. Reply false if term < currentTerm
    if req.term < current_term:
        return {"term": current_term, "success": False}

    # We received a valid RPC from the active leader of the current (or higher) term
    last_known_leader = req.leader_id
    last_rpc_time = time.time()
    election_timeout = random.uniform(1.5, 3.0)

    if role == "CANDIDATE":
        role = "FOLLOWER"

    # 2. Reply false if log doesn’t contain an entry at prevLogIndex matching prevLogTerm
    if req.prev_log_index >= len(log_data) or log_data[req.prev_log_index]["term"] != req.prev_log_term:
        log(f"AppendEntries failed: prev_log_index {req.prev_log_index} term mismatch", "FOLLOWER")
        return {"term": current_term, "success": False}

    # 3. If an existing entry conflicts with a new one (same index but different terms),
    # delete the existing entry and all that follow it
    idx = req.prev_log_index + 1
    entry_pos = 0
    while idx < len(log_data) and entry_pos < len(req.entries):
        if log_data[idx]["term"] != req.entries[entry_pos]["term"]:
            log(f"Log conflict at index {idx}. Deleting trailing log.", "FOLLOWER")
            del log_data[idx:]
            break
        idx += 1
        entry_pos += 1

    # 4. Append any new entries not already in the log
    if entry_pos < len(req.entries):
        log(f"Appending {len(req.entries) - entry_pos} entries starting at index {idx}", "FOLLOWER")
        log_data.extend(req.entries[entry_pos:])

    # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if req.leader_commit > commit_index:
        commit_index = min(req.leader_commit, len(log_data) - 1)
        log(f"Committed up to index {commit_index}. Full Log: {log_data[1:]}", "FOLLOWER")

    return {"term": current_term, "success": True}

# -----------------
# Client and Control API
# -----------------

class WriteRequest(BaseModel):
    value: str

@app.post("/write")
async def write(req: WriteRequest):
    check_status()
    global commit_index

    if role != "LEADER":
        raise HTTPException(
            status_code=307,
            detail={
                "status": "redirect",
                "leader": last_known_leader,
                "msg": f"Node {NODE_ID} is not the Leader"
            }
        )

    # Append locally
    new_entry = {"term": current_term, "command": req.value}
    log_data.append(new_entry)
    new_index = len(log_data) - 1
    log(f"Appended local log index {new_index}: '{req.value}'", "LEADER")

    # Trigger immediate replication
    await replicate_log()

    # Wait for replication/commit to complete
    timeout = 4.0
    start_time = time.time()
    while time.time() - start_time < timeout:
        if commit_index >= new_index:
            return {
                "status": "success",
                "index": new_index,
                "term": current_term,
                "value": req.value
            }
        await asyncio.sleep(0.1)
        if role != "LEADER" or is_down:
            raise HTTPException(status_code=500, detail="Lost leadership during replication")

    raise HTTPException(status_code=500, detail="Write timeout: failed to replicate to majority")

async def send_rpc(peer_id: str, url: str, payload: dict) -> Optional[dict]:
    if peer_id in blocked_peers or is_down:
        return None
    try:
        # Optimization: call self methods directly to avoid network
        if peer_id == NODE_ID:
            if url.endswith("/request_vote"):
                return await request_vote(RequestVoteRequest(**payload))
            elif url.endswith("/append_entries"):
                return await append_entries(AppendEntriesRequest(**payload))

        response = await client.post(url, json=payload, timeout=0.8)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None

async def replicate_log():
    if role != "LEADER" or is_down:
        return

    tasks = []
    active_peers = {pid: url for pid, url in peers.items() if pid not in blocked_peers}
    
    for peer_id, peer_url in active_peers.items():
        if peer_id == NODE_ID:
            continue
        tasks.append(replicate_to_peer(peer_id, peer_url))

    await asyncio.gather(*tasks)

async def replicate_to_peer(peer_id: str, peer_url: str):
    global current_term, role, voted_for

    if role != "LEADER":
        return

    prev_idx = next_index[peer_id] - 1
    if prev_idx >= len(log_data):
        prev_idx = len(log_data) - 1
        next_index[peer_id] = prev_idx + 1

    prev_term = log_data[prev_idx]["term"]
    entries = log_data[next_index[peer_id]:]

    payload = {
        "sender_id": NODE_ID,
        "term": current_term,
        "leader_id": NODE_ID,
        "prev_log_index": prev_idx,
        "prev_log_term": prev_term,
        "entries": entries,
        "leader_commit": commit_index
    }

    res = await send_rpc(peer_id, f"{peer_url}/append_entries", payload)
    if res is None:
        return

    peer_term = res.get("term", 0)
    if peer_term > current_term:
        check_term(peer_term)
        return

    if role == "LEADER":
        if res.get("success"):
            match_index[peer_id] = prev_idx + len(entries)
            next_index[peer_id] = match_index[peer_id] + 1
            check_commit_index()
        else:
            # Backtrack next_index
            next_index[peer_id] = max(1, next_index[peer_id] - 1)

def check_commit_index():
    global commit_index
    majority = (len(peers) // 2) + 1
    
    for N in range(len(log_data) - 1, commit_index, -1):
        # Raft leader only commits entries of its own term directly
        if log_data[N]["term"] == current_term:
            count = 1  # count leader self
            for pid in peers:
                if pid != NODE_ID and match_index.get(pid, 0) >= N:
                    count += 1
            if count >= majority:
                commit_index = N
                log(f"Committed up to index {commit_index}. Full Log: {log_data[1:]}", "LEADER")
                break

# -----------------
# Background loops
# -----------------

async def run_election_timeout_loop():
    global election_timeout, last_rpc_time
    while True:
        await asyncio.sleep(0.1)
        if is_down:
            continue
        if role != "LEADER":
            if time.time() - last_rpc_time > election_timeout:
                log(f"Election timeout ({election_timeout:.2f}s) expired. Starting election.", role)
                await start_election()

async def start_election():
    global role, current_term, voted_for, last_rpc_time, election_timeout
    role = "CANDIDATE"
    current_term += 1
    voted_for = NODE_ID
    last_rpc_time = time.time()
    election_timeout = random.uniform(1.5, 3.0)

    log(f"Starting election for term {current_term}", "CANDIDATE")
    votes = 1

    tasks = []
    active_peers = {pid: url for pid, url in peers.items() if pid not in blocked_peers}
    
    last_log_idx = len(log_data) - 1
    last_log_term = log_data[last_log_idx]["term"]

    for peer_id, peer_url in active_peers.items():
        if peer_id == NODE_ID:
            continue
        tasks.append(send_rpc(peer_id, f"{peer_url}/request_vote", {
            "sender_id": NODE_ID,
            "term": current_term,
            "candidate_id": NODE_ID,
            "last_log_index": last_log_idx,
            "last_log_term": last_log_term
        }))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, Exception) or res is None:
            continue
        peer_term = res.get("term", 0)
        if peer_term > current_term:
            check_term(peer_term)
            return
        if role == "CANDIDATE" and res.get("vote_granted"):
            votes += 1

    majority = (len(peers) // 2) + 1
    if role == "CANDIDATE" and votes >= majority:
        role = "LEADER"
        log(f"Became Leader for term {current_term}", "LEADER")
        # Initialize nextIndex and matchIndex
        for pid in peers:
            next_index[pid] = len(log_data)
            match_index[pid] = 0
        # Replicate heartbeats immediately
        await replicate_log()

async def run_heartbeat_loop():
    while True:
        await asyncio.sleep(0.4)
        if is_down:
            continue
        if role == "LEADER":
            await replicate_log()

@app.on_event("startup")
async def startup_event():
    # Make sure self is in peers list for self-references
    if NODE_ID not in peers:
        peers[NODE_ID] = f"http://localhost:{PORT}"
    
    # Start timer loops in the background
    asyncio.create_task(run_election_timeout_loop())
    asyncio.create_task(run_heartbeat_loop())

@app.get("/state")
async def get_state():
    return {
        "node_id": NODE_ID,
        "role": role,
        "current_term": current_term,
        "voted_for": voted_for,
        "log": log_data,
        "commit_index": commit_index,
        "is_down": is_down,
        "blocked_peers": list(blocked_peers),
    }

# -----------------
# Chaos Endpoints
# -----------------

class ChaosPartitionRequest(BaseModel):
    blocked: List[str]

@app.post("/chaos/partition")
async def chaos_partition(req: ChaosPartitionRequest):
    global blocked_peers
    blocked_peers = set(req.blocked)
    log(f"Chaos network partition: Blocking peers {req.blocked}", "CHAOS")
    return {"status": "partitioned", "blocked": list(blocked_peers)}

@app.post("/chaos/heal")
async def chaos_heal():
    global blocked_peers
    blocked_peers.clear()
    log("Chaos healed: All peer connections restored", "CHAOS")
    return {"status": "healed"}

@app.post("/chaos/down")
async def chaos_down():
    global is_down
    is_down = True
    log("Chaos node down: Server stopping to respond to consensus requests", "CHAOS")
    return {"status": "down"}

@app.post("/chaos/up")
async def chaos_up():
    global is_down, last_rpc_time, election_timeout
    is_down = False
    # Reset timers on startup/recovery to prevent immediate election
    last_rpc_time = time.time()
    election_timeout = random.uniform(1.5, 3.0)
    log("Chaos node up: Server recovered and online", "CHAOS")
    return {"status": "up"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
