import asyncio
import os
import sys
import random
import logging
from typing import Dict, List, Optional, Any, Union
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import httpx
import uvicorn

# Configuration from environment variables
NODE_ID = os.environ.get("NODE_ID", "node-1")
PORT = int(os.environ.get("PORT", "8000"))

# Parse peers environment variable (e.g. "node-1=http://node-1:8000,node-2=http://node-2:8000")
PEERS_ENV = os.environ.get("PEERS", "")
INITIAL_PEERS: Dict[str, str] = {}
if PEERS_ENV:
    for item in PEERS_ENV.split(","):
        if "=" in item:
            name, url = item.split("=", 1)
            INITIAL_PEERS[name] = url

# Helper to dynamically retrieve the configuration active for a given slot index
def get_config_for_index(idx: int) -> dict:
    for i in range(idx - 1, -1, -1):
        if i in committed_v and committed_v[i] is not None:
            entry = committed_v[i]
            if isinstance(entry, dict) and entry.get("type") in ("CONFIG", "CONFIG_JOINT"):
                return entry
    return {"type": "CONFIG", "peers": INITIAL_PEERS}

# Helper to check if responding nodes satisfy configuration quorums (supporting joint consensus)
def check_quorum(config: dict, responses: list) -> bool:
    if config["type"] == "CONFIG":
        active_peers = config["peers"]
        majority = (len(active_peers) // 2) + 1
        votes = sum(1 for pid in responses if pid in active_peers)
        return votes >= majority
    elif config["type"] == "CONFIG_JOINT":
        old_peers = config["old_peers"]
        new_peers = config["new_peers"]
        majority_old = (len(old_peers) // 2) + 1
        majority_new = (len(new_peers) // 2) + 1
        votes_old = sum(1 for pid in responses if pid in old_peers)
        votes_new = sum(1 for pid in responses if pid in new_peers)
        return votes_old >= majority_old and votes_new >= majority_new
    return False

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
    if component == "PROPOSER":
        comp_color += "\033[93m"  # Yellow for proposer
    elif component == "ACCEPTOR":
        comp_color += "\033[96m"  # Cyan for acceptor
    elif component == "LEARNER":
        comp_color += "\033[92m"  # Green for learner
    elif component == "CHAOS":
        comp_color += "\033[91m"  # Red for chaos
    print(f"{color}[{NODE_ID}]{RESET} {comp_color}[{component}]{RESET} {msg}", flush=True)

# Initialize state
is_down = False
blocked_peers = set()

# Paxos State per log index
# index -> list [counter, node_id]
promised_n: Dict[int, List] = {}
# index -> list [counter, node_id]
accepted_n: Dict[int, List] = {}
# index -> value
accepted_v: Dict[int, str] = {}
# index -> value
committed_v: Dict[int, str] = {}

# Flat representation of the committed log (with None for holes)
log_list: List[Optional[str]] = []

# Proposal counter to ensure uniqueness
proposal_counter = 0

# Lock to ensure only one active proposal is run by this proposer at a time
propose_lock = asyncio.Lock()

app = FastAPI()
client = httpx.AsyncClient(timeout=2.0)

# Request schemas
class PrepareRequest(BaseModel):
    sender_id: str
    index: int
    n: List  # [counter, node_id]

class AcceptRequest(BaseModel):
    sender_id: str
    index: int
    n: List
    v: Any

class CommitRequest(BaseModel):
    sender_id: str
    index: int
    v: Any

# Helper to check if node can process requests
def check_status(sender_id: Optional[str] = None):
    if is_down:
        raise HTTPException(status_code=503, detail="Node is down")
    if sender_id and sender_id in blocked_peers:
        raise HTTPException(status_code=503, detail="Sender partitioned")

# -----------------
# Paxos RPC Endpoints
# -----------------

@app.post("/prepare")
async def prepare(req: PrepareRequest):
    check_status(req.sender_id)
    idx = req.index
    n = req.n

    # Initialize states for this slot if not present
    if idx not in promised_n:
        promised_n[idx] = [-1, ""]
        accepted_n[idx] = None
        accepted_v[idx] = None

    if n > promised_n[idx]:
        promised_n[idx] = n
        log(f"Promise accepted: index {idx}, proposal {n} (prev promise: {promised_n[idx]})", "ACCEPTOR")
        return {
            "status": "promise",
            "accepted_n": accepted_n[idx],
            "accepted_v": accepted_v[idx]
        }
    else:
        log(f"Promise rejected: index {idx}, proposal {n} <= promised {promised_n[idx]}", "ACCEPTOR")
        return {
            "status": "reject",
            "max_n": promised_n[idx]
        }

@app.post("/accept")
async def accept(req: AcceptRequest):
    check_status(req.sender_id)
    idx = req.index
    n = req.n
    v = req.v

    if idx not in promised_n:
        promised_n[idx] = [-1, ""]
        accepted_n[idx] = None
        accepted_v[idx] = None

    # Accept if the proposal number is >= promised number
    if n >= promised_n[idx]:
        promised_n[idx] = n
        accepted_n[idx] = n
        accepted_v[idx] = v
        log(f"Accept accepted: index {idx}, proposal {n} with value '{v}'", "ACCEPTOR")
        return {"status": "accepted"}
    else:
        log(f"Accept rejected: index {idx}, proposal {n} < promised {promised_n[idx]}", "ACCEPTOR")
        return {
            "status": "reject",
            "max_n": promised_n[idx]
        }

@app.post("/commit")
async def commit(req: CommitRequest):
    check_status(req.sender_id)
    idx = req.index
    v = req.v

    committed_v[idx] = v
    # Pad log list
    global log_list
    while len(log_list) <= idx:
        log_list.append(None)
    log_list[idx] = v

    log(f"Committed index {idx} = '{v}'. Full Log: {[x for x in log_list if x is not None]} (len: {len(log_list)})", "LEARNER")
    
    # Trigger hole filling asynchronously
    asyncio.create_task(fill_holes_from_peers())
    
    return {"status": "ok"}

# -----------------
# Client and Control API
# -----------------

async def propose_value_at_index(idx: int, val: Any) -> dict:
    global proposal_counter
    attempts = 0
    while attempts < 10:
        attempts += 1
        if idx in committed_v and committed_v[idx] is not None:
            committed_val = committed_v[idx]
            if isinstance(val, dict) and isinstance(committed_val, dict):
                if committed_val.get("command") == val.get("command"):
                    return {"status": "success", "index": idx, "value": val}
            elif committed_val == val:
                return {"status": "success", "index": idx, "value": val}
            log(f"Index {idx} already committed to '{committed_val}'. Advancing index.", "PROPOSER")
            idx += 1
            continue

        n = [proposal_counter, NODE_ID]
        proposal_counter += 1

        # Get configuration active for this slot
        config = get_config_for_index(idx)
        if config["type"] == "CONFIG":
            target_peers = config["peers"]
        elif config["type"] == "CONFIG_JOINT":
            # Union of old and new peers
            target_peers = {**config["old_peers"], **config["new_peers"]}
        else:
            target_peers = INITIAL_PEERS

        log(f"Phase 1a: Sending Prepare(index={idx}, n={n}) using config {config['type']}", "PROPOSER")

        # Send prepares to active target peers
        tasks = []
        active_targets = {pid: url for pid, url in target_peers.items() if pid not in blocked_peers}
        for peer_id, peer_url in active_targets.items():
            tasks.append(send_rpc(peer_id, f"{peer_url}/prepare", {"sender_id": NODE_ID, "index": idx, "n": n}))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        promises = []
        successful_responders = []
        rejects = 0
        max_rejected_n = [-1, ""]

        for peer_id, res in zip(active_targets.keys(), results):
            if isinstance(res, Exception) or res is None:
                continue
            if res.get("status") == "promise":
                promises.append(res)
                successful_responders.append(peer_id)
            elif res.get("status") == "reject":
                rejects += 1
                if res.get("max_n", [-1, ""]) > max_rejected_n:
                    max_rejected_n = res["max_n"]

        # Check quorum based on config
        if not check_quorum(config, successful_responders):
            log(f"Phase 1a failed: No quorum promise reached. Retrying.", "PROPOSER")
            if max_rejected_n[0] >= proposal_counter:
                proposal_counter = max_rejected_n[0] + 1
            await asyncio.sleep(random.uniform(0.1, 0.4))
            continue

        # Find highest accepted value
        highest_accepted_n = [-1, ""]
        chosen_v = val
        for p in promises:
            acc_n = p.get("accepted_n")
            acc_v = p.get("accepted_v")
            if acc_n and acc_n > highest_accepted_n:
                highest_accepted_n = acc_n
                chosen_v = acc_v

        if chosen_v != val:
            log(f"Phase 1a resolved: Index {idx} has already accepted value. Adopting it.", "PROPOSER")
        else:
            log(f"Phase 1a resolved: No accepted value. Proposing our value.", "PROPOSER")

        # Phase 2a: Send accept
        log(f"Phase 2a: Sending Accept(index={idx}, n={n}) using config {config['type']}", "PROPOSER")
        tasks = []
        for peer_id, peer_url in active_targets.items():
            tasks.append(send_rpc(peer_id, f"{peer_url}/accept", {"sender_id": NODE_ID, "index": idx, "n": n, "v": chosen_v}))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        accept_responders = []
        max_rejected_n = [-1, ""]

        for peer_id, res in zip(active_targets.keys(), results):
            if isinstance(res, Exception) or res is None:
                continue
            if res.get("status") == "accepted":
                accept_responders.append(peer_id)
            elif res.get("status") == "reject":
                if res.get("max_n", [-1, ""]) > max_rejected_n:
                    max_rejected_n = res["max_n"]

        if not check_quorum(config, accept_responders):
            log(f"Phase 2a failed: No quorum acceptance reached. Retrying.", "PROPOSER")
            if max_rejected_n[0] >= proposal_counter:
                proposal_counter = max_rejected_n[0] + 1
            await asyncio.sleep(random.uniform(0.1, 0.4))
            continue

        # Phase 3: Commit!
        log(f"Phase 2a success! Committing value at index {idx}", "PROPOSER")
        tasks = []
        for peer_id, peer_url in active_targets.items():
            tasks.append(send_rpc(peer_id, f"{peer_url}/commit", {"sender_id": NODE_ID, "index": idx, "v": chosen_v}))
        await asyncio.gather(*tasks, return_exceptions=True)

        if chosen_v == val:
            return {"status": "success", "index": idx, "value": val}
        else:
            idx += 1
            continue

    raise HTTPException(status_code=500, detail="Failed to reach consensus after max attempts")

class WriteRequest(BaseModel):
    value: str

@app.post("/write")
async def write(req: WriteRequest):
    check_status()
    async with propose_lock:
        val = req.value
        idx = 0
        # Find first uncommitted slot
        while idx in committed_v and committed_v[idx] is not None:
            idx += 1

        log(f"Received write request for '{val}'. Proposing at index {idx}", "PROPOSER")
        return await propose_value_at_index(idx, val)

class ChangeMembershipRequest(BaseModel):
    node_id: str
    node_url: str
    action: str  # "ADD" or "REMOVE"

@app.post("/membership/change")
async def change_membership(req: ChangeMembershipRequest):
    check_status()
    async with propose_lock:
        idx = 0
        while idx in committed_v and committed_v[idx] is not None:
            idx += 1

        log(f"Received Paxos membership change: {req.action} {req.node_id}. Proposing at index {idx}", "PROPOSER")

        # 1. Get current configuration
        current_config = get_config_for_index(idx)
        if current_config["type"] != "CONFIG":
            raise HTTPException(status_code=400, detail="Cannot change membership while another change is in progress")

        old_peers = current_config["peers"].copy()
        new_peers = old_peers.copy()
        if req.action == "ADD":
            new_peers[req.node_id] = req.node_url
        elif req.action == "REMOVE":
            new_peers.pop(req.node_id, None)
        else:
            raise HTTPException(status_code=400, detail="Invalid action")

        # 2. Phase A: Propose CONFIG_JOINT at index idx
        joint_config = {
            "type": "CONFIG_JOINT",
            "old_peers": old_peers,
            "new_peers": new_peers,
            "command": f"CONFIG_JOINT_{req.action}_{req.node_id}"
        }

        res = await propose_value_at_index(idx, joint_config)
        if res.get("status") != "success":
            raise HTTPException(status_code=500, detail="Failed to commit Joint Configuration")

        # 3. Phase B: Propose CONFIG_NEW at index idx + 1
        idx_new = idx + 1
        while idx_new in committed_v and committed_v[idx_new] is not None:
            idx_new += 1

        final_config = {
            "type": "CONFIG",
            "peers": new_peers,
            "command": f"CONFIG_NEW_{req.action}_{req.node_id}"
        }

        res2 = await propose_value_at_index(idx_new, final_config)
        if res2.get("status") != "success":
            raise HTTPException(status_code=500, detail="Failed to commit Final Configuration")

        return {
            "status": "success",
            "joint_index": idx,
            "final_index": idx_new,
            "peers": new_peers
        }

# Helper to check if sending to self to bypass network
async def send_rpc(peer_id: str, url: str, payload: dict) -> Optional[dict]:
    # Simulate network block
    if peer_id in blocked_peers or is_down:
        return None
    try:
        # If sending to self, handle it synchronously to avoid network hops and simplify debugging
        if peer_id == NODE_ID:
            if url.endswith("/prepare"):
                return await prepare(PrepareRequest(**payload))
            elif url.endswith("/accept"):
                return await accept(AcceptRequest(**payload))
            elif url.endswith("/commit"):
                return await commit(CommitRequest(**payload))
        
        response = await client.post(url, json=payload, timeout=0.8)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        pass
    return None

@app.get("/state")
async def get_state():
    return {
        "node_id": NODE_ID,
        "is_down": is_down,
        "blocked_peers": list(blocked_peers),
        "log": log_list,
        "committed_v": committed_v,
        "promised_n": promised_n,
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
    global is_down
    is_down = False
    log("Chaos node up: Server recovered and online", "CHAOS")
    return {"status": "up"}

# Helper to fill holes from other nodes
# Helper to fill holes from other nodes
async def fill_holes_from_peers():
    # Find all holes in log_list (indices < len(log_list) where value is None)
    holes = [i for i, val in enumerate(log_list) if val is None]
    if not holes:
        return
    
    # Query active peers for their committed state
    config = get_config_for_index(len(log_list))
    if config["type"] == "CONFIG":
        peers_dict = config["peers"]
    elif config["type"] == "CONFIG_JOINT":
        peers_dict = {**config["old_peers"], **config["new_peers"]}
    else:
        peers_dict = INITIAL_PEERS

    active_peers = {pid: url for pid, url in peers_dict.items() if pid not in blocked_peers}
    for peer_id, peer_url in active_peers.items():
        if peer_id == NODE_ID:
            continue
        try:
            response = await client.get(f"{peer_url}/state", timeout=0.8)
            if response.status_code == 200:
                peer_state = response.json()
                peer_committed = peer_state.get("committed_v", {})
                for h in holes:
                    h_str = str(h)
                    if h_str in peer_committed and peer_committed[h_str] is not None:
                        val = peer_committed[h_str]
                        committed_v[h] = val
                        log_list[h] = val
                        log(f"Learned hole at index {h} = '{val}' from peer {peer_id}", "LEARNER")
                # Recompute holes
                holes = [i for i, val in enumerate(log_list) if val is None]
                if not holes:
                    break
        except Exception:
            pass

async def run_sync_loop():
    while True:
        await asyncio.sleep(1.0)
        if not is_down:
            await fill_holes_from_peers()

@app.on_event("startup")
async def startup_event():
    # Make sure self is in INITIAL_PEERS list
    global INITIAL_PEERS
    if NODE_ID not in INITIAL_PEERS:
        INITIAL_PEERS[NODE_ID] = f"http://localhost:{PORT}"
    asyncio.create_task(run_sync_loop())

if __name__ == "__main__":
    # Start Fast API Server
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
