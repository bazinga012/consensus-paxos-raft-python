import urllib.request
import urllib.error
import json
import time
import sys

# Port mappings on localhost
PAXOS_NODES = {
    "node-1": "http://localhost:8001",
    "node-2": "http://localhost:8002",
    "node-3": "http://localhost:8003",
}

RAFT_NODES = {
    "node-1": "http://localhost:9001",
    "node-2": "http://localhost:9002",
    "node-3": "http://localhost:9003",
}

# Colors
GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RESET = "\033[0m"

def log_step(msg):
    print(f"\n{CYAN}=== STEP: {msg} ==={RESET}")

def log_pass(msg):
    print(f"{GREEN}[PASS] {msg}{RESET}")

def log_fail(msg):
    print(f"{RED}[FAIL] {msg}{RESET}")

def log_info(msg):
    print(f"{YELLOW}[INFO] {msg}{RESET}")

def post_json(url, data):
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=5.0) as response:
            return json.loads(response.read().decode("utf-8")), response.getcode()
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8")), e.code
        except Exception:
            return None, e.code
    except Exception as e:
        return None, 999

def get_json(url):
    try:
        with urllib.request.urlopen(url, timeout=3.0) as response:
            return json.loads(response.read().decode("utf-8")), response.getcode()
    except Exception as e:
        return None, 999

# -----------------------------------
# Test Scenarios
# -----------------------------------

async def run_paxos_tests():
    log_step("Starting Paxos Cluster Tests")
    
    # 1. Happy Path Writes
    log_info("Writing 'paxos-val-1' to paxos-node-1...")
    res, code = post_json(f"{PAXOS_NODES['node-1']}/write", {"value": "paxos-val-1"})
    if code == 200 and res.get("status") == "success":
        log_pass(f"Write succeeded at index {res.get('index')}")
    else:
        log_fail(f"Write failed: {res} (code {code})")
        return False

    log_info("Writing 'paxos-val-2' to paxos-node-2...")
    res, code = post_json(f"{PAXOS_NODES['node-2']}/write", {"value": "paxos-val-2"})
    if code == 200 and res.get("status") == "success":
        log_pass(f"Write succeeded at index {res.get('index')}")
    else:
        log_fail(f"Write failed: {res} (code {code})")
        return False

    # Check logs
    time.sleep(1.0)
    logs = {}
    for node_id, url in PAXOS_NODES.items():
        state, _ = get_json(f"{url}/state")
        logs[node_id] = state.get("log", []) if state else []
    
    log_info(f"Current Paxos logs: {logs}")
    if logs["node-1"] == logs["node-2"] == logs["node-3"] == ["paxos-val-1", "paxos-val-2"]:
        log_pass("Happy Path logs are consistent across all nodes")
    else:
        log_fail("Happy Path logs are inconsistent!")
        return False

    # 2. Network Partition
    log_step("Simulating Network Partition in Paxos: [node-1, node-2] vs [node-3]")
    # Partition node-3 away
    post_json(f"{PAXOS_NODES['node-1']}/chaos/partition", {"blocked": ["node-3"]})
    post_json(f"{PAXOS_NODES['node-2']}/chaos/partition", {"blocked": ["node-3"]})
    post_json(f"{PAXOS_NODES['node-3']}/chaos/partition", {"blocked": ["node-1", "node-2"]})

    log_info("Writing 'paxos-val-3' to node-1 (majority side)...")
    res, code = post_json(f"{PAXOS_NODES['node-1']}/write", {"value": "paxos-val-3"})
    if code == 200 and res.get("status") == "success":
        log_pass("Write to majority side succeeded as expected")
    else:
        log_fail(f"Write to majority failed: {res} (code {code})")
        return False

    log_info("Writing 'paxos-val-4' to node-3 (minority side)...")
    res, code = post_json(f"{PAXOS_NODES['node-3']}/write", {"value": "paxos-val-4"})
    if code != 200:
        log_pass("Write to minority side failed/timed out as expected (no majority consensus possible)")
    else:
        log_fail(f"Write to minority side succeeded? This violates safety! Response: {res}")
        return False

    # 3. Heal partition
    log_step("Healing Paxos Partition")
    for url in PAXOS_NODES.values():
        post_json(f"{url}/chaos/heal", {})

    log_info("Writing 'paxos-val-5' to node-2...")
    res, code = post_json(f"{PAXOS_NODES['node-2']}/write", {"value": "paxos-val-5"})
    if code == 200:
        log_pass("Write after healing succeeded")
    else:
        log_fail(f"Write after healing failed: {res}")
        return False

    # Check catch-up
    time.sleep(1.0)
    for node_id, url in PAXOS_NODES.items():
        state, _ = get_json(f"{url}/state")
        logs[node_id] = state.get("log", []) if state else []
    
    log_info(f"Final Paxos logs: {logs}")
    # Note: paxos-val-3 and paxos-val-5 should replicate everywhere.
    # The proposal 'paxos-val-4' submitted to node-3 during partition was never committed and is dropped/overwritten.
    expected_log = ["paxos-val-1", "paxos-val-2", "paxos-val-3", "paxos-val-5"]
    
    # We clean up None values from the logs if there are holes
    cleaned_logs = {nid: [x for x in log if x is not None] for nid, log in logs.items()}
    if cleaned_logs["node-1"] == cleaned_logs["node-2"] == cleaned_logs["node-3"] == expected_log:
        log_pass("Paxos logs healed and fully consistent!")
        return True
    else:
        log_fail("Paxos logs inconsistent after healing!")
        return False

async def run_raft_tests():
    log_step("Starting Raft Cluster Tests")

    # 1. Happy Path Writes
    # Find current leader
    leader = None
    for nid, url in RAFT_NODES.items():
        state, _ = get_json(f"{url}/state")
        if state and state.get("role") == "LEADER":
            leader = nid
            break
            
    if not leader:
        log_fail("Could not find active Raft Leader! Is the cluster still electing?")
        return False
        
    log_info(f"Current Raft Leader is: {leader}")

    log_info(f"Writing 'raft-val-1' to Leader ({leader})...")
    res, code = post_json(f"{RAFT_NODES[leader]}/write", {"value": "raft-val-1"})
    if code == 200:
        log_pass("Write to Leader succeeded")
    else:
        log_fail(f"Write to Leader failed: {res} (code {code})")
        return False

    # Verify logs
    time.sleep(1.0)
    logs = {}
    for node_id, url in RAFT_NODES.items():
        state, _ = get_json(f"{url}/state")
        # Format log entries (skip sentinel at index 0)
        logs[node_id] = [x["command"] for x in state.get("log", [])[1:]] if state else []

    log_info(f"Current Raft logs: {logs}")
    if logs["node-1"] == logs["node-2"] == logs["node-3"] == ["raft-val-1"]:
        log_pass("Raft logs are consistent across all nodes")
    else:
        log_fail("Raft logs are inconsistent!")
        return False

    # 2. Leader Crash and Election
    log_step(f"Simulating Leader Crash: Shutting down {leader}")
    post_json(f"{RAFT_NODES[leader]}/chaos/down", {})

    log_info("Waiting 3.5 seconds for election timeout and new leader election...")
    time.sleep(3.5)

    new_leader = None
    for nid, url in RAFT_NODES.items():
        if nid == leader:
            continue
        state, _ = get_json(f"{url}/state")
        if state and state.get("role") == "LEADER":
            new_leader = nid
            break

    if not new_leader:
        log_fail("Failed to elect a new leader after crash!")
        return False
    log_pass(f"New Raft Leader elected: {new_leader}")

    log_info(f"Writing 'raft-val-2' to new Leader ({new_leader})...")
    res, code = post_json(f"{RAFT_NODES[new_leader]}/write", {"value": "raft-val-2"})
    if code == 200:
        log_pass("Write to new Leader succeeded")
    else:
        log_fail(f"Write to new Leader failed: {res}")
        return False

    # 3. Recover original leader and check catch-up
    log_step(f"Recovering original leader: {leader}")
    post_json(f"{RAFT_NODES[leader]}/chaos/up", {})

    log_info("Waiting 2.5 seconds for synchronization...")
    time.sleep(2.5)

    # Check states and logs
    logs = {}
    states = {}
    for nid, url in RAFT_NODES.items():
        state, _ = get_json(f"{url}/state")
        states[nid] = state
        logs[nid] = [x["command"] for x in state.get("log", [])[1:]] if state else []

    log_info(f"Final Raft logs: {logs}")
    log_info(f"Former leader {leader} role: {states[leader].get('role') if states[leader] else 'UNKNOWN'}")

    if logs["node-1"] == logs["node-2"] == logs["node-3"] == ["raft-val-1", "raft-val-2"]:
        log_pass("Raft cluster fully synchronized and logs are consistent!")
        return True
    else:
        log_fail("Raft logs failed to synchronize after leader recovery!")
        return False

if __name__ == "__main__":
    log_info("Starting consensus chaos simulation...")
    paxos_ok = False
    raft_ok = False
    
    # Run tests synchronously
    import asyncio
    try:
        paxos_ok = asyncio.run(run_paxos_tests())
    except Exception as e:
        log_fail(f"Paxos tests crashed with exception: {e}")

    # Heal all Paxos just in case of crash
    for url in PAXOS_NODES.values():
        post_json(f"{url}/chaos/heal", {})
        post_json(f"{url}/chaos/up", {})

    print("\n" + "="*40 + "\n")

    try:
        raft_ok = asyncio.run(run_raft_tests())
    except Exception as e:
        log_fail(f"Raft tests crashed with exception: {e}")

    # Heal all Raft just in case of crash
    for url in RAFT_NODES.values():
        post_json(f"{url}/chaos/heal", {})
        post_json(f"{url}/chaos/up", {})

    print("\n" + "="*40 + "\n")
    if paxos_ok and raft_ok:
        print(f"{GREEN}ALL TESTS PASSED SUCCESSFULLY! 🎉{RESET}")
        sys.exit(0)
    else:
        print(f"{RED}SOME TESTS FAILED! ❌{RESET}")
        sys.exit(1)
