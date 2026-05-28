import urllib.request
import urllib.error
import json
import time
import sys

PAXOS_NODES = {
    "node-1": "http://localhost:8001",
    "node-2": "http://localhost:8002",
    "node-3": "http://localhost:8003",
    "node-4": "http://localhost:8004",
}

GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RESET = "\033[0m"

def log_step(msg):
    print(f"\n{CYAN}=== {msg} ==={RESET}")

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
        with urllib.request.urlopen(req, timeout=8.0) as response:
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

def get_logs():
    logs = {}
    for nid, url in PAXOS_NODES.items():
        state, _ = get_json(f"{url}/state")
        if state:
            log_entries = state.get("log", [])
            formatted = []
            for entry in log_entries:
                if entry is None:
                    formatted.append("<hole>")
                elif isinstance(entry, dict) and entry.get("type") == "CONFIG_JOINT":
                    formatted.append(f"JOINT({list(entry['new_peers'].keys())})")
                elif isinstance(entry, dict) and entry.get("type") == "CONFIG":
                    formatted.append(f"CONFIG({list(entry['peers'].keys())})")
                else:
                    formatted.append(entry)
            logs[nid] = formatted
        else:
            logs[nid] = "OFFLINE/ERROR"
    return logs

def main():
    log_step("Starting Paxos Joint Consensus Membership Change Verification")

    # 1. Write value before adding node-4
    log_step("Writing value 'apple' to paxos-node-1 (before adding node-4)")
    res, code = post_json(f"{PAXOS_NODES['node-1']}/write", {"value": "apple"})
    if code != 200:
        log_fail(f"Write failed: {res}")
        sys.exit(1)
    log_pass("Write succeeded")

    # Print logs
    logs = get_logs()
    log_info(f"Current logs:\n  " + "\n  ".join([f"{k}: {v}" for k, v in logs.items()]))
    
    if "apple" not in logs["node-1"] or "apple" in logs["node-4"]:
        log_fail("Initial logs check failed. node-4 should not contain 'apple'")
        sys.exit(1)
    log_pass("Initial logs verification succeeded")

    # 2. Add node-4 dynamically
    log_step("Dynamically adding node-4 via Joint Consensus")
    payload = {
        "node_id": "node-4",
        "node_url": "http://paxos-node-4:8000",
        "action": "ADD"
    }
    res, code = post_json(f"{PAXOS_NODES['node-1']}/membership/change", payload)
    if code == 200:
        log_pass(f"Membership change committed! Joint Index: {res.get('joint_index')}, Final Index: {res.get('final_index')}")
    else:
        log_fail(f"Failed to add node-4: {res} (code {code})")
        sys.exit(1)

    # 3. Write value after adding node-4
    log_step("Writing value 'banana' to cluster (after adding node-4)")
    res, code = post_json(f"{PAXOS_NODES['node-1']}/write", {"value": "banana"})
    if code != 200:
        log_fail(f"Write failed: {res}")
        sys.exit(1)
    log_pass("Write succeeded")

    # Check logs
    time.sleep(1.0)
    logs = get_logs()
    log_info(f"Logs after adding node-4:\n  " + "\n  ".join([f"{k}: {v}" for k, v in logs.items()]))

    # Verify node-4 caught up
    if "apple" in logs["node-4"] and "banana" in logs["node-4"]:
        log_pass("node-4 successfully caught up on history and replicated the new write!")
    else:
        log_fail("node-4 failed to replicate logs!")
        sys.exit(1)

    # 4. Remove node-3 dynamically
    log_step("Dynamically removing node-3 via Joint Consensus")
    payload = {
        "node_id": "node-3",
        "node_url": "",
        "action": "REMOVE"
    }
    res, code = post_json(f"{PAXOS_NODES['node-1']}/membership/change", payload)
    if code == 200:
        log_pass(f"Membership change committed! Joint Index: {res.get('joint_index')}, Final Index: {res.get('final_index')}")
    else:
        log_fail(f"Failed to remove node-3: {res} (code {code})")
        sys.exit(1)

    # 5. Write value after removing node-3
    log_step("Writing value 'cherry' to cluster (after removing node-3)")
    res, code = post_json(f"{PAXOS_NODES['node-1']}/write", {"value": "cherry"})
    if code != 200:
        log_fail(f"Write failed: {res}")
        sys.exit(1)
    log_pass("Write succeeded")

    # Check logs
    time.sleep(1.0)
    logs = get_logs()
    log_info(f"Logs after removing node-3:\n  " + "\n  ".join([f"{k}: {v}" for k, v in logs.items()]))

    # Verify node-3 did NOT get 'cherry' and node-4 DID get 'cherry'
    if "cherry" in logs["node-4"] and "cherry" not in logs["node-3"]:
        log_pass("Replication verified! node-4 received 'cherry', but removed node-3 did not.")
        print(f"\n{GREEN}ALL PAXOS MEMBERSHIP TESTS PASSED SUCCESSFULLY! 🎉{RESET}")
    else:
        log_fail("Log mismatch after removing node-3!")
        sys.exit(1)

if __name__ == "__main__":
    main()
