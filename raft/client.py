import sys
import httpx
import json

# Node URL mappings
NODES = {
    "node-1": "http://localhost:9001",
    "node-2": "http://localhost:9002",
    "node-3": "http://localhost:9003",
}

def print_help():
    print("Raft Client Tool")
    print("Usage:")
    print("  python client.py write <value> [node_id]")
    print("  python client.py state")
    print("Examples:")
    print("  python client.py write hello node-1")
    print("  python client.py state")

def get_node_url(node_id):
    if node_id not in NODES:
        print(f"Error: Unknown node '{node_id}'. Valid nodes: {list(NODES.keys())}")
        sys.exit(1)
    return NODES[node_id]

def write_value(value, node_id="node-1"):
    url = f"{get_node_url(node_id)}/write"
    print(f"Sending write request '{value}' to {node_id} at {url}...")
    try:
        response = httpx.post(url, json={"value": value}, timeout=10.0)
        if response.status_code == 200:
            print("Response:", json.dumps(response.json(), indent=2))
        elif response.status_code == 307:
            # Handle redirection
            detail = response.json().get("detail", {})
            leader = detail.get("leader")
            if leader and leader in NODES:
                print(f"Node {node_id} redirected us. Active Leader is {leader}. Retrying write on {leader}...")
                write_value(value, leader)
            else:
                print(f"Redirected by {node_id}, but no active leader known yet: {detail}")
        else:
            print(f"Failed with status code {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Error: Could not connect to {node_id} ({e})")

def show_state():
    print("Querying state from all Raft nodes...")
    for node_id, url in NODES.items():
        try:
            response = httpx.get(f"{url}/state", timeout=2.0)
            if response.status_code == 200:
                data = response.json()
                role = data.get("role", "UNKNOWN")
                term = data.get("current_term", 0)
                commit_idx = data.get("commit_index", 0)
                status = "DOWN" if data.get("is_down") else "ONLINE"
                blocked = data.get("blocked_peers", [])
                
                # Format log for readability
                log_entries = data.get("log", [])
                # Skip the sentinel at index 0
                log_clean = [f"{x['command']}(T:{x['term']})" for x in log_entries[1:]]
                
                blocked_str = f" (Blocked: {blocked})" if blocked else ""
                print(f"{node_id} ({status}) [{role}, Term: {term}, CommitIdx: {commit_idx}]{blocked_str}: Log = {log_clean}")
            else:
                print(f"{node_id}: HTTP Error {response.status_code}")
        except Exception as e:
            print(f"{node_id}: UNREACHABLE ({type(e).__name__})")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_help()
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "write":
        if len(sys.argv) < 3:
            print("Error: Specify a value to write.")
            sys.exit(1)
        val = sys.argv[2]
        node = sys.argv[3] if len(sys.argv) > 3 else "node-1"
        write_value(val, node)
    elif cmd == "state":
        show_state()
    else:
        print_help()
