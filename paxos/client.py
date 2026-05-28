import sys
import httpx
import json

# Node URL mappings
NODES = {
    "node-1": "http://localhost:8001",
    "node-2": "http://localhost:8002",
    "node-3": "http://localhost:8003",
}

def print_help():
    print("Paxos Client Tool")
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
        else:
            print(f"Failed with status code {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Error: Could not connect to {node_id} ({e})")

def show_state():
    print("Querying state from all Paxos nodes...")
    for node_id, url in NODES.items():
        try:
            response = httpx.get(f"{url}/state", timeout=2.0)
            if response.status_code == 200:
                data = response.json()
                status = "DOWN" if data.get("is_down") else "ONLINE"
                blocked = data.get("blocked_peers", [])
                log = data.get("log", [])
                log_clean = [x if x is not None else "<hole>" for x in log]
                blocked_str = f" (Blocked: {blocked})" if blocked else ""
                print(f"{node_id} ({status}){blocked_str}: Log = {log_clean}")
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
