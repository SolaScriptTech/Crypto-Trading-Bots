import sys
import json
import subprocess

INSTANCES = {
    "pitchdeck": {
        "user_host": "ubuntu@54.195.139.152",
        "key": r"C:\Users\matti\Desktop\Kraken\Kraken_Key_eu-west-1.pem"
    },
    "v6": {
        "user_host": "ubuntu@54.194.207.166",
        "key": r"C:\Users\matti\Desktop\Kraken\Kraken_Key_eu-west-1.pem"
    },
    "quant_trader": {
        "user_host": "ubuntu@3.252.235.32",
        "key": r"C:\Users\Matti\Desktop\Quant Trader\Kraken_Key_eu-west-1.pem"
    }
}

def ssh(instance, command):
    cfg = INSTANCES[instance]
    result = subprocess.run(
        ["ssh", "-i", cfg["key"],
         "-o", "StrictHostKeyChecking=no",
         "-o", "ConnectTimeout=15",
         cfg["user_host"], command],
        capture_output=True, text=True, timeout=60
    )
    return result.stdout + result.stderr

def handle(req):
    method = req.get("method")
    rid = req.get("id")

    if method == "initialize":
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "ec2-ssh", "version": "1.0"}
        }}

    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": [{
            "name": "ssh_exec",
            "description": "Run a shell command on a named EC2 instance",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "instance": {
                        "type": "string",
                        "enum": list(INSTANCES.keys()),
                        "description": "Which instance: pitchdeck, v6, or quant_trader"
                    },
                    "command": {
                        "type": "string",
                        "description": "Shell command to run"
                    }
                },
                "required": ["instance", "command"]
            }
        }]}}

    if method == "tools/call":
        args = req["params"]["arguments"]
        output = ssh(args["instance"], args["command"])
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "content": [{"type": "text", "text": output}]
        }}

    return {"jsonrpc": "2.0", "id": rid, "result": {}}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
        resp = handle(req)
        print(json.dumps(resp), flush=True)
    except Exception as e:
        print(json.dumps({
            "jsonrpc": "2.0", "id": None,
            "error": {"code": -32603, "message": str(e)}
        }), flush=True)
