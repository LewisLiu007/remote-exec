# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running

**Machine B (controller) — start server first:**
```bash
python3 server.py                          # default: 127.0.0.1:9876
python3 server.py --bind 0.0.0.0 --port 9876
```

**Machine A (controlled) — start agent:**
```bash
python3 agent.py <host-B-IP> -u <user> -k ~/.ssh/id_rsa
python3 agent.py <host-B-IP> -u <user> -k ~/.ssh/id_rsa --tunnel-port 9876
python3 agent.py <host-B-IP> -u <user> -k ~/.ssh/id_rsa --no-reconnect
```

## Architecture

Two components communicate over a **reverse SSH tunnel**:

```
Machine A (agent.py)  ──SSH──→  Machine B (server.py)
    │                                │
    └── direct-tcpip channel ──────→ 127.0.0.1:9876
```

- **`agent.py`** runs on the controlled machine (A). It initiates the SSH connection to B, opens a `direct-tcpip` channel to B's `server.py` port, sends a `register` message with its hostname, then enters a loop executing `exec` messages via `subprocess.run(shell=True)` and returning results as JSON.

- **`server.py`** runs on the controller machine (B). It listens on a local TCP port, manages multiple `AgentConn` objects (one per connected agent), and provides an interactive REPL. Each `AgentConn` has a background reader thread and a heartbeat thread.

## Protocol

All messages are newline-delimited JSON over the TCP channel:

| Direction | Type | Fields |
|-----------|------|--------|
| A → B | `register` | `hostname` |
| B → A | `exec` | `cmd`, `id`, `timeout` |
| A → B | `result` | `id`, `stdout`, `stderr`, `returncode` |
| Both | `heartbeat` | — |
| B → A | `ping` | `id` |
| A → B | `pong` | `id` |

`AgentConn.exec()` on the server side sends an `exec` message, blocks on a `threading.Event` keyed by `req_id`, and returns when the matching `result` arrives.

## Key Design Details

- Agent auto-reconnects on disconnect (5s delay) unless `--no-reconnect` is set.
- `paramiko.AutoAddPolicy()` is used — host key verification is skipped.
- Server default bind is `127.0.0.1` so only SSH-tunneled connections are accepted.
- `readline` is imported in `server.py` purely for interactive history/editing side effects.
- No tests exist in this repo.
