#!/usr/bin/env python3
"""
server.py — 运行在机器B上
监听本地 TCP 端口，等待 agent.py 的连接，提供交互式命令行界面
"""

import sys
import json
import socket
import threading
import time
import uuid
import logging
import argparse
import readline  # noqa: F401  — 启用命令行历史

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [server] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 15


class AgentConn:
    """代表一个已连接的 Agent（机器A）"""

    def __init__(self, sock: socket.socket, addr):
        self.sock = sock
        self.addr = addr
        self.hostname = str(addr)
        self.buf = ""
        self._lock = threading.Lock()
        self._pending: dict[str, threading.Event] = {}
        self._results: dict[str, dict] = {}
        self._alive = True
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        self._heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat.start()

    # ── 发送 ────────────────────────────────────────────────
    def send(self, msg: dict):
        with self._lock:
            data = (json.dumps(msg) + "\n").encode()
            try:
                self.sock.sendall(data)
            except Exception as e:
                log.warning(f"发送失败: {e}")
                self._alive = False

    # ── 接收循环 ────────────────────────────────────────────
    def _read_loop(self):
        self.sock.settimeout(HEARTBEAT_INTERVAL + 10)
        while self._alive:
            try:
                data = self.sock.recv(4096)
            except socket.timeout:
                continue
            except Exception as e:
                log.info(f"Agent 连接断开: {e}")
                break

            if not data:
                log.info("Agent 已断开连接")
                break

            self.buf += data.decode("utf-8", errors="replace")
            while "\n" in self.buf:
                line, self.buf = self.buf.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self._handle(msg)

        self._alive = False
        self.sock.close()

    def _handle(self, msg: dict):
        mtype = msg.get("type")
        if mtype == "register":
            self.hostname = msg.get("hostname", str(self.addr))
            log.info(f"Agent 注册: hostname={self.hostname}")
        elif mtype in ("heartbeat", "pong"):
            pass  # 静默处理
        elif mtype == "result":
            req_id = msg.get("id", "")
            self._results[req_id] = msg
            ev = self._pending.get(req_id)
            if ev:
                ev.set()
        else:
            log.warning(f"未知消息: {msg}")

    def _heartbeat_loop(self):
        while self._alive:
            time.sleep(HEARTBEAT_INTERVAL)
            if self._alive:
                self.send({"type": "heartbeat"})

    # ── 执行命令 ────────────────────────────────────────────
    def exec(self, cmd: str, timeout: int = 60) -> dict:
        req_id = str(uuid.uuid4())[:8]
        ev = threading.Event()
        self._pending[req_id] = ev
        self.send({"type": "exec", "cmd": cmd, "id": req_id, "timeout": timeout})
        if not ev.wait(timeout=timeout + 5):
            return {"stdout": "", "stderr": "等待结果超时", "returncode": -1}
        result = self._results.pop(req_id, {})
        self._pending.pop(req_id, None)
        return result

    @property
    def alive(self):
        return self._alive


# ── 全局连接注册表 ──────────────────────────────────────────
agents: dict[str, AgentConn] = {}
agents_lock = threading.Lock()


def accept_loop(server_sock: socket.socket):
    while True:
        try:
            conn, addr = server_sock.accept()
        except Exception:
            break
        log.info(f"新连接来自: {addr}")
        agent = AgentConn(conn, addr)
        # 稍等一下，让 register 消息到达
        time.sleep(0.5)
        with agents_lock:
            agents[agent.hostname] = agent
        # 清理已断开的连接
        _cleanup()


def _cleanup():
    with agents_lock:
        dead = [k for k, v in agents.items() if not v.alive]
        for k in dead:
            del agents[k]


def list_agents():
    _cleanup()
    with agents_lock:
        if not agents:
            print("（无已连接的 Agent）")
        else:
            for i, (name, ag) in enumerate(agents.items(), 1):
                print(f"  [{i}] {name}  ({ag.addr})")


def select_agent(spec: str) -> AgentConn | None:
    _cleanup()
    with agents_lock:
        keys = list(agents.keys())
        # 按编号选
        try:
            idx = int(spec) - 1
            if 0 <= idx < len(keys):
                return agents[keys[idx]]
        except ValueError:
            pass
        # 按主机名前缀选
        for k in keys:
            if k.startswith(spec):
                return agents[k]
    print(f"找不到 Agent: {spec!r}")
    return None


HELP = """
命令：
  list                  列出已连接的 Agent
  use <n|hostname>      选择 Agent（编号或主机名前缀）
  <shell命令>           在当前 Agent 上执行命令
  exit / quit           退出
  help / ?              显示此帮助
"""


def interactive(default_agent: AgentConn | None = None):
    current: AgentConn | None = default_agent
    print("server 已就绪。输入 help 查看命令。")

    while True:
        if current:
            prompt = f"[{current.hostname}]> "
        else:
            prompt = "[未选择]> "

        try:
            line = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line:
            continue

        if line in ("exit", "quit"):
            break
        elif line in ("help", "?"):
            print(HELP)
        elif line == "list":
            list_agents()
        elif line.startswith("use "):
            spec = line[4:].strip()
            ag = select_agent(spec)
            if ag:
                current = ag
                print(f"已切换到: {current.hostname}")
        else:
            # 当作 shell 命令发给当前 Agent
            if current is None:
                print("请先用 'use <n>' 选择一个 Agent，或等待 Agent 连接")
                continue
            if not current.alive:
                print("当前 Agent 已断开，请重新 use")
                current = None
                continue
            result = current.exec(line)
            if result.get("stdout"):
                print(result["stdout"], end="" if result["stdout"].endswith("\n") else "\n")
            if result.get("stderr"):
                print(f"\033[33m[stderr]\033[0m {result['stderr']}")
            rc = result.get("returncode", 0)
            if rc != 0:
                print(f"\033[31m[exit {rc}]\033[0m")


def main():
    parser = argparse.ArgumentParser(description="Server: 运行在控制机B，等待 Agent 连接")
    parser.add_argument("--bind", default="127.0.0.1",
                        help="监听地址（默认 127.0.0.1，仅接受 SSH 隧道；0.0.0.0 开放所有接口）")
    parser.add_argument("--port", type=int, default=9876, help="监听端口（默认9876）")
    args = parser.parse_args()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((args.bind, args.port))
    server_sock.listen(10)
    log.info(f"监听 {args.bind}:{args.port}，等待 Agent 连接...")

    t = threading.Thread(target=accept_loop, args=(server_sock,), daemon=True)
    t.start()

    interactive()

    server_sock.close()
    log.info("Server 已退出")


if __name__ == "__main__":
    main()
