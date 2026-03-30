#!/usr/bin/env python3
"""
server.py — 运行在机器B上
监听本地 TCP 端口，等待 agent.py 的连接，提供交互式命令行界面

启动模式：
  python3 server.py                         # 前台交互式
  python3 server.py --non-interactive       # 后台静默运行
  python3 server.py attach                  # 连接到后台运行的 server，进入交互

后台运行时通过 Unix socket（默认 /tmp/remote-exec.sock）接受 attach 连接。
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
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [server] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 15
DEFAULT_CTRL_SOCK = "/tmp/remote-exec.sock"


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


def list_agents(out=print):
    _cleanup()
    with agents_lock:
        if not agents:
            out("（无已连接的 Agent）")
        else:
            for i, (name, ag) in enumerate(agents.items(), 1):
                out(f"  [{i}] {name}  ({ag.addr})")


def select_agent(spec: str, out=print) -> AgentConn | None:
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
    out(f"找不到 Agent: {spec!r}")
    return None


HELP = """
命令：
  list                  列出已连接的 Agent
  use <n|hostname>      选择 Agent（编号或主机名前缀）
  <shell命令>           在当前 Agent 上执行命令
  exit / quit           退出（后台模式下仅断开 attach，server 继续运行）
  help / ?              显示此帮助
"""


def interactive(out=None, inp=None, default_agent: AgentConn | None = None):
    """
    交互式 REPL。
    out: 输出函数，默认 print
    inp: 输入函数，默认 input
    """
    if out is None:
        out = print
    if inp is None:
        inp = input

    current: AgentConn | None = default_agent
    out("server 已就绪。输入 help 查看命令。")

    while True:
        if current:
            prompt = f"[{current.hostname}]> "
        else:
            prompt = "[未选择]> "

        try:
            line = inp(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            out("")
            break

        if not line:
            continue

        if line in ("exit", "quit"):
            break
        elif line in ("help", "?"):
            out(HELP)
        elif line == "list":
            list_agents(out)
        elif line.startswith("use "):
            spec = line[4:].strip()
            ag = select_agent(spec, out)
            if ag:
                current = ag
                out(f"已切换到: {current.hostname}")
        else:
            # 当作 shell 命令发给当前 Agent
            if current is None:
                out("请先用 'use <n>' 选择一个 Agent，或等待 Agent 连接")
                continue
            if not current.alive:
                out("当前 Agent 已断开，请重新 use")
                current = None
                continue
            result = current.exec(line)
            if result.get("stdout"):
                out(result["stdout"] if result["stdout"].endswith("\n") else result["stdout"] + "\n", end="")
            if result.get("stderr"):
                out(f"\033[33m[stderr]\033[0m {result['stderr']}")
            rc = result.get("returncode", 0)
            if rc != 0:
                out(f"\033[31m[exit {rc}]\033[0m")


# ── Unix socket 控制通道（non-interactive 模式用）──────────
def _handle_attach(conn: socket.socket):
    """处理一个 attach 客户端连接，提供完整的交互式会话"""
    f = conn.makefile("rwb", buffering=0)

    def out(text, end="\n"):
        try:
            line = text if text.endswith("\n") else text + end
            f.write(line.encode())
            f.flush()
        except Exception:
            pass

    def inp(prompt=""):
        try:
            f.write(prompt.encode())
            f.flush()
            data = b""
            while True:
                ch = f.read(1)
                if not ch or ch == b"\n":
                    break
                data += ch
            return data.decode("utf-8", errors="replace").rstrip("\r")
        except Exception:
            raise EOFError

    try:
        interactive(out=out, inp=inp)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ctrl_accept_loop(ctrl_sock: socket.socket):
    """接受 attach 连接，每个连接开一个线程"""
    while True:
        try:
            conn, _ = ctrl_sock.accept()
        except Exception:
            break
        t = threading.Thread(target=_handle_attach, args=(conn,), daemon=True)
        t.start()


def start_ctrl_socket(path: str) -> socket.socket:
    """创建并监听控制 Unix socket"""
    if os.path.exists(path):
        os.remove(path)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(path)
    sock.listen(5)
    t = threading.Thread(target=ctrl_accept_loop, args=(sock,), daemon=True)
    t.start()
    log.info(f"控制 socket 监听: {path}")
    return sock


# ── attach 客户端 ───────────────────────────────────────────
def do_attach(path: str):
    """连接到后台 server 的控制 socket，进行交互"""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(path)
    except FileNotFoundError:
        print(f"找不到控制 socket: {path}，server 是否已用 --non-interactive 启动？")
        sys.exit(1)
    except ConnectionRefusedError:
        print(f"连接被拒绝: {path}，server 可能已退出")
        sys.exit(1)

    f = s.makefile("rwb", buffering=0)

    # 两个线程：一个读 server 输出并打印，一个读本地输入并发送
    stop = threading.Event()

    def reader():
        try:
            while not stop.is_set():
                ch = f.read(1)
                if not ch:
                    break
                sys.stdout.buffer.write(ch)
                sys.stdout.buffer.flush()
        finally:
            stop.set()

    t = threading.Thread(target=reader, daemon=True)
    t.start()

    try:
        while not stop.is_set():
            try:
                line = input()
            except (EOFError, KeyboardInterrupt):
                break
            try:
                f.write((line + "\n").encode())
                f.flush()
            except Exception:
                break
    finally:
        stop.set()
        s.close()


def main():
    parser = argparse.ArgumentParser(
        description="Server: 运行在控制机B，等待 Agent 连接",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    # attach 子命令
    attach_parser = subparsers.add_parser("attach", help="连接到后台运行的 server，进入交互")
    attach_parser.add_argument("--ctrl-sock", default=DEFAULT_CTRL_SOCK,
                               help=f"控制 socket 路径（默认 {DEFAULT_CTRL_SOCK}）")

    # 默认启动参数（加在主 parser 上）
    parser.add_argument("--bind", default="127.0.0.1",
                        help="监听地址（默认 127.0.0.1）")
    parser.add_argument("--port", type=int, default=9876, help="监听端口（默认9876）")
    parser.add_argument("--non-interactive", action="store_true",
                        help="后台运行，不进入交互式界面，通过 attach 子命令连接操作")
    parser.add_argument("--ctrl-sock", default=DEFAULT_CTRL_SOCK,
                        help=f"控制 socket 路径（默认 {DEFAULT_CTRL_SOCK}，仅 --non-interactive 时有效）")

    args = parser.parse_args()

    if args.command == "attach":
        do_attach(args.ctrl_sock)
        return

    # 启动 TCP 监听
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((args.bind, args.port))
    server_sock.listen(10)
    log.info(f"监听 {args.bind}:{args.port}，等待 Agent 连接...")

    t = threading.Thread(target=accept_loop, args=(server_sock,), daemon=True)
    t.start()

    if args.non_interactive:
        start_ctrl_socket(args.ctrl_sock)
        print(f"server 后台运行中。使用以下命令进入交互：")
        print(f"  python3 server.py attach --ctrl-sock {args.ctrl_sock}")
        # 主线程保持运行
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass
    else:
        interactive()

    server_sock.close()
    log.info("Server 已退出")


if __name__ == "__main__":
    main()
