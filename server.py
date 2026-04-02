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
import os
import select
import struct
import fcntl
import termios
import tty
import signal
import base64
import queue
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
DEFAULT_CTRL_SOCK = "/tmp/remote-exec.sock"

# 自动触发 PTY 模式的命令集合
PTY_COMMANDS = {
    "vim", "vi", "nvim", "nano", "emacs", "pico", "micro",
    "less", "more", "man",
    "top", "htop", "btop", "iotop", "atop",
    "crontab",
    "python3", "python", "python2", "ipython", "ipython3",
    "bash", "sh", "zsh", "fish", "dash",
    "ssh", "telnet", "mosh",
    "mysql", "psql", "sqlite3", "mongo", "redis-cli",
    "ftp", "sftp",
    "watch", "tmux", "screen",
}


def _needs_pty(line: str) -> tuple[bool, str]:
    """判断命令是否需要 PTY。! 前缀强制使用 PTY。返回 (use_pty, actual_cmd)。"""
    if line.startswith("!"):
        return True, line[1:].strip()
    tokens = line.split()
    if not tokens:
        return False, line
    basename = os.path.basename(tokens[0])
    return basename in PTY_COMMANDS, line


def _get_terminal_size() -> tuple[int, int]:
    """返回当前终端的 (cols, rows)，失败时返回 (80, 24)。"""
    try:
        buf = struct.pack("HHHH", 0, 0, 0, 0)
        result = fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, buf)
        rows, cols, _, _ = struct.unpack("HHHH", result)
        return cols or 80, rows or 24
    except Exception:
        return 80, 24


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
        # PTY 会话状态
        self._pty_data_queues: dict[str, queue.Queue] = {}
        self._pty_end_events: dict[str, threading.Event] = {}
        self._pty_end_results: dict[str, dict] = {}
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
            pass
        elif mtype == "result":
            req_id = msg.get("id", "")
            self._results[req_id] = msg
            ev = self._pending.get(req_id)
            if ev:
                ev.set()
        elif mtype == "pty_data":
            req_id = msg.get("id", "")
            q = self._pty_data_queues.get(req_id)
            if q:
                q.put(msg)
        elif mtype == "pty_end":
            req_id = msg.get("id", "")
            self._pty_end_results[req_id] = msg
            ev = self._pty_end_events.get(req_id)
            if ev:
                ev.set()
            q = self._pty_data_queues.get(req_id)
            if q:
                q.put(None)  # 通知消费者流已结束
        else:
            log.warning(f"未知消息: {msg}")

    def _heartbeat_loop(self):
        while self._alive:
            time.sleep(HEARTBEAT_INTERVAL)
            if self._alive:
                self.send({"type": "heartbeat"})

    # ── 执行普通命令 ────────────────────────────────────────
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

    # ── 执行 PTY 命令 ───────────────────────────────────────
    def exec_pty(self, cmd: str, cols: int, rows: int,
                 input_iter_fn, output_fn,
                 req_id_callback=None) -> dict:
        """
        在 PTY 中执行命令。
        input_iter_fn(): 返回 bytes（原始键盘输入），空 bytes 表示暂无输入，None 表示结束
        output_fn(bytes): 将 PTY 输出写到终端
        req_id_callback(str): 可选，将 req_id 传回调用方（用于发送 pty_resize）
        """
        req_id = str(uuid.uuid4())[:8]
        data_q: queue.Queue = queue.Queue()
        end_ev = threading.Event()
        self._pty_data_queues[req_id] = data_q
        self._pty_end_events[req_id] = end_ev

        if req_id_callback:
            req_id_callback(req_id)

        self.send({"type": "pty_start", "id": req_id,
                   "cmd": cmd, "cols": cols, "rows": rows})

        # 转发键盘输入的后台线程
        def stdin_forwarder():
            while not end_ev.is_set():
                try:
                    chunk = input_iter_fn()
                except Exception:
                    break
                if chunk is None:
                    break
                if chunk:
                    self.send({"type": "pty_input", "id": req_id,
                               "data": base64.b64encode(chunk).decode("ascii")})
            # 发送 abort 通知 agent 侧可以清理
            self.send({"type": "pty_input", "id": req_id, "data": "",
                       "abort": True})

        fwd_thread = threading.Thread(target=stdin_forwarder, daemon=True)
        fwd_thread.start()

        # 主循环：消费 pty_data 队列写到终端
        while True:
            try:
                item = data_q.get(timeout=0.5)
            except queue.Empty:
                if end_ev.is_set():
                    break
                continue
            if item is None:
                break
            raw = base64.b64decode(item.get("data", ""))
            if raw:
                try:
                    output_fn(raw)
                except Exception:
                    break

        end_ev.wait(timeout=30)
        result = self._pty_end_results.pop(req_id, {"returncode": -1, "final_cwd": ""})
        self._pty_data_queues.pop(req_id, None)
        self._pty_end_events.pop(req_id, None)
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
        time.sleep(0.5)
        with agents_lock:
            agents[agent.hostname] = agent
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
        try:
            idx = int(spec) - 1
            if 0 <= idx < len(keys):
                return agents[keys[idx]]
        except ValueError:
            pass
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
  !<shell命令>          强制使用 PTY 模式执行（适用于任意交互式命令）
  exit / quit           退出（后台模式下仅断开 attach，server 继续运行）
  help / ?              显示此帮助

PTY 模式自动触发：vim/nano/less/top/htop/crontab/python 等交互式命令
Ctrl+] 强制终止当前 PTY 会话
"""


def interactive(out=None, inp=None, default_agent: AgentConn | None = None,
                is_tty: bool = False, raw_sock=None):
    """
    交互式 REPL。
    out: 输出函数
    inp: 输入函数（返回字符串行）
    is_tty: 是否运行在真实终端上（决定能否进入 raw 模式）
    raw_sock: attach 模式下的 Unix socket（用于 PTY 原始 I/O）
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
            if current is None:
                out("请先用 'use <n>' 选择一个 Agent，或等待 Agent 连接")
                continue
            if not current.alive:
                out("当前 Agent 已断开，请重新 use")
                current = None
                continue

            use_pty, actual_cmd = _needs_pty(line)

            if use_pty:
                if is_tty:
                    _exec_pty_direct(current, actual_cmd)
                elif raw_sock is not None:
                    _exec_pty_attach(current, actual_cmd, raw_sock)
                else:
                    out("[PTY] 当前模式不支持交互式命令，请使用直连模式或 attach 模式")
            else:
                result = current.exec(line)
                if result.get("stdout"):
                    out(result["stdout"] if result["stdout"].endswith("\n")
                        else result["stdout"] + "\n", end="")
                if result.get("stderr"):
                    out(f"\033[33m[stderr]\033[0m {result['stderr']}")
                rc = result.get("returncode", 0)
                if rc != 0:
                    out(f"\033[31m[exit {rc}]\033[0m")


def _exec_pty_direct(agent: AgentConn, cmd: str):
    """直连交互式模式下的 PTY 执行：切换终端到 raw 模式，转发 I/O。"""
    cols, rows = _get_terminal_size()

    # 保存终端状态
    stdin_fd = sys.stdin.fileno()
    old_tc = termios.tcgetattr(stdin_fd)
    tty.setraw(stdin_fd)

    # 屏蔽 SIGINT（Ctrl+C 作为原始字节转发），暂存 SIGWINCH
    old_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
    resize_ev = threading.Event()
    cur_size = [cols, rows]
    req_id_box: list[str] = []

    def _winch(s, f):
        c, r = _get_terminal_size()
        cur_size[0], cur_size[1] = c, r
        resize_ev.set()

    old_winch = signal.signal(signal.SIGWINCH, _winch)

    def input_iter_fn():
        # 发送待处理的 resize
        if resize_ev.is_set() and req_id_box:
            resize_ev.clear()
            agent.send({"type": "pty_resize", "id": req_id_box[0],
                        "cols": cur_size[0], "rows": cur_size[1]})
        r, _, _ = select.select([sys.stdin], [], [], 0.05)
        if r:
            data = os.read(stdin_fd, 256)
            # Ctrl+] (0x1d) 强制终止
            if b"\x1d" in data:
                if req_id_box:
                    agent.send({"type": "pty_input", "id": req_id_box[0],
                                "data": "", "abort": True})
                return None
            return data
        return b""

    def output_fn(data: bytes):
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()

    try:
        result = agent.exec_pty(cmd, cols, rows, input_iter_fn, output_fn,
                                req_id_callback=lambda rid: req_id_box.append(rid))
        rc = result.get("returncode", 0)
        sys.stdout.write("\r\n")
        if rc != 0:
            sys.stdout.write(f"\033[31m[exit {rc}]\033[0m\r\n")
        sys.stdout.flush()
    finally:
        termios.tcsetattr(stdin_fd, termios.TCSAFLUSH, old_tc)
        signal.signal(signal.SIGWINCH, old_winch)
        signal.signal(signal.SIGINT, old_sigint)


def _exec_pty_attach(agent: AgentConn, cmd: str, raw_sock: socket.socket):
    """
    attach 模式下的 PTY 执行：通过 Unix socket 转发原始 PTY 字节流。
    协议：
      server → client: 原始 PTY 输出字节（直接写入）
      client → server: 原始键盘字节，或 resize 帧 \x1e{"resize":[r,c]}\x1e
    """
    cols, rows = _get_terminal_size()
    req_id_box: list[str] = []
    end_ev = threading.Event()

    # 从 attach socket 读取输入的缓冲区（处理 resize 帧）
    sock_buf = bytearray()
    sock_file = raw_sock.makefile("rwb", buffering=0)

    RESIZE_START = b"\x1e"
    RESIZE_END = b"\x1e"

    def input_iter_fn():
        nonlocal sock_buf
        if end_ev.is_set():
            return None
        try:
            raw_sock.settimeout(0.05)
            chunk = raw_sock.recv(256)
        except socket.timeout:
            return b""
        except Exception:
            return None

        if not chunk:
            return None

        sock_buf += chunk

        # 扫描 resize 帧 \x1e{...}\x1e
        while True:
            start = sock_buf.find(b"\x1e")
            if start == -1:
                break
            end = sock_buf.find(b"\x1e", start + 1)
            if end == -1:
                break
            frame_data = sock_buf[start + 1:end]
            # 验证是否为 JSON resize 帧
            try:
                frame = json.loads(frame_data.decode())
                if "resize" in frame and req_id_box:
                    r, c = frame["resize"]
                    agent.send({"type": "pty_resize", "id": req_id_box[0],
                                "cols": c, "rows": r})
                del sock_buf[start:end + 1]
                continue
            except (json.JSONDecodeError, UnicodeDecodeError):
                # 不是 resize 帧，保留原始字节
                break
            break

        # 检测 Ctrl+] 强制终止
        if b"\x1d" in sock_buf:
            if req_id_box:
                agent.send({"type": "pty_input", "id": req_id_box[0],
                            "data": "", "abort": True})
            return None

        result = bytes(sock_buf)
        sock_buf.clear()
        return result if result else b""

    def output_fn(data: bytes):
        try:
            raw_sock.sendall(data)
        except Exception:
            end_ev.set()

    result = agent.exec_pty(cmd, cols, rows, input_iter_fn, output_fn,
                            req_id_callback=lambda rid: req_id_box.append(rid))
    rc = result.get("returncode", 0)
    try:
        msg = f"\r\n\033[31m[exit {rc}]\033[0m\r\n" if rc != 0 else "\r\n"
        raw_sock.sendall(msg.encode())
    except Exception:
        pass


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
        interactive(out=out, inp=inp, is_tty=False, raw_sock=conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def ctrl_accept_loop(ctrl_sock: socket.socket):
    while True:
        try:
            conn, _ = ctrl_sock.accept()
        except Exception:
            break
        t = threading.Thread(target=_handle_attach, args=(conn,), daemon=True)
        t.start()


def start_ctrl_socket(path: str) -> socket.socket:
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
    stop = threading.Event()

    # 注册 SIGWINCH，编码为 resize 帧发送给 server
    def _winch_handler(signum, frame):
        try:
            buf = struct.pack("HHHH", 0, 0, 0, 0)
            result = fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, buf)
            rows, cols, _, _ = struct.unpack("HHHH", result)
            frame_data = json.dumps({"resize": [rows or 24, cols or 80]}).encode()
            s.sendall(b"\x1e" + frame_data + b"\x1e")
        except Exception:
            pass

    old_winch = signal.signal(signal.SIGWINCH, _winch_handler)

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
        signal.signal(signal.SIGWINCH, old_winch)
        s.close()


def main():
    parser = argparse.ArgumentParser(
        description="Server: 运行在控制机B，等待 Agent 连接",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    attach_parser = subparsers.add_parser("attach", help="连接到后台运行的 server，进入交互")
    attach_parser.add_argument("--ctrl-sock", default=DEFAULT_CTRL_SOCK,
                               help=f"控制 socket 路径（默认 {DEFAULT_CTRL_SOCK}）")

    parser.add_argument("--bind", default="127.0.0.1", help="监听地址（默认 127.0.0.1）")
    parser.add_argument("--port", type=int, default=9876, help="监听端口（默认9876）")
    parser.add_argument("--non-interactive", action="store_true",
                        help="后台运行，不进入交互式界面，通过 attach 子命令连接操作")
    parser.add_argument("--ctrl-sock", default=DEFAULT_CTRL_SOCK,
                        help=f"控制 socket 路径（默认 {DEFAULT_CTRL_SOCK}，仅 --non-interactive 时有效）")
    parser.add_argument("--log-file", default="/tmp/remote-exec-server.log",
                        help="后台模式下的日志文件路径（默认 /tmp/remote-exec-server.log）")

    args = parser.parse_args()

    if args.command == "attach":
        do_attach(args.ctrl_sock)
        return

    if args.non_interactive:
        # 立即 detach，在任何 I/O 之前脱离控制终端
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        signal.signal(signal.SIGTTIN, signal.SIG_IGN)
        try:
            os.setsid()
        except OSError:
            pass
        devnull = os.open(os.devnull, os.O_RDWR)
        for fd in (0, 1, 2):
            os.dup2(devnull, fd)
        os.close(devnull)
        # 重建 logging，写到文件
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [server] %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
            filename=args.log_file,
        )

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((args.bind, args.port))
    server_sock.listen(10)
    log.info(f"监听 {args.bind}:{args.port}，等待 Agent 连接...")

    t = threading.Thread(target=accept_loop, args=(server_sock,), daemon=True)
    t.start()

    if args.non_interactive:
        start_ctrl_socket(args.ctrl_sock)
        try:
            while True:
                time.sleep(3600)
        except (KeyboardInterrupt, SystemExit):
            pass
    else:
        interactive(is_tty=sys.stdin.isatty())

    server_sock.close()
    log.info("Server 已退出")


if __name__ == "__main__":
    main()
