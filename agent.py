#!/usr/bin/env python3
"""
agent.py — 运行在机器A上
通过SSH连接到机器B，建立长连接，接收并执行命令，返回结果
"""

import os
import sys
import time
import json
import shlex
import select
import struct
import fcntl
import termios
import pty
import base64
import queue
import errno
import subprocess
import threading
import socket
import paramiko
import argparse
import logging
from pathlib import Path

log = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 15   # 秒
RECONNECT_DELAY = 5        # 断线后等待秒数

_cwd = os.getcwd()
_pty_sessions: dict[str, queue.Queue] = {}
_pty_sessions_lock = threading.Lock()


def run_command(cmd: str, timeout: int = 60) -> dict:
    """在本机执行 shell 命令，返回 stdout/stderr/returncode，并维护跨命令的工作目录"""
    global _cwd
    try:
        wrapped = f'cd {_shlex_quote(_cwd)} && {cmd}; printf "\\x1e%s" "$(pwd)"'
        result = subprocess.run(
            wrapped,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        stdout = result.stdout
        if "\x1e" in stdout:
            output, new_cwd = stdout.rsplit("\x1e", 1)
            new_cwd = new_cwd.strip()
            if new_cwd:
                _cwd = new_cwd
        else:
            output = stdout
        return {
            "stdout": output,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {"stdout": "", "stderr": f"命令超时（>{timeout}s）", "returncode": -1}
    except Exception as e:
        return {"stdout": "", "stderr": str(e), "returncode": -1}


def _shlex_quote(s: str) -> str:
    return shlex.quote(s)


def run_pty_command(cmd: str, req_id: str, cols: int, rows: int,
                    input_q: queue.Queue, send_fn) -> None:
    """在 PTY 中执行命令，流式转发 I/O，完成后发送 pty_end。在独立线程中运行。"""
    global _cwd

    sentinel = b"\x1ePWD"
    wrapped = f'cd {_shlex_quote(_cwd)} && {cmd}; printf "\x1ePWD%s" "$(pwd)"'

    master_fd, slave_fd = pty.openpty()

    # 设置初始终端尺寸
    winsize = struct.pack("HHHH", rows, cols, 0, 0)
    fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)

    proc = subprocess.Popen(
        ["bash", "-c", wrapped],
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        preexec_fn=os.setsid,
        close_fds=True,
        env={**os.environ, "TERM": os.environ.get("TERM", "xterm-256color")},
    )
    os.close(slave_fd)

    final_cwd = _cwd

    def _flush_master():
        """读取 master_fd 所有剩余数据，处理 sentinel，发送 pty_data。"""
        nonlocal final_cwd
        while True:
            r, _, _ = select.select([master_fd], [], [], 0.05)
            if not r:
                break
            try:
                data = os.read(master_fd, 4096)
            except OSError:
                break
            if not data:
                break
            # 检测 sentinel
            if sentinel in data:
                idx = data.index(sentinel)
                prefix = data[:idx]
                cwd_bytes = data[idx + len(sentinel):]
                cwd_str = cwd_bytes.decode("utf-8", errors="replace").strip()
                if cwd_str:
                    final_cwd = cwd_str
                if prefix:
                    send_fn({"type": "pty_data", "id": req_id,
                             "data": base64.b64encode(prefix).decode("ascii")})
                return  # sentinel 后的内容不发送
            send_fn({"type": "pty_data", "id": req_id,
                     "data": base64.b64encode(data).decode("ascii")})

    try:
        while True:
            # 先排空输入队列
            while True:
                try:
                    item = input_q.get_nowait()
                except queue.Empty:
                    break
                if item is None:
                    proc.kill()
                    break
                elif isinstance(item, dict):
                    if item.get("abort"):
                        proc.kill()
                    elif "resize" in item:
                        r, c = item["resize"]
                        ws = struct.pack("HHHH", r, c, 0, 0)
                        try:
                            fcntl.ioctl(master_fd, termios.TIOCSWINSZ, ws)
                        except OSError:
                            pass
                else:
                    try:
                        os.write(master_fd, item)
                    except OSError:
                        pass

            # 读取 PTY 输出
            r, _, _ = select.select([master_fd], [], [], 0.05)
            if not r:
                if proc.poll() is not None:
                    _flush_master()
                    break
                continue

            try:
                data = os.read(master_fd, 4096)
            except OSError as e:
                if e.errno in (errno.EIO, errno.EBADF):
                    _flush_master()
                    break
                break

            if not data:
                break

            # 检测 sentinel
            if sentinel in data:
                idx = data.index(sentinel)
                prefix = data[:idx]
                cwd_bytes = data[idx + len(sentinel):]
                cwd_str = cwd_bytes.decode("utf-8", errors="replace").strip()
                if cwd_str:
                    final_cwd = cwd_str
                if prefix:
                    send_fn({"type": "pty_data", "id": req_id,
                             "data": base64.b64encode(prefix).decode("ascii")})
                break  # sentinel 已收到，退出循环

            send_fn({"type": "pty_data", "id": req_id,
                     "data": base64.b64encode(data).decode("ascii")})

    finally:
        try:
            os.close(master_fd)
        except OSError:
            pass

    proc.wait()
    returncode = proc.returncode if proc.returncode is not None else -1
    _cwd = final_cwd

    send_fn({"type": "pty_end", "id": req_id,
             "returncode": returncode, "final_cwd": final_cwd})

    with _pty_sessions_lock:
        _pty_sessions.pop(req_id, None)


def agent_loop(channel: paramiko.Channel):
    """主循环：读取命令，执行，返回结果"""
    buf = ""
    channel.settimeout(HEARTBEAT_INTERVAL + 5)

    # 线程安全的发送函数（供 run_pty_command 线程复用）
    _send_lock = threading.Lock()

    def _channel_send(msg: dict):
        data = (json.dumps(msg) + "\n").encode()
        with _send_lock:
            try:
                channel.send(data)
            except Exception as e:
                log.warning(f"channel send failed: {e}")

    while True:
        try:
            data = channel.recv(4096)
        except socket.timeout:
            try:
                _channel_send({"type": "heartbeat"})
            except Exception:
                log.warning("心跳发送失败，连接可能已断开")
                break
            continue
        except Exception as e:
            log.warning(f"recv 异常: {e}")
            break

        if not data:
            log.info("连接已关闭")
            break

        buf += data.decode("utf-8", errors="replace")

        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                log.warning(f"无法解析消息: {line!r}")
                continue

            mtype = msg.get("type")

            if mtype == "heartbeat":
                _channel_send({"type": "heartbeat"})

            elif mtype == "exec":
                cmd = msg.get("cmd", "")
                req_id = msg.get("id", "")
                timeout = msg.get("timeout", 60)
                log.info(f"执行命令 [{req_id}]: {cmd!r}")
                result = run_command(cmd, timeout=timeout)
                result["type"] = "result"
                result["id"] = req_id
                _channel_send(result)

            elif mtype == "pty_start":
                cmd = msg.get("cmd", "")
                req_id = msg.get("id", "")
                cols = msg.get("cols", 80)
                rows = msg.get("rows", 24)
                log.info(f"PTY 会话 [{req_id}]: {cmd!r}")
                iq: queue.Queue = queue.Queue()
                with _pty_sessions_lock:
                    _pty_sessions[req_id] = iq
                t = threading.Thread(
                    target=run_pty_command,
                    args=(cmd, req_id, cols, rows, iq, _channel_send),
                    daemon=True,
                )
                t.start()

            elif mtype == "pty_input":
                req_id = msg.get("id", "")
                with _pty_sessions_lock:
                    iq = _pty_sessions.get(req_id)
                if iq:
                    raw = base64.b64decode(msg.get("data", ""))
                    if raw:
                        iq.put(raw)

            elif mtype == "pty_resize":
                req_id = msg.get("id", "")
                with _pty_sessions_lock:
                    iq = _pty_sessions.get(req_id)
                if iq:
                    iq.put({"resize": [msg.get("rows", 24), msg.get("cols", 80)]})

            elif mtype == "ping":
                _channel_send({"type": "pong", "id": msg.get("id", "")})

            else:
                log.warning(f"未知消息类型: {mtype}")


def load_ssh_config(alias: str) -> dict:
    """从 ~/.ssh/config 读取 alias 对应的配置"""
    config_path = Path.home() / ".ssh" / "config"
    cfg = paramiko.SSHConfig()
    if config_path.exists():
        with config_path.open() as f:
            cfg.parse(f)
    return cfg.lookup(alias)


def connect_and_run(args):
    """建立SSH连接，打开隧道通道，进入代理循环"""
    ssh_cfg = load_ssh_config(args.host)

    hostname = ssh_cfg.get("hostname", args.host)
    port = args.port if args.port != 22 else int(ssh_cfg.get("port", 22))
    username = args.user or ssh_cfg.get("user", os.getenv("USER", os.getenv("LOGNAME", "")))

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connect_kwargs = dict(
        hostname=hostname,
        port=port,
        username=username,
        timeout=15,
        banner_timeout=15,
    )
    if args.key:
        connect_kwargs["key_filename"] = os.path.expanduser(args.key)
    elif "identityfile" in ssh_cfg:
        id_files = ssh_cfg["identityfile"]
        connect_kwargs["key_filename"] = [os.path.expanduser(f) for f in id_files]
    if args.password:
        connect_kwargs["password"] = args.password

    log.info(f"连接到 {username}@{hostname}:{port} ...")
    ssh.connect(**connect_kwargs)
    log.info("SSH 已连接")

    transport = ssh.get_transport()
    transport.set_keepalive(10)

    channel = transport.open_channel(
        "direct-tcpip",
        dest_addr=("127.0.0.1", args.tunnel_port),
        src_addr=("127.0.0.1", 0),
    )
    log.info(f"通道已打开（B:127.0.0.1:{args.tunnel_port}）")

    channel.send(json.dumps({"type": "register", "hostname": socket.gethostname()}) + "\n")

    agent_loop(channel)

    channel.close()
    ssh.close()
    log.info("连接已断开")


def main():
    parser = argparse.ArgumentParser(description="Agent: 运行在被控机A，连接到控制机B")
    parser.add_argument("host", help="机器B的 SSH 地址或 ~/.ssh/config 中的 alias")
    parser.add_argument("-p", "--port", type=int, default=22, help="SSH 端口（默认22）")
    parser.add_argument("-u", "--user", default=None, help="SSH 用户名（不填则从 ~/.ssh/config 读取）")
    parser.add_argument("-k", "--key", default=None, help="SSH 私钥路径（默认用系统默认密钥）")
    parser.add_argument("--password", default=None, help="SSH 密码（不推荐，建议用密钥）")
    parser.add_argument("--tunnel-port", type=int, default=9876,
                        help="B 上 server.py 监听的本地端口（默认9876）")
    parser.add_argument("--no-reconnect", action="store_true", help="断线后不自动重连")
    parser.add_argument("--debug", action="store_true", help="输出调试日志")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.WARNING,
        format="%(asctime)s [agent] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    while True:
        try:
            connect_and_run(args)
        except KeyboardInterrupt:
            log.info("用户中断，退出")
            sys.exit(0)
        except Exception as e:
            log.error(f"连接失败: {e}")

        if args.no_reconnect:
            break

        log.info(f"{RECONNECT_DELAY}s 后重连...")
        time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    main()
