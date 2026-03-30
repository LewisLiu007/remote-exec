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


def run_command(cmd: str, timeout: int = 60) -> dict:
    """在本机执行 shell 命令，返回 stdout/stderr/returncode，并维护跨命令的工作目录"""
    global _cwd
    try:
        # 将用户命令包装成：执行命令后打印实际 cwd，以 \x00 分隔
        wrapped = f'cd {_shlex_quote(_cwd)} && {cmd}; printf "\\x1e%s" "$(pwd)"'
        result = subprocess.run(
            wrapped,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        stdout = result.stdout
        # 解析末尾的 \x1e<newcwd>
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
    """安全地引用路径用于 shell"""
    return shlex.quote(s)


def agent_loop(channel: paramiko.Channel):
    """主循环：读取命令行，执行，返回结果"""
    buf = ""
    channel.settimeout(HEARTBEAT_INTERVAL + 5)

    while True:
        try:
            data = channel.recv(4096)
        except socket.timeout:
            # 超时说明B端没发数据，发心跳
            try:
                channel.send(json.dumps({"type": "heartbeat"}) + "\n")
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

        # 按行处理（每条消息以 \n 结尾）
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
                # B端发来心跳，回应
                channel.send(json.dumps({"type": "heartbeat"}) + "\n")

            elif mtype == "exec":
                cmd = msg.get("cmd", "")
                req_id = msg.get("id", "")
                timeout = msg.get("timeout", 60)
                log.info(f"执行命令 [{req_id}]: {cmd!r}")
                result = run_command(cmd, timeout=timeout)
                result["type"] = "result"
                result["id"] = req_id
                channel.send(json.dumps(result) + "\n")

            elif mtype == "ping":
                channel.send(json.dumps({"type": "pong", "id": msg.get("id", "")}) + "\n")

            else:
                log.warning(f"未知消息类型: {mtype}")


def load_ssh_config(alias: str) -> dict:
    """从 ~/.ssh/config 读取 alias 对应的配置，返回 dict"""
    config_path = Path.home() / ".ssh" / "config"
    cfg = paramiko.SSHConfig()
    if config_path.exists():
        with config_path.open() as f:
            cfg.parse(f)
    return cfg.lookup(alias)


def connect_and_run(args):
    """建立SSH连接，打开隧道通道，进入代理循环"""
    # 读取 SSH config，以 args.host 作为 alias 查询
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
        # identityfile 可能是列表
        id_files = ssh_cfg["identityfile"]
        connect_kwargs["key_filename"] = [os.path.expanduser(f) for f in id_files]
    if args.password:
        connect_kwargs["password"] = args.password

    log.info(f"连接到 {username}@{hostname}:{port} ...")
    ssh.connect(**connect_kwargs)
    log.info("SSH 已连接")

    transport = ssh.get_transport()
    transport.set_keepalive(10)

    # 打开一个直接 TCP 通道到 B 上的 server.py 监听端口
    channel = transport.open_channel(
        "direct-tcpip",
        dest_addr=("127.0.0.1", args.tunnel_port),
        src_addr=("127.0.0.1", 0),
    )
    log.info(f"通道已打开（B:127.0.0.1:{args.tunnel_port}）")

    # 发送注册消息
    channel.send(json.dumps({"type": "register", "hostname": socket.gethostname()}) + "\n")

    agent_loop(channel)

    channel.close()
    ssh.close()
    log.info("连接已断开")


def main():
    parser = argparse.ArgumentParser(description="Agent: 运行在被控机A，连接到控制机B")
    parser.add_argument("host", help="机器B的 SSH 地址")
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
