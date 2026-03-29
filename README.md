# remote-exec

机器A主动SSH连到机器B，机器B通过交互式终端向机器A发送任意命令并获取结果。

## 架构

```
机器A (agent.py)  ──SSH──→  机器B (server.py)
    │                           │
    │   direct-tcpip 通道       │
    └──────────────────────────→ 127.0.0.1:9876
```

- `agent.py`：运行在被控机A，主动发起SSH连接，通过 `direct-tcpip` 隧道与 server.py 通信
- `server.py`：运行在控制机B，监听本地端口，提供交互式命令行

## 安装依赖

```bash
pip install paramiko
# 或
pip install -r requirements.txt
```

## 使用方法

### 第一步：在机器B上启动 server.py

```bash
python3 server.py
# 默认监听 127.0.0.1:9876
```

### 第二步：在机器A上启动 agent.py

```bash
# 使用 SSH 密钥
python3 agent.py <机器B的IP> -u <用户名> -k ~/.ssh/id_rsa

# 使用密码（不推荐）
python3 agent.py <机器B的IP> -u <用户名> --password <密码>

# 自定义 SSH 端口和隧道端口
python3 agent.py <机器B的IP> -p 2222 -u user -k ~/.ssh/id_rsa --tunnel-port 9876
```

### 第三步：在机器B的 server.py 终端中执行命令

```
server 已就绪。输入 help 查看命令。
[未选择]> list
  [1] my-machine-A  (('127.0.0.1', 54321))
[未选择]> use 1
已切换到: my-machine-A
[my-machine-A]> whoami
root
[my-machine-A]> uname -a
Linux my-machine-A 5.15.0 ...
[my-machine-A]> ls /tmp
...
```

## 参数说明

### agent.py

| 参数 | 说明 | 默认值 |
|------|------|--------|
| host | 机器B的SSH地址 | 必填 |
| -p / --port | SSH端口 | 22 |
| -u / --user | SSH用户名 | 必填 |
| -k / --key | SSH私钥路径 | 系统默认 |
| --password | SSH密码 | - |
| --tunnel-port | B上server.py的监听端口 | 9876 |
| --no-reconnect | 断线后不自动重连 | 否 |

### server.py

| 参数 | 说明 | 默认值 |
|------|------|--------|
| --bind | 监听地址 | 127.0.0.1 |
| --port | 监听端口 | 9876 |

## 安全说明

- server.py 默认只监听 `127.0.0.1`，外部无法直连，流量通过SSH加密隧道传输
- 推荐使用SSH密钥认证，不要明文传递密码
- agent 断线后自动重连，可用 `--no-reconnect` 禁用
