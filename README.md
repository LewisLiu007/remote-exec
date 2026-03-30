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
pip install -r requirements.txt
```

## 使用方法

### 第一步：在机器B上启动 server.py

**前台交互模式（默认）：**
```bash
python3 server.py
```

**后台静默模式：**
```bash
python3 server.py --non-interactive
# server 后台运行中。使用以下命令进入交互：
#   python3 server.py attach

# 随时 attach 进入操作
python3 server.py attach
```

`attach` 退出后 server 继续在后台运行，可多次 attach。

### 第二步：在机器A上启动 agent.py

```bash
# 使用 ~/.ssh/config 中配置的 host alias（无需指定用户名和密钥）
python3 agent.py my-host-alias

# 手动指定连接参数
python3 agent.py <机器B的IP> -u <用户名> -k ~/.ssh/id_rsa

# 自定义 SSH 端口和隧道端口
python3 agent.py <机器B的IP> -p 2222 -u user -k ~/.ssh/id_rsa --tunnel-port 9876

# 输出调试日志
python3 agent.py my-host-alias --debug
```

### 第三步：在 server 终端中执行命令

```
server 已就绪。输入 help 查看命令。
[未选择]> list
  [1] my-machine-A  (('127.0.0.1', 54321))
[未选择]> use 1
已切换到: my-machine-A
[my-machine-A]> whoami
root
[my-machine-A]> cd /tmp
[my-machine-A]> pwd
/tmp
[my-machine-A]> ls
...
```

工作目录在命令间持久保留，`cd` 会影响后续所有命令。

## 参数说明

### agent.py

| 参数 | 说明 | 默认值 |
|------|------|--------|
| host | 机器B的SSH地址或 ~/.ssh/config 中的 alias | 必填 |
| -p / --port | SSH端口 | 22 |
| -u / --user | SSH用户名 | 从 ~/.ssh/config 读取 |
| -k / --key | SSH私钥路径 | 从 ~/.ssh/config 读取 |
| --password | SSH密码 | - |
| --tunnel-port | B上server.py的监听端口 | 9876 |
| --no-reconnect | 断线后不自动重连 | 否 |
| --debug | 输出详细日志（默认静默） | 否 |

### server.py

| 参数 | 说明 | 默认值 |
|------|------|--------|
| --bind | 监听地址 | 127.0.0.1 |
| --port | 监听端口 | 9876 |
| --non-interactive | 后台运行，不进入交互界面 | 否 |
| --ctrl-sock | attach 控制 socket 路径 | /tmp/remote-exec.sock |

### server.py attach

| 参数 | 说明 | 默认值 |
|------|------|--------|
| --ctrl-sock | 连接的控制 socket 路径 | /tmp/remote-exec.sock |

## 安全说明

- server.py 默认只监听 `127.0.0.1`，外部无法直连，流量通过SSH加密隧道传输
- 推荐使用SSH密钥认证，优先通过 `~/.ssh/config` 配置
- agent 断线后自动重连，可用 `--no-reconnect` 禁用
