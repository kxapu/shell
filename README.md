# Shell 游戏服务器框架

**Shell** 是一款使用Rust语言编写的**轻量级**、**高性能**的游戏服务器框架。它专为高并发、低延迟的实时交互场景（如 MMO、MOBA、棋牌、聊天室等）设计。

### 系统架构
**Shell** 采用经典的分层架构设计：
```
┌─────────────────────────────────────────────────────────────┐
│                      Application Layer                      │
│   (Chat Room, Battle Logic, Inventory, Guild, etc.)         │
├─────────────────────────────────────────────────────────────┤
│                       Core Framework                        │
│  ┌──────────┐         ┌──────────┐          ┌──────────┐    │
│  │  Router  │         │  Actor   │          │  Timer   │    │
│  │（消息路由）│         │（Actor池）│          │ (定时器)  │    │
│  └────┬─────┘         └────┬─────┘          └────┬─────┘    │
│       └────────────────────┴─────────────────────┘          │
├─────────────────────────────────────────────────────────────┤
│                      Network Layer (Gate)                   │
│        ┌────────────────┐      ┌────────────────┐           │
│        │  TCP Server    │      │ WebSocket Srv  │           │
│        │ (Length-prefix)│      │ (Binary/Text)  │           │
│        └────────────────┘      └────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### 核心概念
##### 1. Actor (模块)
在**Shell**中，所有的业务逻辑都被封装在 `Actor` 中。每个 `Actor` 拥有独立的消息队列（`mpsc::channel`），通过 `MessageHandler trait` 处理消息。`Actor` 之间不共享状态，彻底避免了锁竞争。

##### 2. Message & Router (消息与路由)
- `Message`：统一的消息载体，包含 `msg_id`、`session_id` 和 `payload`。
- `Route`r：消息的分发中心。当网络层收到消息时，Router 会根据`msg_id`将消息精准投递到目标 Actor 的队列中。

##### 3. Session (会话)
`Session` 代表一个客户端连接。`SessionManager` 使用无锁并发哈希表（`DashMap`）管理所有在线会话，支持会话数据的动态绑定、单播、广播和条件广播。

##### 4. Timer (定时器)
内置基于最小堆（`BinaryHeap`）的定时器服务，支持一次性延迟和周期性循环任务，精度可达毫秒级，且不会阻塞主线程。

### 网络协议
**Shell** TCP 网关默认采用大端序（Big-Endian）长度前缀的二进制协议：
| 字段 | 长度 | 说明 |
| :--- | :--- | :--- |
| MagicNum | 2 Bytes| 固定值0x7368 |
| MsgID | 2 Bytes | 消息类型标识 (u16) |
| Length | 4 Bytes | 后续数据的总长度 (u32) |
| SessionID| 8 Bytes | 客户端会话ID (u64)，由服务器在接收时自动填充 |
| Payload | N Bytes | 消息体数据 (推荐使用 bincode, protobuf 或 json) |


### 快速入门
##### 1. 环境准备
- **Rust 工具链**：rustc 1.70+ (推荐使用 rustup 安装)
- **操作系统**：Linux (推荐 Ubuntu 22.04+), macOS, Windows

##### 2. 编译与运行
```sh
# 1. 克隆项目
git clone https://github.com/kxapu/shell.git
cd shell

# 2. 编译 Debug 版本
cargo build

# 3. 运行示例游戏服务器
cargo run --example game-server

# 4. 运行示例客户端
python3 examples/chat-client.py
```
