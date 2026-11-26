# 冷归档包的按段回温与在途读取

存储联调使用合成设备、时间轴和原始字节。设备记录由各客户端及介质的输入组成，每份记录可独立使用。

## 业务协议

逻辑范围使用从零起的 `offset` 与 `length`。索引将 `version` 下的区间对应至若干 `segments`，单段内容由给定摘要核对。设备分别限制 `cold_window_ms` 内的物理读取字节和 `parallel` 个同时处理的读取。每次读取成本是介质实际承诺的整段字节，和用户只要几个字节无关。
`POST /media/{pack}/read/{segment}` 申请取回这个物理段，`GET /media/{pack}/read/{segment}` 查询受理并供取得 `file`。同一段在不同 `pack` 下属于不同物理原件。 `hot_bytes` 约束热盘实际及已为活跃读答应的空间。读取的 `wait_ms` 相对于请求到达，带 `priority=field` 的是现场紧急读取。

## 本地设备资料

运行环境为 Python 3.11.2，使用标准库。设备资料端默认仅在 127.0.0.1:8090 运行，使用 --port 可另设端口。

~~~sh
python3 device_fixture.py fixtures/shared-segment-and-expiring-reader
~~~

GET /fixture/events 返回设备参数、字节目录和按到达顺序记录的输入。同一时刻按文件先后排列。GET /fixture/objects/文件名 取原始字节。GET /fixture/journal 查看资料端已收到的设备请求，每个请求记录实际正文长度和摘要。

events.json 中的 packets 是可重放的设备响应。资料端按实际 HTTP 方法、路径和完整原始请求正文匹配第一条尚未使用的记录。没有匹配时返回 404。lag_ms 是回包延迟，drop 代表设备收到请求后直接断开连接，接收方需要以其他报告确认结果。没有 HTTP 交互的设备和客户端动作直接取 events。重新启动资料端会回到记录开头。

在项目根目录运行 python3 -m unittest -v 检查事件、对应字节、设备响应及接收日志。

## 冷资料读取服务

仓库另提供仅依赖 Python 3.11 标准库的冷资料读取服务：

~~~sh
# 终端 1：启动合成介质/索引设备
python3 device_fixture.py fixtures/shared-segment-and-expiring-reader --port 8090

# 终端 2：启动读取服务，状态和热盘缓存落在 .cold-reader-state/
python3 cold_reader.py \
  --fixture fixtures/shared-segment-and-expiring-reader \
  --media-url http://127.0.0.1:8090 \
  --state-dir .cold-reader-state \
  --port 8080
~~~

也可以直接重放资料中的时间轴（会在进程内启动 fixture）：

~~~sh
python3 -m coldarchive.replay fixtures/shared-segment-and-expiring-reader
~~~

### 读取 API

提交范围读取；同一 `request_id` 可持续轮询：

~~~sh
curl -sS http://127.0.0.1:8080/v1/reads \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "crew",
    "version": "map/v1",
    "offset": 10,
    "length": 4,
    "wait_ms": 240,
    "priority": "field"
  }'

curl -sS 'http://127.0.0.1:8080/v1/reads/crew?wait_ms=100&cursor=0'
curl -sS -X POST http://127.0.0.1:8080/v1/reads/crew/close
~~~

Python 嵌入用法：

```python
from cold_reader import ColdReaderService, HttpMediaClient, IndexRegistry

registry, policy = IndexRegistry.from_fixture("fixtures/shared-segment-and-expiring-reader")
svc = ColdReaderService(
    HttpMediaClient("http://127.0.0.1:8090"),
    policy=policy,
    state_dir="/var/lib/cold-reader",
)

snapshot = svc.read(
    request_id="crew",
    version="map/v1",
    offset=10,
    length=4,
    wait_ms=240,
    priority="field",
)
for chunk in snapshot["chunks"]:
    print(chunk["offset"], chunk["length"], chunk["data"])

print(svc.status())
svc.close("crew")   # 释放该读取者的热盘承诺
svc.close()         # 关闭调度器并持久化状态
```

### 状态模型与可追踪原因

- 每个请求在创建时固定 `version -> pack/segment/digest`；之后切换当前索引不会改变在途请求。
- 物理段身份是 `pack/segment`；多个请求只触发一次实际取回，`promises` 是仍需要该段的活跃读取者。
- `cold_window_ms/cold_bytes` 按已承诺的整段长度做滑动窗口计量；POST 回执丢失也保留计量，并通过 GET 对账。
- `parallel` 限制 POST/GET/下载/校验中的介质 worker 数。
- `hot_bytes` 计入：已校验文件、下载中/预留位置、校验失败后尚未清理的隔离数据。没有活跃承诺的已校验段按 LRU 逐出；有承诺的段不会被逐出，也不会被覆盖（临时文件唯一，校验后原子发布到内容摘要路径）。
- 重启后，已校验且文件仍在的段进入 `recovery.verified_reusable`；所有 `posting/polling/fetching/...` 状态进入 `recovery.reconciling`，优先用 GET 对账而不是直接重新 POST。
- `GET /v1/ops/status`（以及 Python 的 `svc.status()`）给出介质窗口消耗、并行槽、累计承诺字节、热盘分类占用、每个活跃读取者占住的段和字节，以及 `blocked_ranges`。

每个不可读段都带有两组兼容分类：

- `category`：`capacity`、`corrupt`、`quota`、`in_transit`、`media_error`、`index`
- `blocker`：`capacity`、`corruption`、`quota`、`in_flight` 等更贴近运维口径的词
- `reason`：如 `hot_capacity`、`checksum_mismatch`、`parallel_limit`、`cold_window_quota`、`receipt_lost_reconciling`

损坏段可由运维显式清理：

~~~sh
curl -sS -X POST http://127.0.0.1:8080/v1/ops/corrupt/clear \
  -H 'Content-Type: application/json' \
  -d '{"key":"P1/c"}'
~~~

资料端的稀疏录制可能没有普通 POST 响应；HTTP 适配器收到 fixture 特有的 `{"state":"unmatched"}` 后会回退到幂等 GET，并在后续段记住该设备可直接 GET 对账。真实介质协议仍优先执行 `POST /media/{pack}/read/{segment}`。

