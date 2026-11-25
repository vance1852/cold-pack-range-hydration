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
