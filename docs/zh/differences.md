[English](../en/differences.md) | 简体中文

# 与 Sanic 的差异

框架层源自 Sanic 25.12.1，接口大体沿用 Sanic，路由、中间件、蓝图、信号等用法见 [Sanic 的文档](https://sanic.dev/)。本包不再追求与 Sanic 的公开接口一致，下面列出刻意的差异，oldman 等按 Sanic 写的代码据此调整；每处改动在同一个 commit 里改测试。

## 接口

- 包名 `oldman_server`，不再提供 `import sanic` 的别名。类名去掉 Sanic：`Sanic` → `OldmanServer`、`SanicException` → `OldmanServerException`、`DefaultSanic` → `DefaultServer`、`SanicTestClient` → `TestClient`（内部类同样去掉 Sanic 字样）。配置环境变量前缀 `SANIC_` → `OLDMAN_SERVER_`（如 `OLDMAN_SERVER_NATIVE_SERVER`；不用 `OLDMAN_`，因为 oldman 自己的 `OLDMAN_*` 变量会被读成设置）；日志名 `sanic.root` / `error` / `access` / `server` / `websockets` → `oldman_server.*`；进程名 `Sanic-Main`、`Sanic-Server-*` → `Oldman-Main`、`Oldman-Server-*`。HTML 错误页显示 oldman-server，不再带 Sanic 的标志和 sanic.dev 链接；颜色 `Colors.SANIC` → `Colors.PINK`。日志与报错里仍指向 sanic.dev 的说明，是原版 Sanic 的文档（框架层源自它）。
- 不支持 Sanic Extensions：没有 `app.ext`、`app.extend()` 与 `AUTO_EXTEND`，服务器启动时也不查找、不加载 sanic-ext（原版装了它就会自动扩展应用）。sanic-ext 导入的是 `sanic` 包本身，不能用在本包的应用上。
- 不再把 setuptools 列为运行依赖（包里没有用到它；原版 Sanic 列着）。
- 测试客户端随包提供为 `oldman_server.testing`（源自 sanic-testing 24.6.0），`app.test_client` 需要 httpx；不再需要 sanic-testing。
- 新增 `response.abort()`：对 `request.respond()` 得到的流式响应，在已发出的数据之后关闭连接、不完成响应（没有结束块，或少于 Content-Length），请求方因此知道内容不完整；之后再 `send()` 会抛出，handler 在下一次 await 被取消（与请求方断开时相同）。还没发出任何数据时抛 `ServerError`（这时应抛异常或返回错误响应），响应已结束时什么都不做。asyncio 服务器与原生主机行为相同；oldman 代理里的 `request.transport.get_protocol().close()`（在原生主机上不起作用）可以换成它。
- 新增 `@app.rewrite_path`：注册的函数在路由与所有中间件之前对每个请求执行，返回新路径（以 `/` 开头）或 None；按注册顺序依次执行。之后请求的 `path` 和路由都用新路径，查询串保留，`raw_url` 仍是请求方发来的。抛出的异常交给错误处理器。oldman 的 `I18nRequest`（重写私有的 `_parsed_url`）可以换成它：自定义请求类会让原生主机走一般路径，用这个钩子则保留快速路径。
- 新增设置 `UNIX_SOCKET_MODE`：服务器绑定的 unix socket（`unix=`、`NATIVE_WORKER_LISTEN`）的权限位，默认 0o666（与 Sanic 相同）；可写 `0o660` 或八进制字符串（环境变量写 `OLDMAN_SERVER_UNIX_SOCKET_MODE=0o660`）。不是 0–0o777 的值在绑定时报 `ValueError`。
- 新增设置 `TRUSTED_PROXIES`：地址或网段（列表，或逗号分隔的字符串，如 `"127.0.0.1, 10.0.0.0/8"`），`"unix"` 表示 unix socket 连接（能连上 socket 的进程都被信任，权限见 `UNIX_SOCKET_MODE`）。设置后，只有来自这些来源的连接，其 `Forwarded` / `X-Forwarded-*` 等转发头才按 `FORWARDED_SECRET`、`REAL_IP_HEADER`、`PROXIES_COUNT` 生效；其他连接的 `remote_addr` 为空，`client_ip`、`scheme`、`host` 取实际连接。双栈监听收到的 IPv4 映射地址（`::ffff:a.b.c.d`）按 IPv4 网段比较。格式错误在服务器启动时报 `ValueError`。不设置时与原版相同：转发头对所有连接都按上述设置生效。
- 新增 `HTTPMethodView.before_dispatch(request, *args, **kwargs)`：覆写后在方法 handler 之前以同样的参数执行（可为同步或协程），返回响应则直接用它回应，返回 None 才进入 handler；抛出的异常交给错误处理器。不覆写时没有额外开销。oldman 的 `HTTPMethodView` 不必再复制整个 `dispatch_request`，把登录与权限检查放进这里即可。
- 请求头的值去掉两端的空白（RFC 9110 的可选空白；原版只去掉开头，`X-A: v␠␠` 原版得到 `"v  "`），两种服务器相同；迭代 `request.headers` 得到的名字都是小写（原版的解析器本来如此，原生主机现在也一样）。
- 监听 socket 的 backlog 默认 2048（`oldman_server.constants.DEFAULT_BACKLOG`，与 Gunicorn、Uvicorn 相同），Sanic 为 100：nginx 等代理同时打开上百条上游连接时，unix socket 的 `connect()` 超出 backlog 即失败，nginx 回 502；仍可用 `run(backlog=...)` 指定，内核以 `net.core.somaxconn` 为上限。
- JSON 默认用 orjson（`json()`、`request.json`、JSON 错误页与 JSON 日志），不再用 ujson：
  - 输出为紧凑的 UTF-8（非 ASCII 字符不再转成 `\uXXXX`）；datetime、UUID、dataclass、enum 可直接编码；非 str 的键照旧转成字符串。
  - 超过 64 位的整数会抛 `TypeError`；`NaN`、`Infinity` 输出为 `null`（原版输出不合法的 `NaN`）；不认识的对象不再调用 `__json__` 或 `toDict()`，`Decimal`（数据库驱动的 DECIMAL 列）与含孤立代理项的字符串也会抛 `TypeError`，要编码它们就用 `json(..., default=...)`（如 `default=str`，或把 `Decimal` 转成 `float`）。
  - `request.json` 用 orjson 解析：超出 64 位的整数会变成 `float`（失去精度），`NaN`、`Infinity` 不被接受（400）；要精确的大整数就用 `OldmanServer(loads=json.loads)`。
  - JSON 与文本错误页里编码器不认识的上下文值显示为它的 `str()`，不会让错误响应本身变成 500。
  - `json()` 的额外关键字参数交给 orjson（`default`、`option`，如 `option=orjson.OPT_SORT_KEYS`）；`json_dumps` 返回 bytes。要换编码器仍可用 `json(..., dumps=...)` 或 `OldmanServer(dumps=..., loads=...)`。

## 已移除

- 命令行（`oldman-server` 命令与 `python -m oldman_server`，包括 `--simple` 静态文件服务器）：由 `app.run()` 或上层框架自己的命令启动，多进程加载应用照旧可用 `AppLoader`。
- 静态目录的列表页（`app.static(..., directory_view=True)` 与 `DirectoryHandler` 的 `directory_view`）：目录只按 `index` 回答索引文件。
- 启动信息（MOTD 与标志；`run()` / `prepare()` 的 `motd`、`motd_display`，设置 `MOTD`、`MOTD_DISPLAY`）：主进程开始服务时改为一行 INFO 日志（`oldman_server.root`），写出应用名、地址、运行方式与 worker 数。
- coffee 模式（`app.make_coffee()`、`prepare(coffee=...)`、`--coffee`，Sanic 标志的彩蛋）。
- REPL（`--repl`、`app.repl_ctx`）。
- 后台运行（`--daemon` 及 `kill` / `status` / `restart` / `stop` 命令）：进程管理交给 systemd 或 oldman 自己的服务命令。
- inspector（`INSPECTOR*` 设置、`app.inspector`、`inspect` 命令）。
- TLS（`run()` / `prepare()` / `create_server()` 的 `ssl`、`auto_tls`，`--cert` / `--key` / `--tls` / `--auto-tls`，`LOCAL_CERT_CREATOR` 等设置，`certloader_class`，`conn_info.ssl` / `cert` / `server_name`）与 HTTP/3（`version` 参数、`--http` / `-1` / `-3`、`request.stream_id`、`Alt-Svc`）：TLS 由前端代理终止，`request.scheme` 照旧取 `Forwarded` / `X-Forwarded-Proto`。
- ASGI：应用不再是 ASGI 可调用对象，`app.asgi`、`app.asgi_client`、`request.scope` 一并移除，只由本包自己的服务器运行。

## 行为修正

oldman 原先为这些打的补丁，切换后可以删除。

- worker 启动期间收到 SIGTERM 时，主进程结束服务，而不是一直等待 worker 确认（Sanic 25.12.1 会空转；oldman 的 `_wait_for_ack`）。
- worker 重启（`app.m.restart()`、自动重载、扩缩，包括零停机重启）进行中收到停止信号时，主进程在处理完手上这一步之后停止全部进程，包括重启刚启动的新进程与零停机重启还保留着的旧进程，然后退出；停止开始之后不再启动新进程。原版只停止信号到达那一刻的进程，重启出来的 worker 没人叫停，主进程就一直等着它。
- worker 在已确认启动之后、开始服务之前失败（`after_server_start`，或原生主机启动失败）时，主进程结束服务，而不是把这条消息当作要重启的进程名、留下没有 worker 的主进程（oldman 的 `_handle_message`）。只有启动阶段的失败才结束服务：已经服务过的 worker 在停止时（重启、缩减）出错只记日志；重启的 worker 在确认之前失败（如 auto-reload 读到有语法错误的文件）时记一条错误，其他 worker 照常服务（与原版相同）。服务器启动失败时 `app.run()` 抛出异常（原版只记日志、正常返回，退出码 0）。
- multiprocessing 的启动方式已被设为本包需要的那一种时不再报错（oldman 的 `prepare_process_context` 直接设 `START_METHOD_SET`）。
- 服务器监听器成对执行：`before_server_start` 失败时仍执行 `after_server_stop`；`after_server_start` 失败时照常停止服务器并执行 `before_server_stop`、`after_server_stop`；`before_server_stop` 失败时清理与 `after_server_stop` 仍执行；失败最后照样抛出。监听器抛出的 `RuntimeError` 也不再被当成事件循环提前停止而吞掉或再执行一次。（oldman 的 `_services_*_server_*` 里的 except 清理）
- 服务进程的 SIGINT / SIGTERM 处理在启动的各步骤（包括 `before_server_start`）之前就装好，并且是 Python 层的处理：启动期间收到的信号等启动完成再停止，停止阶段照常执行。单进程运行时第二个信号立即停止；由主进程管理的 worker 只按第一个信号停止一次，之后的信号不起作用：Ctrl+C 与 systemd 的停止会让每个 worker 收到两次信号（终端或 systemd 一次、主进程一次），强制结束由主进程负责，它再收到一个信号，或开始停止之后过了 2 × `GRACEFUL_SHUTDOWN_TIMEOUT` + 5 秒，就强制结束仍在运行的进程（如卡在启动中的 worker）。存放 worker 共享状态的进程不理会 SIGINT 与 SIGTERM，由主进程在服务结束时关闭（Linux 上也随主进程结束）；原版里 systemd 默认的停止会先结束它，主进程与 worker 之后每次访问共享状态都出错。原版用事件循环的 `add_signal_handler`，uvloop 会丢掉循环没在运行时（启动的各步骤之间）到达的信号，进程就停不下来；原生主机还会在注册之前就应答请求，这时的 SIGTERM 直接杀死进程、不执行停止监听器。`loop.add_signal_handler` 不再被调用。
- worker 进程在主进程退出（包括被 SIGKILL）时收到 SIGTERM 而停止，不再成为无人管理、仍占着端口的孤儿进程（Linux；oldman 的 `_process_group` 检查仍可保留给其他子进程）。
- worker manager 结束服务时（worker 启动失败、关机时再收到信号等）只杀 worker 进程：原版对 worker 所在的进程组 `killpg`，而 worker 与主进程同组，主进程也随之被 SIGKILL，`main_process_stop` 监听器、套接字与 unix socket 文件的清理都不执行。现在主进程照常收尾，以退出码 1 结束；自成进程组的 worker 仍连同其进程组一起杀掉。
- `Authorization` 的认证方案取头部第一个词、不分大小写比较，不再按子字符串匹配：`request.token` 只认 `Bearer` / `Token`，其他方案（如 `Basic`）得到 None，不再返回整个头部；`request.credentials` 的 Basic 密码可以含冒号，非法 base64 视为没有凭据（oldman 的 `bearer_credential`、`basic_credentials`；只接受 Bearer 时用 `parse_credentials(header, ("Bearer",))` 并检查返回的方案）。不带方案的单个令牌仍按令牌处理；只有方案名、没有凭据（`Authorization: Bearer`）时没有令牌（`token` 为 None，原版是空字符串）；方案与凭据之间的任意空白（如 Tab）都算分隔。
- 响应中间件抛出的异常与 handler 抛出的一样交给错误处理器，送出错误处理器的响应，这个响应不再经过响应中间件；原版只记一条日志、照样送出原响应（oldman session 的 `save_session` 为此自己把响应换成 500，切换后直接抛出即可）。流式响应在 `request.respond()` 处抛出，handler 不拦截时同样交给错误处理器；拦截之后可以改回别的响应（它不再经过响应中间件）。送错误响应时（包括错误处理器自己调用 `request.respond()`）响应中间件再出错，仍只记日志、照送错误响应（与原版相同）。
- 分块请求体按 RFC 9112 解析：块长度只能是十六进制数字（原版也接受 `0x5`、带空白的长度），块数据后必须是 CRLF（原版不检查这两个字节），块长度行（含扩展）最长 64 字节、扩展里不能有控制字符（原版只在这一行还没收完时限长，扩展内容不看），否则 400，与原生主机相同。HTTP/1.0 请求带 `Transfer-Encoding`（RFC 9112 6.1）或请求有多个 `Host`（RFC 9112 3.2）时 400；缺 `Host` 的 HTTP/1.1 请求仍照原版接受。
- `content_type=None` 的响应不发送 `Content-Type`，不再发送字面值 `None`（oldman 代理里的默认值）。
- `file_stream()` 带 Range 时按剩余量读取：范围大于 `chunk_size` 又不是它的整数倍时，原版最后一次多读多发，送出的字节超过 `Content-Range` 声明的范围。两种服务器相同。
- 蓝图注册之后再加入的路由、静态文件、中间件、异常处理器、监听器与信号，与注册之前定义的一样：按每一次注册的参数（直接注册时给的前缀与版本，所在蓝图组的前缀、版本与 `name_prefix`）加入应用，蓝图（及其所在组）的中间件与异常处理器同样作用于它们；同一个蓝图经几个组注册时，它的中间件作用于每一次注册的路由。原版在这时按蓝图自己的参数整个重新注册：经蓝图组注册的蓝图在启动时报路由重名，直接注册的则让后加的路由不经过蓝图的中间件（如认证）。应用开始服务之后再往已注册的蓝图里加入任何东西，抛 `OldmanServerException`。
- 错误处理函数不论是普通函数还是协程，规则相同：返回响应就送出它；返回 None 时退回默认错误页（它已用 `request.respond()` 自己回应时除外）；抛出框架异常时按那个异常生成默认错误页；抛出其他异常时记一条错误日志，回 500 “An error occurred while handling an error”。`ErrorHandler.default()` 可以是协程，总会被 await。原版只在普通函数返回 None 时退回默认错误页（协程得到 “Invalid response type None” 的错误页），只对协程抛出的框架异常生成相应的错误页（普通函数得到 500），处理函数抛出框架异常时也不 await 协程的 `default()`。
- `file_stream()` 的响应在响应中间件里读 `body` 得到 None（内容在送出时才读出），读 `processed_headers` 得到它将要送出的响应头；原版两者都抛 `AttributeError`。
- 响应辅助函数（`text()`、`html()`、`json()`、`raw()`、`redirect()`、`empty()`、`file()`、`file_stream()`）、`ResponseStream` 与 `request.respond()` 的 `headers` 接受任何 Mapping（包括只读的与 multidict，多值保留），并复制进响应自己的响应头，不改动调用方的对象。原版的 `redirect()` 把 `Location` 写进调用方传入的字典（传只读 Mapping 时抛 `TypeError`，重复使用同一个字典时 `Location` 留在里面），`file()`、`file_stream()` 同样往里写 `Last-Modified`、`Cache-Control` 等，`ResponseStream` 直接使用传入的 `Header` 对象。

## 经原生主机时

- `request.headers` 是 `NativeHeaders`：`[]`、`get`、`getone`、`getall`、`in` 与属性访问（`headers.content_type`）直接查原生请求；迭代、`items()`、`len()`、`==`、`copy()` 与修改在第一次用到时建一个 `Header`，之后都在它上面进行，结果与 `Header` 相同。它是 `multidict.MultiMapping`（也是 `Mapping`），但不是 `CIMultiDict`：`isinstance(request.headers, CIMultiDict)` 为 False。
- 请求行之前的空行按 RFC 9112 跳过，计入请求头上限（`REQUEST_MAX_HEADER_SIZE` 等），超出回 413；原版对任何前导空行都回 400。
- `request.head`、`raw_headers`、`request_line` 由解析出的请求行与字段重建（冒号后一个空格、值已去掉两端空白、名字保留请求方的大小写），不是请求方发来的原始字节。
- `file()`（`app.static` 也用它）回应 128 KiB 以上的文件时，文件内容不读进 Python：handler 里只打开文件（打不开时照原版在 handler 里抛出），送出响应时由原生主机从文件读出、直接写入连接。响应中间件看到的 `response.body` 是 None；中间件设了别的 body，就送出那个 body。`Content-Length` 是打开时的文件大小，之后文件变短的话，连接在已送出的数据之后关闭。这种响应不受 `NATIVE_MAX_RESPONSE_SIZE` 限制。更小的文件与 asyncio 服务器照旧把文件读进 `response.body`。
- 响应头最多 128 个字段、合计约 8 KiB，超出时交给错误处理器（500）；asyncio 服务器没有这个上限（放在 nginx 之后时，nginx 默认的 `proxy_buffer_size` 也只容纳 4 或 8 KiB 的上游响应头）。
- 不派发 `http.lifecycle.begin`、`http.lifecycle.read_head`、`http.lifecycle.read_body`、`http.lifecycle.send`、`http.lifecycle.complete` 这五个连接与收发层面的信号，只有 asyncio 服务器派发；其余 `http.*` 信号在相同的时点派发。
- `request.protocol` 是原生连接的替身：提供 `state`、`version`、`conn_info`，不是 `HttpProtocol` 实例。
- 原生服务的默认运行没有 worker manager，所以没有 `app.m`（multiplexer）。这与原版 `single_process=True` 相同。
- 原生 WebSocket 不构造 `WebSocketProtocol`，但同样由 `WebsocketImplProtocol` 处理，`WEBSOCKET_*` 设置照样生效。
- 设了 `NATIVE_SHARDS` 时，`fast=True` 启动 CPU 数 ÷ (1 + `NATIVE_SHARDS`) 个 worker，而不是每个 CPU 一个：每个这样的原生 worker 要占一组 CPU。不设时与原版一样每个 CPU 一个（同线程模式）。
- `app.create_server()` 创建后即开始服务：`asyncio_server_kwargs`（如 `start_serving=False`）只用于 asyncio 服务器。
- 原生主机开始服务时把垃圾回收的第一个阈值调到 10000（`NATIVE_GC_THRESHOLD`，只在进程仍是 Python 的默认值时，见[配置](configuration.md#原生主机的设置)），对整个进程生效；原版不动垃圾回收。
- 关机时被截断的请求不另记 `Request: ... stopped. Transport is closed.` 日志（asyncio 服务器会记）。
- 流式响应开始后不再受 `RESPONSE_TIMEOUT` 约束；原版在两次发送之间计时。
- 带 `Expect: 100-continue` 的请求：请求体还没到时，原生主机在请求交给 Python 之前就回 `100 Continue`（请求体已随请求头到齐时不回，RFC 9110 允许）。原版在第一次读请求体时回；非 stream 路由在 handler 之前就读请求体，所以同样是立即回，只有 `stream=True` 且 handler 不读请求体就回应时，原版把 100 与响应一起发出。
- 请求头的 `REQUEST_TIMEOUT` 从请求的第一个字节起计，原版从最后一次收到数据起计：一个字节一个字节慢慢发请求头的客户端不能无限拖住连接。
- 请求体超过 `REQUEST_MAX_SIZE` 时（`stream=True` 的 handler 照常读完），这个请求的响应之后连接关闭；原版在 stream handler 读完请求体时保持 keep-alive。
- 优雅停止与原版一样先停止接受新连接、关闭空闲连接，忙的连接照常服务（已经送达的 pipelined 请求也服务），在 `GRACEFUL_SHUTDOWN_TIMEOUT` 内等在途请求结束、响应写完；不同的是忙的连接在最后一个响应之后关闭（响应带 `Connection: close`），原版则让它保持 keep-alive，到宽限期结束才断开。宽限期过后取消仍在运行的 handler，最多再等 1 秒：不理会取消、继续运行的 handler 被留下（记一条警告），服务器照常停止（原版关闭事件循环时同样不等它）。asyncio 服务器在 Python 3.12 起 asyncio 自带的事件循环上（Windows、`USE_UVLOOP=False`）也按这个顺序停止；原版在那里先等所有连接自己关闭（包括空闲的 keep-alive 连接）才开始这些步骤，停止可能一直等下去。
