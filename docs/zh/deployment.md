[English](../en/deployment.md) | 简体中文

# 部署

本包只服务明文 HTTP/1.1，TLS 由前端代理终止；进程由 systemd 或上层框架（如 oldman）的服务命令管理。

## 放在 nginx 之后

多 worker 放在 nginx（或 OpenResty）之后时，推荐让每个 worker 另外监听自己的地址，由 nginx 按请求均衡：

```python
app.config.NATIVE_SERVER = True
app.config.NATIVE_WORKER_LISTEN = "/run/app/{n}.sock"   # worker 0 是 /run/app/0.sock，依次类推
app.config.TRUSTED_PROXIES = "unix"                     # 只信任经 unix socket 来的转发头
app.config.UNIX_SOCKET_MODE = 0o660                     # 只有服务的用户与同组（nginx）能连上
app.config.PROXIES_COUNT = 1
app.run(unix="/run/app/app.sock", workers=2)            # 运行本身的地址照常服务（健康检查、直接访问）
```

```nginx
upstream app {
    least_conn;
    server unix:/run/app/0.sock;
    server unix:/run/app/1.sock;
    keepalive 32;
}
server {
    location / {
        proxy_pass http://app;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

原因（OpenResty 1.31 实测）：新版 nginx 不写任何设置时也对上游用 HTTP/1.1 并复用连接，且优先取最近释放的空闲连接。负载不高时少数几条“热”连接承担大部分请求，所以只给 nginx 一个地址时，请求集中在这几条连接所在的 worker：约四成负载（2 万请求/秒，两个 worker 共约 5 万/秒的能力）时，较闲 worker 与较忙 worker 的请求比，TCP 地址最低 0.10，unix socket 最低 0.72，较忙 worker 的 Python 线程到 0.9 核；每个 worker 一个地址并用 `least_conn` 时为 0.97–1.0，两边各约 0.65 核。服务器只能决定连接落在哪个进程，决定不了 nginx 在已有连接上怎么分请求，所以要让 nginx 分别看待各 worker。

- `TRUSTED_PROXIES = "unix"` 信任所有能连上这些 socket 的进程。socket 的权限默认与 Sanic 一样是 0o666，本机任何账号都能连上并伪造 `X-Forwarded-For`；所以同时设 `UNIX_SOCKET_MODE = 0o660`，并让 nginx 的用户属于服务进程的组（或把 socket 放在只有两者能进入的目录里）。
- 同机部署用 unix socket，省掉 loopback TCP 的开销；`NATIVE_WORKER_LISTEN` 也可以是 `"127.0.0.1:81{n:02d}"` 这样的 TCP 地址（8100、8101……）。
- worker 退出时删除自己的 socket 文件；重启的 worker 绑定同一地址。重启期间连接不上的那个地址被 nginx 暂时标记为失败（`max_fails`、`fail_timeout`，默认 1 次、10 秒），请求改走其他 worker。
- worker 数改变时，nginx 的 `upstream` 要同时增减。
- 监听 socket 的 backlog 默认 2048：nginx 同时打开上百条上游连接时，unix socket 不会因队列满而让 nginx 回 502（Sanic 的 100 会）。
- 原生主机（`NATIVE_SERVER`）时，Sanic 绑定的 TCP 监听 socket 不可继承（原版设为可继承）：应用经 exec 启动的程序（`os.system` 等）不会在服务器退出后还占着这个端口的监听，让客户端连上却没人应答。worker 进程经 multiprocessing 收到这个 socket，不受影响；unix socket 在两边都不可继承。
