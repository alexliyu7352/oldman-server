English | [简体中文](../zh/deployment.md)

# Deployment

This package serves plain HTTP/1.1 only, and a front proxy terminates TLS; processes are managed by systemd or by the service commands of a higher-level framework (such as oldman).

## Under systemd

For a service with several workers, set `KillMode=mixed` in the unit: a stop then sends SIGTERM to the main process only, which stops the workers in order, waits for them to finish and exits; once it has exited (or when `TimeoutStopSec` expires), systemd ends the processes left with SIGKILL. The default `control-group` sends SIGTERM to every process at once: the workers still stop once and finish as usual, but processes started with `app.manager.manage()` and the like also stop on their own at the same time, outside the main process's order. `TimeoutStopSec` (90 s by default) must be longer than the time after which the main process kills workers, 2 × `GRACEFUL_SHUTDOWN_TIMEOUT` + 5 s.

To have a worker whose event loop is blocked and no longer answering restarted automatically, set `WORKER_HEALTH_TIMEOUT` (see [Configuration](configuration.md#worker-health)).

## Behind nginx

With multiple workers behind nginx (or OpenResty), have each worker listen on an extra address of its own and let nginx balance the requests:

```python
app.config.NATIVE_SERVER = True
app.config.NATIVE_WORKER_LISTEN = "/run/app/{n}.sock"   # worker 0 is /run/app/0.sock, and so on
app.config.TRUSTED_PROXIES = "unix"                     # trust forwarding headers only from unix socket connections
app.config.UNIX_SOCKET_MODE = 0o660                     # only the service's user and its group (nginx) can connect
app.config.PROXIES_COUNT = 1
app.run(unix="/run/app/app.sock", workers=2)            # the run's own address keeps serving (health checks, direct access)
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

Why (measured with OpenResty 1.31): recent nginx uses HTTP/1.1 to upstreams and reuses connections even with no settings, and it prefers the most recently released idle connection. Under moderate load a few "hot" connections carry most of the requests, so when nginx is given a single address, the requests concentrate on the workers those connections belong to: at about 40% load (20k requests/s, with two workers able to do about 50k/s together), the ratio of requests between the less busy and the busier worker was as low as 0.10 for a TCP address and 0.72 for a unix socket, with the busier worker's Python thread at 0.9 cores; with one address per worker and `least_conn` it was 0.97–1.0, with each side at about 0.65 cores. The server can only decide which process a connection lands in, not how nginx spreads requests over its existing connections, so nginx has to see each worker separately.

- `TRUSTED_PROXIES = "unix"` trusts every process that can connect to these sockets. The sockets' permissions are 0o666 by default, as in Sanic, so any local account could connect and forge `X-Forwarded-For`; therefore also set `UNIX_SOCKET_MODE = 0o660` and put nginx's user in the service process's group (or put the sockets in a directory that only the two can enter).
- For a same-host deployment, use unix sockets to save the loopback TCP overhead; `NATIVE_WORKER_LISTEN` can also be a TCP address such as `"127.0.0.1:81{n:02d}"` (8100, 8101, …).
- A worker removes its socket file on exit; a restarted worker binds the same address. While it restarts, nginx temporarily marks the address it cannot connect to as failed (`max_fails`, `fail_timeout`, by default 1 attempt and 10 seconds) and sends the requests to the other workers.
- The configuration above sets `Connection` to empty so that upstream connections are reused, which keeps WebSocket upgrade requests from reaching the application. To proxy WebSocket through nginx, add `map $http_upgrade $connection_upgrade { default upgrade; '' ''; }` to the `http` block and set `proxy_set_header Upgrade $http_upgrade;` and `proxy_set_header Connection $connection_upgrade;` in the `location`: for ordinary requests `Upgrade` is empty, `Connection` stays empty, and upstream connections are still reused.
- When the number of workers changes, nginx's `upstream` has to change with it.
- The listening socket's backlog is 2048 by default: when nginx opens hundreds of upstream connections at once, a unix socket does not make nginx answer 502 because of a full queue (Sanic's 100 does).
- Under the native host (`NATIVE_SERVER`), the TCP listening socket that Sanic binds is not inheritable (stock Sanic makes it inheritable): programs the application starts through exec (`os.system` and the like) do not keep listening on the port after the server exits, which would leave clients connecting with nobody answering. Worker processes receive the socket through multiprocessing and are not affected; unix sockets are not inheritable in either.
