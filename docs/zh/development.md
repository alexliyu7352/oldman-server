[English](../en/development.md) | 简体中文

# 开发

## 开发环境

- 宿主：Ubuntu 20.04。
- Python：CPython 3.14.7（普通 GIL 构建；支持 3.12–3.14），由 uv 管理，装在 uv 共用的 Python 目录，需要认得这个版本的 uv。
- 编译器：锁定的 GCC 14，放在 `/opt/tools/gcc-14`（`PROBE_TOOLCHAIN`）。
- 原生依赖：Asio、liburing，在 `build/deps`（`PROBE_DEPS`）。

第一次先运行 `bash scripts/bootstrap.sh`：它按 `dependency-lock.json` 下载编译器（已有则跳过）与依赖，构建 liburing、扩展与性能对照的压测客户端（`build/http_load`，也基于 Asio），要求宿主的系统包与锁文件 `host_requirements` 的版本逐个相同（不同即拒绝，不会换用别的版本）。其他环境请用发布构建的容器流程（见[发布](#发布)，`scripts/build_wheels.sh` 在 manylinux_2_28 容器里从源码构建全部依赖）。

```sh
bash scripts/bootstrap.sh               # 第一次：锁定的工具链与原生依赖、扩展、压测客户端
uv sync --locked                        # Python 依赖（官方 PyPI），本包以可编辑方式安装
source scripts/env.sh                   # 原生库路径
bash scripts/build.sh                   # 构建原生扩展 oldman_server.native._native（增量约 10 秒）
# 其他 Python 版本：各自的环境与构建目录，只构建扩展
UV_PROJECT_ENVIRONMENT=build/venv312 uv sync --locked --python 3.12
PROBE_PYTHON=$PWD/build/venv312/bin/python PROBE_PROJECT_BUILD=$PWD/build/cmake312 bash scripts/build.sh _native
```

## 测试与测量

`tests/framework` 源自 Sanic 25.12.1 的测试集（MIT），已改为本包的 import 名和行为，经 asyncio 服务器和经原生主机两种方式都要全部通过：

```sh
python scripts/run_framework_correctness.py --sanic --out build/correct-<目录>    # 本项目原生主机测试，每个用例独立进程
OLDMAN_SERVER_NATIVE_SERVER=false .venv/bin/python -m pytest tests/framework   # 框架测试：asyncio 服务器
OLDMAN_SERVER_NATIVE_SERVER=true OLDMAN_SERVER_NATIVE_SHARDS=1 .venv/bin/python -m pytest tests/framework   # 框架测试：经原生主机
OLDMAN_SERVER_NATIVE_SERVER=true OLDMAN_SERVER_NATIVE_SAME_THREAD=true .venv/bin/python -m pytest tests/framework   # 同上，同线程模式
OLDMAN_SERVER_NATIVE_SAME_THREAD=true python scripts/run_framework_correctness.py --sanic --out build/correct-<目录>   # 原生主机测试，同线程模式
python scripts/calibrate.py build/calib-<目录> candidate:small candidate:body      # 服务端吞吐与主线程 µs
python probes/sanic_comparison/run.py --python-only --out build/compare-<新目录>     # 阶段验收：与原版 Sanic 配对对照
python probes/wrk_comparison/run.py --out build/wrk-<新目录> --quiet                # 4 核同机：各布局、裸函数（原生主机自己的上限）与原版，wrk 压测
```

完整的性能对照及复现命令见[性能对照](benchmarks.md#复现)。

## 发布

推送 tag `v<版本>`（与 `src/oldman_server/__version__.py` 一致，如 `v0.1.0`）后，`.github/workflows/release.yml`：

1. 在 manylinux_2_28 容器里分别为 x86_64 与 aarch64 构建 wheel（`scripts/build_wheels.sh`）：按 `dependency-lock.json` 下载 liburing 与 Asio 的源码并校验 SHA-256（`scripts/fetch_sources.py`），构建 liburing，再为 CPython 3.12–3.14 各构建扩展与运行时的 io_uring 构建（liburing 静态链入）、打包（`hatch_build.py` 把两者放进 wheel 并标上解释器与平台；树里没有扩展时拒绝构建 wheel，`OLDMAN_SERVER_PURE_WHEEL=1` 才构建只能用 asyncio 服务器的纯 Python wheel），由 auditwheel 核对所链接的库并标为 manylinux_2_28。容器镜像（连同编译器）按标签与摘要固定（`wheels.yml`）。
2. 核对每个 wheel 所带的库都附有许可证（`scripts/check_wheel_licenses.py`），再把每个 wheel 装进全新环境：只装声明的依赖时能导入，再装上测试依赖，跑经 asyncio 服务器与经原生主机的框架测试、原生主机的运行测试（`scripts/test_wheels.sh`）。auditwheel 只标 manylinux_2_28（`--only-plat`：测试只在 glibc 2.28 的镜像里做，不标更早的 glibc）。另在 `python:3.12/3.13/3.14-slim` 镜像里安装每个 wheel 并加载原生扩展：manylinux 允许 wheel 依赖少数系统库（libstdc++、libgcc_s、libz、libatomic）而不自带，精简镜像未必都有。
3. 经 PyPI trusted publishing 发布（环境 `pypi`，只接受 `v*` tag 的部署），再用 runner 自带的 `gh` 建 GitHub release，附上 wheel 与 SHA256SUMS。工作流里的 action 都固定到提交。

每次推送与 PR 由 `.github/workflows/ci.yml` 构建并测试同样的 wheel，不发布。wheel 带上所含第三方库的许可证（`LICENSES/`，作为包的 license files）：编进扩展的 pybind11 与 Asio、编进 io_uring 构建的 liburing 都要有许可证文件；auditwheel 若复制了库进 wheel，它也要在上述脚本的清单里并有许可证文件，否则测试失败。

## 目录

- `docs/`：文档，英文在 `docs/en`，简体中文在 `docs/zh`；与原版 Sanic 的完整性能对照及复现命令在其中的 `benchmarks.md`。
- `src/oldman_server/`：框架（源自 Sanic 25.12.1 的 fork）。
- `src/oldman_server/native/host.py`：应用与原生主机之间的适配层，包括请求、响应、流、文件、协议错误和 WebSocket。
- `src/oldman_server/native/engine.py`：原生运行时的 Python 端（事件取出、每请求 Task、回传批次、背压、文件操作）。
- `native/`：原生主机（`server.cc`）与运行时（`runtime.cc`，另有 io_uring 构建 `liboldman_native_uring.so`），基于 Asio；HTTP/1.1 解析与写出（`http1.cc`）、multipart 解析（`multipart.hh`）、Python 绑定（`bridge.cc`，构建为 `oldman_server.native._native`）。
- `src/oldman_server/routing/`：路由器（源自 sanic-routing 23.12.0，许可见 `LICENSE.sanic-routing`），它的测试在 `tests/routing`。
- `src/oldman_server/testing/`：测试客户端（源自 sanic-testing 24.6.0，许可见 `LICENSE.sanic-testing`）。
- `upstream/`：所依据的上游版本（归档地址、提交）与各文件的 SHA-256。与原版 Sanic 的性能对照要先把 `upstream/sanic.json` 的归档解压到 `build/sanic-upstream/`（得到 `sanic-<提交>/`），对照前逐个文件核对。
- `tests/`：本项目测试，其中 `tests/framework` 源自 Sanic 的测试集；`scripts/`：构建、测试与测量工具；`probes/`：对照与诊断用的服务端和驱动。
