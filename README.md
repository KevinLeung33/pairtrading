# OKX Pair Executor

这是一个以“合约 Maker 成交驱动现货 IOC 对冲”为核心的执行器 MVP。

当前版本包含：

- 按基础币数量拆分父订单；
- 合约 `post_only` Maker 订单；
- 合约部分成交后，按成交增量发送现货保护性 IOC；
- 部分 IOC 成交、敞口阈值、重试和恢复状态；
- WebSocket 事件作为主驱动，REST 作为校准接口；
- Lark Webhook 报告；
- 模拟交易所和自动化测试。
- 公共 `books5` 订单簿 WebSocket 自动重连；
- Maker 子订单撤单重挂；
- JSON 状态持久化和恢复。

真实 OKX 下单适配器还没有默认启用。先运行测试和模拟场景，再配置 API key，并显式接入 `OkxV5Client`。

## WSL 本地运行

推荐使用 Python 3.10+。把项目放在 WSL 的 Linux 文件系统中，例如 `~/projects/okx-pair-executor`，不要长期直接从 `/mnt/e` 运行，Python 虚拟环境和大量小文件会更慢。

首次安装：

```bash
cd ~/projects/okx-pair-executor
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
bash scripts/setup_wsl.sh
nano .env
```

`.env` 只保存 WSL 本机的 Demo 密钥，并已被 `.gitignore` 忽略。

运行本地场景测试：

```bash
bash scripts/run_local_scenarios.sh
```

运行 Demo 只读检查：

```bash
bash scripts/demo_readonly_check.sh
```

运行 Demo Trading：

```bash
bash scripts/run_demo.sh --request-id DEMO-001
```

`run_demo.sh` 强制要求 `OKX_DEMO=1`，不会启动实盘。

## Smoke test

```bash
source .venv/bin/activate
PYTHONPATH=src python scripts/smoke.py
```

`OkxBookStream` 收到盘口后，应调用 `OkxV5Client.update_orderbook()`，执行器再从适配器读取 Maker 价格。实盘/模拟盘都必须先使用 Demo Trading 验证。

## Demo Trading runner

```bash
bash scripts/setup_wsl.sh
bash scripts/run_demo.sh --request-id DEMO-001
```

当前 runner 默认只允许 Demo Trading。实盘必须同时设置 `OKX_DEMO=0` 和传入 `--allow-live`。

交易执行默认使用 `OKX_TRADE_WS=1`：下单、改单、撤单优先走 OKX 私有 WebSocket，失败时才降级到 REST；任务执行期间不推送 child 卡片，只发送一张开始卡片，并按 `STATUS_REPORT_INTERVAL_SECONDS`（默认 30 秒）发送任务级状态；进入终态后发送一次最终结果卡片。现货腿的 `SPOT_TD_MODE=cross` 表示使用 Cross Margin，设为 `cash` 才是普通现货。Maker 改单使用 `amend-order`，不会默认执行 cancel-and-replace。私有 `orders` 回报是主状态来源，程序每 5 秒只对仍在工作的已知 Maker 订单做一次单订单 REST 核对，不轮询 `orders-history`。

执行效率报告会额外记录 `Amend ACK P95 ms`、Maker 报价年龄、改单次数和 IOC 成交率。首次切换到 WS 后建议先运行 Demo Trading，并确认最终效率报告中的改单延迟和无异常敞口。

## 系统化建设方案

从当前单进程执行器演进到多账户、多策略、Account OMS 的详细方案见 [SYSTEM_BUILD_PLAN.md](docs/SYSTEM_BUILD_PLAN.md)。

## Basis strategy mode

后续分层架构、Basis 状态机、OMS 事件契约和迁移计划见 [BASIS_ARB_FRAMEWORK.md](docs/BASIS_ARB_FRAMEWORK.md)。

完整操作说明见 [BASIS_USAGE.md](docs/BASIS_USAGE.md)。

快速启用 Basis Demo：

```bash
STRATEGY_MODE=basis bash scripts/run_demo.sh --request-id BASIS-DEMO-001 \
--target-base-qty 0.01 --child-base-qty 0.01
```

## Local scenario tests

本地场景测试不访问网络，也不会下单：

```powershell
python scripts/run_local_scenarios.py
```

测试报告会写入 `runtime/reports/`。Demo API 只读联通测试：

```powershell
python scripts/demo_readonly_check.py
```

Basis 运行中动态调整阈值：策略启动后，使用与 `--state-path` 相同的状态文件执行下面的命令；策略会在约 1 秒内读取更新，文件通过原子替换写入，不需要重启进程：

```bash
python3 scripts/update_basis_threshold.py \
  --state-path runtime/basis-1btc-001.json \
  --entry-threshold-bp 15
```

可更新 `--pause-threshold-bp`、`--resume-threshold-bp`、`--exit-threshold-bp` 和 `--resume-exposure-base-qty`。入场阈值只影响尚未触发的入场；已开始的任务使用暂停/恢复阈值继续管理。