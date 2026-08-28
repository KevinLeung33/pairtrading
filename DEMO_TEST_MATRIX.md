# Demo Trading 测试矩阵

真实 Demo 测试必须逐项执行，每项使用独立 request id。不要并行启动多个 runner。

.env 只需要保存 API、交易对和默认安全参数。测试参数可以直接通过命令行覆盖，不需要反复编辑 .env：

命令示例：

    bash scripts/run_demo.sh --request-id DEMO-P01 --direction short_spot_long_swap --target-base-qty 0.10 --child-base-qty 0.02 --max-unhedged-base-qty 0.01 --maker-reprice-interval-ms 150

可覆盖的参数包括：

- --direction
- --target-base-qty
- --child-base-qty
- --max-unhedged-base-qty
- --max-hedge-retries
- --maker-reprice-interval-ms
- --state-path

每次测试只改变命令行参数和 request id，测试结束后检查仓位、挂单和日志，再执行下一组。


## 0. 前置检查

```bash
cd ~/code/pairtrading
source .venv/bin/activate
bash scripts/run_local_scenarios.sh
bash scripts/demo_readonly_check.sh
```

确认 Demo 账户没有遗留仓位和挂单：

```text
OKX Demo → Positions = 0
OKX Demo → Open Orders = 0
```

## 1. 单子单完整成交

配置：

```env
TARGET_BASE_QTY=0.01
CHILD_BASE_QTY=0.01
ARB_DIRECTION=short_spot_long_swap
```

运行：

```bash
bash scripts/run_demo.sh --request-id DEMO-FULL-001 2>&1 | tee runtime/demo-full-001.log
```

检查：

- 合约 Maker 成交；
- 现货 IOC 成交；
- 合约和现货基础币数量一致；
- 敞口为 0；
- 收到 `CHILD_TERMINAL` 和 `PARENT_COMPLETED`；
- Demo 账户没有遗留仓位。

## 2. 多子单顺序执行

配置：

```env
TARGET_BASE_QTY=0.05
CHILD_BASE_QTY=0.01
```

运行：

```bash
bash scripts/run_demo.sh --request-id DEMO-MULTI-001 2>&1 | tee runtime/demo-multi-001.log
```

检查：

- 子单是否按 C0001、C0002、C0003 顺序推进；
- 没有一次性创建全部 Maker；
- 每个子单都各自完成对冲；
- 最终累计敞口为 0。

## 3. 反向开仓方向

先确认 Demo 账户和现货杠杆条件允许，再改：

```env
ARB_DIRECTION=long_spot_short_swap
TARGET_BASE_QTY=0.01
CHILD_BASE_QTY=0.01
```

运行：

```bash
bash scripts/run_demo.sh --request-id DEMO-REVERSE-001 2>&1 | tee runtime/demo-reverse-001.log
```

检查：

- 合约 Maker 为卖出；
- 现货 IOC 为买入；
- 最终两腿数量一致；
- 没有错误地增加相反方向仓位。

## 4. BBO 跟价改单

配置：MAKER_REPRICE_INTERVAL_MS=150，TARGET_BASE_QTY=0.01，CHILD_BASE_QTY=0.01。

观察 Maker 等待期间盘口变化：

- 盘口移动后约 150ms 内最多触发一次改单；
- 不应每个 BBO 跳动都立即撤单重挂；
- 买入方向跟随 best_bid，卖出方向跟随 best_ask；
- 不应出现两个同时工作的 Maker；
- 日志中的订单数量应符合盘口变化次数，而不是 WebSocket 消息总数。

## 4. Maker 长时间不成交

将目标数量保持最小，启动后观察 2～5 分钟，不要手动追价：

```bash
bash scripts/run_demo.sh --request-id DEMO-NOFILL-001 2>&1 | tee runtime/demo-nofill-001.log
```

检查：

- 没有现货 IOC；
- 没有虚假成交报告；
- 盘口订单状态正常；
- 停止程序后确认 Maker 挂单已撤销。

## 5. REST 校准和程序重启

启动一个小数量任务后，在 Maker 等待期间停止程序：

```bash
bash scripts/run_demo.sh --request-id DEMO-RESTART-001 2>&1 | tee runtime/demo-restart-001.log
```

另一个终端停止后，检查 OKX 活动订单和持仓，再重新启动。重点观察：

- 是否重复创建相同 Maker；
- 是否重复发送现货 IOC；
- REST 校准后订单状态是否一致；
- 是否出现未记录敞口。

## 6. WebSocket 断线观察

运行一个小任务期间临时阻断或恢复服务器网络，只做一次短暂测试。检查：

- 断线期间是否停止新动作；
- 重连后是否恢复订单监听；
- 是否出现重复对冲；
- 是否通过 REST 找回断线期间成交。

## 7. Lark 报告检查

每个成功父订单应至少出现：

- 绿色 `CHILD_TERMINAL` 卡片；
- 绿色 `PARENT_COMPLETED` 卡片。

风险任务应出现：

- 红色风险卡片；
- 当前敞口；
- 对冲次数；
- OKX 具体错误原因。

## 8. 每次测试后的日志浓缩

```bash
python scripts/summarize_demo_log.py runtime/demo-full-001.log
```

如果有问题，脚本返回非零状态，并生成：

```text
runtime/reports/demo-log-summary-*.md
runtime/reports/demo-log-summary-*.json
```

只需要把 Markdown 报告发回即可，不要发送 `.env`、API key 或 secret。

## 9. 不建议在真实 Demo 中强行制造的场景

以下场景应主要使用本地 Fake Exchange 测试：

- 强制 IOC 只成交一半；
- 强制 API 超时；
- 强制重复 WebSocket 事件；
- 强制成交事件乱序；
- 强制借币失败；
- 强制 REST 与 WebSocket 状态不一致。

真实 Demo 只能自然观察这些情况，不能为了制造故障而故意扩大仓位或留下未对冲敞口。