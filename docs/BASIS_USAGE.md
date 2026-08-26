# Basis 策略使用说明

本文说明当前快速落地版本的 Basis 策略如何在 WSL + OKX Demo Trading 中运行，以及如何在进程运行后动态修改阈值。

## 1. 工作方式

Basis 模式不是收到命令就立即下单，而是：

```text
订阅现货/合约 BBO
    ↓
计算当前可执行 Basis
    ↓
达到入场阈值？
    ├─ 否：继续等待
    └─ 是：启动配对执行
             ↓
       合约 Maker 成交
             ↓
       现货 IOC 对冲
             ↓
       价差消失或敞口超限：暂停
             ↓
       价差恢复：继续执行
```

触发后仍然使用原有执行器：合约腿 `post_only Maker`，现货腿 IOC；订单回报由私有 WebSocket 驱动，REST 只做已知活动订单的校准和最终成交/资产核对。

## 2. 启动前准备

在 WSL 项目目录执行一次安装：

```bash
cd ~/code/pairtrading
bash scripts/setup_wsl.sh
source .venv/bin/activate
```

`.env` 至少需要配置：

```dotenv
OKX_API_KEY=你的Demo API Key
OKX_SECRET_KEY=你的Demo Secret Key
OKX_PASSPHRASE=你的Demo Passphrase
LARK_WEBHOOK_URL=你的Lark Webhook
```

确认 API Key 只开启交易权限，不开启提现权限；先运行只读检查：

```bash
bash scripts/demo_readonly_check.sh
```

只读检查应确认：

- 现货和永续合约交易规则可读取；
- 账户有足够的现货保证金/借贷额度；
- 永续账户模式、持仓方向和可用保证金正确；
- Demo 环境认证成功。

## 3. 开仓命令

例如：做空现货、做多永续，目标 1 BTC，每次 0.1 BTC：

```bash
OKX_DEMO=1 STRATEGY_MODE=basis bash scripts/run_demo.sh \
  --request-id BASIS-1BTC-OPEN-001 \
  --strategy-mode basis \
  --direction short_spot_long_swap \
  --action open \
  --spot-td-mode cross \
  --target-base-qty 1 \
  --child-base-qty 0.1 \
  --max-unhedged-base-qty 0.01 \
  --max-maker-attempts 50 \
  --status-report-interval-seconds 30 \
  --basis-entry-threshold-bp 10 \
  --basis-pause-threshold-bp 5 \
  --basis-resume-threshold-bp 8 \
  --state-path runtime/basis-1btc-open-001.json
```

另一方向使用：

```text
--direction long_spot_short_swap
```

开仓方向含义：

| direction | 现货腿 | 永续腿 |
|---|---|---|
| `short_spot_long_swap` | 卖出 | 买入 |
| `long_spot_short_swap` | 买入 | 卖出 |

程序启动后如果当前 Basis 没有达到阈值，会保持等待，不会下单；这不是卡死。终端日志会保持进程运行，直到信号触发、任务完成、进入恢复状态或收到停止信号。

## 4. Basis 阈值

所有 Basis 阈值单位都是 bp：

```text
1 bp = 0.01%
10 bp = 0.10%
```

开仓时：

| 参数 | 作用 |
|---|---|
| `basis-entry-threshold-bp` | 尚未开始时，Basis 达到该值才启动开仓 |
| `basis-pause-threshold-bp` | 执行中 Basis 低于该值时暂停 |
| `basis-resume-threshold-bp` | 暂停后 Basis 达到该值且敞口足够小时恢复 |
| `max-unhedged-base-qty` | 敞口超过该值时进入暂停/恢复流程 |

推荐保持入场阈值高于暂停阈值，恢复阈值高于暂停阈值，形成滞回，避免行情在阈值附近反复启停。

当前可执行 Basis 使用保守盘口：

- 买永续、卖现货：`(现货 Bid1 - 永续 Ask1) / 永续 Ask1 × 10000`；
- 卖永续、买现货：`(永续 Bid1 - 现货 Ask1) / 现货 Ask1 × 10000`。

报告里的“市场可执行 TWAP”是执行期间的审计均值，不是启动瞬间的触发值。

## 5. 运行中动态修改阈值

启动时使用的 `--state-path` 决定控制文件路径。例如：

```text
状态文件：runtime/basis-1btc-open-001.json
控制文件：runtime/basis-1btc-open-001.basis-control.json
```

策略运行后，在另一个 WSL 终端执行：

```bash
python3 scripts/update_basis_threshold.py \
  --state-path runtime/basis-1btc-open-001.json \
  --entry-threshold-bp 15
```

策略通常在约 1 秒内读取新值，不需要重启。也可以一次更新多个值：

```bash
python3 scripts/update_basis_threshold.py \
  --state-path runtime/basis-1btc-open-001.json \
  --entry-threshold-bp 15 \
  --pause-threshold-bp 8 \
  --resume-threshold-bp 12
```

可动态更新的参数：

```text
--entry-threshold-bp
--pause-threshold-bp
--resume-threshold-bp
--exit-threshold-bp
--resume-exposure-base-qty
```

注意：

- `entry-threshold` 只影响尚未触发的入场；
- 已经开始执行的任务主要由 `pause-threshold`、`resume-threshold` 和敞口限制管理；
- 任务完成、失败或进入 recovery 后不会因为修改阈值自动重新启动；
- 修改的必须是正在运行任务对应的 `state-path`；
- 运行日志中出现 `basis runtime controls updated`，表示策略已经加载新值。

## 6. 平仓命令

平仓必须新建 `request-id` 和状态文件，不能复用已完成开仓任务的状态文件。

平掉“做空现货、做多永续”的仓位：

```bash
OKX_DEMO=1 STRATEGY_MODE=basis bash scripts/run_demo.sh \
  --request-id BASIS-1BTC-CLOSE-001 \
  --strategy-mode basis \
  --direction short_spot_long_swap \
  --action close \
  --spot-td-mode cross \
  --target-base-qty 1 \
  --child-base-qty 0.1 \
  --max-unhedged-base-qty 0.01 \
  --basis-exit-threshold-bp 0 \
  --state-path runtime/basis-1btc-close-001.json
```

平掉“做多现货、做空永续”的仓位时使用：

```text
--direction long_spot_short_swap --action close
```

`close` 会自动反转两条腿的买卖方向，并使用永续合约 reduce-only 逻辑。执行前必须确认当前持仓方向与平仓方向一致。

当前版本不会在开仓完成后自动提交平仓任务；开仓和平仓是两个独立请求。

## 7. 通知、日志和报告

Lark 通知包含：

- 任务开始参数；
- 运行中的任务状态；
- 当前成交数量和敞口；
- Basis 触发、暂停、恢复或风险事件；
- 最终成交均价、手续费、实际价差；
- 资产余额和永续仓位对账结果。

本地文件：

```text
runtime/*.json                         任务状态和恢复信息
runtime/reports/*.json                 结构化报告
runtime/reports/*.md                   可读报告
runtime/reports/execution-efficiency-* 执行效率报告
```

正常最终结果应重点检查：

```text
合约成交数量 = 现货成交数量
未对冲敞口 = 0 或在允许误差内
资产对账 = MATCHED
IOC 成交率正常
没有 EXECUTION_RISK 或 recovery
```

`CHECK_REQUIRED` 表示成交回报、余额或仓位与预期不一致，需要先核对，不应直接继续下一笔。

## 8. 费用和阈值注意事项

入场阈值目前比较的是毛 Basis，不会自动替你扣除手续费、资金费和执行滑点。实际使用时至少要考虑：

```text
可交易毛 Basis
> 现货手续费 + 永续手续费 + 预估滑点 + 资金费缓冲 + 安全边际
```

例如此前报告手续费约 9.6 bp，而市场可执行 Basis 只有约 3.6 bp，这种行情即使执行链路正常，也不适合开仓。不要为了让程序“跑起来”把入场阈值降到明显低于综合交易成本。

`SPOT_TD_MODE=cross` 只表示使用 OKX Cross Margin 交易模式，不会替你提高杠杆或自动增加借贷额度；保证金、借贷权限和最大杠杆需要在 OKX Demo 账户侧预先配置。

## 9. 安全停止和异常处理

按 `Ctrl+C` 停止进程时，程序会尝试停止策略、撤掉活动 Maker，并发送最终状态。遇到以下情况不要直接复用原任务继续下单：

- Lark 收到 `EXECUTION_RISK`；
- 最终状态是 `recovery`；
- 未对冲敞口不为 0；
- 资产对账为 `CHECK_REQUIRED`；
- 进程被杀掉但交易所仍有活动订单。

应先运行只读检查、检查 OKX 活动订单和仓位，再决定是否人工处理或新建恢复任务。

## 10. 实盘前检查

当前命令默认 Demo Trading。切换实盘前必须单独确认：

- API Key、账户、交易模式和持仓方向；
- 手续费等级和资金费假设；
- 现货借贷额度和永续保证金；
- 入场阈值覆盖全部交易成本；
- 至少完成多方向、部分成交、改单、IOC 部分成交和异常恢复测试；
- Lark 能收到开始、过程、风险和最终报告。

实盘权限必须显式使用 `OKX_DEMO=0` 和 `--allow-live`，不要在 Demo 脚本上直接改默认值。