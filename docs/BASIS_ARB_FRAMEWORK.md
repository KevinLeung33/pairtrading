# Basis Arb 与 OMS 分层建设方案

## 1. 目标与范围

本项目分两步建设：

1. **快速落地**：在当前单进程执行器中加入 Basis 策略协调层，复用现有的拆单、永续 Maker、现货 IOC、部分成交、原子改单、敞口恢复和 Lark 报告能力。
2. **生产化演进**：将交易所私有 WebSocket、写入命令、账户级限频和订单回报收敛到独立 OMS；策略通过 OMSConsumer 和标准化事件工作。

快速落地版本默认不改变现有 `STRATEGY_MODE=pair` 行为。设置 `STRATEGY_MODE=basis` 后，策略才按 Basis 信号连续运行。

## 2. 三层目标架构

```text
┌─────────────────────────────────────────────────────────────┐
│ Layer 3: Strategy                                           │
│  BasisArb / TWAP / future strategies                        │
│  signal + lifecycle + risk + order intent                   │
└──────────────────────┬──────────────────────────────────────┘
                       │ normalized events / commands
┌──────────────────────▼──────────────────────────────────────┐
│ Layer 2: OMSConsumer / OMS Gateway                          │
│  routing by account/tag/channel/symbol                      │
│  idempotency, correlation, command ownership                │
└──────────────────────┬──────────────────────────────────────┘
                       │ ZMQ IPC
┌──────────────────────▼──────────────────────────────────────┐
│ Layer 1: OMS process per account                            │
│  private WS, REST fallback, rate limits, reconnect, cache   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                 Exchange private WS / REST

Public WS ───────────────────────────────► market-data callbacks
```

### 2.1 当前快速版本的映射

| 目标组件 | 当前快速版本 |
|---|---|
| Public WS | `OkxBookStream` |
| OMS 私有 WS | `OkxV5Client` |
| OMSConsumer | 暂未拆进程，由策略回调直接接收事件 |
| Strategy | `BasisArbStrategy` + `PairExecutor` |
| OMS Gateway | `ExchangeAdapter` 协议 |
| 状态持久化 | `JsonStateStore` |
| 通知 | `LarkNotifier` |

## 3. Basis 策略定义

### 3.1 交易方向

旧算法的“做空永续 + 做多现货”对应：

```text
ARB_DIRECTION=long_spot_short_swap
ARB_ACTION=open
```

反向方向对应：

```text
ARB_DIRECTION=short_spot_long_swap
ARB_ACTION=open
```

### 3.2 信号 Basis

策略使用实际两条腿的保守盘口价格计算信号：

- 卖永续、买现货：

```text
(perp_bid - spot_ask) / spot_ask × 10000
```

- 买永续、卖现货：

```text
(spot_bid - perp_ask) / perp_ask × 10000
```

该值是入场资格信号，不等于最终成交 Basis。最终报告必须使用实际成交均价重新计算，并同时报告手续费、市场 TWAP 和相对市场 TWAP 的偏差。

### 3.3 Maker 价格规则

卖出永续的 Maker 价格使用 best ask，买入永续的 Maker 价格使用 best bid。不能把卖单挂在 best bid 后仍然认为是 Maker；卖单挂在买一会立即具有成交性，`post_only` 可能被交易所拒绝或取消。

## 4. 策略状态机

```text
WAITING_BASIS
      │ basis >= entry/exit threshold
      ▼
RUNNING ───────────── basis < pause threshold / exposure limit
   ▲                         │
   │ basis >= resume         ▼
   └──────────────────── PAUSED

RUNNING + target filled + exposure zero ──► COMPLETED
任意阶段出现无法确认订单/对冲失败 ────────► RECOVERY
```

Parent 和 Child 仍由 `PairExecutor` 管理。策略层只决定是否允许继续挂 Maker、是否暂停和何时恢复。

## 5. 关键参数

```env
STRATEGY_MODE=basis
BASIS_ENTRY_THRESHOLD_BP=10
BASIS_PAUSE_THRESHOLD_BP=5
BASIS_RESUME_THRESHOLD_BP=8
BASIS_EXIT_THRESHOLD_BP=0
BASIS_RESUME_EXPOSURE_BASE_QTY=0.005
BASIS_SIGNAL_INTERVAL_MS=50
```

现阶段执行器在永续成交后默认立即对冲，这是更安全的默认行为。后续如需严格复刻旧算法的“敞口达到阈值才开始 IOC”，再增加 `HEDGE_TRIGGER_BASE_QTY`，但必须保留最大敞口和 Basis 消失时的强制对冲。

## 6. 事件与命令契约

OMS 对外不要直接暴露交易所原始 JSON，建议统一为：

```json
{
  "account_id": "acct-a",
  "exchange": "okx",
  "channel": "orders",
  "event_type": "order_update",
  "inst_id": "BTC-USDT-SWAP",
  "ord_id": "123",
  "cl_ord_id": "REQC0001M001",
  "state": "live",
  "acc_fill_sz": "10",
  "fill_px": "78132.6",
  "fee": "-0.125",
  "amend_result": "0",
  "exchange_ts": "1787580019390",
  "recv_ts": "1787580019391",
  "seq": 123
}
```

策略向 OMS 发送的命令至少包含：

```text
command_id
account_id
strategy_id
request_id
child_id
attempt
inst_id
side
ord_type
size
price
reduce_only
```

命令必须幂等，`clOrdId` 必须由账户、策略、任务、子单和尝试次数确定性生成。

## 7. OMS 生产化职责

- 每账户独立私有 WS 连接和重连。
- 订单频道作为订单最终状态主来源。
- WS 下单、改单、撤单优先；REST 作为降级和定向核对通道。
- 不轮询 `orders-history` 作为常规心跳。
- 按账户统一限频，避免多个策略分散消耗同一个账户额度。
- 维护事件序号、去重和断线恢复。
- 交易所原始消息转换为统一事件后再通过 ZMQ 转发。

## 8. 启动与恢复

启动顺序：

1. 启动 OMS，完成私有 WS 登录、订阅和健康检查。
2. 启动策略，订阅 Public WS 和 OMSConsumer。
3. 策略恢复本地状态，确认活动订单归属。
4. 只有在恢复完成后才允许发出新命令。

退出顺序：

1. 策略停止接受新的 Basis 入场。
2. 撤掉活动 Maker。
3. 等待最终订单回报。
4. 对未对冲敞口执行保护性 IOC。
5. 持久化状态并通知。

## 9. 迁移步骤

### Phase 1：当前快速落地

- `STRATEGY_MODE=basis` 启用 Basis 策略协调层。
- 继续使用单进程 Public WS、Private WS 和 REST Client。
- 使用本地 Fake Exchange 覆盖入场、暂停、恢复、方向和敞口场景。
- Demo Trading 验证实际 WS 下单、改单、撤单和订单回报。

### Phase 2：OMS Gateway

- 把 `OkxV5Client` 封装为 OMS Gateway。
- 把 `FillEvent` 统一成 OMS 事件。
- 将现货 IOC 订单也加入订单回报映射，减少下单后 REST 查询。

### Phase 3：OMS 独立进程

- 每账户一个 OMS 进程。
- ZMQ IPC 传输标准化事件。
- OMSConsumer 根据 `(account, tag, channel, symbol)` 路由。
- 策略进程不再直接持有交易所连接。

### Phase 4：多策略与生产监控

- 命令所有权和账户级限频。
- 事件堆积、WS 断连、REST 降级、重连次数监控。
- 统一 Prometheus/日志指标和 Lark 告警。