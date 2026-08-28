# Pair Trading 系统化建设方案

本文说明如何在当前 OKX Pair Executor MVP 的基础上，结合 tmp/ 中已有的 OMS、资产检查和策略代码，逐步建设成可长期运行、可扩展多账户和多策略的交易系统。

这不是一次性重写方案。目标是先保持当前已经能够在 OKX Demo Trading 上完成的交易能力，再逐步把连接管理、账户级风控、事件分发、持久化和运维能力补齐。

## 1. 建设目标和边界

### 1.1 目标

系统最终应支持：

- OKX Demo 和实盘两套环境；
- 一个或多个账户；
- Pair 直接执行和 Basis 价差触发两种策略；
- short_spot_long_swap、long_spot_short_swap 两个方向；
- open、close 两种动作；
- 按基础币数量拆单；
- 永续合约 Maker、现货 IOC 对冲；
- Maker 部分成交、IOC 部分成交、撤单/成交竞态、网络重连和进程重启恢复；
- 账户资产、借贷、保证金和永续仓位的实时风险控制；
- Lark 状态、风险和最终报告；
- 后续接入 TWAP 或其他单腿/多腿策略。

### 1.2 当前不做的事情

第一阶段不建议同时建设完整的交易平台、Web 管理后台、数据库集群或高频做市系统。当前任务的核心是“可靠地完成两条腿配对执行”，系统化建设优先保证：

1. 不重复下单；
2. 不丢失成交回报；
3. 敞口可计算、可限制、可恢复；
4. 重启后知道哪些订单已经成交；
5. 任何异常都有日志、告警和人工接管路径。

## 2. 现有代码和 tmp 的定位

### 2.1 当前仓库已经具备的能力

当前版本适合作为“单账户、单策略进程”的快速落地版本：

- PairExecutor 管理父订单、拆单、Maker 和现货 IOC；
- 使用订单累计成交量计算成交增量，避免重复对冲；
- 支持两种方向、开仓和平仓、reduce-only；
- 使用私有订单 WebSocket 作为主状态来源；
- REST 只对已知活动订单做定向核对和在启动/恢复时校准；
- LatestBookQueue 把公共 BBO 接收和执行决策解耦，避免行情回调被下单请求阻塞；
- Maker 使用 post-only 和原子改单，设置最小改单间隔；
- JSON 状态文件支持任务恢复；
- Lark 通知包含开始、运行中状态、风险和最终结果；
- 本地 Fake Exchange 场景测试和 Demo Trading 已覆盖多种异常。

这些能力不应在迁移到系统架构时被推倒重写。

### 2.2 tmp 中值得借鉴的部分

tmp/basis.py、tmp/twap.py 和 tmp/support/ 体现了比较好的目标分层：

- 每账户一个 OMS 进程，集中持有私有 WebSocket；
- 策略只接收标准化订单回报和公共行情；
- 公共行情驱动策略，而不是固定频率轮询行情；
- 账户资产、可用余额、冻结余额、仓位和保证金参与交易前检查；
- Ctrl+C 时撤单、处理敞口并通知；
- 通过标签、频道、symbol 路由到策略。

但 tmp 不能直接作为生产实现：

- OMSConsumer 内部直接 await handler，慢回调会阻塞后续消息；
- ZMQ PUB/SUB 没有回放和确认机制，断线期间可能丢事件；
- 订单事件没有完整的持久化、序列检查和幂等处理；
- 对冲循环在异步成交回报延迟时存在重复 IOC 风险；
- 回调中直接做 REST 操作会影响行情和私有 WS 接收；
- 有些余额/借贷逻辑是占位实现，不能视为已经支持自动借贷。

因此建议“借鉴架构，不复制实现”。

## 3. 目标系统架构

~~~mermaid
flowchart TB
    subgraph L3[策略层]
        PAIR[Pair Strategy - 直接执行请求]
        BASIS[Basis Strategy - 价差触发/暂停/恢复]
        FUTURE[未来策略 - TWAP / 其他]
    end

    subgraph L2[执行与适配层]
        CONSUMER[Strategy Runtime / OMS Consumer - 路由、幂等、生命周期]
        RISK[Risk Coordinator - 敞口、保证金、熔断]
        EXEC[Pair Executor - Maker + IOC + Recovery]
        REPORT[Report Outbox - Lark 重试/去重]
    end

    subgraph L1[账户 OMS 层：每账户一个进程]
        OMS[Account OMS - 连接管理、账户事件、限频、命令所有权]
        EVENTLOG[Event Log / Snapshot]
    end

    subgraph OKX[OKX]
        PWS[Public WS - books5 / BBO]
        AWS[Private WS - orders / fills / account]
        REST[REST - 下单、改单、撤单、定向核对]
    end

    PAIR --> CONSUMER
    BASIS --> CONSUMER
    FUTURE --> CONSUMER
    PWS --> CONSUMER
    CONSUMER --> RISK
    RISK --> EXEC
    EXEC --> CONSUMER
    CONSUMER --> REPORT
    CONSUMER <--> OMS
    OMS --> EVENTLOG
    AWS --> OMS
    OMS <--> REST
~~~

### 3.1 三条数据路径

读行情路径：

~~~text
OKX Public WS → 快速解析 → 最新 BBO 缓存 → spread tracker → 决策队列 → 策略/执行器
~~~

行情接收回调只做轻量工作：校验、记录时间、更新 BBO、计算观察值、放入 latest-only 队列。不能在回调中等待下单、改单、Lark 或数据库。

读订单和账户路径：

~~~text
OKX Private WS → Account OMS → 标准化事件 → 事件日志 → 路由队列 → 执行器/风控/报告
~~~

订单和账户事件必须尽量保序、可去重。与行情不同，订单事件不能简单地只保留最新一条，因为中间的成交增量可能影响对冲。

写交易路径：

~~~text
策略意图 → 风控准入 → OMS 命令队列 → OKX Trade WS → REST 降级 → 回报事件
~~~

下单、改单和撤单是写操作，可以通过私有 Trade WS 优先发送；REST 作为失败降级和定向核对。orders-history 不作为常规心跳。

## 4. 推荐的代码分层

当前包可以逐步演进为以下结构，第一阶段不必一次创建所有目录：

~~~text
src/okx_pair_executor/
├── domain/
│   ├── models.py              # 订单、成交、BBO、资产、事件契约
│   └── enums.py
├── gateway/okx/
│   ├── rest.py                # 下单/改单/撤单/定向查询
│   ├── public_ws.py           # books5
│   ├── private_ws.py          # orders/account/positions
│   └── normalize.py           # OKX JSON -> 统一事件
├── oms/
│   ├── account_oms.py         # 单账户连接、重连、限频、命令所有权
│   ├── consumer.py            # 策略订阅和路由
│   └── recovery.py            # 断线和重启恢复
├── execution/
│   ├── pair_executor.py       # 当前 PairExecutor 的后续归属
│   ├── maker_policy.py        # 报价、改单和 post-only
│   ├── hedge_policy.py        # IOC、部分成交和恢复
│   └── quantity.py            # lot/min/合约换算
├── strategy/
│   ├── pair.py
│   ├── basis.py
│   └── twap.py
├── risk/
│   ├── preflight.py           # 启动前检查
│   ├── exposure.py            # 实时敞口
│   └── kill_switch.py
├── storage/
│   ├── event_log.py
│   └── snapshot_store.py
├── reporting/
│   ├── lark.py
│   └── outbox.py
└── observability/
    ├── metrics.py
    └── health.py
~~~

模块依赖方向应保持单向：策略依赖领域模型和 OMS 接口，执行器依赖 OMS 接口，不直接依赖具体 OKX HTTP/WebSocket 实现。这样 Fake Exchange、Demo 和实盘可以使用同一套执行逻辑。

## 5. 统一事件和命令契约

交易所原始 JSON 只在 Gateway 内部存在，不能直接暴露给策略。

### 5.1 BBO 事件

~~~json
{
  "event_type": "bbo",
  "account_id": null,
  "inst_id": "BTC-USDT-SWAP",
  "best_bid": "79000.1",
  "best_ask": "79000.2",
  "exchange_ts": "1787800000000",
  "received_ts": "1787800000004",
  "seq": 12345,
  "source": "public_ws"
}
~~~

必须保留交易所时间和本地接收时间，后续才能区分市场本身变化、网络延迟和内部队列延迟。

### 5.2 订单/成交事件

~~~json
{
  "event_type": "order_update",
  "account_id": "acct-demo",
  "request_id": "REQ-001",
  "child_id": "REQ-001-C0001",
  "inst_id": "BTC-USDT-SWAP",
  "ord_id": "123456",
  "cl_ord_id": "REQ001C0001M001",
  "state": "partially_filled",
  "acc_fill_sz": "7",
  "fill_px": "79000.1",
  "fee": "-0.08",
  "trade_id": "T-001",
  "exchange_ts": "1787800000100",
  "received_ts": "1787800000104",
  "source": "private_ws",
  "event_id": "okx:acct-demo:123456:T-001"
}
~~~

执行器使用 ord_id + acc_fill_sz 或 trade_id 做幂等处理，不能因为同一订单回报重复到达而再次发起相同对冲。

### 5.3 账户事件

账户事件至少包含：账户、币种、可用、冻结、总额、借贷/负债、保证金率、永续仓位、仓位方向、可平数量和时间戳。资产核对必须以成交回报和账户/仓位回报分别计算，不能只相信某一条数据。

### 5.4 交易命令

~~~json
{
  "command_id": "REQ-001-C0001-M001",
  "account_id": "acct-demo",
  "request_id": "REQ-001",
  "child_id": "REQ-001-C0001",
  "attempt": 1,
  "inst_id": "BTC-USDT-SWAP",
  "side": "sell",
  "ord_type": "post_only",
  "size": "10",
  "price": "79000.1",
  "td_mode": "cross",
  "reduce_only": false,
  "expires_at": "1787800001000"
}
~~~

command_id 和 clOrdId 必须确定性生成。重试时先查询命令是否已经被交易所接受，再决定是否重新发送，避免网络超时造成重复下单。

## 6. 订单生命周期

### 6.1 Parent 生命周期

~~~mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> PREFLIGHT: 参数/资产/仓位检查通过
    PREFLIGHT --> RUNNING: 允许执行
    PREFLIGHT --> FAILED: 检查失败
    RUNNING --> PAUSED: Basis 消失/风险接近阈值
    PAUSED --> RUNNING: 信号恢复且敞口安全
    RUNNING --> RECOVERY: 对冲失败/状态不确定/超限
    PAUSED --> RECOVERY: 无法安全恢复
    RUNNING --> COMPLETED: 目标完成且敞口归零
    RECOVERY --> COMPLETED: 人工或自动恢复完成
    RECOVERY --> FAILED: 无法恢复，需要人工接管
    RUNNING --> CANCELED: 用户停止且敞口已处理
~~~

### 6.2 每个 child 的生命周期

~~~mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> MAKER_WORKING: Maker accepted
    MAKER_WORKING --> REPRICING: BBO 偏离且超过最小间隔
    REPRICING --> MAKER_WORKING: amend accepted
    REPRICING --> MAKER_WORKING: cancel/replace accepted
    MAKER_WORKING --> HEDGE_PENDING: 收到新的累计成交量
    HEDGE_PENDING --> HEDGE_EXECUTING: 发送 spot IOC
    HEDGE_EXECUTING --> HEDGE_PENDING: IOC 部分成交且有剩余
    HEDGE_EXECUTING --> MAKER_WORKING: 本次成交已对冲，Maker 仍有剩余
    MAKER_WORKING --> COMPLETED: Maker 终态且累计量已对冲
    HEDGE_EXECUTING --> RECOVERY: 重试耗尽/敞口超限/状态不确定
    RECOVERY --> COMPLETED: 剩余敞口已处理
    RECOVERY --> FAILED: 需要人工接管
~~~

一个 Maker 订单收到累计成交 10，此前已处理累计成交 7，只允许对冲增量 3。现货 IOC 如果成交 2，剩余 1 要进入下一次对冲，不得按原始目标量重复发送。

剩余量小于交易所最小下单量时：

- 不是最后一个 child：合并到下一个 child 的目标量；
- 是最后一个 child：如果在容忍度内，标记为 dust 并在报告中明确；如果超过容忍度，进入 Recovery，使用允许的最小数量保护性对冲或等待人工处理；
- 永远不能静默地把未对冲量当作完成。

## 7. Pair 与 Basis 策略在系统中的关系

两者共用同一个执行器，区别只在“何时允许创建/继续执行任务”。

| 项目 | Pair | Basis |
|---|---|---|
| 触发 | 收到命令后立即执行 | BBO 价差达到入场阈值后执行 |
| 执行中 | 主要由订单回报驱动 | 订单回报 + BBO 信号共同驱动 |
| 暂停 | 通常不自动暂停 | 价差跌破 pause 阈值或风险超限时暂停 |
| 恢复 | 由恢复流程处理 | 价差达到 resume 阈值且敞口安全时恢复 |
| 适用场景 | 确定执行一笔配对订单 | 只在市场条件满足时持续捕捉机会 |

Basis 的进入信号应使用可执行盘口，而不是两个中间价：

- 买现货、卖永续： (perp_bid - spot_ask) / spot_ask；
- 卖现货、买永续： (spot_bid - perp_ask) / perp_ask。

这是入场资格，不是最终成交结果。最终报告仍须用真实成交均价、手续费和实际成交时间计算执行价差。

建议市场 TWAP 同时保留两种定义：

1. 时间加权 BBO Basis：每次 BBO 变化记录一次，并按相邻报价持续时间加权，反映市场在执行期间通常处于什么水平；
2. 可执行成交参考：按每个执行时刻的对应 Bid/Ask 计算，反映当时按照目标方向立即成交的成本。

当前 spread.py 已有基础实现，后续只需补充时间戳、报价年龄、样本数和无效样本统计，不能把 BBO 数量直接乘在价格上称为时间 TWAP。

## 8. 执行效率设计

### 8.1 Maker 报价

永续腿是 Maker，默认逻辑：

1. 收到合约 BBO；
2. 按方向选择能保持 post-only 的价格；
3. 当前订单距离目标 BBO 超过一个 tick，且距离上次改单达到 150ms 左右时，发起改单；
4. 使用原子 amend；交易所不支持或失败时才 cancel/replace；
5. 只允许一个活动 Maker 命令，撤单确认或成交状态确认后再发下一次。

每次 BBO 跳动不必都改单。需要同时判断：报价是否已经过时、价格是否真的偏离、订单是否仍然 live、是否有未完成的改单、队列是否拥塞。若 BBO 已经过旧，宁可撤掉等待新报价，也不要继续用旧价挂单。

### 8.2 现货 IOC

永续成交后，按累计成交增量换算成基础币数量，立刻向现货对侧发送 IOC：

- 价格按当前可执行盘口和最大滑点计算；
- 下单响应只说明请求已接受，实际成交量必须由订单回报或定向 get_order 确认；
- 部分成交后只重试剩余量；
- IOC 失败、余额不足、借贷不足或价格保护失败时立即进入可解释的风险状态；
- 不在一个循环里无限重试。

### 8.3 交易效率指标

每个任务和每个账户至少记录：

- BBO 接收至执行器处理的 P50/P95/P99；
- BBO 队列积压和 coalesce 比例；
- Maker 下单 ACK、改单 ACK、撤单 ACK；
- Maker 报价年龄和等待时间；
- Maker 成交回报到 IOC 提交的延迟；
- IOC ACK、IOC 实际成交率和重试次数；
- WS 断连次数、REST 降级次数、定向核对次数；
- 未对冲峰值、恢复时长、最终敞口。

第一阶段可继续使用现有 JSON 效率报告；系统化后应输出 Prometheus 指标和按任务聚合的报告。

## 9. OMS 设计和 orders-history 使用规则

### 9.1 Account OMS 负责什么

每个账户 OMS 统一负责：

- 私有 WS 登录、订阅、心跳、重连；
- 订单、成交、余额、仓位事件标准化；
- Trade WS 下单/改单/撤单；
- REST fallback 和限频；
- 账户级 command queue，防止多个策略争抢同一个账户的额度；
- 维护 ord_id、cl_ord_id、策略任务的映射；
- 记录事件序号、断线时间和重连后的恢复状态；
- 将事件广播给订阅策略。

### 9.2 什么时候允许 REST 查询

正常运行不轮询 orders-history。允许使用 REST 的场景：

- 启动恢复时查询本任务的已知订单；
- 私有 WS 重连后检查断线窗口内的已知活动订单；
- 订单回报序号出现 gap；
- 下单/改单/撤单响应超时，需要按 clOrdId 幂等确认；
- 最终报告前做已知订单和资产核对；
- 人工诊断或定时低频审计。

不能用全量 orders-history 作为每秒心跳。查询应该按 symbol、时间窗口、已知订单 ID 尽量收窄，并带退避和限频统计。

### 9.3 队列原则

- 公共 BBO：latest-only，允许合并旧报价；
- 私有订单/成交：有界但不丢失，串行处理，处理异常隔离；
- 账户资产：按事件序号或快照版本更新；
- Lark：独立 outbox，交易执行不等待通知成功。

当前 dispatch.py 已完成第一阶段的进程内解耦。后续引入 ZMQ 时，应在此接口之上增加序号、重放和队列监控，不应把目前的同步回调重新搬回消息接收线程。

## 10. 风控和账户状态

### 10.1 启动前 Preflight

每个任务开始前检查：

- Demo/实盘环境和 API 权限；
- spot/swap instrument、tick、lot、min、合约面值；
- 账户模式、持仓模式、tdMode、reduce-only 兼容性；
- 现货可用余额、冻结余额、借贷权限和 Cross/Isolated 条件；
- 永续可用保证金、当前持仓、最大杠杆和可开数量；
- 方向和当前仓位是否匹配；
- 是否存在相同 request_id 或孤儿活动订单；
- 目标量换算后是否能被两条腿的最小单位表示；
- 目标价差是否覆盖手续费、滑点、资金费缓冲和安全边际。

### 10.2 运行中风控

风控协调器持续接收订单和账户事件，计算：

~~~text
净配对敞口 = 永续成交基础币数量 - 现货成交基础币数量
账户实际敞口 = 交易任务敞口 + 任务启动前账户仓位/余额变化
~~~

触发以下任一条件时暂停挂新 Maker，并推送具体原因：

- 未对冲敞口超过硬上限；
- 可用保证金或借贷额度不足；
- 订单状态不确定；
- 连续 IOC 失败；
- BBO 过期或行情 WS 断线；
- 超过任务最大执行时间或改单次数；
- 价差跌破策略允许范围。

风险卡片要包含错误码、交易所原始 sCode/sMsg、订单 ID、当前敞口、已成交量和建议动作，不能只推送“EXPOSURE_LIMIT”。

### 10.3 熔断和人工接管

至少要有：

- 进程级 kill switch：停止新下单、撤 Maker、处理现有敞口；
- 账户级 kill switch：所有策略停止写操作；
- 任务级 cancel：只停止当前任务；
- recovery lock：状态不确定时禁止盲目重启提交；
- 人工确认后才能从 FAILED/RECOVERY 重新执行。

## 11. 持久化、恢复和一致性

### 11.1 当前阶段

单任务 JSON 状态文件可以继续使用，但必须：

- 原子写入临时文件后 rename；
- 每次订单状态变化保存关键字段；
- 保存 request、child、订单 ID、clOrdId、累计成交、手续费、最后事件时间；
- 不把 API key 写入状态文件；
- 状态文件和报告文件按任务隔离。

### 11.2 系统阶段

建议升级为：

~~~text
不可变 Event Log
        ↓
定期 Snapshot
        ↓
Runtime State Projection
~~~

事件日志保存原始标准化事件和处理结果；快照用于快速恢复；Projection 用于查询当前任务状态。SQLite 足以支撑单服务器多个账户，后续才考虑 PostgreSQL。

恢复流程：

1. 读取最近快照和事件日志；
2. 校验任务和账户；
3. 连接私有 WS；
4. 查询已知活动订单和断线窗口内状态；
5. 对比本地累计成交、交易所订单和账户仓位；
6. 找出孤儿订单和未对冲量；
7. 只有状态确定并通过风控后，才允许发新命令。

绝不能因为进程刚启动就重新提交所有未完成 Maker；必须先按确定性的 clOrdId 查明原命令是否已被接受。

## 12. 报告和通知

通知系统应与交易主流程隔离，用 outbox + 重试 + 去重保证“尽量送达”，但 Lark 失败不能阻塞交易。

建议只保留四类卡片：

1. EXECUTION_STARTED：任务参数、方向、动作、目标量、拆单量、tdMode、风险阈值；
2. EXECUTION_STATUS：每 30 秒或状态显著变化时，当前状态、成交量、敞口、Maker 等待/对冲情况、最近错误；
3. EXECUTION_RISK：下单失败、余额/保证金不足、WS 断线、恢复状态、交易所错误码；
4. EXECUTION_COMPLETED/FAILED：最终成交数量、合约/现货均价、手续费、实际价差、市场 TWAP、效率、最终敞口和资产/仓位核对。

通知中不要逐个推送 child 的中间噪声。详细 child 和订单明细写入 JSON/Markdown 报告；Lark 只展示能帮助判断交易是否正常的摘要。

## 13. 测试体系

### 13.1 单元测试

- 数量换算、lot/min、合约面值和 dust 合并；
- 两个方向和 open/close 的下单方向；
- reduce-only 和单向持仓参数；
- 累计成交增量、重复回报、乱序回报；
- IOC 部分成交和重试；
- Maker 成交/撤单竞态；
- 原子改单和 cancel/replace fallback；
- 资产与仓位差异计算；
- Lark 卡片字段和通知重试。

### 13.2 场景测试

至少覆盖：

- 0.01、0.1、1 和多个 BTC；
- 单 child、多 child、不均匀尾单；
- 两方向开仓和两方向平仓；
- 现货 cross、cash、isolated 的参数校验；
- Maker 全成、部分成、长时间不成、频繁改单；
- IOC 全成、部分成、零成交、连续失败；
- 最小下单量不足、最后一批 dust；
- 余额不足、保证金不足、借贷额度不足；
- WS 断线、重连、重复/乱序事件、REST 500/401；
- Lark 失败、进程 Ctrl+C、重启恢复；
- Basis 入场、暂停、恢复、退出和运行中修改阈值；
- 1 BTC 任务最终成交回报、余额和仓位三方核对。

### 13.3 Demo 验证顺序

1. demo_readonly_check.sh：规则、余额、仓位、账户模式；
2. 一笔 0.01 BTC Pair 开仓和平仓；
3. 一笔 0.1 BTC 多 child 开仓和平仓；
4. 一笔 1 BTC Demo 开仓和平仓；
5. Basis 低阈值等待、触发、暂停和恢复；
6. 导出效率报告，重点看 quote age、改单 P95、IOC 成交率和 WS/REST 降级；
7. 检查 OKX 实际活动订单、成交、资产和仓位；
8. 只有连续多次 PASS，且没有 RECOVERY、未解释敞口或 CHECK_REQUIRED，才考虑实盘极小量。

## 14. 部署演进

### Phase 1：当前单进程快速落地

~~~text
一个账户 + 一个策略进程
Public WS + Private WS + REST + PairExecutor
JSON 状态 + Lark
~~~

适合 Demo 和第一阶段实盘验证。每个进程只管理一个任务，降低并发和账户资源争抢风险。

### Phase 2：单账户 OMS

~~~text
Account OMS（长期运行）
        ↑↓ 本地 IPC/进程内接口
策略进程 / PairExecutor / BasisStrategy
~~~

OMS 接管私有 WS、Trade WS、REST 限频、账户事件和订单映射。策略进程不再直接管理交易所私有连接。

### Phase 3：多账户、多策略

~~~text
OMS-Account-A ─┐
OMS-Account-B ─┼─ Strategy Runtime / OMSConsumer
OMS-Account-N ─┘
~~~

此时需要：

- account_id、strategy_id、request_id、symbol 路由；
- 命令所有权；
- 账户级限频；
- 事件日志和重放；
- systemd/supervisor 自动拉起；
- health endpoint 和监控告警。

### Phase 4：生产运维

- systemd 管理 OMS 和策略进程；
- 日志按账户和任务轮转；
- Prometheus/Grafana 监控；
- SQLite/PostgreSQL 保存事件和报告索引；
- 密钥使用服务器环境变量或 Secret Manager；
- 实盘账户与 Demo 账户完全隔离；
- 部署前自动运行本地场景测试和只读检查。

tmp/support/oms.py 和 oms_consumer.py 可以作为 Phase 2 的原型，但生产版必须补充事件序号、重连恢复、持久化、慢消费者隔离和队列指标。

## 15. 推荐落地顺序

### 第一步：保持现有执行器稳定

冻结当前已验证的 Pair Executor 行为，继续补测试和 Demo 报告，不在此阶段引入跨进程通信。

### 第二步：补账户状态和风险协调器

把余额、冻结、借贷、保证金、仓位和可平数量转成标准事件；将余额不足、最大杠杆、仓位方向错误变成明确的风控状态。

### 第三步：补事件日志和通知 outbox

在现有 JSON 状态之外记录标准化订单事件，Lark 改为异步 outbox，确保交易状态和通知状态分开。

### 第四步：抽象 OMS 接口

先在同一进程内实现 OmsGateway 接口，Fake、Demo、实盘都走同一套命令和事件契约。

### 第五步：拆出 Account OMS 进程

使用 ZMQ IPC 或本机 Unix socket。ZMQ 可以复用 tmp 的思路，但必须增加 bounded queue、序号、重连和 replay；不应以“共享内存级延迟 <0.1ms”作为未经测量的设计前提。

### 第六步：再接入更多策略

TWAP、Basis 和未来策略都只生成策略意图，执行细节由统一执行器和 OMS 管理。任何新策略必须复用风控、持久化、报告和恢复组件。

## 16. 关键设计决策

建议现在确定以下原则：

- WS 是行情、订单和账户状态的主读路径；
- REST 是写入降级、启动恢复和定向核对路径；
- orders-history 不做常规轮询；
- 订单状态必须按累计成交和唯一事件幂等处理；
- 公共行情可以合并，成交事件不能静默丢失；
- 执行器不等待 Lark；
- 账户风控优先于策略继续下单；
- Pair 和 Basis 共用执行器，不维护两套下单逻辑；
- 单账户系统稳定前，不引入多 child 并发；
- 实盘默认关闭提现权限，Demo/Live 密钥和状态目录分离；
- 任何 RECOVERY、非零敞口或 CHECK_REQUIRED 都必须先核对再继续。

按这个路线，当前代码可以直接作为 Phase 1 运行，tmp 的 OMS 结构作为 Phase 2 原型，最终形成“策略层—执行/风控层—账户 OMS—交易所 Gateway”的系统，而不是把策略、连接、账户和通知继续堆在一个进程里。
