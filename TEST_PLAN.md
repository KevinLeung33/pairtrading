# OKX Pair Executor 测试方案

## 1. 测试目标

验证以下核心行为：

1. 父订单可以按基础币数量拆成多个子订单；
2. 合约腿始终使用 `post_only` Maker；
3. 合约部分成交后，只按新增成交量发送现货 IOC；
4. 现货 IOC 部分成交时，系统可以继续计算并处理剩余敞口；
5. Maker 撤单重挂不会重复计算历史成交；
6. WebSocket 断线后可以通过 REST 恢复状态；
7. 程序重启后可以从本地状态恢复；
8. 每个子订单和父订单都能生成 Lark 报告；
9. Demo Trading 下不会出现重复下单、超额对冲或未控制的敞口。

## 2. 环境和权限

不需要提供完整电脑环境。推荐在项目目录创建独立虚拟环境：

```powershell
cd E:\code\script
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test]"
```

需要准备一个 OKX Demo Trading API Key：

- 只开 `Read` 和 `Trade` 权限；
- 不开 `Withdraw`；
- 如果 OKX 支持，绑定当前机器的 IP；
- 使用独立 Demo 子账户；
- 不要把 API key、secret、passphrase 发到聊天中；
- 写入本地环境变量或 `.env`，不要提交到 Git。

## 3. 测试阶段

### 阶段 A：静态检查和单元测试

不连接网络、不调用 OKX。

测试内容：

- 数量拆分；
- `ctVal` 合约张数转换；
- `tickSz`、`lotSz`、`minSz` 校验；
- 合约成交增量计算；
- 重复 WebSocket 事件去重；
- 现货 IOC 部分成交；
- 超过敞口阈值进入 `RECOVERY`；
- 最大重试次数；
- Maker 重挂后的成交累计量；
- JSON 状态保存和恢复；
- 重复 `request_id` 拒绝。

执行：

```powershell
$env:PYTHONPATH = "src"
python -m pytest -q
python scripts/smoke.py
```

通过标准：所有测试通过，且 smoke test 输出 `smoke ok`。

### 阶段 B：模拟撮合测试

使用 Fake Exchange 模拟以下事件序列：

#### B1. 完整成交

```text
Maker 目标 0.1 BTC
Maker 完整成交 10 张
IOC 完整成交 0.1 BTC
最终敞口 0
```

期望：子订单和父订单均为 `COMPLETED`。

#### B2. Maker 部分成交

```text
Maker 成交 4 张
IOC 对冲 0.04 BTC
Maker 再成交 6 张
IOC 对冲 0.06 BTC
```

期望：两次对冲，不重复对冲第一次的 0.04 BTC。

#### B3. IOC 部分成交

```text
合约成交 0.1 BTC
第一次 IOC 成交 0.05 BTC
第二次 IOC 成交剩余 0.05 BTC
```

期望：最终敞口为 0，IOC 次数为 2。

#### B4. 对冲失败

```text
合约成交
IOC 连续失败
达到最大重试次数
```

期望：停止新的 Maker，父订单进入 `RECOVERY`，发送报警报告。

#### B5. Maker 重挂

```text
旧 Maker 成交 4 张
撤销旧订单
新 Maker 剩余目标 6 张
新 Maker 成交 6 张
```

期望：合计成交 10 张，不能因为新订单的累计成交量从 0 开始而丢失或重复计算。

#### B6. 重复事件

```text
重复发送同一个 accFillSz
```

期望：不会重复发送现货 IOC。

### 阶段 C：OKX Demo API 冒烟测试

使用极小数量，例如：

```text
BTC 现货：0.001 BTC 或交易规则允许的最小数量
拆单数量：最小可交易数量
最大滑点：很小的保护值
最大敞口：小于一个正常子订单
```

测试顺序：

1. 查询账户和交易规则；
2. 查询现货余额和合约保证金；
3. 连接公共 `books5` WebSocket；
4. 连接私有 `orders` WebSocket；
5. 只读取盘口，不下单；
6. 提交一个极小 Demo Maker 单；
7. 验证订单状态和撤单；
8. 再执行一个极小的完整配对测试；
9. 检查本地状态、REST 状态和持仓是否一致。

验收要求：

- 下单响应中的 `ordId` 能被 WebSocket 找到；
- WebSocket 的成交累计量与 REST 查询一致；
- 撤单后没有残留活动订单；
- 现货实际成交量不超过合约实际成交对应数量；
- 任务结束后敞口在容差内；
- Lark 收到子订单和父订单报告。

### 阶段 D：异常和恢复测试

在 Demo 环境执行：

- 下单后断开公共 WebSocket；
- 下单后断开私有 WebSocket；
- 在 IOC 执行期间停止程序；
- 程序重启后执行恢复；
- 模拟 REST 请求超时；
- 模拟重复订单事件；
- Maker 订单长时间不成交；
- Maker 被交易所取消；
- IOC 只成交一部分；
- 余额或保证金不足。

验收要求：

- 恢复前不创建新的 Maker；
- 重连后先 REST 校准，再处理新的成交事件；
- 未对冲敞口超过阈值时进入恢复流程；
- 不因请求超时而重复下单；
- 程序退出前不会留下未记录的本地状态。

### 阶段 E：长时间 Demo 运行

建议运行 2～4 小时，观察：

- WebSocket 是否持续稳定；
- 重连次数；
- REST 校准次数；
- Maker 撤单重挂次数；
- IOC 部分成交次数；
- 最大未对冲敞口；
- 本地状态文件是否持续可恢复；
- Lark 是否出现重复或遗漏报告。

## 4. 安全停止条件

出现以下任一情况立即停止测试并撤销全部活动订单：

- 本地敞口和交易所持仓不一致；
- 现货对冲数量超过合约成交对应数量；
- 无法确认订单是否已经提交；
- 私有 WebSocket 和 REST 同时不可用；
- 未对冲敞口超过配置上限；
- 发现重复下单；
- 订单状态无法恢复；
- API key 权限或 Demo/实盘环境判断异常。

## 5. 最终验收标准

满足以下条件才考虑进入小规模实盘：

- 阶段 A、B 全部通过；
- 阶段 C 至少完成 10 次 Demo 配对交易；
- 阶段 D 所有故障场景均能恢复或安全停止；
- 阶段 E 连续运行无未解释敞口；
- 所有订单均有本地 `request_id`、`child_id`、`clOrdId`；
- 所有异常都能在 Lark 中收到；
- 实盘仍然默认关闭，并设置独立的最大仓位和最大敞口。
