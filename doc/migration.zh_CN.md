# 迁移指南

[English](migration.md) · [用户手册](user_guide.zh_CN.md) · [设计](design.zh_CN.md)

## 从 0.17 升级到 0.18

协调版本为 `qubit-event-bus = "0.18.0"`、`qubit-event-bus-redis = "0.6.0"`、`qubit-task = "0.9.0"`。所有根与消费 fixture 必须解析同一个本地 core 0.18；IoC/执行服务仅迁移 fixture，不提升生产版本。更新锁文件并运行 ecosystem metadata 门禁。

### 把接手数量与执行额度分开

旧同步 `max_in_flight` 同时限制等待与执行，异步使用另一套 admission 配置：

```rust
// Before: 0.17; historical API, removed in 0.18.
let sync = EventBusFacadeConfig::new()
    .with_sync_delivery_scheduler(SyncDeliverySchedulerConfig::new(4, 0)?);
let asynchronous = EventBusFacadeConfig::new()
    .with_delivery_admission(DeliveryAdmissionConfig::new(4)?);
```

两种 facade 现在共用以下四参数配置及独立结算预算：

```rust
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::time::Duration;

use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SettlementRetryConfig;

let scheduling = DeliverySchedulingConfig::new(
    NonZeroUsize::new(4).unwrap(),   // running handlers
    NonZeroUsize::new(256).unwrap(), // globally owned deliveries
    NonZeroUsize::new(32).unwrap(),  // owned per subscription
    NonZeroUsize::new(256).unwrap(), // registered subscriptions
)?;
let settlement = SettlementRetryConfig::new(
    NonZeroU32::new(5).unwrap(),
    Duration::from_secs(5),
    Duration::from_millis(10),
    Duration::from_secs(1),
)?;
let config = EventBusFacadeConfig::new()
    .with_delivery_scheduling(scheduling)
    .with_settlement_retry(settlement);
```

四项调度参数均为非零值；running、per-subscription 不得超过全局 owned。owned 包括 receive 预留、排队、执行和结算；暂停 session 仍计订阅数。旧 `queue=0` 的直接交接不再有同义开关：可选 `owned=running=4`、`per_subscription=4` 来压低预取，但其中仍包括 receive/settlement，不能把它当旧语义的精确替代。新默认是 4/256/32/256；旧公开配置、builder 与 getter 没有兼容别名。

### 采用有限结算重试与结构化诊断

旧结算逻辑没有明确的公共总尝试/耗时预算；新配置默认 5 次（含首次）、5 秒、10 ms 初始退避和 1 秒上限。只有 `retryable()==Some(true)` 重试；`None` 默认停止，false 立即停止。handler 不因结算错误重跑，token/disposition 不变；已在途调用不受该预算强制中断。终止只停止所属订阅，保留首个原因。

```rust
// Before: error was a display string; do not classify by its text.
if let Diagnostic::SettlementFailed { error, .. } = diagnostic {
    eprintln!("{error}");
}
```

```rust
use std::error::Error;

use qubit_event_bus::pipeline::Diagnostic;

// After: borrow the real SpiError and its original source chain.
match diagnostic {
    Diagnostic::SettlementFailed { attempt, error, .. } => {
        eprintln!("attempt={attempt}, retryable={:?}, source={:?}", error.retryable(), error.source());
    }
    Diagnostic::SettlementStopped { attempts, termination, error, .. } => {
        eprintln!("stopped after {attempts}: {termination:?}, source={:?}", error.source());
    }
    _ => {}
}
```

`SettlementFailed.error` 从字符串改为 `Arc<SpiError>`，新增 `attempt`；`SettlementStopped` 提供 attempts、termination 与同一结构化 error。保留 `terminal_failure()`，读取 bus/订阅的 `delivery_metrics()` 后修复原因，关闭旧订阅，再以同一 durable group 建立新订阅恢复未终结工作。provider 可能已应用结算，不能保证每次失败一定重投；local 重新订阅不能恢复丢弃消息。

### 更新重发与关闭分支

从“最后 ACK 无人接纳就整条重发”改为先检查 `receipt.duplicate_possible()`：为 true 时按 event ID 核对；为 false 且 `NoDestinations`/`NoneAccepted` 才考虑整条重发。部分接纳只修复被拒目标，拦截器 `Dropped` 不自动重发。`check_admission` 不检查历史。

删除“Graceful 超时后 Immediate 救援”的示例；使用两次各带期限的 Graceful，把 `TimedOut` 当尚未完成，其他错误传播，最终 false 记录快照并交外部监督器。该策略不证明不合作 handler 会结束或进程会自动退出。完整可执行双语例子见[用户手册](user_guide.zh_CN.md)。Redis `XADD` Accepted 不承诺 fsync，已有组的 StartPosition 不重置 cursor，PEL 受 trimming/claim 策略影响。任务通知仍以 `state_version` 缺口触发权威查询，不能回滚已持久化状态。

同步回调或异步应用需要不阻塞地发起停机时，改用 `EventBus::request_shutdown(mode)` 并保留 `EventBusShutdown` ticket。可在应用 executor 上调用 `wait_async().await`，或通过 `wait(Some(timeout))` 同步观察；取消异步观察或同步等待超时都不会取消清理。丢弃 ticket 只释放观察者。`AsyncEventBus` 仍使用调用方驱动的异步 `shutdown` API。详见[非阻塞地请求停机](user_guide.zh_CN.md#非阻塞地请求停机)。

以下保留早期版本迁移步骤，供跨版本升级时逐项处理。

## 从 0.16 升级到 0.17

应用与 provider 须协调升级：`qubit-event-bus` 0.17、
`qubit-event-bus-redis` 0.5；使用任务通知时采用 `qubit-task` 0.8。
旧 minor 与 0.17 属于不同的 Rust 类型/SPI 代际。更新直接依赖、可选依赖、
fixture 及 lockfile，再执行五仓 metadata 门禁和各项目 CI。仅变动消费
fixture 的 IoC/执行服务，在生产 API 不变时无须提升生产版本。
以下移除的 API 不保留兼容别名。

### 迁移 codec 并验证历史消息

将 `EventCodec::decode(&[u8])` 改为 `decode(&EncodedPayload)`，用
`payload.bytes()` 读取字节，`content_type()` 和 `schema_id()` 读取元数据。
facade 会在 decode 前调用 `validate_metadata`；默认精确比较 content type
文本及 `Option<SchemaId>`，`None` 不会隐式匹配 `Some`，也不会规范化 MIME。

支持历史 schema 的 codec 应重写验证方法，明确允许哪些版本，并按接受的
schema 选择解码逻辑。任务指南中的 JSON codec 写入 `task-event-v1`，
明确允许相同 `application/json` 下的历史 `None`；这是该 codec 的兼容规则，
不是全局默认。Redis wire 版本 1 仍可读取，公开 Rust API 升级不会删除旧
stream 记录。部署前使用新 codec 测试真实保留的历史记录。

### 替换无限额编码配置

用 `with_payload_limits(PayloadLimits::new(publish_limit, receive_limit))`
替换 `with_max_encoded_payload_bytes(Option<NonZeroUsize>)`。两个参数必须为
正数，默认各 1,048,576 字节；等于上限仍允许，没有无限额配置。发布在编码
完成后、SPI 接纳前检查，接收在元数据验证和 decode 前检查。
`CodecError::PayloadTooLarge` 新增 `PayloadDirection::Publish/Receive`。
这不是 codec 内部分配、Native 对象或 Redis 首次 RESP 分配的硬内存预算。

Redis 独立限制 `redis.max_wire_bytes`（默认 8 MiB）、
`redis.max_payload_bytes`（1 MiB）和 `redis.max_headers_bytes`（64 KiB）。
现有消息需要更大容量时，应有意设置正数 provider options。三个限额彼此独立，
payload 未超限也可能使最终 wire 超限。provider 发布检查失败发生在 `XADD` 前。

### 根据发布效果判断是否重试

`EventBus::publish`、`AsyncEventBus::publish`、批量结果、
`NotificationOutcome::PublishFailed` 和 `PublishErrorHandler` 使用
`PublishFailure`，保留原 `event_id()`、聚合 `effect()`、结构化 `cause()`
及 source 链。需要区分原因时匹配 cause，重试前先检查效果。
应用返回 `Result<_, EventBusError>` 时，可以直接传播 `bus.publish(request)?`：
`From<PublishFailure>` 使用新增透明 `EventBusError::PublishFailure`，保留事件 ID、
效果和原因。原 `Publish(PublishError)` 仍用于没有发布身份的仅原因转换。
不要为了满足聚合错误转换先调用 `into_cause()`，它会丢弃发布 wrapper。

provider 只有确定没有发生接纳时才返回含 `NotAccepted` 的 `SpiError::Publish`。
通用 operation 错误和 provider panic 保守归为 `MayHaveBeenAccepted`。

`PublishAttemptError::new` 也新增明确的 effect 参数；自定义尝试包装应更新，
不能从错误文本推断效果。

默认 `DuplicateRiskPolicy::Forbid` 是硬安全门，自定义 `RetryRule` 也不能
强制重试未知效果。只有业务接受重复时才选择 `AllowDuplicates`；仍须满足
原重试策略和可重试性条件。前序未知效果不会被后续确定拒绝抹去；后来成功的
`PublishReceipt::duplicate_possible()` 会报告该风险。typed interceptor 不能
改变事件 ID；重试共用 ID、时间戳及编码字节，Redis 不按 EventId 自动去重。

retry 取消中断已经轮询的 SPI future 并返回失败时，效果未知；SPI 前取消则
没有接纳效果。丢弃公开 publish future 不产生错误值：对已启动操作须保留
请求 ID 并按可能已发布核对，未轮询 future 没有尝试。RetryPolicy 是软预算，
不是 provider 执行中命令或同步用户代码的通用硬超时。

Redis 按阶段分类：提交前打开连接失败和明确 server 拒绝为 `NotAccepted`；
进入 `XADD` query 后的回复丢失、超时或响应转换失败为 `MayHaveBeenAccepted`。
死信转发同样受安全门约束。转发与源确认不是事务；转发成功后源结算失败，
仍可能产生重复逻辑死信，因此消费者必须幂等。

### 恢复停止的订阅

元数据不兼容、接收超限、validate/decode panic 和 Native 类型不匹配会停止
订阅，不执行 `Accept`、`Reject` 或 `Retry`。普通 `CodecError::Decode` 仍拒绝
坏消息。`terminal_failure()` 保留首个 `Arc<SubscriptionStopReason>`；异步
`run` 返回 `ReceiveError::Stopped`，同一 handle 再次运行仍返回同一原因，不再
接收。已启动 handler 继续完成，关闭错误独立报告，取消 run/close 不清除原因。

持久 provider 的恢复步骤是修复 codec/版本或容量、关闭旧 handle，再以同一
消费组创建新订阅来认领未结算工作。Redis 的 `receive_limit_exceeded` 和
`unsupported_wire_version` 保留 PEL，不执行 `XACK`、`XDEL` 或隔离；限额内
格式错误的版本 1 记录仍使用既有隔离路径。不要通过删除 pending 数据隐藏
不兼容。临时 provider 清理可能丢弃消息，只能统计已知损失；重新订阅 local
无法恢复已经丢弃的工作。

通知 close 现在同时等待处理循环与资源清理。worker 清理 panic 发布一次失败
终态，所有关闭者观察同一结果；超时后可继续等待。observer 调用 panic 仍独立
隔离。库无法恢复 `panic=abort`，也不能强制停止阻塞析构。

### 验证部署

核心单仓运行 `./scripts/project-ci-check.sh` 检查自身 metadata。五仓协调迁移运行
`./scripts/project-ci-check.sh --ecosystem-root <repos-dir>`，五个仓库及七个声明的
消费 fixture 必须存在；门禁解析 locked/all-features metadata，拒绝混用旧
minor。它不代替各项目 alignment、CI、conformance、codec 往返、持久恢复
和故障注入检查。部署后先检查保留 wire 的消费结果、终止原因、发布效果、
任务 `uncertain_publish` 和 Redis `XPENDING`，再开放业务流量。任务事件仍是
尽力通知，应按 TaskId 比较 `state_version` 并查询任务服务的权威状态。

## 早期迁移：从 0.14/0.15 到 0.16

本文说明如何从 `qubit-event-bus` 0.14 或 0.15 升级到 0.16.0。0.16 是
破坏性版本；应用和 provider 实现需要一起升级。

## 更新依赖

```toml
qubit-event-bus = "0.16"
```

provider crate 也应依赖对应的 SPI 版本。部署前，使用 0.16 运行 provider
自己的 conformance 测试。

## 更新死信策略构造函数

这些构造函数已重命名，以明确表达接纳保证：

| 旧调用 | 0.16 调用 |
| --- | --- |
| `DeadLetterPolicy::topic(name)` | `DeadLetterPolicy::with_topic_name(name)` |
| `DeadLetterPolicy::known_destination(name)` | `DeadLetterPolicy::with_known_destination(name)` |

`with_topic` 和 `with_admission` 仍可使用。只有 provider 能报告目标接纳情况时，
才选择 known-destination 策略；opaque acknowledgement 无法满足这一要求。

## 更新 conformance hooks

`ConformanceHooks` 和 `AsyncConformanceHooks` 新增了 `ephemeral_cleanup`。
部分结构体初始化应补上 `..Default::default()`，或显式初始化全部 hook。Strict
模式下，durable provider 缺少恢复 fixture、ephemeral provider 缺少清理 fixture
都会失败；跳过的用例不代表通过。

同步 conformance runner 会把 future cancellation 检查标记为 `NotApplicable`，
因为同步 API 没有可取消的 future。请继续单独测试 receiver close、shutdown、重复
settlement 和 provider 恢复。异步 provider 对可取消的操作仍需保留由 barrier 控制的
真实测试 fixture。

## 重新核对消息恢复假设

Durable 订阅在 receiver close 或 drop 后，必须保留已接纳但尚未终结的消息，使其
能够恢复。Ephemeral 订阅在 receiver 销毁时可以丢弃未结算消息。两种清理行为都不
会隐式确认消息。取消 receive future 与销毁 receiver 不同：如果消息已被消费，之后
的 receive 或 provider 恢复流程仍必须能取回它。请根据 provider 实际的 durability
和 settlement 能力，复核 shutdown 与死信失败处理。

完整契约和可运行示例见[用户手册](user_guide.zh_CN.md)、[设计文档](design.zh_CN.md)
和 [API 文档](https://docs.rs/qubit-event-bus/0.16.0/qubit_event_bus/)。
