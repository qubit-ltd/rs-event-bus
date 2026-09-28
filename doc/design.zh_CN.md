# Qubit Event Bus 设计文档（0.15）

> 本文档以 `qubit-event-bus` 0.15.0 的实际源码为准。
> 如果文档与代码出现分歧，以代码为准，并请修订本文档。
> 英文版：[design.md](design.md)。
>
> 阅读对象：需要理解 crate 内部结构的维护者、需要实现 provider 的后端作者，
> 以及需要判断"某个保证到底由谁负责"的集成方。日常使用请先读
> [用户手册](user_guide.zh_CN.md)。

---

## 目录

1. [定位与设计理念](#1-定位与设计理念)
2. [总体架构](#2-总体架构)
3. [领域模型](#3-领域模型)
4. [Provider SPI](#4-provider-spi)
5. [能力模型](#5-能力模型)
6. [Registry、发现与 provider 装配](#6-registry发现与-provider-装配)
7. [Facade 公共层：配置、发布管线与消费管线](#7-facade-公共层配置发布管线与消费管线)
8. [同步 Facade：`EventBus`](#8-同步-facadeeventbus)
9. [异步 Facade：`AsyncEventBus` 与 `AsyncSubscription`](#9-异步-facadeasynceventbus-与-asyncsubscription)
10. [内置 `local` provider](#10-内置-local-provider)
11. [`NotificationPublisher`：单类型通知的轻量出口](#11-notificationpublisher单类型通知的轻量出口)
12. [错误模型](#12-错误模型)
13. [诊断与指标](#13-诊断与指标)
14. [并发不变量汇总](#14-并发不变量汇总)
15. [测试与验证策略](#15-测试与验证策略)
16. [公开 API 稳定性](#16-公开-api-稳定性)
17. [非目标与已知边界](#17-非目标与已知边界)

---

## 1. 定位与设计理念

### 1.1 这个库解决什么问题

`qubit-event-bus` 为 Rust 应用提供一套**类型安全的发布/订阅 API**，同时把
"消息如何被传输"这件事交给可插拔的 provider。它要同时满足三类使用者：

- **应用代码**：只想写 `bus.publish(request)` 和 `bus.subscribe(request, handler)`，
  拿到强类型的 `Delivery<T>`，不关心底层是进程内队列还是消息中间件。
- **后端作者**：实现"发一条字节/对象消息、收一条消息、确认一条消息"以及
  provider 自身的关闭操作；重试、死信、中间件、保序、背压和 facade 生命周期协调
  等通用逻辑由 facade 负责。
- **集成/运维方**：需要清楚地知道某项保证（顺序、延迟、持久化、回执语义）
  到底由 facade 提供还是由 provider 提供，以及在 provider 不支持时会发生什么。

### 1.2 核心理念

下面这些原则贯穿全部实现；后续章节会不断回指它们。

**(P1) 强类型 facade 之下是最小、对象安全、类型擦除的 SPI。**
公开 API（`EventBus`、`AsyncEventBus`、`Topic<T>`、`Delivery<T>`）保留泛型和
类型安全；provider 面向的 SPI（`EventBusSpi`、`AsyncEventBusSpi`）只处理
`OutboundMessage`/`InboundMessage` 和 `Arc<dyn Any>`/字节。泛型在 facade 边界
被擦除，因此 provider 可以放进 `Arc<dyn EventBusSpi>` 由 registry 管理，
而应用永远不会看到 `dyn Any`。

**(P2) 语义归 facade，传输归 provider。**
重试、错误处理器、死信、拦截器/中间件、ACK 模式、按 key 保序、in-flight
背压、生命周期跟踪、诊断，全部在 facade 实现一次；provider 负责后端特定的
发布、接收、settlement 和关闭。各后端在其声明能力允许的范围内共享 facade
策略；后端特有的保证仍由 provider 负责。

**(P3) 能力要诚实声明，不做最小公分母，也不做静默降级。**
provider 通过 `EventBusCapabilities` 如实报告自己能做什么。facade 在**订阅/发布
时**校验请求是否超出能力（如要求延迟投递但 provider 不支持），超出即报
`CapabilityError`，不会假装支持。同时 facade 也不会因为某个后端做不到就把
所有后端的 API 削成最小集合。
每个 SPI 实例的能力声明在其生命周期内保持不变，facade 只在构造时读取一次；
此处发生 panic 会转换为终态 SPI 错误，不会越过构造 API。

**(P4) 回退只发生在创建期，运行期永不切换 provider。**
`EventBusRegistry` 支持 `ProviderSelection` 的候选/回退链，但只在 `create*`
时解析一次。一旦 facade 建好，就绑定唯一 provider；运行期失败只会变成
错误、重试、死信或诊断事件，不会隐式换后端。

**(P5) 同步与异步是平行的一等公民，异步不绑定任何 runtime。**
`EventBus` 是纯 `std` 线程实现；`AsyncEventBus` 只依赖
`Pin<Box<dyn Future + Send>>`、`Waker` 和注入的 `qubit_clock::Timer`，
不 spawn 任务、不依赖 tokio/async-std。调用方在自己的 runtime 上驱动
`AsyncSubscription::run`。两条路径共享 model、pipeline、registry 与错误模型。

**(P6) 发布回执、应用确认、传输 settlement 是三件不同的事。**
`PublishReceipt` 只表示 provider 已接受消息（其语义由 `PublishGuarantee`
决定），不代表任何订阅者处理完成；`Acknowledgement` 是 handler 层面的
业务决定（`AckMode::Auto/Manual`）；`SettlementToken` + `DeliveryDisposition`
是 facade 与 provider 之间的传输层确认。三者解耦，避免"发布返回了 = 处理完了"
这类误解。

**(P7) 一切都有界。**
同步 handler 池、handler 队列、异步 admission、local 队列容量、provider 级
outstanding 预算、通知发布器队列，全部有显式上限；超限时的行为（阻塞、
拒绝、`Retry` 回退）都有明确定义，不会无界堆积。

**(P8) 失败必须可观测且被隔离。**
用户代码（handler、filter、interceptor、error handler、retry rule、诊断观察者）
和 provider SPI 调用全部经 `catch_unwind` 隔离；一个 panic 不会带崩 worker
线程或整个 bus，而是转成 `DeliveryError`/`PublishError`/`Diagnostic`。
所有"无法返回给调用方"的失败（settlement 失败、接收 gap、投递终态失败）
都通过 `observe_diagnostics` 发出。

**(P9) 复用 qubit 生态，不重复造轮子。**
重试用 `qubit-retry`（`RetryPolicy`/`RetryRule`/`Retry`/`AsyncRetry`），
时间用 `qubit-clock`（`Timer`），provider 目录与发现用 `qubit-spi`
（`ServiceSpec`/`ProviderRegistry`/inventory），事件 ID 用 `qubit-id`。
本 crate **不重导出**这些类型，调用方按需依赖对应 crate，避免版本耦合。

**(P10) 生命周期显式且幂等。**
`shutdown(ShutdownMode)` 是唯一停机入口，重复调用安全；停机后 API
返回 `Closed`；`Immediate` 可以加强正在进行的 `Graceful`；drop 订阅句柄
不等于取消订阅（同步侧），或者等于立即处置（异步侧），但两者都有明确定义。

### 1.3 非目标

- 不提供 exactly-once；以 provider 声明的 `PublishGuarantee` 与 settlement
  能力为上限。
- 不实现分布式事务、事件溯源存储、schema registry。
- 不内置任何外部 broker 的 provider；本 crate 只带进程内 `local`。
- 不做运行期 provider 热切换或多 provider 桥接。
- 不承诺跨订阅、跨 topic 的全局顺序。
- 不重导出 `qubit-retry`/`qubit-clock`/`qubit-spi` 的公共类型。

---

## 2. 总体架构

### 2.1 分层图

```
┌──────────────────────────────────────────────────────────────────────┐
│  应用代码                                                             │
│   Topic<T> / PublishRequest<T> / SubscribeRequest<T> / handler       │
└───────────────┬──────────────────────────────────┬───────────────────┘
                │                                  │
      ┌─────────▼──────────┐             ┌─────────▼──────────────┐
      │ EventBus (sync)    │             │ AsyncEventBus (async)  │
      │ · OperationGate    │             │ · AsyncAdmission       │
      │ · 每订阅协调线程    │             │ · AsyncOrderingLanes   │
      │ · SyncDelivery-    │             │ · AsyncSubscription    │
      │   Scheduler 共享池 │             │   (session/lease)      │
      │ · ShutdownCoord.   │             │ · Timer 注入           │
      └─────────┬──────────┘             └─────────┬──────────────┘
                │        共享 facade 公共层          │
      ┌─────────▼──────────────────────────────────▼──────────────┐
      │ pipeline: PublisherPipeline / SubscriberPipeline          │
      │          admission / ordering_lane / dead_letter / retry  │
      │          diagnostic                                       │
      │ model:   Topic / EventEnvelope / Delivery / Acknowledgement│
      │          PublishReceipt / SubscriberId / EventId / ...    │
      │ codec:   EventCodec / CodecRegistry                       │
      │ error:   EventBusError 及各层错误                          │
      └─────────┬──────────────────────────────────┬──────────────┘
                │ Arc<dyn EventBusSpi>              │ Arc<dyn AsyncEventBusSpi>
      ┌─────────▼──────────────────────────────────▼──────────────┐
      │ spi: EventBusSpi / EventSubscriptionSpi (+Async 版本)      │
      │      OutboundMessage / InboundMessage / TransportPayload  │
      │      SettlementToken / DeliveryDisposition / Capabilities │
      │      conformance (feature)                                │
      └─────────┬──────────────────────────────────┬──────────────┘
                │                                  │
      ┌─────────▼──────────┐             ┌─────────▼──────────────┐
      │ registry           │             │ local provider         │
      │ EventBusSpec       │◀────────────│ LocalEventBusSpi       │
      │ EventBusRegistry   │  注册/发现   │ AsyncLocalEventBusSpi  │
      │ AsyncEventBusReg.  │             │ LocalQueue / Budget    │
      │ RequiredCapabilities│            └────────────────────────┘
      └────────────────────┘
      ┌────────────────────┐
      │ notification       │  独立于 bus 的单类型通知出口
      │ NotificationPub.   │
      └────────────────────┘
```

### 2.2 模块布局（与 `src/` 一致）

| 模块 | 职责 | 关键类型 |
| --- | --- | --- |
| `model` | 与传输无关的领域类型 | `Topic<T>`、`EventEnvelope<T>`、`Headers`、`PublishRequest<T>`、`PublishOptions<T>`、`SubscribeRequest<T>`、`SubscribeOptions<T>`、`Delivery<T>`、`DeliveryContext`、`Acknowledgement`、`PublishReceipt`、`AdmissionOutcome`、`SubscriberId`、`EventId`、`DeadLetterEvent<T>`、`PublishMetadata`、`PublishFailureContext<T>` |
| `codec` | 编码能力接口与注册表 | `EventCodec<T>`、`CodecRegistry`、`ContentType`、`SchemaId`、`EncodedPayload`、`resolve_codec` |
| `spi` | provider 契约 | `EventBusSpi`、`EventSubscriptionSpi`、`AsyncEventBusSpi`、`AsyncEventSubscriptionSpi`、`SpiFuture`、`OutboundMessage`、`InboundMessage`、`TransportPayload`、`ReceiveOutcome`、`SettlementToken`、`DeliveryDisposition`、`SpiSubscriptionRequest`、`EventBusCapabilities` 及各能力枚举、`ShutdownOutcome`、`DeliveryGap`、`conformance` |
| `registry` | provider 目录与装配 | `EventBusSpec`、`EventBusProvider`/`AsyncEventBusProvider`（`qubit-spi` 定义 trait 的别名）、`EventBusRegistry`、`AsyncEventBusRegistry`、`EventBusConfig`、`RequiredCapabilities`、`EventBusProviderError`、内部 `EventBusProviderAdapter`/`IdentifiedEventBusSpi`、`sync_provider_inventory`/`async_provider_inventory`（discovery） |
| `pipeline` | 两条 facade 共享的处理逻辑 | `PublisherPipeline`、`SubscriberPipeline`、`DeliveryFailureAction`、`AdmissionTracker`、`OrderingLanes`、`DeadLetter*`、`retry` 适配、`Diagnostic` |
| `facade` | 用户可见的 bus 实现 | `EventBus`、`Subscription`、`AsyncEventBus`、`AsyncSubscription`、`EventBusFacadeConfig`、`SyncDeliverySchedulerConfig`、`DeliveryAdmissionConfig`、`PublishMetricsSnapshot`、`WaitOutcome`、内部 `SyncDeliveryScheduler`/`ShutdownCoordinator`/`LifecycleTracker` |
| `local` | 内置进程内 provider | `LocalEventBusConfig`、`LocalEventBusProvider`、`AsyncLocalEventBusProvider`、`LocalEventBusSpi`、`AsyncLocalEventBusSpi`、`LocalQueue`、`OutstandingBudget` |
| `notification` | 在 `EventBus` 前面加一层永不阻塞的有界发布队列 | `NotificationPublisher<T>`、`NotificationOutcome`、`TryPublishError<T>`、`NotificationStatsSnapshot` |
| `error` | 分层错误类型 | `EventBusError`、`PublishError`、`SubscribeError`、`DeliveryError`、`LifecycleError`、`ShutdownError`、`ProviderError`、`SpiError`、`CapabilityError`、`CodecError`、`ConfigurationError` |

依赖方向严格为 `facade → pipeline → {model, codec, spi, error}`、
`registry → spi`、`local → spi`。`pipeline` 与 `spi` 不知道 facade 的存在，
`local` 不知道 registry 之外的任何上层。

### 2.3 crate 元数据、feature 与外部依赖

- 包名 `qubit-event-bus`，版本 `0.15.0`，edition 2024，`rust-version = 1.94`。
- features：
  - `discovery = ["qubit-spi/inventory"]`：启用 `inventory` 驱动的 provider
    自动登记（见 §6.4）。
  - `conformance`：暴露 `qubit_event_bus::spi::conformance` 模块，供 provider
    作者在自己的测试里跑一致性用例（见 §15.3）。
- 运行期真正使用的 qubit 依赖：`qubit-spi`（目录/发现）、`qubit-retry`
  （`worker` + `async` feature）、`qubit-clock`（`Timer`/`TimeError`）、
  `qubit-id`（`Uuid` 生成 `EventId`）。错误类型依赖 `thiserror`。
- dev 依赖 `loom`（并发模型）；有界通道传输使用标准库实现
  与 `loom`（并发模型检查）。

---

## 3. 领域模型

领域模型的设计目标是：**在不知道任何 provider 的情况下就能完整描述"一次发布"
和"一次投递"**，并且所有构造器都在构造期做完校验，运行期不再产生配置类错误。

### 3.1 标识体系

| 类型 | 含义 | 约束 / 生成方式 |
| --- | --- | --- |
| `Topic<T>` | 带类型的主题 | 名称为 `Cow<'static, str>`；可携带可选 `Arc<dyn EventCodec<T>>`；相等性与哈希基于 **名称 + `TypeId::of::<T>()`**，因此同名不同类型是不同 topic；`Topic::new_static` 为 `const fn`，支持全局常量 |
| `SubscriberId` | 应用给订阅者的稳定名字 | 1..=128 字节，首字符字母或数字，其余允许 `.`、`_`、`-`、`:`；用于死信记录、consumer group 语义与 provider 侧可读标识 |
| `subscription::Id` | facade 内部分配的订阅唯一 ID | 单调递增整数，同一 bus 内不重复；provider 用它索引订阅，与 `SubscriberId` 无关 |
| `EventId` | 每个 `EventEnvelope` 的唯一 ID | 默认由 `qubit-id` 生成 UUID v4；也可 `EventId::new` 传入外部 ID（1..=128 字节，无首尾空白与控制字符） |
| `ProviderId` | provider 标识 | 来自 `qubit-spi`；内置 `local` 另有别名 `memory`、`in-process` |

`SubscriberId` 与 `subscription::Id` 分离是刻意的：前者是**业务身份**
（可能被多个订阅复用，也用于死信归因），后者是**运行期实例身份**。

### 3.2 `EventEnvelope<T>`

```rust
pub struct EventEnvelope<T> {
    id: EventId,
    topic: Topic<T>,
    payload: Arc<T>,
    headers: Headers,            // BTreeMap<String, String>
    timestamp: SystemTime,
    ordering_key: Option<Box<str>>,
    delay: Option<Duration>,
}
```

设计要点：

- **payload 用 `Arc<T>`**：发布一次、多订阅者共享同一份对象，local provider
  直接传 `Arc<dyn Any>`，零拷贝；`Delivery::payload_arc()` 让 handler 也能
  继续共享。
- **`Headers` 是 `BTreeMap`**：确定性迭代顺序，方便测试与日志。
- **`ordering_key` 与 `delay` 放在 envelope 上**而不是 options 上：它们是消息
  自身的属性，需要随消息进入 provider（`OutboundMessage` 会原样携带）。
- 保留头 `x-qubit-event-bus-dead-letter: v1`：由死信管线写入，普通调用方通过
  `PublishRequestBuilder::header` 无法设置（`ConfigurationError`），
  `PublishMetadata::set_header` 同样拒绝。facade 用它识别"这条消息已经是死信"，
  从而避免死信递归（见 §7.6）。

### 3.3 请求对象与 builder

`PublishRequest<T>` = `EventEnvelope<T>` + `PublishOptions<T>`；
`SubscribeRequest<T>` = `Topic<T>` + `SubscriberId` + `SubscribeOptions<T>`。

为什么用 request 对象而不是一堆参数：

- 一次性把 envelope、重试策略、拦截器、错误处理器等全部收进不可变对象，
  在 builder `build()` 里校验完，`publish`/`subscribe` 内部不再有"参数组合非法"的分支。
- `PublishOptions<T>`/`SubscribeOptions<T>` 的字段是 `pub(crate)`，
  外部只能经 builder 设置，从而保证保留头、非法 delay 等在构造期被拒。
- request 对象可 `Clone`（拦截器等用 `Arc` 共享），便于在测试和重发场景复用。

`PublishOptions<T>` 内含：`retry_policy`、`retry_rule`（`Arc<dyn RetryRule<PublishAttemptError>>`）、
`retry_cancellation_token`、`interceptors: Vec<Arc<PublisherInterceptor<T>>>`、
`error_handlers: Vec<Arc<PublishErrorHandler<T>>>`。

`SubscribeOptions<T>` 内含：`ack_mode: AckMode`、`filter`、`interceptors`
（同步 `SubscriberInterceptor<T>`）、`async_interceptors`（`AsyncSubscriberInterceptor<T>`）、
`retry_policy`/`retry_rule`/`retry_cancellation_token`、`error_handlers`
（`SubscribeErrorHandler<T>` 返回 `FailureDirective`）、`dead_letter: Option<DeadLetterPolicy>`、
`ordering: OrderingPolicy`、`durability: SubscriptionDurability`、
`start_position: StartPosition`、`consumer_group: Option<ConsumerGroup>`、
`provider_options: ProviderOptions`（`BTreeMap<String, String>`）。

同一份 `SubscribeRequest<T>` 既能给 `EventBus` 也能给 `AsyncEventBus`，
区别只在于：同步 facade 拒绝带 `async_interceptors` 的请求（`SubscribeError::Configuration`），
异步 facade 会拒绝同步 `interceptors`；其 handler 链只接受异步 `async_interceptors`。

### 3.4 `Delivery<T>`、`DeliveryContext` 与 `Acknowledgement`

handler 收到的是 `Delivery<T>`：

- `event()` / `payload()` / `payload_arc()`：只读访问 envelope。
- `context()` → `DeliveryContext`：`provider_id`、`subscription_id`、`subscriber_id`、
  `retry_attempt`（facade 本地重试的第几次尝试，从 1 起）、`provider_attempt`
  （为 provider 侧重投计数预留的可选字段；`InboundMessage` 目前没有对应来源，
  两个 facade 都不会填充它）、`provider_metadata`（`BTreeMap<String, String>`，
  分区/偏移等非敏感元数据，原样来自 `InboundMessage`）、`can_settle`
  （本条消息是否带 settlement token）、`is_dead_letter()`（是否带死信保留头）。
  facade 重试与 provider 重投在模型上**分开计数**，避免把两种语义混进一个数字。
- `acknowledgement()` → 共享的 `Acknowledgement`，其上有 `ack()` / `nack()`。

`Acknowledgement` 是一个原子的"**首个决定生效**"单元：`ack()`/`nack()` 只有
第一次调用会写入，之后调用返回 `false` 且不改变状态。它被 `Delivery` 和
facade 共同持有，handler 返回后 facade 读取它来决定 settlement（见 §7.4 ACK 矩阵）。
即使 handler 把 `Delivery` 移入别的线程再 ack，也只会有一种终态。

### 3.5 `PublishReceipt` 与接纳可见性

```rust
pub struct PublishReceipt {
    input_event_id: EventId,            // 调用方传入的 envelope ID
    dispatched_event_id: Option<EventId>, // 拦截器可能替换 envelope，这是真正发出的 ID；被丢弃时为 None
    provider_id: ProviderId,
    acknowledgement: PublishAcknowledgement,
}

pub enum PublishAcknowledgement {
    Accepted { provider_message_id: Option<String>, metadata: ProviderMessageMetadata }, // 不透明接受
    DestinationAdmissions(Vec<DestinationAdmission>),   // 逐目的地：Accepted / Filtered / Rejected
    DroppedByInterceptor,
}
```

- provider SPI 的 `publish` 只返回 `PublishAcknowledgement`；facade 在其上补充事件 ID
  与 provider ID 生成 `PublishReceipt`。
- `admission_outcome()` 把 `PublishAcknowledgement` 归一为 `AdmissionOutcome`：
  `OpaqueAccepted`（provider 不报告目的地）、`Accepted`/`PartiallyAccepted`/
  `NoneAccepted(AdmissionSummary)`、`NoDestinations`（provider 报告了空目的地列表）、
  `Dropped`（被拦截器丢弃）。`Filtered` 目的地（订阅 filter 拒绝）与 `Rejected`
  目的地（容量/准入拒绝）在 `AdmissionSummary` 中分别计数。
- `check_admission(AdmissionRequirement)` 让调用方声明"我要求至少一个/全部目的地接纳"，
  返回 `Result<(), AdmissionCheckError>`（`VisibilityUnavailable`、`Dropped`、
  `NoAcceptedDestination`、`RejectedDestinations { .. }`）。

这体现 P6：`publish` 返回 `Ok(receipt)` 只表示 provider 调用产生了回执，不一定表示
目的地已接纳。`admission_outcome()` 区分已知接纳、部分接纳、无目的地、丢弃和 opaque
确认；只有 `PublishVisibility::DestinationAdmissions` 才能提供目的地明细。
`publish_all` 返回 `BatchPublishResult`，按输入顺序保留每个请求的
`Result<PublishReceipt, PublishError>`，某一条失败不影响后续请求继续发布。

### 3.6 `DeadLetterEvent<T>`

死信 topic 的 payload 类型固定为 `DeadLetterEvent<T>`，包含
`original_event: Arc<EventEnvelope<T>>`、`subscriber_id`、`reason`
（最终 `DeliveryError` 的 `Display`）。死信 topic 由 `DeadLetterPolicy::with_topic_name(name)` 指定并默认要求 transport acceptance；
`with_known_destination(topic_name)` 要求可观察到目的地接纳，opaque provider 会拒绝该策略。facade 按
`Topic::<DeadLetterEvent<T>>::new(name)` 构造；encoded provider 必须为该类型单独注册 codec。
同一原始 event 和 subscriber 会得到稳定的死信 event ID，便于去重，但不保证 exactly-once。

---

## 4. Provider SPI

### 4.1 设计原则

1. **对象安全**：全部 SPI trait 都能放进 `Arc<dyn ...>`/`Box<dyn ...>`，
   registry 才能统一持有不同 provider。
2. **类型擦除**：SPI 只见 `TransportPayload`，泛型 `T` 只出现在
   `SpiSubscriptionRequest::payload_type_id()`（`TypeId`）里，供 provider 做类型冲突检查。
3. **最小化**：provider 只需实现 `capabilities`、`publish`、`subscribe`、
   `shutdown`，以及订阅句柄的 `receive`、`settle`、`close`。`wait_for_topic_idle`
   有默认实现（返回 `Ok(None)` 表示不支持）。
4. **单所有者订阅**：`EventSubscriptionSpi`/`AsyncEventSubscriptionSpi` 的方法都是
   `&mut self`，provider 不需要为并发接收做锁；并发由 facade 负责。
5. **幂等与取消安全**：`settle` 对同一 `(token, disposition)` 幂等；异步 `receive`
   被 drop 后不得丢消息；`shutdown` 可重复调用。

### 4.2 同步契约

```rust
pub trait EventBusSpi: Send + Sync + 'static {
    fn capabilities(&self) -> EventBusCapabilities;
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError>;
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError>;
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError>;
    fn wait_for_topic_idle(&self, topic: &TopicAddress, timeout: Option<Duration>) -> Result<Option<bool>, SpiError> {
        Ok(None)   // 默认：不支持；Some(true) = 已空闲，Some(false) = 超时
    }
    #[doc(hidden)] fn provider_id(&self) -> Option<ProviderId> { None }
}

pub trait EventSubscriptionSpi: Send + 'static {
    fn id(&self) -> subscription::Id;
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError>;
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError>;
    fn close(&mut self) -> Result<(), SpiError>;
}
```

`provider_id()` 是 `#[doc(hidden)]` 的内部钩子：registry 用
`IdentifiedEventBusSpi` 代理包住真实 provider 并覆写它，这样 facade 可以在
`DeliveryContext`、`Diagnostic`、`SpiError` 里带上 provider 标识，而不要求
每个 provider 自己实现。

### 4.3 异步契约

```rust
pub type SpiFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait AsyncEventBusSpi: Send + Sync + 'static {
    fn capabilities(&self) -> EventBusCapabilities;
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>>;
    fn subscribe<'a>(&'a self, request: SpiSubscriptionRequest) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>>;
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>>;
    #[doc(hidden)] fn provider_id(&self) -> Option<ProviderId> { None }
}

pub trait AsyncEventSubscriptionSpi: Send + 'static {
    fn id(&self) -> subscription::Id;
    fn receive(&mut self, timeout: Duration) -> SpiFuture<'_, Result<ReceiveOutcome, SpiError>>;
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> SpiFuture<'_, Result<(), SpiError>>;
    fn close(&mut self) -> SpiFuture<'_, Result<(), SpiError>>;
}
```

选择 `Pin<Box<dyn Future + Send>>` 而不是 `async fn in trait` 是为了对象安全
并明确 `Send` 约束（P5）。异步 facade 只会对 `receive` 传 `Duration::MAX`
并依靠 `Waker` 被唤醒；provider 若需要定时（如延迟投递），应在自己内部使用
注入的 `Timer`（`AsyncLocalEventBusSpi` 就是这么做的）。

**取消安全要求**：`receive` 返回的 future 在被 drop 时不得让消息丢失
（应留在队列或 in-flight 中），因为 `AsyncSubscription::run` 在停机或暂停时会
drop 正在等待的 `receive`。`settle` 与 `close` 的 future 应尽量短小且在被 drop
时留下可重试的状态——facade 对 settlement 有重试退避（§9.6）。

### 4.4 传输消息

- `OutboundMessage`：`topic: TopicAddress`、`id: EventId`、`timestamp`、`headers`、
  `ordering_key: Option<OrderingKey>`、`delay`、`payload: TransportPayload`。
  由 facade 从 `EventEnvelope<T>` 构造；`TopicAddress`/`OrderingKey` 是在 SPI 边界
  上经过校验的 newtype，provider 不必再检查空字符串等非法值。
- `InboundMessage`：`topic`、`id`、`timestamp`、`headers`、`ordering_key`、`payload`、
  `settlement: Option<SettlementToken>`、`provider_metadata: ProviderMessageMetadata`。
  没有 `delay`（延迟由 provider 消化）。`into_parts()` 一次性拆出
  payload/元数据/token，避免部分移动。
- `TransportPayload`
  - `Native(Arc<dyn Any + Send + Sync>)`：进程内传递原对象，facade 用 `downcast`。
  - `Encoded(EncodedPayload { content_type, schema_id, bytes: Arc<[u8]> })`：跨进程传输。
  - facade 依据 `PayloadModes` 选择：`Native`/`NativeAndEncoded` → `Native`；
    `Encoded` → 必须有 codec，否则 `CapabilityError::CodecRequired`。
- `ReceiveOutcome`
  - `Message(InboundMessage)`；
  - `Gap(DeliveryGap)`：provider 检测到丢失/跳过（facade 转成 `Diagnostic::ReceiveGap`）；
  - `TimedOut`；
  - `Closed`：订阅已在 provider 侧终止（facade 结束 worker/run）。
- `SpiSubscriptionRequest`：`subscription_id`、`topic: TopicAddress`、`subscriber_id`、
  `group: Option<ConsumerGroup>`、`durability`、`start_position`、`provider_options`、
  `payload_type_id: TypeId`。它是 `SubscribeRequest<T>` 去掉 facade 层选项
  （filter/中间件/重试/死信/ack_mode/ordering）后的投影；`OrderingPolicy` 不下传，
  因为按 key 串行由 facade 保证，provider 只需保证自己声明的 `OrderingCapability`。

### 4.5 `SettlementToken` 与 settlement 契约

```rust
pub struct SettlementToken {
    subscription_id: subscription::Id,
    inner: Box<dyn Any + Send>,      // provider 私有状态
}
```

- **不可 `Clone`**：一条消息只可能被 settle 一次（facade 侧保证），provider 不需要
  应付并发 settle 同一 token。
- `belongs_to(subscription_id)`：facade 在 settle 前校验 token 归属；provider
  也可用 `SpiError::InvalidSettlementToken` 拒绝错配 token。
- **幂等**：同一 `(token, disposition)` 重复 settle 必须返回 `Ok(())`（异步 facade
  失败后会重试同一 disposition）。
- **冲突**：同一 token 用不同 disposition 二次 settle，provider 应报错或忽略，
  但不能造成重复投递以外的状态破坏。
- `DeliveryDisposition::{Accept, Retry, Reject}`：`Retry` 请求 provider 重投
  （需要 `SettlementCapabilities::AcceptRetryReject`），`Reject` 表示终态丢弃
  （死信已由 facade 处理）。

### 4.6 停机契约

`shutdown(ShutdownMode)`：

- `ShutdownMode::Graceful { timeout }`：为 facade 停机调用方设置 deadline。调用方超时
  返回 `ShutdownError::TimedOut`；`ShutdownOutcome::TimedOut` 专指 provider 在自身宽限期
  结束后完成清理；
- `ShutdownMode::Immediate`：立刻关闭所有订阅，丢弃或按 provider 语义保留未处理消息；
- 幂等；停机后 `publish`/`subscribe` 返回 `kind` 为 closed 的 `SpiError::Operation`。

facade 同一时刻最多只有一个 provider `shutdown` 调用在途；失败或取消后后续调用可重试。
facade API 返回 `ShutdownReport`，SPI 方法仍返回 `ShutdownOutcome`。

---

## 5. 能力模型

```rust
pub struct EventBusCapabilities {
    payload_modes: PayloadModes,                 // Native | Encoded | NativeAndEncoded
    settlement: SettlementCapabilities,          // None | AcceptOnly | AcceptRetryReject
    ordering: OrderingCapability,                // None | PerSubscription | PerKey | PerPartition
    delayed_delivery: DelayedDeliveryCapability, // None | Native
    durability: DurabilityCapability,            // Ephemeral | Durable
    subscription_modes: SubscriptionModes,      // 接受 Ephemeral、Durable 或两种请求
    consumer_groups: bool,
    replay: ReplayCapability,                    // None | Position | Timestamp
    publish_guarantee: PublishGuarantee,         // FireAndForget | Accepted | Confirmed | DurablyStored
    publish_visibility: PublishVisibility,       // Opaque | DestinationAdmissions
}
```

facade 如何使用这些能力（对应 P3）：

| 请求内容 | 校验位置 | 缺失能力时的行为 |
| --- | --- | --- |
| `delay` | 发布管线 | `PublishError::Capability`，不发布 |
| `ordering_key` | 发布管线 | `OrderingCapability::None` 时 `PublishError::Capability` |
| `AckMode::Manual` | 订阅 | settlement 不是 `AcceptRetryReject` 时 `SubscribeError::Capability`（手动 nack 需要 provider 能 `Retry`/`Reject`） |
| `OrderingPolicy::PerKey` | 订阅 | `ordering.supports_per_key()`（`PerKey` 或 `PerSubscription`）为假时 `SubscribeError::Capability` |
| `SubscriptionDurability::Durable` | 订阅 | `DurabilityCapability::Ephemeral` 时拒绝 |
| 任意订阅持久模式 | 订阅 | provider 不接受该请求模式时以 `subscription_durability` 拒绝 |
| `consumer_group` | 订阅 | `consumer_groups == false` 时拒绝 |
| `StartPosition::Earliest` | 订阅 | `ReplayCapability::None` 时拒绝 |
| `PayloadModes::Encoded` | 发布 / 订阅 | 无 codec 时 `CapabilityError::CodecRequired`（编解码本身失败才是 `CodecError`） |
| `FailureDirective::Requeue` | 投递失败 | settlement 不支持 `Retry` 时降级为 `Discard`/`Reject`（见 §7.5） |
| `Diagnostic::SettlementUnavailable` | 投递失败 | 无法 settle 时发诊断而非静默 |

`RequiredCapabilities`（§6.2）复用同一套枚举，让调用方在**创建期**就要求
"至少这些能力”，把能力不匹配尽早暴露。
`DurabilityCapability` 描述 provider 的保留保证；`SubscriptionModes` 独立声明它接受哪些订阅请求模式。
从旧构造器迁移时，需要在 `DurabilityCapability` 后增加必填的 `SubscriptionModes` 参数：按
`subscribe` 实际接受的请求模式选择 `EPHEMERAL`、`DURABLE` 或 `BOTH`。这项声明不会改变保留保证。
facade 会在调用 `subscribe` 前拒绝不支持的模式。

---

## 6. Registry、发现与 provider 装配

### 6.1 `EventBusSpec`：接入 `qubit-spi`

```rust
pub struct EventBusSpec;                    // 同步与异步 provider 共用
impl ServiceSpec for EventBusSpec {
    type Config = EventBusConfig;
    type Output = Arc<dyn EventBusSpi>;
    type Error = EventBusProviderError;
}
// EventBusSpec 的 AsyncServiceSpec 选择 Arc<dyn AsyncEventBusSpi>。
```

`EventBusProvider` / `AsyncEventBusProvider` 是 `qubit-spi` 定义 trait
（`dyn ProviderDefinition<EventBusSpec>` / `dyn AsyncProviderDefinition<EventBusSpec>`）
的别名：provider 作者实现 `qubit-spi` 的 `ServiceProvider`/`ProviderMetadata`
（ID、别名、`create`），无需本 crate 额外的 trait。

`EventBusRegistry`/`AsyncEventBusRegistry` 包装 `qubit_spi::ProviderRegistry<Spec>`，
提供：

- `new()` / `with_local()`（后者预注册内置 `local`）/ `discover()`（见 §6.4）；
- `register(provider)` / `register_shared(Arc<EventBusProvider>)`；
- `descriptors()`、`provider_ids()`、`default_selection()`/`set_default_selection()`；
- `seal()` / `is_sealed()`：冻结目录，之后 `register` 报错；
- `create(&EventBusConfig)`：使用 config 内的 `selection`，否则用目录默认选择；
- `create_selected(&ProviderSelection, &EventBusConfig)`：显式选择。

两者都直接返回构造好的 `EventBus`/`AsyncEventBus`（facade 配置也来自
`EventBusConfig`）。`ProviderSelection` 来自 `qubit-spi`，支持带回退链的选择；
创建期按顺序尝试候选，全部失败则聚合成 `ProviderError::Resolution`/`Creation`（P4）。

### 6.2 `EventBusConfig` 与 `RequiredCapabilities`

`EventBusConfig` 是**一次创建**的全部输入，builder 风格：
`with_selection(ProviderSelection)`、`with_facade_config(EventBusFacadeConfig)`、
`with_required_capabilities(RequiredCapabilities)`、`with_provider_options(ProviderOptions)`。
它把"选哪个 provider""facade 怎么配""provider 私有参数"放在同一个对象里，
调用方可以整体从配置文件映射。

`RequiredCapabilities::missing_from(&caps)` 返回缺失项列表；
`EventBusProviderAdapter` 在 provider 构造完后立即检查，缺失则该候选失败并进入下一个候选。
**这是唯一一处 registry 会因能力做回退的地方**；facade 建好后再也不会因能力切换 provider。

`ProviderOptions`（`BTreeMap<String, String>`）是命名空间化、可 `Debug` 的非敏感配置。
订阅 builder 在 `build()` 时拒绝不合规的键值：键必须包含 `.` 且不能以 `.` 开头或结尾，
键和值都不能含控制字符，否则返回 `SubscribeRequestBuildError::InvalidProviderOption`。
约定用前缀区分归属，例如 `local.queue_capacity`。核心 crate 不解释别人的命名空间，
也不提供通用认证模型；密码、token 和私钥不得写入这些字段，provider 应通过外部凭据引用
或自己的安全配置取得凭据。provider 只解释自己的命名空间，其下的未知键必须报错：
内置 `local` 在 `LocalEventBusConfig::from_provider_options` 中对未知键返回
`ConfigurationError::InvalidField`。

### 6.3 `EventBusProviderAdapter` 与 `IdentifiedEventBusSpi`

- `EventBusProviderAdapter` 把 `Arc<EventBusProvider>` 适配成 `qubit-spi` 目录需要的
  形态：暴露 `descriptor()`，在 `create_configured` 中调用 provider 的 `create`，
  然后做 `RequiredCapabilities` 检查。
- 创建成功的 SPI 会被 `IdentifiedEventBusSpi { provider_id, inner }` 包裹，
  代理全部方法并把 `provider_id()` 覆写为真实 ID；registry 随后用它构造
  `EventBus::with_config(provider_id, spi, facade_config)`。
- facade 的构造函数（`from_spi`/`with_config`/`with_timer`）都**要求**显式传入
  `ProviderId`，因此 `DeliveryContext::provider_id`、`PublishReceipt::provider_id`
  与诊断中的 provider 字段总是有值；`provider_id()` 钩子只是让 registry
  不必在返回值中额外携带 ID。

### 6.4 discovery：`inventory` 驱动的自动登记

启用 `discovery` feature 后：

- 本 crate 在 `registry::provider_inventory` 中用 `qubit_spi::declare_sync_provider_inventory!` /
  `declare_async_provider_inventory!` 声明两个目录模块
  `qubit_event_bus::registry::sync_provider_inventory` 与 `async_provider_inventory`；
- provider crate 用 `qubit_spi::submit_sync_provider! { inventory_entry = qubit_event_bus::registry::sync_provider_inventory::Entry; spec = ...; provider = ...; }`
  （异步为 `submit_async_provider!`）把自己登记进去；
- 消费方调用 `EventBusRegistry::discover()` / `AsyncEventBusRegistry::discover()`
  收集全部已链接的 provider；两个 provider 使用相同选择名称时 `discover()` 报错。

内置 `local` 的处理是**非对称的**：`local_event_bus_provider.rs` 在
`#[cfg(feature = "discovery")]` 下用 `qubit_spi::submit_sync_provider!` 把同步
`local` 提交到同步目录；`async_local_event_bus_provider.rs` **没有**对应的提交。因此
`AsyncEventBusRegistry::discover()` 不会包含 `local`，需要显式
`AsyncEventBusRegistry::with_local()` 或 `register(AsyncLocalEventBusProvider)`。
（`AsyncEventBus::local(config)` 内部就是 `with_local()` + `create`。）

`tests/discovery_*` 与 fixtures `discovery_consumer`/`discovery_provider`
覆盖跨 crate 链接场景（含 workspace 多 crate 情况）。

---

## 7. Facade 公共层：配置、发布管线与消费管线

同步与异步 facade 共用 `pipeline` 模块。本章描述的行为对两者完全一致，
除非特别标注。

### 7.1 `EventBusFacadeConfig`

facade 构建期配置，`EventBus::with_config` / `AsyncEventBus::with_config`
或 `Registry::create` 传入，创建后冻结：

| 项 | 类型 | 作用 |
| --- | --- | --- |
| `codecs` | `CodecRegistry` | 按 `TypeId` 注册 `Arc<dyn EventCodec<T>>`；`resolve_codec` 优先取 `Topic` 自带 codec，其次查注册表 |
| `publisher_interceptors` | `Vec<Arc<dyn Fn(&mut PublishMetadata) -> Result<bool, PublishError>>>` | **全局**发布拦截器，只能读写 headers（`PublishMetadata`），返回 `false` 丢弃消息 |
| `subscriber_interceptors` | `HashMap<TypeId, Vec<Arc<SubscriberInterceptor<T>>>>` | 按 payload 类型注册的**全局同步**中间件 |
| `async_subscriber_interceptors` | 同上，异步版本 | 全局异步中间件（仅 `AsyncEventBus` 使用） |
| `sync_scheduler` | `SyncDeliverySchedulerConfig { max_in_flight: 4, handler_queue_capacity: 32, max_subscription_workers: 256 }` | 同步接手上限、handler 队列与接收线程预算 |
| `delivery_admission` | `DeliveryAdmissionConfig { max_in_flight: 4 }` | 异步 facade 全局 in-flight 上限 |
| `max_encoded_payload_bytes` | `Option<NonZeroUsize>` | 可选的编码输出上限，在调用 provider 前检查；`None` 表示不限制 |

facade 创建时只读取一次 provider capabilities，并在后续校验中使用不可变快照。
直接构造时若 `capabilities()` panic，构造函数返回 `SpiError`，分类为终态
`provider_panicked`。

全局拦截器/中间件与请求级拦截器/中间件**叠加**而非替代：发布侧先跑请求级
（typed），后跑全局（metadata）；消费侧全局中间件包在请求级中间件**外层**
（先进入全局，再进入 typed，最后 handler）。

### 7.2 codec 解析

`resolve_codec::<T>(topic, registry)`：

1. `topic.codec()` 存在 → 用它；
2. 否则 `registry.get::<T>()`；
3. 否则 `None`。

只有 `PayloadModes::Encoded` 的 provider 才**必须**有 codec（缺失 →
`CapabilityError::CodecRequired`）；`Native` 与 `NativeAndEncoded` 都走
`TransportPayload::Native`。消费侧对 `Encoded` 载荷用同一解析规则解码；
解码失败 → `DeliveryError::Codec`，直接 `Reject`（若 provider 支持）并发
`Diagnostic::DeliveryFailed { attempts: 0 }`。
设置 `max_encoded_payload_bytes` 后，publisher 在完成编码后、重试和调用 provider
前检查字节向量长度。它会拒绝超限编码输出，但不会限制 codec 执行时的内存分配。原生
payload 无 facade 字节上限，因为无法可靠递归计算 Rust 对象保留的内存。

### 7.3 发布管线（`PublisherPipeline`）

`publish(request)` 的步骤（`publish_all` 对每个请求顺序执行，逐条收集结果）：

1. **生命周期门禁**：bus 非 `Running` → `PublishError::Closed`。
2. **请求级 typed 拦截器**：`Fn(EventEnvelope<T>) -> Result<Option<EventEnvelope<T>>, PublishError>`，
   按注册顺序链式执行；可以整体替换 envelope（因此回执区分 `input_event_id` 与
   `dispatched_event_id`）；返回 `None` 表示丢弃 → 回执 `AdmissionOutcome::Dropped`；
   panic 被隔离为 `PublishError::InterceptorPanicked`。
3. **全局 metadata 拦截器**：只能改 headers；任一返回 `false` → 丢弃。
4. **死信头保护**：无论拦截器如何改动，如果原始 envelope 带死信头，重新写回，
   保证死信身份不丢。
5. **能力校验**：`delay.is_some()` 且 `DelayedDeliveryCapability::None`；
   `ordering_key.is_some()` 且 `OrderingCapability::None` → `PublishError::Capability`。
6. **载荷准备**：依据 `PayloadModes` 选 `Native`/`Encoded`；`Encoded` 时用 codec 编码，
   得到 `OutboundMessage`。
7. **重试发布**：若配置 `retry_policy`，用 `qubit_retry::Retry`（同步）/
   `AsyncRetry`（异步，带 `Timer`）包裹 `spi.publish`。重试判定综合
   `SpiError::retryable()` 与用户 `RetryRule<PublishAttemptError>`；
   `RetryFallback::Abort`；耗尽 → `PublishError::Retry(Box<RetryError<...>>)`。
   未配置策略则单次尝试。provider panic 被捕获为不可重试的 `SpiError::Operation`
   （`kind = "provider_panicked"`）。
8. **错误处理器**：发布失败时按注册顺序调用 `PublishErrorHandler<T>(&PublishFailureContext<T>, &PublishError)`；
   任何一个 panic → 最终错误替换为 `PublishError::ErrorHandlerPanicked`，
   其余处理器仍继续执行。
9. **接纳诊断**：回执携带 `DestinationAdmissions` 时，对每个被拒绝的目的地发
   `Diagnostic::AdmissionRejected { event_id, topic, subscriber_id, reason }`。
10. **指标**：更新 `PublishMetricsSnapshot`（`attempts`、`errors`、`dropped`、
    `opaque_accepted`、`zero_destinations`、`accepted_destinations`、
    `filtered_destinations`、`rejected_destinations`），通过 `publish_metrics()` 读取。

内部用 `PipelineFailure { origin, error }` 携带失败**发生在哪一步**（拦截器、
能力、编码、SPI、错误处理器…），便于测试断言与日志，但对外只暴露 `PublishError`。

### 7.4 消费管线（`SubscriberPipeline`）：单条消息的处理

以下逻辑封装在 `SubscriberPipeline<T>` 中，由同步 worker 与异步 run 循环调用：

```
InboundMessage
  │ into_parts()
  ├─ TransportPayload → Arc<T>（Native downcast / Encoded decode）── 失败 → Reject + DeliveryFailed(attempts=0)
  ├─ 重建 EventEnvelope<T>
  ├─ filter(&envelope)? ── false → settle Accept，结束
  │                     ── panic → 视为 handler 失败进入错误处理
  ├─ ordering lane（PerKey 时按 ordering_key 串行，见 §8.2 / §9.3）
  ├─ 全局中间件 → typed 中间件 → handler     （中间件签名 Fn(Delivery<T>, next) ）
  ├─ ACK 矩阵 → 成功 / DeliveryError
  ├─ 失败：本地重试（qubit-retry）→ 错误处理器 → FailureDirective
  ├─ 终态：DeliveryFailureAction → 死信 / settle Retry|Reject|Discard
  └─ settle SettlementToken（若有）
```

**中间件模型**：`next` 是 `Box<dyn FnOnce(Delivery<T>) -> ...>`，中间件必须
且只能调用一次；不调用即短路（返回自己的结果），调用两次不可能（类型层面）。
异步版本 `AsyncSubscriberNext<T>` 返回 `SpiFuture<'static, ...>`，由中间件持有
并 await。

**ACK 矩阵**（handler 返回值 × `Acknowledgement` 状态 → 结果）：

| `AckMode` | handler 结果 | `Acknowledgement` | 结论 |
| --- | --- | --- | --- |
| `Auto` | `Ok` | 任意 | 成功，`Accept` |
| `Auto` | `Err(e)` | 任意 | 失败 `e` |
| `Manual` | `Ok` | `ack()` 已调用 | 成功，`Accept` |
| `Manual` | `Ok` | 未调用 | 失败：`DeliveryError::Handler`（"manual acknowledgement remained pending"） |
| `Manual` | `Ok` | `nack()` 已调用 | 失败：`DeliveryError::Handler`（"delivery was negatively acknowledged"） |
| `Manual` | `Err(e)` | 任意 | 失败 `e` |

handler 或中间件 panic 同样被包装成 `DeliveryError::Handler`（附带 panic 信息），
进入同样的失败流程。`DeliveryError` 刻意只有四个变体（`Handler`、`Codec`、`Spi`、
`Retry`），让错误处理器与 `RetryRule` 只需区分"应用失败 / 解码失败 / 后端失败 / 重试耗尽"。

### 7.5 失败处理：本地重试、错误处理器、`FailureDirective`

```
attempt ──失败──▶ 错误处理器链 (SubscribeErrorHandler<T>) ──▶ FailureDirective
                                                                 │
   ┌─────────────────────────────────────────────────────────────┤
   │ Retry      → 交给 qubit-retry 决定是否再试（有策略时）；无策略等同 Discard
   │ Requeue    → 结束本地尝试，settle Retry（需 AcceptRetryReject，否则不 settle）
   │ DeadLetter → 结束本地尝试，发布死信后 settle Reject；转发失败会停止订阅并保留未结算的源 token
   │ Discard    → 结束本地尝试，settle Reject（需 AcceptRetryReject，否则不 settle）
   └─────────────────────────────────────────────────────────────
```

细节：

- **错误处理器聚合**：所有处理器都会被调用（便于记录日志/指标），最终 directive
  的规则是：**第一个非 `Retry` 的返回值生效**；全部返回 `Retry` 才继续重试；
  处理器 panic 记为 `Diagnostic::InternalFailure` 并等价于返回 `Discard`。
  没有配置任何处理器时：有 `retry_policy` → `Retry`，否则 `Discard`。
  这条规则让"停止重试"的决定总是保守地胜出，任何一个处理器都能叫停。
- **本地重试**：配置了 `retry_policy` 才会在 facade 内重试；每次失败后先跑错误处理器，
  只有 directive 为 `Retry` 才交给 `qubit-retry` 的规则链继续判定
  （用户 `RetryRule` → `DeliveryAttemptError::retryable` → 策略默认）。
  同步侧 retry rule 本身 panic 时结果改为 `Requeue`（把决定权交还 provider）；
  未配置策略时 `Retry` 等价于 `Discard`。重试间隔同步用 `qubit_retry::Retry`
  （阻塞 sleep，在 handler 池线程上），异步用 `AsyncRetry` + `Timer`。
- **`DeliveryFailureAction`** 是 directive 与能力结合后的终态：
  - `DeadLetter`：有 `DeadLetterPolicy` 且消息**不是**死信 → 通过内部 publish 发布
    `DeadLetterEvent<T>`（**不经过**全局发布拦截器，避免死信被再次改写/丢弃），
    成功后 `Reject`；发布失败且 provider 支持 `Retry` → `Retry`，否则 `Reject`；
    未配置死信策略 → 退化为 `Requeue`。若消息本身已经是死信（保留头存在）→
    不再发布，直接 `Reject`。
  - `Requeue`：settlement 为 `AcceptRetryReject` → `Retry`。
  - `Discard`/本地重试耗尽：`AcceptRetryReject` → `Reject`。
  - settlement 能力为 `None` 或 `AcceptOnly` 时，**任何失败都不 settle**
    （`SubscriberPipeline::failure_disposition` 返回 `None`）；若消息带 token，
    发 `Diagnostic::SettlementUnavailable { requested }` 说明 facade 本想做什么。
    `AcceptOnly` 语义下未 settle 的消息是否重投由 provider 决定。
  - 死信构造/发布过程中的内部错误（策略缺失、envelope 构造失败、发布失败）都以
    `Diagnostic::InternalFailure` 记录，然后按 `Requeue` 处理。
- 每次终态失败都会发 `Diagnostic::DeliveryFailed { event_id, topic, subscription_id, subscriber_id, attempts, error }`。

### 7.6 死信递归防护

死信消息带 `x-qubit-event-bus-dead-letter: v1` 头，`SubscriberPipeline` 遇到带头消息
的终态失败时不再发布第二级死信，只 `Reject`。加上发布管线的"死信头保护"，
可保证一条消息最多产生一条死信记录。

### 7.7 与 `qubit-retry` 的关系

- 本 crate 只依赖 `qubit-retry` 的 `RetryPolicy`、`RetryRule`、`RetryCancellationToken`、
  `Retry`/`AsyncRetry`、`RetryError`、`RetryFallback`，**不重导出**（P9）。
- `PublishAttemptError`/`DeliveryAttemptError` 是内部包装，把 `SpiError::retryable()`
  与 `DeliveryError` 的分类传给 `RetryRule`。
- 取消：`retry_cancellation_token` 可提前终止重试；同步停机 `Immediate` 时 facade
  也会触发取消。

---

## 8. 同步 Facade：`EventBus`

### 8.1 结构

```
EventBus (Arc<Inner>)
 ├─ spi: Arc<dyn EventBusSpi>
 ├─ config: EventBusFacadeConfig
 ├─ lifecycle: LifecycleTracker            Running → Closing → Closed
 ├─ operations: OperationGate              统计正在进行的 publish/subscribe，停机时等待归零
 ├─ scheduler: SyncDeliveryScheduler       共享 handler 线程池（懒启动）
 ├─ subscriptions: Mutex<HashMap<Id, SubscriptionControl>>
 ├─ shutdown: ShutdownCoordinator
 ├─ diagnostics: DiagnosticObservers
 └─ metrics: PublishMetrics
```

线程模型（按名称可在调试器中识别）：

| 线程 | 数量 | 职责 |
| --- | --- | --- |
| `event-bus-subscription-{id}` | 每个订阅一个 | 协调线程：循环 `receive(50 ms)`、解码/过滤、把 handler 任务交给调度器、**执行 settlement** |
| `event-bus-handler-{i}` | `max_in_flight` 个（默认 4），首次 `subscribe` 时懒启动 | 执行中间件 + handler + 本地重试 + 错误处理器 + 死信发布 |
| `event-bus-shutdown` | 停机时最多一个 | 后台执行 drain/join/provider shutdown |

设计动机：`EventSubscriptionSpi` 是 `&mut self` 单所有者，所以每个订阅必须有
**唯一**线程去 `receive` 与 `settle`；而 handler 可能很慢，如果每个订阅一个线程
串行跑 handler，既没有并发也无法全局限流。于是把两者分离：协调线程只做
I/O 与 settlement，handler 统一在有界共享池里跑，`max_in_flight` 即全局 in-flight 上限。

### 8.2 `SyncDeliveryScheduler`

- 每个订阅一个 `VecDeque` 任务队列（`handler_queue_capacity`，默认 32）；
  worker 以 round-robin 遍历订阅队列，避免一个高吞吐订阅饿死其他订阅。
- `try_reserve()` 是准入：`queued < capacity` 才允许协调线程入队；否则协调线程把
  消息留在 `pending`，**不再 `receive` 新消息**，形成对 provider 的自然背压
  （local provider 的队列随之填满，最终反映到发布侧 `AdmissionOutcome`）。
  `handler_queue_capacity = 0` 时退化为直接交接：只有空闲 worker 多于已预留任务且
  key 不活跃时才接受。
- **按 key 保序**：`OrderingPolicy::PerKey` 的任务带 `ordering_key`；调度器维护
  `active_keys: HashSet<(subscription_id, key)>`。`take_ready()` 在遍历某订阅队列时
  跳过 key 正活跃的任务，取第一个可执行的任务；因为队列本身 FIFO 且每次取"最前可执行"，
  同一 key 的任务天然按到达顺序执行，不同 key 可并行。
- `AdmissionTracker` 记录已发出的 permit，`wait_for_received_deliveries` 依赖它判断
  facade 内是否还有未完成投递。
- `cancel_subscription(id)`：取出该订阅所有排队任务，以 `cancelled = true` 运行
  （直接生成 `Retry` settlement 交还 provider），保证取消时不丢消息。
- `stop_admission(immediate)`：`Graceful` 保留已排队任务让其自然 drain；`Immediate`
  清空队列并以 `cancelled` 方式让它们 `Retry`。

### 8.3 协调线程（`run_subscription_worker`）

```
loop {
  if cancel_requested { 把 pending 以 Retry 归还；break }
  apply 队列里由 handler 线程回传的 settlement（OwnerSettlementRouter）
  if let Some(job) = pending.take() {
     if let Some(r) = scheduler.try_reserve(...) { r.submit(job) } else { pending = Some(job); sleep 1ms; continue }
  }
  match receiver.receive(50ms) {
     Message(m)  → process_inbound_parts → 生成 job → 尝试 reserve/submit，失败则放 pending
     Gap(g)      → Diagnostic::ReceiveGap
     TimedOut    → continue
     Closed      → break
     Err(e)      → Diagnostic::InternalFailure，短暂 sleep 后继续
  }
}
drain 剩余 settlement → receiver.close() → 标记 stopped
```

**settlement 回流**：handler 线程完成后不能直接调用 `receiver.settle`（它不持有
`&mut` 接收器），而是把 `(token, disposition)` 通过 mpsc 发回协调线程；协调线程在
每轮循环开头统一 `settle`。settlement 失败会发 `Diagnostic::SettlementFailed`，
并在下一轮**再试一次**（依赖 §4.5 的幂等契约）。

### 8.4 `subscribe` 流程

1. `OperationGate::enter()`，非 `Running` → `SubscribeError::Closed`。
2. 拒绝 `async_interceptors`（请求级或 facade 级中该 `T` 的异步中间件）。
3. 能力校验（§5）。
4. 解析 codec（`Encoded` 模式下必须有）。
5. `spi.subscribe(SpiSubscriptionRequest)`（panic 隔离）。
6. 构造 `SubscriberPipeline<T>`、`SubscriptionControl`、`Arc<AtomicBool>` 取消标志。
7. 懒启动调度器 worker；spawn 协调线程。
8. 返回 `Subscription` 句柄。

`Subscription`：`id()`、`subscriber_id()`、`is_cancelled()`、`cancel()`。**drop 不取消**——订阅随
bus 生命周期存活直到 `cancel()` 或 `shutdown()`。`cancel()` 先让调度器归还排队任务，
再设置取消标志并 `join` 协调线程；如果 `cancel()` 是在 bus 上下文
（handler/中间件/协调线程）内调用，会跳过 join 以避免自我死锁。

### 8.5 等待原语

两者签名相同：`(&Topic<T>, Option<Duration>) -> Result<WaitOutcome, LifecycleError>`，
`WaitOutcome::{Idle, TimedOut}`，`None` 表示无限等待。

- `wait_for_idle`：转发 `spi.wait_for_topic_idle`；provider 返回 `None`
  → `LifecycleError::IdleWaitUnsupported`。这是 **provider 视角**的"该 topic 无 pending/in-flight"。
- `wait_for_received_deliveries`：等待 facade 中该 topic **已接收但尚未完成**的投递归零
  （`LifecycleTracker` 按 topic 名计数）。这是 **facade 视角**，与 provider 无关。
  两者互补：前者确认 provider 队列排空，后者确认 handler 都跑完。测试里常见的
  "publish 后立刻断言"应先调用其中之一。

### 8.6 死锁检测：`BusContextGuard`

协调线程与 handler 线程进入用户代码前会设置 thread-local `BusContextGuard`。
在这些上下文中调用会阻塞等待自己的操作（`shutdown` 同步等待、`cancel()` join、
`wait_for_*`）时，facade 返回 `LifecycleError::WouldDeadlock { operation }`
（停机场景包在 `ShutdownError::Lifecycle` 里）而不是真的死锁。停机在 bus 上下文里仍然可以**发起**（切换到后台线程执行），只是
不能同步等待完成。

### 8.7 停机：`ShutdownCoordinator`

```
shutdown(mode: ShutdownMode)
  ├─ lifecycle: Running → Closing（已 Closing/Closed 则进入"加入现有停机"路径）
  ├─ OperationGate::close_admission()          新 publish/subscribe 立即 Closed
  ├─ coordinator.begin(mode) → generation       Immediate 可加强正在进行的 Graceful
  ├─ scheduler.stop_admission(immediate)；所有订阅 request_cancel()
  ├─ spawn `event-bus-shutdown` 线程执行 perform_shutdown：
  │     等待 OperationGate 归零 → 等待/清空调度器任务 → join 全部协调线程
  │     → scheduler.join() → spi.shutdown(mode)（同一时刻最多一次）→ lifecycle Closed
  └─ 调用方：等待自己的 deadline，返回 facade 的 ShutdownReport；
              bus 上下文 → 立即返回 ShutdownError::Lifecycle(WouldDeadlock)（停机仍在后台进行）
```

- generation 机制让并发的多次 `shutdown()` 调用都能等到**同一次**停机的结果；
  `Immediate` 到来时会把正在 `Graceful` 的这一代升级，正在排队的任务被 `Retry` 归还。
- 调用方 deadline 到期返回 `ShutdownError::TimedOut`，不代表 bus 已关闭；同步
  coordinator 继续后台收尾。SPI 的 `ShutdownOutcome::TimedOut` 表示 provider 在自身
  Graceful 宽限期后完成清理。
- 报告包含 facade 已知放弃的 ephemeral delivery 数量，以及 provider 可能放弃
  未能精确计数工作的标志。
- 后台停机线程本身也在 `catch_unwind` 内运行；若它 panic，会以 `Diagnostic::InternalFailure`
  报告并把状态推进到 `Closed`，避免调用方永久等待。
- `EventBus` 是 `Clone` 的 `Arc` 句柄，**没有 `Drop` 停机逻辑**：协调线程持有
  `Arc<EventBusInner>`，仅丢弃句柄不会停止它们。调用方必须显式 `shutdown`。

---

## 9. 异步 Facade：`AsyncEventBus` 与 `AsyncSubscription`

### 9.1 运行时中立的实现手段

- **不 spawn**：facade 不知道有没有 executor。`subscribe` 只返回 `AsyncSubscription`，
  消费循环在调用方 `.await subscription.run()` 时才开始。
- **定时靠注入**：构造函数有 `from_spi(provider_id, spi)`、`with_config(provider_id, spi, config)`、
  `with_timer(provider_id, spi, timer)`、`with_config_and_timer(...)` 以及
  `AsyncEventBus::local(config).await`；所有 sleep（重试退避、settlement 退避、
  `wait_for_received_deliveries` 超时、停机 deadline）都通过 `Arc<dyn Timer>`。
  未提供时使用 `qubit_clock::StdTimer`。测试用 `tests/support/manual_async.rs` 的手动 timer
  可以确定性地推进时间。
- **唤醒靠 `Waker`**：`AsyncSignal`（一次性/可重置通知）、`AsyncAdmission`、
  `AsyncOrderingLanes` 都基于 `Waker` 注册与唤醒，不使用 channel 或线程。
- **SPI 调用全部 boxed `Send`** future，配合 `catch_spi_future` 捕获 provider panic。

### 9.2 `AsyncAdmission`：全局 in-flight 限流

- 容量来自 `DeliveryAdmissionConfig::max_in_flight`（默认 4）。
- `acquire()` 返回 future；没有空位时把 `Waker` 加入 **FIFO** 等待队列，释放 permit
  时唤醒队首，保证公平、无饥饿。
- permit 是 RAII 类型，drop 即释放。投递任务持有 permit 直到 handler 与 settlement
  意图产生完成。
- `AsyncTracker` 记录 in-flight 数，用于 `wait_for_received_deliveries` 与停机 drain。

### 9.3 `AsyncOrderingLanes`

按 `(subscription_id, ordering_key)` 维护 lane：同 key 的投递任务在进入 handler 前
`acquire` lane，前一个未释放时挂起并登记 `Waker`。释放时按 FIFO 唤醒下一个。
`OrderingPolicy::Unordered` 的订阅不使用 lane。

### 9.4 `AsyncSubscription`：session、lease 与 run 的可恢复性

```
AsyncSubscription<T>
 ├─ control: Arc<AsyncSubscriptionControl<T>>   停机信号 (SessionSignals)、runner 计数、SessionSlot
 └─ SessionSlot → Mutex<Option<AsyncSession<T>>>
        AsyncSession
         ├─ receiver: Option<Box<dyn AsyncEventSubscriptionSpi>>   单所有者接收器
         ├─ pending / waiting_admission: Option<PendingDelivery>    至多一条已收未准入
         ├─ tasks: Vec<OwnedDeliveryTask>                          正在执行的投递 future
         ├─ completed: VecDeque<PendingDelivery>                    待 settle 的完成项
         ├─ admission_waiter: Option<AsyncAdmissionFuture>
         └─ handler: Option<SharedAsyncHandler<T>>                  run() 传入，暂停后保留
```

`PendingDelivery` 是一条消息在 facade 内的完整生命周期记录：事件、token、
`settlement_intent`、`settlement_failures`、待发的失败诊断、持有的 admission permit
与 ordering lane guard。permit/guard 随 `PendingDelivery` 一起 drop 释放，
因此不可能"忘记释放"。

- `run()`：`lease()` 取出 `Session`（若已被另一个 `run` 持有则等待），执行 `run_loop`，
  结束后如果订阅未处置，`Session` 放回 slot。因此：
  - **drop `run()` 的 future = 暂停**：接收器留在 session 中，未 settle 的消息留在
    provider（依赖 receive 的取消安全），再次调用 `run()` 从原状态继续；
  - **停机可接管暂停中的 session**：`AsyncEventBus::shutdown` 对每个 control 调用
    `shutdown(mode)`，它 `lease()` 到 session 后先把 pending 投递跑完（`Graceful`）
    再 `close_inner()`。
- `run_loop` 每轮：检查停机信号 → 处理 settlement 意图 → 若有 pending（已接收但未获
  admission 的消息）先 `acquire` permit → 否则 `receive(Duration::MAX)`。
  **每个订阅至多一条未准入消息**，这既是背压（不从 provider 拉更多），也让暂停/停机
  时只需归还一条消息。
- 投递任务（`OwnedDeliveryTask`）是 `'static` 的 owned future，由 `run_loop` 在
  `tasks` 中逐个轮询（`select` 语义：任一任务完成、receive 返回或停止信号到来都会
  产生一个 `AsyncRunnerEvent`）；handler 完成后 `PendingDelivery` 带着
  `settlement_intent` 进入 `completed`，由 `run_loop` 在下一轮以 `&mut receiver`
  执行 settle——与同步侧"回流到 owner 线程"是同一思想。
- `close()`：停止 run（`Immediate`），`lease` 后 `receiver.close()`，从 bus 注销。
- `Drop`：等价于 `dispose()`——立刻停止、丢弃 session（receiver 被 drop，provider 按
  Ephemeral/Durable 语义处理）、注销 control。**与同步侧不同**：异步句柄 drop 即处置，
  因为没有后台线程能替它继续消费。

### 9.5 `AsyncEventBus::subscribe` 与未启动订阅

`subscribe` 在 SPI 侧建好接收器后立即返回；如果调用方从未 `run()` 就 `shutdown`，
`close_unstarted_subscriptions` 会对每个 control `lease()` 并 `close_inner()`，确保
provider 侧接收器被正确关闭、不留悬挂订阅。若 `subscribe` 完成时 bus 已不是 `Running`，
接收器会被立即关闭并返回 `SubscribeError::Closed`。

### 9.6 settlement 重试退避

settlement 失败发 `Diagnostic::SettlementFailed` 并在意图队列保留；重试间隔为
`10 ms × 2^n`，上限 1 s（指数最多 7），使用注入 `Timer`。重试依赖 §4.5 幂等契约；
停机 `Immediate` 时放弃剩余意图并发 `SettlementUnavailable`。

### 9.7 停机

异步 facade 没有 `wait_for_idle`（provider 视角），只有 `wait_for_received_deliveries`；
`AsyncSubscription` 句柄提供 `id()`、`subscriber_id()`、`run(handler).await`、`close().await`。

```
shutdown(mode: ShutdownMode).await
  ├─ CAS 选出 leader；非 leader 等待 leader 完成并返回同一结果
  ├─ lifecycle Closing；OperationGate 关闭准入
  ├─ 通知所有 control 停止（Graceful: 完成 in-flight；Immediate: 尽快返回）
  ├─ 对每个 control: shutdown(mode) —— lease session → drain（Graceful）→ close_inner
  ├─ 等待 runners_stopped（有 timeout 则以 Timer 计时，超时 TimedOut）
  ├─ catch_spi_future(spi.shutdown(mode))
  └─ lifecycle Closed
```

`Immediate` 同样可以加强正在进行的 `Graceful`（control 的停止级别只升不降）。

### 9.8 `BusContextFuture`：poll 级死锁检测

异步侧没有线程可绑定，因此用 **poll 作用域**：投递任务在 poll handler/中间件时进入
bus 上下文，poll 返回后退出。在此作用域内调用 `wait_for_received_deliveries` 或
`shutdown().await` 会立刻得到 `WouldDeadlock`，因为等待自己所在的投递完成必然死锁。

---

## 10. 内置 `local` provider

`local` 是唯一随 crate 提供的 provider，目标是**进程内、低延迟、按 topic 广播、
有界**，同时尽可能覆盖能力矩阵以便测试 facade：

| 能力 | 声明值 | 说明 |
| --- | --- | --- |
| `payload_modes` | `Native` | 只传 `Arc<dyn Any>`；收到 `Encoded` 载荷返回 `unsupported_payload_mode` 错误（facade 按能力选择载荷，正常不会触发） |
| `settlement` | `AcceptRetryReject` | 完整 settlement 语义，支持手动 ACK |
| `ordering` | `PerKey` | lane 结构天然保证 |
| `delayed_delivery` | `Native` | 延迟堆 |
| `durability` | `Ephemeral` | 关闭即丢弃 |
| `consumer_groups` | `false` | 每个订阅独立收到全部消息（广播） |
| `replay` | `None` | 不保留历史 |
| `publish_guarantee` | `Accepted` | 入队即返回 |
| `publish_visibility` | `DestinationAdmissions` | 逐订阅报告接纳结果 |

`Encoded` 载荷路径由 `tests/support/provider_shapes.rs` / `fake_spi.rs` 中声明
`Encoded`/`NativeAndEncoded` 能力的测试 provider 覆盖，以保证 codec 管线不依赖 local。

### 10.1 共享的队列结构 `LocalQueueState`

每个订阅一个 `LocalQueue`：

```
LocalQueueState
 ├─ lanes: HashMap<QueueKey, QueueLane { events: VecDeque<LocalEvent>, version, delayed_version }>
 │                                                   按 ordering_key 分 lane（无 key 也是一个 lane）
 ├─ ready_lanes: VecDeque<(QueueKey, version)>       round-robin 选择下一条可投递 lane
 ├─ delayed_lanes: BinaryHeap<Reverse<DelayedQueueHead>> 延迟消息到期最小堆，带版本以惰性失效
 ├─ delayed_live_count / delayed_stale_count         触发堆压缩的计数
 ├─ in_flight: HashMap<Box<str>, LocalInFlight>      key = "{event_id}:{seq}"，已投递未 settle
 ├─ pending_count / next_delivery_token
 └─ closed
```

`LocalEvent.payload` 是 `SharedPayload::Native(Arc<dyn Any>)`——同一个 `Arc`
被广播给全部订阅队列，发布不复制 payload。

- **lane 即顺序**：同 key 消息进入同一 lane 的队尾；一个 lane 只有队头可投递，
  且投递中的消息 settle 前该 lane 不再出队——`PerKey` 顺序由此保证，无需额外锁。
- **`Retry` 回队头**（`enqueue_front`），保持原顺序；`Accept`/`Reject` 释放 outstanding
  预算，`Retry` 不释放。
- **延迟堆惰性清理**：lane 被消费或重排时旧堆项失效；当失效项超过 `max(live, 8)`
  时重建堆，避免堆无界增长。
- 同步总线状态维护 **`change_version`**（每次路由/队列变化 +1），让
  `wait_for_topic_idle` 能在条件变量上等待"任何变化"再重新检查
  `pending == 0 && in_flight == 0`；异步版本用 `AsyncSignal` 达到同样效果。

### 10.2 容量与预算

| 参数 | 默认 | 作用 |
| --- | --- | --- |
| `queue_capacity` | 1024 | 每个订阅的 `pending + in_flight` 最大投递数 |
| `max_total_outstanding` | 65 536 | provider 级 `OutstandingBudget`：全部订阅的排队 + in-flight 总数 |

发布时逐目的地判断：队列满或预算耗尽 → 该目的地拒绝（`AdmissionSummary` 中计为
rejected，并附 reason）；topic 无订阅 → `NoDestinations`。发布**永不阻塞**，
背压通过回执反映（P7）。

### 10.3 `LocalEventBusSpi`（同步）

- 总线状态按 topic 分桶：每个桶记录 `payload_type_id: Option<TypeId>` 与
  `queues: BTreeMap<subscription::Id, Weak<LocalQueue>>`。以 `Weak` 持有，
  订阅句柄被 drop 后自动从路由中消失，publish 时顺带清理失效项。
- **类型绑定**：一个 topic 第一次被 publish/subscribe 时记录 `TypeId`；后续不同类型
  → `SpiError { kind: InvalidArgument, reason: "topic_type_conflict" }`。这弥补了
  类型擦除后 `Topic<T>` 相等性中 `TypeId` 那一半的检查。
- `receive(timeout)`：条件变量等待到有可投递消息、到期延迟消息、超时或关闭。
- `settle`：解析 token 中的 `"{event_id}:{seq}"`，找到 in-flight 项，按 disposition
  处理；未知 token → `InvalidSettlementToken`；同 token 重复 `Accept` 幂等返回 `Ok`。
- `close`：标记关闭、清空 pending 与 in-flight（Ephemeral 语义：未 settle 的消息丢弃），
  从路由移除。
- `wait_for_topic_idle`：遍历该 topic 全部活跃队列，等待 `pending == 0 && in_flight == 0`。
- `shutdown`：`Graceful` 等待所有队列空或超时；随后关闭全部队列。幂等；之后
  publish/subscribe → `Closed`。
- 重复 `subscription_id` → `duplicate_subscription`（facade 保证不会发生；防御性检查）。

### 10.4 `AsyncLocalEventBusSpi`（异步）

结构与同步版基本对称，差异在于阻塞原语与索引方式：

- 等待用 `Waker` 而非条件变量：`receive` future 在无消息时登记 waker，publish/settle
  后唤醒；延迟消息到期用注入的 `Timer` 建立 sleep future。
- 路由使用按 `subscription_id` 保存 mailbox 的主索引，以及按 topic 保存有序订阅 ID 集合的辅助索引。
  发布只读取目标 topic 的订阅，并按 `subscription_id` 升序分配 provider 总预算；
  关闭时在同一 bus-state 锁内同步维护两个索引，并校验 mailbox 实例身份，避免旧 handle
  删除复用 ID 后的新 mailbox。相同 `(topic, subscriber_id)` 的不同订阅实例各有独立
  mailbox，也会各自收到广播消息，与同步 local 语义一致。
- 容量与预算共享同一套 `LocalQueue`/`OutstandingBudget` 实现，`AsyncLocalShared`
  额外持有 `AsyncSignal changed` 与 `Arc<dyn Timer>`。
- `close_mailbox` 同为 Ephemeral 语义。
- `AsyncLocalEventBusSpi::with_timer(config, timer)` 允许测试注入手动 timer；构造时会同时
  校验 queue 与 provider 总容量，零值返回配置错误；
  `AsyncLocalEventBusProvider` 通过 registry 创建时使用 `qubit_clock::StdTimer`。
  该 provider 目前**未**提交到异步 inventory 目录（§6.4）。

### 10.5 与 facade 的分工示例

以一条 `PerKey` 延迟消息为例：facade 校验能力并把 `ordering_key`/`delay` 放进
`OutboundMessage`；local 把它放进对应 lane 并压入延迟堆；到期后 `receive` 返回；
同步 facade 的调度器再次以 `active_keys` 保证同 key 不并发（因为 local 已经保证同 key
串行出队，这一层在 local 上是冗余的，但对不保证串行出队、只保证分区顺序的 provider
是必要的）。

### 10.6 资源与基准

`benches/local_scale.rs`（不依赖基准框架的可重复热路径测量）与
`benches/local_threads.rs`（同步/异步订阅创建与销毁的线程/资源占用）提供
数量级参考。`benches/encoded_publish.rs` 检查同步编码载荷在发布重试间是否复用
分配，`benches/facade_delivery.rs` 测量调用方驱动的异步 facade 经 local SPI 发布。
数值随机器变化，请直接运行：

```bash
cargo bench --bench local_scale
cargo bench --bench local_threads
cargo bench --bench encoded_publish
cargo bench --bench facade_delivery
```

同步 facade 为每个订阅创建一个阻塞接收线程，并在调用 provider 前执行
`max_subscription_workers` 预算检查（默认 256）。handler 池大小由 `max_in_flight`
决定；停机时还可能临时启动协调线程。异步 facade 不会为每个订阅创建线程；local
provider 自身不创建线程。

---

## 11. `NotificationPublisher`：单类型通知的轻量出口

`notification` 模块解决一个具体场景：**热路径代码想把 `T` 类型的通知发到某个固定
topic，但绝不能被 `EventBus::publish` 的重试、拦截器或 provider 阻塞。**
它是在同步 `EventBus` 前面加的一层有界、永不阻塞的队列，而不是另一种总线。

```rust
pub struct NotificationPublisher<T: Send + Sync + 'static> { ... }
impl<T: Send + Sync + 'static> NotificationPublisher<T> {
    pub fn new<F>(bus: EventBus, topic: Topic<T>, capacity: NonZeroUsize, observer: F) -> io::Result<Self>
    where F: Fn(NotificationOutcome) + Send + Sync + 'static;
    pub const fn default_capacity() -> NonZeroUsize;                     // 256
    pub fn try_publish(&self, payload: T) -> Result<(), TryPublishError<T>>; // Full(T) | Closed(T)，原样归还 payload
    pub fn stats(&self) -> NotificationStatsSnapshot;
    pub fn close(&self) -> io::Result<()>;
    pub fn close_with_timeout(&self, timeout: Duration) -> io::Result<()>;
}

pub enum NotificationOutcome {
    Published(PublishReceipt),
    PublishFailed(PublishError),
    RequestFailed(EventIdGenerationError),
}
```

设计要点：

- **有界 `sync_channel(capacity)` + 单 worker 线程 `event-notification-publisher`**：
  worker 逐条构造 `PublishRequest::new(topic.clone(), payload)` 并调用 `bus.publish`，
  因此全部 facade 语义（拦截器、重试、回执）仍然生效，只是搬到了后台线程。
- **`try_publish` 永不阻塞**：队列满返回 `TryPublishError::Full(payload)`，
  已关闭返回 `Closed(payload)`，都把 payload 还给调用方决定如何处置（P7）。
- **结果通过 observer 回推**：每次发布的 `NotificationOutcome` 交给 observer；
  它在 worker 线程上运行，应尽快返回。observer panic 被隔离并计入
  `observer_panicked`，不影响后续发布（P8）。
- **统计**：`NotificationStatsSnapshot` 提供 `enqueued`、`queue_full`、`queue_closed`、
  `published`、`publish_errors`、`request_errors`、`observer_panicked`、`worker_panicked`。
- **`close()`**：关闭发送端、等待 worker 排空并 join；从 worker 线程（即 observer 内）
  调用会返回错误而不是自我 join 死锁（与 §8.6 同一思想）；worker 曾 panic 时返回错误。
  `close_with_timeout(timeout)` 在期限内等待相同的排空和退出流程；超时返回
  `io::ErrorKind::TimedOut`，已接纳消息仍由 worker 继续处理，新入队已关闭。之后可再次
  调用任一 close 方法等待结果。它不能强制中断正在执行的同步 provider 调用。
- **`Drop`** 只关闭发送端，不等待 worker；worker 会在排空剩余队列后自然退出。
- 它不拥有 `EventBus` 的生命周期：bus 停机后 worker 收到 `PublishError::Closed`
  并通过 observer 报告，调用方仍需自行 `close()`。

---

## 12. 错误模型

分层原则：**每个公开操作有自己的错误枚举，`EventBusError` 只是聚合**；
错误类型携带足够的上下文（provider、操作、资源、可重试性）但不带业务 payload。

| 类型 | 出处 | 主要变体 |
| --- | --- | --- |
| `PublishError` | `publish`/`publish_all` | `Configuration`、`Capability`、`Codec`、`Spi`、`Retry(Box<RetryError<PublishAttemptError>>)`、`InterceptorPanicked { .. }`、`ErrorHandlerPanicked { .. }`、`Closed` |
| `AdmissionCheckError` | `PublishReceipt::check_admission` | `VisibilityUnavailable`、`Dropped`、`NoAcceptedDestination`、`RejectedDestinations { .. }` |
| `SubscribeError` | `subscribe` | `Configuration`、`Capability`、`Spi`、`Closed`（codec 缺失归入 `Capability`） |
| `DeliveryError` | handler 返回 / 管线 | `Handler { source }`、`Codec`、`Spi`、`Retry(Box<RetryError<DeliveryAttemptError>>)` |
| `LifecycleError` | `wait_for_*`、`cancel`、停机内部 | `Timer(TimeError)`、`WouldDeadlock { operation }`、`Closed`、`IdleWaitUnsupported`、`Spi`、`SubscriptionClose(Arc<SubscriptionCloseErrors>)` |
| `ShutdownError` | `shutdown` | `TimedOut { .. }`、`CoordinatorStart(io::Error)`、`Lifecycle(LifecycleError)`（含 `WouldDeadlock`）、`Spi`、`SubscriptionClose` |
| `ProviderError` | registry | `Resolution`（找不到 provider / 选择非法）、`Creation`（provider 构造失败或 `RequiredCapabilities` 缺失） |
| `EventBusProviderError` | provider 作者 | provider `create` 返回的错误包装，供 `qubit-spi` 聚合 |
| `SpiError` | provider | `Operation { provider_id, operation, resource, kind, retryable, source }`、`InvalidSettlementToken { .. }` |
| `CapabilityError` / `CodecError` / `ConfigurationError` / `EventIdGenerationError` | 构造期或校验 | 见各自定义 |

- `SpiError::retryable()` 是 provider 向 facade 传达"值得重试"的唯一通道，
  发布/投递重试都参考它。
- `SpiError::Operation::kind` 是 `&'static str` 分类（如 `closed`、`invalid_argument`、
  `provider_panicked`、`worker_panicked`、`duplicate_subscription`、`topic_type_conflict`），
  用于日志与测试断言。facade 各层的 `Closed` 变体来自 facade 自身的生命周期门禁；
  provider 在停机后返回的关闭类 `SpiError` 按 `Spi` 变体原样传出，不做二次映射。
- 所有枚举标注 `#[non_exhaustive]`，为未来新增变体保留空间。
- 所有错误 `Send + Sync + 'static`，可跨线程/任务传递。
- 内部 `PipelineFailure { origin, error }` 不对外暴露，只用于把失败阶段传给诊断与测试。

---

## 13. 诊断与指标

`observe_diagnostics(observer) -> DiagnosticObserverHandle`：注册一个
`Fn(&Diagnostic) + Send + Sync` 观察者，句柄 drop 即注销。观察者 panic 被隔离，
不影响其他观察者与主流程。

| `Diagnostic` 变体 | 何时发出 |
| --- | --- |
| `AdmissionRejected` | 发布回执中某目的地被拒绝（仅 `DestinationAdmissions` provider） |
| `ReceiveGap` | provider `receive` 返回 `Gap` |
| `DeliveryFailed` | 一条投递到达终态失败（含 attempts、`DeliveryFailureAction`、错误） |
| `SettlementFailed` | `settle` 返回错误（会重试） |
| `SettlementUnavailable` | 需要 settle 但能力不允许或已放弃（如 `Immediate` 停机） |
| `InternalFailure` | facade 内部不应发生的错误（如协调线程 receive 返回 Err） |

`PublishMetricsSnapshot`（`publish_metrics()`）提供发布侧计数（字段见 §7.3 第 10 步），
用于测试与轻量监控；它不是完整指标系统。异步侧只有 future 被首次 poll 时才计入
`attempts`，构造后未 poll 即丢弃的 publish future 不计数。

设计取舍：诊断是**推**模型而不是日志——本 crate 不依赖 `log`/`tracing`，
把接入监控系统的选择留给调用方。

---

## 14. 并发不变量汇总

1. 任一 `EventSubscriptionSpi`/`AsyncEventSubscriptionSpi` 在任意时刻只有一个持有者
   在调用其方法（同步：协调线程；异步：持有 lease 的 `run`/`shutdown`）。
2. 一条消息的 `SettlementToken` 最多产生一次成功 settlement；重试只会重复相同 disposition。
3. `Acknowledgement` 首个决定生效，之后不可变。
4. 同一订阅、同一 `ordering_key` 的 handler 不会并发执行（同步：`active_keys`；
   异步：`AsyncOrderingLanes`）。
5. 同时运行的 handler 数 ≤ `max_in_flight`（同步池大小 / 异步 admission 容量）。
6. 每个订阅至多一条"已从 provider 取出但尚未进入 handler"的消息（同步 `pending`，
   异步 pending + 单 permit）。
7. `publish`/`subscribe` 在 `Closing` 之后必返回 `Closed`；`shutdown` 幂等且
   provider shutdown 同一时刻最多有一个调用在途；失败或取消后可由后续调用重试。
8. 用户回调 panic 不会导致线程/任务退出或 bus 状态损坏。
9. 死信最多一级；死信头无法被外部设置或篡改。
10. Durable provider 支持时，取消/停机将未开始任务以 `Retry` 归还；Ephemeral provider
    可以丢弃未开始任务，facade 会报告已知放弃数量及无法精确统计的 provider 放弃标志。
11. 持有 facade 内部锁时不调用用户代码。诊断观察者先在 `observers` 锁内做成快照，
    `emit` 在释放锁之后才调用回调；handler、中间件和错误处理器运行在调度线程或
    异步投递任务上，不持有订阅目录、ordering lane 或 tracker 的锁。这样用户回调
    再进入 bus API 时不会在同一把锁上自死锁，配合 §8.6 / §9.8 的 `WouldDeadlock` 检测。

`tests/concurrency_contract_tests.rs` 在 `loom` 下对第 4、5、7、10 条相关的原语建模检查。

---

## 15. 测试与验证策略

### 15.1 测试分层

| 位置 | 内容 |
| --- | --- |
| 各 `src/**/*.rs` 单元测试 | 模型校验、builder 拒绝规则、能力枚举、队列/堆行为、admission/lane 原语 |
| `tests/sync_facade_tests.rs`、`tests/async_facade_tests.rs` | 端到端：发布/订阅、ACK 矩阵、重试、死信、顺序、背压、停机、暂停恢复、死锁检测 |
| `tests/*_contract_tests.rs`（model / registry / spi / spi_error / public_error_trait） | 公开契约与不变量 |
| `tests/*_coverage_tests.rs`（sync / async / local / pipeline / publisher / registry / model / error / spi） | 分支覆盖补充 |
| `tests/local_provider_tests.rs`、`tests/async_local_provider_tests.rs` | 直接针对 SPI 层的 local 行为 |
| `tests/publish_admission_tests.rs`、`tests/request_builder_tests.rs`、`tests/notification_publisher_tests.rs` | 接纳回执、builder 校验、通知发布器 |
| `tests/support/fake_spi.rs` | 可编程假 provider：注入错误、panic、能力矩阵、settlement 失败 |
| `tests/support/flume_spi.rs` | 基于标准库有界通道的第二个真实传输实现，验证 SPI 不为 local 定制 |
| `tests/support/manual_async.rs` | 手动 executor / 手动 timer，让异步测试确定性推进 |
| `tests/support/provider_shapes.rs` | 各能力组合的 provider 形态 |
| `tests/support/{scheduler_race,spawn_failure,panic_hook}.rs` | 调度器竞态、线程创建失败、panic 钩子隔离 |
| `tests/discovery_{sync,async,conflict}_tests.rs` + `tests/fixtures/discovery_{provider,consumer}` | `discovery` feature 跨 crate 链接与冲突检测 |
| `tests/spi_conformance_tests.rs` | 用 `conformance` 模块跑 local 与 flume |
| `tests/concurrency_contract_tests.rs` | `loom` 模型检查：admission permit 精确释放一次、lane 取消后唤醒下一位、cancel 与 receive 竞争、graceful shutdown 与 publish 的单一准入线性化点 |
| `fuzz/fuzz_targets/{provider_options,transport_envelope}.rs` | 解析/构造边界 fuzz |
| `benches/` | 见 §10.6 |

### 15.2 CI

`.github/workflows/ci.yml` 使用仓库共享的 `rs-infra` 编排（`ci-check.sh`）：
依赖基线检查、rustfmt/clippy（含 coverage cfg）、feature 与依赖矩阵、
`cargo +1.94.0 test --doc`、README 依赖版本校验，以及
`RUSTDOCFLAGS="-D warnings -D missing-docs" cargo doc --all-features` 的严格文档构建。

### 15.3 `conformance` feature

`qubit_event_bus::spi::conformance::{run_sync, run_async}` 接受一个 provider 工厂
（`Fn() -> Arc<dyn EventBusSpi>` / 异步返回 future；每个用例新建实例避免相互污染）
与 `ConformanceHooks`（可选的 settlement、receive/settlement/close/shutdown cancellation 和 durable recovery 钩子，让 provider
作者补充只有自己能验证的幂等/取消检查），返回 `ConformanceReport`
（`Vec<ConformanceCase::{Passed, Failed, Skipped}>`）。
当前用例覆盖：能力/载荷模式一致性、`subscribe`、`publish`、`receive-payload`、
settlement 幂等/冲突、`shutdown`。Structural profile 用于 smoke check；Strict profile 会把缺失必需 hook
转为失败，同时保留 typed unsupported-capability skip。异步 hook 以 future 形式运行，不阻塞 executor。
Strict profile 配置 provider 专属 fixture 后才可作为验收门。

公共 runner 不覆盖、需要 provider 作者自己补充的检查：

- descriptor、provider 选择与创建；
- capability 声明稳定且与真实行为一致；
- native / encoded 载荷契约；
- `publish` 之后能 `receive`；
- `receive` 超时；
- `close` 之后 `receive` 返回 `Closed`；
- gap 映射为 `ReceiveOutcome::Gap`；
- settlement token 的 subscription 归属；
- 重复 settlement 幂等、冲突 disposition 不造成额外投递；
- `shutdown` 幂等；
- 异步 `receive` 被取消后消息不丢；
- 错误携带 provider 上下文，且 `source` 可追溯；
- `Debug` 输出不泄漏敏感的 provider options。

### 15.4 文档验证

公共 trait 和主要类型带可运行的 rustdoc 示例。README 与用户手册在介绍 retry
高级配置时列出 `qubit-retry` 依赖并使用 `qubit_retry::*` 路径；基础示例不引入
用不到的 retry import。文档把 provider 接纳、facade admission 和 handler 完成
分成三件事（P6）。

---

## 16. 公开 API 稳定性

以下类型构成需要谨慎演进的稳定边界：

- `EventBus` / `AsyncEventBus`；
- `Topic<T>` / `EventEnvelope<T>` / 两类 request 及其 builder / `Delivery<T>`；
- `EventBusSpi` / `AsyncEventBusSpi` 与两类 subscription SPI；
- 传输消息和 settlement 契约；
- capability 类型；
- provider spec 与 registry；
- 各操作的错误类型及其访问器。

公开错误 enum、capability enum 和 `Diagnostic` 使用 `#[non_exhaustive]`。
SPI 输入结构使用私有字段、构造函数和访问器，避免新增字段成为破坏性修改。
后端特有扩展走命名空间化的 `ProviderOptions` 或独立扩展 trait，不向最小 SPI 持续加方法。

---

## 17. 非目标与已知边界

- **没有 exactly-once**：facade 只能在 provider 声明的 settlement 能力内工作；
  `AcceptOnly` provider 下失败消息可能被重投也可能丢失，取决于 provider。
- **顺序范围**：只保证同一订阅、同一 key 的 handler 串行；跨订阅、跨 topic 无序。
- **同步重试会占用 handler 线程**：`Retry` 的退避 sleep 在池线程上进行，
  长退避会降低有效并发；需要长退避时应使用 `Requeue` 让 provider 重投，或改用异步 facade。
- **异步 local 不参与 discovery**（§6.4）。
- **`wait_for_idle` 依赖 provider**：不支持时返回 `IdleWaitUnsupported`，请用
  `wait_for_received_deliveries` 或业务层信号替代。
- **`provider_attempt` 目前恒为 `None`**：`InboundMessage` 尚无 provider 重投计数来源。

---

*本文档随 `qubit-event-bus` 0.15.x 维护；修改 facade/SPI 行为时应同时更新本文档与 [英文版](design.md) 的对应章节。*
