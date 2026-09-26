# Qubit Event Bus SPI 正式架构设计

## 文档状态

本文档记录 `qubit-event-bus` 0.12 的正式 SPI、provider registry、同步/异步 facade 和内置 local provider 设计。它最初是重构目标文档，现已按落地实现更新；当前实际行为以代码、rustdoc、测试和 [`design.zh_CN.md`](design.zh_CN.md) 为准。明确标为后续扩展的后端或能力尚未在本 crate 中提供。

本次保留完整中文版。英文 [`design.md`](design.md) 仅作为权威状态入口，避免将旧架构描述误读为当前实现。

## 目标

`qubit-event-bus` 提供统一、类型安全、与具体消息中间件无关的事件总线接口，并通过 `qubit-spi` 发现、选择和创建不同后端。crate 同时提供一个无需外部服务的本地实现。

目标后端包括但不限于：

- 内置进程内实现；
- `tokio::sync::broadcast`；
- `crossbeam-channel`；
- `flume`；
- `bus`；
- RabbitMQ / `lapin`；
- Kafka / `rdkafka`；
- Redis Pub/Sub；
- Redis Streams。

设计必须满足以下原则：

1. 普通用户只面对类型安全的 `EventBus` 或 `AsyncEventBus` facade。
2. 后端作者只实现最小、对象安全、类型擦除的 `EventBusSpi` 或 `AsyncEventBusSpi`。
3. provider 只负责创建 SPI；统一的事件处理语义由 facade 实现，不能由 provider 绕过。
4. 同步与异步接口并列存在；异步接口不绑定 Tokio、async-std、smol 或其他运行时。
5. 后端能力差异必须显式呈现，不能用虚假的最小公分母掩盖可靠性、持久性或确认语义。
6. 创建阶段可以使用 `qubit-spi` fallback；运行期间绝不因一次操作失败而静默切换后端。
7. 保留当前实现中已经验证的类型安全主题、拦截器、重试、死信、回执、背压、顺序和观测设计。

## 非目标

本次重构不试图：

- 在所有后端上提供完全相同的持久性或投递保证；
- 将 Kafka 事务、RabbitMQ exchange、Redis Stream trimming 等特性强行抽象成通用 API；
- 提供分布式事务或“恰好一次”承诺；
- 自动推断 payload 的序列化格式或 schema；
- 在运行时错误后自动迁移到另一个 provider；
- 让同步 API 在内部阻塞异步运行时；
- 让异步 API 隐式选择或启动某个 executor；
- 在本 crate 内实现所有第三方后端。

## 总体架构

```text
应用代码
   │
   ├── EventBus                 同步、类型安全 facade
   └── AsyncEventBus            异步、类型安全 facade
          │
          ├── Topic<T> / EventEnvelope<T> / Delivery<T>
          ├── codec 与类型擦除
          ├── interceptor / filter / handler
          ├── retry / error handler / dead-letter
          ├── admission / ordering / backpressure
          └── diagnostic / lifecycle
                    │
                    ▼
       EventBusSpi / AsyncEventBusSpi
          ├── publish
          ├── subscribe
          ├── receive
          ├── settle
          └── shutdown
                    │
                    ▼
     local / tokio / crossbeam / flume / bus /
     RabbitMQ / Kafka / Redis Pub/Sub / Redis Streams
```

`qubit-spi` 位于创建边界：

```text
ProviderRegistry<EventBusSpec>
  → ProviderSelection
  → ProviderDefinition<EventBusSpec>
  → Arc<dyn EventBusSpi>
  → EventBus facade
```

## crate 和模块边界

`qubit-event-bus` 同时包含公共领域模型、SPI 契约、registry facade 和内置 local provider。推荐的逻辑模块如下：

```text
src/
├── facade/          EventBus、AsyncEventBus、builder 和执行管线
├── model/           Topic、EventEnvelope、Delivery、receipt 和 options
├── pipeline/        retry、dead-letter、ordering、diagnostic 和执行管线
├── codec/           EventCodec 和 CodecRegistry
├── spi/             对象安全的同步/异步 SPI 及传输类型
├── registry/        EventBusSpec、provider adapter 和两个 registry
├── local/           内置 local provider 和 local SPI
└── error/           分层错误类型
```

第三方后端位于独立 crate，例如 `qubit-event-bus-kafka`、`qubit-event-bus-rabbitmq` 和 `qubit-event-bus-redis`。第三方 crate 只依赖公共 `spi`、`registry` 和模型中明确标记为 SPI 稳定契约的类型，不依赖 `local` 或 facade 内部实现。

## 公共 facade

### 同步 facade

`EventBus` 是可克隆的具体 facade，而不是带泛型方法、无法形成 trait object 的后端 trait：

```rust
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<EventBusInner>,
}
```

核心 API 形态：

```rust
impl EventBus {
    pub fn publish<T>(
        &self,
        request: PublishRequest<T>,
    ) -> Result<PublishReceipt, PublishError>
    where
        T: Send + Sync + 'static;

    pub fn publish_all<T, I>(
        &self,
        requests: I,
    ) -> BatchPublishResult
    where
        T: Send + Sync + 'static,
        I: IntoIterator<Item = PublishRequest<T>>;

    pub fn subscribe<T, H, R>(
        &self,
        request: SubscribeRequest<T>,
        handler: H,
    ) -> Result<Subscription, SubscribeError>
    where
        T: Send + Sync + 'static,
        H: Fn(Delivery<T>) -> R + Send + Sync + 'static,
        R: IntoHandlerResult + 'static;

    pub fn wait_for_idle<T>(
        &self,
        topic: &Topic<T>,
        timeout: Option<Duration>,
    ) -> Result<WaitOutcome, LifecycleError>
    where
        T: Send + Sync + 'static;

    pub fn shutdown(
        &self,
        mode: ShutdownMode,
    ) -> Result<ShutdownOutcome, ShutdownError>;
}
```

同步 `subscribe` 的 `handler` 是 facade 在每次成功接收并解码一个事件后调用的用户业务函数。它接收 `Delivery<T>`，执行领域逻辑，并通过返回值以及 manual ACK 状态决定本次尝试成功或失败。facade 负责在自己的 worker 上调用它，并在调用前后执行 filter、subscriber interceptor、retry、错误处理、死信和 settlement。`Fn + Send + Sync` 允许同一 subscription 的不同 ordering lane 并发调用 handler；需要可变业务状态时，调用方应显式使用线程安全的内部可变性。

`publish_all` 是 `publish` 的 best-effort 批量对应接口。同步 facade 建立底层 SPI subscription，并使用 facade 管理的 worker 执行 `receive` 和 handler 管线。worker 调度器属于 facade 配置；local SPI 不直接执行用户 handler。

### 异步 facade

`AsyncEventBus` 同样是可克隆的具体 facade：

```rust
#[derive(Clone)]
pub struct AsyncEventBus {
    inner: Arc<AsyncEventBusInner>,
}
```

`AsyncEventBus::from_spi(provider_id, spi)` 默认注入 `qubit-clock` 的标准单调时钟 timer；`AsyncEventBus::with_timer(provider_id, spi, timer)` 允许应用注入共享 timer，以便与 Tokio 或手动时钟统一时间域。idle wait、graceful shutdown deadline 和异步 retry delay 都使用该 timer。等待 Future 被取消时会丢弃未完成的 timer future 并注销等待；event-bus 不创建后台 timer 线程，也不隐式启动 executor task。

发布和建立订阅是异步操作：

```rust
impl AsyncEventBus {
    pub async fn publish<T>(
        &self,
        request: PublishRequest<T>,
    ) -> Result<PublishReceipt, PublishError>
    where
        T: Send + Sync + 'static;

    pub async fn publish_all<T, I>(
        &self,
        requests: I,
    ) -> BatchPublishResult
    where
        T: Send + Sync + 'static,
        I: IntoIterator<Item = PublishRequest<T>>;

    pub async fn subscribe<T>(
        &self,
        request: SubscribeRequest<T>,
    ) -> Result<AsyncSubscription<T>, SubscribeError>
    where
        T: Send + Sync + 'static;

    pub async fn wait_for_received_deliveries<T>(
        &self,
        topic: &Topic<T>,
        timeout: Option<Duration>,
    ) -> Result<WaitOutcome, LifecycleError>
    where
        T: Send + Sync + 'static;

    pub async fn shutdown(
        &self,
        mode: ShutdownMode,
    ) -> Result<ShutdownOutcome, ShutdownError>;
}
```

`AsyncEventBus` 与 `EventBus` 的业务能力保持对称：两者都提供单一 request 发布、best-effort 批量发布、订阅、idle wait 和 shutdown。唯一有意保留的形态差异是消费循环：同步 facade 在 `subscribe` 时接收 handler 并管理 handler worker；异步 facade 返回由调用方 executor 驱动的 `AsyncSubscription<T>`，handler 传给 `AsyncSubscription::run`。这种差异来自执行模型，而不是功能删减。

异步 facade 不隐式 spawn。调用方负责驱动消费循环：

```rust
let request = SubscribeRequest::new("audit", orders.clone())?.with_options(options);
let mut subscription = bus.subscribe(request).await?;
subscription
    .run(|delivery| async move {
        audit(delivery.payload()).await?;
        Ok(())
    })
    .await?;
```

应用可用任意 executor spawn `run` future。未来可以提供接受抽象 `TaskSpawner` 的便利 API，但核心 API 不依赖具体异步运行时。

### 为什么使用具体 facade

泛型 `publish<T>` 和 `subscribe<T>` 不是对象安全方法，不能直接成为 `qubit-spi` 的统一输出。具体 facade 在外层提供泛型类型安全 API，在内层调用对象安全 SPI，兼顾用户体验与 provider 扩展性。

## 类型安全领域模型

### subscriber 身份与 subscription 对象 ID

`subscriber_id` 是调用方提供、跨进程可表达的逻辑名称，不是内部对象编号，因此使用经过验证的字符串封装：

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SubscriberId(String);
```

`SubscriberId::new` 的构成规则固定为：

- UTF-8 字节长度为 `1..=128`；
- 第一个字符必须是 ASCII 字母或数字；
- 后续字符只能是 ASCII 字母、数字、`.`、`_`、`-` 或 `:`；
- 不执行大小写折叠，比较按原始字节进行；
- 不允许前后空白、控制字符或不可见字符。

这些规则让 ID 可以安全写入日志、metric label 和常见 broker 配置，同时避免 provider 各自接受不同的非法名称。provider 不应直接把它当作 Kafka consumer group、RabbitMQ queue 或 Redis group；这些资源名称由独立的 `ConsumerGroup` 或 provider options 表达。

每次成功订阅产生的内部 subscription 对象 ID 使用 `qubit_id::Id`。facade 负责生成该 `u64` 标识，并将其用于 `Subscription`、`DeliveryContext`、local routing 和 diagnostic 关联。它只保证在当前 bus 实例内唯一，不承担业务身份或跨进程持久身份。设计中凡是“subscription ID”均指 `qubit_id::Id`，凡是“subscriber ID”均指 `SubscriberId`。

事件的 `EventId` 是不同概念：它写入 envelope、provider 消息、回执和重放流程，是可移植的事件标识，默认由 UUID v4 生成，也可由调用方提供符合校验规则的字符串。不能把它替换为进程内递增的 `u64`：事件可能跨进程、跨后端持久化或在另一进程重放，单进程计数器无法保证全局唯一，也无法在重启后保留身份。`qubit_id::Id` 只用于 bus 实例内部关联 subscription，不承载事件身份。

### `Topic<T>`

`Topic<T>` 继续使用名称和 Rust payload 类型共同确定本进程内身份。它包含：

- 已验证的 topic 名称；
- `TypeId` 和类型名称；
- 可选 `Arc<dyn EventCodec<T>>`；
- 可选 schema ID。

topic 的相等和哈希只使用名称与 payload 类型，不使用 codec 实例身份。

构造方式：

```rust
let local = Topic::<OrderCreated>::new("orders.created")?;
let portable = Topic::new_with_codec(
    "orders.created",
    JsonCodec::<OrderCreated>::new(),
)?;
```

没有 codec 的 topic 只能用于支持 `PayloadModes::Native` 的后端。只支持编码载荷的后端在 publish 或 subscribe 前返回 `CapabilityError::CodecRequired`。

### `EventEnvelope<T>`

保留现有名称和大部分设计，但移除 acknowledgement 与 dead-letter 内部状态：

```rust
pub struct EventEnvelope<T> {
    id: EventId,
    topic: Topic<T>,
    payload: Arc<T>,
    headers: Headers,
    ordering_key: Option<OrderingKey>,
    timestamp: SystemTime,
    delay: Option<Duration>,
}
```

`EventEnvelope` 以 `Arc<T>` 持有 payload。普通 `new(topic, payload)` 和显式 ID 构造会将值包装为共享所有权；`from_shared_payload(topic, Arc<T>)` 与 `with_id_and_shared_payload(topic, Arc<T>, id)` 可直接接入 provider fan-out 的 native payload。`payload()` 仍借用返回 `&T`，`Clone` 只增加 Arc 强引用而不要求 `T: Clone`；`into_payload()` 消耗 envelope 并返回 `Arc<T>`。这是 0.12 的有意破坏性变更，用来保证同一个非 `Clone` native payload 可以被多个订阅零复制共享。

publish 消费 envelope。facade 将 payload 转换为共享内部表示，所以基础 publish 不再要求 `T: Clone`。publisher interceptor 按顺序取得并返回 envelope 的所有权。

`EventId` 保持可携带规则的字符串值对象：调用方可通过 `EventId::new` 构造并校验自定义 ID；未显式提供 ID 时，库使用 `qubit-id 0.6` 的 UUID v4 生成器创建跨进程可用的默认 ID。UUID 随机源可能失败，因此 `EventId::generate`、`EventEnvelope::new` 和简单 `PublishRequest::new` 都返回 `Result`，底层错误通过 `EventIdGenerationError` 的标准错误来源链保留。该错误包装类型不重导出 `qubit-id` 的成员。

### `PublishRequest<T>` 与 `SubscribeRequest<T>`

Rust 不支持默认参数。为避免 `publish`、`publish_with_options`、`publish_envelope` 和 `publish_envelope_with_options` 随功能增长继续组合爆炸，facade 只提供一个 `publish` 方法，所有输入由 request 值表达：

```rust
pub struct PublishRequest<T> {
    envelope: EventEnvelope<T>,
    options: PublishOptions<T>,
}

impl<T> PublishRequest<T> {
    pub fn new(topic: Topic<T>, payload: T) -> Result<Self, EventIdGenerationError>;
    pub fn from_envelope(envelope: EventEnvelope<T>) -> Self;
    pub fn builder() -> PublishRequestBuilder<T>;
    pub fn with_options(self, options: PublishOptions<T>) -> Self;
}
```

普通发布仍然简洁：

```rust
let receipt = bus.publish(PublishRequest::new(
    orders.clone(),
    OrderCreated { /* ... */ },
)?)?;
```

需要完整控制时，request builder 直接覆盖 envelope metadata 和 publish options，不要求调用方先创建 `EventEnvelope` 或 `PublishOptions`：

```rust
let request = PublishRequest::builder()
    .topic(orders.clone())
    .payload(OrderCreated { /* ... */ })
    .event_id(EventId::new("order-event-42")?)
    .header("trace-id", trace_id)
    .ordering_key("customer-42")
    .timestamp(SystemTime::now())
    .delay(Duration::from_secs(5))
    .retry_policy(RetryPolicy::builder().max_attempts(3).build()?)
    .error_handler(log_publish_failure)
    .build()?;
let receipt = bus.publish(request)?;
```

`PublishRequestBuilder<T>` 内部同时构造 `EventEnvelope<T>` 和 `PublishOptions<T>`，提供以下类别的链式方法：

- 必填：`topic`、`payload`；
- envelope metadata：`event_id`、`header`、`headers`、`ordering_key`、`timestamp`、`delay`；
- publish policy：`retry_policy`、`retry_rule`、`retry_cancellation_token`、`error_handler`；
- 整体复用：`options(PublishOptions<T>)`。

未设置 event ID 和 timestamp 时分别生成 UUID v4 并使用当前时间；UUID 生成依赖操作系统随机源，不能取得随机字节时会以 `EventIdGenerationError` 可恢复地返回，绝不 panic。`EventId::new` 继续校验调用方提供的字符串 ID。显式传入 event ID 时不会调用随机生成器。未设置 options 时使用不可变默认值。`build()` 验证 topic、payload、header、ordering key、delay 和 policy 组合；缺少必填字段或组合非法时返回 `PublishRequestBuildError`，自动 ID 生成失败时其 `EventIdGeneration` 变体保留完整错误来源链。builder 不暴露 acknowledgement、dead-letter marker 或其他 delivery-only 状态。

`EventEnvelope::new(topic, payload)` 同样返回 `Result<EventEnvelope<T>, EventIdGenerationError>`，因为直接创建 envelope 也需要生成默认事件 ID。调用方无需依赖或导入 `qubit-id` 的类型；如果需要诊断底层失败，可通过标准 `Error::source()` 沿错误来源链检查。

`from_envelope` 只用于重放、转发或已有 envelope 的高级场景；普通调用方使用 `new` 或 `builder`。批量发布接收 `IntoIterator<Item = PublishRequest<T>>`，因此每个事件可以拥有自己的 metadata 和 options。

订阅采用同一思路：

```rust
pub struct SubscribeRequest<T> {
    subscriber_id: SubscriberId,
    topic: Topic<T>,
    options: SubscribeOptions<T>,
}

impl<T> SubscribeRequest<T> {
    pub fn new(subscriber_id: SubscriberId, topic: Topic<T>) -> Self;
    pub fn builder() -> SubscribeRequestBuilder<T>;
    pub fn with_options(self, options: SubscribeOptions<T>) -> Self;
}
```

最简单的订阅只需要 subscriber ID、topic 和 handler：

```rust
let request = SubscribeRequest::new("audit", orders.clone())?;
let subscription = bus.subscribe(request, |delivery| {
    audit(delivery.payload())?;
    Ok(())
})?;
```

`SubscribeRequest::new` 使用 automatic ACK、无 filter、无 retry、无 error handler、无 dead-letter、无 consumer group、ephemeral durability、从新消息开始消费以及空 provider options。若具体 provider 无法支持这组默认值，建立订阅时返回明确的 configuration 或 capability error，不进行静默转换。

`SubscribeRequestBuilder<T>` 同样不要求调用方先创建 `SubscribeOptions<T>`：

```rust
let request = SubscribeRequest::builder()
    .subscriber_id(SubscriberId::new("audit")?)
    .topic(orders.clone())
    .ack_mode(AckMode::Manual)
    .filter(|event| event.payload().tenant_id == "acme")
    .retry_policy(RetryPolicy::builder().max_attempts(3).build()?)
    .error_handler(handle_delivery_failure)
    .dead_letter(DeadLetterPolicy::topic("orders.dead")?)
    .build()?;
```

builder 覆盖 subscriber ID、topic、ACK mode、filter、retry policy/rule/cancellation、error handlers、dead-letter、ordering policy、consumer group、durability、start position 和 provider options，同时提供 `options(SubscribeOptions<T>)` 复用已有配置。`build()` 缺少 subscriber ID 或 topic 时返回 `SubscribeRequestBuildError`，并在建立 SPI subscription 之前验证 options 与 required capabilities。

订阅没有 priority 设置或基于优先级的调度保证。同步 facade 的 delivery scheduler 会在各订阅之间轮转选择符合条件的排队 handler 工作，同时遵守 bus-wide in-flight 上限和 ordering-key lane 限制。请求 `OrderingPolicy::PerKey` 时，provider 必须声明 `OrderingCapability::PerKey` 或 `PerSubscription`；否则 facade 会在调用 SPI `subscribe` 前拒绝该订阅。

两个 request builder 遵循相同的组合规则：标量字段最后一次设置生效；`header` 按 key 覆盖而 `headers` 按迭代顺序合并；可重复的 interceptor 和 error handler 按调用顺序追加；`options(...)` 在调用位置整体替换 policy 状态，后续链式 policy 方法再覆盖或追加。rustdoc 必须为这些规则提供断言示例。

因此 options 在两个方向上都不是额外的 facade 变种：`PublishRequest::new` 和 `SubscribeRequest::new` 使用默认 options；builder 服务于一次性完整配置；`with_options` 或 builder 的 `options` 方法服务于可复用配置。

### `Delivery<T>`

handler 不再直接收到发布者构造的 envelope，而是收到投递视图：

```rust
pub struct Delivery<T> {
    event: Arc<DeliveredEvent<T>>,
    context: DeliveryContext,
    acknowledgement: Acknowledgement,
}
```

`DeliveryContext` 至少包含：

- provider ID；
- `qubit_id::Id` 类型的 subscription ID；
- `SubscriberId` 类型的 subscriber ID；
- facade retry attempt；
- provider delivery attempt（provider 能提供时）；
- provider message metadata；
- settlement 能力；
- dead-letter 标记。

payload 通过 `Delivery::payload(&self) -> &T` 访问。发布者无法注入或覆盖 acknowledgement。

## codec 和传输载荷

### `EventCodec<T>`

```rust
pub trait EventCodec<T>: Send + Sync + 'static {
    fn content_type(&self) -> &ContentType;
    fn schema_id(&self) -> Option<&SchemaId>;
    fn encode(&self, value: &T) -> Result<Arc<[u8]>, CodecError>;
    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError>;
}
```

核心 crate 不默认选择 JSON、CBOR、Protobuf 或 Avro。相应 codec 由独立 crate、可选 feature 或应用提供。

### SPI 传输载荷

```rust
pub enum TransportPayload {
    Native(Arc<dyn Any + Send + Sync>),
    Encoded(EncodedPayload),
}

pub struct EncodedPayload {
    bytes: Arc<[u8]>,
    content_type: ContentType,
    schema_id: Option<SchemaId>,
}
```

- local、crossbeam、flume、bus 和 Tokio broadcast 可以使用 `Native`，避免无意义的序列化。
- RabbitMQ、Kafka、Redis 等跨进程后端使用 `Encoded`。
- 同时支持两种模式的后端默认优先 `Native`，除非 facade 配置明确要求验证编码路径。
- SPI 不执行 Rust downcast 或 codec 调用；这些工作由 facade 完成。

## 最小同步 SPI

```rust
pub trait EventBusSpi: Send + Sync + 'static {
    fn capabilities(&self) -> EventBusCapabilities;

    fn publish(
        &self,
        message: OutboundMessage,
    ) -> Result<PublishAcknowledgement, SpiError>;

    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn EventSubscriptionSpi>, SpiError>;

    fn wait_for_topic_idle(
        &self,
        topic: &TopicAddress,
        timeout: Option<Duration>,
    ) -> Result<Option<bool>, SpiError>;

    fn shutdown(
        &self,
        mode: ShutdownMode,
    ) -> Result<ShutdownOutcome, SpiError>;
}
```

SPI 不包含 interceptor、handler、filter、retry、dead-letter 或 observer。

`wait_for_topic_idle` 有默认实现并返回 `Ok(None)`，表示 provider 不提供此能力。实现可返回 `Ok(Some(true))` 表示没有该 Topic 的未完成消息，`Ok(Some(false))` 表示超时；SPI 错误保留为 `Err`。同步 facade 把不支持映射为 `LifecycleError::IdleWaitUnsupported`。

底层订阅是单一所有者的 receiver：

```rust
pub trait EventSubscriptionSpi: Send + 'static {
    fn receive(
        &mut self,
        timeout: Duration,
    ) -> Result<ReceiveOutcome, SpiError>;

    fn settle(
        &mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> Result<(), SpiError>;

    fn close(&mut self) -> Result<(), SpiError>;
}
```

facade 总是使用有限的 `timeout` 调用 `receive`，从而能够观察取消和 shutdown，不要求另一个线程并发调用 `close`。SPI 必须在超时附近返回，不能无限阻塞。

## 最小异步 SPI

异步 SPI 使用与 `qubit-spi::ProviderFuture` 相同的 runtime-neutral boxed future 形式：

```rust
pub type SpiFuture<'a, T> =
    Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait AsyncEventBusSpi: Send + Sync + 'static {
    fn capabilities(&self) -> EventBusCapabilities;

    fn publish<'a>(
        &'a self,
        message: OutboundMessage,
    ) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>>;

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>>;

    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>>;
}
```

```rust
pub trait AsyncEventSubscriptionSpi: Send + 'static {
    fn receive<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>>;

    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>>;

    fn close<'a>(
        &'a mut self,
    ) -> SpiFuture<'a, Result<(), SpiError>>;
}
```

`receive` future 被取消时不得使 subscription 永久失效。provider 无法提供取消安全保证时，必须在内部使用持续运行的消费任务和缓冲队列，将可取消边界放在该队列的读取端。

## SPI 消息和接收结果

```rust
pub struct OutboundMessage {
    topic: TopicAddress,
    payload_type_id: TypeId,
    id: EventId,
    timestamp: SystemTime,
    headers: Headers,
    ordering_key: Option<OrderingKey>,
    delay: Option<Duration>,
    payload: TransportPayload,
}

pub struct InboundMessage {
    topic: TopicAddress,
    id: EventId,
    timestamp: SystemTime,
    headers: Headers,
    ordering_key: Option<OrderingKey>,
    payload: TransportPayload,
    settlement: Option<SettlementToken>,
    provider_metadata: ProviderMessageMetadata,
}
```

```rust
pub enum ReceiveOutcome {
    Message(InboundMessage),
    Gap(DeliveryGap),
    TimedOut,
    Closed,
}
```

`Gap` 表示消费仍可继续，但 provider 已知有消息未被当前订阅观察到。例如 Tokio broadcast 的 lagged receiver 应返回 `Gap`，而不是伪装成普通超时或永久关闭。facade 发出 data-loss diagnostic 后继续或终止，取决于订阅的 gap policy。

`SettlementToken` 是不透明、不可克隆、与产生它的 subscription 实例绑定的值。facade 只能将它原样交回同一个 subscription，因此 sync/async `settle` 都借用 token，facade 在结果明确前持续持有 token。错误 subscription 必须返回结构化 `SpiError`。

Settlement 必须以 `(token, disposition)` 为幂等键：对同一个 token 重复提交相同 disposition，provider 必须返回与首次调用相同的终态结果；同一个 token 使用不同 disposition 必须返回结构化 `SpiError::InvalidSettlementToken`（稳定原因 `conflicting_disposition`），不得改变已选定的终态。async `settle` 的返回 future 可能在 provider 已执行或正在执行操作时被取消；调用方可以保留 token 并以相同 disposition 重试。provider 必须让执行中和已完成操作都满足上述幂等语义，使取消边界上的结果不确定不会导致丢失 token、重复应用终态或产生互相矛盾的终态。为了让 future 保持 runtime-neutral 的 `Send` 契约且不额外要求 token 内部状态为 `Sync`，provider 在 `settle` 方法返回 future 前必须从借用的 token 同步提取后续操作所需的自有数据，future 不得继续借用 token。provider 可在内部缓存终态结果，缓存生命周期至少覆盖 token 仍有效的整个时段。

Settlement 合同对同步与异步 SPI 完全相同：重复提交同一 token 和同一 disposition 必须幂等并返回一致终态；同一 token 配合冲突 disposition 必须返回结构化无效 token 错误。同步调用失败且结果不确定时，facade 也可能再次提交同一对值，因此 provider 不能重复应用非幂等副作用。异步 future 被取消后，facade 必须能持有 token 并安全地以相同 disposition 重试；provider 在返回 future 前从借用 token 提取后续所需的自有状态，future 本身不得继续借用 token。

## 能力模型

能力描述创建完成后的 SPI 实例，并在实例生命周期内保持不变：

```rust
pub struct EventBusCapabilities {
    payload_modes: PayloadModes,
    settlement: SettlementCapabilities,
    ordering: OrderingCapability,
    delayed_delivery: DelayedDeliveryCapability,
    durability: DurabilityCapability,
    consumer_groups: bool,
    replay: ReplayCapability,
    publish_guarantee: PublishGuarantee,
    publish_visibility: PublishVisibility,
}
```

首版能力具有以下精确定义：

| 能力 | 取值 | 含义 |
| --- | --- | --- |
| payload | `Native`、`Encoded` 或两者 | SPI 接受和产生的 payload 形态 |
| settlement | `None`、`AcceptOnly`、`AcceptRetryReject` | provider 能执行的消息终态操作 |
| ordering | `None`、`PerSubscription`、`PerKey`、`PerPartition` | provider 自身保证的最大顺序范围 |
| delayed delivery | `None`、`Native` | provider 是否原生支持到期后可见 |
| durability | `Ephemeral`、`Durable` | subscription 离线时消息是否可保留 |
| consumer groups | 布尔值 | provider 是否支持组内竞争消费 |
| replay | `None`、`Position`、`Timestamp` | 能否从历史位置开始消费 |
| publish guarantee | `FireAndForget`、`Accepted`、`Confirmed`、`DurablyStored` | publish 成功可证明的最大保证 |
| publish visibility | `Opaque`、`DestinationAdmissions` | publish 能否报告具体目标的准入结果 |

核心 API 不声称提供 exactly-once。Kafka transaction、RabbitMQ publisher confirms 等更强或更具体的语义由 provider 扩展 API 表达。

`EventBusConfig` 可以声明 `RequiredCapabilities`。registry 使用 validating provider adapter 检查 provider 返回的 SPI；能力不足会在创建阶段通过 `ProviderFailure::unsupported(...)` 报告，因此可以按照 `FallbackPolicy` 尝试下一候选。SPI 已通过创建验证后，某次操作的 capability mismatch 只返回 `CapabilityError`，不会再触发 fallback。

## 发布回执

现有逐订阅者准入回执值得保留，但不能强迫 Kafka 等后端虚构消费者列表：

```rust
pub enum PublishAcknowledgement {
    Accepted {
        provider_message_id: Option<String>,
        metadata: ProviderMessageMetadata,
    },
    DestinationAdmissions(Vec<DestinationAdmission>),
    DroppedByInterceptor,
}

pub struct PublishReceipt {
    input_event_id: EventId,
    dispatched_event_id: Option<EventId>,
    provider_id: ProviderId,
    acknowledgement: PublishAcknowledgement,
}
```

- local SPI 返回 `DestinationAdmissions`，继续提供现有逐订阅者 accepted/filtered/rejected 信息。
- broker 一般返回 `Accepted`，可携带 partition、offset、message ID 等非敏感元数据。
- publisher interceptor 主动丢弃返回 `DroppedByInterceptor`，不是错误。
- publish 成功只表示达到 provider 声明的 `PublishGuarantee`，不表示 handler 已完成。

批量发布继续采用 best-effort 语义，保留输入顺序，并为每项保存 `Result<PublishReceipt, PublishError>`。除非未来 SPI 明确增加独立的事务扩展接口，否则通用批量 API 不承诺原子性。

## 订阅请求和 provider 扩展

SPI 订阅请求只携带传输层需要的内容：

```rust
pub struct SpiSubscriptionRequest {
    subscription_id: Id,
    topic: TopicAddress,
    subscriber_id: SubscriberId,
    group: Option<ConsumerGroup>,
    durability: SubscriptionDurability,
    start_position: StartPosition,
    provider_options: ProviderOptions,
}
```

`SpiSubscriptionRequest` 是类型擦除后的 SPI 输入，不同于应用使用的泛型 `SubscribeRequest<T>`。其中 `Id` 来自 `qubit-id`，由 facade 在建立订阅前生成；`SubscriberId` 是调用方提供的已验证逻辑名称。SPI 必须原样保留两者，并在 admission、message metadata 和错误上下文中返回相同的 subscription ID。

`payload_type_id` 由 facade 从 `Topic<T>` 传入，供支持原生 Rust payload 的 provider 在路由前拒绝同名异类型的活跃订阅或发布。编码 provider 可以忽略此 Rust 进程内类型标识。

filter、interceptor、application retry、error handler 和 dead-letter 不进入 SPI；API 不提供 priority 配置，也不会按该规则调度。

Kafka isolation level、RabbitMQ exchange/queue 参数、Redis stream trimming 等使用命名空间化 `ProviderOptions`。provider 只解释自己的命名空间；其命名空间下的未知键必须报错。核心 crate 不为这些选项赋予跨 provider 语义。

## facade 执行管线

### 发布管线

```text
PublishRequest<T>
  → 取出 EventEnvelope<T> 和 PublishOptions<T>
  → 参数和生命周期验证
  → typed publisher interceptors
  → capability 和 codec 验证
  → encode 或 type erase
  → facade publish retry
  → EventBusSpi::publish
  → PublishReceipt
  → diagnostic
```

publisher interceptor 分为 request/type-scoped 和 facade-wide global 两层，先执行 typed，再执行 global metadata interceptor；各层内按注册顺序执行，任一 interceptor 丢弃或失败后不再执行后续 interceptor。global 层通过 `PublishMetadata` 只能读取、设置和删除经验证的普通 headers，不能改 payload、event ID、topic、timestamp、ordering key 或 delay。facade-wide global interceptor 通过 `EventBusFacadeConfig::publisher_interceptor` 注册，并同时用于 sync/async facade；返回 `Ok(false)` 时会产生 dropped receipt 且不调用 SPI。

### 消费管线

```text
EventSubscriptionSpi::receive
  → gap / receive error 处理
  → decode 或 downcast
  → filter
  → facade admission 和 backpressure
  → ordering lane
  → typed subscriber interceptors
  → handler
  → facade retry
  → error handlers
  → dead-letter policy
  → settle(Accept | Retry | Reject)
  → DeliveryFailure diagnostic
```

subscriber interceptor 使用 middleware 语义：正式目标由 facade-wide global middleware 包围 request-level typed middleware，handler 位于最内层；返回路径按相反顺序展开。filter 在 interceptor 之前执行，因此被过滤的 delivery 不进入 middleware 链。`EventBusFacadeConfig` 提供按 payload type 注册的全局同步和 runtime-neutral async middleware；每个 facade 在对应订阅通过 filter 后执行其全局链，再执行 request-level typed 链。未匹配当前 facade 执行模型的全局 middleware 会在 provider subscribe 前作为配置错误拒绝。

同步与异步 typed middleware 使用不同的类型：同步 middleware 返回 `Result<(), DeliveryError>`，异步 middleware 返回 runtime-neutral 的 `SpiFuture<'static, Result<(), DeliveryError>>`。两者都收到一个 `FnOnce` continuation，只能继续内层处理一次；不调用 continuation 表示有意短路内层 middleware 与 handler。async continuation 取得 delivery 的所有权并返回 `'static` future，因此调用方不需要借用特定 runtime 或在 future 中持有 facade 栈帧。facade 在调用 middleware/handler 时隔离 panic；async 实现也捕获 future poll 中的 panic，并将其转换为 delivery failure。

`SubscribeOptions<T>` 可以同时保存同步 `SubscriberInterceptor<T>` 和异步 `AsyncSubscriberInterceptor<T>`，以便同一请求模型表达两类中间件。但执行模型必须匹配：`EventBus::subscribe` 遇到非空 async interceptor 列表、`AsyncEventBus::subscribe` 遇到非空 sync interceptor 列表时，都会返回配置错误，不会阻塞、跳过或隐式转换中间件。

内部错误传播使用带来源的包装：

```rust
struct PipelineFailure {
    origin: FailureOrigin,
    error: EventBusError,
}
```

因此无需像当前 `subscriber_interceptor_chain` 那样为整个公开错误枚举维护错误指纹。

## ACK、NACK 与 settlement

`Acknowledgement` 只存在于 `Delivery<T>`。状态机为：

```text
Pending ──ack──> Acknowledged
Pending ──nack─> NegativelyAcknowledged
```

首次终态决定生效。重复相同决定是幂等成功；相反决定返回 `AcknowledgementError::AlreadyCompleted`，不再采用“最后一次写入获胜”。

handler 结果解释规则：

| Ack 模式 | handler 返回 | ACK 状态 | 结果 |
| --- | --- | --- | --- |
| Auto | `Ok` | 不适用 | 成功 |
| Auto | `Err` 或 panic | 不适用 | 失败 |
| Manual | `Ok` | ACK | 成功 |
| Manual | `Ok` | NACK 或 Pending | 失败 |
| Manual | `Err` 或 panic | 任意 | 失败 |

handler 返回错误始终优先，避免代码先 ACK 后失败造成消息丢失。

每次 facade 本地 retry 创建新的 `Delivery` attempt：event 和 transport context 保持共享，retry attempt 编号递增，但 acknowledgement 从 `Pending` 重新开始。相同 attempt 内的 delivery clone 仍共享 ACK/NACK 状态。handler 错误、panic 和 manual ACK/NACK 结果统一进入 `qubit-retry`；终态 retry 错误通过 `DeliveryError::Retry(RetryError<DeliveryAttemptError>)` 保留 `RetryError`、attempt error 和原始 delivery error 的 source chain。

facade application retry 在同一个收到的消息上重新执行 handler，不在每次尝试后调用 SPI settlement。达到终态后只 settle 一次：

- 成功 → `Accept`；
- error handler 返回 `Requeue` 且 provider 支持 → SPI `Retry`；
- 已死信、明确丢弃或不可重试 → `Reject`；
- 没有 settlement token → 只记录 facade 终态，无法影响 provider。

Manual ACK 只有在 facade 能在成功和失败路径都提供有意义的投递终态时才允许，因此要求 provider 至少支持 `AcceptRetryReject`；`None` 与 `AcceptOnly` 均拒绝 Manual ACK 配置。对于短暂广播后端，应用仍可使用 handler 返回值和 facade retry，但不能误以为 NACK 会让消息重新出现。

`FailureDirective::Retry` 表示由 `qubit-retry` 控制的 facade 本地有界重试；它不等于 broker requeue。`Requeue` 才请求 provider 的 `DeliveryDisposition::Retry`，创建订阅时必须验证 settlement capability。达到本地 retry 终态后，facade 按 error handler 最终决定执行 dead-letter、discard 或 provider requeue，且对同一个 settlement token 至多执行一次终态 settlement。没有 settlement token 时只保留 facade 终态，不虚构 provider disposition。

DeadLetter 必须先成功发布标准死信 `EventEnvelope` 才能 Reject 原消息。死信构造或发布失败时不得 Reject；若原 settlement token 属于当前 subscription 且 provider 支持 Retry，则请求 Requeue，否则不执行 settlement，并发出 `SettlementUnavailable` 诊断。Discard/Requeue 对应的 Reject/Retry 若 provider capability 不支持，也不得静默忽略：有有效 token 时发出 `SettlementUnavailable`，并保留消息未结算。

## retry、错误处理和死信

保留当前“尝试 → 错误处理 → 死信 → 终态观察”的总体设计。retry 是独立、完整且已经由 `qubit-retry` 定义的通用领域；`qubit-event-bus` 不再复制或包装一套同义类型。

核心 crate 直接在公开签名中使用所需类型，但不重新导出：

```rust
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryError;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;
```

调用方需要在 `Cargo.toml` 中显式依赖 `qubit-retry`，并直接通过 `qubit_retry::*` 使用这些类型。第三方规则直接实现 `RetryRule<PublishAttemptError>` 或 `RetryRule<DeliveryAttemptError>`，不会发生 wrapper 转换或能力落后。

不重新导出的原因是依赖归属应当透明：使用 retry 高级配置的应用明确依赖并导入 `qubit-retry`；只使用默认 retry 行为的应用不需要在源码中引用它。`qubit-event-bus` 与 `qubit-retry` 的兼容版本由两者的 Cargo 约束和 changelog 管理，不通过隐藏路径制造第二个入口。

这种选择会让 `qubit-event-bus` 的公开 API 与选定的 `qubit-retry` 主版本形成明确的版本关系，但包装无法消除真正的行为耦合，只会把它转化成重复模型、错误映射和长期同步成本。

`qubit-event-bus` 只定义事件总线特有的 attempt error、默认错误分类规则和 retry 管线接入，不重新定义 `RetryPolicy`、`RetryDecision`、`RetryContext`、cancellation token 或 retry terminal outcome。最终 `PublishError` 或 `DeliveryError` 可以把对应的 `RetryError<PublishAttemptError>` 或 `RetryError<DeliveryAttemptError>` 作为 `source()` 保留，同时通过稳定的 event-bus error kind 提供高层分类。

发布终态错误 handler 与订阅投递错误处理器承担不同职责：

- 发布流程由 `qubit-retry` policy 独占控制有界重试。若未配置 retry policy，SPI publish 的直接失败就是终态；若配置了 policy，则在 retry 到达终态后，publish error handlers 按注册顺序接收终态错误和 `PublishFailureContext<T>`，返回 `()`。它们只记录或上报，不改变发布结果，也不能请求再次重试、重新入队、死信或丢弃。
- 该回调只处理进入发布重试/SPI 调用后最终失败的事件；请求构造、能力检查、编解码或 interceptor 等预检错误不会调用它。
- `PublishFailureContext<T>` 持有共享 `Arc<T>`，并提供事件 ID、topic、headers、ordering key、时间戳和 delay 的只读访问。native payload 即使不实现 `Clone`，handler 仍可读取它。
- 每个 publish error handler 的 panic 均被隔离，后续 handler 仍会运行。若任一 handler panic，发布返回结构化 `PublishError::ErrorHandlerPanicked`，其 `source()` 保留原始终态发布错误。
- 下方 `FailureDirective` 只用于订阅投递失败后的生命周期决策，不用于发布失败 callback。

订阅投递错误处理器可返回显式决定，而不是通过修改 ACK 暗示结果：

```rust
pub enum FailureDirective {
    Retry,
    Requeue,
    DeadLetter,
    Discard,
}
```

订阅错误处理器对每次失败的 handler attempt 按注册顺序调用；同一次失败中所有未 panic 的处理器都会被通知。`Retry` 只表示允许 `qubit-retry` 的 `RetryRule` 和 `RetryPolicy` 继续评估，不绕过规则、attempt 上限、取消、backoff 或时间预算。任一处理器返回 `Requeue`、`DeadLetter` 或 `Discard` 时，该失败 attempt 立即终止本地重试；其他处理器不能用 `Retry` 覆盖这个显式终态动作。panic 被隔离并按 `Discard` 处理。对重试最终失败的 attempt，错误处理器已经完成通知，不会再额外重复调用一轮终态回调。未配置 retry policy 时，`Retry` 不会隐式创建重试，最终按 `Discard` 处理。

dead-letter 默认由 facade 构造标准死信事件并再次调用同一 bus publish。provider 原生 DLQ、Kafka transaction 或 RabbitMQ dead-letter exchange 属于 provider 扩展；启用后必须明确说明它替代还是补充 facade dead-letter，不能双重发布。

标准 facade 死信的 payload 类型是公开的 `model::DeadLetterEvent<T>`，包含原事件 envelope 的共享 `Arc`、失败的 `SubscriberId` 和终态错误文本。消费者可以按该类型订阅配置的死信 topic；`T` 无需 `Clone`。使用编码传输时，应用/provider 必须为此 payload 类型注册 codec。

标准 facade 死信事件在 headers 中携带保留标记 `x-qubit-event-bus-dead-letter: v1`。该标记是跨 provider transport 的契约：SPI/provider 必须原样保留此 header，facade 在解码入站消息时据此将 `DeliveryContext::is_dead_letter()` 设为 `true`。应用和拦截器不得自行设置、删除或改写该保留 header。若带标记的事件再次失败并选择 DeadLetter，facade 不再发布第二条死信，避免同 topic 或 provider 回投造成递归；原消息仍按终态 settlement 规则处理。该 header 不改变 SPI 的最小接口，也不代表 broker-native DLQ 状态。

异步死信发布遵循至少一次（at-least-once）语义：若调用方在 provider 可能已接受死信、但发布 future 尚未返回时取消 runner，恢复后可能重新发布同一死信。需要跨取消严格去重的 provider 应基于原事件 ID 或业务幂等键提供去重；facade 不声称此边界恰好一次。

## 顺序、延迟和背压

### 顺序

顺序保证由两部分组成：

1. provider 声明的传输顺序；
2. facade 在单个 subscription handler 上施加的执行顺序。

现有 local ordering lane 设计保留：同一 topic、ordering key 和 subscription ID 进入同一串行 lane。其他后端可以依赖 partition 或 provider 顺序，但 facade 不把 `PerPartition` 宣称为全局 `PerKey`。

同步 facade 的 ordering lane 可以用条件变量等待；runtime-neutral async facade 必须使用 future/waker 驱动的 lane，不得阻塞 executor worker。lane guard 覆盖当前 delivery 的 middleware、handler、本地 retry、error handler、dead-letter 和 settlement 生命周期；取消或 panic 时 guard 的 drop 仍释放 lane 并唤醒下一项。

### 延迟

通用 `delay` 只有 provider 声明 `DelayedDeliveryCapability::Native` 时才由 SPI 保证。facade 可以为 local/native 后端提供进程内调度，但该能力必须报告为非持久，不能在网络后端上伪装成可靠延迟队列。

### 背压

正式设计中的 facade 背压模型区分以下限制：

- 最大 in-flight delivery；
- handler queue capacity；
- 每 subscription 缓冲上限；
- diagnostic 队列上限。

admission permit 使用 RAII 释放：delivery 进入终态、被取消或处理中 panic/drop 时均释放一次；准入拒绝在进入 handler/middleware 前发生。ordering queue 和 admission permit 是不同资源，等待某 key 的 lane 不得绕过全局 in-flight 上限。

达到限制时不得无限增长。local SPI 可以在 publish receipt 中报告具体 subscription rejection；无法观察远端消费者的 broker 只能报告本次 publish 的 provider acknowledgement。

实现状态：同步 facade 通过 `EventBusFacadeConfig::with_sync_delivery_scheduler` 配置 bus 级 `max_in_flight` 和 handler queue capacity；in-flight 限额覆盖排队、handler/retry/middleware 执行及最终 settlement，不同 ordering key 可并行、相同 key 保序。同步 scheduler 默认最多 4 个 in-flight delivery、最多排队 32 个 handler；队列容量设为 0 时仅允许交给空闲 worker 的直接移交。每个 subscription coordinator 至多暂存一个已经从 SPI 接收、尚未准入 scheduler 的消息，饱和或关闭时会按 Retry 能力归还，不会静默丢弃。异步 facade 由调用方驱动、不 spawn task，并通过 `DeliveryAdmissionConfig::max_in_flight` 使用 bus-wide 有界准入（默认 4）；permit 覆盖收到的消息、lane wait、middleware、handler/retry 到最终 settlement。单个 subscription 可同时持有多个 owned delivery future，不同 ordering key 并行、同 key 保序；每个 subscription 最多暂存一条未准入消息。取消 `run` future 会暂停并保留任务及 permit；重新 `run` 续跑旧任务，shutdown 可接管暂停 session。Immediate 会丢弃未开始的 lane waiter并依赖 SPI receiver close/drop 恢复未结算 token，已开始 handler 则继续等待并 settlement。`LocalEventBusConfig::queue_capacity` 限制每个 local subscription 的排队及未 settlement 消息数；已经 receive 的消息仍占用容量，Retry 会保留额度。诊断通过同步 observer 直接回调，没有独立诊断队列；observer 会被隔离 panic，但阻塞的 observer 仍可能延迟触发它的线程。`EventBusFacadeConfig` 同时承载 codec registry 及按 payload type 注册的 global sync/async subscriber middleware。

## 错误模型

公开错误按操作分层，不再要求 `Clone + Eq`：

```rust
pub enum EventBusError {
    Configuration(ConfigurationError),
    Capability(CapabilityError),
    Codec(CodecError),
    Publish(PublishError),
    Subscribe(SubscribeError),
    Receive(ReceiveError),
    Delivery(DeliveryError),
    Settlement(SettlementError),
    Lifecycle(LifecycleError),
    Provider(ProviderError),
}
```

每个跨 SPI 的错误保存：

- provider ID；
- operation；
- topic 或 subscription 上下文；
- 稳定的错误 kind；
- 可选重试分类；
- 原始错误作为 `source()`。

`SpiError` 同样不要求可克隆或相等。测试使用稳定 kind 和访问器断言，不比较完整错误对象。

## 诊断和观察

统一观察类型：

```rust
pub enum Diagnostic {
    AdmissionRejected { event_id, topic, subscriber_id, reason },
    ReceiveGap { subscription_id, subscriber_id, topic, gap },
    DeliveryFailed { event_id, topic, subscription_id, subscriber_id, attempts, error },
    SettlementFailed { event_id, topic, subscription_id, subscriber_id, disposition, error },
    SettlementUnavailable { event_id, topic, subscription_id, subscriber_id, requested },
    InternalFailure { origin, message },
}
```

语义固定如下：

- 每个 facade 可观察到的 admission rejection 恰好发出一次 `AdmissionRejected`；
- 每个经过 retry、error handler 和 dead-letter 后仍失败的 delivery 恰好发出一次 `DeliveryFailed`；
- non-fatal receive gap 发出一次 `ReceiveGap`；
- settlement 失败独立发出 `SettlementFailed`，不能覆盖原 handler 结果；
- provider 不支持请求的终态 settlement 时发出 `SettlementUnavailable`，不能静默假定消息已完成；
- 参数和配置错误直接返回调用方，默认不发 runtime diagnostic；
- observer panic 被隔离并抑制递归上报；
- observer 不得改变操作结果。

当前 public observer 入口是 `EventBus::observe_diagnostics` / `AsyncEventBus::observe_diagnostics`，通过统一 `Diagnostic` 流接收运行期观测；尚无独立的 `DeliveryFailureObserver` 便利 API。

## 生命周期

provider 创建成功即返回可用 SPI；不再暴露 `new → start` 的半初始化状态。facade 状态机为：

```text
Running → Closing → Closed
```

关闭后不可重启。需要新实例时重新通过 registry 创建，这与 provider 生命周期及外部连接资源更一致。

```rust
pub enum ShutdownMode {
    Graceful { timeout: Duration },
    Immediate,
}
```

两个 facade 都公开 shutdown，并保持同步/异步对称：

```rust
impl EventBus {
    pub fn shutdown(
        &self,
        mode: ShutdownMode,
    ) -> Result<ShutdownOutcome, ShutdownError>;
}

impl AsyncEventBus {
    pub async fn shutdown(
        &self,
        mode: ShutdownMode,
    ) -> Result<ShutdownOutcome, ShutdownError>;
}
```

facade shutdown 不是对 SPI shutdown 的简单转发。facade 必须先停止自身准入并协调 subscription、handler、retry、ordering lane 和 settlement；只有完成或放弃这些 facade 所有的工作后，才调用 `EventBusSpi::shutdown` 或 `AsyncEventBusSpi::shutdown` 关闭传输资源。应用不得直接取得 SPI 并绕过该流程。

Graceful shutdown 顺序：

1. 原子地停止接受新 publish 和 subscribe；
2. 通知 subscription runner 停止获取新消息；
3. 等待已经进入 facade 的 handler 和 retry 流程完成；
4. 对已有 token 完成 settlement；
5. 关闭底层 subscription；
6. 调用 SPI shutdown；
7. 转为 `Closed`。

`Graceful { timeout }` 的期限覆盖上述完整序列，而不只覆盖 handler drain；异步 facade 以同一个注入 timer deadline 限制未启动 receiver close、活跃工作等待和 provider shutdown。超时返回 `ShutdownError::TimedOut`，bus 保持 `Closing`，后续 shutdown 调用可继续清理。异步 close/shutdown Future 可能在 provider 已产生副作用后被取消，因此 SPI 必须允许在同一个 receiver/backend 上安全重复调用并最终完成关闭；已关闭对象的重复 close/shutdown 必须成功，shutdown outcome 保持稳定，后续 `Immediate` 可以加强先前的 `Graceful`。

Immediate shutdown 停止准入，不再接收新消息，并丢弃尚未开始的队列工作；它仍等待当前 handler/投递完成、settlement、subscription close 和 SPI shutdown，以便同步返回这些阶段的全部错误。Rust 无法安全强制终止已经运行的用户代码。`EventBusSpi::shutdown(Immediate)` / `AsyncEventBusSpi::shutdown(Immediate)` 本身只表达 provider 传输层关闭策略；先停止消费、处理当前投递并关闭 receiver 的协调顺序由 facade 保证。

同步 handler 从自己的 bus worker 调用 Graceful shutdown 返回 `LifecycleError::WouldDeadlock`，不 panic。worker 内调用任意同 bus `Subscription::cancel` 都只请求取消、不等待 join，以规避不同 subscription worker 交叉等待；只有外部线程调用 cancel 才保证等待 worker close 完成。异步 subscription runner 若等待包含自身的 shutdown future，同样返回结构化错误。所有 shutdown 错误必须返回，不得静默吞掉 tracker 错误。

`EventBus::wait_for_idle` 查询同步 provider 的 Topic 队列及未 settlement 投递：SPI 返回 `Ok(Some(true))` 表示没有未完成消息，`Ok(Some(false))` 表示超时，`Ok(None)` 表示不支持；facade 将不支持映射为 `LifecycleError::IdleWaitUnsupported`。该等待不证明 handler 成功或远端 broker 全局空闲。旧的 facade 本地已接收工作统计改名为 `wait_for_received_deliveries`；异步 facade 只提供该方法，不承诺 provider 队列为空。

## `qubit-spi` 集成

```rust
pub struct EventBusSpec;

impl ServiceSpec for EventBusSpec {
    type Config = EventBusConfig;
    type Error = EventBusProviderError;
}

impl SyncServiceSpec for EventBusSpec {
    type Output = Arc<dyn EventBusSpi>;
}

impl AsyncServiceSpec for EventBusSpec {
    type Output = Arc<dyn AsyncEventBusSpi>;
}
```

```rust
pub type EventBusProvider = dyn ProviderDefinition<EventBusSpec>;
pub type AsyncEventBusProvider = dyn AsyncProviderDefinition<EventBusSpec>;
```

provider alias 对应的 trait object 仍要求第三方 provider 只实现 metadata 与 SPI
创建。`qubit-spi` 创建 resolver 只返回 `Output`，不会附带成功候选的 descriptor；为
确保 fallback 后 facade 和 `PublishReceipt` 使用实际成功的 provider ID，sync/async
SPI trait 提供默认返回 `None` 的隐藏 metadata hook `provider_id()`。registry 在注册时
只读取一次 provider descriptor，并用透明 SPI proxy 覆盖该 hook；provider 无须实现
该方法，也无须在运行期报告自己的注册 ID。

validating adapter 先调用 provider 创建 SPI，再检查 `RequiredCapabilities`。不满足时
返回 `ProviderFailure::unsupported(...)`，仍由 `qubit-spi` resolver 按 selection 的
fallback policy 决定是否尝试下一 provider。代理只附加身份并转发操作；SPI 创建成功后，
publish、subscribe、settle、shutdown 等操作失败不再进入 resolver。

provider 只做四件事：

1. 读取并验证自己的创建配置；
2. 创建连接、channel 或底层资源；
3. 构造 SPI；
4. 返回 `Arc<dyn EventBusSpi>` 或异步对应类型。

provider 不创建 facade，也不执行用户 handler。

## Registry

```rust
pub struct EventBusRegistry {
    providers: ProviderRegistry<EventBusSpec>,
}

pub struct AsyncEventBusRegistry {
    providers: AsyncProviderRegistry<EventBusSpec>,
}
```

两个 registry 提供：

- `register` / `register_shared`；
- provider descriptor 和 ID 查询；
- default selection；
- `seal`；
- 指定 selection 或默认 selection 创建 facade；
- 创建错误与 `qubit-spi` resolution/fallback 诊断。

`new()` 和 `Default` 创建空 registry，与 `qubit-spi`、`qubit-fs-registry` 保持一致。内置 local 使用显式入口：

```rust
let registry = EventBusRegistry::with_local()?;
let bus = registry.create_selected(
    &ProviderSelection::named("local")?,
    &EventBusConfig::default(),
)?;
```

同时提供低心智负担的便利入口：

```rust
let bus = EventBus::local(LocalEventBusConfig::default())?;
```

两条路径使用同一个 `LocalEventBusProvider`，不存在第二套 local 构建逻辑。

### fallback 边界

- provider 缺失、创建失败或创建结果不满足 `RequiredCapabilities` 时，validating provider adapter 将失败交给 `qubit-spi` fallback policy；
- provider 返回并通过验证后，fallback 完成；
- publish、subscribe、receive、settle 和 shutdown 失败都不会切换 provider；
- provider panic 传播，不作为普通缺失或不支持处理；
- registry 在 provider 创建期间不持有 catalog 锁。

对外错误区分“没有解析出候选”的 `ProviderError::Resolution` 与“候选已经解析但
创建/能力校验失败”的 `ProviderError::Creation`；两者均保留 `qubit-spi` 原始错误链。

## 配置归属

```rust
pub struct EventBusConfig {
    selection: Option<ProviderSelection>,
    required_capabilities: RequiredCapabilities,
    facade: EventBusFacadeConfig,
    provider_options: ProviderOptions,
}
```

`EventBusFacadeConfig` 是 facade 构建期配置，目前仅包含并实际接入 publisher 使用的
共享 `CodecRegistry`。`EventBus::from_spi` 和 `AsyncEventBus::from_spi` 使用空 codec registry；
相应的 `with_config` 构造器以及 registry 的 `EventBusConfig::with_facade_config` 可安装
预先构造的 codec registry。该配置在 facade 创建时冻结。

worker/admission、interceptor、retry、dead-letter、diagnostic 和 shutdown 等全局 facade
默认值尚不属于当前 `EventBusFacadeConfig` 的可用字段。需要在 sync/async facade 都有
可验证消费行为后，才能分别加入该配置；不能只存储不生效的占位配置。现有 publish 与
subscribe policy 仍由各自 request/options 提供。

`ProviderOptions` 使用命名空间化、非敏感 metadata。密码、token 和私钥不得直接存入可 Debug 的 metadata；provider 通过外部 credential reference、注入的 credential resolver 或自身安全配置取得凭据。核心 crate 不定义一个虚假的通用认证模型。

provider selection、capability requirement、provider options 和 facade codec table 都在
创建调用期间固定。运行期间允许注册 observer；不提供修改 codec table 或已创建 provider
连接配置的入口。

## 内置 local provider

内置 provider ID 为 `local`，aliases 为 `memory` 和 `in-process`。它实现同步 `EventBusSpi`；后续异步 local SPI 必须使用真正的异步接收原语，不能简单在 async future 中阻塞同步 receiver。

local SPI 的职责：

- 维护 topic 到 subscription receiver 的路由快照；
- 按 subscription 队列执行 publish admission；
- 返回逐 destination admission；
- 提供 native payload；
- 支持进程内 settlement；
- 实现 ordering/delay 所需的底层队列能力；
- 在 shutdown 时关闭 receiver 并释放资源。

facade 的职责：

- worker 池；
- filter/interceptor/handler；
- retry/error handler/dead-letter；
- handler ordering lane；
- observer；
- facade in-flight tracking。

本次是 0.12 的破坏性重构，不提供 0.11 公开 API 的兼容层。以下表格总结当前 facade 与 provider 的行为边界，不构成旧配置的自动迁移承诺：

| 当前配置 | 新归属 |
| --- | --- |
| worker 数和 handler queue | `EventBusFacadeConfig::with_sync_delivery_scheduler` 设置 bus 级 `max_in_flight` 与 handler queue capacity；异步消费由调用方驱动，不创建 worker |
| local subscription queue | `LocalEventBusConfig::queue_capacity`，默认每个 subscription 1024 |
| publish/subscribe policy | 放在每个 `PublishRequest` / `SubscribeRequest` 的 options 中；builder 支持链式配置 |
| publisher/subscriber interceptor | request 级 typed interceptor；`EventBusFacadeConfig` 还支持按 payload type 注册全局 sync/async subscriber middleware |
| dead-letter policy | 通过 `SubscribeOptionsBuilder::dead_letter` 设置，并按 provider settlement capabilities 验证 |
| admission/backpressure | facade-wide sync in-flight 限额包括排队、handler/retry/middleware 与 settlement；local queue capacity 另行限制每 subscription 的 provider 队列 |
| `create_started` | `EventBus::local` 或 registry create，返回即为可用状态 |

0.11 的公开 `EventBus` trait、`EventBusFactory`、`LocalEventBus`、`LocalEventBusFactory` 和 transactional API 均不是 0.12 兼容层的一部分。使用 0.12 时应改用具体 `EventBus` / `AsyncEventBus` facade 和 typed request。异步 facade 需要一个 async provider；内置 local provider 当前仅实现同步 SPI。

## 后端适配验证

| 后端 | SPI | payload | settlement | 能力和限制 |
| --- | --- | --- | --- | --- |
| 内置 local | 同步 | Native | 完整模拟 | 进程内、非持久，可报告 destination admission |
| crossbeam-channel | 同步 | Native | 无 | receiver 模型；关闭和超时需适配 |
| flume | 同步和异步 | Native | 无 | 可分别实现两类 SPI |
| bus | 同步 | Native | 无 | 广播；慢消费者语义需报告 gap |
| Tokio broadcast | 异步 | Native | 无 | `RecvError::Lagged` 映射为 `ReceiveOutcome::Gap` |
| RabbitMQ/lapin | 异步 | Encoded | ACK/NACK/reject | exchange、queue 和 prefetch 属于 provider 配置 |
| Kafka/rdkafka | 异步为主 | Encoded | offset commit 型 | partition ordering、consumer group；内部需持续 poll |
| Redis Pub/Sub | 异步为主 | Encoded | 无 | 短暂广播、无重放、无离线持久性 |
| Redis Streams | 异步为主 | Encoded | XACK 型 | consumer group、pending entries、位置重放 |

该矩阵证明最小 SPI 不依赖某一种 channel、broker 或 async runtime。无法提供某项语义的后端通过 capability 明确拒绝配置，而不是静默降级。

## 并发和线程安全不变量

1. facade 和 SPI handle 为 `Send + Sync`；单个 subscription SPI 只有一个消费所有者，只要求 `Send`。
2. 同一 settlement token 最多成功终结一次。
3. subscription cancel、bus shutdown 和 receive timeout 竞争时，不得重复执行 handler。
4. observer、interceptor、handler 和 error handler panic 都转换为结构化失败；observer panic 不递归上报。
5. 不在持有 registry、subscription catalog、ordering lane 或 tracker 锁时调用用户代码。
6. admission permit 在 delivery 进入终态后准确释放一次。
7. ordering lane 中的取消、延迟、retry 和 panic 都必须推进后续项。
8. Async receive future 的取消不能丢失 provider 已交给 facade 但尚未返回的消息。
9. shutdown 幂等；第一个调用执行状态转换，后续调用观察相同终态。

## 测试策略

### SPI conformance suite

仓库的 `tests/spi_conformance_tests.rs` 使用内部测试辅助检查内置 provider、模拟 channel provider 和 fake broker provider 的 SPI 行为。这些辅助代码位于 crate 的集成测试目录，不属于 `qubit-event-bus` 公共 API，也不会随发布包作为第三方可复用的 conformance harness 提供。provider 作者可以将下列项目作为自行编写测试的检查清单：

- descriptor/provider 选择与创建；
- capability 稳定性和真实性；
- native/encoded payload 契约；
- publish 后 receive；
- receive timeout；
- close 后 receive 返回 `Closed`；
- gap 映射；
- settlement token 的 subscription 归属；
- 重复和冲突 settlement；
- shutdown 幂等；
- async receive cancellation safety；
-错误 source 和 provider context；
- Debug 输出不泄漏敏感 provider options。

不支持某项能力的 provider 必须通过显式 capability 跳过对应 conformance case，而不是让测试调用后再返回含糊错误。

### facade 合约测试

使用确定性的 fake SPI 覆盖：

- publish interceptor 顺序和 drop；
- codec 成功、失败和类型不匹配；
- capability validation；
- destination admission 与 opaque acknowledgement；
- filter、subscriber interceptor 和 handler 顺序；
- auto/manual ACK 状态机；
- retry cancellation 和 backoff；
- error handler directive；
- dead-letter 成功、拒绝和失败；
- receive gap；
- settlement 失败；
- observer 恰好一次语义和 panic 隔离；
- subscription cancel 竞争；
- graceful/immediate shutdown；
- worker 自死锁检测；
- wait-for-idle 的 facade-local 语义。

### 并发验证

对 admission tracker、ordering lane、subscription cancel 和 shutdown 状态机增加 loom 模型测试。对 transport envelope 解码、provider metadata 和错误转换增加 fuzz targets。CI 至少运行 Linux 常规测试，并在可用时增加 macOS 和 Windows 的 local/provider contract 测试。

### 文档验证

- 所有公共 trait 和主要类型提供可运行 rustdoc 示例；
- 同步、异步、local、custom provider、encoded payload 各有完整示例；
- 中文和英文 README、用户指南、设计文档保持语义对应；
- 文档明确区分 provider acknowledgement、facade admission 和 handler completion；
- README 和用户指南在介绍 retry 高级配置时明确列出 `qubit-retry` 依赖，并使用 `qubit_retry::*` 路径；基础示例不添加无用的 retry import。

## 公开 API 稳定性

以下类型构成需要谨慎演进的稳定边界：

- `EventBus` / `AsyncEventBus`；
- `Topic<T>` / `EventEnvelope<T>` / 两类 request 及其 builder / `Delivery<T>`；
- `EventBusSpi` / `AsyncEventBusSpi`；
- 两类 subscription SPI；
- transport message 和 settlement 契约；
- capability 类型；
- provider spec 和 registry；
- 主要错误访问器。

公开错误 enum、capability enum 和 diagnostic enum 应使用 `#[non_exhaustive]`。SPI 输入结构优先使用私有字段、构造函数和访问器，避免新增字段成为破坏性修改。provider-specific 扩展通过 namespaced options 或独立扩展 trait，不向最小 SPI 持续添加方法。

## 从当前实现迁移

这是允许破坏性变更的重构，不提供旧 API 的长期兼容层。迁移原则如下：

| 当前 API | 目标 API |
| --- | --- |
| generic `EventBus` trait | 具体类型安全 `EventBus` facade |
| `EventBusFactory` | `EventBusRegistry` + `qubit-spi` provider |
| `LocalEventBus` | `EventBus::local` 或 local provider 创建的 `EventBus` |
| `LocalEventBusFactory` | `LocalEventBusConfig` + facade builder |
| `EventEnvelopeBuilder` | 以 `PublishRequestBuilder` 为主要发布构造入口 |
| handler 接收 `EventEnvelope<T>` | handler 接收 `Delivery<T>` |
| envelope 内 acknowledgement | delivery-only acknowledgement |
| `qubit-retry` 公开类型 | 保持直接使用 `qubit_retry::*`，不包装也不重导出 |
| 单一 `EventBusError` 大枚举 | 按操作分层的错误模型 |
| start/stop/restart | provider 创建即 Running，shutdown 后不可重启 |
| 错误指纹识别来源 | 内部 `PipelineFailure { origin, error }` |
| error observer + delivery observer | 统一 `Diagnostic`，保留便利过滤器 |

现有测试中描述有效语义的部分应先迁移为 facade 合约测试，再替换实现。测试文件按 publish、subscription、retry、interceptor、dead-letter、ordering、lifecycle、diagnostic 和 local SPI 拆分，避免继续扩展单个数千行测试文件。

## 0.12 实现与验证状态

本节记录仓库当前提供的验证范围，不代表每个 provider 都具备相同能力，也不表示所有 CI 套件已在每个开发检出中运行。

1. 集成测试覆盖内置 local provider 的同步 SPI 行为；运行 `cargo test --all-features` 可执行这些测试。
2. 集成测试包含 runtime-neutral fake async SPI，并覆盖异步 receive 取消边界；它验证 facade/SPI 合同，不是生产 async provider。
3. 测试辅助中有 channel-shaped SPI 和 broker-shaped fake，用于验证适配形状；crate 没有随包提供真实的 channel 或 broker adapter。
4. 同步与异步 facade、local provider、pipeline、registry、settlement 和并发合同分别由 crate 内测试覆盖；各 provider 仍需验证自身声明的 capability。
5. registry 的 provider 选择、创建期 capability 检查和 fallback 由集成测试覆盖；运行期错误不会触发 provider 切换。
6. retry 类型由应用直接依赖 `qubit-retry`；event-bus 不重新导出 retry 类型，也不公开本地 executor 实现。
7. 项目 CI 配置了测试、Clippy、严格 rustdoc、feature matrix、coverage 和可选高级验证套件。一次本地测试或打包结果不能替代远端 CI 对这些套件的完整结果。
8. 仓库包含 loom 并发合同测试和 fuzz targets；它们不等同于对真实第三方 provider 的压力或互操作验证。
9. README、用户指南和设计文档说明同步/异步边界、capability、settlement 与 publish receipt；文档修订应与实现及具体测试保持一致。

## 最终设计结论

`qubit-event-bus` 的核心价值不是把所有后端削成同一种消息队列，而是提供一套稳定的事件处理语义，并让每个后端诚实地报告自己能够保证什么。

为实现这一点：

- facade 负责类型安全和高级事件处理；
- SPI 只负责传输、接收、可选 settlement 和关闭；
- `qubit-spi` 负责 provider 的身份、选择、创建和创建阶段 fallback；
- capability 负责描述不可消除的后端差异；
- local provider 既是开箱即用的实现，也是 SPI 合约的基准实现；
- 第三方 provider 可以扩展后端能力，但不能改变公共 facade 的基础语义。
