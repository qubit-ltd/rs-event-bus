# Qubit Event Bus 用户指南

本指南对应 `qubit-event-bus` 0.12 和 Rust 1.94 及以上版本，面向希望获得类型安全事件分发、又不想让业务代码绑定某种传输方式的 Rust 开发者。crate 自带同步进程内 provider；其他传输需要由独立的 provider 适配器实现。

[English user guide](user_guide.md) · [中文 README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-event-bus)

## 手册目标与能力边界

假设订单服务在接收订单后要通知审计 handler。事件总线为发布方和订阅方提供共享的类型化 Topic，并用回执报告准入结果。若调用方要观察 handler 是否执行，可以等待 facade 跟踪的工作完成。内置 local provider 适用于进程内分发；它不是消息代理，不持久化消息，也不负责跨进程路由。

facade 将应用策略与具体传输分开：同步和 runtime-neutral 异步 API 位于对象安全的 `EventBusSpi`、`AsyncEventBusSpi` 之上。本 crate 目前只内置同步 local provider，不包含 Tokio、crossbeam、flume、RabbitMQ、Kafka 或 Redis 适配器。

## 概念模型

- `Topic<T>` 将经过校验的主题名称绑定到 Rust payload 类型。
- `PublishRequest<T>` 包含 Topic、payload、envelope 元数据和发布策略。`new(topic, payload)` 使用默认选项并生成事件 ID；builder 可设置 headers、顺序键、延迟、重试和拦截器。
- `SubscribeRequest<T>` 包含 `SubscriberId`、Topic 和 `SubscribeOptions<T>`。`new(subscriber_id, topic)` 使用默认选项；builder 可配置 ACK、过滤器、中间件、重试、死信策略和 provider 专属命名空间选项。
- `Delivery<T>` 提供事件、投递上下文和 ACK/NACK 句柄；它不是传输层 settlement token。
- `PublishReceipt` 返回 provider 身份及准入确认，不表示 handler 已完成。
- provider SPI 传输类型擦除后的 payload 并接收 provider 专属订阅请求。`qubit-spi` registry 负责选择和创建 provider。

订阅中间件分为同步和异步两种。一个 `SubscribeRequest` 可以保存其中任一种或两种，但同步 `EventBus` 遇到异步中间件、异步 `AsyncEventBus` 遇到同步中间件时，都会以配置错误拒绝建立订阅。这避免了在异步 executor 中阻塞，也避免把同步回调伪装成可 await 的函数。

## 场景：在本地记录订单事件

本例的成功标准是审计订阅者收到 `order-1001`，并且调用方能在退出前确认 handler 已执行。

### 安装与创建总线

```toml
[dependencies]
qubit-event-bus = "0.12"
```

```rust
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::EventBus;

let bus = EventBus::local(LocalEventBusConfig::default())?;
```

local provider 默认限制每个订阅最多 1024 条未终结消息；队列中的消息和已接收但尚未 settlement 的消息都会占用容量，`Retry` 会将原消息放回队列并保留该额度。可通过 `LocalEventBusConfig::new().queue_capacity(n)` 设置其他正数上限。

### 订阅、发布并检查结果

```rust
use std::sync::{Arc, Mutex};

use qubit_event_bus::model::{PublishRequest, SubscribeRequest, Topic};
use qubit_event_bus::{DeliveryError, SubscriberId};

let orders = Topic::<String>::new("orders.created")?;
let received = Arc::new(Mutex::new(Vec::new()));
let captured = Arc::clone(&received);
let request = SubscribeRequest::new(SubscriberId::new("audit-log")?, orders.clone());
let subscription = bus.subscribe(request, move |delivery| {
    captured.lock().expect("received events should lock").push(delivery.payload().clone());
    Ok::<(), DeliveryError>(())
})?;

let receipt = bus.publish(PublishRequest::new(orders.clone(), "order-1001".to_owned())?)?;
assert_eq!(receipt.provider_id().as_str(), "local");
bus.wait_for_idle(&orders, None)?;
assert_eq!(received.lock().expect("received events should lock").as_slice(), &["order-1001"]);
subscription.cancel()?;
```

`wait_for_idle` 会询问 provider 该 Topic 是否仍有排队或尚未 settlement 的消息；它不表示 handler 成功，也不代表远端 broker 全局空闲。不支持此查询的 provider 会返回 `LifecycleError::IdleWaitUnsupported`。如需等待当前 facade 已接收并跟踪的工作，请用 `wait_for_received_deliveries`。

### 根据发布回执决定后续处理

`publish` 返回 `Ok(receipt)` 表示 provider 已返回接纳回执。应用应先检查 acknowledgement，再决定是否需要业务补偿：

```rust
use qubit_event_bus::model::{AdmissionStatus, DestinationAdmission, PublishAcknowledgement, PublishReceipt};

let receipt: PublishReceipt = todo!("使用 EventBus::publish 返回的回执");
match receipt.acknowledgement() {
    PublishAcknowledgement::Accepted { .. } => {
        // broker 已接纳事件；它可能不公开消费者身份。
    }
    PublishAcknowledgement::DroppedByInterceptor => {
        // 拦截器有意停止了分发。
    }
    PublishAcknowledgement::DestinationAdmissions(destinations) => {
        if destinations.is_empty() {
            // provider 没有报告目的地，例如当前没有本地订阅者。
        }
        for destination in destinations {
            match destination.status() {
                AdmissionStatus::Accepted => record_admission(destination),
                AdmissionStatus::Filtered => record_filtered(destination),
                AdmissionStatus::Rejected(reason) => record_rejection(destination, reason),
                _ => record_unknown_status(destination),
            }
        }
    }
    _ => record_unknown_acknowledgement(),
}

fn record_admission(_: &DestinationAdmission) {}
fn record_filtered(_: &DestinationAdmission) {}
fn record_rejection(_: &DestinationAdmission, _: &str) {}
fn record_unknown_status(_: &DestinationAdmission) {}
fn record_unknown_acknowledgement() {}
```

以上 `record_*` 函数代表应用自己的策略。本地 provider 可能因某个有界队列已满而接纳一个订阅者、拒绝另一个；这种部分结果仍然是成功回执。`Filtered` 表示有意排除，不代表队列满。部分接纳后不要盲目重发整条事件，否则已接纳的订阅者可能收到重复投递。需要时使用幂等键，或通过显式补偿/重试策略处理被拒绝的业务工作。

`receipt.check_admission(requirement)` 是对已返回回执的发布后检查。它不会再次发布，也不会等待 handler。`AdmissionRequirement::AtLeastOneAccepted` 要求至少一个报告的目的地接纳；`AtLeastOneAcceptedAndNoRejected` 还要求拒绝数为零。因此部分接纳可以使更严格的检查失败，即使已有目的地接纳了事件。不要把这个错误当作重发整条事件的信号。

两种 facade 都提供 `publish_metrics()`。`PublishMetricsSnapshot` 的字段为 `attempts`、`errors`、`dropped`、`opaque_accepted`、`zero_destinations`、`accepted_destinations`、`filtered_destinations` 和 `rejected_destinations`。这些由 facade clone 共享的饱和计数器分别读取；并发发布时，一个快照的字段可能对应略有差异的时刻。它们统计 provider 准入，不表示 handler 完成或业务成功。provider 隐藏目的地的 `Accepted` 回执会增加 `opaque_accepted`，但不会披露接纳了多少目的地。

## 核心流程与策略

### 添加事件元数据

要设置 envelope 字段但不手动创建 envelope 时，使用 request builder：

```rust
use std::time::Duration;
use qubit_event_bus::model::{PublishRequest, Topic};

let request = PublishRequest::builder()
    .topic(Topic::<String>::new("orders.created")?)
    .payload("order-1002".to_owned())
    .header("trace-id", "trace-42")
    .ordering_key("customer-7")
    .delay(Duration::from_millis(25))
    .build()?;
```

`x-qubit-event-bus-dead-letter` 是 facade 保留的 header。应用和拦截器都不能设置或删除它；provider 传输事件时必须保留该标记。

### 确认、重试与错误处理

自动确认模式会在 handler 成功返回后 ACK。使用 `AckMode::Manual` 时，handler 必须在返回前调用 `delivery.acknowledgement().ack()` 或 `.nack()`；未作决定属于投递失败。

重试类型直接来自 `qubit-retry`，`qubit-event-bus` 不会重新导出它们。配置重试时，应用应显式依赖两个 crate：

```toml
[dependencies]
qubit-event-bus = "0.12"
qubit-retry = "0.25"
```

在 builder/options 中直接使用 `qubit_retry::RetryPolicy`，必要时实现或传入 `qubit_retry::RetryRule`。重试分类规则本身不会启用重试：还必须提供 retry policy。取消令牌会阻止下一次尝试，但无法中断正在运行的同步 handler。

`FailureDirective` 在配置策略和 provider 能力允许的范围内选择重试请求、重新入队、死信或丢弃等处理。异步取消发生在 provider 可能已经接纳死信、但发布 future 尚未返回时，runner 恢复后可能再次发布同一死信。因此该路径按至少一次处理；需要去重时应基于事件 ID 或业务幂等键实现。

facade 生成的死信 payload 类型为 `model::DeadLetterEvent<T>`。消费者可用该类型订阅配置的死信 topic，并读取原事件、失败的 `SubscriberId` 和终态错误文本。原始 `EventEnvelope<T>` 通过 `Arc` 共享，`T` 不需要实现 `Clone`。使用编码传输的 provider 需要为 `DeadLetterEvent<T>` 注册 codec。

`PublishFailureContext<T>` 是发布终态错误 handler 读取事件信息的视图。它共享 payload，并保留事件 ID、Topic、headers、顺序键、时间戳和延迟，因此 `T` 不需要实现 `Clone`。如果没有 retry policy，SPI publish 直接失败也会触发该 handler；配置了 retry policy 时，则在重试进入终态失败后触发。请求构建、能力检查、编解码和 interceptor 等预检错误不会经过该 handler。

### 同步与异步中间件

`SubscriberInterceptor<T>` 是同步中间件，接收 continuation；`AsyncSubscriberInterceptor<T>` 返回 crate 提供的 runtime-neutral boxed future，并接收异步 continuation。中间件按注册顺序执行；不调用 continuation 即表示短路内层中间件和 handler。同步总线若配置异步中间件，或异步总线若配置同步中间件，会在建立订阅时返回配置错误。

## 选择 provider

内置同步 provider 可通过 `EventBus::local(LocalEventBusConfig)` 直接使用。需要显式选择时，创建 `EventBusRegistry`、注册 provider definition，可选设置 `qubit_spi::ProviderSelection`，再将 `EventBusConfig` 传给 `create`。`EventBusRegistry::with_local()` 注册 canonical ID 为 `local`、别名为 `memory` 和 `in-process` 的内置 provider。

`qubit-spi` 的 fallback 只发生在创建后端的阶段。运行期间 publish/receive 失败时不会静默切换传输。`RequiredCapabilities` 可在创建阶段拒绝不满足 durability、settlement、ordering、replay、delay、payload mode 或 publish visibility 要求的 provider。Provider options 是由适配器解释的命名空间键值数据；由于配置可调试输出，不要把凭据放进去。

异步 facade 不绑定某个运行时，也不会替调用方启动消费 task。通过 `AsyncEventBusRegistry` 创建后端特定的异步 provider，再由应用 executor 驱动 `AsyncSubscription`。`AsyncEventBus::new` 默认使用 `qubit-clock` 提供的标准单调 timer；需要替换时可用 `with_timer` 或 `with_config_and_timer` 注入其他 `qubit_clock::Timer`。空闲等待、优雅停机 deadline 和异步重试退避都依赖 timer future 在 deadline 到达时唤醒 executor；event-bus 不会另建计时线程。

```rust
use qubit_event_bus::model::{SubscribeRequest, Topic};
use qubit_event_bus::{AsyncEventBus, DeliveryError, SubscriberId};

async fn consume(bus: &AsyncEventBus) -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.created")?;
    let request = SubscribeRequest::new(SubscriberId::new("audit-log")?, topic);
    let mut subscription = bus.subscribe(request).await?;
    subscription.run(|delivery| async move {
        audit(delivery.payload()).await?;
        Ok::<(), DeliveryError>(())
    }).await?;
    Ok(())
}

async fn audit(_order: &str) -> Result<(), DeliveryError> { Ok(()) }
```

此函数假设 `bus` 已由异步 provider adapter 创建。本 crate 没有内置 async local provider，也不附带第三方传输适配器。

## 错误与诊断

错误按操作区分为 `PublishError`、`SubscribeError`、`ReceiveError`、`LifecycleError` 和 `ShutdownError`；SPI/provider 错误尽可能保留 source 链。检查发布回执中的 acknowledgement；若需要观察 provider gap、不支持的 disposition 或回调故障，可注册 `observe_diagnostics`。从本总线自己的同步 worker 调用 `wait_for_idle`、`wait_for_received_deliveries` 或 shutdown 会得到 `WouldDeadlock`，不会让 worker 等待自己。

`Subscription::cancel` 会显式停止订阅；从外部调用时会等待同步 worker 收尾。丢弃同步 handle 本身不会取消订阅。异步订阅使用 `AsyncSubscription::close().await` 确定性释放资源并返回 close 错误。丢弃异步订阅 handle 会立即丢弃暂停的 session 和 provider receiver；provider 必须在 receiver 被丢弃时恢复未结算 delivery。`run` 不会 spawn；丢弃其 future 会暂停并保留在途 delivery future 与 permit。再次调用 `run` 会续跑这些旧 future（新 handler 只处理之后收到的消息），bus shutdown 也能接管暂停 session。异步 receiver 的 close/drop 契约必须保证未结算 delivery 仍可恢复，并且绝不能隐式确认。

## 限制与最佳实践

- publish 成功表示 provider 返回了准入确认，不表示订阅 handler 已完成。
- 请求 `OrderingPolicy::PerKey` 的订阅只有在 provider 声明 `OrderingCapability::PerKey` 或 `PerSubscription` 时才会建立。旧订阅 `priority` 设置已删除，因为它不会影响调度。保留同步 `Subscription` 句柄并显式调用 `cancel()`；丢弃句柄不会停止其 worker。
- `publish_all` 按输入顺序分别尝试请求，并保留各自结果；它不是原子操作。
- local 队列容量限制每个订阅的排队及尚未 settlement 消息数；消息被接收后仍占用额度，直到 settlement。这不是全局 broker 配额。local provider 仅在进程内工作，不提供持久性。
- 两种 facade 都有 bus-wide 准入上限。同步 facade 用 `with_sync_delivery_scheduler(...)` 配置 `max_in_flight` 与 handler queue capacity；异步 facade 用 `EventBusFacadeConfig::with_delivery_admission(DeliveryAdmissionConfig::new(max_in_flight)?)` 配置（默认 4）。异步 permit 覆盖每条已接收消息的 lane wait、中间件、handler/retry 及最终 settlement；不同顺序键可并行，同一顺序键保持顺序。每个订阅最多暂存一条尚未准入的消息；空闲的 receive 不占用 permit。
- 诊断 observer 在触发诊断的线程上同步调用。observer panic 会被隔离，但阻塞的 observer 会延迟该线程；诊断不会进入独立缓冲队列。
- 能力标志是 provider 对自身契约的声明。按需要求能力，并单独记录具体 provider 的增强保证。
- 异步 settlement 的不确定结果可能导致重试；provider 必须让相同 token 和 disposition 的重复操作幂等，同一 token 使用冲突 disposition 时必须失败。
- shutdown 会停止准入并协调订阅。`Immediate` 会丢弃尚未开始的队列工作，不再接收新消息；但会等待当前 handler 完成、settlement、subscription close 和 SPI shutdown，以便同步返回完整错误。Rust 无法强制中断 handler。同步 `Graceful` 的期限限制调用方等待完整关闭过程的时间，包括已准入的 publish/subscribe SPI 调用、receiver close 和 provider shutdown。返回 `TimedOut` 时 bus 保持 `Closing`、拒绝新操作，并由唯一后台协调者继续清理；再次调用 shutdown 可继续等待，或用 `Immediate` 加强当前尝试。阻塞中的同步 provider 调用或 handler 可能让协调线程持续存在。异步 shutdown 由调用者驱动 future；丢弃超时/取消的 future 不会回滚 provider 副作用，因此异步 provider 必须支持幂等 close/shutdown 重试。SPI 的 `shutdown(mode)` 只关闭 provider 传输资源；先停止消费、完成当前投递和 close 的顺序由 facade 保证。

## 延伸阅读

- [中文 README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-event-bus)
- [架构设计（中文）](design.zh_CN.md) · [正式 SPI 设计（中文）](spi_design.zh_CN.md) · [SPI design (English)](spi_design.md)
- [中文更新日志](../CHANGELOG.zh_CN.md) · [English user guide](user_guide.md)
