# Qubit Event Bus 用户指南

本指南面向使用 `qubit-event-bus` 0.11 和 Rust 1.94+ 的应用开发者。示例采用 `LocalEventBus`；crate 同时提供供其他后端实现的 `EventBus` 与 `EventBusFactory` 契约。

## 概念模型

事件由类型化的 `Topic<T>`、`EventEnvelope<T>` 以及一个或多个匹配的订阅组成。发布调用先经过发布拦截，再执行订阅准入，随后把已接纳的 handler 工作提交到本地 worker 池。`PublishReceipt` 描述这次准入快照，不会等待 handler 完成。

`LocalEventBus` 明确不提供事务语义。`publish_all` 按输入 envelope 尽力提交；单个事件出错后仍会继续，并返回 `BatchPublishResult`。当前版本没有事务 staged-event 契约。

## 场景：记录订单事件

成功标准是审计订阅者收到订单，并且测试在退出前能够观察到 handler 的执行结果。

### 安装和启动

```toml
[dependencies]
qubit-event-bus = "0.11"
```

```rust
use qubit_event_bus::{LocalEventBus, Topic};

let bus = LocalEventBus::started()?;
let orders = Topic::<String>::try_new("orders.created")?;
```

`LocalEventBus::new()` 创建停止状态的 bus；调用 `start()` 启动，或者使用常见的 `started()` 完成创建并启动。

### 订阅和发布

```rust
use std::sync::{Arc, Mutex};

let received = Arc::new(Mutex::new(Vec::new()));
let captured = Arc::clone(&received);
bus.subscribe("audit-log", &orders, move |event| {
    captured.lock().expect("received events should lock").push(event.payload().clone());
    Ok(())
})?;

let receipt = bus.publish(&orders, "order-1001".to_string())?;
assert!(matches!(receipt.outcome(), qubit_event_bus::PublishOutcome::Dispatched(_)));
bus.wait_for_idle(&orders)?;
assert_eq!(received.lock().expect("received events should lock").as_slice(), &["order-1001".to_string()]);
```

订阅 handler 接收 `EventEnvelope<T>`，可以查看请求头、事件 ID、顺序键、延迟和 payload。通过 `LocalEventBus` 发布的 payload 必须满足 `Clone + Send + Sync + 'static`。

## 核心工作流

需要显式 envelope 元数据时使用 `publish_envelope`；需要发布重试或错误回调配置时使用 `publish_with_options` 或 `publish_envelope_with_options`。`subscribe_with_options` 可以增加确认模式、过滤器、优先级、重试、错误处理器和死信策略。

手动确认时，必须在返回前做出决定：

```rust
use qubit_event_bus::{AckMode, SubscribeOptions};

let options = SubscribeOptions::<String>::builder()
    .ack_mode(AckMode::Manual)
    .build();
bus.subscribe_with_options("manual-audit", &orders, |event| {
    event.acknowledgement().expect("manual ACK should be available").ack();
    Ok(())
}, options)?;
```

返回 `Ok(())` 却没有 ACK 或 NACK 会被视为 handler 失败，随后参与重试，再进入错误处理或死信流程。handler 返回后作出的确认无法改变这次投递。

## 批量发布与准入

`publish_all` 和 `publish_all_with_options` 按输入顺序提交 envelope。通过 `BatchPublishItem::result()` 查看每个事件的 `PublishReceipt` 或全局 `EventBusError`。

`BatchPublishResult` 提供三个有意区分的视图：

| 方法 | 含义 |
| --- | --- |
| `accepted_count()` | 至少有一个订阅状态为 `DispatchStatus::Accepted` 的输入项数量。 |
| `dropped_count()` | 被发布拦截器丢弃的项数量。 |
| `failure_count()` | 全局失败项，或包含任一被订阅者拒绝投递的项数量。 |

这些计数不互斥：一个项可以同时拥有已接纳和被拒绝的订阅，因此同时计入 `accepted_count()` 和 `failure_count()`。`accepted_count()` 不代表 handler 已成功完成。

## 容量和并发

在创建 bus 前配置本地准入上限和执行队列容量：

```rust
use qubit_event_bus::{DeliveryLimits, LocalEventBusFactory};

let mut factory = LocalEventBusFactory::new();
factory.set_delivery_limits(DeliveryLimits::bounded(4096, Some(128)))?;
factory.set_subscription_handler_pool_size(4)?;
let bus = factory.create_started()?;
```

`max_in_flight` 和提供时的 `handler_queue_capacity` 必须大于零。默认值是 `DeliveryLimits::default()`：4096 个已接纳的 in-flight 投递，且不显式限制 handler 队列容量。队列拒绝表现为 `DispatchStatus::Rejected(EventBusError::ExecutionRejected { .. })`，对应 handler 不会执行；需要监控执行失败时可使用 `add_error_observer`。

匹配的 handler 在订阅 worker 池中执行。相同 `ordering_key` 的事件会在每个 Topic 和订阅者内串行执行；没有顺序键的事件可以并发执行。重试退避会占用调用线程或 handler worker，因此应一并规划 worker 数量和重试预算。

## 拦截器、重试和死信

在 `LocalEventBusFactory` 上配置类型化或全局发布/订阅拦截器，再调用 `create()` 或 `create_started()`。`LocalEventBus` 不提供运行时修改拦截器的入口。

`RetryPolicy` 控制尝试次数和退避；重试规则负责分类失败，仅设置规则不会启用重试。订阅重试可通过 `SubscribeOptionsBuilder::retry_cancellation_token` 使用取消令牌；取消会唤醒退避并阻止下一次尝试，但不能打断已经运行的 handler。

死信策略可以设置在订阅选项或 factory 默认值中。`standard_dead_letters_to`、`prefixed_dead_letters` 和 `discard_dead_letters` 覆盖常见路由需求。`DeliveryFailure` 观察器会在重试、错误处理和死信路由结束后收到终态失败。

## 生命周期、错误和排障

- 对停止状态的 bus 发布或订阅会返回生命周期错误。`shutdown()` 会阻塞；在订阅 worker 中应使用 `shutdown_nonblocking()` 或 `shutdown_with_timeout()`。
- `wait_for_idle` 和 `wait_for_idle_timeout` 用于测试及受控排空。从 bus 自己的订阅 worker 调用会返回 `EventBusError::WouldDeadlock`。
- `shutdown_with_timeout` 报告超时后，旧订阅工作进入 idle 前，`start()` 仍会被拒绝。
- 发布成功表示完成了投递准入，不表示 handler 最终送达。需要关注丢失时，请检查回执状态并注册错误/投递失败观察器。
- 延迟投递到期时若队列拒绝，handler 不会执行；可通过 `add_error_observer` 观察 `ExecutionRejected`。

## 迁移说明

旧的事务 API（`TransactionalEventBus`、`TransactionalPublisher`、`StagedEvent` 和 `StagedEventEnvelope`）已删除。只有在能够接受 best-effort、非原子语义时才用普通 `publish_all` 替代；否则应由应用或具体后端自行协调事务。

`DeliveryLimits` 取代单一的 in-flight 调优入口。使用 `DeliveryLimits::bounded(max_in_flight, handler_queue_capacity)` 或 `DeliveryLimits::unbounded(max_in_flight)` 配合 `LocalEventBusFactory::set_delivery_limits`；零值会被拒绝。

使用 `BatchPublishResult::accepted_count()` 的代码需要按当前语义处理：它表示订阅准入，不是输入项数量、已完成 handler 数量或所有订阅成功的数量。拒绝详情应结合 `failure_count()` 和每个回执的状态读取。

泛型 `EventBus` 实现通过关联类型暴露后端自有的 `Subscription<T>`，并约束为实现 `SubscriptionHandle<T>`。泛型代码应使用 `B::Subscription<T>`，不要写死本地具体的 `Subscription<T>`。

## 延伸阅读

- [API 文档](https://docs.rs/qubit-event-bus)
- [English user guide](user_guide.md)
- [设计说明](design.zh_CN.md)
- [Design guide](design.md)
