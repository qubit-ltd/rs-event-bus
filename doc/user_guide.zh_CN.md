# Qubit Event Bus 用户手册

[English user guide](user_guide.md) · [中文 README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-event-bus)

本文适用于 `qubit-event-bus` 0.12 和 Rust 1.94 及以上版本，面向需要让多个进程内任务响应同一业务事件的 Rust 开发者。订单服务创建订单后，若直接调用审计写入和客户视图更新，订单流程就得了解两套实现及其失败处理。本库让订单流程发布带类型的事件，由各任务自行订阅。内置 provider 只处理单进程内的消息，不持久化事件。

## 场景与验收目标

订单服务已接受 `order-1001`，审计记录和客户视图都需要收到它。下面先为 `orders.created` 注册两个订阅者，再发布一次事件，等待本地待处理消息结算，最后分别检查两项效果。以后增加进程内任务时，只需增加订阅者，订单发布代码不用改。

示例用内存数组模拟两项效果，便于直接运行。接入实际服务时，应换成真实的业务操作，并明确重复投递和失败时的处理方式。本库不把订单数据库事务与事件发布合并成原子操作。

## 先理解几个对象

| 对象 | 在订单场景中的作用 |
| --- | --- |
| `Topic<T>` | 将 `orders.created` 与 Rust 载荷类型绑定；两个订阅者共用 `Topic<String>`。 |
| `SubscribeRequest<T>` | 指明订阅者和主题；返回的 `Subscription` 要保留，并显式取消。 |
| `PublishRequest<T>` | 装入一次事件及其发布选项。 |
| `PublishReceipt` | 记录 provider 是否接纳事件，不表示 handler 已完成。 |
| `EventBus` 与 local provider | facade 处理通用策略；provider 向本进程的订阅者分发消息。 |

## 安装并运行

应用需要 Rust 1.94 或以上版本。在 `Cargo.toml` 中添加：

```toml
[dependencies]
qubit-event-bus = "0.12"
```

将以下代码保存为 `src/main.rs`，运行 `cargo run`：

```rust
use std::sync::{Arc, Mutex};

use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::{PublishRequest, SubscribeRequest, Topic};
use qubit_event_bus::{EventBus, SubscriberId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建进程内总线，两个订阅者共用同一个带类型的 Topic。
    let bus = EventBus::local(LocalEventBusConfig::default())?;
    let orders = Topic::<String>::new("orders.created")?;
    let audit = Arc::new(Mutex::new(Vec::new()));
    let view = Arc::new(Mutex::new(Vec::new()));

    // 审计和客户视图独立订阅，发布方无需知道它们的实现。
    let audit_log = Arc::clone(&audit);
    let audit_subscription = bus.subscribe(
        SubscribeRequest::new(SubscriberId::new("audit-log")?, orders.clone()),
        move |delivery| {
            audit_log.lock().unwrap().push(delivery.payload().clone());
            Ok::<(), qubit_event_bus::DeliveryError>(())
        },
    )?;
    let customer_view = Arc::clone(&view);
    let view_subscription = bus.subscribe(
        SubscribeRequest::new(SubscriberId::new("customer-view")?, orders.clone()),
        move |delivery| {
            customer_view.lock().unwrap().push(delivery.payload().clone());
            Ok::<(), qubit_event_bus::DeliveryError>(())
        },
    )?;

    // 只发布一次；回执说明 provider 的接纳结果，不说明处理已成功。
    let receipt = bus.publish(PublishRequest::new(orders.clone(), "order-1001".to_owned())?)?;
    assert_eq!(receipt.provider_id().as_str(), "local");
    // 等待本地待处理消息结算，再检查两个业务效果。
    bus.wait_for_idle(&orders, None)?;
    assert_eq!(audit.lock().unwrap().as_slice(), &["order-1001"]);
    assert_eq!(view.lock().unwrap().as_slice(), &["order-1001"]);
    // 显式取消订阅，并关闭总线以释放 worker 和 provider 资源。
    audit_subscription.cancel()?;
    view_subscription.cancel()?;
    bus.shutdown(qubit_event_bus::spi::ShutdownMode::Graceful {
        timeout: std::time::Duration::from_secs(2),
    })?;
    Ok(())
}
```

程序先创建两个订阅，再发布事件。`wait_for_idle` 等待本地 provider 在该 Topic 上没有排队或尚未结算的消息；后面的断言才验证业务效果。仅仅等待空闲，不能证明 handler 成功。`cancel()` 停止同步订阅的 worker，`shutdown` 关闭总线。不要从本总线自己的同步 handler 内调用这些会阻塞的生命周期方法；它们可能返回 `LifecycleError::WouldDeadlock`。

## 有订阅者未接纳事件时

`publish` 成功返回的只是 provider 的接纳报告，里面仍可能有被拒绝的目的地。使用 local provider 时，先看 `receipt.admission_outcome()`；需要知道具体订阅者的结果，再看 `receipt.acknowledgement()`。结果可能是全部接纳、部分接纳、全部未接纳、没有目的地、被拦截器丢弃，或 provider 不公开目的地的接纳。`receipt.check_admission(requirement)` 仅检查已有回执，不会再次发布，也不会等待消费者。

假设审计订阅者已接纳，而客户视图订阅者因队列已满被拒绝，重发整条事件可能重复写入审计记录。应记录事件 ID 或业务幂等键，决定如何补偿被拒绝的工作，并让 handler 能安全处理所选重试策略。`Filtered` 表示按规则主动过滤，不是队列压力。`PublishError` 与“返回了部分接纳回执”是不同情况；重试前先看错误来源和可能已发生的接纳。

## 策略与 provider 选择

- 需要 header、顺序键、延迟、拦截器或重试选项时，使用请求 builder。`x-qubit-event-bus-dead-letter` 是 facade 保留的 header。配置重试时还需直接依赖 `qubit-retry = "0.25"`；只有错误分类规则不会启动重试。
- 默认自动确认会接纳成功的 handler 结果。使用 `AckMode::Manual` 时，handler 必须调用 `delivery.acknowledgement().ack()` 或 `.nack()`；直接返回且未作决定会成为投递失败。死信需要配置合适的 Topic；编码型 provider 还需要 `DeadLetterEvent<T>` 的 codec。
- `EventBus::local(LocalEventBusConfig::default())` 是内置路径。`EventBusRegistry::with_local()` 注册同一个 provider，ID 为 `local`，别名为 `memory` 和 `in-process`。`RequiredCapabilities` 在创建时检查 provider 声明的能力；registry fallback 只发生在创建阶段，不会在运行时故障后自动切换。
- `AsyncEventBus` 不绑定运行时，但本库没有内置 async local provider 或 broker 适配器。应用需要提供异步 SPI，并在自己的 executor 上驱动 `AsyncSubscription::run`；`run` 不会自行 spawn 任务。

## 错误与排障

| 现象 | 检查与处理 |
| --- | --- |
| `PublishError` | 检查请求校验、codec/能力要求、provider 错误来源；重试前确认是否已有目的地接纳。 |
| 回执为 `NoDestinations` 或 `NoneAccepted` | 确认订阅存在且仍有效，并查看目的地结果中的过滤或拒绝原因。 |
| `LifecycleError::IdleWaitUnsupported` | 所选 provider 无法报告 Topic 的整体空闲状态；`wait_for_received_deliveries` 只跟踪本 facade 已收到的工作。 |
| 等待空闲后仍缺少业务效果 | 查看 handler 结果和应用日志；空闲只表示工作已结算。可注册 `observe_diagnostics` 观察 provider 缺口、回调或 disposition 故障。 |
| 关闭超时 | 同步 graceful shutdown 限制调用方等待时间；总线继续拒绝新工作，后台清理继续进行。再次调用 shutdown 可获取最终结果。 |

错误按操作区分，包括 `PublishError`、`SubscribeError`、`LifecycleError` 和 `ShutdownError`。诊断 observer 在触发诊断的线程上同步运行，回调应保持简短。`publish_metrics()` 统计接纳结果，不统计 handler 的业务成功次数。

## 本地 provider 资源指南

内置 provider 同步、只在进程内工作且不持久化。每个同步订阅都有一个阻塞式接收 worker 线程。`LocalEventBusConfig::new().queue_capacity(n)` 为**每个订阅者**设置正数的未完成消息上限（默认 1024），排队和已接收但尚未结算的消息都占用额度；重试保留原额度。facade 调度上限属于另一层，任何一个上限都不能单独代表总内存预算。订阅数量和队列容量应按实际负载规划。可用 `cargo bench --bench local_threads` 与 `cargo bench --bench local_scale` 在自己的机器测量，结果不是跨机器保证。

`publish_all` 对每个请求分别尝试，不提供事务语义。本地 provider 无法持久恢复或跨进程投递。provider 必须声明相应的顺序、结算能力，facade 才能使用；`OrderingPolicy::PerKey` 要求 `PerKey` 或 `PerSubscription` 顺序能力。如果业务要求持久移交或数据库与事件的原子提交，应另行设计该机制并选用合适的 provider。

## 延伸阅读

- [中文 README](../README.zh_CN.md) · [English user guide](user_guide.md) · [API 文档](https://docs.rs/qubit-event-bus)
- [架构设计](design.zh_CN.md) · [SPI 设计](spi_design.zh_CN.md) · [更新日志](../CHANGELOG.zh_CN.md)
