# Qubit Event Bus 用户手册

[English user guide](user_guide.md) · [中文 README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-event-bus)

本文适用于 `qubit-event-bus` 0.13 和 Rust 1.94 及以上版本，面向需要让多个进程内任务响应同一业务事件的 Rust 开发者。订单服务创建订单后，若直接调用审计写入和客户视图更新，订单流程就得了解两套实现及其失败处理。本库让订单流程发布带类型的事件，由各任务自行订阅。内置 local providers 只处理单进程内的消息，不持久化事件；不能用它单独保证审计记录或客户视图必然更新。

## 场景与验收目标

订单事务提交成功后，订单服务把 `OrderCreated { order_id, customer_id, total_cents }` 发布到 `orders.created`。审计订阅者写审计记录，客户视图订阅者更新查询视图。两者都订阅 `Topic<OrderCreated>`，发布方不依赖它们的实现。下面先用进程内集合模拟两处存储，以验证两个订阅者都收到事件；接入数据库时，应由应用定义存储失败处理和幂等策略。订单提交与事件发布是两个独立操作，进程退出或发布失败可能使已提交订单没有对应事件。如果业务要求可靠审计或最终必达，需要另行设计持久移交与补偿机制。

## 先理解几个对象

| 对象 | 在订单场景中的作用 |
| --- | --- |
| `Topic<T>` | 将 `orders.created` 与 Rust 载荷类型绑定；两个订阅者共用 `Topic<OrderCreated>`。 |
| `SubscribeRequest<T>` | 指明订阅者和主题；返回的 `Subscription` 要保留，并显式取消。 |
| `PublishRequest<T>` | 装入一次事件及其发布选项。 |
| `PublishReceipt` | 记录 provider 是否接纳事件，不表示 handler 已完成。 |
| `EventBus` 与 local provider | facade 处理通用策略；provider 向本进程的订阅者分发消息。 |

## 安装与接入

应用需要 Rust 1.94 或以上版本。在 `Cargo.toml` 中添加：

```toml
[dependencies]
qubit-event-bus = "0.13"
```

下面的完整示例可放入依赖本库的应用的 `src/main.rs`，用集合模拟审计记录和客户视图。实际订单服务应先完成数据库事务，再执行示例中的发布步骤：

```rust
use std::sync::{Arc, Mutex};
use std::time::Duration;

use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::{AdmissionRequirement, PublishRequest, SubscribeRequest, Topic};
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::{EventBus, WaitOutcome};

struct OrderCreated {
    order_id: String,
    customer_id: String,
    total_cents: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = EventBus::local(LocalEventBusConfig::default())?;
    let topic = Topic::<OrderCreated>::new("orders.created")?;
    let audit_log = Arc::new(Mutex::new(Vec::<String>::new()));
    let customer_view = Arc::new(Mutex::new(Vec::<(String, String, u64)>::new()));

    let audit_store = Arc::clone(&audit_log);
    let audit_request = SubscribeRequest::new("audit-log", topic.clone())?;
    let audit = bus.subscribe(audit_request, move |delivery| {
        audit_store.lock().unwrap().push(delivery.payload().order_id.clone());
    })?;

    let view_store = Arc::clone(&customer_view);
    let view_request = SubscribeRequest::new("customer-view", topic.clone())?;
    let view = bus.subscribe(view_request, move |delivery| {
        let event = delivery.payload();
        view_store.lock().unwrap().push((
            event.order_id.clone(),
            event.customer_id.clone(),
            event.total_cents,
        ));
    })?;

    // 真实服务应在订单数据库事务提交成功后才执行这一发布步骤。
    let event = OrderCreated {
        order_id: "order-42".to_owned(),
        customer_id: "customer-7".to_owned(),
        total_cents: 1299,
    };
    let receipt = bus.publish(PublishRequest::new(topic.clone(), event)?)?;
    receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected)?;

    assert_eq!(bus.wait_for_idle(&topic, Some(Duration::from_secs(2)))?, WaitOutcome::Idle);
    assert_eq!(*audit_log.lock().unwrap(), ["order-42".to_owned()]);
    assert_eq!(
        *customer_view.lock().unwrap(),
        [("order-42".to_owned(), "customer-7".to_owned(), 1299)],
    );

    audit.cancel()?;
    view.cancel()?;
    bus.shutdown(ShutdownMode::Graceful { timeout: Duration::from_secs(2) })?;
    Ok(())
}
```

`EventBus::local` 创建内置本地总线；两个 handler 分别写入演示用集合。`check_admission` 要求至少一个目的地接纳且没有拒绝，过滤掉的目的地不计作拒绝；`wait_for_idle` 等待此 Topic 的本地投递结算，然后示例检查集合内容，最后取消订阅并关闭总线。真实应用应在启动时注册并持有订阅，把集合替换为自己的存储实现，并从总线 worker 之外执行取消和关闭。回执与空闲状态都不能单独证明业务写入成功；生产环境还应记录 handler 的失败并安排重试或补偿。

### 发现 provider 并装配共享总线

使用发现功能时，为 `qubit-event-bus` 启用 `discovery` feature，并直接依赖 `qubit-spi`：

```toml
qubit-event-bus = { version = "0.13", features = ["discovery"] }
qubit-spi = "0.13"
```

内置 `local` 会出现在同步目录。以下是应用启动装配片段；`OrderService` 由应用定义：

```rust
use qubit_event_bus::{EventBusConfig, EventBusRegistry};
use qubit_spi::ProviderSelection;

let registry = EventBusRegistry::discover()?;
registry.set_default_selection(ProviderSelection::named("local")?)?;
registry.seal();
let bus = registry.create(&EventBusConfig::default())?;
let orders = OrderService::new(bus.clone());
```

inventory 在链接期收集最终可执行程序中的 provider 提交。外部 provider 需要成为应用依赖，并在装配模块用 `use provider_crate as _;` 显式链接。`discover()` 遇到重复 selector 会返回带提交来源的错误。若不确定已链接的 provider，可先查看 `provider_ids()`；空目录无法解析创建候选。应用应持有共享总线及订阅句柄，停机时取消订阅并关闭总线。

## 有订阅者未接纳事件时

`publish` 成功返回的只是 provider 的接纳报告，里面仍可能有被拒绝的目的地。使用 local provider 时，先看 `receipt.admission_outcome()`；需要知道具体订阅者的结果，再看 `receipt.acknowledgement()`。结果可能是全部接纳、部分接纳、全部未接纳、没有目的地、被拦截器丢弃，或 provider 不公开目的地的接纳。`receipt.check_admission(requirement)` 仅检查已有回执，不会再次发布，也不会等待消费者。

如果需要定位被拒绝的订阅者，可在上述 `bus.publish(...)` 之后、`check_admission(...)` 之前检查回执：

```rust
use qubit_event_bus::model::{AdmissionStatus, PublishAcknowledgement};

if let PublishAcknowledgement::DestinationAdmissions(destinations) = receipt.acknowledgement() {
    for destination in destinations {
        if let AdmissionStatus::Rejected(reason) = destination.status() {
            eprintln!("{} 未接纳事件：{reason}", destination.subscriber_id().as_str());
        }
    }
}
```

例如审计订阅者已接纳，而客户视图订阅者因队列已满被拒绝，重发整条事件可能重复写入审计记录。应记录事件 ID 或业务幂等键，针对未接纳的工作制定补偿方案，并让 handler 能安全处理可能重复的事件。`Filtered` 表示按规则主动过滤，不是队列压力。`PublishError` 与“返回了部分接纳回执”是不同情况；重试前先看错误来源和可能已发生的接纳。

## 策略与 provider 选择

- 需要 header、顺序键、延迟、拦截器或重试选项时，使用请求 builder。`x-qubit-event-bus-dead-letter` 是 facade 保留的 header。配置重试时还需直接依赖 `qubit-retry = "0.25"`；只有错误分类规则不会启动重试。
- 默认自动确认会接纳成功的 handler 结果。使用 `AckMode::Manual` 时，handler 必须调用 `delivery.acknowledgement().ack()` 或 `.nack()`；直接返回且未作决定会成为投递失败。死信需要配置合适的 Topic；编码型 provider 还需要 `DeadLetterEvent<T>` 的 codec。
- `EventBus::local(LocalEventBusConfig::default())` 是内置路径。`EventBusRegistry::with_local()` 注册同一个 provider，ID 为 `local`，别名为 `memory` 和 `in-process`。`RequiredCapabilities` 在创建时检查 provider 声明的能力；registry fallback 只发生在创建阶段，不会在运行时故障后自动切换。
- `AsyncEventBus` 不绑定运行时。可用 `AsyncEventBus::local(LocalEventBusConfig::default()).await` 或 `AsyncEventBusRegistry::with_local()` 创建内置异步 local provider；应用仍需在自己的 executor 上驱动 `AsyncSubscription::run`。

编码型 payload 优先使用 `Topic<T>` 自带的 codec；Topic 未配置时才回退到 facade 的 `CodecRegistry`。创建订阅时会固定所选 codec。编码型 provider 在两处都没有 codec 时，会在调用 provider 的 `subscribe` 前返回错误。

任务状态变更不能等待同步发布时，可使用 `NotificationPublisher<T>` 和正数有界容量（默认 256）。`try_publish` 因容量已满或 publisher 已关闭而无法入队时，会把原 payload 随 `Full` 或 `Closed` 返回。`close` 停止入队、排空已入队通知并等待 worker，不会关闭注入的 bus。observer 收到请求构造和 provider 接纳结果，不表示 handler 已处理；observer 在 publisher worker 上同步运行。

`AsyncEventBusRegistry::discover()` 读取独立的异步目录；同步 `local` 不会出现在其中。`AsyncEventBusRegistry::with_local()` 会在该 catalog 注册 async local。异步 provider 在创建时通过 `registry.create(&config).await` 创建。不启用 `discovery` 时，应用仍可用 `new()` 创建任一种 registry，并用 `register()` 显式注册。发现阶段只收集定义；选择、能力校验及按策略回退发生在创建阶段，运行中的发布、接收或关闭故障不会切换 provider。密码、token 和私钥不要放进可由 Debug 输出的 `ProviderOptions`；应使用 provider 自己的安全配置、凭据引用或解析器。

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

两种内置 local provider 均仅在进程内工作且不持久化。同步 provider 每个订阅使用一个阻塞接收线程；异步 provider 使用 waker 等待，不为每个订阅创建接收线程。`LocalEventBusConfig::new().queue_capacity(n)` 为**每个订阅者**设置正数的未完成消息上限（默认 1024），排队和已接收但尚未结算的消息都占用额度；重试保留原额度。异步订阅 close/drop 后的未结算消息只在同一进程、同一 provider 实例中恢复。publish receipt 只表示 provider 接纳，不代表 handler 已完成。facade 调度上限属于另一层，任何一个上限都不能单独代表总内存预算。可用 `cargo bench --bench local_threads` 与 `cargo bench --bench local_scale` 在自己的机器测量，结果不是跨机器保证。

`publish_all` 对每个请求分别尝试，不提供事务语义。本地 provider 无法持久恢复或跨进程投递。provider 必须声明相应的顺序、结算能力，facade 才能使用；`OrderingPolicy::PerKey` 要求 `PerKey` 或 `PerSubscription` 顺序能力。如果业务要求持久移交或数据库与事件的原子提交，应另行设计该机制并选用合适的 provider。

## 延伸阅读

- [中文 README](../README.zh_CN.md) · [English user guide](user_guide.md) · [API 文档](https://docs.rs/qubit-event-bus)
- [架构设计](design.zh_CN.md) · [SPI 设计](spi_design.zh_CN.md) · [更新日志](../CHANGELOG.zh_CN.md)
