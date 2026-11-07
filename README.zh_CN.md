# Qubit Event Bus（`rs-event-bus`）

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-event-bus` 为 Rust 应用提供带类型的事件总线。当多个模块需要响应同一事件时，发布方只需发布一次，各模块独立订阅，不必让发布方依赖各处理模块及其失败路径。内置 local provider 适合允许事件丢失、由应用自行补偿的单进程任务；需要其他传输方式时，可通过 provider SPI 接入。本库本身不保证业务事务与事件发布之间的可靠移交。

## 订单服务实战场景

订单事务提交成功后，订单服务向 `orders.created` 发布 `OrderCreated`。两个订阅者分别处理审计和客户视图更新；以后增加消费者，无须改动发布方。这里的进程内投递与订单数据库事务相互独立：如果审计记录必须可靠保存，应另行设计持久移交与补偿机制。

## 安装

```toml
[dependencies]
qubit-event-bus = "0.21"
```

## 快速开始

<!-- event-bus-source: examples/local_delivery.rs -->
```rust
//! Minimal local-provider example showing subscription, publication, and
//! graceful shutdown.

use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = EventBus::local(LocalEventBusConfig::new())?;
    let topic = Topic::<String>::new("orders.created")?;
    let (sender, receiver) = mpsc::channel();
    let _subscription = bus.subscribe(SubscribeRequest::new("audit", topic.clone())?, move |delivery| {
        sender.send(delivery.payload().clone()).unwrap();
    })?;
    let _ = bus.publish_checked(
        PublishRequest::new(topic, "order-42".to_owned())?,
        AdmissionRequirement::ProviderOrDestinationAccepted,
    )?;
    assert_eq!(receiver.recv_timeout(Duration::from_secs(3))?, "order-42");
    let shutdown_report = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })?;
    assert_eq!(shutdown_report.outcome, ShutdownOutcome::Complete);
    assert_eq!(shutdown_report.known_abandoned_deliveries, 0);
    assert!(shutdown_report.provider_may_have_abandoned_deliveries);
    Ok(())
}
```


可运行的同步与不绑定异步运行时示例位于
[`examples/local_minimal.rs`](examples/local_minimal.rs) 和
[`examples/async_local_minimal.rs`](examples/async_local_minimal.rs)，展示订阅句柄持有、发布和显式关闭。

订单事件、审计订阅、客户视图订阅和发布函数共享同一份可编译业务示例：

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/readme_orders.rs -->
```rust
use std::sync::Arc;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

// Published after the order transaction commits.
pub struct OrderCreated {
    pub order_id: String,
    pub customer_id: String,
    pub total_cents: u64,
}

impl OrderCreated {
    pub const TOPIC_CREATED: Topic<Self> = Topic::new_static("orders.created");
}

// The application supplies implementations backed by its audit and view stores.
pub trait AuditLog: Send + Sync {
    fn append_order_created(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}
pub trait CustomerOrderView: Send + Sync {
    fn upsert_order(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}

// Called during application startup; retain the returned subscription.
pub fn subscribe_audit(bus: &EventBus, audit: Arc<dyn AuditLog>) -> Result<Subscription, Box<dyn std::error::Error>> {
    let topic = OrderCreated::TOPIC_CREATED;
    let request = SubscribeRequest::new("audit-log", topic)?;
    Ok(bus.subscribe(request, move |delivery| audit.append_order_created(delivery.payload()))?)
}

pub fn subscribe_customer_view(
    bus: &EventBus,
    view: Arc<dyn CustomerOrderView>,
) -> Result<Subscription, Box<dyn std::error::Error>> {
    let topic = OrderCreated::TOPIC_CREATED;
    let request = SubscribeRequest::new("customer-view", topic)?;
    Ok(bus.subscribe(request, move |delivery| view.upsert_order(delivery.payload()))?)
}

// Called by the order service only after its database commit succeeds.
pub fn publish_order_created(
    bus: &EventBus,
    order_id: String,
    customer_id: String,
    total_cents: u64,
) -> Result<PublishReceipt, Box<dyn std::error::Error>> {
    let topic = OrderCreated::TOPIC_CREATED;
    let event = OrderCreated {
        order_id,
        customer_id,
        total_cents,
    };
    let receipt = bus.publish(PublishRequest::new(topic, event)?)?;
    Ok(receipt)
}
```

先检查 `duplicate_possible`；若为真，应先按输入事件 ID 核对历史，再决定是否重新发布。否则再检查 `admission_outcome` 和逐个目的地的接纳记录。部分接纳时保留输入/派发事件 ID、subscriber ID 与拒绝原因，只为拒绝目标设计补偿；回执不提供定向重投接口。`OpaqueAccepted` 只表示 provider 接纳了发布，不代表所有订阅者均已接纳或完成业务处理。`NoDestinations` 和 `NoneAccepted` 也应保留完整回执，由应用决定是否重试。

### 启动时装配共享总线

为 `qubit-event-bus` 启用 `discovery` feature，并直接依赖 `qubit-spi = "0.14"` 以使用 `ProviderSelection`。内置 `local` 会自动登记到同步目录。`AsyncEventBusRegistry::discover()` 不包含异步 local provider；使用异步总线时，需要通过 `AsyncEventBusRegistry::with_local()` 显式登记。下面的启动装配代码选择同步 provider，再创建总线并把克隆句柄交给服务：

```rust
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_spi::ProviderSelection;

let registry = EventBusRegistry::discover()?;
registry.set_default_selection(ProviderSelection::named("local")?)?;
registry.seal();
let bus = registry.create(&EventBusConfig::default())?;
let orders = OrderService::new(bus.clone());
```

`OrderService` 是应用中的示意类型，上面仅展示启动装配片段。若 provider 位于独立 crate，应用需依赖该 crate，并在装配模块写入 `use provider_crate as _;`，让它链接进可执行程序。应用应持有总线和订阅句柄。整体停机时，先停止业务入口和事件来源，再保留这些句柄并直接驱动有界总线停机。单独停止某个消费者时使用 `cancel()`；同步取消可能无期限等待 handler，local receiver 关闭还会丢弃排队消息。详见[生命周期指南](doc/user_guide.zh_CN.md#生命周期等待与停机)；发现机制和配置边界见[用户手册](doc/user_guide.zh_CN.md)。

应用可以用 `EventBus::local(LocalEventBusConfig::default())` 创建总线，在启动时调用前述两个订阅函数并持有返回的句柄；订单事务提交后调用 `publish_order_created`。回执只报告 provider 接纳情况，不代表审计和客户视图已写入；事务提交后、发布前进程退出也不会自动补发。整体停机时的有界处理顺序见[生命周期指南](doc/user_guide.zh_CN.md#生命周期等待与停机)；接纳结果和投递策略见[用户手册](doc/user_guide.zh_CN.md)。

停机时先关闭业务入口和事件来源，再保留订阅句柄并直接驱动有界总线停机。不要在 deadline 之前调用同步 `Subscription::cancel()`：它可能一直等到正在运行的 handler 返回；local receiver 关闭时还会丢弃队列中尚未接收的消息。Graceful 会处理 facade 已接管的工作，但不保证 provider 队列一定在期限内排空。丢弃同步句柄不会取消订阅。异步总线应在后台任务中驱动 `AsyncSubscription::run()`，然后直接驱动有界总线停机；停机会接管保留的 session。如果总线仍在运行，且应用需要单独观察 provider receiver 的关闭错误，可在应用 deadline 内等待 `close()`。若总线停机已经开始，`close()` 会返回 `LifecycleError::Closed`；应继续驱动 shutdown 并获取其结果。异步句柄被丢弃时会请求处置，但无法返回异步 provider 的关闭错误。需要单独观察该错误时，按[生命周期指南](doc/user_guide.zh_CN.md#生命周期等待与停机)在期限内等待 `close()`。

## 能力与边界

- `Topic<T>`、`PublishRequest<T>`、`SubscribeRequest<T>` 将事件主题、发布和订阅保持为类型化 API。
- 同步 facade 和不绑定运行时的异步 facade 使用对象安全的 provider SPI；`qubit-spi` registry 可在创建时选择 provider、检查能力并尝试 fallback。
- 内置 local provider 为每个订阅者设置有界队列；facade 在 provider 能力允许时支持拦截器、重试、ACK/NACK、死信、顺序、诊断和关闭控制。
- Codec 回调受 panic 边界保护并返回结构化错误；编码后的字节在 provider 重试间共享。
- 可选的有界 `NotificationPublisher<T>` 使应用入队时无需等待队列容量或 provider 工作；内部互斥锁竞争仍可能让入队等待。provider 接纳回执不表示 handler 已处理完成。
- `EventBus::request_shutdown` 返回可复用 ticket，用于非阻塞观察停机；观察者超时不会取消清理（见[停机指南](doc/user_guide.zh_CN.md#非阻塞地请求停机)）。
- 可选启用 `conformance` feature，为 provider SPI 契约检查提供结构化报告。

本库未内置 Tokio、crossbeam、flume、RabbitMQ、Kafka 或 Redis 适配器，也不保证消息持久化、跨进程投递、事务批量发布或恰好一次处理。两种 local provider 都限制每个订阅者的未完成消息数（默认 1024），并限制每个 provider 实例的总未完成投递数（默认 65,536）；限额满时回执会拒绝对应目标，Retry 保留额度直到 accept、reject、close 或 shutdown。限额统计投递条数，不统计 payload 字节。同步 facade 默认最多创建 256 个活跃订阅接收线程，可用 `EventBusFacadeConfig::with_delivery_scheduling` 配置 `DeliverySchedulingConfig` 的执行、全局持有、每订阅持有和订阅数量四项上限。订阅量较大时，在目标主机运行 `cargo bench --bench local_threads` 和 `cargo bench --bench local_scale` 实测，不把样本结果当作固定容量阈值。`EventBusFacadeConfig::with_payload_limits(PayloadLimits)` 分别设置编码发布和接收的正数上限，默认均为 1 MiB；原生 payload 的内存占用没有字节上限。异步 local 订阅 close/drop 会丢弃排队和未结算消息；同 ID 重订阅从空队列开始。持久 provider 按自身恢复协议处理。保留 `AsyncSubscription` 句柄但取消 `run` future，仍可在之后重新运行 facade 任务。详情见[用户手册](doc/user_guide.zh_CN.md#配置内置-local-事件总线)。

发布失败通过 `PublishFailure` 保留原始事件 ID、结构化原因及 `PublishEffect`。默认 `DuplicateRiskPolicy::Forbid` 会在可能已经接纳消息时停止自动重试，自定义重试规则也不能绕过。编码接收先检查长度，再通过 codec 验证 content type/schema，最后解码；默认要求精确匹配，codec 可覆盖 `validate_metadata`，显式支持并记录兼容格式或 schema 版本。详见[codec 恢复指南](doc/user_guide.zh_CN.md#恢复因编码边界失败而停止的订阅)。元数据不兼容、输入超限或 codec panic 会停止该订阅。修复配置或 codec 后，应创建新订阅恢复持久消息。

仓库脚本和 `.infra` 命令须在完整 Git checkout 根目录运行，crate 解包目录不包含这些工具。本 crate 版本为 0.21.0。CodecRegistry 遇到重复载荷类型会报错；确需替换时应显式调用 `replace`。provider 可确认的投递次数与 facade 本地重试次数分开记录，无法确认时保持未知。能力、恢复步骤与限制见[用户手册](doc/user_guide.zh_CN.md)。

## 延伸阅读

- [用户手册](doc/user_guide.zh_CN.md)
- [架构设计](doc/design.zh_CN.md) · [SPI 设计](doc/design.zh_CN.md#4-provider-spi)
- [API 文档](https://docs.rs/qubit-event-bus)
- [English README](README.md) · [English user guide](doc/user_guide.md)

## 投递缺口与接纳检查

provider 报告投递缺口后，订阅默认停止接收。同步 API 可通过 `Subscription::terminal_failure()` 查看稳定的 `SubscriptionStopReason::Gap`；异步 `run()` 会返回包含该原因的 `ReceiveError::Stopped`。只有消费者能够接受消息遗漏并希望继续处理后续消息时，才设置 `GapPolicy::Continue`。两种策略都会发出缺口诊断；`Stop` 会在调用诊断回调前建立停止屏障。

只有 provider 能报告 `DestinationAdmissions`（例如 local）时，才用 `publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)` 要求具体目标接纳。Redis 等不公开目标的 provider 遇到两种逐目标条件，会在发布前返回 `CheckedPublishError::UnsupportedVisibility`。对 Redis 应改用 `AdmissionRequirement::ProviderOrDestinationAccepted`：成功只代表 broker 接纳事件，不能说明订阅者已执行、数据已持久化或业务写入已完成。可见 provider 的接纳条件不满足时，`CheckedPublishError::Admission` 仍保留完整回执；部分目标已接纳时重试可能造成重复。同步订阅每个占用一个协调线程，默认上限为 256，应按订阅规模配置容量。

local 可选用声明权重预算：为每种发布的原生载荷 `T` 配置 `PublishOptions<T>::builder().native_payload_weight(...)` 估算器，再设置 `LocalEventBusConfig::max_total_outstanding_weight_bytes(...)`。启用预算后，未提供权重的发布会在入队前被拒绝；扇出到多个目标时，每份已接纳的投递分别占用额度。额度按应用声明的字节数计算，不能当作进程真实内存上限。配置方法和边界见[本地容量说明](doc/user_guide.zh_CN.md#配置内置-local-事件总线)。

## 测试

以下仓库命令须在完整 Git checkout 根目录运行；crate 解包目录不包含 `scripts/` 和 `.infra/`。

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./.infra/bin/ci-check.sh

# 本 crate 项目检查
./scripts/project-ci-check.sh

# 检查代码覆盖率
./.infra/bin/coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./.infra/bin/align-ci.sh` 格式化代码，运行 `./.infra/bin/ci-check.sh` 对齐 CI 要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-event-bus](https://github.com/qubit-ltd/rs-event-bus)

直接构造 local provider 时，可通过 `EventBus::local_with_monitor` 查看容量诊断。需要同时配置 facade 时，使用 `EventBus::local_with_config_and_monitor`，详见[用户指南](doc/user_guide.zh_CN.md)。
