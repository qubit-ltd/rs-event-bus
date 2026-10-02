# Qubit Event Bus（`rs-event-bus`）

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-event-bus` 帮助 Rust 应用把业务事件的发布与后续处理分开。例如订单提交后，审计和客户视图可以各自订阅同一个带类型的事件，订单流程无须直接调用两套处理逻辑。内置 local provider 适合允许事件丢失、由应用自行补偿的单进程任务；需要其他传输方式时，可通过 provider SPI 接入。本库本身不提供订单事务与事件发布之间的可靠移交。

## 订单服务实战场景

订单事务提交成功后，订单服务向 `orders.created` 发布 `OrderCreated`。两个订阅者分别处理审计和客户视图更新；以后增加消费者，无须改动发布方。这里的进程内投递与订单数据库事务相互独立：如果审计记录必须可靠保存，应另行设计持久移交与补偿机制。

## 安装

```toml
[dependencies]
qubit-event-bus = "0.19"
```

## 快速开始

<!-- event-bus-source: examples/local_delivery.rs -->
```rust
// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Minimal local-provider example showing subscription, publication, and
//! graceful shutdown.

use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
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
    let _ = bus.publish(PublishRequest::new(topic, "order-42".to_owned())?)?;
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

订单、审计和客户视图属于应用的不同模块，共用一个事件类型。下面展示各模块与总线相接的部分；`OrderRepository`、`AuditStore` 和 `CustomerViewStore` 由应用连接实际存储。

订单模块定义事件，供发布方和订阅方引用：

```rust
// src/orders/events.rs
use qubit_event_bus::model::Topic;

pub struct OrderCreated {
    pub order_id: String,
    pub customer_id: String,
    pub total_cents: u64,
}

impl OrderCreated {
    pub const TOPIC: Topic<Self> = Topic::new_static("orders.created");
}
```

订单服务在仓储确认事务提交后发布事件。它不依赖审计或客户视图模块：

```rust
// src/orders/service.rs
use qubit_event_bus::EventBus;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;

use super::events::OrderCreated;

pub struct CreateOrder {
    pub customer_id: String,
    pub total_cents: u64,
}

pub struct CommittedOrder {
    pub order_id: String,
    pub customer_id: String,
    pub total_cents: u64,
}

pub trait OrderRepository: Send + Sync {
    // 成功返回表示订单事务已提交。
    fn create_and_commit(&self, command: CreateOrder)
        -> Result<CommittedOrder, Box<dyn std::error::Error>>;
}

pub fn create_order(
    repository: &dyn OrderRepository,
    bus: &EventBus,
    command: CreateOrder,
) -> Result<PublishReceipt, Box<dyn std::error::Error>> {
    let order = repository.create_and_commit(command)?;
    let event = OrderCreated {
        order_id: order.order_id,
        customer_id: order.customer_id,
        total_cents: order.total_cents,
    };
    let receipt = bus.publish(PublishRequest::new(OrderCreated::TOPIC, event)?)?;
    if receipt.duplicate_possible() {
        // Retain the receipt and reconcile by event ID before deciding to republish.
        return Ok(receipt);
    }
    match receipt.admission_outcome() {
        AdmissionOutcome::Accepted(_) => Ok(receipt),
        AdmissionOutcome::OpaqueAccepted => {
            // provider 只报告 broker 接收，不提供逐个订阅者的接纳情况。
            Ok(receipt)
        }
        AdmissionOutcome::PartiallyAccepted(summary) => {
            // 部分订阅者已接纳；应单独修复被拒绝的目标，避免重复处理已接纳事件。
            Err(std::io::Error::other(format!("{} 个订阅者拒绝了事件", summary.rejected)).into())
        }
        AdmissionOutcome::NoDestinations
        | AdmissionOutcome::NoneAccepted(_) => {
            Err(std::io::Error::other("没有订阅者接纳事件").into())
        }
        other => Err(std::io::Error::other(format!("需要应用策略处理接纳结果：{other:?}")).into()),
    }
}
```

审计模块在应用启动时建立自己的订阅，并把事件写入审计存储：

```rust
// src/audit.rs
use std::sync::Arc;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::SubscribeRequest;

use crate::orders::events::OrderCreated;

pub trait AuditStore: Send + Sync {
    fn append_order_created(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}

pub fn subscribe(bus: &EventBus, store: Arc<dyn AuditStore>)
    -> Result<Subscription, Box<dyn std::error::Error>>
{
    let request = SubscribeRequest::new("audit-log", OrderCreated::TOPIC)?;
    Ok(bus.subscribe(request, move |delivery| {
        store.append_order_created(delivery.payload())
    })?)
}
```

客户视图模块独立订阅同一事件，更新查询视图：

```rust
// src/customer_view.rs
use std::sync::Arc;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::SubscribeRequest;

use crate::orders::events::OrderCreated;

pub trait CustomerViewStore: Send + Sync {
    fn upsert_order(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}

pub fn subscribe(bus: &EventBus, store: Arc<dyn CustomerViewStore>)
    -> Result<Subscription, Box<dyn std::error::Error>>
{
    let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?;
    Ok(bus.subscribe(request, move |delivery| {
        store.upsert_order(delivery.payload())
    })?)
}
```

应用启动时用 `EventBus::local(LocalEventBusConfig::default())` 创建总线，调用两个订阅模块的 `subscribe`，并持有返回的 `Subscription`；关闭时显式取消订阅并关闭总线。订单请求调用 `create_order`。返回的 `PublishReceipt` 只报告 provider 的接纳情况，不代表两处存储写入成功；进程在事务提交后、发布前退出时也不会自动补发。接纳结果、投递策略和关闭流程见[用户手册](doc/user_guide.zh_CN.md)。

### 启动时装配共享总线

为 `qubit-event-bus` 启用 `discovery` feature，并直接依赖 `qubit-spi = "0.13"` 以使用 `ProviderSelection`。内置 `local` 会自动登记到同步目录。`AsyncEventBusRegistry::discover()` 不包含异步 local provider；使用异步总线时，需要通过 `AsyncEventBusRegistry::with_local()` 显式登记。下面的启动装配代码选择同步 provider，再创建总线并把克隆句柄交给服务：

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

`OrderService` 是应用中的示意类型，上面仅展示启动装配片段。若 provider 位于独立 crate，应用需依赖该 crate，并在装配模块写入 `use provider_crate as _;`，让它链接进可执行程序。应用应持有总线和订阅句柄；关闭时先取消订阅，再关闭总线。发现机制和配置边界见[用户手册](doc/user_guide.zh_CN.md)。

## 能力与边界

- `Topic<T>`、`PublishRequest<T>`、`SubscribeRequest<T>` 将事件主题、发布和订阅保持为类型化 API。
- 同步 facade 和不绑定运行时的异步 facade 使用对象安全的 provider SPI；`qubit-spi` registry 可在创建时选择 provider、检查能力并尝试 fallback。
- 内置 local provider 为每个订阅者设置有界队列；facade 在 provider 能力允许时支持拦截器、重试、ACK/NACK、死信、顺序、诊断和关闭控制。
- Codec 回调受 panic 边界保护并返回结构化错误；编码后的字节在 provider 重试间共享。
- 可选的有界 `NotificationPublisher<T>` 为应用提供非阻塞通知入队；provider 接纳回执不表示 handler 已处理完成。
- `EventBus::request_shutdown` 返回可复用 ticket，用于非阻塞观察停机；观察者超时不会取消清理（见[停机指南](doc/user_guide.zh_CN.md#非阻塞地请求停机)）。
- 可选启用 `conformance` feature，为 provider SPI 契约检查提供结构化报告。

本库未内置 Tokio、crossbeam、flume、RabbitMQ、Kafka 或 Redis 适配器，也不保证消息持久化、跨进程投递、事务批量发布或恰好一次处理。两种 local provider 都限制每个订阅者的未完成消息数（默认 1024），并限制每个 provider 实例的总未完成投递数（默认 65,536）；限额满时回执会拒绝对应目标，Retry 保留额度直到 accept、reject、close 或 shutdown。限额统计投递条数，不统计 payload 字节。同步 facade 默认最多创建 256 个活跃订阅接收线程，可用 `EventBusFacadeConfig::with_delivery_scheduling` 配置 `DeliverySchedulingConfig` 的执行、全局持有、每订阅持有和订阅数量四项上限。订阅量较大时，在目标主机运行 `cargo bench --bench local_threads` 和 `cargo bench --bench local_scale` 实测，不把样本结果当作固定容量阈值。`EventBusFacadeConfig::with_payload_limits(PayloadLimits)` 分别设置编码发布和接收的正数上限，默认均为 1 MiB；原生 payload 的内存占用没有字节上限。异步 local 订阅 close/drop 会丢弃排队和未结算消息；同 ID 重订阅从空队列开始。持久 provider 按自身恢复协议处理。保留 `AsyncSubscription` 句柄但取消 `run` future，仍可在之后重新运行 facade 任务。详情见[用户手册](doc/user_guide.zh_CN.md#配置内置-local-事件总线)。

发布失败通过 `PublishFailure` 保留原始事件 ID、结构化原因及 `PublishEffect`。默认 `DuplicateRiskPolicy::Forbid` 会在可能已经接纳消息时停止自动重试，自定义重试规则也不能绕过。编码接收先检查长度，再精确验证 content type/schema，最后解码；元数据不兼容、输入超限或 codec panic 会停止该订阅。修复配置或 codec 后，应创建新订阅恢复持久消息。升级 provider 或 codec 前请阅读[迁移指南](doc/migration.zh_CN.md)。

配套版本为 core 0.19、Redis 0.7、task 0.8。CodecRegistry 遇到重复载荷类型会报错；确实要替换时应显式调用 `replace`。provider 能确认的投递次数与 facade 本地重试次数分开记录；无法确认时保持未知。能力、恢复步骤与限制见[用户手册](doc/user_guide.zh_CN.md)。

## 延伸阅读

- [用户手册](doc/user_guide.zh_CN.md)
- [迁移指南](doc/migration.zh_CN.md)
- [架构设计](doc/design.zh_CN.md) · [SPI 设计](doc/design.zh_CN.md#4-provider-spi)
- [API 文档](https://docs.rs/qubit-event-bus)
- [English README](README.md) · [English user guide](doc/user_guide.md)

## 投递缺口与接纳检查

provider 报告投递缺口后，订阅默认停止接收。同步 API 可通过 `Subscription::terminal_failure()` 查看稳定的 `SubscriptionStopReason::Gap`；异步 `run()` 会返回包含该原因的 `ReceiveError::Stopped`。只有消费者能够接受消息遗漏并希望继续处理后续消息时，才设置 `GapPolicy::Continue`。两种策略都会发出缺口诊断。

调用方要求目标接纳时使用 `publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)`。它只发布一次；条件不满足时，`CheckedPublishError::Admission` 保留完整回执。接纳检查可区分不可见的接纳结果、拦截器丢弃、没有目标、没有目标接纳、有目标接纳和部分接纳。部分接纳表示部分目标可能已接收事件，重试可能造成重复。目标接纳不代表 handler 已完成或数据已持久化。同步订阅每个占用一个协调线程，默认上限为 256，应按订阅规模配置容量。

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./.infra/bin/ci-check.sh

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
