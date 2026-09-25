# Qubit Event Bus（`rs-event-bus`）

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-event-bus` 解决订单服务中的一个实际问题：订单创建后，审计记录和客户视图更新都要得到通知。如果订单代码直接调用两套处理逻辑，每增加一个后续任务，就要改动订单流程并处理它的失败。使用本库，订单流程只发布一次带类型的事件，独立的订阅者分别处理自己的工作。内置 local provider 适合单进程内的任务；需要其他传输方式时，可通过 provider SPI 接入。

## 订单服务实战场景

订单 `order-1001` 创建后，服务向 `orders.created` 发布事件。审计和客户视图各有一个订阅者；以后增加进程内任务时，不必改动发布事件的代码。示例等待本地 provider 处理完该 Topic 上的待处理消息，再分别检查两项效果。这个模式适合进程内的后续工作，但不保证订单与后续工作原子提交。

## 安装

```toml
[dependencies]
qubit-event-bus = "0.12"
```

## 快速开始

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

`publish` 返回的是 provider 接纳事件的回执，不代表 handler 成功。`wait_for_idle` 确认本地 provider 在该 Topic 上没有排队或尚未结算的消息；代码中的断言才检查业务效果。其他 provider 可能返回 `LifecycleError::IdleWaitUnsupported`。接纳失败、重试和资源清理见[用户手册](doc/user_guide.zh_CN.md)。

## 能力与边界

- `Topic<T>`、`PublishRequest<T>`、`SubscribeRequest<T>` 将事件主题、发布和订阅保持为类型化 API。
- 同步 facade 和不绑定运行时的异步 facade 使用对象安全的 provider SPI；`qubit-spi` registry 可在创建时选择 provider、检查能力并尝试 fallback。
- 内置 local provider 为每个订阅者设置有界队列；facade 在 provider 能力允许时支持拦截器、重试、ACK/NACK、死信、顺序、诊断和关闭控制。

本库未内置 Tokio、crossbeam、flume、RabbitMQ、Kafka 或 Redis 适配器，也不保证消息持久化、跨进程投递、事务批量发布或恰好一次处理。同步 local provider 每个订阅者使用一个阻塞式接收 worker 线程；队列额度按订阅者计算，包含排队和已接收但尚未结算的消息。规划订阅数量和容量时，请阅读[资源指南](doc/user_guide.zh_CN.md#本地-provider-资源指南)。

## 延伸阅读

- [中文用户手册](doc/user_guide.zh_CN.md) · [English user guide](doc/user_guide.md)
- [架构设计（中文）](doc/design.zh_CN.md) · [Architecture status (English)](doc/design.md) · [正式 SPI 设计（中文）](doc/spi_design.zh_CN.md) · [SPI design (English)](doc/spi_design.md)
- [API 文档](https://docs.rs/qubit-event-bus)
- [中文更新日志](CHANGELOG.zh_CN.md) · [Changelog](CHANGELOG.md)
- [English README](README.md)

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./ci-check.sh

# 检查代码覆盖率
./coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./align-ci.sh`格式化代码，运行`./ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-event-bus](https://github.com/qubit-ltd/rs-event-bus)
