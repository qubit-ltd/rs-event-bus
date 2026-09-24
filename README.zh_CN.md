# Qubit Event Bus（`rs-event-bus`）

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-event-bus` 为应用提供统一、类型安全的事件总线 API，并把传输方式隔离在精简的 provider SPI 后面。进程内使用时可直接选择内置 local provider；需要接入其他传输时，由对应 provider 实现并注册 SPI。facade 统一处理可移植的请求、拦截器、重试、确认、诊断和生命周期语义，应用无需绑定某一种 channel 或 broker API。

例如，订单创建后，审计记录和缓存更新都可以订阅同一 Topic。使用本地 provider 时不必先部署消息代理。发布回执反映的是 provider 是否接纳消息，而不是 handler 是否已经处理完；需要确认处理结果时，应用可以显式等待总线跟踪的投递工作。

本地 provider 按目的地逐个报告接纳情况：空列表表示没有报告目的地，部分结果可能同时包含已接纳和被拒绝的订阅者。重试前先检查回执；重发整条事件可能让已接纳的目的地重复收到消息。同步 graceful shutdown 限制调用方的等待时间；返回 `TimedOut` 后，总线仍拒绝新工作，后台清理会继续。

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
    let bus = EventBus::local(LocalEventBusConfig::default())?;
    let orders = Topic::<String>::new("orders.created")?;
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    let subscriber = SubscribeRequest::new(SubscriberId::new("audit-log")?, orders.clone());
    let _subscription = bus.subscribe(subscriber, move |delivery| {
        captured.lock().expect("received events should lock").push(delivery.payload().clone());
        Ok::<(), qubit_event_bus::DeliveryError>(())
    })?;

    let receipt = bus.publish(PublishRequest::new(orders.clone(), "order-1001".to_owned())?)?;
    assert_eq!(receipt.provider_id().as_str(), "local");
    bus.wait_for_idle(&orders, None)?;
    assert_eq!(received.lock().expect("received events should lock").as_slice(), &["order-1001"]);
    bus.shutdown(qubit_event_bus::spi::ShutdownMode::Graceful {
        timeout: std::time::Duration::from_secs(2),
    })?;
    Ok(())
}
```

## 能力与边界

- 提供类型化的 `Topic<T>`、`PublishRequest<T>`、`SubscribeRequest<T>`、事件 envelope、delivery 和 publish receipt。
- 提供同步及 runtime-neutral 异步 facade，底层均通过对象安全的 provider SPI 工作。
- 通过 `qubit-spi` registry 完成 provider 发现和创建，并在创建时检查能力及执行 fallback。
- 内置每订阅者有界队列的进程内 provider（`LocalEventBusProvider`）。
- 在后端能力允许时，由 facade 统一处理拦截器、重试、ACK/NACK、死信、顺序、诊断和生命周期。

本 crate 不包含 Tokio、crossbeam、flume、RabbitMQ、Kafka 或 Redis 的适配器；也不承诺持久化、跨进程投递、事务批量发布或恰好一次处理。具体后端若提供更强保证，应由该后端单独说明。

## 延伸阅读

- [中文用户指南](doc/user_guide.zh_CN.md) · [English user guide](doc/user_guide.md)
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
