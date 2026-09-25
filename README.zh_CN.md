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
qubit-event-bus = "0.12"
```

## 快速开始

```rust
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::{AdmissionRequirement, PublishRequest, SubscribeRequest, Topic};
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::{EventBus, SubscriberId, WaitOutcome};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = EventBus::local(LocalEventBusConfig::default())?;
    let topic = Topic::<String>::new("orders.created")?;
    let request = SubscribeRequest::new(SubscriberId::new("audit-log")?, topic.clone());
    let audit = bus.subscribe(request, |delivery| {
        println!("审计收到订单：{}", delivery.payload());
    })?;

    // 订单事务提交成功后再发布；这里只演示总线调用。
    let receipt = bus.publish(PublishRequest::new(topic, "order-42".to_owned())?)?;
    receipt.check_admission(AdmissionRequirement::AtLeastOneAccepted)?;
    let topic = Topic::<String>::new("orders.created")?;
    assert_eq!(
        bus.wait_for_idle(&topic, Some(std::time::Duration::from_secs(2)))?,
        WaitOutcome::Idle,
    );

    audit.cancel()?;
    bus.shutdown(ShutdownMode::Graceful {
        timeout: std::time::Duration::from_secs(2),
    })?;
    Ok(())
}
```

运行示例会打印收到的订单号。真实应用应在启动时创建并持有订阅，在订单事务提交后发布，关闭时显式取消订阅并关闭总线。回执检查只证明至少一个目的地报告接纳，不证明审计写入成功；完整的双订阅者场景、结果验证与失败处理见[用户手册](doc/user_guide.zh_CN.md)。

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
