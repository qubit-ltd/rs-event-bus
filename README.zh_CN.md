# Qubit Event Bus（`rs-event-bus`）

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-event-bus` 是一个轻量、线程安全的 Rust 进程内发布/订阅事件总线，提供类型化 Topic 和 envelope、可配置的确认与重试、拦截器、死信路由、投递失败观测以及 best-effort 批量发布能力。

它是进程内组件，不负责事件持久化，也不负责跨进程投递。完整的场景教程、API 细节、迁移说明和运行限制请阅读[中文用户指南](doc/user_guide.zh_CN.md)或 [English user guide](doc/user_guide.md)；运行时模型见[设计说明](doc/design.zh_CN.md)和 [design guide](doc/design.md)。

## 安装

```toml
[dependencies]
qubit-event-bus = "0.11"
```

## 快速开始

```rust
use std::sync::{Arc, Mutex};

use qubit_event_bus::{LocalEventBus, Topic};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = LocalEventBus::started()?;
    let topic = Topic::<String>::try_new("orders.created")?;
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    bus.subscribe("audit-log", &topic, move |event| {
        captured.lock().expect("received events should lock").push(event.payload().clone());
        Ok(())
    })?;
    bus.publish(&topic, "order-1001".to_string())?;
    bus.wait_for_idle(&topic)?;

    assert_eq!(received.lock().expect("received events should lock").as_slice(), &["order-1001".to_string()]);
    Ok(())
}
```

`publish` 返回描述准入结果的 `PublishReceipt`，不代表 handler 已完成。测试或受控停机流程需要观察 handler 效果时，应调用 `wait_for_idle`。

## API 速览

| 需求 | API |
| --- | --- |
| 创建总线 | `LocalEventBus::new`、`LocalEventBus::started`、`LocalEventBusFactory` |
| 定义类型安全 Topic | `Topic::<T>::try_new` |
| 发布一个或多个事件 | `publish`、`publish_envelope`、`publish_all`、`BatchPublishResult` |
| 订阅 handler | `subscribe`、`subscribe_with_options`、`Subscription` |
| 配置投递容量 | `DeliveryLimits::bounded`、`DeliveryLimits::unbounded`、`LocalEventBusFactory::set_delivery_limits` |
| 配置重试和 ACK/NACK | `SubscribeOptions`、`RetryPolicy`、`AckMode`、`Acknowledgement` |
| 配置拦截器 | `PublisherInterceptor`、`SubscriberInterceptor` 及其 global 版本 |
| 路由死信 | `standard_dead_letters_to`、`prefixed_dead_letters`、`discard_dead_letters` |
| 观测终态失败 | `add_delivery_failure_observer`、`DeliveryFailure` |
| 停止和测试 | `shutdown`、`shutdown_nonblocking`、`shutdown_with_timeout`、`wait_for_idle` |

## 重要语义

- `LocalEventBus` 不提供事务语义。`publish_all` 是 best effort：按输入顺序提交每个 envelope，并记录每个事件的结果，不保证全有或全无。
- `BatchPublishResult::accepted_count()` 统计其回执中至少有一个订阅投递为 `DispatchStatus::Accepted` 的输入项。它不是已完成 handler 的数量；同一个项还可能因另一个订阅者拒绝而计入 `failure_count()`。
- `DeliveryLimits` 分别控制已接纳的 in-flight 投递上限和可选的 handler 执行队列容量。两个值（若提供）都必须大于零；按需使用 `DeliveryLimits::bounded` 或 `DeliveryLimits::unbounded`。默认是 `DeliveryLimits::default()`（`4096`，不显式限制队列容量）。
- 发布 payload 必须满足 `Clone + Send + Sync + 'static`。匹配的订阅会被提交到本地 worker 池，发布不会等待 handler 完成。
- `AckMode::Manual` handler 必须在返回前 ACK 或 NACK。缺少确认决策会被视为失败，可能重试或进入死信处理。
- 具有相同 `ordering_key` 的事件会在每个 Topic 和订阅者内串行执行；没有顺序键的事件可以并发执行。
- handler 内需要请求停机时应调用 `shutdown_nonblocking()`。当前 handler 仍在运行时，`shutdown_with_timeout()` 无法完成并会报告超时；它只适用于必须有界等待的调用方。
- 丢弃 `Subscription` 句柄不会取消订阅；请调用句柄的取消 API。生命周期、重试、延迟和停机细节见用户指南。

## 延伸阅读

- [API 文档](https://docs.rs/qubit-event-bus)
- [English user guide](doc/user_guide.md)
- [中文用户指南](doc/user_guide.zh_CN.md)
- [Design guide](doc/design.md)
- [设计说明](doc/design.zh_CN.md)
- [中文更新日志](CHANGELOG.zh_CN.md)
- [Changelog](CHANGELOG.md)
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
