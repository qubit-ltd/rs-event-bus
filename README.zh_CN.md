# Qubit Event Bus（`rs-event-bus`）

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-event-bus.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-event-bus)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-event-bus/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-event-bus?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Docs.rs](https://docs.rs/qubit-event-bus/badge.svg)](https://docs.rs/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

文档：[API 文档](https://docs.rs/qubit-event-bus)

`qubit-event-bus` 是一个轻量、线程安全的 Rust 进程内事件总线。

它提供类型安全的 Topic、事件信封、订阅配置、发布配置、`qubit-retry` 重试策略、确认句柄、发布/订阅拦截器，以及带 `qubit-metadata` 诊断信息的死信记录。

## 为什么使用

当你需要以下能力时，可以使用 `qubit-event-bus`：

- 在单进程内做类型安全的发布订阅路由
- 通过 `EventEnvelope` 统一事件元数据
- 为订阅处理器使用自动或手动确认
- 为订阅处理器配置重试和死信行为
- 用发布拦截器修改或丢弃待发布事件
- 用订阅拦截器包装、观测或短路处理器执行
- 在测试中通过 `wait_for_idle` 等待处理器工作完成

## 安装

```toml
[dependencies]
qubit-event-bus = "0.2.0"
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

    assert_eq!(
        received.lock().expect("received events should lock").as_slice(),
        &["order-1001".to_string()],
    );
    Ok(())
}
```

## 后续阅读

| 任务 | API |
| --- | --- |
| 创建事件总线 | `LocalEventBus::new`、`LocalEventBus::started`、`LocalEventBusFactory` |
| 定义类型安全 Topic | `Topic::<T>::try_new` |
| 发布 payload 或 envelope | `publish`、`publish_envelope`、`publish_envelope_with_options`、`publish_all` |
| 注册订阅处理器 | `subscribe`、`subscribe_with_options`、`Subscription` |
| 配置重试和确认 | `RetryOptions`、`SubscribeOptions`、`AckMode`、`Acknowledgement` |
| 添加发布拦截器 | `add_publisher_interceptor` |
| 添加订阅拦截器 | `add_subscriber_interceptor`、`SubscriberInterceptorChain` |
| 添加发布错误处理 | `PublishOptions` |
| 观测内部回调失败 | `add_error_observer` |
| 在测试中等待处理器工作完成 | `wait_for_idle` |
| 关闭本地事件总线 | `shutdown`、`shutdown_nonblocking`、`shutdown_with_timeout` |

## 核心 API 概览

| 类型 | 用途 |
| --- | --- |
| `LocalEventBus` | 线程安全的进程内事件总线实现。 |
| `LocalEventBusFactory` | 使用类型化默认发布配置、订阅配置、拦截器和死信策略创建事件总线。 |
| `Topic<T>` | 按名称和 payload 类型区分的类型安全 Topic。 |
| `EventEnvelope<T>` | 事件 payload 以及请求头、时间戳、顺序键、延迟、确认句柄和死信标记。 |
| `PublishOptions<T>` | 发布重试元数据和发布错误回调。 |
| `SubscribeOptions<T>` | 订阅确认模式、重试配置、过滤器、错误回调、死信策略和优先级。 |
| `DeadLetterPayload` | 标准死信记录，包含诊断元数据和类型擦除的原始 payload。 |
| `Subscription<T>` | 用于查看和取消订阅的句柄。 |
| `EventBusError` | 生命周期、校验、处理器、锁和类型擦除失败的统一错误类型。 |

## 项目范围

- `qubit-event-bus` 是进程内事件总线，不负责事件持久化或跨进程投递。
- 订阅处理器会在可配置的 `rs-thread-pool` 固定工作线程池中执行。发布操作会在调度处理器工作后返回。
- 通过 `LocalEventBus` 发布的 payload 需要满足 `Clone + Send + Sync + 'static`。
- 死信策略返回 `EventEnvelope<DeadLetterPayload>`，因此一个死信 Topic 可以接收来自多个源事件类型的归档记录。
- 订阅级死信策略返回 `Ok(None)` 时，会禁用本次失败投递对 factory 默认死信策略的回退。
- 手动 NACK 会被视为订阅处理失败，并先参与订阅重试；重试耗尽后才进入错误处理器和死信路由。
- 订阅错误处理器按注册顺序执行，直到某个处理器记录新的确认决策，或把决策改为 ACK。
- `publish_all` 会按输入顺序提交 envelope。带有相同 `ordering_key` 的 envelope 会按 topic 和订阅者串行投递；没有顺序键的 envelope 可以并发执行。
- `delay` 会让本地订阅处理至少推迟指定时长。延迟等待期间仍会占用本地处理器线程池容量。
- `LocalEventBus` 会拒绝 retry 的 `attempt_timeout` 选项，因为本地处理器没有协作取消信号。
- 不要在同一个 bus 的订阅工作线程中调用阻塞式 `shutdown`；订阅代码中应使用 `shutdown_nonblocking` 或 `shutdown_with_timeout`。
- `shutdown_with_timeout` 返回超时后，旧订阅工作进入 idle 之前，`start` 会拒绝重新启动。
- `wait_for_idle` 面向测试和需要等待已调度处理器完成的受控关闭流程。

## 贡献

欢迎提交 issue 和 pull request。

为了让维护和评审更顺畅，请尽量遵循以下约定：

- bug 报告、设计问题或较大的功能建议，先提交 issue 讨论
- pull request 尽量聚焦一个行为变更、问题修复或文档更新
- 遵循现有 `rs-*` 项目使用的 Rust 编码风格
- 修改运行时行为时，请补充相应测试
- 公共 API 行为变化时，请同步更新 README

向本项目提交贡献，即表示你同意该贡献使用与本项目相同的许可证。

## 许可证

本项目使用 [Apache License, Version 2.0](LICENSE) 许可证。
