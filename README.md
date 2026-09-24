# Qubit Event Bus (`rs-event-bus`)

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-event-bus` gives applications one typed event-bus API while keeping transport choice behind a small provider SPI. Use its built-in local provider for in-process dispatch, or implement/register a provider for another transport; the facade owns portable request handling, middleware, retry, settlement, diagnostics, and lifecycle behavior instead of tying applications to a channel or broker API.

The local provider is useful when an application needs to fan an order event out to local consumers—for example, an audit subscriber and a cache updater—without introducing a broker. A publish receipt describes provider admission, not completed handler work, so the application can explicitly wait for tracked delivery work when it needs an observable result.

Local publish admission is per destination: an empty list reports no destinations, and a partial result can include both accepted and rejected subscribers. Inspect the receipt before retrying; resending the whole event can duplicate delivery to destinations that already accepted it. Synchronous graceful shutdown bounds the caller's wait; after `TimedOut`, the bus remains closed to new work while background cleanup continues.

`PublishReceipt::check_admission` evaluates the provider's completed receipt without publishing again or changing it; it does not wait for handlers. A successful check after partial admission still means some destinations rejected the event, so do not blindly republish the whole event. `EventBus::publish_metrics()` and `AsyncEventBus::publish_metrics()` expose admission counters; snapshots load fields independently and do not indicate handler completion. A `PerKey` subscription requires a provider that declares either `PerKey` or `PerSubscription` ordering. Subscription priority has been removed because it did not affect delivery order. Keep the synchronous `Subscription` handle and call `cancel()` explicitly; dropping it alone leaves its worker subscribed.

## Installation

```toml
[dependencies]
qubit-event-bus = "0.12"
```

## Quick start

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

`wait_for_idle` checks that the local provider has no queued or unsettled messages for this topic. It does not prove handler success; providers without this capability return `LifecycleError::IdleWaitUnsupported`.

## What it provides

- Typed `Topic<T>`, `PublishRequest<T>`, `SubscribeRequest<T>`, envelopes, deliveries, and publication receipts.
- Synchronous and runtime-neutral asynchronous facades over object-safe provider SPI contracts.
- Provider discovery and creation through `qubit-spi` registries, with creation-time capability checks and fallback.
- A built-in bounded-queue, in-process provider (`LocalEventBusProvider`).
- Facade-level interception, retry through the caller's direct `qubit-retry` dependency, ACK/NACK, dead-letter handling, ordering, diagnostics, and lifecycle controls where supported by provider capabilities.

The crate does not itself include Tokio, crossbeam, flume, RabbitMQ, Kafka, or Redis adapters. It does not promise durable or cross-process delivery, transactional batches, or exactly-once processing. A backend's stronger guarantees remain provider-specific and must be documented by that backend.

## Learn more

- [English user guide](doc/user_guide.md) · [中文用户指南](doc/user_guide.zh_CN.md)
- [Architecture status (English)](doc/design.md) · [架构设计（中文）](doc/design.zh_CN.md) · [SPI design (English)](doc/spi_design.md) · [正式 SPI 设计（中文）](doc/spi_design.zh_CN.md)
- [API reference](https://docs.rs/qubit-event-bus)
- [Changelog](CHANGELOG.md) · [中文更新日志](CHANGELOG.zh_CN.md)
- [中文 README](README.zh_CN.md)

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-event-bus](https://github.com/qubit-ltd/rs-event-bus)
