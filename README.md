# Qubit Event Bus (`rs-event-bus`)

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-event-bus` solves a common problem inside an order service: once an order is accepted, the order code must trigger several independent tasks, such as writing an audit trail and refreshing a customer-facing view. Directly calling both tasks couples order creation to their implementations and failure paths. This crate lets the order code publish one typed event while each task subscribes independently. Its built-in local provider handles work inside one process; a provider SPI lets applications integrate a different transport without changing the event-facing API.

## An order service example

When order `order-1001` is created, the service publishes it to `orders.created`. Two subscribers record separate effects. A new local task can later subscribe to the same topic without changing the publishing code. The example waits until the local provider has no outstanding work on that topic, then checks both effects. This is a useful pattern for process-local side effects, not a guarantee that an order and its side effects are committed atomically.

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
    // Create an in-process bus and a typed topic shared by both subscribers.
    let bus = EventBus::local(LocalEventBusConfig::default())?;
    let orders = Topic::<String>::new("orders.created")?;
    let audit = Arc::new(Mutex::new(Vec::new()));
    let view = Arc::new(Mutex::new(Vec::new()));

    // Register independent consumers; the publisher does not call either one.
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

    // Publish once. The receipt reports admission, not handler success.
    let receipt = bus.publish(PublishRequest::new(orders.clone(), "order-1001".to_owned())?)?;
    assert_eq!(receipt.provider_id().as_str(), "local");
    // Wait for local work to settle, then verify both business effects.
    bus.wait_for_idle(&orders, None)?;
    assert_eq!(audit.lock().unwrap().as_slice(), &["order-1001"]);
    assert_eq!(view.lock().unwrap().as_slice(), &["order-1001"]);
    // Explicitly cancel subscriptions and close the bus to release resources.
    audit_subscription.cancel()?;
    view_subscription.cancel()?;
    bus.shutdown(qubit_event_bus::spi::ShutdownMode::Graceful {
        timeout: std::time::Duration::from_secs(2),
    })?;
    Ok(())
}
```

`publish` returns a receipt about provider admission, not handler success. `wait_for_idle` checks that the local provider has no queued or unsettled messages for this topic; the assertions check the business effects. Other providers may return `LifecycleError::IdleWaitUnsupported`. See the [user guide](doc/user_guide.md) for admission failures, retries, and cleanup.

## What it provides

- Typed `Topic<T>`, `PublishRequest<T>`, `SubscribeRequest<T>`, envelopes, deliveries, and publication receipts.
- Synchronous and runtime-neutral asynchronous facades over object-safe provider SPI contracts.
- Provider discovery and creation through `qubit-spi` registries, with creation-time capability checks and fallback.
- A built-in bounded-queue, in-process provider (`LocalEventBusProvider`).
- Facade-level interception, retry through the caller's direct `qubit-retry` dependency, ACK/NACK, dead-letter handling, ordering, diagnostics, and lifecycle controls where supported by provider capabilities.

The crate does not itself include Tokio, crossbeam, flume, RabbitMQ, Kafka, or Redis adapters. It does not promise durable or cross-process delivery, transactional batches, or exactly-once processing. A backend's stronger guarantees remain provider-specific and must be documented by that backend.

The local provider bounds queued and unsettled events per subscription and uses one blocking receive worker thread per synchronous subscription. Plan queue and thread capacity before adding many consumers. See [resource guidance](doc/user_guide.md#local-provider-resource-guidance).

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
