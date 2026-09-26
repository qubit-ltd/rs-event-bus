# Qubit Event Bus (`rs-event-bus`)

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-event-bus` solves a common problem inside an order service: once an order is accepted, the order code must trigger several independent tasks, such as writing an audit trail and refreshing a customer-facing view. Directly calling both tasks couples order creation to their implementations and failure paths. This crate lets the order code publish one typed event while each task subscribes independently. Its built-in local provider handles work inside one process; a provider SPI lets applications integrate a different transport without changing the event-facing API.

## An order service example

After an order transaction commits, the order service publishes `OrderCreated { order_id, customer_id, total_cents }` on `orders.created`. The audit subscriber appends an audit record; the customer-view subscriber updates its read model. Both subscribe to `Topic<OrderCreated>` independently, so a new consumer does not change the publisher. These are in-process side effects, not part of the order database transaction.

## Installation

```toml
[dependencies]
qubit-event-bus = "0.14"
```

## Quick start

```rust
use std::sync::Arc;

use qubit_event_bus::model::{PublishReceipt, PublishRequest, SubscribeRequest, Topic};
use qubit_event_bus::{DeliveryError, EventBus, Subscription};

// Published after the order transaction commits.
struct OrderCreated {
    order_id: String,
    customer_id: String,
    total_cents: u64,
}

impl OrderCreated {
    const TOPIC_CREATED: Topic<Self> = Topic::new_static("orders.created");
}

// The application supplies implementations backed by its audit and view stores.
trait AuditLog: Send + Sync {
    fn append_order_created(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}
trait CustomerOrderView: Send + Sync {
    fn upsert_order(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}

// Called during application startup; retain the returned subscription.
fn subscribe_audit(bus: &EventBus, audit: Arc<dyn AuditLog>)
    -> Result<Subscription, Box<dyn std::error::Error>>
{
    let topic = OrderCreated::TOPIC_CREATED;
    let request = SubscribeRequest::new("audit-log", topic)?;
    Ok(bus.subscribe(request, move |delivery| {
        audit.append_order_created(delivery.payload())
    })?)
}

fn subscribe_customer_view(bus: &EventBus, view: Arc<dyn CustomerOrderView>)
    -> Result<Subscription, Box<dyn std::error::Error>>
{
    let topic = OrderCreated::TOPIC_CREATED;
    let request = SubscribeRequest::new("customer-view", topic)?;
    Ok(bus.subscribe(request, move |delivery| {
        view.upsert_order(delivery.payload())
    })?)
}

// Called by the order service only after its database commit succeeds.
fn publish_order_created(
    bus: &EventBus,
    order_id: String,
    customer_id: String,
    total_cents: u64,
) -> Result<PublishReceipt, Box<dyn std::error::Error>> {
    let topic = OrderCreated::TOPIC_CREATED;
    let event = OrderCreated { order_id, customer_id, total_cents };
    Ok(bus.publish(PublishRequest::new(topic, event)?)?)
}
```

At startup, call both subscription functions and keep their `Subscription` handles in application state; cancel them during shutdown. After a successful order commit, call `publish_order_created`. Its receipt reports provider admission, not successful audit or view writes. The `AuditLog` and `CustomerOrderView` traits are application interfaces; connect them to your actual stores. See the [user guide](doc/user_guide.md) for admission failures and delivery policy.

### Assemble one shared bus at startup

Enable the `discovery` feature on `qubit-event-bus` and add a direct `qubit-spi = "0.13"` dependency for `ProviderSelection`. The built-in `local` provider is submitted to the synchronous catalog, with a separate entry for the async catalog. In the application's startup wiring, select it before creating one bus and pass cloned handles to services:

```rust
use qubit_event_bus::{EventBusConfig, EventBusRegistry};
use qubit_spi::ProviderSelection;

let registry = EventBusRegistry::discover()?;
registry.set_default_selection(ProviderSelection::named("local")?)?;
registry.seal();
let bus = registry.create(&EventBusConfig::default())?;
let orders = OrderService::new(bus.clone());
```

`OrderService` stands for an application type; this is a startup wiring excerpt. To discover a provider from a separate crate, depend on it and add `use provider_crate as _;` in the application wiring module so it is linked into the executable. Keep the bus and subscription handles in application state, then cancel subscriptions and shut down the bus during shutdown. See the [user guide](doc/user_guide.md) for discovery and provider configuration boundaries.

## What it provides

- Typed `Topic<T>`, `PublishRequest<T>`, `SubscribeRequest<T>`, envelopes, deliveries, and publication receipts.
- Synchronous and runtime-neutral asynchronous facades over object-safe provider SPI contracts.
- Provider discovery and creation through `qubit-spi` registries, with creation-time capability checks and fallback.
- A built-in bounded-queue, in-process provider (`LocalEventBusProvider`).
- Facade-level interception, retry through the caller's direct `qubit-retry` dependency, ACK/NACK, dead-letter handling, ordering, diagnostics, and lifecycle controls where supported by provider capabilities.
- Optional bounded `NotificationPublisher<T>` for nonblocking application notifications; provider admission receipts do not mean handlers have completed.
- Optional `conformance` feature with a report API for provider-specific SPI contract checks.

The crate does not itself include Tokio, crossbeam, flume, RabbitMQ, Kafka, or Redis adapters. It does not promise durable or cross-process delivery, transactional batches, or exactly-once processing. A backend's stronger guarantees remain provider-specific and must be documented by that backend.

Both local providers bound queued and unsettled events per subscription (default 1,024) and across one provider instance (default 65,536). A full limit rejects that destination in the publish receipt; a retry keeps its reservation until accept, reject, close, or shutdown. These limits count delivery items, not payload bytes. The synchronous provider uses one blocking receive worker per subscription plus a shared handler pool. The async provider does not create a receive thread per subscription, but the application must drive `AsyncSubscription::run`. For higher subscription counts, measure `cargo bench --bench local_threads` and `cargo bench --bench local_scale` on the target host; the results are measurements, not a fixed capacity threshold. Async provider subscriptions are ephemeral: close or drop discards pending and in-flight deliveries, and resubscribing with the same subscriber ID starts empty. Dropping an `AsyncSubscription::run` future while retaining its handle still permits a later `run` to resume facade-owned tasks. See [resource guidance](doc/user_guide.md#local-provider-resource-guidance).

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
