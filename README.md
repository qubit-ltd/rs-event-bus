# Qubit Event Bus (`rs-event-bus`)

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-event-bus` solves a common problem inside an order service: once an order is accepted, the order code must trigger several independent tasks, such as writing an audit trail and refreshing a customer-facing view. Directly calling both tasks couples order creation to their implementations and failure paths. This crate lets the order code publish one typed event while each task subscribes independently. Its built-in local provider handles work inside one process, where events may be lost and the application must arrange any needed recovery; a provider SPI lets applications integrate a different transport without changing the event-facing API. The crate does not make the handoff from the order transaction to publication reliable.

## An order service example

After an order transaction commits, the order service publishes `OrderCreated { order_id, customer_id, total_cents }` on `orders.created`. The audit subscriber appends an audit record; the customer-view subscriber updates its read model. Both subscribe to `Topic<OrderCreated>` independently, so a new consumer does not change the publisher. These are in-process side effects, not part of the order database transaction.

## Installation

```toml
[dependencies]
qubit-event-bus = "0.20"
```

## Quick start

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


The complete, runnable sync and runtime-neutral async applications live in
[`examples/local_minimal.rs`](examples/local_minimal.rs) and
[`examples/async_local_minimal.rs`](examples/async_local_minimal.rs). They show
subscription ownership, publication, and explicit shutdown.

```rust
use std::sync::Arc;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

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
    let receipt = bus.publish(PublishRequest::new(topic, event)?)?;
    if receipt.duplicate_possible() {
        // Retain the receipt and reconcile by event ID before deciding to republish.
        return Ok(receipt);
    }
    match receipt.admission_outcome() {
        AdmissionOutcome::Accepted(_) => Ok(receipt),
        AdmissionOutcome::OpaqueAccepted => {
            // The provider reports broker acceptance, not individual subscribers.
            Ok(receipt)
        }
        AdmissionOutcome::PartiallyAccepted(summary) => {
            // Some subscribers already admitted it; repair rejected destinations individually.
            Err(std::io::Error::other(format!("{} subscribers rejected the event", summary.rejected)).into())
        }
        AdmissionOutcome::NoDestinations
        | AdmissionOutcome::NoneAccepted(_) => {
            Err(std::io::Error::other("no subscriber admitted the event").into())
        }
        other => Err(std::io::Error::other(format!("admission needs an application policy: {other:?}")).into()),
    }
}
```

At startup, call both subscription functions and keep their `Subscription` handles in application state; cancel them during shutdown. After a successful order commit, call `publish_order_created`. Its receipt reports provider admission, not successful audit or view writes. The `AuditLog` and `CustomerOrderView` traits are application interfaces; connect them to your actual stores. See the [user guide](doc/user_guide.md) for admission failures and delivery policy.

### Assemble one shared bus at startup

Enable the `discovery` feature on `qubit-event-bus` and add a direct `qubit-spi = "0.13"` dependency for `ProviderSelection`. The built-in `local` provider is automatically submitted to the synchronous catalog. `AsyncEventBusRegistry::discover()` does not include the async local provider; register it explicitly with `AsyncEventBusRegistry::with_local()`. In the application's startup wiring, select the sync provider before creating one bus and pass cloned handles to services:

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

`OrderService` stands for an application type; this is a startup wiring excerpt. To discover a provider from a separate crate, depend on it and add `use provider_crate as _;` in the application wiring module so it is linked into the executable. Keep the bus and subscription handles in application state, then cancel subscriptions and shut down the bus during shutdown. See the [user guide](doc/user_guide.md) for discovery and provider configuration boundaries.

## What it provides

- Typed `Topic<T>`, `PublishRequest<T>`, `SubscribeRequest<T>`, envelopes, deliveries, and publication receipts.
- Synchronous and runtime-neutral asynchronous facades over object-safe provider SPI contracts.
- Provider discovery and creation through `qubit-spi` registries, with creation-time capability checks and fallback.
- A built-in bounded-queue, in-process provider (`LocalEventBusProvider`).
- Facade-level interception, retry through the caller's direct `qubit-retry` dependency, ACK/NACK, dead-letter handling, ordering, diagnostics, and lifecycle controls where supported by provider capabilities.
- Panic-contained codec callbacks, structured codec errors, and shared encoded payload bytes across provider publish attempts.
- Optional bounded `NotificationPublisher<T>` for nonblocking application notifications; provider admission receipts do not mean handlers have completed.
- `EventBus::request_shutdown` returns a reusable ticket for nonblocking shutdown observation; observer timeouts do not cancel cleanup ([shutdown guide](doc/user_guide.md#request-shutdown-without-waiting)).
- Optional `conformance` feature with a report API for provider-specific SPI contract checks.

The crate does not itself include Tokio, crossbeam, flume, RabbitMQ, Kafka, or Redis adapters. It does not promise durable or cross-process delivery, transactional batches, or exactly-once processing. A backend's stronger guarantees remain provider-specific and must be documented by that backend.

Both local providers bound queued and unsettled events per subscription (default 1,024) and across one provider instance (default 65,536). A full limit rejects that destination in the publish receipt; a retry keeps its reservation until accept, reject, close, or shutdown. These limits count delivery items, not payload bytes. The synchronous facade allows at most 256 live subscription receiver threads by default; configure `DeliverySchedulingConfig::new(running, owned, per_subscription, subscriptions)` through `EventBusFacadeConfig::with_delivery_scheduling` to change the shared sync/async limits. The async provider does not create a receive thread per subscription, but the application must drive `AsyncSubscription::run`. For higher subscription counts, measure `cargo bench --bench local_threads` and `cargo bench --bench local_scale` on the target host; the results are measurements, not a fixed capacity threshold. `EventBusFacadeConfig::with_payload_limits(PayloadLimits)` sets independent finite encoded publish and receive limits, both 1 MiB by default; native payload memory is not byte bounded. Async local subscriptions are ephemeral: close or drop discards pending and in-flight deliveries, and resubscribing with the same subscriber ID starts empty. Durable providers follow their own recovery protocol. Dropping an `AsyncSubscription::run` future while retaining its handle still permits a later `run` to resume facade-owned tasks. See the [user guide](doc/user_guide.md#configure-the-built-in-local-event-bus).

Publication failures carry the original event ID, a structured cause, and `PublishEffect` in `PublishFailure`. The default `DuplicateRiskPolicy::Forbid` stops automatic retries when admission may have happened, even if a custom retry rule asks to continue. Encoded receivers validate size and exact content type/schema before decoding. Incompatible metadata, oversized input, or codec panic stops that subscription; repair the configuration or codec and create a new subscription to recover durable work. See the [migration guide](doc/migration.md) before upgrading providers or codecs.

The coordinated versions are core 0.19, Redis 0.7, and task 0.8. Codec registration rejects duplicate payload types; use `replace` when replacement is intentional. Provider delivery attempts are kept separate from facade retries and remain unknown when the provider cannot establish them. See the [user guide](doc/user_guide.md) for capabilities, recovery, and limits.

## Learn more

- [User guide](doc/user_guide.md)
- [Migration guide](doc/migration.md)
- [Architecture](doc/design.md) · [SPI design](doc/design.md#4-provider-spi)
- [API reference](https://docs.rs/qubit-event-bus)
- [中文 README](README.zh_CN.md) · [中文用户手册](doc/user_guide.zh_CN.md)

## Delivery gaps and admission checks

Subscriptions stop receiving after a provider-reported delivery gap by default. Inspect the stable `SubscriptionStopReason::Gap` through `Subscription::terminal_failure()` (sync) or the `ReceiveError::Stopped` returned by async `run()`. Set `GapPolicy::Continue` only when the consumer accepts missed messages and wants later messages to continue. The gap diagnostic is emitted in either mode.

Use `publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)` only with a provider that reports `DestinationAdmissions`, such as local. An opaque provider such as Redis returns `CheckedPublishError::UnsupportedVisibility` before publishing for either per-destination requirement. Use `AdmissionRequirement::ProviderOrDestinationAccepted` for Redis: success means the broker accepted the event, not that a subscriber ran, data was durably stored, or a business write completed. A visible provider still returns the complete receipt in `CheckedPublishError::Admission` when the condition fails. Partial admission can mean some destinations already accepted the event; retrying may duplicate those deliveries. Sync subscriptions use one coordinator thread each; the default limit is 256, so configure capacity for the expected subscription count.

Local providers also support an optional declared-weight budget. Supply a `PublishOptions<T>::builder().native_payload_weight(...)` estimator for each published native payload type and set `LocalEventBusConfig::max_total_outstanding_weight_bytes(...)`. With that budget enabled, publication without a declared weight is rejected before enqueueing; each accepted fanout delivery consumes its own share. The budget is based on application-declared bytes and does not bound actual process memory. See the [local capacity guide](doc/user_guide.md#configure-the-built-in-local-event-bus) for setup and limits.

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./.infra/bin/ci-check.sh

# Check code coverage
./.infra/bin/coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./.infra/bin/align-ci.sh` to format code and
`./.infra/bin/ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-event-bus](https://github.com/qubit-ltd/rs-event-bus)
