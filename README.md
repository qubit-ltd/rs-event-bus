# Qubit Event Bus (`rs-event-bus`)

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Docs.rs](https://docs.rs/qubit-event-bus/badge.svg)](https://docs.rs/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Documentation: [API Reference](https://docs.rs/qubit-event-bus)

`qubit-event-bus` is a lightweight, thread-safe, in-process event bus for Rust applications.

It provides type-safe topics, event envelopes, subscriber options, publish options, `qubit-retry` retry policies, acknowledgement handles, factory-configured typed and global interceptors, best-effort batch results, transactional staged-event contracts, and dead-letter records with `qubit-metadata` diagnostics.

## Why Use It

Use `qubit-event-bus` when you need:

- type-safe publish/subscribe routing inside one process
- consistent event metadata through `EventEnvelope`
- automatic or manual acknowledgement for subscriber handlers
- subscriber retry and dead-letter behavior
- publisher interceptors that can modify or drop outgoing events
- global metadata interceptors for tracing, logging, and metrics across all payload types
- subscriber interceptors that can wrap, observe, or short-circuit handler execution
- typed and global default dead-letter strategies
- heterogeneous staged-event contracts for transactional backends
- deterministic test synchronization through `wait_for_idle`

## Installation

```toml
[dependencies]
qubit-event-bus = "0.6.0"
```

## Quick Start

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

## Common Next Steps

| Task | API |
| --- | --- |
| Create an event bus | `LocalEventBus::new`, `LocalEventBus::started`, `LocalEventBusFactory` |
| Define a type-safe topic | `Topic::<T>::try_new` |
| Publish payloads or envelopes | `publish`, `publish_with_options`, `publish_envelope`, `publish_envelope_with_options`, `publish_all`, `publish_all_with_options`, `BatchPublishResult` |
| Subscribe handlers | `subscribe`, `subscribe_with_options`, `Subscription` |
| Configure retries and acknowledgements | `RetryOptions`, `SubscribeOptions`, `AckMode`, `Acknowledgement` |
| Configure publisher interceptors | `LocalEventBusFactory::add_publisher_interceptor`, `LocalEventBusFactory::add_global_publisher_interceptor`, `PublisherInterceptor`, `PublisherInterceptorAny` |
| Configure subscriber interceptors | `LocalEventBusFactory::add_subscriber_interceptor`, `LocalEventBusFactory::add_global_subscriber_interceptor`, `SubscriberInterceptor`, `SubscriberInterceptorAny` |
| Attach publish error handling | `PublishOptions` |
| Consume dead-letter events | `add_dead_letter_handler`, `DeadLetterPayload`, `standard_dead_letters_to`, `discard_dead_letters` |
| Observe internal callback failures | `add_error_observer` |
| Model transactional batches | `TransactionalEventBus`, `TransactionalPublisher`, `StagedEvent`, `StagedEventEnvelope` |
| Wait for scheduled handler work in tests | `wait_for_idle`, `wait_for_idle_timeout` |
| Shut down a local bus | `shutdown`, `shutdown_nonblocking`, `shutdown_with_timeout` |

## Core API At A Glance

| Type | Purpose |
| --- | --- |
| `EventBus` | Common event bus contract for concrete backends. |
| `EventBusFactory` | Common factory contract for backend creation and default configuration. |
| `LocalEventBus` | Thread-safe in-process event bus implementation. |
| `LocalEventBusFactory` | Creates buses with typed default publish options, subscribe options, interceptors, and typed or global dead-letter strategies. |
| `Topic<T>` | Type-safe event topic keyed by name and payload type. |
| `EventEnvelope<T>` | Event payload plus headers, timestamp, ordering key, delay, acknowledgement, and dead-letter marker. |
| `EventEnvelopeMetadata` | Type-erased metadata view used by global interceptors. |
| `PublishOptions<T>` | Publish retry metadata and publish error callbacks. |
| `SubscribeOptions<T>` | Subscriber acknowledgement mode, retry settings, filters, error callbacks, dead-letter strategy, and priority. |
| `PublisherInterceptor<T>` | Public interceptor contract that can enrich or drop outgoing envelopes. |
| `PublisherInterceptorAny` | Global publisher interceptor contract for metadata-only cross-cutting behavior. |
| `SubscriberInterceptor<T>` | Public around-style interceptor contract for subscriber handling. |
| `SubscriberInterceptorAny` | Global subscriber interceptor contract for metadata-only wrapping. |
| `BatchPublishResult` | Best-effort batch summary with accepted, dropped, and failed counts. |
| `DeadLetterPayload` | Standard dead-letter record containing metadata and the original type-erased payload. |
| `DeadLetterOriginalPayload` | Type-erased original payload stored in dead-letter records and global dead-letter callbacks. |
| `StagedEvent` | Type-erased staged event used by transactional backends for heterogeneous batches. |
| `StagedEventEnvelope<T>` | Typed staged event retaining an envelope and publish options. |
| `Subscription<T>` | Handle used to inspect and cancel a subscription. |
| `EventBusError` | Unified error type for lifecycle, validation, handler, lock, and type-erasure failures. |

## Project Scope

- `qubit-event-bus` is an in-process event bus. It does not persist events or provide cross-process delivery.
- Subscriber handlers run on a configurable `rs-thread-pool` fixed worker pool. Publishing schedules handler work and returns after dispatch.
- Payloads must be `Clone + Send + Sync + 'static` when published through `LocalEventBus`.
- Dead-letter strategies return `EventEnvelope<DeadLetterPayload>` so one dead-letter topic can receive archived records from multiple source event types.
- `standard_dead_letters_to`, `prefixed_dead_letters`, and `discard_dead_letters` cover common dead-letter routing policies without requiring custom closures.
- A subscription-level dead-letter strategy that returns `Ok(None)` disables fallback to a factory default strategy for that failed delivery. If no subscription or typed default strategy is configured, the local factory can use a global default dead-letter strategy.
- Interceptors are configured on `LocalEventBusFactory` before creating a bus. Runtime interceptor mutation is intentionally not part of `LocalEventBus`; use `add_error_observer` for runtime error observation.
- Explicit publish and subscribe options are merged with type-level factory defaults. Scalar subscribe settings such as acknowledgement mode and priority override defaults only when explicitly set through the builder.
- Manual NACK is treated as subscriber failure and participates in subscriber retry before error handlers or dead-letter routing run.
- Subscribe error handlers run in registration order until one records a new acknowledgement decision, or changes the decision to ACK.
- `publish_all` is best-effort after lifecycle and option validation. It submits every envelope in input order and returns `BatchPublishResult` with accepted, dropped, and failed counts. Envelopes with the same `ordering_key` are delivered serially per topic and subscriber; envelopes without an ordering key may run concurrently.
- `delay` defers local subscriber handling for at least the requested duration. Delayed work is scheduled by `rs-executor`'s `SingleThreadScheduledExecutorService` and does not occupy handler workers while waiting.
- Transactional traits use `StagedEvent` as the core batch abstraction. Typed convenience methods lower into staged events so backends can commit heterogeneous event batches atomically.
- Retry `attempt_timeout` options are rejected by `LocalEventBus` because local handlers do not receive a cooperative cancellation signal.
- Blocking `shutdown` must not be called from one of the same bus's subscriber worker threads. Use `shutdown_nonblocking` or `shutdown_with_timeout` from subscriber code.
- After `shutdown_with_timeout` reports a timeout, `start` is rejected until the old subscriber work has become idle.
- `wait_for_idle` and `wait_for_idle_timeout` are intended for tests and controlled shutdown flows that need to wait for scheduled handler work.

## Contributing

Issues and pull requests are welcome.

Please keep contributions focused and easy to review:

- open an issue for bug reports, design questions, or larger feature proposals
- keep pull requests scoped to one behavior change, fix, or documentation update
- follow the Rust coding style used by the existing `rs-*` projects
- include tests when changing runtime behavior
- update the README when public API behavior changes

By contributing to this project, you agree that your contribution will be licensed under the same license as the project.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
