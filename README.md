# Qubit Event Bus (`rs-event-bus`)

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-event-bus.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-event-bus)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-event-bus/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-event-bus?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Docs.rs](https://docs.rs/qubit-event-bus/badge.svg)](https://docs.rs/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Documentation: [API Reference](https://docs.rs/qubit-event-bus)

`qubit-event-bus` is a lightweight, thread-safe, in-process event bus for Rust applications.

It provides type-safe topics, event envelopes, subscriber options, publish options, retries, acknowledgement handles, publisher and subscriber interceptors, and dead-letter routing hooks.

## Why Use It

Use `qubit-event-bus` when you need:

- type-safe publish/subscribe routing inside one process
- consistent event metadata through `EventEnvelope`
- automatic or manual acknowledgement for subscriber handlers
- subscriber retry and dead-letter behavior
- publisher interceptors that can modify or drop outgoing events
- subscriber interceptors that can wrap, observe, or short-circuit handler execution
- deterministic test synchronization through `wait_for_idle`

## Installation

```toml
[dependencies]
qubit-event-bus = "0.1.1"
```

## Quick Start

```rust
use std::sync::{Arc, Mutex};

use qubit_event_bus::{LocalEventBus, Topic};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = LocalEventBus::started();
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
| Publish payloads or envelopes | `publish`, `publish_envelope`, `publish_envelope_with_options`, `publish_all`, `publish_async` |
| Subscribe handlers | `subscribe`, `subscribe_with_options`, `Subscription` |
| Configure retries and acknowledgements | `RetryOptions`, `SubscribeOptions`, `AckMode`, `Acknowledgement` |
| Add publisher interceptors | `add_publisher_interceptor` |
| Add subscriber interceptors | `add_subscriber_interceptor`, `SubscriberInterceptorChain` |
| Attach publish error handling | `PublishOptions` |
| Wait for scheduled handler work in tests | `wait_for_idle` |

## Core API At A Glance

| Type | Purpose |
| --- | --- |
| `LocalEventBus` | Thread-safe in-process event bus implementation. |
| `LocalEventBusFactory` | Creates started buses with typed default subscription options. |
| `Topic<T>` | Type-safe event topic keyed by name and payload type. |
| `EventEnvelope<T>` | Event payload plus headers, timestamp, ordering key, delay, acknowledgement, and dead-letter marker. |
| `PublishOptions<T>` | Publish retry metadata and publish error callbacks. |
| `SubscribeOptions<T>` | Subscriber acknowledgement mode, retry settings, filters, error callbacks, dead-letter strategy, and priority. |
| `DeadLetterPayload` | Cloneable type-erased payload for dead-letter envelopes. |
| `Subscription<T>` | Handle used to inspect and cancel a subscription. |
| `EventBusError` | Unified error type for lifecycle, validation, handler, lock, and type-erasure failures. |

## Project Scope

- `qubit-event-bus` is an in-process event bus. It does not persist events or provide cross-process delivery.
- Subscriber handlers run on a configurable `rs-thread-pool` fixed worker pool. Publishing schedules handler work and returns after dispatch.
- Payloads must be `Clone + Send + Sync + 'static` when published through `LocalEventBus`.
- Dead-letter strategies return `EventEnvelope<DeadLetterPayload>` so one dead-letter topic can receive archived payloads from multiple source event types.
- `wait_for_idle` is intended for tests and controlled shutdown flows that need to wait for scheduled handler work.

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
