# Qubit Event Bus (`rs-event-bus`)

[![Rust CI](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-event-bus/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-event-bus/coverage-badge.json)](https://qubit-ltd.github.io/rs-event-bus/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-event-bus.svg?color=blue)](https://crates.io/crates/qubit-event-bus)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-event-bus` is a lightweight, thread-safe, in-process publish/subscribe event bus for Rust. It provides typed topics and envelopes, configurable acknowledgements and retries, interceptors, dead-letter routing, delivery-failure observation, and best-effort batch publishing.

It is an in-process component: it does not persist events or deliver them across processes. For the complete scenario, API details, migration notes, and operational limits, see the [English user guide](doc/user_guide.md) or [中文用户指南](doc/user_guide.zh_CN.md). The [design guide](doc/design.md) and [设计说明](doc/design.zh_CN.md) describe the runtime model.

## Installation

```toml
[dependencies]
qubit-event-bus = "0.11"
```

## Quick start

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

`publish` returns a `PublishReceipt` describing admission, not handler completion. Use `wait_for_idle` in tests or controlled shutdown flows when the handler result must be observable.

## API at a glance

| Need | API |
| --- | --- |
| Create a bus | `LocalEventBus::new`, `LocalEventBus::started`, `LocalEventBusFactory` |
| Define a typed topic | `Topic::<T>::try_new` |
| Publish one or many events | `publish`, `publish_envelope`, `publish_all`, `BatchPublishResult` |
| Subscribe handlers | `subscribe`, `subscribe_with_options`, `Subscription` |
| Configure delivery capacity | `DeliveryLimits::bounded`, `DeliveryLimits::unbounded`, `LocalEventBusFactory::set_delivery_limits` |
| Configure retries and ACK/NACK | `SubscribeOptions`, `RetryPolicy`, `AckMode`, `Acknowledgement` |
| Configure interceptors | `PublisherInterceptor`, `SubscriberInterceptor`, and their global variants |
| Route dead letters | `standard_dead_letters_to`, `prefixed_dead_letters`, `discard_dead_letters` |
| Observe terminal failures | `add_delivery_failure_observer`, `DeliveryFailure` |
| Stop and test | `shutdown`, `shutdown_nonblocking`, `shutdown_with_timeout`, `wait_for_idle` |

## Important semantics

- `LocalEventBus` is non-transactional. `publish_all` is best effort: it submits every input envelope in order and records each per-event result. It does not provide atomic all-or-nothing publication.
- `BatchPublishResult::accepted_count()` counts input items whose receipt has at least one `DispatchStatus::Accepted` subscriber delivery. It is not a count of completed handlers, and it may coexist with `failure_count()` when another subscriber rejected the same item.
- `DeliveryLimits` independently configures the maximum accepted in-flight deliveries and an optional handler executor queue capacity. Both values must be positive when present; use `DeliveryLimits::bounded` or `DeliveryLimits::unbounded` as appropriate. The default is `DeliveryLimits::default()` (`4096`, no explicit queue capacity).
- Published payloads must be `Clone + Send + Sync + 'static`. Matching subscribers are scheduled on the local worker pool; publishing does not wait for handler completion.
- `AckMode::Manual` handlers must ACK or NACK before returning. A missing decision is a failure and may retry or reach dead-letter handling.
- Events sharing an `ordering_key` are serialized per topic and subscriber. Events without one may execute concurrently.
- Inside a handler, request shutdown with `shutdown_nonblocking()`. `shutdown_with_timeout()` cannot complete while that handler remains active and reports a timeout; reserve it for callers that require a bounded wait.
- Dropping a `Subscription` handle does not unsubscribe it; call its cancellation API. See the user guide for lifecycle, retry, delay, and shutdown details.

## Learn More

- [API reference](https://docs.rs/qubit-event-bus)
- [English user guide](doc/user_guide.md)
- [中文用户指南](doc/user_guide.zh_CN.md)
- [Design guide](doc/design.md)
- [设计说明](doc/design.zh_CN.md)
- [Changelog](CHANGELOG.md)
- [中文更新日志](CHANGELOG.zh_CN.md)
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
