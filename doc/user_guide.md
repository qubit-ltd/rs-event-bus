# Qubit Event Bus user guide

[中文用户手册](user_guide.zh_CN.md) · [README](../README.md) · [API reference](https://docs.rs/qubit-event-bus)

This guide covers `qubit-event-bus` 0.12 on Rust 1.94 or later. It is for Rust application developers who need several local tasks to react to one business event. In an order service, calling the audit writer and customer-view updater directly from order creation makes that path depend on both implementations and their failure handling. With this bus, the order path publishes a typed event; each task owns its subscription. The included provider works within one process and does not persist events.

## Scenario and success criteria

An order service has accepted `order-1001`. Its audit trail and customer view must each observe the order. We will register both consumers on `orders.created`, publish once, wait for local work to settle, and check both recorded effects. Adding another in-process consumer then requires another subscription, rather than a change to the order publisher.

This example simulates the two effects with memory-backed vectors so it is runnable. In a real service, replace those writes with application operations and define how those operations handle repeated deliveries and failures. Publication and a database transaction are not atomic here.

## Conceptual model

| Object | Role in this scenario |
| --- | --- |
| `Topic<T>` | Gives `orders.created` a Rust payload type. Both subscribers use the same `Topic<String>`. |
| `SubscribeRequest<T>` | Identifies a subscriber and its topic. The returned `Subscription` must be retained and explicitly cancelled. |
| `PublishRequest<T>` | Carries one event and its publish options. |
| `PublishReceipt` | Reports provider admission. It does not report handler completion. |
| `EventBus` and local provider | The facade applies shared policy; the provider routes events to in-process subscribers. |

## Install and run the example

Add the dependency to an application using Rust 1.94 or later:

```toml
[dependencies]
qubit-event-bus = "0.12"
```

Put this program in `src/main.rs` and run `cargo run`:

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

The program creates the bus and two subscriptions before publishing. `wait_for_idle` waits until the local provider has no queued or unsettled messages for this topic. The assertions then verify the two effects; waiting alone cannot prove that either handler succeeded. `cancel()` stops the synchronous subscription workers, and `shutdown` closes the bus. Do not call these blocking lifecycle methods from one of this bus's own synchronous handlers; they can return `LifecycleError::WouldDeadlock`.

## When publication is only partly accepted

A successful `publish` call returns the provider's admission report, which can still contain rejected destinations. For the local provider, inspect `receipt.admission_outcome()` and then `receipt.acknowledgement()` when individual subscriber results matter. Possible outcomes include accepted, partly accepted, none accepted, no destinations, an interceptor drop, or opaque acceptance from a provider that does not reveal destinations. `receipt.check_admission(requirement)` checks this existing receipt; it does not publish again or wait for consumers.

If the audit subscriber accepted an event but the customer-view subscriber's bounded queue rejected it, publishing the whole event again may duplicate audit work. Record the event ID or a business idempotency key, decide how to repair the rejected effect, and make each handler safe for any retry policy you choose. `Filtered` is an intentional exclusion, not queue pressure. A returned `PublishError` is separate from a receipt with partial admission; inspect its source before deciding whether a retry is safe.

## Policy and provider choices

- Use the request builders for headers, an ordering key, delay, interceptors, and retry options. The facade reserves the `x-qubit-event-bus-dead-letter` header. Retry policy types come from a direct `qubit-retry = "0.25"` dependency; a classification rule alone does not enable retries.
- Automatic acknowledgement accepts a successful handler result. With `AckMode::Manual`, the handler must explicitly call `delivery.acknowledgement().ack()` or `.nack()`; returning without a decision is a delivery failure. Dead-letter delivery requires an appropriate topic and, with an encoded provider, a codec for `DeadLetterEvent<T>`.
- `EventBus::local(LocalEventBusConfig::default())` is the built-in path. `EventBusRegistry::with_local()` registers the same provider as `local`, with `memory` and `in-process` aliases. `RequiredCapabilities` checks a provider's declared features at creation; registry fallback happens during creation, not after a runtime failure.
- `AsyncEventBus` is runtime-neutral, but this crate supplies no async local provider or broker adapter. An application must provide an async SPI implementation and drive `AsyncSubscription::run` on its own executor; `run` does not spawn a task.

## Errors and diagnostics

| Symptom | Check and response |
| --- | --- |
| `PublishError` | Check validation, codec/capability requirements, provider source, and whether any destination may already have accepted the event before retrying. |
| Receipt says `NoDestinations` or `NoneAccepted` | Confirm subscriptions exist and are still active; inspect destination admissions for filtering or rejection. |
| `LifecycleError::IdleWaitUnsupported` | The selected provider cannot report provider-wide topic idleness. `wait_for_received_deliveries` only tracks work this facade already received. |
| A handler effect is missing after idle wait | Check handler results and application logs; idle means settled work, not business success. Register `observe_diagnostics` for provider gaps and callback or disposition failures. |
| Shutdown times out | Synchronous graceful shutdown bounds the caller's wait. The bus rejects new work while background cleanup continues; call shutdown again to observe its result. |

Errors are separated by operation, including `PublishError`, `SubscribeError`, `LifecycleError`, and `ShutdownError`. Diagnostic observers run synchronously on the emitting thread, so keep callbacks short. `publish_metrics()` counts admission outcomes; it is not a handler-success counter.

## Local provider resource guidance

The included provider is synchronous, in-process, and non-durable. Each synchronous subscription has a blocking receive worker thread. `LocalEventBusConfig::new().queue_capacity(n)` sets a positive outstanding-message bound **per subscription** (default 1024), counting queued and received-but-unsettled messages. A retry keeps its reservation. The facade's scheduling limits are a separate layer; neither limit alone is a total memory budget. Size both subscriber count and queue capacity for the application's workload. `cargo bench --bench local_threads` and `cargo bench --bench local_scale` provide measurements on your own host, not portable guarantees.

`publish_all` attempts each request independently and is not transactional. The local provider does not offer durable recovery or cross-process delivery. A provider must explicitly declare ordering and settlement capabilities before the facade can use them. For `OrderingPolicy::PerKey`, it must declare `PerKey` or `PerSubscription` ordering. If the business process requires a durable handoff or an atomic database-and-event commit, design that mechanism separately and use an appropriate provider.

## Further reading

- [README](../README.md) · [中文用户手册](user_guide.zh_CN.md) · [API reference](https://docs.rs/qubit-event-bus)
- [Architecture status](design.md) · [SPI design](spi_design.md) · [Changelog](../CHANGELOG.md)
