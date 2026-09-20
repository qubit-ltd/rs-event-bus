# Qubit Event Bus user guide

This guide targets `qubit-event-bus` 0.11 and Rust 1.94+. It is for application developers embedding a typed, in-process event bus. The examples use `LocalEventBus`; the crate also exposes the `EventBus` and `EventBusFactory` contracts for other backends.

## Conceptual model

An event has a typed `Topic<T>`, an `EventEnvelope<T>`, and zero or more matching subscriptions. A publish call performs publisher interception and subscriber admission, then schedules accepted handler work on the local worker pool. A `PublishReceipt` reports that admission snapshot; it does not wait for handlers.

`LocalEventBus` is deliberately non-transactional. `publish_all` is a best-effort loop over the input envelopes. It continues after per-event errors and returns `BatchPublishResult`; no API in this release provides a transactional staged-event contract.

## Scenario: audit an order event

The success criterion is that an audit subscriber receives an order and the test can observe the handler before exiting.

### Install and start

```toml
[dependencies]
qubit-event-bus = "0.11"
```

```rust
use qubit_event_bus::{LocalEventBus, Topic};

let bus = LocalEventBus::started()?;
let orders = Topic::<String>::try_new("orders.created")?;
```

`LocalEventBus::new()` creates a stopped bus. Use `start()` to start it, or `started()` for the common create-and-start path.

### Subscribe and publish

```rust
use std::sync::{Arc, Mutex};

let received = Arc::new(Mutex::new(Vec::new()));
let captured = Arc::clone(&received);
bus.subscribe("audit-log", &orders, move |event| {
    captured.lock().expect("received events should lock").push(event.payload().clone());
    Ok(())
})?;

let receipt = bus.publish(&orders, "order-1001".to_string())?;
assert!(matches!(receipt.outcome(), qubit_event_bus::PublishOutcome::Dispatched(_)));
bus.wait_for_idle(&orders)?;
assert_eq!(received.lock().expect("received events should lock").as_slice(), &["order-1001".to_string()]);
```

The subscription handler receives an `EventEnvelope<T>`, so it can inspect headers, event ID, ordering key, delay, and payload. Payloads published through `LocalEventBus` must be `Clone + Send + Sync + 'static`.

## Core workflow

Use `publish_envelope` when the event needs explicit envelope metadata, and `publish_with_options` or `publish_envelope_with_options` for publish retry/error options. `subscribe_with_options` adds acknowledgement mode, filters, priority, retry settings, error handlers, and dead-letter strategy.

For manual acknowledgement, decide before returning:

```rust
use qubit_event_bus::{AckMode, SubscribeOptions};

let options = SubscribeOptions::<String>::builder()
    .ack_mode(AckMode::Manual)
    .build();
bus.subscribe_with_options("manual-audit", &orders, |event| {
    event.acknowledgement().expect("manual ACK should be available").ack();
    Ok(())
}, options)?;
```

Returning `Ok(())` without ACK or NACK is a handler failure. It participates in retry and then error/dead-letter handling. A decision made after the handler returns cannot change that delivery.

## Batch publication and admission

`publish_all` and `publish_all_with_options` submit envelopes in input order. Inspect each `BatchPublishItem::result()` for its `PublishReceipt` or global `EventBusError`.

`BatchPublishResult` has three deliberately different views:

| Method | Meaning |
| --- | --- |
| `accepted_count()` | Number of input items with at least one subscriber status `DispatchStatus::Accepted`. |
| `dropped_count()` | Number of items dropped by a publisher interceptor. |
| `failure_count()` | Number of globally failed items or items with any rejected subscriber delivery. |

Counts are not mutually exclusive: one item can have an accepted and a rejected subscriber, so it contributes to both `accepted_count()` and `failure_count()`. `accepted_count()` does not mean a handler completed successfully.

## Capacity and concurrency

Configure local admission and executor capacity before creating the bus:

```rust
use qubit_event_bus::{DeliveryLimits, LocalEventBusFactory};

let mut factory = LocalEventBusFactory::new();
factory.set_delivery_limits(DeliveryLimits::bounded(4096, Some(128)))?;
factory.set_subscription_handler_pool_size(4)?;
let bus = factory.create_started()?;
```

`max_in_flight` and `handler_queue_capacity` must be greater than zero when present. The default is `DeliveryLimits::default()`: 4096 accepted in-flight deliveries and no explicit handler queue capacity. Queue rejection appears as `DispatchStatus::Rejected(EventBusError::ExecutionRejected { .. })`; the corresponding handler is not run. Use `add_error_observer` to monitor execution failures.

Matching handlers run on the subscription worker pool. Events with the same `ordering_key` are serialized per topic and subscriber; events without an ordering key may run concurrently. Retry backoff occupies the calling or handler worker thread, so size worker and retry budgets together.

## Interceptors, retries, and dead letters

Configure typed or global publisher/subscriber interceptors on `LocalEventBusFactory` before `create()` or `create_started()`. Runtime interceptor mutation is not part of `LocalEventBus`.

`RetryPolicy` controls retry attempts and backoff. A retry rule classifies failures; a rule alone does not enable retries. Subscriber retry cancellation uses `SubscribeOptionsBuilder::retry_cancellation_token`; cancellation wakes backoff and prevents the next attempt but cannot interrupt a handler already running.

Dead-letter strategies can be attached to subscription options or factory defaults. `standard_dead_letters_to`, `prefixed_dead_letters`, and `discard_dead_letters` cover common routing needs. `DeliveryFailure` observers receive terminal failures after retry, error handling, and dead-letter routing finish.

## Lifecycle, errors, and troubleshooting

- Publishing or subscribing a stopped bus returns a lifecycle error. `shutdown()` is blocking; from a subscriber worker use `shutdown_nonblocking()` or `shutdown_with_timeout()`.
- `wait_for_idle` and `wait_for_idle_timeout` are for tests and controlled draining. Calling them from the bus's own subscriber worker returns `EventBusError::WouldDeadlock`.
- After `shutdown_with_timeout` reports a timeout, `start()` remains rejected until old subscriber work becomes idle.
- A successful publish means dispatch admission completed, not eventual handler delivery. Inspect receipt statuses and register error/delivery-failure observers when losses matter.
- For a delayed delivery rejected at expiry, the handler does not run; observe `ExecutionRejected` through `add_error_observer`.

## Migration notes

The old transactional API (`TransactionalEventBus`, `TransactionalPublisher`, `StagedEvent`, and `StagedEventEnvelope`) is removed. Replace it with ordinary `publish_all` only when best-effort, non-atomic semantics are acceptable; otherwise provide transaction coordination in the application or backend.

`DeliveryLimits` replaces the single max-in-flight tuning point. Use `DeliveryLimits::bounded(max_in_flight, handler_queue_capacity)` or `DeliveryLimits::unbounded(max_in_flight)` with `LocalEventBusFactory::set_delivery_limits`; zero values are rejected.

Code consuming `BatchPublishResult::accepted_count()` must adopt its current meaning: accepted subscriber admission, not accepted input count, completed handler count, or all-subscriber success. Use `failure_count()` and each receipt status for rejection details.

Generic `EventBus` implementations expose a backend-owned associated `Subscription<T>` constrained by `SubscriptionHandle<T>`. Generic code should use `B::Subscription<T>` rather than a concrete local `Subscription<T>`.

## Further reading

- [API reference](https://docs.rs/qubit-event-bus)
- [Design guide](design.md)
- [中文用户指南](user_guide.zh_CN.md)
- [设计说明（中文）](design.zh_CN.md)
