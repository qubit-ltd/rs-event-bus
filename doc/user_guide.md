# Qubit Event Bus user guide

This guide describes `qubit-event-bus` 0.12 on Rust 1.94 or later. It is for Rust application developers who want typed event dispatch without coupling application code to a particular transport. The crate includes a synchronous in-process provider; third-party transport adapters are separate provider implementations.

[中文用户指南](user_guide.zh_CN.md) · [README](../README.md) · [API reference](https://docs.rs/qubit-event-bus)

## Purpose and boundaries

Suppose an order service must notify an audit handler after accepting an order. The event bus gives the publisher and subscriber a shared typed topic and a receipt that describes admission. The application can wait for this facade's tracked work when it needs to observe a handler effect. The local provider is appropriate for in-process work; it is not a message broker and does not persist or route messages across processes.

The facade separates application policy from provider transport. Synchronous and runtime-neutral asynchronous APIs sit above object-safe `EventBusSpi` and `AsyncEventBusSpi` contracts. Only the synchronous local provider ships in this crate. Tokio, crossbeam, flume, RabbitMQ, Kafka, and Redis adapters are not included.

## Conceptual model

- `Topic<T>` binds a validated topic name to a Rust payload type.
- `PublishRequest<T>` combines a typed topic, payload, envelope metadata, and publish policy. Its `new(topic, payload)` path supplies generated event identity and default options; its builder can set headers, ordering key, delay, retry, and interceptors.
- `SubscribeRequest<T>` combines `SubscriberId`, topic, and `SubscribeOptions<T>`. `new(subscriber_id, topic)` is the default-options path; the builder configures acknowledgement, filtering, middleware, retry, dead-letter policy, and provider-specific namespaced options.
- `Delivery<T>` exposes the event, delivery context, and ACK/NACK handle. It is not the transport settlement token.
- `PublishReceipt` reports the provider admission acknowledgement and provider identity, not handler completion.
- The provider SPI transports erased payloads and receives provider-specific subscription requests. `qubit-spi` registries select and create provider instances.

Subscriber middleware has distinct sync and async forms. A `SubscribeRequest` can carry either or both, but a synchronous `EventBus` rejects async middleware and an `AsyncEventBus` rejects synchronous middleware with a configuration error. This avoids blocking an async executor or pretending a sync callback is awaitable.

## Scenario: record an order locally

The success criterion is that an audit subscriber receives `order-1001` and the calling test can observe it before exit.

### Install and create a bus

```toml
[dependencies]
qubit-event-bus = "0.12"
```

```rust
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::EventBus;

let bus = EventBus::local(LocalEventBusConfig::default())?;
```

The local provider limits each subscription to 1024 outstanding messages by default. Queued and received but unsettled messages both use this capacity. `Retry` returns the same reservation to the queue. Set `LocalEventBusConfig::new().queue_capacity(n)` to choose another positive bound.

### Register, publish, and observe

```rust
use std::sync::{Arc, Mutex};

use qubit_event_bus::model::{PublishRequest, SubscribeRequest, Topic};
use qubit_event_bus::{DeliveryError, SubscriberId};

let orders = Topic::<String>::new("orders.created")?;
let received = Arc::new(Mutex::new(Vec::new()));
let captured = Arc::clone(&received);
let request = SubscribeRequest::new(SubscriberId::new("audit-log")?, orders.clone());
let subscription = bus.subscribe(request, move |delivery| {
    captured.lock().expect("received events should lock").push(delivery.payload().clone());
    Ok::<(), DeliveryError>(())
})?;

let receipt = bus.publish(PublishRequest::new(orders.clone(), "order-1001".to_owned())?)?;
assert_eq!(receipt.provider_id().as_str(), "local");
bus.wait_for_idle(&orders, None)?;
assert_eq!(received.lock().expect("received events should lock").as_slice(), &["order-1001"]);
subscription.cancel()?;
```

`wait_for_idle` asks the provider whether this topic has queued or unsettled work. It does not report handler success or global activity on a remote broker. Providers that cannot report this return `LifecycleError::IdleWaitUnsupported`. Use `wait_for_received_deliveries` to wait for work received and tracked by this facade.

### Decide what to do with a publish receipt

`publish` returning `Ok(receipt)` means the provider returned an acknowledgement. Inspect that acknowledgement before deciding whether application work needs compensation:

```rust
use qubit_event_bus::model::{AdmissionStatus, DestinationAdmission, PublishAcknowledgement, PublishReceipt};

let receipt: PublishReceipt = todo!("use the receipt returned by EventBus::publish");

match receipt.acknowledgement() {
    PublishAcknowledgement::Accepted { .. } => {
        // The broker accepted the event; it may not expose consumer identities.
    }
    PublishAcknowledgement::DroppedByInterceptor => {
        // An interceptor intentionally stopped dispatch.
    }
    PublishAcknowledgement::DestinationAdmissions(destinations) => {
        if destinations.is_empty() {
            // The provider reported no destinations (for example, no local subscribers).
        }
        for destination in destinations {
            match destination.status() {
                AdmissionStatus::Accepted => record_admission(destination),
                AdmissionStatus::Filtered => record_filtered(destination),
                AdmissionStatus::Rejected(reason) => record_rejection(destination, reason),
                _ => record_unknown_status(destination),
            }
        }
    }
    _ => record_unknown_acknowledgement(),
}

fn record_admission(_: &DestinationAdmission) {}
fn record_filtered(_: &DestinationAdmission) {}
fn record_rejection(_: &DestinationAdmission, _: &str) {}
fn record_unknown_status(_: &DestinationAdmission) {}
fn record_unknown_acknowledgement() {}
```

The recording functions above stand for application-owned policy. A local provider can accept one subscriber and reject another because its bounded queue is full; that partial result is still a successful receipt. `Filtered` means intentional exclusion, not queue pressure. Avoid blindly publishing the same event again after a partial result: an accepted subscriber could receive a duplicate. Use an idempotency key, or route rejected business work through an explicit compensation/retry policy.

## Core workflow and choices

### Add event metadata

Use the request builder when setting envelope fields without manually constructing an envelope:

```rust
use std::time::Duration;
use qubit_event_bus::model::{PublishRequest, Topic};

let request = PublishRequest::builder()
    .topic(Topic::<String>::new("orders.created")?)
    .payload("order-1002".to_owned())
    .header("trace-id", "trace-42")
    .ordering_key("customer-7")
    .delay(Duration::from_millis(25))
    .build()?;
```

The facade owns the reserved `x-qubit-event-bus-dead-letter` header. Applications and interceptors cannot set or remove it; providers must preserve it when transporting an event.

### Acknowledgement, retry, and error policy

Automatic acknowledgement accepts a successful handler result. With `AckMode::Manual`, the handler must call `delivery.acknowledgement().ack()` or `.nack()` before returning. Missing a decision is a delivery failure.

Retry policy types intentionally come from a direct `qubit-retry` dependency; `qubit-event-bus` does not re-export them. Add both dependencies when configuring retries:

```toml
[dependencies]
qubit-event-bus = "0.12"
qubit-retry = "0.25"
```

Use `qubit_retry::RetryPolicy` and, where needed, `qubit_retry::RetryRule` in the request/options builder. A retry classification rule does not itself enable retries: supply a policy too. A retry cancellation token prevents a subsequent attempt, but cannot interrupt a synchronous handler already running.

`FailureDirective` selects the terminal action (retry request, requeue, dead-letter, or discard) according to the configured policy and provider capabilities. Dead-letter handling is at-least-once around async cancellation: after an uncertain publish result, a resumed runner may publish the same dead letter again. Use event IDs or a business idempotency key for deduplication when required.

Facade-generated dead-letter payloads use `model::DeadLetterEvent<T>`. A consumer can subscribe to the configured dead-letter topic with that payload type and inspect the original event, failed `SubscriberId`, and terminal error text. The original `EventEnvelope<T>` is shared through `Arc`; `T` does not need to implement `Clone`. Encoded providers need a codec registered for `DeadLetterEvent<T>`.

Terminal publish error handlers receive `PublishFailureContext<T>`, which shares the payload and exposes event ID, topic, headers, ordering key, timestamp, and delay without requiring `T: Clone`. They run when the SPI publish operation fails directly if no retry policy is configured, or after configured retries reach a terminal failure. Request-building and other preflight errors do not invoke them.

### Sync and async middleware

`SubscriberInterceptor<T>` is synchronous and receives a continuation. `AsyncSubscriberInterceptor<T>` returns the crate's runtime-neutral boxed future and receives an async continuation. Middleware runs in registration order and may short-circuit by not invoking its continuation. A sync bus configured with async middleware or an async bus configured with sync middleware fails subscription setup with a configuration error.

## Selecting a provider

For the built-in sync path, `EventBus::local(LocalEventBusConfig)` is the shortcut. For explicit selection, create an `EventBusRegistry`, register provider definitions, optionally set a `qubit_spi::ProviderSelection`, and pass `EventBusConfig` to `create`. `EventBusRegistry::with_local()` registers the local provider with canonical ID `local` and aliases `memory` and `in-process`.

The registry uses `qubit-spi` fallback only while creating a backend. It does not switch transports after a runtime publish/receive failure. `RequiredCapabilities` lets callers reject a provider at creation when required durability, settlement, ordering, replay, delay, payload mode, or publish visibility is unavailable. Provider options are opaque namespaced key/value data interpreted by the selected adapter; do not put credentials in these debuggable values.

The async facade is runtime-neutral and does not spawn a consumer task. A backend-specific async provider is created through `AsyncEventBusRegistry`; the application drives the returned `AsyncSubscription` on its executor. `AsyncEventBus::new` uses the standard monotonic timer supplied by `qubit-clock`; `with_timer` or `with_config_and_timer` injects another `qubit_clock::Timer`. Idle waits, graceful-shutdown deadlines, and async retry delays rely on the timer future waking the executor when its deadline expires; the bus does not create a timer thread.

```rust
use qubit_event_bus::model::{SubscribeRequest, Topic};
use qubit_event_bus::{AsyncEventBus, DeliveryError, SubscriberId};

async fn consume(bus: &AsyncEventBus) -> Result<(), Box<dyn std::error::Error>> {
    let topic = Topic::<String>::new("orders.created")?;
    let request = SubscribeRequest::new(SubscriberId::new("audit-log")?, topic);
    let mut subscription = bus.subscribe(request).await?;
    subscription.run(|delivery| async move {
        audit(delivery.payload()).await?;
        Ok::<(), DeliveryError>(())
    }).await?;
    Ok(())
}

async fn audit(_order: &str) -> Result<(), DeliveryError> { Ok(()) }
```

This function assumes `bus` was created by an async provider adapter. No async local provider or concrete third-party transport adapter is bundled here.

## Errors and diagnostics

Errors are separated by operation (`PublishError`, `SubscribeError`, `ReceiveError`, `LifecycleError`, and `ShutdownError`) and retain SPI/provider sources where possible. Check a publish receipt's acknowledgement and register `observe_diagnostics` when provider gaps, unsupported dispositions, or callback failures need observation. Either `wait_for_idle`, `wait_for_received_deliveries`, or bus shutdown invoked from the bus's own sync worker returns `WouldDeadlock` instead of blocking itself.

`Subscription::cancel` explicitly stops and joins a sync subscription from an external caller; dropping its handle does not unsubscribe. `AsyncSubscription::close().await` provides deterministic async cleanup and close errors. Dropping an async subscription handle immediately drops its paused receiver and owned work; the provider must recover unsettled deliveries on receiver drop. Async `run` does not spawn; dropping its future pauses and retains owned delivery futures and permits. A later `run` resumes those futures (the replacement handler only handles newly received messages), and bus shutdown can take over a paused session. The async receiver's close/drop contract must leave every unsettled delivery recoverable and must never acknowledge it implicitly.

## Limitations and best practices

- A successful publish means the provider returned its admission acknowledgement. It does not prove subscriber handler completion.
- `publish_all` attempts each request independently in input order and retains each result. It is not atomic.
- Local queue capacity bounds queued and unsettled messages per subscription; a received message continues to occupy capacity until settlement. It is not a global broker quota. The local provider is in-process and non-durable.
- Both facades bound admitted work bus-wide. Sync uses `with_sync_delivery_scheduler(...)` for `max_in_flight` and handler queue capacity. Async uses `EventBusFacadeConfig::with_delivery_admission(DeliveryAdmissionConfig::new(max_in_flight)?)` (default 4); its permit covers each received message through lane wait, middleware, handler/retry, and settlement. Async subscriptions process different ordering keys concurrently while preserving order within each key. A received but unadmitted message is buffered per subscription; idle receive calls do not consume permits.
- Diagnostic observers run synchronously on the thread emitting the diagnostic. Panics are contained, but a blocking observer can delay that thread; diagnostics are not buffered in a separate queue.
- Capability flags are the provider's declared contract. Select required capabilities explicitly and document any stronger provider-specific guarantee separately.
- Settlement retries after an uncertain async result require providers to make the same token/disposition idempotent. A conflicting disposition for one token must fail.
- Shutdown stops admission and coordinates subscriptions. `Immediate` drops work that has not started and receives no more messages, but waits for the active handler, settlement, subscription close, and SPI shutdown so it can return all errors. Rust cannot forcibly interrupt a handler. Synchronous `Graceful` applies its deadline to the caller's wait for the whole close sequence, including already admitted publish/subscribe SPI calls, receiver close, and provider shutdown. When it returns `TimedOut`, the bus remains `Closing`, rejects new operations, and one background coordinator continues cleanup; call shutdown again to wait for its result, or request `Immediate` to strengthen the attempt. A blocked synchronous provider call or handler can keep that coordinator alive. Async shutdown is driven by its future; dropping a timed-out/cancelled future does not roll back provider side effects, so async providers must support idempotent close/shutdown retries. SPI `shutdown(mode)` closes provider transport resources; the facade owns the preceding stop/settle/close order.

## Further reading

- [README](../README.md) · [API reference](https://docs.rs/qubit-event-bus)
- [Architecture status (English)](design.md) · [SPI design (English)](spi_design.md) · [正式 SPI 设计（中文）](spi_design.zh_CN.md)
- [Changelog](../CHANGELOG.md) · [中文用户指南](user_guide.zh_CN.md)
