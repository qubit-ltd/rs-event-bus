# Qubit Event Bus user guide

[Chinese user guide](user_guide.zh_CN.md) · [README](../README.md) · [API reference](https://docs.rs/qubit-event-bus)

This guide covers `qubit-event-bus` 0.20.0 on Rust 1.94 or later. It is for Rust application developers who need several modules to react to one business event. Developers who write a transport implementation only need [Write a transport yourself](#write-a-transport-yourself). Reading through [Check the publication result](#check-the-publication-result) is enough to integrate the built-in in-process bus. Later sections cover message metadata, ordering, failure handling, configuration, async use, and third-party implementations.

## Contents

- [The problem it solves](#the-problem-it-solves)
- [Where to start](#where-to-start)
- [Integrate an order service](#integrate-an-order-service)
  - [Define the shared event](#define-the-shared-event)
  - [Register both handlers](#register-both-handlers)
  - [Publish after the transaction commits](#publish-after-the-transaction-commits)
  - [Types used on this path](#types-used-on-this-path)
- [Check the publication result](#check-the-publication-result)
  - [What success looks like](#what-success-looks-like)
- [Add message details when needed](#add-message-details-when-needed)
- [Keep events for one object in order](#keep-events-for-one-object-in-order)
- [Handle failures and retries](#handle-failures-and-retries)
  - [Retry after a database write fails](#retry-after-a-database-write-fails)
  - [Let the handler decide when to acknowledge](#let-the-handler-decide-when-to-acknowledge)
  - [Keep an event that fails for good](#keep-an-event-that-fails-for-good)
- [Filter or intercept a message](#filter-or-intercept-a-message)
  - [Skip events the handler should not see](#skip-events-the-handler-should-not-see)
  - [Run code around the handler](#run-code-around-the-handler)
  - [Change or stop one publication](#change-or-stop-one-publication)
  - [Change or stop every publication](#change-or-stop-every-publication)
- [Configure the built-in local event bus](#configure-the-built-in-local-event-bus)
  - [Create the bus directly](#create-the-bus-directly)
- [Choose and connect a third-party implementation](#choose-and-connect-a-third-party-implementation)
  - [Use an implementation someone else wrote](#use-an-implementation-someone-else-wrote)
  - [Write a transport yourself](#write-a-transport-yourself)
  - [Encode events for a cross-process implementation](#encode-events-for-a-cross-process-implementation)
- [Asynchronous bus and subscriptions](#asynchronous-bus-and-subscriptions)
- [Non-blocking notification entry](#non-blocking-notification-entry)
  - [Create the notification publisher at startup](#create-the-notification-publisher-at-startup)
  - [Enqueue on the request path](#enqueue-on-the-request-path)
  - [Drain the queue during shutdown](#drain-the-queue-during-shutdown)
- [Lifecycle, waiting, and shutdown](#lifecycle-waiting-and-shutdown)
  - [Shut down a sync bus](#shut-down-a-sync-bus)
  - [Wait for a topic to become idle](#wait-for-a-topic-to-become-idle)
  - [Shut down an async bus](#shut-down-an-async-bus)
- [Errors, diagnostics, and troubleshooting](#errors-diagnostics-and-troubleshooting)
- [Boundaries and a practice checklist](#boundaries-and-a-practice-checklist)
- [Further reading](#further-reading)

## The problem it solves

Take an order service. After the order transaction commits, more work remains: write an audit record, and refresh the customer-order view that support staff query. Later the service may also send a notification or sync a warehouse. If the order service calls each of those modules itself, the order module depends on every downstream module. Each new follow-up changes the order path, and that path has to absorb every downstream failure and delay.

With an event bus, the order service does one thing after the commit: publish an `OrderCreated` event to the topic `orders.created`, carrying the order id, customer id, and amount. The audit module and the customer-view module each subscribe to that topic at startup and handle the event on their own. Publisher and subscribers share only the event type. They do not depend on each other, and a new consumer does not require a change to the order service.

This crate separates business code from the way a message actually travels, behind one publish and subscribe API. A different backend yields an in-process, cross-process, or cross-node bus. The included `local` implementation delivers messages only **inside the same process**. If the process exits after the order commits and before the message is sent, or while a handler is still running, the message is not replayed. Work that must complete reliably, such as the audit record, still needs an application-owned durable record and a compensation path. For cross-process delivery, see [Choose and connect a third-party implementation](#choose-and-connect-a-third-party-implementation). The relevant steps below state this boundary as well.

## Where to start

1. [Integrate an order service](#integrate-an-order-service) covers the event type, the subscriptions, and the publish call.
2. [Check the publication result](#check-the-publication-result) shows what a successful receipt looks like, and the difference between “the message was admitted” and “the business work is done.” The basic integration ends there.
3. Read on as needed: [Add message details when needed](#add-message-details-when-needed), [Keep events for one object in order](#keep-events-for-one-object-in-order), [Handle failures and retries](#handle-failures-and-retries), [Configure the built-in local event bus](#configure-the-built-in-local-event-bus), [Asynchronous bus and subscriptions](#asynchronous-bus-and-subscriptions), or [Choose and connect a third-party implementation](#choose-and-connect-a-third-party-implementation).

Third-party implementations, codecs, and extension interfaces come after the basic path.

## Integrate an order service

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
    let _subscription = bus.subscribe(
        SubscribeRequest::new("audit", topic.clone())?,
        move |delivery| {
            sender.send(delivery.payload().clone()).unwrap();
        },
    )?;
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


Add the dependency:

```toml
[dependencies]
qubit-event-bus = "0.20"
```

The order, audit, and customer-view modules are separate parts of the application. The application injects its database access objects. `OrderRepository`, `AuditStore`, and `CustomerViewStore` stand for the interfaces that talk to real storage. Integration has three steps: define the shared event, register both subscribers at startup, and publish after the order transaction commits.

### Define the shared event

```rust
// src/orders/events.rs
use qubit_event_bus::model::Topic;

pub struct OrderCreated {
    pub order_id: String,
    pub customer_id: String,
    pub total_cents: u64,
}

impl OrderCreated {
    pub const TOPIC: Topic<Self> = Topic::new_static("orders.created");
}
```

`OrderCreated` is the payload type. `orders.created` is its topic (`Topic`). `Topic<T>` carries the payload type, so the publisher and the subscribers share one `Topic<OrderCreated>` and the compiler checks the payload. Use `Topic::new_static` when the name is fixed in code. Use `Topic::<T>::new(name)?` when the name comes from configuration; it rejects an illegal name. A field or semantic change has to be checked against every subscriber.

### Register both handlers

```rust
// src/audit.rs
use std::sync::Arc;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::SubscribeRequest;

use crate::orders::events::OrderCreated;

pub trait AuditStore: Send + Sync {
    fn append_order_created(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}

pub fn subscribe(
    bus: &EventBus,
    store: Arc<dyn AuditStore>,
) -> Result<Subscription, Box<dyn std::error::Error>> {
    let request = SubscribeRequest::new("audit-log", OrderCreated::TOPIC)?;
    Ok(bus.subscribe(request, move |delivery| {
        store.append_order_created(delivery.payload())
    })?)
}
```

```rust
// src/customer_view.rs
use std::sync::Arc;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::SubscribeRequest;

use crate::orders::events::OrderCreated;

pub trait CustomerViewStore: Send + Sync {
    fn upsert_order(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}

pub fn subscribe(
    bus: &EventBus,
    store: Arc<dyn CustomerViewStore>,
) -> Result<Subscription, Box<dyn std::error::Error>> {
    let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?;
    Ok(bus.subscribe(request, move |delivery| {
        store.upsert_order(delivery.payload())
    })?)
}
```

At startup, create the bus and the subscriptions before accepting order requests:

```rust
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;

let bus = EventBus::local(LocalEventBusConfig::default())?;
let audit_subscription = audit::subscribe(&bus, audit_store)?;
let view_subscription = customer_view::subscribe(&bus, view_store)?;
let order_bus = bus.clone(); // inject into the order service; keep bus and both handles
```

`audit_store` and `view_store` are storage objects the application has already built. The `Subscription` returned by `subscribe` is the subscription handle. Hold it, and call `cancel()` during shutdown. Dropping a synchronous subscription handle does not cancel the subscription. The closure passed to `bus.subscribe` is the handler. It calls the store when an event arrives. A store error is returned to the bus, and the subscription policy decides whether to retry or record the failure. See [Handle failures and retries](#handle-failures-and-retries).

### Publish after the transaction commits

```rust
// src/orders/service.rs
use qubit_event_bus::EventBus;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;

use super::events::OrderCreated;

pub struct CreateOrder {
    pub customer_id: String,
    pub total_cents: u64,
}

pub struct CommittedOrder {
    pub order_id: String,
    pub customer_id: String,
    pub total_cents: u64,
}

pub trait OrderRepository: Send + Sync {
    // A successful return means the order transaction has committed.
    fn create_and_commit(
        &self,
        command: CreateOrder,
    ) -> Result<CommittedOrder, Box<dyn std::error::Error>>;
}

pub fn create_order(
    repository: &dyn OrderRepository,
    bus: &EventBus,
    command: CreateOrder,
) -> Result<PublishReceipt, Box<dyn std::error::Error>> {
    let order = repository.create_and_commit(command)?;
    let event = OrderCreated {
        order_id: order.order_id,
        customer_id: order.customer_id,
        total_cents: order.total_cents,
    };
    Ok(bus.publish(PublishRequest::new(OrderCreated::TOPIC, event)?)?)
}
```

Publish `OrderCreated` only after `create_and_commit` returns successfully. The caller should also check the receipt and record the event id, the order id, and the failure reason:

```rust
use qubit_event_bus::model::AdmissionRequirement;

let receipt = orders::service::create_order(repository, &order_bus, command)?;
if receipt.duplicate_possible() {
    eprintln!("reconcile uncertain publication by event ID: {}", receipt.input_event_id().as_str());
} else {
    receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected)?;
}
```

This check only means that at least one subscriber admitted the message and that no subscriber explicitly rejected it. It does **not** mean the audit record and the customer view have been written. The next section, [Check the publication result](#check-the-publication-result), shows what else the receipt contains and how to tell the failure cases apart. The order commit and the publication are separate operations. A process exit or a publish failure after the commit leaves an order with no event. If the audit record must be durable, write a pending event (an outbox row) in the same order transaction and let a background task publish and compensate. Retrying the whole order request does not close that gap.

### Types used on this path

| Type | Role |
| --- | --- |
| `EventBus` | Bus handle for publish, subscribe, and shutdown. `clone` it into each module. |
| `Topic<OrderCreated>` | Topic `orders.created` with payload type `OrderCreated`. |
| `SubscribeRequest` | Subscription request: subscriber id, topic, and optional subscribe options. |
| `PublishRequest` | Publication request: topic, payload, and optional event id, headers, and other metadata. |
| `Subscription` | Subscription handle, used to retain and cancel the subscription. |
| `PublishReceipt` | Publication receipt. It reports provider admission, not handler completion. |
| `local` | Built-in in-process provider. Messages are gone after the process exits. |

In the API, a `provider` is the backend that actually delivers messages. `local` is one of them. The business code above depends only on the bus interface.

## Check the publication result

`Err(PublishFailure)` from `bus.publish(...)` means the call did not obtain an admission report. `Ok(receipt)` can still mean that only some destinations received the message. `receipt` is that report. `receipt.admission_outcome()` distinguishes:

| Outcome | Meaning |
| --- | --- |
| `Accepted(summary)` | At least one destination admitted the message, and none rejected it. Some destinations may still have been skipped by a rule. |
| `PartiallyAccepted(summary)` | Someone admitted it and someone rejected it. Publishing the whole event again can make an admitted subscriber handle it twice. |
| `NoneAccepted(summary)` | Destinations were found, but none admitted the message. Inspect skip and rejection reasons. |
| `NoDestinations` | No destination was found. Check that subscriptions exist and that the topic name matches. |
| `OpaqueAccepted` | The transport says it accepted the message and does not identify the destinations. |
| `Dropped` | A publish interceptor discarded the message before delivery. |

### Decide whether a failed publication can be repeated

`publish` and each `publish_all` item return `PublishFailure`, including `event_id()`, `effect()`, and `cause()`; `source()` preserves the cause chain. `NotificationOutcome::PublishFailed` and `PublishErrorHandler` receive the same wrapper. `NotAccepted` means no admission occurred; `MayHaveBeenAccepted` means a provider may have accepted the event even though the caller got an error. Keep the event ID in application telemetry and reconcile uncertain results with the business store or an idempotent consumer.

`PublishOptions::builder().duplicate_risk_policy(...)` defaults to `DuplicateRiskPolicy::Forbid`. This hard gate stops retries after an uncertain attempt, ahead of custom `RetryRule` decisions. `AllowDuplicates` permits the configured `qubit-retry` policy to consider another attempt; it neither enables retry by itself nor guarantees that retry will occur. Retries reuse the event ID, timestamp, and encoded bytes. Typed interceptors may transform the envelope but cannot change its event ID.

Uncertainty is retained across the whole logical publication: a later definite rejection cannot turn an earlier uncertain attempt into `NotAccepted`. If permitted retries eventually succeed, `receipt.duplicate_possible()` is true when an earlier attempt was uncertain. Successful provider admission still does not prove handler completion or persistence.

Cancellation is also a result boundary. If `RetryCancellationToken` cancels an already-polled SPI attempt and the call returns an error, its effect is uncertain. Cancelling before the SPI is called has no admission effect. Dropping the public publish future produces no returned failure; dropping an unpolled future makes no attempt, while dropping an already-started operation requires the application to retain its event ID and treat the outcome as possibly published. RetryPolicy budgets are soft budgets and do not promise a hard timeout of every in-flight provider operation, especially synchronous I/O.

Dead-letter forwarding uses the same uncertainty gate. A failed or uncertain forward stops the source subscription and leaves durable source work unsettled. Forwarding and source acknowledgement are separate operations: a successful forward followed by a failed source settlement can produce the same logical dead-letter again. Consumers must deduplicate; neither a Redis EventId nor this policy provides exactly-once delivery.

### What success looks like

In the order example, both the audit and customer-view subscriptions already exist. After the order transaction commits, one `OrderCreated` is published. A normal receipt looks like this:

```rust
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;

let receipt = bus.publish(PublishRequest::new(OrderCreated::TOPIC, event)?)?;
// The built-in local provider id is "local".
println!("provider = {}", receipt.provider_id().as_str());
match receipt.admission_outcome() {
    AdmissionOutcome::Accepted(summary) => {
        // In the order example: summary.accepted == 2, summary.filtered == 0, summary.rejected == 0.
        println!(
            "event {} was admitted by {} subscribers",
            receipt.input_event_id().as_str(),
            summary.accepted
        );
    }
    other => eprintln!("admission was not a clean acceptance: {other:?}"),
}
if let PublishAcknowledgement::DestinationAdmissions(destinations) = receipt.acknowledgement() {
    for destination in destinations {
        // Two lines, in no fixed order: audit-log -> Accepted, customer-view -> Accepted.
        println!("{} -> {:?}", destination.subscriber_id().as_str(), destination.status());
    }
}
```

`Accepted`, with both destinations in status `Accepted`, means the order scenario is integrated. That is success at the bus layer: the message has entered both subscribers' queues, and the handlers then run on the bus worker threads. Whether the audit row and the customer view were written is visible only in the store's return value and logs. The receipt does not say.

The receipt reports only whether each subscriber **admitted** the message. Handlers have usually not run yet. `check_admission` tests the receipt against the condition you pass. It only reads the receipt: it does not publish again, and it does not wait for handlers.

When one order event goes to both audit and the customer view, the two conditions differ like this:

- `AtLeastOneAccepted`: one admission is enough. If audit admits the event and the customer view rejects it because its queue is full, the check still passes.
- `AtLeastOneAcceptedAndNoRejected`: at least one admission, and nobody rejected. The case above returns `RejectedDestinations`. The order example uses this condition.

These two conditions require a provider that reports destination admissions. The order example uses local, which does. With an opaque provider such as Redis, `publish_checked` returns `CheckedPublishError::UnsupportedVisibility { event_id, provider_id }` before interceptors, codec encoding, metrics, or the provider publish call. If the application only needs the provider to accept the event, use `ProviderOrDestinationAccepted` instead:

```rust
use qubit_event_bus::CheckedPublishError;
use qubit_event_bus::EventBus;
use qubit_event_bus::model::{AdmissionRequirement, PublishReceipt, PublishRequest};

fn publish_to_redis<T: Send + Sync + 'static>(
    bus: &EventBus,
    request: PublishRequest<T>,
) -> Result<PublishReceipt, CheckedPublishError> {
    bus.publish_checked(request, AdmissionRequirement::ProviderOrDestinationAccepted)
}
```

For Redis, success means `XADD` was accepted by the broker; the returned receipt has opaque destination visibility. It does not prove a consumer ran, a business write completed, or Redis performed a disk fsync. On a provider with visible destinations, this new condition accepts any reported destination acceptance, even alongside rejections; `NoDestinations` and `NoneAccepted` return `CheckedPublishError::Admission` with the full receipt. `Dropped` also fails. Inspect the receipt and reconcile uncertain or partial work before retrying.

After publishing, the order service checks the stricter condition and branches on the failure:

Check history for the whole logical publication before interpreting the final admission. These two compiled files define an application decision; they do not publish automatically. `Dropped` does not republish; `NoAcceptedDestination` belongs to `AdmissionCheckError`, not `AdmissionOutcome`.

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/republish_action.rs -->
```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Application decisions after examining publication evidence.

/// A decision for the application; this enum performs no publication itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum RepublishAction {
    /// Query by the original event ID or use an idempotent reconciliation path.
    ReconcileByEventId,
    /// No attempt admitted the event; a whole-event retry can be considered.
    RepublishWhole,
    /// Repair only rejected destinations; others have already accepted.
    RetryRejectedDestinations,
    /// Do not automatically repeat accepted, opaque, or intentionally dropped work.
    NoRepublish,
}
```

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/receipt_safety.rs -->
```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Check retained history before the final attempt's admission summary.

use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::PublishReceipt;

use crate::republish_action::RepublishAction;

/// Chooses an application action without publishing or altering the receipt.
#[must_use]
#[inline]
pub fn republish_action(receipt: &PublishReceipt) -> RepublishAction {
    if receipt.duplicate_possible() {
        return RepublishAction::ReconcileByEventId;
    }
    match receipt.admission_outcome() {
        AdmissionOutcome::NoDestinations | AdmissionOutcome::NoneAccepted(_) => {
            RepublishAction::RepublishWhole
        }
        AdmissionOutcome::PartiallyAccepted(_) => RepublishAction::RetryRejectedDestinations,
        _ => RepublishAction::NoRepublish,
    }
}
```


`Filtered` on a receipt means the transport decided, during admission, that the message does not belong to that subscriber. It is not a rejection: if another subscriber admitted the message, both conditions pass. It is not an admission either: if every destination is `Filtered`, `accepted` is 0, the outcome is `NoneAccepted`, and both conditions return `NoAcceptedDestination`. A full queue is a rejection (`Rejected`). Do not treat the two as the same thing. Only a transport that can evaluate a filter during admission reports `Filtered`. The built-in local provider does not. On local, a subscription `filter` runs after the bus has already taken the message. A filtered order still shows as `Accepted` on the receipt; its handler is simply not called.

Some transports report only that the message was accepted, without listing subscribers. That is `OpaqueAccepted` in the table. The crate cannot evaluate either condition, so `check_admission` returns `VisibilityUnavailable`. The built-in local provider reports each subscriber and does not produce this outcome.

To see who rejected the message, walk the per-subscriber status on the receipt:

```rust
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::PublishAcknowledgement;

if let PublishAcknowledgement::DestinationAdmissions(destinations) = receipt.acknowledgement() {
    for destination in destinations {
        if let AdmissionStatus::Rejected(reason) = destination.status() {
            eprintln!("{} rejected admission: {reason}", destination.subscriber_id().as_str());
        }
    }
}
```

On a partial admission, do not republish the original event unchanged. Audit has already admitted it, and a second publication makes audit handle it again. Record the event id, who admitted it, and who rejected it, then repair only the rejected part. Each subscriber also has to tolerate duplicates. The audit module can use “order id + audit event type” as a unique key and skip the insert when the same order-created event arrives again. Handling that has the same result when it runs once or many times is **idempotent**.

`receipt.input_event_id()` is the event id on the publish request. When a publish interceptor drops the message before delivery, the message was not sent and `dispatched_event_id()` is `None`.

The basic order integration ends here. The following sections are optional: extra publish metadata and ordering first, then subscriber failure handling and interception, then capacity, third-party implementations, async use, and shutdown.

### Diagnose settlement termination and restore consumption

Settlement retry is separate from rerunning a business handler. `SettlementRetryConfig::default()` permits 5 attempts including the first, a 5-second elapsed budget, 10 ms initial backoff, and a 1-second backoff cap. Only `SpiError::retryable() == Some(true)` retries. `Some(false)` stops immediately and `None` stops conservatively. Attempts use the same token and disposition; the handler is not rerun. A retry budget does not interrupt an in-flight blocking SPI call or a future that never becomes ready.

On permanent failure, exhausted attempts/deadline, invalid token, panic, or timer/infrastructure failure, that subscription stops receiving and starting handlers. Its first `SubscriptionStopReason::Settlement` preserves event ID, disposition, attempts, termination and `Arc<SpiError>` with the source chain. Already-started handlers may finish; other subscriptions continue. Inspect the retained reason and snapshots:

```rust
if let Some(reason) = subscription.terminal_failure() {
    eprintln!("stopped: {reason:?}");
}
let per_subscription = subscription.delivery_metrics();
let global = bus.delivery_metrics();
eprintln!("subscription={per_subscription:?}, global={global:?}");
```

The bus snapshot exposes `reserved_receives`, `queued`, `running_handlers`, `settling`, and `lane_waiting` (a subset of queued), plus settlement attempts/retries/terminal failures, completed/abandoned counts, handler and settlement duration totals/counts/maxima, and `oldest_owned_age`. Gauges and totals need not be a transactionally consistent cross-thread read. Closed handles keep their final subscription counts; the bus retains aggregate counters, not a permanent index of every event or key. Native payload size and codec allocations remain outside these count bounds.

For an order consumer that stops, retain the reason and `Diagnostic::SettlementFailed` / `SettlementStopped`, fix the provider, codec, capacity, or policy, close the old subscription, then create a new subscription with the same durable group. Redis can claim outstanding PEL work under its configured policy; entries already ACKed cannot be recovered by replaying the failure alone. Local resubscription starts empty, so use application compensation. Async `run` reports `ReceiveError::Stopped`; rerunning that same stopped handle does not clear the reason. Merely cancelling a live `run` future pauses its owned tasks and timers; resume `run` or let shutdown take over.

Diagnostic observers run synchronously on the emitting executor. Forward records through an application-owned bounded, nonblocking queue; panic isolation does not isolate a slow observer.

## Add message details when needed

`PublishRequest::new(topic, payload)?` is enough to publish, and it generates an event id. Use the request builder when you need to choose the id, attach a request id for log correlation, or control processing order for one customer:

```rust
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::PublishRequest;

let request = PublishRequest::builder()
    .topic(OrderCreated::TOPIC)
    .payload(event)
    .event_id(EventId::new("order-created-42")?)
    .header("correlation-id", "request-42")
    .ordering_key("customer-7")
    .build()?;
let receipt = bus.publish(request)?;
```

The event id answers “is this the same event?”. A retry of the same business event should keep the same id. Do not reuse one id for different events. A `header` is text that travels with the message. A request id belongs there; a password does not. The crate reserves `x-qubit-event-bus-dead-letter`. An `ordering_key` keeps events for one business object in order, and the subscriber has to opt in. See [Keep events for one object in order](#keep-events-for-one-object-in-order). The builder can also set a timestamp, a delay, retry, and interceptors. Those options appear later.

To publish several events of the same type, call `publish_all`. It tries each input in order and keeps each success or error in `BatchPublishResult::items()`. A failure does not stop the later items, and the call is not a database transaction around the batch.

## Keep events for one object in order

By default the bus does **not** promise that a subscriber handles events in publish order. The default `ordering_policy` is `OrderingPolicy::None`. On the built-in local bus, both the sync and async buses allow up to 4 handlers to run at once. Two events received by the same subscriber can be in progress together, and a later publication can finish first. Other transports decide their own behavior. Do not assume order.

Many cases depend on order. The customer-view module keeps “the latest order” for each customer. Customer `customer-7` places `order-42` and then `order-43`. If the two `OrderCreated` events run concurrently, `order-43` can be written first and then overwritten by `order-42`, so the view treats the earlier order as the latest. Balance changes and order-status transitions have the same problem whenever a later event depends on the earlier result.

Both sides have to be configured.

**Publisher: set an ordering key.** The key is an application string that identifies the business object whose events must stay in order. It must not be empty, and it must not have leading or trailing whitespace or control characters. Otherwise `build()` returns `InvalidOrderingKey`. To order one customer's orders into the view, use the customer id:

```rust
let request = PublishRequest::builder()
    .topic(OrderCreated::TOPIC)
    .payload(event)
    .ordering_key("customer-7") // in real code, order.customer_id
    .build()?;
bus.publish(request)?;
```

**Subscriber: request per-key order.** Set `ordering_policy` to `PerKey`:

```rust
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;

let options = SubscribeOptions::builder()
    .ordering_policy(OrderingPolicy::PerKey)
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, handler)?;
```

With both sides configured, the customer-view subscription behaves as follows:

- **Same key.** `order-42` and `order-43` for `customer-7` are handled one after another, in the order they entered this subscription. The handler for `order-43` starts only after `order-42` settles; termination does not start successors.
- **Different keys.** An already-received, eligible `customer-8` event can progress independently when a handler slot is available. Receiving B requires owned capacity: an infinite upstream backlog of A, or exhausted capacity, can prevent B from being received. Round-robin fairness across subscriptions and keys applies only to eligible work while executors keep progressing.
- **Other subscriptions.** Ordering applies only to a subscription that requested `PerKey`, and each subscription orders itself. If audit did not request `PerKey`, its `customer-7` events can still arrive out of order. Which of the two subscriptions runs first is not guaranteed.
- **Other topics.** Lanes are per topic. Events with the same key on `orders.created` and on another topic are not ordered against each other. To order across event types, put those events on one topic, for example with an enum payload.

Also keep these limits in mind:

- **One side alone does nothing.** If the publisher sets a key and the subscriber does not request `PerKey`, the key is ignored and handling stays unordered. If the subscriber requests `PerKey` and the event has no key, every keyless event on that subscription shares one lane and is handled one at a time: order holds, and parallelism is gone.
- **The transport must support per-key order.** When the subscription is created, the bus checks the implementation. If it does not support the capability, `subscribe` returns `SubscribeError::Capability(CapabilityError::Unsupported { capability: "ordering.per_key" })`. It does not silently fall back to unordered delivery. Both the sync and async local providers support it.
- **Order means the order of entry into the subscription.** On one thread, that is the order of the publish calls. When several threads publish the same key concurrently, which call enters first is a race. The publisher has to serialize those calls itself.
- **Key granularity is the parallelism.** A customer id serializes one customer and lets different customers run together. An order id orders only the events of one order. A constant key serializes the whole subscription. A slow or blocking handler holds every later event for that key. Handlers should not block for long.

## Handle failures and retries

`SubscribeRequest::new(subscriber_id, topic)?` uses the defaults: only messages published after the subscription, no filter, and no automatic handler retry. On success the crate acknowledges the message to the transport. That acknowledgement is an **ACK**. To change these settings, build options with `SubscribeOptions::<T>::builder()` and attach them with `request.with_options(options)`, or use `SubscribeRequest::builder()` directly.

| Goal | Entry point | Notes |
| --- | --- | --- |
| Filter events | `filter` | After the bus has taken the message and before the handler runs, inspect the event and decide whether to skip it. On local, a skipped message is still `Accepted` on the publish receipt. A skip is not a rejection. See [Filter or intercept a message](#filter-or-intercept-a-message). |
| Let the handler decide when to acknowledge | `ack_mode(AckMode::Manual)` | After the business write, call `delivery.acknowledgement().ack()`. On failure, call `nack()`. Returning without a decision counts as failure. See [Let the handler decide when to acknowledge](#let-the-handler-decide-when-to-acknowledge). |
| Retry after failure | `retry_policy`, optionally `retry_rule` / `retry_cancellation_token` | The policy sets the attempt count and the delay. A classification rule alone does not enable retries. These types require a direct `qubit-retry = "0.25"` dependency. See [Retry after a database write fails](#retry-after-a-database-write-fails). |
| Choose an action after failure | `error_handler` | The handler can ask for a retry, a requeue, a move to a failure topic, or a discard. Requeue requires support from the transport. |
| Keep an event that fails for good | `dead_letter(DeadLetterPolicy::with_topic_name(name)?)` | Forward the failed message to another topic (the dead-letter topic). Someone still has to subscribe and handle it. See [Keep an event that fails for good](#keep-an-event-that-fails-for-good). |
| Handle one customer's messages in order | `ordering_policy(OrderingPolicy::PerKey)` | The publisher must set an ordering key, and the transport must support the capability. See [Keep events for one object in order](#keep-events-for-one-object-in-order). |
| Share work across instances, or read older messages | `consumer_group`, `durability`, `start_position` | Only a transport that supports these capabilities can use them. local does not support durable subscriptions or historical reads. |
| Transport-specific parameters | `provider_option` | The implementation defines the meaning. Do not put a password here. |

A handler may return `()` or `Result<(), DeliveryError>`. Return an error when a database write fails, so the crate applies the subscription policy. `delivery.payload()` is the business data. `delivery.event()` carries the event id and attached information. `delivery.context()` carries the transport, the subscriber, and which attempt this is (`retry_attempt()`, starting at 1). Retries can deliver the same event more than once, so handlers should use a business unique key. The two subscriptions below show the three settings used most often.

### Retry after a database write fails

When the customer-view write occasionally times out, let the crate retry. Retry policy types come from `qubit-retry`, so the application depends on `qubit-retry = "0.25"` directly:

```rust
use std::time::Duration;

use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_retry::BackoffPolicy;
use qubit_retry::RetryPolicy;

let options = SubscribeOptions::<OrderCreated>::builder()
    .retry_policy(
        RetryPolicy::builder()
            .max_attempts(3)
            .backoff(BackoffPolicy::fixed(Duration::from_millis(200)))
            .build()?,
    )
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, move |delivery| store.upsert_order(delivery.payload()))?;
```

When the handler returns `Err`, the crate calls it again after 200 milliseconds, up to 3 attempts in total. After the third failure the message is `Discard`: no further retry, and no dead-letter topic. The callback registered with `observe_diagnostics` receives one `Diagnostic::DeliveryFailed` carrying the attempt count and the final error. Setting `retry_policy` alone enables retries. `retry_rule` chooses which errors are worth retrying; setting only the rule does not start retries. The message keeps its local outstanding slot for the whole retry.

### Let the handler decide when to acknowledge

By default, a handler that returns normally is acknowledged (ACK). If the audit module should acknowledge only after the row is stored, set `ack_mode` to `Manual` and call `ack()` after the write:

```rust
use qubit_event_bus::DeliveryError;
use qubit_event_bus::model::AckMode;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;

let options = SubscribeOptions::<OrderCreated>::builder()
    .ack_mode(AckMode::Manual)
    .build();
let request = SubscribeRequest::new("audit-log", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, move |delivery| {
    store.append_order_created(delivery.payload())?;
    delivery
        .acknowledgement()
        .ack()
        .map_err(|error| DeliveryError::Handler { source: Box::new(error) })?;
    Ok(())
})?;
```

In manual mode, `Ok` without `ack()` is still a failed attempt. `nack()` is an explicit rejection. Repeating `ack()` on the same message is safe. `nack()` after `ack()`, or the reverse, returns `AlreadyCompleted`. An ACK is the handler's confirmation to the bus. It does not replace a database transaction.

### Keep an event that fails for good

If a failed customer-view write should be kept for a later repair instead of discarded, configure a dead-letter topic. The failing subscriber returns `FailureDirective::DeadLetter` from `error_handler` and names the topic with `dead_letter`. The reader subscribes to that topic. Its payload type is `DeadLetterEvent<OrderCreated>`, not `OrderCreated`:

```rust
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

// The dead-letter topic carries DeadLetterEvent<OrderCreated>, not OrderCreated.
let dead_letter_topic = Topic::<DeadLetterEvent<OrderCreated>>::new("orders.created.dead")?;

// Failing subscriber: move a failed delivery to the topic above.
let options = SubscribeOptions::<OrderCreated>::builder()
    .error_handler(|_event, _error| FailureDirective::DeadLetter)
    .dead_letter(DeadLetterPolicy::with_topic(&dead_letter_topic))
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let view_subscription = bus.subscribe(request, move |delivery| store.upsert_order(delivery.payload()))?;

// Reader: subscribe to the same topic and record the reason for a person or a background task.
let dead_letter_subscription = bus.subscribe(
    SubscribeRequest::new("dead-letter-reader", dead_letter_topic)?,
    |delivery| {
        let dead = delivery.payload();
        eprintln!(
            "subscriber {} gave up on order {}: {}",
            dead.subscriber_id().as_str(),
            dead.original_event().payload().order_id,
            dead.reason()
        );
    },
)?;
```

When `error_handler` returns `DeadLetter`, the facade forwards the failure to the dead-letter topic, even if `retry_policy` is also set. If forwarding fails, the async runner stops with `ReceiveError::DeadLetterForwardFailed`; the sync facade cancels that subscription. The source token remains unsettled and diagnostics report the failure. Recovery depends on provider durability and close semantics: a durable provider can redeliver the unsettled message after the subscription is resumed or recreated, while an ephemeral provider may discard it when closed. Do not assume the facade will rerun the handler while forwarding remains broken. The dead-letter event ID is stable for the same original event and subscriber, which helps consumers deduplicate, but does not promise exactly-once delivery. The dead-letter topic is ordinary: someone must subscribe to it, and an empty topic can still lose the record. Encoded transports also need a codec for `DeadLetterEvent<OrderCreated>`. `NoDestinations`, `NoneAccepted`, `Dropped`, and publish errors do not count as forwarding. Known partial acceptance completes the source delivery with a diagnostic because republishing the whole record may duplicate it. An opaque provider such as Redis meets the default policy only when its publish guarantee is at least `Accepted`. Use `DeadLetterPolicy::with_known_destination(topic_name)` when a reported consumer admission is required; subscription creation rejects that policy for opaque providers. Watch diagnostics for forwarding failures.

## Filter or intercept a message

A **filter** runs on the receiving side: before the handler, it decides whether this message should be given to the handler. An **interceptor** is application code on the publish or handling path. It is the place to add correlation data, write a log, or stop the rest of the path. A normal integration does not need an interceptor first.

On a subscriber, the bus runs `filter` first. A filter that returns `true` continues. One that returns `false` settles the delivery as accepted and skips every interceptor and the handler. After a passing filter, a bus-wide subscriber interceptor runs, then the interceptor on that subscription, then the handler. Each interceptor receives `next` and must call it to reach the rest of the chain. A sync subscription registers that callback with `interceptor`. An async subscription uses `async_interceptor`. Putting the async callback on a sync bus, or the sync callback on an async bus, fails when the subscription is created.

### Skip events the handler should not see

The customer-view subscriber can ignore an order whose total is zero:

```rust
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;

let options = SubscribeOptions::<OrderCreated>::builder()
    .filter(|event| event.payload().total_cents > 0)
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, move |delivery| store.upsert_order(delivery.payload()))?;
```

A zero-total order never reaches `upsert_order`. On the built-in local provider the publish receipt for that destination is still `Accepted`. A panic inside `filter` is reported as a handler failure.

### Run code around the handler

The audit subscriber logs the order id and then calls the handler through `next`:

```rust
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;

let options = SubscribeOptions::<OrderCreated>::builder()
    .interceptor(|delivery, next| {
        eprintln!("audit received {}", delivery.payload().order_id);
        next(delivery)
    })
    .build();
let request = SubscribeRequest::new("audit-log", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, move |delivery| store.append_order_created(delivery.payload()))?;
```

`next(delivery)` is the rest of the chain. The log line appears, then the store write. Returning without calling `next` skips the handler. The value returned from the interceptor is the delivery result.

The same synchronous callback can wrap every `OrderCreated` subscription on one bus. It runs outside the per-subscription interceptor, and only after that subscription's filter returns `true`. Install it while building the bus settings, then pass those settings through `EventBusConfig`. `EventBus::local` does not accept this configuration:

```rust
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::SubscriberNext;

let bus_settings = EventBusFacadeConfig::new().subscriber_interceptor(
    |delivery: Delivery<OrderCreated>, next: SubscriberNext<OrderCreated>| next(delivery),
);
```

An async subscription awaits its own `next`. The callback's return type is `SpiFuture`:

```rust
use qubit_event_bus::DeliveryError;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::spi::SpiFuture;

let options = SubscribeOptions::<OrderCreated>::builder()
    .async_interceptor(|delivery: Delivery<OrderCreated>, next: AsyncSubscriberNext<OrderCreated>| {
        Box::pin(async move { next(delivery).await }) as SpiFuture<'static, Result<(), DeliveryError>>
    })
    .build();
```

Attach `options` with `SubscribeRequest::with_options` before `subscribe(...).await`. The bus-wide async equivalent is `EventBusFacadeConfig::async_subscriber_interceptor`.

### Change or stop one publication

A publisher interceptor on the request sees the envelope for that publication only. Return `Ok(None)` to stop, or `Ok(Some(envelope))` to continue with the envelope you return:

```rust
use qubit_event_bus::model::PublishRequest;

let request = PublishRequest::builder()
    .topic(OrderCreated::TOPIC)
    .payload(event)
    .interceptor(|mut envelope| {
        if envelope.payload().total_cents == 0 {
            return Ok(None);
        }
        envelope.set_header("request-id", "req-42")?;
        Ok(Some(envelope))
    })
    .build()?;
let receipt = bus.publish(request)?;
```

`Ok(None)` finishes before the provider is called. `receipt.admission_outcome()` is `Dropped`, and a bus-wide publisher interceptor does not run for that call. `Ok(Some(envelope))` continues with the headers on the returned envelope. `Err` fails the publication.

### Change or stop every publication

`EventBusFacadeConfig::publisher_interceptor` runs for every message sent through that bus object, after a request interceptor that returned an envelope. It may edit headers. It cannot change the payload or the event id. `Ok(false)` stops publication; `Ok(true)` continues:

```rust
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishMetadata;

let local = LocalEventBusConfig::default();
let bus_settings = EventBusFacadeConfig::new().publisher_interceptor(|metadata: &mut PublishMetadata| {
    if metadata.header("suppress") == Some("true") {
        return Ok(false);
    }
    metadata.set_header("service", "orders")?;
    Ok(true)
});
let config = EventBusConfig::default()
    .with_provider_options(local.provider_options())
    .with_facade_config(bus_settings);
let bus = EventBusRegistry::with_local()?.create(&config)?;
```

`Ok(false)` leaves the admission outcome as `Dropped` and does not call the provider. A publish error handler only observes a terminal publish failure. It does not turn that failure into success, and a dropped publication does not invoke it.

## Configure the built-in local event bus

### Create the bus directly

`LocalEventBusConfig` sets transport capacity:

```rust
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;

let local = LocalEventBusConfig::new()
    .queue_capacity(2_048)
    .max_total_outstanding(20_000);
let bus = EventBus::local(local)?;
```

`queue_capacity` limits how many messages **one subscriber** may have outstanding. The default is 1,024. `max_total_outstanding` limits outstanding deliveries **across every subscriber of this local instance**. The default is 65,536. A message that has been taken but not finished counts, and a retry keeps its slot. Both values must be greater than zero. The unit is a delivery, not a byte. When a queue is full, that subscriber may reject the message while others still accept it. Terminal settlement or provider cleanup releases the outstanding slot; handler completion alone does not. Size these from measured handling speed and memory. A message count is not a memory budget.

To bound application-declared native payload weight as well as delivery count, opt in to `max_total_outstanding_weight_bytes` and supply a `native_payload_weight` callback for each concrete payload type you publish. For example, a `String` publisher can declare its UTF-8 length (at least one byte for an empty string):

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/local_capacity.rs -->
```rust
// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Compiled local-provider capacity example used by both user guides.

use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

/// Publishes one weighed `String`, waits for its handler, and closes the bus.
///
/// # Errors
/// Returns an error if bus construction, subscription, admission, delivery wait,
/// or graceful shutdown fails.
pub fn publish_with_local_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let budget = NonZeroUsize::new(8 * 1024 * 1024).expect("positive weight budget");
    let local = LocalEventBusConfig::new()
        .queue_capacity(2_048)
        .max_total_outstanding(20_000)
        .max_total_outstanding_weight_bytes(budget);
    let bus = EventBus::local(local)?;
    let topic = Topic::<String>::new("orders.created")?;
    let (sender, receiver) = mpsc::channel();
    let _subscription = bus.subscribe(
        SubscribeRequest::new("audit", topic.clone())?,
        move |delivery| {
            sender.send(delivery.payload().clone()).expect("receiver remains open");
        },
    )?;
    let options = PublishOptions::<String>::builder()
        .native_payload_weight(|payload| {
            NonZeroUsize::new(payload.len().max(1)).expect("positive payload weight")
        })
        .build();
    let request = PublishRequest::new(topic, "order-42".to_owned())?.with_options(options);
    let _receipt = bus.publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)?;
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(3))?,
        "order-42"
    );
    let shutdown = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })?;
    assert_eq!(shutdown.outcome, ShutdownOutcome::Complete);
    Ok(())
}
```

The budget is disabled by default. The callback sees the payload after publisher interceptors and runs once before provider retries. When enabled, a native publish without a declared weight fails with a non-retryable provider error before any destination is enqueued. Each accepted fanout delivery consumes one copy of the declared weight; rejected destinations do not. A retry keeps its reservation until terminal settlement or cleanup. Treat the weight as an application estimate, not a measurement or hard RSS/process-memory cap: shared allocations, queues, handlers, and transport overhead still consume memory. Monitor the payload-weight distribution, queue admission rejections, and handler latency together, then tune the estimate and limits from observed load. Encoded providers use the separate encoded-payload byte limits.

Both facades use `DeliverySchedulingConfig`: defaults are 4 running handlers, 256 owned deliveries globally, 32 owned deliveries per subscription, and 256 registered subscriptions. Owned includes a pre-receive reservation, queued, running, and settling work. Reservation happens before receive, with no extra pending message outside the limit. Same-key queues and settlement backoff do not consume handler slots; a lane stays owned until settlement succeeds or the subscription terminates. All four parameters are `NonZeroUsize`; running and per-subscription limits must not exceed the global owned limit.

```rust
use std::num::NonZeroUsize;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::local::LocalEventBusConfig;

let scheduling = DeliverySchedulingConfig::new(
    NonZeroUsize::new(8).unwrap(),
    NonZeroUsize::new(256).unwrap(),
    NonZeroUsize::new(32).unwrap(),
    NonZeroUsize::new(64).unwrap(),
)?;
let local = LocalEventBusConfig::new().queue_capacity(2_048).max_total_outstanding(20_000);
let config = EventBusConfig::default()
    .with_provider_options(local.provider_options())
    .with_facade_config(EventBusFacadeConfig::new().with_delivery_scheduling(scheduling));
let bus = EventBusRegistry::with_local()?.create(&config)?;
```

The registration limit bounds sync receiver threads and async sessions; a paused async subscription still counts. Excess registration fails before provider subscription creation. Both required capacity keys and the optional weight key in `local.provider_options()` must be positive; unknown keys and nonnumeric values fail creation. `EventBus::local` sets provider capacity; use registry assembly for facade configuration.

For encoded transports, `EventBusFacadeConfig::with_payload_limits(PayloadLimits::new(publish_limit, receive_limit))` sets two positive `NonZeroUsize` limits. Both default to 1,048,576 bytes; exactly the limit is allowed. Publishing checks completed codec output before calling the provider; receiving checks bytes before any codec callback. There is no unlimited setting. This does not cap allocations inside encoding or the transport client. Native Rust payloads have no automatically measured byte-size check because their retained memory cannot be measured reliably by the facade; the optional local weight budget instead uses the application's declaration. The local queue count limit still counts deliveries, not bytes.

The async bus reuses the same `config`, changing only assembly:

```rust
use qubit_event_bus::AsyncEventBusRegistry;

let bus = AsyncEventBusRegistry::with_local()?.create(&config).await?;
```

Sync local starts one receive thread per subscriber, and handlers run on the bus's shared worker threads. Async local does not start a receive thread per subscriber, but the application must keep `AsyncSubscription::run` running. With many subscriptions, measure on the deployment machine with `cargo bench --bench local_threads` and `cargo bench --bench local_scale`. Someone else's benchmark result is not a fixed capacity.

## Choose and connect a third-party implementation

By how far a message can travel, an event bus falls into three kinds:

- **In-process.** Publisher and subscribers are in the same process, and messages move through memory. No extra service is required, and latency is low. Messages that are not finished when the process exits are lost.
- **Cross-process.** Publisher and subscribers are different processes on one machine. Messages travel through an operating-system IPC mechanism or a message service running on that machine.
- **Cross-node.** Publisher and subscribers are on different machines. Messages travel over the network, usually through middleware such as Kafka, RabbitMQ, or Redis. That middleware often also provides persistence and redelivery.

The core of this crate is one abstraction. Business code is written against the same publish and subscribe API, and a replaceable backend (a provider) delivers the message. Pairing the API with backends of different reach produces an in-process, cross-process, or cross-node bus without rewriting the publish and subscribe code. Capabilities still differ. Whether messages are persisted, and which delivery policies are supported, has to be checked for the chosen backend.

This crate currently ships only the `local` implementation, an in-process bus. It does not include Kafka, RabbitMQ, Redis, or similar backends. Cross-process or cross-node delivery has two routes: use an implementation someone else wrote, or write one. Both are described below.

### Use an implementation someone else wrote

Suppose a crate connects to a message server. Add that crate as a dependency, then tell the bus to use it. One way is explicit registration: build `EventBusRegistry::new()`, call `register` with that implementation, then `create(&config)`. Sync implementations go in `EventBusRegistry`. Async implementations go in `AsyncEventBusRegistry`, and async creation uses `.await`. `provider_ids()` lists registered ids. `EventBusConfig::with_selection` selects one of them.

Some third-party crates register themselves. At link time the crate places its definition in a catalog. That mechanism is `discovery`. Enable the feature and make sure the crate is linked:

```toml
qubit-event-bus = { version = "0.20", features = ["discovery"] }
qubit-spi = "0.13"
# Also add the chosen provider crate's real package name and version.
```

```rust
use provider_crate as _; // replace with the real crate name so its provider definition is linked
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_spi::ProviderSelection;

let registry = EventBusRegistry::discover()?;
registry.set_default_selection(ProviderSelection::named("your-provider-id")?)?;
registry.seal();
let bus = registry.create(&EventBusConfig::default())?;
```

Replace `provider_crate` and `your-provider-id` with the real crate name and the id from its documentation. If the program cannot find an implementation, inspect `provider_ids()` first. `discover()` fails when two implementations use the same selection name. The built-in `local` provider auto-registers only in the **synchronous** catalog. Async local has to be added explicitly with `AsyncEventBusRegistry::with_local()`. The sync and async catalogs are not interchangeable. Both local implementations use the id `local`, and they can also be selected as `memory` or `in-process`. Sync local can also be added explicitly with `EventBusRegistry::with_local()`.

`EventBusConfig::with_provider_options` passes that implementation's own configuration. `with_facade_config` configures how the bus handles messages. `with_required_capabilities` states what the application requires. `RequiredCapabilities::new().durable()` means “messages must be persistable.” The built-in local provider cannot do that, so creation fails. The registry tries another candidate only **when the bus is created**. A publish or receive failure at runtime does not switch implementations. `ProviderOptions` can appear in debug output. Do not put a password or a token there.

### Compare local and Redis capabilities

These are the actual `EventBusCapabilities` values for both sync and async providers:

| Capability | local (core 0.20) | Redis Streams (provider 0.7) |
| --- | --- | --- |
| `payload_modes` | `Native` | `Encoded` (register a codec) |
| `durability` | `Ephemeral` | `Durable` |
| `subscription_modes` | `EPHEMERAL` | `DURABLE` |
| `consumer_groups` | `false` | `true` |
| `replay` | `None` | `Position` |
| `ordering` | `PerKey` | `None`; per-key requests are rejected |
| `delayed_delivery` | `Native` | `None`; delay requests are rejected |
| `settlement` | `AcceptRetryReject` | `AcceptRetryReject` |
| `publish_guarantee` | `Accepted` | `Accepted` (successful `XADD`, not fsync) |
| `publish_visibility` | `DestinationAdmissions` | `Opaque` |
| Close / recovery | Close/drop can discard backlog; resubscription starts empty | Close/drop does not silently ACK; unsettled entries follow PEL claim/recovery policy |

For Redis, `StartPosition` initializes a newly created durable group; it does not reset an existing group's cursor. PEL recovery can be affected by stream trimming and claim policy. A settlement error can arrive after Redis applied `XACK`, so failure does not prove an entry will be delivered again. Publish acceptance proves neither disk fsync nor handler completion. For order-transaction atomicity, implement an application outbox and idempotent consumers.

### Run the Redis order examples

The provider repository contains the real [sync source](https://github.com/qubit-ltd/rs-event-bus-redis/blob/main/examples/sync_orders.rs), [async source](https://github.com/qubit-ltd/rs-event-bus-redis/blob/main/examples/async_orders.rs), [README](https://github.com/qubit-ltd/rs-event-bus-redis/blob/main/README.md), and [user guide](https://github.com/qubit-ltd/rs-event-bus-redis/blob/main/doc/user_guide.md). Run these from an `rs-event-bus-redis` 0.7 checkout against an explicitly chosen disposable Redis server:

```bash
export EVENT_BUS_REDIS_URL='redis://127.0.0.1:16379/'
export EVENT_BUS_EXAMPLE_NAMESPACE="docs-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
cargo run --locked --all-features --example sync_orders -- "$EVENT_BUS_REDIS_URL" "$EVENT_BUS_EXAMPLE_NAMESPACE-sync"
cargo run --locked --all-features --example async_orders -- "$EVENT_BUS_REDIS_URL" "$EVENT_BUS_EXAMPLE_NAMESPACE-async"
```

Each example registers `Utf8Codec` for `String` with `ContentType("text/plain")`, selects `redis-streams`, and supplies `redis.url` / `redis.namespace`. It creates durable group `billing` at `StartPosition::Earliest` before publishing. The sync example prints `consumed order event: order-42`; the async example prints `consumed order event: order-43`, then the user presses Enter to stop it. Wait for that delivery line before pressing Enter. Namespaces must be unique so an old group or record cannot affect the result. A missing codec fails configuration; a Redis connection failure is not admission. For automated verification, first run `cargo build --locked --all-features --examples`, then `cargo test --locked --all-features --test documentation_examples_tests` in the provider checkout: its disposable `RedisServer` fixture owns the server and namespace. Core documentation fixtures do not depend on Redis; these external files are links, never source-checker path exceptions.

### Write a transport yourself

This section is for developers who **write the transport library**. If you only use an existing implementation, the previous section is enough. A new implementation has to both deliver messages and make itself selectable, so it has two layers:

1. **Deliver messages.** The crate calls this set of interfaces the SPI. A sync implementation provides the capability description, publish, subscribe, and shutdown methods of `EventBusSpi`. Each subscription returns an `EventSubscriptionSpi` that receives messages, acknowledges the handling result, and shuts down. The async counterparts are `AsyncEventBusSpi` and `AsyncEventSubscriptionSpi`. Report capabilities honestly. Do not claim persistence or ordering that the implementation does not have.
2. **Let the application create it.** Implement `ProviderMetadata` with a unique id, then `ServiceProvider<EventBusSpec>` or the async `AsyncServiceProvider<EventBusSpec>`. `create_configured` reads configuration, connects to the backend, and returns the implementation from the previous step. Report configuration errors explicitly.
3. **Install it and verify it.** The application can call `registry.register(MyProvider)`. For automatic discovery, submit the provider to the matching catalog. The `conformance` feature runs the crate's interface contract checks. Also test a full queue, cancellation, a repeated acknowledgement, a repeated shutdown, and error text.

An in-process implementation can pass Rust objects directly (`TransportPayload::Native`). A cross-process transport usually encodes them to bytes first (`Encoded`). The return value of `publish(OutboundMessage)` must match what actually happened: return `DestinationAdmissions` only when each destination's admission is known; return `Accepted` without destinations when only the message server's acceptance is known. Do not describe “written to a local buffer” as “persisted.” `receive` must distinguish a message, a wait timeout, a gap, and closed. If settlement is supported, repeating the same result must be safe, and a conflicting result must be an error. When an async operation is cancelled, a message that has not been acknowledged must not be treated as successfully handled.

The code that creates the implementation validates configuration before it connects. When it cannot connect, the configuration is invalid, or the backend is temporarily unavailable, return the matching `ProviderFailure<EventBusProviderError>`. Application handlers, filters, retries, and dead letters belong to the bus object. The transport only delivers messages and performs the acknowledgement modes it explicitly declares.

The skeleton below is the registration surface a sync implementation offers to the application. `MyTransport::connect` belongs to your transport library. It connects and maps errors. This skeleton is the integration point, not a complete message-server client:

```rust
use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;

struct MyProvider;

impl ProviderMetadata for MyProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(ProviderId::new("my-transport").unwrap())
    }
}

impl ServiceProvider<EventBusSpec> for MyProvider {
    fn create_configured(
        &self,
        config: &EventBusConfig,
    ) -> Result<Arc<dyn EventBusSpi>, ProviderFailure<EventBusProviderError>> {
        // MyTransport::connect is the backend: validate config, connect,
        // and map errors to ProviderFailure<EventBusProviderError>.
        MyTransport::connect(config)
    }
}

// Explicit registration:
// let registry = EventBusRegistry::new();
// registry.register(MyProvider)?;
```

To support discovery, enable `discovery` in the implementation crate and submit to the sync catalog:

```rust
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::registry::sync_provider_inventory::Entry;
use qubit_spi::submit_sync_provider;

submit_sync_provider! {
    inventory_entry = Entry;
    spec = EventBusSpec;
    provider = MyProvider;
}
```

An async implementation uses the separate async catalog and `submit_async_provider!`. The [built-in async implementation](../src/local/async_local_event_bus_provider.rs) is a reference. Before production, verify the result of a wait timeout or a close; that a repeated acknowledgement is safe; that a message can be processed again after an async operation is cancelled; that unacknowledged messages are still retained after close; and that closing twice is safe. The interface contract is in the [architecture design](design.md) and the [SPI API](https://docs.rs/qubit-event-bus/latest/qubit_event_bus/spi/).

### Encode events for a cross-process implementation

The built-in local provider passes Rust objects directly and needs no conversion. A message that crosses a process boundary usually has to be turned into bytes and restored on receipt. The component that does this is a **codec**. `Topic::with_codec` / `with_shared_codec` attaches a codec to one event type. A codec can also be placed in a `CodecRegistry` and given to the bus with `EventBusFacadeConfig::with_codec_registry`. A codec on the topic wins. The bus registry is consulted only when the topic has none. The codec is chosen when the subscription is created, and creation fails when both are missing. The application also has to agree on the data format and on version compatibility. This crate does not include a general JSON codec.

Codec callbacks run behind a panic boundary. A returned encode error or an encode/metadata panic fails publication before the provider is called; a validate/decode panic becomes `CodecError::Panicked` and stops that subscription without settling the source. Metadata mismatch, receive-size overflow, and native type mismatch also stop reception. A regular `CodecError::Decode` is rejected as an invalid message. The [codec round-trip example](../examples/codec_round_trip.rs) shows a minimal executable implementation for `String`. The fragments below attach a codec to the order event. The byte layout is the application's own convention: three lines, `order_id`, `customer_id`, and `total_cents`, and none of those fields contains a newline. This crate does not supply that layout.

This application module imports `OrderCreated` from the `orders::events` module introduced above. The complete codec module below is compiled by the documentation fixture:

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/order_created_codec.rs -->
```rust
// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Order-event codec compiled from the bilingual user guides.

use std::io::Error;
use std::str::from_utf8;
use std::sync::Arc;

use qubit_event_bus::CodecError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;

use crate::orders::events::OrderCreated;

/// Encodes and decodes the `OrderCreated` event for the documentation fixture.
pub struct OrderCreatedCodec(
    /// Content type advertised for encoded order events.
    pub ContentType,
);

impl EventCodec<OrderCreated> for OrderCreatedCodec {
    fn content_type(&self) -> &ContentType {
        &self.0
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &OrderCreated) -> Result<Arc<[u8]>, CodecError> {
        let text = format!(
            "{}\n{}\n{}",
            value.order_id, value.customer_id, value.total_cents
        );
        Ok(Arc::from(text.into_bytes()))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<OrderCreated, CodecError> {
        let text = from_utf8(payload.bytes()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })?;
        let mut lines = text.lines();
        let order_id = lines.next().unwrap_or("").to_owned();
        let customer_id = lines.next().unwrap_or("").to_owned();
        let total_cents = lines
            .next()
            .unwrap_or("")
            .parse::<u64>()
            .map_err(|source| CodecError::Decode {
                source: Box::new(source),
            })?;
        if lines.next().is_some() || order_id.is_empty() || customer_id.is_empty() {
            return Err(CodecError::Decode {
                source: Box::new(Error::other(
                    "expected order_id, customer_id, and total_cents",
                )),
            });
        }
        Ok(OrderCreated {
            order_id,
            customer_id,
            total_cents,
        })
    }
}
```

`encode` returns shared bytes and a content type. `decode` rebuilds `OrderCreated` or returns `CodecError::Decode`. Attach this codec to the topic both sides use. The constant `OrderCreated::TOPIC` has no codec; an encoded provider will not use it for bytes:

```rust
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

let topic = Topic::new("orders.created")?.with_codec(OrderCreatedCodec(ContentType::TEXT_PLAIN));
let subscription = bus.subscribe(
    SubscribeRequest::new("audit-log", topic.clone())?,
    move |delivery| store.append_order_created(delivery.payload()),
)?;
let receipt = bus.publish(PublishRequest::new(topic, event)?)?;
```

On publish, the provider receives the bytes from `encode`. On delivery, `decode` restores the value passed to the handler. The built-in local provider still passes the Rust value and does not call this codec.

To share one codec across topics of the same payload type, register it on the bus. A topic codec still wins. The registry is used only when the topic has none. An encoded subscription with neither codec fails at creation with `SubscribeError::Capability(CapabilityError::CodecRequired)`, before the provider creates the subscription:

```rust
use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::model::ContentType;

let mut codecs = CodecRegistry::new();
codecs.register::<OrderCreated>(Arc::new(OrderCreatedCodec(ContentType::TEXT_PLAIN)))
    .expect("unique codec type");
let bus_settings = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
let config = EventBusConfig::default().with_facade_config(bus_settings);
```

Pass `config` to the registry `create` for the encoded provider, the same way the local capacity example passes facade settings. `Topic::new("orders.created")` then finds `OrderCreatedCodec` from the bus. `Topic::new("orders.created")?.with_shared_codec(...)` can also attach an `Arc<dyn EventCodec<OrderCreated>>` that you already hold. `CodecRegistry::register` returns an error if this payload type already has a codec; use `replace` to make replacement explicit.

## Asynchronous bus and subscriptions

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/bin/async_local.rs -->
```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous local delivery example compiled by the user-guide checks.

use std::error::Error;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use tokio::main;
use tokio::spawn;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::timeout;

#[main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new()).await?;
    let topic = Topic::<String>::new("orders.created")?;
    let mut subscription = bus
        .subscribe(SubscribeRequest::new("audit", topic.clone())?)
        .await?;
    let (sender, mut receiver) = unbounded_channel();
    let runner = spawn(async move {
        subscription
            .run(move |delivery| {
                let sender = sender.clone();
                async move {
                    sender.send(delivery.payload().clone()).unwrap();
                    Ok(())
                }
            })
            .await
    });
    bus.publish(PublishRequest::new(topic, "order-42".to_owned())?)
        .await?;
    let delivered = timeout(Duration::from_secs(3), receiver.recv()).await?;
    assert_eq!(delivered.as_deref(), Some("order-42"));
    bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })
    .await?;
    runner.await??;
    Ok(())
}
```


An application written in async Rust can use `AsyncEventBus`. The bus is not tied to one runtime, but the application still needs an executor such as Tokio to run the async code. `publish`, `subscribe`, `publish_all`, and `shutdown` all need `.await`. Unlike the sync bus, async `subscribe` only creates the subscription. The application must also start a long-running task that executes `subscription.run(...)`, or messages never reach the handler:

```rust
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::SubscribeRequest;

let bus = AsyncEventBus::local(LocalEventBusConfig::default()).await?;
let mut subscription = bus
    .subscribe(SubscribeRequest::new("audit-log", OrderCreated::TOPIC)?)
    .await?;

// Run this on a long-lived task of the application executor. `store` is an async store.
subscription.run(move |delivery| {
    let store = store.clone();
    async move { store.append_order_created(delivery.payload()).await }
}).await?;
```

At startup, put `run(...)` on a background task before opening the business entry point. Awaiting it directly inside the startup function stops the rest of startup. Cancelling that `run` while keeping the subscription handle allows a later run. `close().await` ends the subscription and reports provider close errors. Dropping the handle requests disposal but cannot await provider close or report its errors; explicitly await `close()` when those errors matter. When the async local implementation closes, it discards messages that are still queued or unfinished. Subscribing again with the same id starts from an empty queue. If a direct local SPI `receive` future is cancelled while waiting, it has not taken a message; a later `receive` can still get that message. Closing the receiver wakes a pending `receive` with `Closed`. `wait_for_received_deliveries` waits only for messages the bus has already taken. It does not look for messages still queued inside the transport. The async bus has no sync equivalent of `wait_for_idle`.

Keep the handle available to the application monitor while the runner borrows it. Allow an application-defined startup grace period before alerting on `Unstarted`; alert on an unexpected `Paused` or `Stopped` state:

```rust
use qubit_event_bus::AsyncSubscriptionRunState;

match subscription.run_state() {
    AsyncSubscriptionRunState::Unstarted if !startup_grace_elapsed => {}
    AsyncSubscriptionRunState::Unstarted => alert("subscription runner did not start"),
    AsyncSubscriptionRunState::Running => {}
    AsyncSubscriptionRunState::Paused => alert("subscription runner is paused"),
    AsyncSubscriptionRunState::Stopped => alert("subscription runner stopped"),
}
```

`Running` does not mean business processing succeeded, and `Stopped` does not mean the provider queue is empty. The state is a momentary snapshot; correlate it with subscription diagnostics and delivery metrics.

### Recover a stopped encoded subscription

`decode(&EncodedPayload)` reads `payload.bytes()`, `content_type()`, and `schema_id()`. The default `validate_metadata` requires exact content type text and exact `Option<SchemaId>` equality: `None` does not match a named schema, and MIME text is not silently normalized. Override validation only for a documented compatible version set, then select the corresponding decoder explicitly. Direct codec callers must validate metadata themselves; the facade does so automatically.

The receive order is byte-limit check, metadata validation, decode, then filter/middleware/handler. Oversized bytes never reach codec callbacks, and incompatible metadata never reaches decode. `MetadataMismatch`, receive `PayloadTooLarge`, `Panicked`, and `NativeTypeMismatch` stop the subscription without `Accept`, `Reject`, or `Retry`. Ordinary `CodecError::Decode` remains a bad-message rejection and is not a schema recovery mechanism.

Inspect `subscription.terminal_failure()` for the first retained `Arc<SubscriptionStopReason>`. `Codec` contains an event ID and structured codec error; `Provider` contains the provider error when no trustworthy event ID is available. Async `run()` returns `ReceiveError::Stopped` and later runs on the same handle return the same cause without receiving or decoding again. Already-started handlers finish under their existing lifecycle. A close failure is reported independently and does not replace this cause; cancelling a run or close future does not clear the retained cause.

For Redis or another durable provider, stop the old handle, fix the codec/version or size configuration, and create a new subscription using the same durable group. Its recovery mechanism can reclaim the unsettled record. Do not acknowledge or delete it just to silence the failure. For an ephemeral provider, destroying the receiver can discard the offending work; the facade counts a known abandonment once, and resubscribing cannot recover discarded messages. Other healthy subscriptions continue running.

## Non-blocking notification entry

When the code that produces a message cannot stop to wait for a synchronous publish, use `NotificationPublisher<T>`. It places the message on a bounded queue, and a background thread publishes each item. The default queue holds 256 items. `try_publish(payload)` means only that the item **was enqueued**, not that it was published. A full queue returns `TryPublishError::Full(payload)`. After close, the call returns `Closed(payload)`. In both cases the original value is given back. The observer callback receives `Published(receipt)`, `PublishFailed(error)`, or `RequestFailed(error)`. `Published` still does not mean the handler finished. `stats()` exposes the counters.

### Create the notification publisher at startup

Back in the order scenario: the request thread of the order service wants to hand off `OrderCreated` after the commit without waiting for `bus.publish` to consult every subscriber. Create one notification publisher at startup, after the bus and both subscriptions exist. One publisher serves one topic. The receipt check that used to run on the request path moves into the observer:

```rust
use std::num::NonZeroUsize;

use qubit_event_bus::NotificationOutcome;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::model::AdmissionRequirement;

// `bus` is the EventBus that already holds the audit-log and customer-view subscriptions.
// Pass NotificationPublisher::<OrderCreated>::default_capacity() as the third argument
// when the default capacity is fine.
let notifier = NotificationPublisher::new(
    bus.clone(),
    OrderCreated::TOPIC,
    NonZeroUsize::new(1_024).expect("capacity is non-zero"),
    |outcome| match outcome {
        NotificationOutcome::Published(receipt) => {
            if receipt.duplicate_possible() {
                eprintln!("reconcile uncertain event {}", receipt.input_event_id().as_str());
            } else if let Err(error) =
                receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected)
            {
                eprintln!("order event {} admission problem: {error}", receipt.input_event_id().as_str());
            }
        }
        NotificationOutcome::PublishFailed(error) => eprintln!("publishing the order event failed: {error}"),
        NotificationOutcome::RequestFailed(error) => eprintln!("no event id for the order event: {error}"),
        // NotificationOutcome is non_exhaustive, so a fallback arm is required.
        _ => {}
    },
)?;
```

`new` starts a background thread named `event-notification-publisher` and returns an `io::Error` if the thread cannot be created. The observer runs on that thread, once per published item. `receipt` is the same receipt `bus.publish` would return, so the checks from [Check the publication result](#check-the-publication-result) apply. Keep the observer to work that returns quickly, such as logging or updating a metric.

### Enqueue on the request path

After the order transaction commits, the request thread hands the event to `try_publish`. This step does not wait for any subscriber:

```rust
use qubit_event_bus::TryPublishError;

match notifier.try_publish(event) {
    Ok(()) => {}
    Err(TryPublishError::Full(event)) => {
        // The queue is full and `event` is returned unchanged. Record it for a later
        // republish, or fall back to a synchronous bus.publish.
        eprintln!("notification queue is full; order {} was not enqueued", event.order_id);
    }
    Err(TryPublishError::Closed(event)) => {
        // The publisher has started closing, which means the application is shutting down.
        eprintln!("notification publisher is closed; order {} was not enqueued", event.order_id);
    }
}
```

`Ok(())` means the event is in the queue. The background thread calls `bus.publish` later, and the result reaches the application through the observer above. When the queue is full or closed, the original `event` comes back inside the error, so the caller can republish or record it without keeping a copy. The queue is measured in items. A slow handler fills it gradually, and `Full` is a signal the application should watch.

`stats()` reports the counters at runtime:

```rust
let stats = notifier.stats();
println!(
    "enqueued={} published={} queue_full={} publish_errors={}",
    stats.enqueued(),
    stats.published(),
    stats.queue_full(),
    stats.publish_errors()
);
```

`enqueued` counts items that entered the queue. `published` counts receipts. `queue_full` and `queue_closed` count attempts rejected because the queue was full or already closed. `publish_errors` and `request_errors` correspond to the two failure outcomes. Each counter is loaded independently, so the snapshot is not consistent at one instant, and `published` counts receipts rather than completed handlers.

### Drain the queue during shutdown

`close()` stops accepting new notifications, finishes items already queued, and waits for the background thread to exit. Use `close_with_timeout(Duration::from_secs(30))` when shutdown needs a bounded wait. It returns an `io::ErrorKind::TimedOut` error if the deadline passes; the worker keeps running and may still publish already-accepted notifications. New calls to `try_publish` return `Closed`, and another `close` or `close_with_timeout` call can wait for the worker later. A timeout cannot interrupt a synchronous provider call. Neither close method shuts down the event bus the publisher uses. The observer runs on that background thread and should return quickly. Do not call either close method on the same notification publisher from inside its observer. Dropping the handle does not wait for the queue to drain. During shutdown, close the notification publisher explicitly, then close the bus:

```rust
use std::io;
use std::time::Duration;

match notifier.close_with_timeout(Duration::from_secs(30)) {
    // The queue is drained and the background thread has exited.
    Ok(()) => {}
    Err(error) if error.kind() == io::ErrorKind::TimedOut => {
        // The background thread is still publishing queued items; try_publish already returns Closed.
        // Log and continue shutting down, or call close() again to wait until it finishes.
        eprintln!("timed out waiting for the notification queue to drain: {error}");
    }
    Err(error) => eprintln!("closing the notification publisher failed: {error}"),
}
// Then cancel the subscriptions and shut down the bus; see the next section.
```

The background thread holds a clone of `bus`, so close the notification publisher before the bus. In the opposite order, queued notifications fail at publish time with a closed-bus error that is visible only as `PublishFailed` in the observer.

Worker completion includes resource cleanup, including user-owned observer captures. An unwind during cleanup records `worker_panicked` once; every closer observes the same failed exit instead of timing out forever. Calling close from the worker itself returns `io::ErrorKind::Other` without closing admission. Observer-call panics remain isolated and later queued notifications continue. A timed-out closer can wait again later; it cannot forcibly stop user code.

## Lifecycle, waiting, and shutdown

Startup order is: create the bus, register every handler, then start accepting business requests. During shutdown, stop new business requests first, then close message sources such as the notification publisher, and then deal with subscriptions and the bus. Cancel a sync subscription with `cancel()`, and an async subscription with `close().await`. If already-received messages should be finished when possible, the order of cancelling subscriptions and shutting down the bus depends on the transport and has to be verified on that transport. Cancelling a subscription does not mean the business write succeeded.

For the synchronous order subscribers created earlier, keep their handles in application state and close them before the bus:

```rust
use std::time::Duration;
use qubit_event_bus::spi::ShutdownMode;

let audit_cancel_result = audit_subscription.cancel();
let view_cancel_result = view_subscription.cancel();
let shutdown_result = bus.shutdown(ShutdownMode::Graceful {
    timeout: Duration::from_secs(3),
});
audit_cancel_result?;
view_cancel_result?;
shutdown_result?;
```

The snippet attempts both cancellations and bus shutdown even if an earlier step fails, then returns the first error in that order. Dropping a synchronous `Subscription` handle does not call `cancel()`; the receiver remains active until cancellation or bus shutdown. Call `cancel()` explicitly so the application can observe a receiver close error. The bounded shutdown helper below handles bus shutdown timeouts.

### Shut down a sync bus

Stop business ingress first, then drain notification sources with a deadline. Avoid an unbounded subscription-cancel wait before the bounded shutdown. Call `try_shutdown` from the outer application shutdown path; the async equivalent is `try_shutdown_async` in the same compiled file. Both waits use `Graceful`, for 2 seconds then 1 second.

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/bounded_shutdown.rs -->
```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Two bounded waits hand unresolved cleanup back to the application supervisor.

use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

/// Waits twice for graceful completion, without forcing running handlers to stop.
///
/// Returns `false` after both waits expire or the provider reports incomplete
/// shutdown. The application should record metrics and hand control to its
/// external supervisor. Errors other than a caller deadline are propagated.
pub fn try_shutdown(bus: &EventBus) -> Result<bool, ShutdownError> {
    match bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(2),
    }) {
        Ok(report) if report.outcome == ShutdownOutcome::Complete => return Ok(true),
        Ok(_) | Err(ShutdownError::TimedOut { .. }) => {}
        Err(error) => return Err(error),
    }
    match bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(1),
    }) {
        Ok(report) => Ok(report.outcome == ShutdownOutcome::Complete),
        Err(ShutdownError::TimedOut { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Applies the same bounded policy while the caller drives async shutdown.
///
/// Returns `false` if cleanup remains incomplete; other shutdown errors propagate.
/// Cancelling this future leaves shutdown state for the coordinator to resume.
pub async fn try_shutdown_async(bus: &AsyncEventBus) -> Result<bool, ShutdownError> {
    match bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(2),
        })
        .await
    {
        Ok(report) if report.outcome == ShutdownOutcome::Complete => return Ok(true),
        Ok(_) | Err(ShutdownError::TimedOut { .. }) => {}
        Err(error) => return Err(error),
    }
    match bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        })
        .await
    {
        Ok(report) => Ok(report.outcome == ShutdownOutcome::Complete),
        Err(ShutdownError::TimedOut { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}
```

`true` means a `Complete` report; `false` means cleanup remains incomplete after both waits. Record `bus.delivery_metrics()`, each subscription’s `terminal_failure()` and snapshot, and hand process policy to an external supervisor. `TimedOut` is a caller deadline; other errors propagate. `ShutdownReport::known_abandoned_deliveries` counts known abandonment, while `provider_may_have_abandoned_deliveries` warns of additional provider loss that cannot be counted precisely; local is ephemeral.

Bounded waiting does not force process exit. The sync background coordinator may keep waiting for an uncooperative handler or SPI operation. `Immediate` also cannot interrupt running user code and is not a bounded timeout rescue. Waiting for this bus from its own handler can return `WouldDeadlock`.

### Request shutdown without waiting

`EventBus::request_shutdown(mode)` closes admission and returns an `EventBusShutdown` ticket while a background coordinator performs cleanup. This is safe to call from a handler; wait for the ticket only after returning from bus-owned work. `ticket.wait(Some(timeout))` bounds this caller's wait without cancelling shutdown, and `ticket.wait_async().await` observes completion without blocking a thread or requiring a runtime. Dropping the ticket only releases that observer; shutdown continues. A later request joins the active attempt, and an `Immediate` request can strengthen a graceful attempt. `Immediate` still cannot interrupt a running handler or provider call, and the background coordinator may continue waiting after a caller times out.

```rust
use std::time::Duration;

use qubit_event_bus::spi::ShutdownMode;

let ticket = bus.request_shutdown(ShutdownMode::Graceful {
    timeout: Duration::from_secs(5),
})?;
match ticket.wait(Some(Duration::from_secs(2))) {
    Ok(report) => println!("shutdown outcome: {:?}", report.outcome),
    Err(qubit_event_bus::ShutdownError::TimedOut { .. }) => {
        // Keep or drop the ticket; the shutdown attempt continues either way.
    }
    Err(error) => return Err(error.into()),
}
```

When an IoC container separates stopping from waiting, have its stop callback call `request_shutdown` and retain the returned ticket in shared managed state. The wait callback can call `ticket.wait_async().await`. If the container cancels that wait future, keep the same `EventBusShutdown` value and poll `wait_async` again; cancellation removes only the waker registration and does not cancel shutdown. The execution-services [IoC fixture](https://github.com/qubit-ltd/rs-execution-services/blob/main/tests/fixtures/ioc_application_consumer/src/managed_event_bus.rs) demonstrates this adapter. Keep the ticket in shared state while the wait future is active; `EventBusShutdown` is reusable but is not `Clone`.

### Wait for a topic to become idle

Sync `wait_for_idle(&topic, timeout)` waits until the transport reports that the topic has nothing queued or unfinished. A transport that cannot answer returns `IdleWaitUnsupported`. `wait_for_received_deliveries` waits only for messages the bus has already taken. Idle from either call does not replace a check of the database and of failure records.

To finish the order events still queued inside local before shutdown, not only the ones the bus has already taken, wait for the topic to become idle before cancelling the subscriptions. The built-in local provider supports this query. For a transport that does not, the `IdleWaitUnsupported` arm falls back to waiting for received deliveries only:

```rust
use std::time::Duration;

use qubit_event_bus::LifecycleError;
use qubit_event_bus::WaitOutcome;

let timeout = Some(Duration::from_secs(10));
let outcome = match bus.wait_for_idle(&OrderCreated::TOPIC, timeout) {
    Err(LifecycleError::IdleWaitUnsupported) => {
        // The transport cannot report topic idleness; wait only for deliveries the bus has taken.
        bus.wait_for_received_deliveries(&OrderCreated::TOPIC, timeout)?
    }
    other => other?,
};
if outcome == WaitOutcome::TimedOut {
    eprintln!("orders.created still had queued or unfinished messages after 10 seconds");
}
```

`Idle` means that, at that moment, the transport holds no queued message for the topic and the bus has no delivery in progress. After new business requests have stopped, an `Idle` result followed by cancellation leaves no admitted-but-unhandled order event behind. `TimedOut` only means the wait expired; a handler may still be running. Both methods look at this bus object in this process only. For a transport connected to a message server, they say nothing about consumers in other processes.

### Shut down an async bus

The async runner must be driven while the application receives events. On a stop signal, end that `run` future, await the handle's close, then shut down the bus. Here `handler` and `shutdown_signal` come from the application; run this lifecycle in its background task:

```rust
use std::time::Duration;
use qubit_event_bus::spi::ShutdownMode;

let run_result = tokio::select! {
    result = subscription.run(handler) => Some(result),
    _ = shutdown_signal => None,
};
let close_result = subscription.close().await;
let shutdown_result = bus.shutdown(ShutdownMode::Graceful {
    timeout: Duration::from_secs(3),
}).await;
close_result?;
shutdown_result?;
if let Some(result) = run_result { result?; }
```

The excerpt attempts both cleanup steps even if `run` or `close` fails; applications that need every error should record each result before returning. `close().await` exposes provider close errors. Dropping `AsyncSubscription` requests disposal but cannot report errors from asynchronous close, so it is unsuitable when the application needs that result. Use `try_shutdown_async(&bus).await` from the complete compiled source above when bounded retries are needed: both awaits have a `Graceful` timeout. After `false`, do not await the runner’s JoinHandle without a deadline; hand snapshots and process policy to the external supervisor. Dropping the shutdown future pauses its driving; a later shutdown resumes coordinator state. The library does not implicitly spawn work outside the caller’s runtime.

The async bus has no `wait_for_idle`; `wait_for_received_deliveries` counts only facade-received work. Async local discards remaining provider backlog on close; durable providers retain nonterminal work according to their recovery protocol.

## Errors, diagnostics, and troubleshooting

| Symptom | What to check |
| --- | --- |
| `PublishFailure` | Inspect `event_id()`, `effect()`, and `cause()`. A failed call may already have reached the provider; do not retry uncertain admission without a duplicate policy. |
| `SubscribeError` | Check the subscription settings, whether the transport supports the capability, whether a codec is required, and whether the bus is already closed. Do not open the business entry point if startup failed. |
| `subscribe` succeeded, the receipt is `Accepted`, and the handler never runs | Async bus: confirm `AsyncSubscription::run` is running on a long-lived task, that the task was not cancelled early, and that an earlier startup step is not blocked on it. Sync bus: confirm an earlier message is not blocking a handler for a long time. Worker limits are in [Configure the built-in local event bus](#configure-the-built-in-local-event-bus). |
| `NoDestinations` | Check the topic name, the payload type, that the subscription was created before the publish, and that the sync handle has not been `cancel()`ed. |
| `PartiallyAccepted` / `NoneAccepted` | Inspect each destination's `Accepted`, `Filtered`, and `Rejected`, then look at local capacity and the compensation plan. |
| Idle wait returns, but the business effect is missing | Check handler errors, retry or dead letter, and the business database. An admission report and an idle result do not mean the write succeeded. |
| `IdleWaitUnsupported` | The transport cannot report whether the whole topic is idle. Record completion in business code. Work the bus has already taken is not all of the work. |
| Graceful shutdown times out | Check for a handler or a transport read or write that never returns, and for unfinished messages. Ask for the shutdown result again later. |

`observe_diagnostics` registers a callback for internal problem notices. Keep the returned `DiagnosticObserverHandle` to keep receiving them. Dropping it stops observation. The callback runs on the thread that hit the problem and should return quickly. `publish_metrics()` counts publication attempts and provider-reported admission outcomes. It does not count message reception or successful business writes. Logs should record the order id, the event id, the subscriber id, the retry count, and the final error together.

## Delivery gaps and admission checks

Subscriptions stop receiving after a provider-reported delivery gap by default. Inspect the stable `SubscriptionStopReason::Gap` through `Subscription::terminal_failure()` (sync) or the `ReceiveError::Stopped` returned by async `run()`. Set `GapPolicy::Continue` only when the consumer accepts missed messages and wants later messages to continue. The gap diagnostic is emitted in either mode.

Use `publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)` only when the provider exposes destination admission, as local does. For Redis, choose `ProviderOrDestinationAccepted`; per-destination conditions return `UnsupportedVisibility` before publishing. A visible provider retains the complete receipt in `CheckedPublishError::Admission` when the requirement fails. Partial admission can mean some destinations already accepted the event; retrying may duplicate those deliveries. Provider acceptance does not mean handler completion, disk fsync, or a successful business write. Sync subscriptions use one coordinator thread each; the default limit is 256, so configure capacity for the expected subscription count.

## Boundaries and a practice checklist

- local fits in-process work that may lose messages when the process exits and that the application compensates for. It does not provide durable recovery or cross-process communication.
- `publish_all` is not transactional. A publish receipt, an ACK, idle, and a business commit are different stages.
- The bus API offers consumer groups, durable subscriptions, historical replay, ordering, and delay. That does not mean every transport supports them. Check the capability and test it before use.
- Important business work needs a durable handoff, idempotency, a failure record, and compensation. Cover partial admission and duplicate delivery in particular.
- Tests should cover a normal receive, no subscribers, a full queue, a handler failure, retry or dead letter, and shutdown. Measure capacity and performance on the target host.

## Further reading

- [README](../README.md) · [API reference](https://docs.rs/qubit-event-bus)

## Validate a single crate or the coordinated ecosystem

`./scripts/project-ci-check.sh` checks this crate's resolved dependency metadata on its own.
An independent single-crate checkout does not need every downstream repository.
For a coordinated migration, run `./scripts/project-ci-check.sh --ecosystem-root <repos-dir>`
with `rs-event-bus`, `rs-event-bus-redis`, `rs-task`, `rs-ioc`, and
`rs-execution-services` below that directory. The gate requires all five roots
and the seven declared event-bus consumer fixtures, resolves locked all-feature Cargo
metadata, and rejects a graph mixing old event-bus minors with 0.20. Missing
inputs fail explicitly; this metadata check supplements each project's CI and
does not prove delivery behavior by itself.
