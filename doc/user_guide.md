# Qubit Event Bus user guide

[Chinese user guide](user_guide.zh_CN.md) · [README](../README.md) · [API reference](https://docs.rs/qubit-event-bus)

This guide covers `qubit-event-bus` 0.21.0 on Rust 1.94 or later. It is for Rust application developers who need several modules to react to one business event. Developers who write a transport implementation only need [Write a transport yourself](#write-a-transport-yourself). Reading through [Check the publication result](#check-the-publication-result) is enough to integrate the built-in in-process bus. Later sections cover message metadata, ordering, failure handling, configuration, async use, and third-party implementations.

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
  - [Inspect local capacity and rejection categories](#inspect-local-capacity-and-rejection-categories)
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

An event bus addresses a general application problem: several independent components need to react to the same event, while the code that produces it should not depend on each handler or absorb its failures and delays. An order service is one example. After an order transaction commits, the application may need to write an audit record and refresh a customer-order view; later it may also send a notification or sync a warehouse. Calling each module directly from the order service would add a dependency and change the order path whenever a new reaction is added.

With an event bus, the order service does one thing after the commit: publish an `OrderCreated` event to the topic `orders.created`, carrying the order id, customer id, and amount. The audit module and the customer-view module each subscribe to that topic at startup and handle the event on their own. Publisher and subscribers share only the event type. They do not depend on each other, and a new consumer does not require a change to the order service.

This crate separates event-facing code from the way a message actually travels, behind one publish and subscribe API. A different backend yields an in-process, cross-process, or cross-node bus. The included `local` implementation delivers messages only **inside the same process**. If the process exits before an event is published or while a handler is still running, the message is not replayed. Work that must complete reliably still needs an application-owned durable record and a compensation path. In the order example, this includes any audit record that must be preserved after the transaction commits. For cross-process delivery, see [Choose and connect a third-party implementation](#choose-and-connect-a-third-party-implementation). The relevant steps below state this boundary as well.

## Where to start

1. [Integrate an order service](#integrate-an-order-service) covers the event type, the subscriptions, and the publish call.
2. [Check the publication result](#check-the-publication-result) shows what a successful receipt looks like, and the difference between “the message was admitted” and “the business work is done.” The basic integration ends there.
3. Read on as needed: [Add message details when needed](#add-message-details-when-needed), [Keep events for one object in order](#keep-events-for-one-object-in-order), [Handle failures and retries](#handle-failures-and-retries), [Configure the built-in local event bus](#configure-the-built-in-local-event-bus), [Asynchronous bus and subscriptions](#asynchronous-bus-and-subscriptions), or [Choose and connect a third-party implementation](#choose-and-connect-a-third-party-implementation).

Third-party implementations, codecs, and extension interfaces come after the basic path.

## Integrate an order service

<!-- event-bus-source: examples/local_delivery.rs -->
```rust
//! Minimal local-provider example showing subscription, publication, and
//! graceful shutdown.

use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionRequirement;
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
    let _ = bus.publish_checked(
        PublishRequest::new(topic, "order-42".to_owned())?,
        AdmissionRequirement::ProviderOrDestinationAccepted,
    )?;
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
qubit-event-bus = "0.21"
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

`audit_store` and `view_store` are storage objects the application has already built. The `Subscription` returned by `subscribe` is the subscription handle. Hold it while the subscription is active. Call `cancel()` when stopping one consumer independently; for application shutdown that needs a deadline, retain the handle and drive bounded bus shutdown directly because `cancel()` can wait without a deadline for a running handler. Dropping a synchronous subscription handle does not cancel the subscription. The closure passed to `bus.subscribe` is the handler. It calls the store when an event arrives. A store error is returned to the bus, and the subscription policy decides whether to retry or record the failure. See [Handle failures and retries](#handle-failures-and-retries).

A synchronous local subscription uses one OS receiver thread. The default limit is 256 live subscriptions, and handlers run on a separate shared pool. The async local provider does not create one receiver thread per subscription; the application must drive `AsyncSubscription::run`.

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

The service returns the `PublishReceipt` to its caller. `inspect_order_receipt` returns an uncertain receipt with `ReconcileByEventId` and no admission error without running the strict check. The caller must reconcile that history by event ID before continuing; the helper does not perform reconciliation or permit a retry. Only certain history reaches the strict destination admission check. If that check fails, record the event ID and destination results, retain the returned receipt, error, and action, and let the caller make the final decision. The helpers only read the receipt: they do not republish, compensate, or retry selected destinations. For providers without destination visibility, use `ProviderOrDestinationAccepted` when only provider acceptance is required; for Redis, that means the broker accepted the event, not that a consumer ran or completed a business write.

```rust
use event_bus_documentation_consumer::receipt_safety::inspect_order_receipt;
use event_bus_documentation_consumer::republish_action::RepublishAction;
use qubit_event_bus::model::{AdmissionCheckError, PublishAcknowledgement, PublishReceipt};

fn inspect_and_record(
    receipt: PublishReceipt,
) -> Result<PublishReceipt, (PublishReceipt, Option<AdmissionCheckError>, RepublishAction)> {
    match inspect_order_receipt(receipt) {
        Ok(receipt) => Ok(receipt),
        Err((receipt, error, action)) => {
            eprintln!("publication review requires action {action:?} for event {} ({error:?})", receipt.input_event_id().as_str());
            if let PublishAcknowledgement::DestinationAdmissions(destinations) = receipt.acknowledgement() {
                for destination in destinations {
                    eprintln!("{} -> {:?}", destination.subscriber_id().as_str(), destination.status());
                }
            }
            // Return the complete receipt, optional admission error, and action.
            Err((receipt, error, action))
        }
    }
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
//! Application decisions after examining publication evidence.

/// A decision for the application; this enum performs no publication itself.
///
/// # Examples
///
/// ```
/// use event_bus_documentation_consumer::republish_action::RepublishAction;
///
/// let action = RepublishAction::RepublishWhole;
/// assert_eq!(action, RepublishAction::RepublishWhole);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum RepublishAction {
    /// Query by the original event ID or use an idempotent reconciliation path.
    ReconcileByEventId,
    /// No attempt admitted the event; a whole-event retry can be considered.
    RepublishWhole,
    /// Repair only rejected destinations; others have already accepted.
    RetryRejectedDestinations,
    /// Do not automatically repeat accepted, opaque, or intentionally dropped
    /// work.
    NoRepublish,
}
```

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/receipt_safety.rs -->
```rust
//! Check retained history before the final attempt's admission summary.

use qubit_event_bus::model::AdmissionCheckError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishReceipt;

use crate::republish_action::RepublishAction;

/// Checks whether at least one destination accepted the event and none rejected
/// it.
///
/// This only reads the receipt. It does not republish, compensate, or retry
/// selected destinations, and it leaves the receipt available to the caller.
///
/// # Examples
///
/// ```
/// use event_bus_documentation_consumer::receipt_safety::check_order_admission;
/// use qubit_event_bus::model::PublishReceipt;
///
/// fn inspect(receipt: &PublishReceipt) -> Result<(), Box<dyn std::error::Error>> {
///     check_order_admission(receipt)?;
///     let _event_id = receipt.input_event_id();
///     Ok(())
/// }
/// ```
pub fn check_order_admission(receipt: &PublishReceipt) -> Result<(), AdmissionCheckError> {
    receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected)
}

/// Reviews retained history before applying the strict admission check.
///
/// Uncertain history returns the complete receipt with a reconciliation action
/// and does not run the strict check. For certain history, a failed check
/// returns the complete receipt, the error, and the application action.
pub fn inspect_order_receipt(
    receipt: PublishReceipt,
) -> Result<PublishReceipt, (PublishReceipt, Option<AdmissionCheckError>, RepublishAction)> {
    if receipt.duplicate_possible() {
        return Err((receipt, None, RepublishAction::ReconcileByEventId));
    }
    match check_order_admission(&receipt) {
        Ok(()) => Ok(receipt),
        Err(error) => {
            let action = republish_action(&receipt);
            Err((receipt, Some(error), action))
        }
    }
}

/// Chooses a retry or reconciliation action from the final receipt.
///
/// Uncertain history takes precedence and requires reconciliation by event ID.
/// With certain history, an empty or fully rejected destination snapshot can
/// be republished as a whole; partial acceptance retries only rejected
/// destinations. Accepted, opaque, and interceptor-dropped publications are
/// not automatically republished.
///
/// This function only classifies the receipt. It does not publish, reconcile,
/// or modify the receipt.
///
/// # Parameters
/// - `receipt`: the final receipt, including any uncertainty retained from
///   earlier attempts.
///
/// # Returns
/// The application action selected from the receipt's history and admission
/// outcome.
#[inline]
pub fn republish_action(receipt: &PublishReceipt) -> RepublishAction {
    if receipt.duplicate_possible() {
        return RepublishAction::ReconcileByEventId;
    }
    match receipt.admission_outcome() {
        AdmissionOutcome::NoDestinations | AdmissionOutcome::NoneAccepted(_) => RepublishAction::RepublishWhole,
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

To publish several events of the same type, call `publish_all`. It tries each input in order and keeps each success or error in `BatchPublishResult::items()`. A failure does not stop the later items, and the call is not a database transaction around the batch. The async batch starts publishing only when polled; dropping it before completion returns no partial results. Completed prefix receipts are dropped with the future, but their publications are not rolled back. The request currently being awaited may already have been accepted, so cancellation does not establish whether it was published or produce a terminal `PublishFailure`. Requests not yet started are skipped. If you need per-request recovery, publish requests individually, retain each event ID before awaiting, and save each returned receipt or failure. Reconcile an event ID for a request cancelled while pending or rely on application idempotency; retrying the whole batch can duplicate the prefix and the in-flight request. Constructing the input and custom iterator destruction may have application-defined side effects.

## Keep events for one object in order

By default the bus does **not** promise that a subscriber handles events in publish order. The default `ordering_policy` is `OrderingPolicy::Unordered`. On the built-in local bus, both the sync and async buses allow up to 4 handlers to run at once. Two events received by the same subscriber can be in progress together, and a later publication can finish first. Other transports decide their own behavior. Do not assume order.

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
| Retry after failure | `retry_policy`, optionally `retry_rule` / `retry_cancellation_token` | The policy sets the attempt count and delay. A rule or cancellation token without a policy is rejected during request building or before facade callbacks and provider work. These types require a direct `qubit-retry = "0.26"` dependency. See [Retry after a database write fails](#retry-after-a-database-write-fails). |
| Choose an action after failure | `error_handler` | The handler can ask for a retry, a requeue, a move to a failure topic, or a discard. Requeue requires support from the transport. |
| Keep an event that fails for good | `dead_letter(DeadLetterPolicy::with_topic_name(name)?)` | Forward the failed message to another topic (the dead-letter topic). Someone still has to subscribe and handle it. See [Keep an event that fails for good](#keep-an-event-that-fails-for-good). |
| Handle one customer's messages in order | `ordering_policy(OrderingPolicy::PerKey)` | The publisher must set an ordering key, and the transport must support the capability. See [Keep events for one object in order](#keep-events-for-one-object-in-order). |
| Share work across instances, or read older messages | `consumer_group`, `durability`, `start_position` | Only a transport that supports these capabilities can use them. local does not support durable subscriptions or historical reads. |
| Transport-specific parameters | `provider_option` | The implementation defines the meaning. Do not put a password here. |

`DeadLetterPolicy::with_topic_name` accepts a nonempty topic of at most 255 UTF-8 bytes, without surrounding whitespace or control characters. `with_topic` accepts an already validated `Topic`. Provider option keys must be namespaced (contain a dot), cannot start or end with a dot, and neither keys nor values may contain control characters. These checks apply to request builders and facade subscriptions; invalid options are rejected before the provider's `subscribe` call. Run `cargo test --locked --test request_validation_regression_tests` for boundary cases.

A handler may return `()` or `Result<(), DeliveryError>`. Return an error when a database write fails, so the crate applies the subscription policy. `delivery.payload()` is the business data. `delivery.event()` carries the event id and attached information. `delivery.context()` carries the transport, the subscriber, and which attempt this is (`retry_attempt()`, starting at 1). Retries can deliver the same event more than once, so handlers should use a business unique key. The two subscriptions below show the three settings used most often.

### Retry after a database write fails

When the customer-view write occasionally times out, let the crate retry. Retry policy types come from `qubit-retry`, so the application depends on `qubit-retry = "0.26"` directly. The compiled fixture keeps this example checked against the version accepted by this crate:

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/retry_policy.rs -->
```rust
//! Compiled retry-policy example used by both user guides.

use std::time::Duration;

use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_retry::BackoffPolicy;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryPolicyError;

use crate::orders::events::OrderCreated;

/// Builds the customer-view request with three total attempts and a fixed
/// delay.
pub fn customer_view_retry_request() -> Result<SubscribeRequest<OrderCreated>, RetryPolicyError> {
    let options = SubscribeOptions::<OrderCreated>::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .backoff(BackoffPolicy::fixed(Duration::from_millis(200)))
                .build()?,
        )
        .build();
    let request = SubscribeRequest::new("customer-view", Topic::<OrderCreated>::new_static("orders.created"))
        .expect("static topic and subscriber ID are valid")
        .with_options(options);
    Ok(request)
}
```

Pass the returned request to `bus.subscribe` with the customer-view handler. When the handler returns `Err`, the crate calls it again after 200 milliseconds, up to 3 attempts in total. After the third failure the message is `Discard`: no further retry and no dead-letter topic. The callback registered with `observe_diagnostics` receives one `Diagnostic::DeliveryFailed` carrying the attempt count and the final error. This reports the handler terminal failure; it does not mean transport settlement or business work completed. Setting `retry_policy` alone enables retries. `retry_rule` chooses which errors are worth retrying; a rule or cancellation token without a policy is rejected instead of being silently ignored. The message keeps its local outstanding slot for the whole retry.

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


Encoded providers need a codec that can rebuild `DeadLetterEvent<T>` from bytes. The public constructor shares the rebuilt event and performs no publication; treat the reason as untrusted diagnostic text. The following fixture is a private example format, not a standard wire protocol.

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/dead_letter_codec.rs -->
```rust
//! Example codec that reconstructs dead-letter events from encoded bytes.

use std::sync::Arc;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use qubit_event_bus::CodecError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::EncodedPayload;

const MAX_ENCODED_BYTES: usize = 1_048_576;
static CONTENT_TYPE: ContentType = ContentType::new_static("application/vnd.example.dead-letter");

/// Codec for the documentation fixture's private dead-letter byte format.
///
/// This format demonstrates external reconstruction and is not a wire protocol
/// for applications to adopt.
pub struct DeadLetterCodec;

impl EventCodec<DeadLetterEvent<String>> for DeadLetterCodec {
    fn content_type(&self) -> &ContentType {
        &CONTENT_TYPE
    }

    fn schema_id(&self) -> Option<&qubit_event_bus::model::SchemaId> {
        None
    }

    fn encode(&self, value: &DeadLetterEvent<String>) -> Result<Arc<[u8]>, CodecError> {
        let original = value.original_event();
        let timestamp = original.timestamp().duration_since(UNIX_EPOCH).map_err(encode_error)?;
        let mut bytes = Vec::new();
        put_text(&mut bytes, original.id().as_str())?;
        put_text(&mut bytes, original.topic().name())?;
        bytes.extend_from_slice(&timestamp.as_secs().to_be_bytes());
        bytes.extend_from_slice(&timestamp.subsec_nanos().to_be_bytes());
        let headers = original.headers();
        put_count(&mut bytes, headers.len())?;
        for (key, value) in headers {
            put_text(&mut bytes, key)?;
            put_text(&mut bytes, value)?;
        }
        match original.ordering_key() {
            Some(key) => {
                bytes.push(1);
                put_text(&mut bytes, key)?;
            }
            None => bytes.push(0),
        }
        put_text(&mut bytes, original.payload())?;
        put_text(&mut bytes, value.subscriber_id().as_str())?;
        put_text(&mut bytes, value.reason())?;
        if bytes.len() > MAX_ENCODED_BYTES {
            return Err(encode_error("encoded dead letter exceeds fixture limit"));
        }
        Ok(Arc::from(bytes))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<DeadLetterEvent<String>, CodecError> {
        let bytes = payload.bytes();
        if bytes.len() > MAX_ENCODED_BYTES {
            return Err(decode_error("encoded dead letter exceeds fixture limit"));
        }
        let mut cursor = Cursor { bytes, offset: 0 };
        let event_id = EventId::new(cursor.text()?).map_err(decode_error)?;
        let topic_name = cursor.text()?.to_owned();
        let seconds = u64::from_be_bytes(cursor.array()?);
        let nanoseconds = u32::from_be_bytes(cursor.array()?);
        if nanoseconds >= 1_000_000_000 {
            return Err(decode_error("invalid timestamp nanoseconds"));
        }
        let timestamp = UNIX_EPOCH
            .checked_add(Duration::new(seconds, nanoseconds))
            .ok_or_else(|| decode_error("timestamp is out of range"))?;
        let header_count = cursor.u32()? as usize;
        if header_count > 1024 {
            return Err(decode_error("too many headers"));
        }
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            headers.push((cursor.text()?.to_owned(), cursor.text()?.to_owned()));
        }
        let ordering_key = match cursor.byte()? {
            0 => None,
            1 => Some(cursor.text()?.to_owned()),
            _ => return Err(decode_error("invalid ordering-key flag")),
        };
        let event_payload = cursor.text()?.to_owned();
        let subscriber_id = SubscriberId::new(cursor.text()?).map_err(decode_error)?;
        let reason = cursor.text()?.to_owned();
        if cursor.offset != bytes.len() {
            return Err(decode_error("trailing bytes after dead letter"));
        }
        let mut builder = PublishRequest::builder()
            .topic(Topic::<String>::new(&topic_name).map_err(decode_error)?)
            .payload(event_payload)
            .event_id(event_id)
            .timestamp(timestamp)
            .headers(headers);
        if let Some(key) = ordering_key {
            builder = builder.ordering_key(key);
        }
        let (original_event, _) = builder.build().map_err(decode_error)?.into_parts();
        Ok(DeadLetterEvent::new(
            Arc::new(original_event),
            subscriber_id,
            reason.into_boxed_str(),
        ))
    }
}

fn put_count(bytes: &mut Vec<u8>, value: usize) -> Result<(), CodecError> {
    let value = u32::try_from(value).map_err(decode_error)?;
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn put_text(bytes: &mut Vec<u8>, value: &str) -> Result<(), CodecError> {
    put_count(bytes, value.len())?;
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn decode_error(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> CodecError {
    CodecError::Decode { source: error.into() }
}

fn encode_error(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> CodecError {
    CodecError::Encode { source: error.into() }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl Cursor<'_> {
    fn byte(&mut self) -> Result<u8, CodecError> {
        let byte = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| decode_error("truncated byte"))?;
        self.offset += 1;
        Ok(byte)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| decode_error("length overflow"))?;
        let field = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| decode_error("truncated integer"))?;
        self.offset = end;
        field.try_into().map_err(decode_error)
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn text(&mut self) -> Result<&str, CodecError> {
        let length = self.u32()? as usize;
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| decode_error("length overflow"))?;
        let field = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| decode_error("truncated text"))?;
        self.offset = end;
        std::str::from_utf8(field).map_err(decode_error)
    }
}
```

When `error_handler` returns `DeadLetter`, the facade forwards the failure to the dead-letter topic, even if `retry_policy` is also set. If forwarding fails, the async runner stops with `ReceiveError::DeadLetterForwardFailed`; the sync facade cancels that subscription. The source token remains unsettled and diagnostics report the failure. Recovery depends on provider durability and close semantics: a durable provider can redeliver the unsettled message after the subscription is resumed or recreated, while an ephemeral provider may discard it when closed. Do not assume the facade will rerun the handler while forwarding remains broken. The dead-letter event ID is stable for the same original event and subscriber, which helps consumers deduplicate, but does not promise exactly-once delivery. The dead-letter topic is ordinary: someone must subscribe to it, and an empty topic can still lose the record. Encoded transports also need a codec for `DeadLetterEvent<OrderCreated>`. `NoDestinations`, `NoneAccepted`, `Dropped`, and publish errors do not count as forwarding. Known partial acceptance completes the source delivery with a diagnostic because republishing the whole record may duplicate it. An opaque provider such as Redis meets the default policy only when its publish guarantee is at least `Accepted`. Use `DeadLetterPolicy::with_known_destination(topic_name)` when a reported consumer admission is required; subscription creation rejects that policy for opaque providers. Watch diagnostics for forwarding failures.


The `source` field on `ReceiveError::DeadLetterForwardFailed` preserves the forwarding error. Adding it is a source compatibility change: explicit constructors and full field destructuring must be updated; a match that only needs the event and message can use `ReceiveError::DeadLetterForwardFailed { event_id, message, .. }`. To inspect retry metadata, walk `std::error::Error::source()` from the receive error and downcast a cause to `qubit_retry::RetryError<qubit_event_bus::pipeline::DeadLetterForwardError>`. Read `reason()`, `context()`, and `last_error()`; `last_error()` can be `None` when cancellation or an infrastructure failure ends the retry flow. For the retained application error, inspect `retry_error.last_error()` and then its `Error::source()` chain. Add a direct `qubit-retry` dependency when your application needs these types; `qubit-event-bus` does not re-export them. Inside the match arm below, walk the retained source chain and inspect the retry terminal metadata when present:

```rust
use std::error::Error;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::pipeline::DeadLetterForwardError;

fn inspect(error: ReceiveError) {
    match error {
        ReceiveError::DeadLetterForwardFailed { source, .. } => {
            let mut cause: &(dyn Error + 'static) = source.as_ref();
            loop {
                if let Some(retry) = cause.downcast_ref::<
                    qubit_retry::RetryError<DeadLetterForwardError>
                >() {
                    let reason = retry.reason();
                    let context = retry.context();
                    let last_error = retry.last_error();
                    // Record or classify these values; last_error can be None.
                    let _ = (reason, context, last_error);
                }
                match cause.source() {
                    Some(parent) => cause = parent,
                    None => break,
                }
            }
        }
        _ => {}
    }
}
```

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

An async filter is polled by the receiver owner within that bus's lifecycle context. Calling `shutdown().await` or waiting for received deliveries on the same bus from the filter returns `WouldDeadlock` (`ShutdownError::Lifecycle` for shutdown). Let the filter return, then request and drive shutdown from an application lifecycle task outside the filter. Operations on a different bus are not treated as self-waits.

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

`queue_capacity` limits how many messages **one subscriber** may have outstanding. The default is 1,024. `max_total_outstanding` limits outstanding deliveries **across every subscriber of this local instance**. The default is 65,536. A message that has been taken but not finished counts, and a retry keeps its slot. Both values must be greater than zero. The unit is a delivery, not a byte. When a queue is full, that subscriber may reject the message while others still accept it. Terminal settlement or provider cleanup releases the outstanding slot; handler completion alone does not. Size these from the expected subscriber count, measured peak payload sizes, handler throughput, and the process memory budget. For example, two subscribers can create two outstanding deliveries for one publication. The values below (2,048 per subscription, 20,000 per instance, and 8 MiB of declared weight) are illustrative, not general capacity thresholds.

When a valid publish has several local destinations, the sync and async local providers rotate the first destination they try for that topic, using subscription ID order and wrapping at the end. The receipt still lists destinations in ascending subscription ID order. With a stable set of N destinations, serialized publishes, and at least one destination able to accept each event, every destination gets a first-attempt opportunity within N publishes; weight-limited publishes also require each declared weight to fit the total budget. This is not fairness across topics or concurrent publishes, and it does not guarantee full fanout. When capacity is saturated, which subscriber gets the available slot can differ from earlier versions. Check each destination's admission in the receipt; do not use subscription IDs as business priority.

To bound application-declared native payload weight as well as delivery count, opt in to `max_total_outstanding_weight_bytes` and supply a `native_payload_weight` callback for each concrete payload type you publish. For example, a `String` publisher can declare its UTF-8 length (at least one byte for an empty string):

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/local_capacity.rs -->
```rust
//! Compiled local-provider capacity example used by both user guides.

use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::CheckedPublishError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

/// Publishes one weighed `String`, waits for its handler, and closes the bus.
///
/// # Errors
/// Returns an error if bus construction, subscription, admission, delivery
/// wait, or graceful shutdown fails.
pub fn publish_with_local_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let budget = NonZeroUsize::new(8 * 1024 * 1024).expect("positive weight budget");
    let local = LocalEventBusConfig::new()
        .queue_capacity(2_048)
        .max_total_outstanding(20_000)
        .max_total_outstanding_weight_bytes(budget);
    let (bus, monitor) = EventBus::local_with_config_and_monitor(local, EventBusFacadeConfig::new())?;
    let topic = Topic::<String>::new("orders.created")?;
    let (sender, receiver) = mpsc::channel();
    let _subscription = bus.subscribe(SubscribeRequest::new("audit", topic.clone())?, move |delivery| {
        sender.send(delivery.payload().clone()).expect("receiver remains open");
    })?;
    let options = PublishOptions::<String>::builder()
        .native_payload_weight(|payload| NonZeroUsize::new(payload.len().max(1)).expect("positive payload weight"))
        .build();
    let request = PublishRequest::new(topic, "order-42".to_owned())?.with_options(options);
    let published = bus.publish_checked(request, AdmissionRequirement::AtLeastOneAcceptedAndNoRejected);
    let metrics = bus.publish_metrics();
    eprintln!(
        "local admission totals: rejected={}, zero_destinations={}",
        metrics.rejected_destinations, metrics.zero_destinations
    );
    let receipt = match published {
        Ok(receipt) => receipt,
        Err(CheckedPublishError::Admission { receipt, reason }) => {
            eprintln!("local admission: {:?}", receipt.admission_outcome());
            report_rejections(receipt.acknowledgement());
            return Err(Box::new(CheckedPublishError::Admission { receipt, reason }));
        }
        Err(error) => return Err(Box::new(error)),
    };
    eprintln!("local admission: {:?}", receipt.admission_outcome());
    report_rejections(receipt.acknowledgement());
    let snapshot = monitor.snapshot();
    eprintln!(
        "local outstanding={}/{} weight={}/ {:?}",
        snapshot.outstanding_count,
        snapshot.max_total_outstanding,
        snapshot.outstanding_weight_bytes,
        snapshot.max_total_outstanding_weight_bytes
    );
    assert_eq!(receiver.recv_timeout(Duration::from_secs(3))?, "order-42");
    let shutdown = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })?;
    assert_eq!(shutdown.outcome, ShutdownOutcome::Complete);
    Ok(())
}

fn report_rejections(acknowledgement: &PublishAcknowledgement) {
    if let PublishAcknowledgement::DestinationAdmissions(destinations) = acknowledgement {
        for destination in destinations {
            let AdmissionStatus::Rejected(reason) = destination.status() else {
                continue;
            };
            match destination.rejection_kind() {
                Some(kind) => eprintln!("classified rejection: {kind:?} ({reason})"),
                None => eprintln!("unclassified rejection: {reason}"),
            }
        }
    }
}
```

The budget is disabled by default. The callback sees the payload after publisher interceptors and runs once before provider retries. When enabled, a native publish without a declared weight fails with a non-retryable provider error before any destination is enqueued. Each accepted fanout delivery consumes one copy of the declared weight; rejected destinations do not. For example, a 1 KiB declaration accepted by two subscribers consumes 2 KiB of the 8 MiB budget. A retry keeps its reservation until terminal settlement or cleanup. The example's `String::len()` counts its UTF-8 bytes; adapt the estimator to cover the payload holding cost your application intends to account for. Shared buffers, unused allocation capacity, handler allocations, queues, and transport overhead need separate assessment. Declared weight is not a measured or hard RSS/process-memory cap. Monitor process RSS independently and tune the estimate and limits from observed payload sizes, handler latency, and load. Encoded providers use the separate encoded-payload byte limits.

The example requires every reported destination to accept the publication. `publish_checked` returns an admission error with the complete receipt if any destination rejects, even if another destination already accepted. The example logs that receipt's `admission_outcome()` and the bus's `publish_metrics()` counters, then returns the error with its receipt intact. In a long-running service, record each receipt and watch `rejected_destinations` and `zero_destinations` over time; the counters aggregate all publications on this facade and do not identify why a destination rejected. Inspect the individual destination status and rejection reason on the receipt to diagnose capacity or other causes. A partial acceptance must be reconciled per destination or by an application idempotency policy; blindly republishing the whole event may duplicate work for destinations that accepted it. Admission still does not prove handler completion.

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

The registration limit bounds sync receiver threads and async sessions; a paused async subscription still counts. Excess registration fails before provider subscription creation. Both required capacity keys and the optional weight key in `local.provider_options()` must be positive; unknown keys and nonnumeric values fail creation. `EventBus::local` uses default facade settings. For direct local construction with custom facade settings, use `EventBus::local_with_config_and_monitor(local_config, facade_config)` or its async equivalent; see [Inspect local capacity and rejection categories](#inspect-local-capacity-and-rejection-categories). Use registry assembly when provider selection, discovery, or creation fallback is needed.

For encoded transports, `EventBusFacadeConfig::with_payload_limits(PayloadLimits::new(publish_limit, receive_limit))` sets two positive `NonZeroUsize` limits. Both default to 1,048,576 bytes; exactly the limit is allowed. Publishing checks completed codec output before calling the provider; receiving checks bytes before any codec callback. There is no unlimited setting. This does not cap allocations inside encoding or the transport client. Native Rust payloads have no automatically measured byte-size check because their retained memory cannot be measured reliably by the facade; the optional local weight budget instead uses the application's declaration. The local queue count limit still counts deliveries, not bytes.

The async bus reuses the same `config`, changing only assembly:

```rust
use qubit_event_bus::AsyncEventBusRegistry;

let bus = AsyncEventBusRegistry::with_local()?.create(&config).await?;
```

Sync local starts one receive thread per subscriber, and handlers run on the bus's shared worker threads. Async local does not start a receive thread per subscriber, but the application must keep `AsyncSubscription::run` running. With many subscriptions, measure on the deployment machine with `cargo bench --bench local_threads` and `cargo bench --bench local_scale`. Someone else's benchmark result is not a fixed capacity.

### Inspect local capacity and rejection categories

For direct local construction, use `EventBus::local_with_monitor` (or the async equivalent) to receive a cloneable `LocalCapacityMonitor` with default facade settings. These methods delegate to `local_with_config_and_monitor` with `EventBusFacadeConfig::default()`. Use `EventBus::local_with_config_and_monitor(local_config, facade_config)` or its async equivalent when the application needs custom facade middleware, codecs, or delivery scheduling together with the local capacity handle; both configurations still go through facade validation. Registry based construction does not expose this local-specific handle. Its snapshot reports outstanding accepted delivery copies, configured limits, and four saturated rejection totals. Outstanding includes both pending and in-flight deliveries; retry keeps a slot until terminal settlement. Fanout is charged per accepted destination, and a retry is another destination attempt. Declared weight is application supplied accounting, not resident memory. `max_total_outstanding_weight_bytes: None` means weight accounting is disabled; in that mode outstanding weight is zero.

A snapshot is a consistent observation under the budget lock, not a reservation or guarantee that a subsequent publication will fit. Reads briefly wait for that lock. The handle retains accounting integers only, so it does not keep queues or payloads alive. The `local_capacity.rs` example earlier in this section shows how to obtain the monitor alongside the configured local bus and inspect its snapshot.

For destination admissions, inspect `DestinationAdmission::rejection_kind()`. A `None` value means the provider did not classify that rejection; retain the original status reason for diagnostics instead of parsing it. The built-in Redis provider returns opaque acknowledgements and does not expose local categories or snapshots.

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
qubit-event-bus = { version = "0.21", features = ["discovery"] }
qubit-spi = "0.14"
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

`EventBusConfig::with_provider_options` passes that implementation's own configuration. `with_facade_config` configures how the bus handles messages. `with_required_capabilities` states what the application requires. `RequiredCapabilities::new().durable()` means “messages must be persistable.” The built-in local provider cannot do that, so creation fails. `with_subscription_modes(SubscriptionModes::DURABLE)` separately requires Durable subscriptions; `SubscriptionModes::BOTH` requires support for both Ephemeral and Durable. The default has no subscription-mode requirement, and this requirement does not follow from `.durable()`. The registry tries another candidate only **when the bus is created**. A publish or receive failure at runtime does not switch implementations. `ProviderOptions` can appear in debug output. Do not put a password or a token there.

After creating a bus, compare its declared capabilities with `RequiredCapabilities::missing_from(bus.capabilities())`. An empty list means the declarations meet the requirements. This is a local value comparison: it does not call the provider, change metrics, trigger registry fallback, or prove that the backend is healthy. The application decides how to report a non-empty list.


`PublishGuarantee` levels are cumulative. `FireAndForget` promises only that the provider completed its send/commit attempt. `Accepted` adds admission at the provider's declared boundary (possibly an in-memory queue); `Confirmed` adds an explicit positive confirmation observed from the receiving transport; `DurablyStored` adds storage at the documented durable boundary and failure model. None implies handler completion or exactly-once delivery. Both built-in local and Redis providers currently declare `Accepted`; Redis `XADD` success does not by itself justify `Confirmed` or `DurablyStored`.

### Compare local and Redis capabilities

These are the actual `EventBusCapabilities` values for both sync and async providers:

| Capability | local (core 0.21) | Redis Streams (provider 0.7) |
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
3. **Install it and verify it.** The application can call `registry.register(MyProvider)`. For automatic discovery, submit the provider to the matching catalog. The `conformance` feature runs the crate's interface contract checks, including zero-time receive on an empty queue and a successful publish followed by close before receive, then checks that receive returns `Closed`. This records the publish result but does not prove an opaque transport had a broker-readable backlog. Each declared payload-mode and durability combination uses a fresh provider factory instance. Case IDs may repeat across combinations; enumerate `report.cases()` and call `report.case_context(index)` to identify the API, single payload mode, and durability. The combination shares one provider and receiver across related report cases; independent probes use their own instances. Retry/Reject structure checks run only for `AcceptRetryReject`, while the settlement hook must verify actual redelivery and terminal rejection behavior; a successful return or brief timeout is insufficient evidence. The six new case IDs increase report size, so query by ID and context rather than fixed indices. Also test a non-zero receive timeout, a full queue, cancellation, a repeated acknowledgement, a repeated shutdown, and error text.

The same SPI instance is associated with one independently constructed facade lifecycle. Clone `EventBus` or `AsyncEventBus` to share that lifecycle. Create another SPI instance when an independent facade lifecycle is needed; sharing an `Arc` alone does not provide separate subscription identities or shutdown behavior.

An in-process implementation can pass Rust objects directly (`TransportPayload::Native`). A cross-process transport usually encodes them to bytes first (`Encoded`). The return value of `publish(OutboundMessage)` must match what actually happened: return `DestinationAdmissions` only when each destination's admission is known; return `Accepted` without destinations when only the message server's acceptance is known. Do not describe “written to a local buffer” as “persisted.” `receive` must distinguish a message, a wait timeout, a gap, and closed. If settlement is supported, repeating the same result must be safe, and a conflicting result must be an error. When an async operation is cancelled, a message that has not been acknowledged must not be treated as successfully handled.

Providers that enable `Strict` async conformance must also supply the `subscribe_cancellation` hook. Its fixture should acquire a partial resource, remain pending before returning a receiver, then prove that dropping the subscribe future releases the resource and permits a later subscription. The Redis provider's [`async_standalone_tests.rs`](https://github.com/qubit-ltd/rs-event-bus-redis/blob/main/tests/async_standalone_tests.rs) contains a cancellation resource-recovery test that can guide this hook.

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
        let text = format!("{}\n{}\n{}", value.order_id, value.customer_id, value.total_cents);
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
                source: Box::new(Error::other("expected order_id, customer_id, and total_cents")),
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
//! Asynchronous local delivery example compiled by the user-guide checks.

use std::error::Error;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use tokio::main;
use tokio::spawn;
use tokio::sync::mpsc::channel;
use tokio::time::timeout;

#[main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new()).await?;
    let topic = Topic::<String>::new("orders.created")?;
    let subscription = bus.subscribe(SubscribeRequest::new("audit", topic.clone())?).await?;
    let (sender, mut receiver) = channel(1);
    let runner = spawn(async move {
        subscription
            .run(move |delivery| {
                let sender = sender.clone();
                async move {
                    sender.send(delivery.payload().clone()).await.unwrap();
                    Ok(())
                }
            })
            .await
    });
    let _receipt = bus.publish(PublishRequest::new(topic, "order-42".to_owned())?).await?;
    let delivered = timeout(Duration::from_secs(3), receiver.recv()).await?;
    assert_eq!(delivered.as_deref(), Some("order-42"));
    let report = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(3),
        })
        .await?;
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    runner.await??;
    Ok(())
}
```

The handler awaits capacity in this bounded application queue when it is full. This adds backpressure alongside the bus and provider capacity limits. A successful send only means the payload entered the application queue; it does not mean downstream business work was persisted. The example uses `unwrap` because its test receiver must remain open. In an application, map a closed receiver to the handler error type or cancel explicitly.


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

At startup, put `run(...)` on a background task before opening the business entry point. Awaiting it directly inside the startup function stops the rest of startup. Cancelling that `run` while keeping the subscription handle allows a later run. When provider `receive` returns `Closed`, `run` stops receiving and drains deliveries already owned by the facade, including their handler, settlement, and retry work, before returning `Ok(())`. Explicit `Immediate` shutdown keeps its cancellation behavior. A successful `close().await` releases the session's handler callback and captures after receiver cleanup; a paused run or failed close retains the callback so the session can resume or retry. The handle continues to expose its run state, terminal failure, and delivery metrics. `close().await` reports provider close errors. Dropping the handle requests disposal but cannot await provider close or report its errors; explicitly await `close()` when those errors matter. When the async local implementation closes, it discards messages that are still queued inside the provider. Subscribing again with the same id starts from an empty queue. If a direct local SPI `receive` future is cancelled while waiting, it has not taken a message; a later `receive` can still get that message. Closing the receiver wakes a pending `receive` with `Closed`. `wait_for_received_deliveries` waits only for messages the bus has already taken. It does not look for messages still queued inside the transport. The async bus has no sync equivalent of `wait_for_idle`.

Keep the handle available to the application monitor while the runner borrows it. Allow an application-defined startup grace period before alerting on `Unstarted`; alert on an unexpected `Paused` or `Stopped` state. Call `monitor_run_state(subscription.run_state(), startup_grace_elapsed, alert)` with the handle's state, whether the grace period has elapsed, and the application alert callback:

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/run_state_monitor.rs -->
```rust
//! Compiled run-state monitoring example used by both user guides.

use qubit_event_bus::AsyncSubscriptionRunState;

pub fn monitor_run_state(
    state: AsyncSubscriptionRunState,
    startup_grace_elapsed: bool,
    mut alert: impl FnMut(&'static str),
) {
    match state {
        AsyncSubscriptionRunState::Unstarted if !startup_grace_elapsed => {}
        AsyncSubscriptionRunState::Unstarted => alert("subscription runner did not start"),
        AsyncSubscriptionRunState::Running => {}
        AsyncSubscriptionRunState::Paused => alert("subscription runner is paused"),
        AsyncSubscriptionRunState::Stopped => alert("subscription runner stopped"),
        _ => alert("subscription runner reported an unknown state"),
    }
}
```

`Running` does not mean business processing succeeded, and `Stopped` does not mean the provider queue is empty. The state is a momentary snapshot; correlate it with subscription diagnostics and delivery metrics.

### Recover a stopped encoded subscription

`decode(&EncodedPayload)` reads `payload.bytes()`, `content_type()`, and `schema_id()`. The default `validate_metadata` requires exact content type text and exact `Option<SchemaId>` equality: `None` does not match a named schema, and MIME text is not silently normalized. Override validation only for a documented compatible version set, then select the corresponding decoder explicitly. Direct codec callers must validate metadata themselves; the facade does so automatically.

The receive order is byte-limit check, metadata validation, decode, then filter/middleware/handler. Oversized bytes never reach codec callbacks, and incompatible metadata never reaches decode. `MetadataMismatch`, receive `PayloadTooLarge`, `Panicked`, and `NativeTypeMismatch` stop the subscription without `Accept`, `Reject`, or `Retry`. Ordinary `CodecError::Decode` remains a bad-message rejection and is not a schema recovery mechanism.

Inspect `subscription.terminal_failure()` for the first retained `Arc<SubscriptionStopReason>`. `Codec` contains an event ID and structured codec error; `Provider` contains the provider error when no trustworthy event ID is available. Async `run()` returns `ReceiveError::Stopped` and later runs on the same handle return the same cause without receiving or decoding again. Already-started handlers finish under their existing lifecycle. A close failure is reported independently and does not replace this cause; cancelling a run or close future does not clear the retained cause.

For Redis or another durable provider, stop the old handle, fix the codec/version or size configuration, and create a new subscription using the same durable group. Its recovery mechanism can reclaim the unsettled record. Do not acknowledge or delete it just to silence the failure. For an ephemeral provider, destroying the receiver can discard the offending work; the facade counts a known abandonment once, and resubscribing cannot recover discarded messages. Other healthy subscriptions continue running.

## Non-blocking notification entry

When the code that produces a message cannot wait for a synchronous publish, use `NotificationPublisher<T>`. It places the message on a bounded queue, and a background thread publishes each item. The default queue holds 256 items. `try_publish(payload)` means only that the item **was enqueued**, not that it was published. It does not wait for queue capacity or provider work, but it acquires an internal mutex and can wait under mutex contention. A full queue returns `TryPublishError::Full(payload)`. After close, the call returns `Closed(payload)`. In both cases the original value is given back. The observer callback receives `Published(receipt)`, `PublishFailed(error)`, or `RequestFailed(error)`. `Published` still does not mean the handler finished. `stats()` exposes the counters.

### Create the notification publisher at startup

Back in the order scenario: the request thread of the order service wants to hand off `OrderCreated` after the commit without waiting for `bus.publish` to consult every subscriber. Create one notification publisher at startup, after the bus and both subscriptions exist. One publisher serves one topic. The receipt check that used to run on the request path moves into the observer:

When using the local provider's outstanding-weight budget, configure `max_total_outstanding_weight_bytes` on `LocalEventBusConfig` and declare a positive estimate with `PublishOptions::native_payload_weight`. The estimate is application-defined, not a measurement of heap usage; accepted weight remains outstanding until the provider settles the delivery.

```rust
use std::num::NonZeroUsize;

use qubit_event_bus::NotificationOutcome;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishOptions;

// `bus` is the EventBus that already holds the audit-log and customer-view subscriptions.
// Pass NotificationPublisher::<OrderCreated>::default_capacity() as the third argument
// when the default capacity is fine.
let options = PublishOptions::<OrderCreated>::builder()
    .native_payload_weight(|event| {
        NonZeroUsize::new(event.order_id.len().max(1)).expect("positive declared weight")
    })
    .build();
let notifier = NotificationPublisher::with_publish_options(
    bus.clone(),
    OrderCreated::TOPIC,
    NonZeroUsize::new(1_024).expect("capacity is non-zero"),
    options,
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

`with_publish_options` starts a background thread named `event-notification-publisher` and returns an `io::Error` if the thread cannot be created. `new` remains the default-options convenience constructor. The example declares a positive application estimate of native payload weight; it is not heap accounting, and the notification queue remains bounded by item count. The observer runs on that thread, once per published item. `receipt` is the same receipt `bus.publish` would return, so the checks from [Check the publication result](#check-the-publication-result) apply. Keep the observer to work that returns quickly, such as logging or updating a metric.

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

`Ok(())` means the event is in the queue. The background thread calls `bus.publish` later, and the result reaches the application through the observer above. When the queue is full or closed, the original `event` comes back inside the error, so the caller can republish or record it without keeping a copy. The notification queue is measured in items. A slow provider call, publish pipeline, or observer can slow its worker and let this queue accumulate. A slow local handler can instead fill the provider outstanding queue and cause destination rejections while the notification queue remains available; `queue_full` can stay zero.

The three stages have different evidence:

| Stage | Evidence | What it tells you |
| --- | --- | --- |
| Notification queue admission | `try_publish`: `Ok`, `Full`, or `Closed` | Whether the background queue accepted the item; there is no provider receipt yet. |
| Provider publication | `NotificationOutcome` and its complete receipt | Publish errors, destination admissions/rejections, opaque acceptance, and duplicate risk. |
| Handler and settlement | Delivery gauges, diagnostics, and application business state | Consumption and processing progress; `Published` alone does not prove business success. |

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

`enqueued` counts items that entered the queue. `published` counts successful receipts, including interceptor drops, rather than completed handlers. The `admission_accepted()`, `admission_partially_accepted()`, `admission_none_accepted()`, `admission_no_destinations()`, `admission_opaque_accepted()`, and `admission_dropped()` counters classify those receipts. Opaque acceptance does not reveal a destination count. Each counter is loaded independently, so a live snapshot is not a consistent instant. Counters saturate at `u64::MAX`; sums lose exactness after saturation.

### Drain the queue during shutdown

`close()` stops accepting new notifications, finishes items already queued, and waits for the background thread to exit. Use `close_with_timeout(Duration::from_secs(30))` when shutdown needs a bounded wait. Its timeout budget starts when the method is called and covers drain and worker-join waits, but internal mutex acquisition cannot be interrupted, so it is not a strict wall-clock upper bound for the whole call. It returns an `io::ErrorKind::TimedOut` error if a covered wait reaches the deadline; the worker keeps running and may still publish already-accepted notifications. New calls to `try_publish` return `Closed`, and another `close` or `close_with_timeout` call can wait for the worker later. A timeout cannot interrupt a synchronous provider call. Neither close method shuts down the event bus the publisher uses. The observer runs on that background thread and should return quickly. Do not call either close method on the same notification publisher from inside its observer. Dropping the handle does not wait for the queue to drain. During shutdown, close the notification publisher explicitly, then close the bus:

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
// Keep the subscription handles and drive bounded bus shutdown; see the next section.
```

The background thread holds a clone of `bus`, so close the notification publisher before the bus. In the opposite order, queued notifications fail at publish time with a closed-bus error that is visible only as `PublishFailed` in the observer.

Worker completion includes resource cleanup, including user-owned observer captures. An unwind during cleanup records `worker_panicked` once; every closer observes the same failed exit instead of timing out forever. Calling close from the worker itself returns `io::ErrorKind::Other` without closing admission. Observer-call panics remain isolated and later queued notifications continue. A timed-out closer can wait again later; it cannot forcibly stop user code.

## Lifecycle, waiting, and shutdown

Startup order is: create the bus, register every handler, then start accepting business requests. During shutdown, stop new business requests first, close message sources such as the notification publisher within the application deadline, retain subscription handles, then drive bounded bus shutdown. Do not call sync `cancel()` before the bounded shutdown when the application needs a deadline: external `cancel()` waits for an active handler and has no timeout parameter. With the local provider, cancellation closes the receiver and discards messages still queued there. Graceful bus shutdown waits for work already owned by the facade, but the deadline can expire while handlers or provider work remain; it does not promise to drain every provider backlog. Cancellation is useful when stopping one consumer independently of shutting down its bus. Dropping a synchronous `Subscription` handle does not cancel it. If `cancel()` is called from a handler or middleware callback, it requests cancellation but skips joining the coordinator to avoid waiting for that callback itself. A return from such a callback does not mean receiver cleanup has completed. Cancelling a subscription does not mean the business write succeeded. Check async close behavior with `cargo test --locked --test async_lifecycle_regression_tests`.

### Shut down a sync bus

Stop business ingress first, close notification sources with a deadline, and keep subscription handles alive while calling `try_shutdown` from the outer application shutdown path. The async equivalent is `try_shutdown_async` in the same compiled file. Both waits use `Graceful`, for 2 seconds then 1 second. A `false` result means cleanup is still incomplete; hand control to the supervisor and record that outcome. These waits do not guarantee that provider-side queued messages were drained.

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/bounded_shutdown.rs -->
```rust
//! Two bounded waits hand unresolved cleanup back to the application
//! supervisor.

use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

/// Waits twice for graceful completion, without forcing running handlers to
/// stop.
///
/// Returns `false` after both waits expire or the provider reports incomplete
/// shutdown. The application should record metrics and hand control to its
/// external supervisor. Errors other than a caller deadline are propagated.
///
/// # Returns
///
/// `true` if either graceful wait completes shutdown; otherwise `false` when
/// both waits expire or shutdown remains incomplete.
///
/// # Errors
///
/// Returns any shutdown error other than a timeout.
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
/// Returns `false` if cleanup remains incomplete; other shutdown errors
/// propagate. Cancelling this future leaves shutdown state for the coordinator
/// to resume.
///
/// # Returns
///
/// `true` if either graceful wait completes shutdown; otherwise `false` when
/// both waits expire or shutdown remains incomplete.
///
/// # Errors
///
/// Returns any shutdown error other than a timeout.
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

`EventBus::request_shutdown(mode)` closes admission and returns an `EventBusShutdown` ticket while a background coordinator performs cleanup. This is safe to call from a handler; wait for the ticket only after returning from bus-owned work. `ticket.wait(Some(timeout))` bounds this caller's wait without cancelling shutdown, and `ticket.wait_async().await` observes completion without blocking a thread or requiring a runtime. The first poll of `wait_async` from a callback or worker owned by the same bus returns `WouldDeadlock` (with operation `shutdown`), including for a cached ticket; creating the future there is allowed. This rejects only that observation and does not cancel shutdown, so an external lifecycle task can wait after the callback returns. Dropping the ticket only releases that observer; shutdown continues. A later request joins the active attempt, and an `Immediate` request can strengthen a graceful attempt. `Immediate` still cannot interrupt a running handler or provider call, and the background coordinator may continue waiting after a caller times out.

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

An `Ok(ShutdownReport)` means the provider returned a report; inspect it before treating cleanup as successful. This fixture accepts only `ShutdownOutcome::Complete` with `known_abandoned_deliveries == 0`. A provider `ShutdownOutcome::TimedOut` or any known abandoned delivery becomes an IoC cleanup diagnostic that includes the full report. This is distinct from `ShutdownError::TimedOut`, which means the caller's observation deadline elapsed. `provider_may_have_abandoned_deliveries` records uncertainty and does not by itself prove loss or fail this policy. The fixture writes the report to stderr; an application can replace that with structured logging and choose its own tolerance for known loss.

### Wait for a topic to become idle

Sync `wait_for_idle(&topic, timeout)` waits until the transport reports that the topic has nothing queued or unfinished. A transport that cannot answer returns `IdleWaitUnsupported`. `wait_for_received_deliveries` waits only for messages the bus has already taken. Idle from either call does not replace a check of the database and of failure records.

To give order events still queued inside local a chance to finish before shutdown, not only the ones the bus has already taken, wait for the topic to become idle before driving bounded bus shutdown. Keep the subscription handles for shutdown. The built-in local provider supports this query. For a transport that does not, the `IdleWaitUnsupported` arm falls back to waiting for received deliveries only:

```rust
use std::time::Duration;

use qubit_event_bus::LifecycleError;
use qubit_event_bus::WaitOutcome;

let timeout = Some(Duration::from_secs(10));
let (outcome, provider_idle_supported) = match bus.wait_for_idle(&OrderCreated::TOPIC, timeout) {
    Err(LifecycleError::IdleWaitUnsupported) => {
        // This fallback observes only deliveries already taken by this bus.
        (bus.wait_for_received_deliveries(&OrderCreated::TOPIC, timeout)?, false)
    }
    other => (other?, true),
};
if outcome == WaitOutcome::TimedOut {
    eprintln!("orders.created still had queued or unfinished messages after 10 seconds");
} else if outcome == WaitOutcome::Idle && provider_idle_supported {
    eprintln!("provider reported no queued or unsettled orders at this observation");
} else if outcome == WaitOutcome::Idle {
    eprintln!("facade-received work finished; provider backlog is unknown");
}
```

When `wait_for_idle` is supported, `Idle` means the provider reported no queued or unsettled delivery for the topic at that observation, and the bus has no delivery in progress. It is not an atomic drain guarantee. The fallback `Idle` from `wait_for_received_deliveries` says only that this facade's received work finished; provider backlog is unknown. `TimedOut` only means the wait expired; a handler may still be running. Both methods look at this bus object in this process only. For a transport connected to a message server, they say nothing about consumers in other processes.

### Shut down an async bus

The async runner must be driven while the application receives events. On a stop signal, end that `run` future, then directly drive bounded bus shutdown. Do not put an unbounded `close().await` before the shutdown helper: shutdown takes ownership of retained sessions and applies its graceful deadlines. Here `handler` and `shutdown_signal` come from the application; run this lifecycle in its background task:

```rust
use std::time::Duration;
use qubit_event_bus::spi::ShutdownMode;

let run_result = tokio::select! {
    result = subscription.run(handler) => Some(result),
    _ = shutdown_signal => None,
};
let shutdown_result = try_shutdown_async(&bus).await;
let mut cleanup_errors = Vec::new();
if let Some(Err(error)) = run_result { cleanup_errors.push(error.to_string()); }
match shutdown_result {
    Ok(true) => {}
    Ok(false) => cleanup_errors.push("shutdown remains incomplete".into()),
    Err(error) => cleanup_errors.push(error.to_string()),
}
if !cleanup_errors.is_empty() {
    for error in cleanup_errors { eprintln!("cleanup error: {error}"); }
}
```

The example retains both the runner result and shutdown error for reporting. `try_shutdown_async` makes two bounded Graceful attempts; it records a shutdown error rather than skipping other application cleanup. When the helper returns `false`, cleanup remains incomplete. Do not await the runner’s JoinHandle without a deadline; hand snapshots and process policy to the external supervisor. If the bus is still `Running` and the application needs to observe a provider receiver close error separately, it may call `close().await` within its own deadline. Once shutdown has started, `close().await` may return `LifecycleError::Closed`; continue driving shutdown to observe its cleanup result. Dropping the shutdown future pauses its driving; a later shutdown resumes coordinator state. The library does not implicitly spawn work outside the caller’s runtime.

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

`observe_diagnostics` registers a callback for internal problem notices. Keep the returned `DiagnosticObserverHandle` to keep receiving them. Dropping it stops observation. The callback runs on the thread that hit the problem and should return quickly. `Diagnostic::AdmissionRejected` includes both `subscriber_id` (logical identity) and `subscription_id` (the rejected instance), so logs can distinguish two subscriptions with the same logical name. Adding this field affects explicit construction and exhaustive field destructuring; use the actual instance ID or `..` as appropriate, and this source change ships in core 0.21.0. `publish_metrics()` counts publication attempts and provider-reported admission outcomes. Its `cancelled` counter counts async publish futures that were first polled and then dropped before returning; it does not mean the provider failed to accept the event, because a dropped call may already have been accepted. It does not count message reception or successful business writes. Logs should record the order id, the event id, both subscriber identities when relevant, the retry count, and the final error together.

Shutdown close-error history retains detailed records for the first 256 failed subscriptions. `SubscriptionCloseErrors::len()` and `iter()` describe those retained samples; `total_count()`, `omitted_count()`, and `is_truncated()` describe the full failure count. Shutdown still returns an error when the sample is truncated, and its source points to the first retained failure. This bounds the number of records retained by the bus, not the bytes held by each provider error or snapshots kept by the application. Applications that need a complete archive should record each explicit subscription-close error as it occurs.

If a provider supplies a settlement token owned by another subscription, the affected subscription stops with an invalid-token terminal failure. The facade does not settle through the wrong receiver or count the delivery as completed. Fix token ownership in the provider; recovery after close depends on its durable or ephemeral contract.

## Delivery gaps and admission checks

Subscriptions stop receiving after a provider-reported delivery gap by default. Inspect the stable `SubscriptionStopReason::Gap` through `Subscription::terminal_failure()` (sync) or the `ReceiveError::Stopped` returned by async `run()`. Set `GapPolicy::Continue` only when the consumer accepts missed messages and wants later messages to continue. The gap diagnostic is emitted in either mode. With `GapPolicy::Stop`, the facade retains the terminal gap and stops receiving and starting newly admitted handlers before it invokes the diagnostic callback. With `Continue`, later messages may be processed. Verify with `cargo test --locked --test gap_policy_sync_tests` and `cargo test --locked --test gap_policy_async_tests`.

Use `publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)` only when the provider exposes destination admission, as local does. For Redis, choose `ProviderOrDestinationAccepted`; per-destination conditions return `UnsupportedVisibility` before publishing. A visible provider retains the complete receipt in `CheckedPublishError::Admission` when the requirement fails. Partial admission can mean some destinations already accepted the event; retrying may duplicate those deliveries. Provider acceptance does not mean handler completion, disk fsync, or a successful business write. Sync subscriptions use one coordinator thread each; the default limit is 256, so configure capacity for the expected subscription count.

### Migrate from 0.20 to 0.21

The 0.21 release includes source-level changes accumulated since 0.20. Update the core dependency and the affected call sites together.

- `Diagnostic::AdmissionRejected` construction requires the actual `subscription_id`; exhaustive destructuring must add the field or use `..`.
- `ReceiveError::DeadLetterForwardFailed` now requires a `source` when constructed, and full field destructuring must include it; matches that use selected fields can add `..`. The dead-letter failure guidance above shows how to traverse sources and inspect `qubit_retry::RetryError<DeadLetterForwardError>` metadata. Depend directly on `qubit-retry`; core does not re-export it.
- Complete `AsyncConformanceHooks` literals must initialize `subscribe_cancellation` or use `..Default::default()`. Strict providers must supply a fixture that cancels a partially allocated pending subscription and proves its resources are reusable.
- `SubscriptionCloseErrors::len()` and `iter()` expose at most 256 retained samples. Use `total_count()`, `omitted_count()`, and `is_truncated()` for aggregate counts.
- A `retry_rule` or `retry_cancellation_token` without `retry_policy` is rejected by request builders and facade execution. Add an explicit policy or remove the unused rule/token.
- Local publish pre-admission failures use `SpiError::Publish`; callers matching the previous `SpiError::Operation` must inspect `kind()`, `retryable()`, and `publish_effect()`.
- `rs-event-bus-redis`, `rs-task`, `rs-ioc`, and `rs-execution-services` checkouts can have manifests or transitive dependencies fixed to 0.20. For local sibling validation, update each relevant checkout's core constraint, keep one core minor across public type boundaries, and run that project's own locked checks. The published Redis 0.7 and task 0.10 releases target core 0.20; a local core 0.21 path graph does not claim those releases are compatible. Published support requires a consumer release built and tested against registry core 0.21.

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
metadata, and rejects a graph mixing old event-bus minors with 0.21. Missing
inputs fail explicitly; this metadata check supplements each project's CI and
does not prove delivery behavior by itself. It checks sibling checkout paths,
not published registry releases. The historical Redis 0.7 and task 0.10 releases
target core 0.20 and are not evidence of compatibility with core 0.21; published
compatibility requires a consumer release that resolves to registry core 0.21
and its own integration evidence. Do not treat a local path graph pass as that
release evidence.
