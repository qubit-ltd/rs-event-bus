# Qubit Event Bus user guide

[Chinese user guide](user_guide.zh_CN.md) · [README](../README.md) · [API reference](https://docs.rs/qubit-event-bus)

This guide covers `qubit-event-bus` 0.15.0 on Rust 1.94 or later. It is for Rust application developers who need several modules to react to one business event. Developers who write a transport implementation only need [Write a transport yourself](#write-a-transport-yourself). Reading through [Check the publication result](#check-the-publication-result) is enough to integrate the built-in in-process bus. Later sections cover message metadata, ordering, failure handling, configuration, async use, and third-party implementations.

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

Add the dependency:

```toml
[dependencies]
qubit-event-bus = "0.15"
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
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::{DeliveryError, EventBus, Subscription};
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
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::{DeliveryError, EventBus, Subscription};
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
use qubit_event_bus::model::{PublishReceipt, PublishRequest};
use qubit_event_bus::EventBus;
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
receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected)?;
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

`Err(PublishError)` from `bus.publish(...)` means the call did not obtain an admission report. `Ok(receipt)` can still mean that only some destinations received the message. `receipt` is that report. `receipt.admission_outcome()` distinguishes:

| Outcome | Meaning |
| --- | --- |
| `Accepted(summary)` | At least one destination admitted the message, and none rejected it. Some destinations may still have been skipped by a rule. |
| `PartiallyAccepted(summary)` | Someone admitted it and someone rejected it. Publishing the whole event again can make an admitted subscriber handle it twice. |
| `NoneAccepted(summary)` | Destinations were found, but none admitted the message. Inspect skip and rejection reasons. |
| `NoDestinations` | No destination was found. Check that subscriptions exist and that the topic name matches. |
| `OpaqueAccepted` | The transport says it accepted the message and does not identify the destinations. |
| `Dropped` | A publish interceptor discarded the message before delivery. |

### What success looks like

In the order example, both the audit and customer-view subscriptions already exist. After the order transaction commits, one `OrderCreated` is published. A normal receipt looks like this:

```rust
use qubit_event_bus::model::{AdmissionOutcome, PublishAcknowledgement, PublishRequest};

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

After publishing, the order service checks the stricter condition and branches on the failure:

```rust
use qubit_event_bus::model::{AdmissionCheckError, AdmissionRequirement};

let receipt = bus.publish(PublishRequest::new(OrderCreated::TOPIC, event)?)?;
match receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected) {
    Ok(()) => {}
    Err(AdmissionCheckError::RejectedDestinations { count }) => {
        // Audit may already have admitted the event. Do not republish the whole event.
        // Find the rejecting subscriber and repair only that part.
        eprintln!("{count} subscribers rejected order {}", receipt.input_event_id().as_str());
    }
    Err(AdmissionCheckError::NoAcceptedDestination) => {
        // Nobody subscribed, or destinations were found and none admitted the event.
        // Republishing the whole event is safe.
        eprintln!("no subscriber admitted the order event");
    }
    Err(AdmissionCheckError::VisibilityUnavailable) => {
        // The transport did not list subscribers, so a rejection cannot be ruled out.
        eprintln!("the receipt does not report per-subscriber admission");
    }
    Err(AdmissionCheckError::Dropped) => {
        eprintln!("a publish interceptor dropped the order event before delivery");
    }
}
```

`Filtered` on a receipt means the transport decided, during admission, that the message does not belong to that subscriber. It is not a rejection: if another subscriber admitted the message, both conditions pass. It is not an admission either: if every destination is `Filtered`, `accepted` is 0, the outcome is `NoneAccepted`, and both conditions return `NoAcceptedDestination`. A full queue is a rejection (`Rejected`). Do not treat the two as the same thing. Only a transport that can evaluate a filter during admission reports `Filtered`. The built-in local provider does not. On local, a subscription `filter` runs after the bus has already taken the message. A filtered order still shows as `Accepted` on the receipt; its handler is simply not called.

Some transports report only that the message was accepted, without listing subscribers. That is `OpaqueAccepted` in the table. The crate cannot evaluate either condition, so `check_admission` returns `VisibilityUnavailable`. The built-in local provider reports each subscriber and does not produce this outcome.

To see who rejected the message, walk the per-subscriber status on the receipt:

```rust
use qubit_event_bus::model::{AdmissionStatus, PublishAcknowledgement};

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

## Add message details when needed

`PublishRequest::new(topic, payload)?` is enough to publish, and it generates an event id. Use the request builder when you need to choose the id, attach a request id for log correlation, or control processing order for one customer:

```rust
use qubit_event_bus::model::{EventId, PublishRequest};

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
use qubit_event_bus::model::{OrderingPolicy, SubscribeOptions, SubscribeRequest};

let options = SubscribeOptions::builder()
    .ordering_policy(OrderingPolicy::PerKey)
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, handler)?;
```

With both sides configured, the customer-view subscription behaves as follows:

- **Same key.** `order-42` and `order-43` for `customer-7` are handled one after another, in the order they entered this subscription. The handler for `order-43` starts only after the handler for `order-42` returns.
- **Different keys.** An event for `customer-8` uses another lane and does not wait for `customer-7`. If a `customer-7` handler stalls, `customer-8` continues.
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
| Filter events | `filter` | After the bus has taken the message and before the handler runs, inspect the event and decide whether to skip it. On local, a skipped message is still `Accepted` on the publish receipt. A skip is not a rejection. |
| Let the handler decide when to acknowledge | `ack_mode(AckMode::Manual)` | After the business write, call `delivery.acknowledgement().ack()`. On failure, call `nack()`. Returning without a decision counts as failure. See [Let the handler decide when to acknowledge](#let-the-handler-decide-when-to-acknowledge). |
| Retry after failure | `retry_policy`, optionally `retry_rule` / `retry_cancellation_token` | The policy sets the attempt count and the delay. A classification rule alone does not enable retries. These types require a direct `qubit-retry = "0.25"` dependency. See [Retry after a database write fails](#retry-after-a-database-write-fails). |
| Choose an action after failure | `error_handler` | The handler can ask for a retry, a requeue, a move to a failure topic, or a discard. Requeue requires support from the transport. |
| Keep an event that fails for good | `dead_letter(DeadLetterPolicy::topic(name)?)` | Forward the failed message to another topic (the dead-letter topic). Someone still has to subscribe and handle it. See [Keep an event that fails for good](#keep-an-event-that-fails-for-good). |
| Handle one customer's messages in order | `ordering_policy(OrderingPolicy::PerKey)` | The publisher must set an ordering key, and the transport must support the capability. See [Keep events for one object in order](#keep-events-for-one-object-in-order). |
| Share work across instances, or read older messages | `consumer_group`, `durability`, `start_position` | Only a transport that supports these capabilities can use them. local does not support durable subscriptions or historical reads. |
| Transport-specific parameters | `provider_option` | The implementation defines the meaning. Do not put a password here. |

A handler may return `()` or `Result<(), DeliveryError>`. Return an error when a database write fails, so the crate applies the subscription policy. `delivery.payload()` is the business data. `delivery.event()` carries the event id and attached information. `delivery.context()` carries the transport, the subscriber, and which attempt this is (`retry_attempt()`, starting at 1). Retries can deliver the same event more than once, so handlers should use a business unique key. The two subscriptions below show the three settings used most often.

### Retry after a database write fails

When the customer-view write occasionally times out, let the crate retry. Retry policy types come from `qubit-retry`, so the application depends on `qubit-retry = "0.25"` directly:

```rust
use std::time::Duration;
use qubit_event_bus::model::{SubscribeOptions, SubscribeRequest};
use qubit_retry::{BackoffPolicy, RetryPolicy};

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
use qubit_event_bus::model::{AckMode, SubscribeOptions, SubscribeRequest};
use qubit_event_bus::DeliveryError;

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
use qubit_event_bus::model::{
    DeadLetterEvent, DeadLetterPolicy, FailureDirective, SubscribeOptions, SubscribeRequest, Topic,
};

// Failing subscriber: move a failed delivery to the dead-letter topic.
let options = SubscribeOptions::<OrderCreated>::builder()
    .error_handler(|_event, _error| FailureDirective::DeadLetter)
    .dead_letter(DeadLetterPolicy::topic("orders.created.dead")?)
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let view_subscription = bus.subscribe(request, move |delivery| store.upsert_order(delivery.payload()))?;

// Reader: subscribe to the dead-letter topic and record the reason for a person or a background task.
let dead_letter_topic = Topic::<DeadLetterEvent<OrderCreated>>::new("orders.created.dead")?;
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

When `error_handler` returns `DeadLetter`, the facade forwards the failure to the dead-letter topic, even if `retry_policy` is also set. If forwarding fails, the async runner stops with `ReceiveError::DeadLetterForwardFailed`; the sync facade cancels that subscription. The source token remains unsettled and diagnostics report the failure. Recovery depends on provider durability and close semantics: a durable provider can redeliver the unsettled message after the subscription is resumed or recreated, while an ephemeral provider may discard it when closed. Do not assume the facade will rerun the handler while forwarding remains broken. The dead-letter event ID is stable for the same original event and subscriber, which helps consumers deduplicate, but does not promise exactly-once delivery. The dead-letter topic is ordinary: someone must subscribe to it, and an empty topic can still lose the record. Encoded transports also need a codec for `DeadLetterEvent<OrderCreated>`. `NoDestinations`, `NoneAccepted`, `Dropped`, and publish errors do not count as forwarding. Known partial acceptance completes the source delivery with a diagnostic because republishing the whole record may duplicate it. An opaque provider such as Redis meets the default policy only when its publish guarantee is at least `Accepted`. Use `DeadLetterPolicy::known_destination(name)` when a reported consumer admission is required; subscription creation rejects that policy for opaque providers. Watch diagnostics for forwarding failures.

## Filter or intercept a message

A **filter** runs on the receiving side: before the handler, it decides whether this message should be given to the handler. An **interceptor** is application code on the publish or handling path. It is the place to add correlation data, write a log, or stop the rest of the path. A normal integration does not need an interceptor first.

A publish interceptor on the request affects only that publication. `EventBusFacadeConfig::publisher_interceptor` affects every message sent through that bus object. The facade interceptor may change headers. `Ok(false)` stops the publication, and the report shows `Dropped`. A publish error handler only observes the final error. It does not turn the error into success.

A subscriber applies `filter` first, then interceptors, then the handler. A sync subscription uses `SubscribeOptions::builder().interceptor(...)`. An async subscription uses `async_interceptor(...)`. The same kinds of interceptor can be set for the whole bus object on `EventBusFacadeConfig`. An interceptor receives a `next` callback and must call it; otherwise the handler does not run. A sync bus cannot be given an async subscriber interceptor, and an async bus cannot be given a sync one. The mismatch is a configuration error when the subscription is created.

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

`queue_capacity` limits how many messages **one subscriber** may have outstanding. The default is 1,024. `max_total_outstanding` limits outstanding deliveries **across every subscriber of this local instance**. The default is 65,536. A message that has been taken but not finished counts, and a retry keeps its slot. Both values must be greater than zero. The unit is a delivery, not a byte. When a queue is full, that subscriber may reject the message while others still accept it. Finishing a handler, or shutting down, releases the slot. Size these from measured handling speed and memory. A message count is not a memory budget.

Besides the local outstanding limits, the bus object limits how many messages it takes on at once. A sync bus takes at most 4 by default, counting messages in progress and messages waiting to run. The handler wait queue has a separate capacity of 32, but the number that can actually wait is still bounded by that limit of 4. An async bus handles at most 4 messages at once by default. Change these through `EventBusFacadeConfig`. `Facade` in that type name is the API name; in this guide it means the bus-wide settings. This example raises the sync intake limit to 8 and the wait-queue capacity to 64:

```rust
use qubit_event_bus::{EventBusConfig, EventBusFacadeConfig, EventBusRegistry};
use qubit_event_bus::facade::SyncDeliverySchedulerConfig;
use qubit_event_bus::local::LocalEventBusConfig;

let local = LocalEventBusConfig::new()
    .queue_capacity(2_048)
    .max_total_outstanding(20_000);
let bus_settings = EventBusFacadeConfig::new()
    .with_sync_delivery_scheduler(SyncDeliverySchedulerConfig::new(8, 64)?);
let config = EventBusConfig::default()
    .with_provider_options(local.provider_options())
    .with_facade_config(bus_settings);
let bus = EventBusRegistry::with_local()?.create(&config)?;
```

The first argument of `SyncDeliverySchedulerConfig::new` must be greater than zero. The second may be 0, which means a message can be handed only to an idle worker immediately. The sync facade also allows at most 256 live subscription receiver threads by default. Set `SyncDeliverySchedulerConfig::with_max_subscription_workers(NonZeroUsize::new(64).unwrap())` to choose another limit. A subscription over the limit fails before the provider is asked to create it. This bounds thread count; it does not reduce the cost of each blocking receiver. Consider the async bus when the application needs more subscriptions. Creating local through the registry requires passing `local.provider_options()` on `EventBusConfig`. Those options contain the keys `local.queue_capacity` and `local.max_total_outstanding`. An unknown key, a non-numeric value, or zero is rejected at creation. `EventBus::local` sets only the local outstanding capacity. To change handler concurrency, codecs, or interceptors, create the bus with `EventBusRegistry::with_local()`.

For encoded transports, `EventBusFacadeConfig::with_max_encoded_payload_bytes(Some(limit))` rejects codec output larger than `limit` before calling the provider. The default is unlimited. This checks the completed encoded byte vector; it does not cap allocations made while encoding. Native Rust payloads have no byte-size check because their retained memory cannot be measured reliably by the facade. Local queue limits remain counts of deliveries, not bytes.

The async bus uses `LocalEventBusConfig` as well. This example allows 8 messages in progress at once:

```rust
use qubit_event_bus::{AsyncEventBusRegistry, DeliveryAdmissionConfig};
use qubit_event_bus::{EventBusConfig, EventBusFacadeConfig};
use qubit_event_bus::local::LocalEventBusConfig;

let local = LocalEventBusConfig::new().queue_capacity(2_048);
let bus_settings = EventBusFacadeConfig::new()
    .with_delivery_admission(DeliveryAdmissionConfig::new(8)?);
let config = EventBusConfig::default()
    .with_provider_options(local.provider_options())
    .with_facade_config(bus_settings);
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
qubit-event-bus = { version = "0.15", features = ["discovery"] }
qubit-spi = "0.13"
# Also add the chosen provider crate's real package name and version.
```

```rust
use provider_crate as _; // replace with the real crate name so its provider definition is linked
use qubit_event_bus::{EventBusConfig, EventBusRegistry};
use qubit_spi::ProviderSelection;

let registry = EventBusRegistry::discover()?;
registry.set_default_selection(ProviderSelection::named("your-provider-id")?)?;
registry.seal();
let bus = registry.create(&EventBusConfig::default())?;
```

Replace `provider_crate` and `your-provider-id` with the real crate name and the id from its documentation. If the program cannot find an implementation, inspect `provider_ids()` first. `discover()` fails when two implementations use the same selection name. The built-in `local` provider auto-registers only in the **synchronous** catalog. Async local has to be added explicitly with `AsyncEventBusRegistry::with_local()`. The sync and async catalogs are not interchangeable. Both local implementations use the id `local`, and they can also be selected as `memory` or `in-process`. Sync local can also be added explicitly with `EventBusRegistry::with_local()`.

`EventBusConfig::with_provider_options` passes that implementation's own configuration. `with_facade_config` configures how the bus handles messages. `with_required_capabilities` states what the application requires. `RequiredCapabilities::new().durable()` means “messages must be persistable.” The built-in local provider cannot do that, so creation fails. The registry tries another candidate only **when the bus is created**. A publish or receive failure at runtime does not switch implementations. `ProviderOptions` can appear in debug output. Do not put a password or a token there.

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
use qubit_event_bus::{EventBusConfig, EventBusProviderError, EventBusSpec};
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::{ProviderDescriptor, ProviderId, ProviderMetadata, ServiceProvider};
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
qubit_spi::submit_sync_provider! {
    inventory_entry = qubit_event_bus::registry::sync_provider_inventory::Entry;
    spec = qubit_event_bus::EventBusSpec;
    provider = MyProvider;
}
```

An async implementation uses the separate async catalog and `submit_async_provider!`. The [built-in async implementation](../src/local/async_local_event_bus_provider.rs) is a reference. Before production, verify the result of a wait timeout or a close; that a repeated acknowledgement is safe; that a message can be processed again after an async operation is cancelled; that unacknowledged messages are still retained after close; and that closing twice is safe. The interface contract is in the [architecture design](design.md) and the [SPI API](https://docs.rs/qubit-event-bus/latest/qubit_event_bus/spi/).

### Encode events for a cross-process implementation

The built-in local provider passes Rust objects directly and needs no conversion. A message that crosses a process boundary usually has to be turned into bytes and restored on receipt. The component that does this is a **codec**. `Topic::new_with_codec` / `new_with_shared_codec` attaches a codec to one event type. A codec can also be placed in a `CodecRegistry` and given to the bus with `EventBusFacadeConfig::with_codec_registry`. A codec on the topic wins. The bus registry is consulted only when the topic has none. The codec is chosen when the subscription is created, and creation fails when both are missing. The application also has to agree on the data format and on version compatibility. This crate does not include a general JSON codec.

Codec callbacks run behind a panic boundary. A returned encode error or an encode/metadata panic fails publication before the provider is called; a decode panic becomes `CodecError::Panicked` and requests provider retry when settlement supports it. A regular decode error is rejected as an invalid message. The [codec round-trip example](../examples/codec_round_trip.rs) shows a minimal executable implementation.

## Asynchronous bus and subscriptions

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

At startup, put `run(...)` on a background task before opening the business entry point. Awaiting it directly inside the startup function stops the rest of startup. Cancelling that `run` while keeping the subscription handle allows a later run. `close().await`, or dropping the handle, ends the subscription. When the async local implementation closes, it discards messages that are still queued or unfinished. Subscribing again with the same id starts from an empty queue. If a direct local SPI `receive` future is cancelled while waiting, it has not taken a message; a later `receive` can still get that message. Closing the receiver wakes a pending `receive` with `Closed`. `wait_for_received_deliveries` waits only for messages the bus has already taken. It does not look for messages still queued inside the transport. The async bus has no sync equivalent of `wait_for_idle`.

## Non-blocking notification entry

When the code that produces a message cannot stop to wait for a synchronous publish, use `NotificationPublisher<T>`. It places the message on a bounded queue, and a background thread publishes each item. The default queue holds 256 items. `try_publish(payload)` means only that the item **was enqueued**, not that it was published. A full queue returns `TryPublishError::Full(payload)`. After close, the call returns `Closed(payload)`. In both cases the original value is given back. The observer callback receives `Published(receipt)`, `PublishFailed(error)`, or `RequestFailed(error)`. `Published` still does not mean the handler finished. `stats()` exposes the counters.

`close()` stops accepting new notifications, finishes items already queued, and waits for the background thread to exit. Use `close_with_timeout(Duration::from_secs(30))` when shutdown needs a bounded wait. It returns an `io::ErrorKind::TimedOut` error if the deadline passes; the worker keeps running and may still publish already-accepted notifications. New calls to `try_publish` return `Closed`, and another `close` or `close_with_timeout` call can wait for the worker later. A timeout cannot interrupt a synchronous provider call. Neither close method shuts down the event bus the publisher uses. The observer runs on that background thread and should return quickly. Do not call either close method on the same notification publisher from inside its observer. Dropping the handle does not wait for the queue to drain. During shutdown, close the notification publisher explicitly, then close the bus.

## Lifecycle, waiting, and shutdown

Startup order is: create the bus, register every handler, then start accepting business requests. During shutdown, stop new business requests first, then close message sources such as the notification publisher, and then deal with subscriptions and the bus. Cancel a sync subscription with `cancel()`, and an async subscription with `close().await`. If already-received messages should be finished when possible, the order of cancelling subscriptions and shutting down the bus depends on the transport and has to be verified on that transport. Cancelling a subscription does not mean the business write succeeded.

`ShutdownMode::Graceful { timeout }` stops accepting new work and tries to finish work already received. A caller deadline returns `ShutdownError::TimedOut`; it does not mean the bus is closed. A sync bus may still be cleaning up in the background, and a later `shutdown` call observes the final `ShutdownReport`. `Immediate` cannot forcibly stop business code that is already running. Do not call `shutdown`, `wait_for_idle`, or `wait_for_received_deliveries` from a handler on this same bus when the call would wait for the bus to finish its own work. Those calls return `WouldDeadlock`. Start shutdown from the outermost shutdown path of the program.

Sync `wait_for_idle(&topic, timeout)` waits until the transport reports that the topic has nothing queued or unfinished. A transport that cannot answer returns `IdleWaitUnsupported`. `wait_for_received_deliveries` waits only for messages the bus has already taken. Idle from either call does not replace a check of the database and of failure records.

## Errors, diagnostics, and troubleshooting

| Symptom | What to check |
| --- | --- |
| `PublishError` | See whether the error is configuration, a codec, a transport failure, retries exhausted, or a closed bus. Before retrying, check whether a destination may already have received the event. |
| `SubscribeError` | Check the subscription settings, whether the transport supports the capability, whether a codec is required, and whether the bus is already closed. Do not open the business entry point if startup failed. |
| `subscribe` succeeded, the receipt is `Accepted`, and the handler never runs | Async bus: confirm `AsyncSubscription::run` is running on a long-lived task, that the task was not cancelled early, and that an earlier startup step is not blocked on it. Sync bus: confirm an earlier message is not blocking a handler for a long time. Worker limits are in [Configure the built-in local event bus](#configure-the-built-in-local-event-bus). |
| `NoDestinations` | Check the topic name, the payload type, that the subscription was created before the publish, and that the sync handle has not been `cancel()`ed. |
| `PartiallyAccepted` / `NoneAccepted` | Inspect each destination's `Accepted`, `Filtered`, and `Rejected`, then look at local capacity and the compensation plan. |
| Idle wait returns, but the business effect is missing | Check handler errors, retry or dead letter, and the business database. An admission report and an idle result do not mean the write succeeded. |
| `IdleWaitUnsupported` | The transport cannot report whether the whole topic is idle. Record completion in business code. Work the bus has already taken is not all of the work. |
| Graceful shutdown times out | Check for a handler or a transport read or write that never returns, and for unfinished messages. Ask for the shutdown result again later. |

`observe_diagnostics` registers a callback for internal problem notices. Keep the returned `DiagnosticObserverHandle` to keep receiving them. Dropping it stops observation. The callback runs on the thread that hit the problem and should return quickly. `publish_metrics()` counts publication attempts and provider-reported admission outcomes. It does not count message reception or successful business writes. Logs should record the order id, the event id, the subscriber id, the retry count, and the final error together.

## Boundaries and a practice checklist

- local fits in-process work that may lose messages when the process exits and that the application compensates for. It does not provide durable recovery or cross-process communication.
- `publish_all` is not transactional. A publish receipt, an ACK, idle, and a business commit are different stages.
- The bus API offers consumer groups, durable subscriptions, historical replay, ordering, and delay. That does not mean every transport supports them. Check the capability and test it before use.
- Important business work needs a durable handoff, idempotency, a failure record, and compensation. Cover partial admission and duplicate delivery in particular.
- Tests should cover a normal receive, no subscribers, a full queue, a handler failure, retry or dead letter, and shutdown. Measure capacity and performance on the target host.

## Further reading

- [README](../README.md) · [API reference](https://docs.rs/qubit-event-bus)
