# Qubit Event Bus Design (0.18)

> This document describes `qubit-event-bus` 0.18.0 as implemented.
> When the document and the code disagree, the code wins; please update this document.
> 中文版：[design.zh_CN.md](design.zh_CN.md).
>
> Audience: maintainers who need the crate's internal structure, backend authors
> who implement a provider, and integrators who need to know which component
> owns a given guarantee. For day-to-day use, start with the
> [user guide](user_guide.md).

---

## Contents

1. [Purpose and design principles](#1-purpose-and-design-principles)
2. [Architecture](#2-architecture)
3. [Domain model](#3-domain-model)
4. [Provider SPI](#4-provider-spi)
5. [Capability model](#5-capability-model)
6. [Registry, discovery, and provider assembly](#6-registry-discovery-and-provider-assembly)
7. [Shared facade layer](#7-shared-facade-layer)
8. [Synchronous facade: `EventBus`](#8-synchronous-facade-eventbus)
9. [Asynchronous facade: `AsyncEventBus` and `AsyncSubscription`](#9-asynchronous-facade-asynceventbus-and-asyncsubscription)
10. [Built-in `local` provider](#10-built-in-local-provider)
11. [`NotificationPublisher`](#11-notificationpublisher)
12. [Error model](#12-error-model)
13. [Diagnostics and metrics](#13-diagnostics-and-metrics)
14. [Concurrency invariants](#14-concurrency-invariants)
15. [Testing and verification](#15-testing-and-verification)
16. [Public API stability](#16-public-api-stability)
17. [Non-goals and known limits](#17-non-goals-and-known-limits)

---

## 1. Purpose and design principles

### 1.1 What this crate is for

`qubit-event-bus` gives Rust applications a **typed publish/subscribe API** and
leaves "how a message is transported" to a pluggable provider. It serves three kinds of caller:

- **Application code** writes `bus.publish(request)` and `bus.subscribe(request, handler)`,
  receives a typed `Delivery<T>`, and does not care whether the transport is an
  in-process queue or a message broker.
- **Backend authors** implement "send one byte or object message, receive one message,
  settle one message" and the provider-specific shutdown operation. They do not
  reimplement retry, dead-letter, middleware, per-key ordering, backpressure, or
  facade lifecycle coordination.
- **Integrators** need a clear answer to which guarantee (ordering, delay, durability,
  receipt semantics) the facade provides and which the provider provides, and what
  happens when the provider does not support a request.

### 1.2 Principles

These principles run through the implementation. Later sections refer back to them.

**(P1) A typed facade sits on a minimal, object-safe, type-erased SPI.**
The public API (`EventBus`, `AsyncEventBus`, `Topic<T>`, `Delivery<T>`) keeps
generics and type safety. The provider SPI (`EventBusSpi`, `AsyncEventBusSpi`)
sees only `OutboundMessage` / `InboundMessage` and `Arc<dyn Any>` or bytes.
Generics are erased at the facade boundary, so a provider can live in
`Arc<dyn EventBusSpi>` and be held by the registry. Application code never sees `dyn Any`.

**(P2) Semantics belong to the facade; transport belongs to the provider.**
Retry, error handlers, dead-letter, interceptors and middleware, ACK mode,
per-key ordering, in-flight backpressure, lifecycle tracking, and diagnostics
are implemented once in the facade. Providers implement transport-specific
publish, receive, settlement, and shutdown. Backends share facade policies where
their declared capabilities allow them; provider-specific guarantees remain the
provider's responsibility.

**(P3) Capabilities are declared honestly. There is no lowest-common-denominator API and no silent downgrade.**
A provider reports what it can do through `EventBusCapabilities`. The facade checks
a publish or subscribe request against those capabilities (for example, a delay when
delayed delivery is unsupported) and returns `CapabilityError` instead of pretending
to support the request. The facade also does not shrink every backend's API down to
whatever the weakest backend can do.
Capabilities are immutable for one SPI instance and are read once when the
facade is constructed; a panic at that boundary is returned as a terminal SPI
error instead of escaping construction.

**(P4) Fallback happens only at creation time. The running bus never switches provider.**
`EventBusRegistry` accepts a `ProviderSelection` candidate and fallback chain, and
resolves it once inside `create*`. After the facade exists it is bound to one provider.
A later failure becomes an error, a retry, a dead-letter record, or a diagnostic.
It does not silently move the bus to another backend.

**(P5) Synchronous and asynchronous facades are peers. The async facade binds no runtime.**
`EventBus` is a `std` thread implementation. `AsyncEventBus` depends only on
`Pin<Box<dyn Future + Send>>`, `Waker`, and an injected `qubit_clock::Timer`.
It does not spawn tasks and does not depend on tokio or async-std. The caller drives
`AsyncSubscription::run` on whatever runtime they already have. Both paths share
the model, pipeline, registry, and error types.

**(P6) A publish receipt, an application acknowledgement, and transport settlement are three different things.**
`PublishReceipt` records the result of a successful provider call; it may describe
acceptance, partial admission, no destinations, drop, or opaque acknowledgement.
`PublishGuarantee` describes what provider acceptance promises. A receipt does not
mean a subscriber finished handling the message.
`Acknowledgement` is the handler's business decision (`AckMode::Auto` or `Manual`).
`SettlementToken` plus `DeliveryDisposition` is the transport-level confirmation
between the facade and the provider. Keeping them apart avoids reading
"publish returned" as "the handler finished".

**(P7) Work counts have explicit bounds.**
The synchronous handler pool, handler queues, async admission, local queue capacity,
the provider-wide outstanding budget, and the notification publisher queue all have
explicit limits. Overflow behavior (block, reject, or fall back to `Retry`) is defined.
Count bounds do not bound native payload size, codec allocations, or time spent in user/provider code.

**(P8) Failures are observable and isolated.**
User code (handler, filter, interceptor, error handler, retry rule, diagnostic observer)
and provider SPI calls go through `catch_unwind`. One panic does not take down a worker
thread or the bus. It becomes a `DeliveryError`, `PublishError`, or `Diagnostic`.
Failures that cannot be returned to the caller (settlement failure, receive gap,
terminal delivery failure) are emitted through `observe_diagnostics`.

**(P9) Reuse the qubit crates. Do not reimplement them, and do not re-export them.**
Retry comes from `qubit-retry` (`RetryPolicy`, `RetryRule`, `Retry`, `AsyncRetry`).
Time comes from `qubit-clock` (`Timer`). Provider catalogs and discovery come from
`qubit-spi` (`ServiceSpec`, `ProviderRegistry`, inventory). Event IDs come from
`qubit-id`. This crate does **not** re-export those types. Callers depend on the
crate they actually use, which keeps versions from being coupled through this crate.

**(P10) Lifecycle is explicit and idempotent.**
`shutdown(ShutdownMode)` is the only shutdown entry point, and calling it again is safe.
After shutdown, APIs return `Closed`. `Immediate` can strengthen a `Graceful` shutdown
that is already in progress. Dropping a subscription handle does not cancel it on the
synchronous facade, and it does dispose it on the asynchronous facade. Both behaviors
are defined.

### 1.3 Non-goals

- No exactly-once delivery. The ceiling is the provider's declared `PublishGuarantee`
  and settlement capability.
- No distributed transactions, event-sourcing store, or schema registry.
- No built-in provider for an external broker. This crate ships only the in-process `local` provider.
- No runtime provider failover and no multi-provider bridge.
- No global order across subscriptions or topics.
- No re-export of the public types of `qubit-retry`, `qubit-clock`, or `qubit-spi`.

---

## 2. Architecture

### 2.1 Layers

```
┌──────────────────────────────────────────────────────────────────────┐
│  Application                                                         │
│   Topic<T> / PublishRequest<T> / SubscribeRequest<T> / handler       │
└───────────────┬──────────────────────────────────┬───────────────────┘
                │                                  │
      ┌─────────▼──────────┐             ┌─────────▼──────────────┐
      │ EventBus (sync)    │             │ AsyncEventBus (async)  │
      │ · OperationGate    │             │ · scheduler core       │
      │ · one coordinator  │             │ · owned leases         │
      │   thread / sub     │             │ · AsyncSubscription    │
      │ · SyncDelivery-    │             │   (session/lease)      │
      │   Scheduler pool   │             │ · injected Timer       │
      │ · ShutdownCoord.   │             │                        │
      └─────────┬──────────┘             └─────────┬──────────────┘
                │        shared facade layer        │
      ┌─────────▼──────────────────────────────────▼──────────────┐
      │ pipeline: PublisherPipeline / SubscriberPipeline          │
      │          admission / ordering_lane / dead_letter / retry  │
      │          diagnostic                                       │
      │ model:   Topic / EventEnvelope / Delivery / Acknowledgement│
      │          PublishReceipt / SubscriberId / EventId / ...    │
      │ codec:   EventCodec / CodecRegistry                       │
      │ error:   EventBusError and the per-operation errors       │
      └─────────┬──────────────────────────────────┬──────────────┘
                │ Arc<dyn EventBusSpi>              │ Arc<dyn AsyncEventBusSpi>
      ┌─────────▼──────────────────────────────────▼──────────────┐
      │ spi: EventBusSpi / EventSubscriptionSpi (+ async)         │
      │      OutboundMessage / InboundMessage / TransportPayload  │
      │      SettlementToken / DeliveryDisposition / Capabilities │
      │      conformance (feature)                                │
      └─────────┬──────────────────────────────────┬──────────────┘
                │                                  │
      ┌─────────▼──────────┐             ┌─────────▼──────────────┐
      │ registry           │             │ local provider         │
      │ EventBusSpec       │◀────────────│ LocalEventBusSpi       │
      │ EventBusRegistry   │  register / │ AsyncLocalEventBusSpi  │
      │ AsyncEventBusReg.  │  discover   │ LocalQueue / Budget    │
      │ RequiredCapabilities│            └────────────────────────┘
      └────────────────────┘
      ┌────────────────────┐
      │ notification       │  bounded non-blocking outlet in front
      │ NotificationPub.   │  of a synchronous EventBus
      └────────────────────┘
```

### 2.2 Modules (matching `src/`)

| Module | Responsibility | Principal types |
| --- | --- | --- |
| `model` | Transport-independent domain types | `Topic<T>`, `EventEnvelope<T>`, `Headers`, `PublishRequest<T>`, `PublishOptions<T>`, `SubscribeRequest<T>`, `SubscribeOptions<T>`, `Delivery<T>`, `DeliveryContext`, `Acknowledgement`, `PublishReceipt`, `AdmissionOutcome`, `SubscriberId`, `EventId`, `DeadLetterEvent<T>`, `PublishMetadata`, `PublishFailureContext<T>` |
| `codec` | Codec trait and registry | `EventCodec<T>`, `CodecRegistry`, `ContentType`, `SchemaId`, `EncodedPayload`, `resolve_codec` |
| `spi` | Provider contract | `EventBusSpi`, `EventSubscriptionSpi`, `AsyncEventBusSpi`, `AsyncEventSubscriptionSpi`, `SpiFuture`, `OutboundMessage`, `InboundMessage`, `TransportPayload`, `ReceiveOutcome`, `SettlementToken`, `DeliveryDisposition`, `SpiSubscriptionRequest`, `EventBusCapabilities` and the capability enums, `ShutdownOutcome`, `DeliveryGap`, `conformance` |
| `registry` | Provider catalog and assembly | `EventBusSpec`, `EventBusProvider` / `AsyncEventBusProvider` (aliases of `qubit-spi` definition traits), `EventBusRegistry`, `AsyncEventBusRegistry`, `EventBusConfig`, `RequiredCapabilities`, `EventBusProviderError`, internal `EventBusProviderAdapter` / `IdentifiedEventBusSpi`, `sync_provider_inventory` / `async_provider_inventory` (discovery) |
| `pipeline` | Processing shared by both facades | `PublisherPipeline`, `SubscriberPipeline`, `DeliveryFailureAction`, `AdmissionTracker`, `OrderingLaneKey`, `DeadLetter*`, retry adapters, `Diagnostic` |
| `facade` | User-facing bus | `EventBus`, `EventBusShutdown`, `Subscription`, `AsyncEventBus`, `AsyncSubscription`, `EventBusFacadeConfig`, `DeliverySchedulingConfig`, `SettlementRetryConfig`, `PublishMetricsSnapshot`, `WaitOutcome`, internal `SyncDeliveryScheduler` / `ShutdownCoordinator` / `LifecycleTracker` |
| `local` | Built-in in-process provider | `LocalEventBusConfig`, `LocalEventBusProvider`, `AsyncLocalEventBusProvider`, `LocalEventBusSpi`, `AsyncLocalEventBusSpi`, `LocalQueue`, `OutstandingBudget` |
| `notification` | A bounded, never-blocking publish queue in front of `EventBus` | `NotificationPublisher<T>`, `NotificationOutcome`, `TryPublishError<T>`, `NotificationStatsSnapshot` |
| `error` | Layered error types | `EventBusError`, `PublishError`, `SubscribeError`, `DeliveryError`, `LifecycleError`, `ShutdownError`, `ProviderError`, `SpiError`, `CapabilityError`, `CodecError`, `ConfigurationError` |

Dependencies point `facade → pipeline → {model, codec, spi, error}`,
`registry → spi`, and `local → spi`. `pipeline` and `spi` do not know about the
facade. `local` does not know about any layer above the registry.

### 2.3 Crate metadata, features, and dependencies

- Package `qubit-event-bus`, version `0.18.0`, edition 2024, `rust-version = 1.94`.
- Features:
  - `discovery = ["qubit-spi/inventory"]` enables inventory-driven provider
    registration (see §6.4).
  - `conformance` exposes `qubit_event_bus::spi::conformance` so provider authors
    can run the contract cases in their own tests (see §15.3).
- Qubit crates actually used at runtime: `qubit-spi` (catalog and discovery),
  `qubit-retry` (`worker` and `async` features), `qubit-clock` (`Timer` / `TimeError`),
  and `qubit-id` (UUID `EventId`s). Error types use `thiserror`.
- Dev-dependencies: `loom` (concurrency models); the bounded channel transport uses the standard library
  and `loom` (concurrency model checking).

---

## 3. Domain model

The domain model can describe **one publication and one delivery without knowing
any provider**. Constructors finish validation. Runtime paths do not discover
configuration errors that `build()` should already have rejected.

### 3.1 Identifiers

| Type | Meaning | Constraints / how it is produced |
| --- | --- | --- |
| `Topic<T>` | A typed topic | Name is `Cow<'static, str>`. May carry an optional `Arc<dyn EventCodec<T>>`. Equality and hashing use **the name plus `TypeId::of::<T>()`**, so the same name with two types is two topics. `Topic::new_static` is `const fn` and can be a global constant |
| `SubscriberId` | The application's stable name for a subscriber | 1..=128 bytes. The first character is a letter or digit; the rest may also be `.`, `_`, `-`, or `:`. Used in dead-letter records, consumer-group semantics, and as a readable id on the provider side |
| `subscription::Id` | The facade-assigned id of one subscription instance | A monotonically increasing integer, unique within one bus. Providers index subscriptions by it. It is unrelated to `SubscriberId` |
| `EventId` | The id of one `EventEnvelope` | Defaults to a UUID v4 from `qubit-id`. `EventId::new` accepts an external id (1..=128 bytes, no leading or trailing whitespace, no control characters) |
| `ProviderId` | Provider identity | From `qubit-spi`. The built-in `local` provider also answers to the aliases `memory` and `in-process` |

`SubscriberId` and `subscription::Id` are separate on purpose. The first is a
**business identity** (several subscriptions may share it, and dead-letter records
attribute work to it). The second is the **runtime instance identity**.

### 3.2 `EventEnvelope<T>`

```rust
pub struct EventEnvelope<T> {
    id: EventId,
    topic: Topic<T>,
    payload: Arc<T>,
    headers: Headers,            // BTreeMap<String, String>
    timestamp: SystemTime,
    ordering_key: Option<Box<str>>,
    delay: Option<Duration>,
}
```

- **The payload is `Arc<T>`.** One publish is shared by every subscriber. The local
  provider forwards `Arc<dyn Any>` without copying the value. `Delivery::payload_arc()`
  lets a handler keep sharing it.
- **`Headers` is a `BTreeMap`.** Iteration order is deterministic, which keeps tests and logs stable.
- **`ordering_key` and `delay` live on the envelope, not on options.** They are
  properties of the message and must travel into the provider. `OutboundMessage` carries them as-is.
- Reserved header `x-qubit-event-bus-dead-letter: v1`. The dead-letter pipeline writes it.
  `PublishRequestBuilder::header` rejects it with `ConfigurationError`, and so does
  `PublishMetadata::set_header`. The facade uses the header to recognize "this message
  is already a dead-letter record" and stop dead-letter recursion (see §7.6).

### 3.3 Requests and builders

`PublishRequest<T>` is an `EventEnvelope<T>` plus `PublishOptions<T>`.
`SubscribeRequest<T>` is a `Topic<T>`, a `SubscriberId`, and `SubscribeOptions<T>`.

A request object, rather than a long argument list, exists so that:

- The envelope, retry policy, interceptors, and error handlers are one immutable value.
  `build()` validates them. `publish` and `subscribe` do not grow branches for illegal combinations.
- Fields of `PublishOptions<T>` and `SubscribeOptions<T>` are `pub(crate)`. Callers set
  them only through the builder, so a reserved header or an illegal delay is rejected at construction.
- A request is `Clone` (interceptors are shared through `Arc`), which makes tests and republish straightforward.

`PublishOptions<T>` holds `retry_policy`, `retry_rule` (`Arc<dyn RetryRule<PublishAttemptError>>`),
`retry_cancellation_token`, `interceptors: Vec<Arc<PublisherInterceptor<T>>>`, and
`error_handlers: Vec<Arc<PublishErrorHandler<T>>>`.

`SubscribeOptions<T>` holds `ack_mode: AckMode`, `filter`, `interceptors`
(synchronous `SubscriberInterceptor<T>`), `async_interceptors` (`AsyncSubscriberInterceptor<T>`),
`retry_policy` / `retry_rule` / `retry_cancellation_token`, `error_handlers`
(`SubscribeErrorHandler<T>` returns `FailureDirective`), `dead_letter: Option<DeadLetterPolicy>`,
`ordering: OrderingPolicy`, `durability: SubscriptionDurability`,
`start_position: StartPosition`, `consumer_group: Option<ConsumerGroup>`, and
`provider_options: ProviderOptions` (`BTreeMap<String, String>`).

The same `SubscribeRequest<T>` can be given to `EventBus` or `AsyncEventBus`.
The synchronous facade rejects a request that carries `async_interceptors`
(`SubscribeError::Configuration`). The asynchronous facade rejects requests that carry synchronous
`interceptors`; its handler chain accepts `async_interceptors` only.

### 3.4 `Delivery<T>`, `DeliveryContext`, and `Acknowledgement`

A handler receives `Delivery<T>`:

- `event()`, `payload()`, and `payload_arc()` expose the envelope read-only.
- `context()` returns `DeliveryContext`: `provider_id`, `subscription_id`, `subscriber_id`,
  `retry_attempt` (the facade-local attempt, starting at 1), `provider_attempt`
  (an optional field reserved for a provider redelivery count; `InboundMessage` has
  no source for it today, and neither facade fills it), `provider_metadata`
  (`BTreeMap<String, String>` of non-sensitive data such as partition or offset,
  copied from `InboundMessage`), `can_settle` (whether this message carries a
  settlement token), and `is_dead_letter()` (whether the reserved dead-letter header
  is present). Facade retries and provider redeliveries are **counted separately**
  so the two meanings do not collapse into one number.
- `acknowledgement()` returns the shared `Acknowledgement`, which exposes `ack()` and `nack()`.

`Acknowledgement` atomically retains the first decision. `ack()` and `nack()` return
`Result<(), AcknowledgementError>`: repeating the same decision returns `Ok(())`,
while the opposite decision returns `AcknowledgementError::AlreadyCompleted`.
Neither changes the retained decision. `Delivery` and the facade share it. After the handler returns, the facade reads it
to choose settlement (see the ACK matrix in §7.4). Moving the `Delivery` to another
thread before acknowledging it still produces exactly one terminal state.

### 3.5 `PublishReceipt` and admission visibility

```rust
pub struct PublishReceipt {
    input_event_id: EventId,             // envelope id supplied by the caller
    dispatched_event_id: Option<EventId>, // id actually sent after interceptors; None when dropped
    provider_id: ProviderId,
    acknowledgement: PublishAcknowledgement,
    duplicate_possible: bool,            // an earlier publish attempt may have been admitted
}

pub enum PublishAcknowledgement {
    Accepted { provider_message_id: Option<String>, metadata: ProviderMessageMetadata }, // opaque accept
    DestinationAdmissions(Vec<DestinationAdmission>),   // per destination: Accepted / Filtered / Rejected
    DroppedByInterceptor,
}
```

- The provider SPI `publish` returns only `PublishAcknowledgement`. The facade adds
  the event ids and provider id to form `PublishReceipt`.
- `admission_outcome()` classifies `PublishAcknowledgement` as `AdmissionOutcome`:
  `OpaqueAccepted` (the provider does not report destinations),
  `Accepted` / `PartiallyAccepted` / `NoneAccepted(AdmissionSummary)`,
  `NoDestinations` (the provider reported an empty destination list),
  or `Dropped` (an interceptor dropped the message). `Filtered` destinations
  (a subscription filter declined them) and `Rejected` destinations (capacity or
  admission declined them) are counted separately in `AdmissionSummary`.
- `check_admission(AdmissionRequirement)` lets the caller require at least one
  destination, or every destination, to be accepted. It returns
  `Result<(), AdmissionCheckError>` (`VisibilityUnavailable`, `Dropped`,
  `NoAcceptedDestination`, `RejectedDestinations { .. }`).

A successful `publish` call means the provider call completed and returned a receipt; it
does not by itself mean acceptance. `admission_outcome()` may report accepted, partial,
no destinations, no accepted destinations, dropped, or opaque admission. Only a provider
with `PublishVisibility::DestinationAdmissions` can report destination-level detail.
`publish_all` returns `BatchPublishResult`, preserving each request's
`Result<PublishReceipt, PublishFailure>` in input order. One failure does not stop the later requests.

Every whole-event republish decision checks `duplicate_possible()` first. Unknown history is not erased by final `NoDestinations`/`NoneAccepted`. Reconcile uncertain history by event ID; consider whole-event retry only without that history and with no acceptance. Partial admission repairs rejected destinations only, and Dropped does not automatically republish. `check_admission` still checks only the final ACK. See the [compiled decision](user_guide.md#check-the-publication-result).

### 3.6 `DeadLetterEvent<T>`

The payload type of a dead-letter topic is `DeadLetterEvent<T>`: `original_event: Arc<EventEnvelope<T>>`,
`subscriber_id`, and `reason` (the `Display` of the final `DeliveryError`).
`DeadLetterPolicy::with_topic_name(name)` names the topic and defaults to transport acceptance;
`with_known_destination(topic_name)` requires visible admission and is rejected for opaque providers.
The facade builds `Topic::<DeadLetterEvent<T>>::new(name)` and requires a codec registered
for that payload type on encoded providers. Dead-letter event IDs are stable for the original
event and subscriber to aid deduplication, but do not provide exactly-once delivery.

---

## 4. Provider SPI

### 4.1 Principles

1. **Object safety.** Every SPI trait fits in `Arc<dyn ...>` or `Box<dyn ...>`, so the registry can hold different providers uniformly.
2. **Type erasure.** The SPI sees `TransportPayload`. The generic `T` appears only as `SpiSubscriptionRequest::payload_type_id()` (`TypeId`), which a provider uses to reject type conflicts.
3. **A small surface.** A provider implements `capabilities`, `publish`, `subscribe`, and `shutdown`, plus `receive`, `settle`, and `close` on the subscription handle. `wait_for_topic_idle` has a default that returns `Ok(None)`, meaning "unsupported".
4. **Single-owner subscriptions.** Methods on `EventSubscriptionSpi` and `AsyncEventSubscriptionSpi` take `&mut self`. The provider does not lock for concurrent receive. The facade owns concurrency.
5. **Idempotence and cancellation safety.** `settle` of the same `(token, disposition)` is idempotent. Dropping an async `receive` future must not lose the message. `shutdown` may be called more than once.

### 4.2 Synchronous contract

```rust
pub trait EventBusSpi: Send + Sync + 'static {
    fn capabilities(&self) -> EventBusCapabilities;
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError>;
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError>;
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError>;
    fn wait_for_topic_idle(&self, topic: &TopicAddress, timeout: Option<Duration>) -> Result<Option<bool>, SpiError> {
        Ok(None)   // default: unsupported; Some(true) = idle, Some(false) = timed out
    }
    #[doc(hidden)] fn provider_id(&self) -> Option<ProviderId> { None }
}

pub trait EventSubscriptionSpi: Send + 'static {
    fn id(&self) -> subscription::Id;
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError>;
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError>;
    fn close(&mut self) -> Result<(), SpiError>;
}
```

`provider_id()` is a `#[doc(hidden)]` hook. The registry wraps the real provider
in an `IdentifiedEventBusSpi` proxy and overrides the hook, so the facade can put
the provider id on `DeliveryContext`, `Diagnostic`, and `SpiError` without every
provider implementing it.

### 4.3 Asynchronous contract

```rust
pub type SpiFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait AsyncEventBusSpi: Send + Sync + 'static {
    fn capabilities(&self) -> EventBusCapabilities;
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>>;
    fn subscribe<'a>(&'a self, request: SpiSubscriptionRequest) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>>;
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>>;
    #[doc(hidden)] fn provider_id(&self) -> Option<ProviderId> { None }
}

pub trait AsyncEventSubscriptionSpi: Send + 'static {
    fn id(&self) -> subscription::Id;
    fn receive(&mut self, timeout: Duration) -> SpiFuture<'_, Result<ReceiveOutcome, SpiError>>;
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> SpiFuture<'_, Result<(), SpiError>>;
    fn close(&mut self) -> SpiFuture<'_, Result<(), SpiError>>;
}
```

`Pin<Box<dyn Future + Send>>` is used instead of `async fn` in traits so the traits
stay object-safe and the `Send` bound is explicit (P5). The async facade calls
`receive` with `Duration::MAX` and relies on a `Waker`. A provider that needs a
timer (delayed delivery, for example) uses an injected `Timer` internally.
`AsyncLocalEventBusSpi` does this.

**Cancellation safety:** dropping the future returned by `receive` must not lose
the message. It stays queued or in-flight, because `AsyncSubscription::run` drops
a waiting `receive` when the subscription pauses or the bus shuts down. `settle`
and `close` futures should be short and, if dropped, leave a state that can be
retried. The facade retries settlement with backoff (§9.6).

### 4.4 Transport messages

- `OutboundMessage`: `topic: TopicAddress`, `id: EventId`, `timestamp`, `headers`,
  `ordering_key: Option<OrderingKey>`, `delay`, `payload: TransportPayload`.
  The facade builds it from `EventEnvelope<T>`. `TopicAddress` and `OrderingKey`
  are validated newtypes at the SPI boundary, so a provider does not re-check empty strings.
- `InboundMessage`: `topic`, `id`, `timestamp`, `headers`, `ordering_key`, `payload`,
  `settlement: Option<SettlementToken>`, `provider_metadata: ProviderMessageMetadata`.
  There is no `delay`; the provider has already applied it. `into_parts()` splits
  payload, metadata, and token in one move.
- `TransportPayload`
  - `Native(Arc<dyn Any + Send + Sync>)`: the in-process object. The facade `downcast`s it.
  - `Encoded(EncodedPayload { content_type, schema_id, bytes: Arc<[u8]> })`: cross-process bytes.
  - The facade chooses from `PayloadModes`: `Native` and `NativeAndEncoded` send `Native`;
    `Encoded` requires a codec, otherwise `CapabilityError::CodecRequired`.
- `ReceiveOutcome`
  - `Message(InboundMessage)`;
  - `Gap(DeliveryGap)`: the provider observed a loss or skip (the facade emits `Diagnostic::ReceiveGap`);
  - `TimedOut`;
  - `Closed`: the provider has ended the subscription (the facade ends the worker or `run`).
- `SpiSubscriptionRequest`: `subscription_id`, `topic: TopicAddress`, `subscriber_id`,
  `group: Option<ConsumerGroup>`, `durability`, `start_position`, `provider_options`,
  `payload_type_id: TypeId`. It is `SubscribeRequest<T>` with the facade-only options
  removed (filter, middleware, retry, dead-letter, `ack_mode`, ordering).
  `OrderingPolicy` is not forwarded. Per-key serialization is the facade's job.
  The provider only has to honor the `OrderingCapability` it declared.

### 4.5 `SettlementToken` and the settlement contract

```rust
pub struct SettlementToken {
    subscription_id: subscription::Id,
    inner: Box<dyn Any + Send>,      // provider-private state
}
```

- **Not `Clone`.** One receiver owner serializes attempts; the provider need not support concurrent settlement of the same token.
- `belongs_to(subscription_id)`: the facade checks ownership before settling. A provider may also reject a mismatched token with `SpiError::InvalidSettlementToken`.
- **Idempotent.** Settling the same `(token, disposition)` again must return `Ok(())`. The async facade retries the same disposition after a failure.
- **Conflict.** Settling the same token with a different disposition should error or be ignored. It must not corrupt state beyond a duplicate delivery.
- `DeliveryDisposition::{Accept, Retry, Reject}`: `Retry` asks the provider to deliver again and requires `SettlementCapabilities::AcceptRetryReject`. `Reject` is a terminal drop. Dead-letter publication, when it happens, has already been done by the facade.

### 4.6 Shutdown contract

`shutdown(ShutdownMode)`:

- `ShutdownMode::Graceful { timeout }` sets a caller deadline for the facade shutdown. A caller timeout returns `ShutdownError::TimedOut`; `ShutdownOutcome::TimedOut` is reserved for a provider that completed cleanup after its own graceful deadline.
- `ShutdownMode::Immediate` requests immediate stop admission and provider-specific discard/retention; it can still wait for running handlers or SPI calls and has no bounded-wait guarantee.
- The call is idempotent. After shutdown, `publish` and `subscribe` return `SpiError::Operation` whose `kind` is closed.

The facade permits at most one provider shutdown call in flight. A failed or
cancelled attempt can be retried by a later caller. The facade-level API returns
`ShutdownReport`; the SPI method still returns `ShutdownOutcome`. The
synchronous side uses `ShutdownCoordinator`. The asynchronous side uses a
leader CAS and lets `Immediate` upgrade an in-flight `Graceful` request.

---

## 5. Capability model

```rust
pub struct EventBusCapabilities {
    payload_modes: PayloadModes,                 // Native | Encoded | NativeAndEncoded
    settlement: SettlementCapabilities,          // None | AcceptOnly | AcceptRetryReject
    ordering: OrderingCapability,                // None | PerSubscription | PerKey | PerPartition
    delayed_delivery: DelayedDeliveryCapability, // None | Native
    durability: DurabilityCapability,            // Ephemeral | Durable
    subscription_modes: SubscriptionModes,      // Ephemeral | Durable | Both requests accepted
    consumer_groups: bool,
    replay: ReplayCapability,                    // None | Position | Timestamp
    publish_guarantee: PublishGuarantee,         // FireAndForget | Accepted | Confirmed | DurablyStored
    publish_visibility: PublishVisibility,       // Opaque | DestinationAdmissions
}
```

How the facade uses these capabilities (P3):

| Request | Checked at | When the capability is missing |
| --- | --- | --- |
| `delay` | Publish pipeline | `PublishError::Capability`; the message is not published |
| `ordering_key` | Publish pipeline | `PublishError::Capability` when ordering is `OrderingCapability::None` |
| `AckMode::Manual` | Subscribe | `SubscribeError::Capability` unless settlement is `AcceptRetryReject` (a manual nack needs the provider to `Retry` or `Reject`) |
| `OrderingPolicy::PerKey` | Subscribe | `SubscribeError::Capability` when `ordering.supports_per_key()` is false (`PerKey` or `PerSubscription`) |
| `SubscriptionDurability::Durable` | Subscribe | Rejected when durability is `DurabilityCapability::Ephemeral` |
| Any subscription durability | Subscribe | Rejected with `subscription_durability` when the provider does not accept that request mode |
| `consumer_group` | Subscribe | Rejected when `consumer_groups == false` |
| `StartPosition::Earliest` | Subscribe | Rejected when replay is `ReplayCapability::None` |
| `PayloadModes::Encoded` | Publish / subscribe | `CapabilityError::CodecRequired` when no codec is available (an encode or decode failure is a `CodecError`) |
| `FailureDirective::Requeue` | Delivery failure | Falls back to `Discard` / `Reject` when settlement cannot `Retry` (see §7.5) |
| `Diagnostic::SettlementUnavailable` | Delivery failure | Emitted when settlement is required but cannot be performed, instead of failing silently |

`RequiredCapabilities` (§6.2) reuses the same enums so a caller can demand
"at least these capabilities" at **creation** time and learn about a mismatch before the bus is used.
`DurabilityCapability` describes the provider's retention guarantee; `SubscriptionModes` separately lists
which requested modes it accepts. Providers migrating from the previous constructor must add the required
`SubscriptionModes` argument immediately after `DurabilityCapability`: use `EPHEMERAL`, `DURABLE`, or
`BOTH` according to the modes their `subscribe` implementation accepts. This declaration does not change
the retention guarantee. The facade rejects an unsupported mode before calling `subscribe`.

---

See the [local/Redis comparison](user_guide.md#compare-local-and-redis-capabilities) for actual capability values and PEL/cursor/fsync/close recovery limits. Local is Native/Ephemeral with PerKey and Native delay; Redis is Encoded/Durable with groups and Position replay, but ordering/delay are both None.

## 6. Registry, discovery, and provider assembly

### 6.1 `EventBusSpec` and `qubit-spi`

```rust
use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::AsyncServiceSpec;
use qubit_spi::ServiceSpec;
use qubit_spi::SyncServiceSpec;

pub struct EventBusSpec;

impl ServiceSpec for EventBusSpec {
    type Config = EventBusConfig;
    type Error = EventBusProviderError;
}

impl SyncServiceSpec for EventBusSpec {
    type Output = Arc<dyn EventBusSpi>;
}

impl AsyncServiceSpec for EventBusSpec {
    type Output = Arc<dyn AsyncEventBusSpi>;
}
```

`EventBusProvider` and `AsyncEventBusProvider` are aliases of `qubit-spi` traits
(`dyn ProviderDefinition<EventBusSpec>` and `dyn AsyncProviderDefinition<EventBusSpec>`).
A provider author implements `qubit-spi`'s `ServiceProvider` and `ProviderMetadata`
(id, aliases, `create`). This crate does not add another provider trait.

`EventBusRegistry` and `AsyncEventBusRegistry` wrap `qubit_spi::ProviderRegistry<Spec>` and offer:

- `new()`, `with_local()` (pre-registers the built-in `local` provider), and `discover()` (see §6.4);
- `register(provider)` and `register_shared(Arc<EventBusProvider>)`;
- `descriptors()`, `provider_ids()`, `default_selection()`, and `set_default_selection()`;
- `seal()` and `is_sealed()`: freeze the catalog; later `register` calls fail;
- `create(&EventBusConfig)`: uses the selection inside the config, otherwise the catalog default;
- `create_selected(&ProviderSelection, &EventBusConfig)`: an explicit selection.

Both return a constructed `EventBus` or `AsyncEventBus`. Facade configuration also
comes from `EventBusConfig`. `ProviderSelection` comes from `qubit-spi` and supports
a fallback chain. Candidates are tried in order at creation time. If every candidate
fails, the errors are aggregated as `ProviderError::Resolution` or `Creation` (P4).

### 6.2 `EventBusConfig` and `RequiredCapabilities`

`EventBusConfig` is the whole input of one creation call, in builder style:
`with_selection(ProviderSelection)`, `with_facade_config(EventBusFacadeConfig)`,
`with_required_capabilities(RequiredCapabilities)`, and `with_provider_options(ProviderOptions)`.
Provider choice, facade configuration, and provider-private parameters travel together,
so a caller can map one configuration object onto the call.

`RequiredCapabilities::missing_from(&caps)` returns the missing items.
`EventBusProviderAdapter` checks them immediately after the provider is constructed.
A miss fails that candidate and the registry tries the next one.
**This is the only place the registry falls back because of capabilities.**
Once the facade exists, a capability mismatch never switches the provider.

`ProviderOptions` (`BTreeMap<String, String>`) is namespaced, `Debug`-printable,
non-sensitive configuration. The subscribe builder rejects a bad pair in `build()`:
the key must contain `.`, must not start or end with `.`, and neither the key nor
the value may contain a control character. Otherwise `build()` returns
`SubscribeRequestBuildError::InvalidProviderOption`. Prefixes mark ownership, for
example `local.queue_capacity`. The core crate does not interpret another provider's
namespace and does not define a generic authentication model. Passwords, tokens,
and private keys must not be stored in these fields. A provider should obtain
credentials through an external credential reference or its own secure configuration.
A provider interprets only its own namespace and must reject unknown keys under that
namespace. The built-in `local` provider does this in
`LocalEventBusConfig::from_provider_options`, returning `ConfigurationError::InvalidField`.

### 6.3 `EventBusProviderAdapter` and `IdentifiedEventBusSpi`

- `EventBusProviderAdapter` adapts `Arc<EventBusProvider>` to the shape `qubit-spi`
  catalogs expect: it exposes `descriptor()`, calls the provider's `create` from
  `create_configured`, then checks `RequiredCapabilities`.
- A successfully created SPI is wrapped in `IdentifiedEventBusSpi { provider_id, inner }`.
  The wrapper forwards every method and overrides `provider_id()` with the real id.
  The registry then builds `EventBus::with_config(provider_id, spi, facade_config)`.
- Facade constructors (`from_spi`, `with_config`, `with_timer`) **require** an explicit
  `ProviderId`. `DeliveryContext::provider_id`, `PublishReceipt::provider_id`, and the
  provider field on diagnostics are therefore always set. The `provider_id()` hook
  exists so the registry does not have to carry the id in a separate return value.

### 6.4 Discovery: inventory registration

With the `discovery` feature:

- This crate declares two catalog modules in `registry::provider_inventory` with
  `qubit_spi::declare_sync_provider_inventory!` and `declare_async_provider_inventory!`:
  `qubit_event_bus::registry::sync_provider_inventory` and `async_provider_inventory`.
- A provider crate registers itself with
  `qubit_spi::submit_sync_provider! { inventory_entry = qubit_event_bus::registry::sync_provider_inventory::Entry; spec = ...; provider = ...; }`
  (async providers use `submit_async_provider!`).
- The application calls `EventBusRegistry::discover()` or `AsyncEventBusRegistry::discover()`
  to collect every linked provider. `discover()` fails when two providers share a selection name.

Built-in `local` is **asymmetric**. `local_event_bus_provider.rs` submits the synchronous
`local` provider with `qubit_spi::submit_sync_provider!` under `#[cfg(feature = "discovery")]`.
`async_local_event_bus_provider.rs` does **not** submit anything. Therefore
`AsyncEventBusRegistry::discover()` does not contain `local`. Callers add it with
`AsyncEventBusRegistry::with_local()` or `register(AsyncLocalEventBusProvider)`.
(`AsyncEventBus::local(config)` is `with_local()` followed by `create`.)

`tests/discovery_*` and the `discovery_consumer` / `discovery_provider` fixtures
cover cross-crate linking, including a multi-crate workspace.

---

## 7. Shared facade layer

Both facades share the `pipeline` module. Behavior in this section is the same
for both unless a sentence says otherwise.

### 7.1 `EventBusFacadeConfig`

Configuration supplied to `EventBus::with_config`, `AsyncEventBus::with_config`,
or `Registry::create`. It is frozen when the facade is created:

| Item | Type | Role |
| --- | --- | --- |
| `codecs` | `CodecRegistry` | Registers `Arc<dyn EventCodec<T>>` by `TypeId`. `resolve_codec` prefers the codec carried by the `Topic`, then the registry |
| `publisher_interceptors` | `Vec<Arc<dyn Fn(&mut PublishMetadata) -> Result<bool, PublishError>>>` | **Global** publish interceptors. They may only read and write headers (`PublishMetadata`). Returning `false` drops the message |
| `subscriber_interceptors` | `HashMap<TypeId, Vec<Arc<SubscriberInterceptor<T>>>>` | **Global synchronous** middleware registered per payload type |
| `async_subscriber_interceptors` | Same shape, async | Global async middleware (used only by `AsyncEventBus`) |
| `delivery_scheduling` | `DeliverySchedulingConfig`: running=4, owned=256, per-subscription=32, subscriptions=256 | Independent count limits shared by both facades |
| `settlement_retry` | `SettlementRetryConfig`: 5 attempts, 5 seconds, initial backoff 10 ms, cap 1 second | Retry only explicitly retryable settlement failures |
| `payload_limits` | `PayloadLimits` | Independent positive encoded publish/receive byte limits, each 1,048,576 by default |

The facade reads provider capabilities once during construction and keeps that
immutable snapshot for later validation. If `capabilities()` panics during direct
construction, the constructor returns a terminal `SpiError` classified as
`provider_panicked`.

Global interceptors and middleware are **added to** request-level ones. On publish,
typed request interceptors run first and global metadata interceptors run after them.
On subscribe, global middleware wraps the request middleware: global, then typed, then the handler.

`DeliverySchedulingConfig::new(NonZeroUsize, NonZeroUsize, NonZeroUsize, NonZeroUsize)` takes running, owned, per-subscription, and subscriptions in that order. Running/per-subscription may not exceed owned. Configure through `with_delivery_scheduling` / `with_settlement_retry`; getters are `delivery_scheduling` / `settlement_retry`.

### 7.2 Codec resolution

`resolve_codec::<T>(topic, registry)`:

1. Use `topic.codec()` when it is present.
2. Otherwise use `registry.get::<T>()`.
3. Otherwise `None`.

Only a `PayloadModes::Encoded` provider **requires** a codec (missing codec →
`CapabilityError::CodecRequired`). `Native` and `NativeAndEncoded` both use
`TransportPayload::Native`. The subscribe side decodes an `Encoded` payload with
the same resolution rule. `EventCodec::decode` receives `&EncodedPayload`; default
`validate_metadata` checks exact content type and optional schema equality. The receive
boundary checks byte length, validates metadata, then decodes, before invoking handlers.
Only ordinary `CodecError::Decode` takes the existing bad-message `Reject` path.
`MetadataMismatch`, receive `PayloadTooLarge`, codec `Panicked`, and
`NativeTypeMismatch` stop reception without any settlement. The first
`Arc<SubscriptionStopReason>` is retained and exposed by `terminal_failure()`;
async `run` returns `ReceiveError::Stopped`, including on later runs of the same
handle. Already-started handlers finish; close errors remain independent.
Durable work remains recoverable by a new subscription after the configuration
or codec is repaired. Ephemeral receiver cleanup may discard work, and known
abandonment is counted once. Healthy subscriptions remain active.

`PayloadLimits` has independent positive `max_publish_bytes` and
`max_receive_bytes`, both 1 MiB by default. Exactly the limit is permitted;
there is no unlimited setting. Publication checks after encoding and before
provider admission; receiving checks before codec callbacks. This does not
bound codec or Redis-client allocations, nor deep native Rust payload memory.

### 7.3 Publish pipeline (`PublisherPipeline`)

Steps of `publish(request)`. `publish_all` runs them for each request in order and collects every result:

1. **Lifecycle gate.** If the bus is not `Running`, return `PublishError::Closed`.
2. **Request-level typed interceptors.** `Fn(EventEnvelope<T>) -> Result<Option<EventEnvelope<T>>, PublishError>`,
   chained in registration order. An interceptor may transform the envelope but cannot change its event ID;
   an ID change fails with a configuration cause before calling the SPI.
   `None` drops the message and the receipt is `AdmissionOutcome::Dropped`.
   A panic is isolated as `PublishError::InterceptorPanicked`.
3. **Global metadata interceptors.** They may change headers only. Any `false` drops the message.
4. **Dead-letter header protection.** If the original envelope carried the dead-letter
   header, it is written back after the interceptors, so the dead-letter identity cannot be lost.
5. **Capability checks.** `delay.is_some()` with `DelayedDeliveryCapability::None`,
   or `ordering_key.is_some()` with `OrderingCapability::None`, returns `PublishError::Capability`.
6. **Payload preparation.** Choose `Native` or `Encoded` from `PayloadModes`. `Encoded` encodes with the codec and produces an `OutboundMessage`.
7. **Publish with retry.** When `retry_policy` is set, `qubit_retry::Retry` (synchronous)
   or `AsyncRetry` (asynchronous, with a `Timer`) wraps `spi.publish`. The retry decision
   combines `SpiError::retryable()` with the caller's `RetryRule<PublishAttemptError>`.
   The fallback is `RetryFallback::Abort`. Exhaustion becomes
   `PublishError::Retry(Box<RetryError<...>>)`. With no policy there is a single attempt.
   Provider errors declare `PublishEffect` through `SpiError::Publish`; generic
   operation errors and provider panics conservatively mean uncertain admission.
   The default `DuplicateRiskPolicy::Forbid` aborts uncertain retries before any
   custom rule. `AllowDuplicates` only permits entry into the existing retry policy.
   Uncertainty is retained across attempts and sets `duplicate_possible()` on a
   later successful receipt. In-flight cancellation that returns an error is
   uncertain; RetryPolicy budgets are soft, not universal hard I/O timeouts.
8. **Error handlers.** On failure, `PublishErrorHandler<T>(&PublishFailureContext<T>, &PublishFailure)`
   runs in registration order. If any handler panics, the final error becomes
   `PublishError::ErrorHandlerPanicked` and the remaining handlers still run.
9. **Admission diagnostics.** When the receipt carries `DestinationAdmissions`, each
   rejected destination emits `Diagnostic::AdmissionRejected { event_id, topic, subscriber_id, reason }`.
10. **Metrics.** Update `PublishMetricsSnapshot` (`attempts`, `errors`, `dropped`,
    `opaque_accepted`, `zero_destinations`, `accepted_destinations`,
    `filtered_destinations`, `rejected_destinations`), readable through `publish_metrics()`.

Internally, `PipelineFailure { origin, error, publish_effect }` records **which step** failed
(interceptor, capability, codec, SPI, error handler, and so on) for tests and logs.
Callers receive `PublishFailure` with the original event ID, aggregate effect,
and structured `PublishError` cause; the source chain is preserved.

Sync and async publication share `prepare_publish`, `finish_publish_success`, and `finish_publish_failure`. Preparation runs interceptors and encoding once; later attempts retain ID, bytes, and monotonic duplicate-possible evidence. SPI calls and retry/cancellation driving stay in their respective adapters.

### 7.4 Subscribe pipeline (`SubscriberPipeline`): one message

`SubscriberPipeline<T>` is called by the synchronous worker and by the asynchronous run loop:

```
InboundMessage
  │ into_parts()
  ├─ Encoded: check receive byte limit, then metadata, then decode
  │    ├─ ordinary CodecError::Decode → ordered Reject settlement (bad message)
  │    └─ oversized / incompatible metadata / panic → StopUnsettled; stop subscription, retain unsettled source
  ├─ Native: downcast → Arc<T>; NativeTypeMismatch → StopUnsettled
  ├─ rebuild EventEnvelope<T>
  ├─ filter(&envelope)? ── false → settle Accept, done
  │                     ── panic → treated as a handler failure
  ├─ ordering lane (PerKey serializes by ordering_key; see §8.2 / §9.3)
  ├─ global middleware → typed middleware → handler     (middleware is Fn(Delivery<T>, next))
  ├─ ACK matrix → success / DeliveryError
  ├─ failure: local retry (qubit-retry) → error handlers → FailureDirective
  ├─ terminal: DeliveryFailureAction → dead-letter / settle Retry|Reject|Discard
  └─ settle SettlementToken (when present)
```

**Middleware.** `next` is `Box<dyn FnOnce(Delivery<T>) -> ...>`. Middleware must call
it at most once. Not calling it short-circuits with the middleware's own result.
Calling it twice is impossible at the type level. The async form,
`AsyncSubscriberNext<T>`, returns `SpiFuture<'static, ...>`, which the middleware owns and awaits.

**ACK matrix** (handler result × `Acknowledgement` state → outcome):

| `AckMode` | Handler result | `Acknowledgement` | Outcome |
| --- | --- | --- | --- |
| `Auto` | `Ok` | any | success, `Accept` |
| `Auto` | `Err(e)` | any | failure `e` |
| `Manual` | `Ok` | `ack()` was called | success, `Accept` |
| `Manual` | `Ok` | not called | failure: `DeliveryError::Handler` ("manual acknowledgement remained pending") |
| `Manual` | `Ok` | `nack()` was called | failure: `DeliveryError::Handler` ("delivery was negatively acknowledged") |
| `Manual` | `Err(e)` | any | failure `e` |

A panic in the handler or middleware is also wrapped as `DeliveryError::Handler`
(with the panic message) and follows the same failure path. `DeliveryError` has
four variants on purpose (`Handler`, `Codec`, `Spi`, `Retry`), so an error handler
and a `RetryRule` only distinguish application failure, decode failure, backend failure, and retry exhaustion.

### 7.5 Failure handling: local retry, error handlers, `FailureDirective`

```
attempt ──failure──▶ error-handler chain (SubscribeErrorHandler<T>) ──▶ FailureDirective
                                                                        │
   ┌────────────────────────────────────────────────────────────────────┤
   │ Retry      → let qubit-retry decide another attempt (when a policy exists); otherwise same as Discard
   │ Requeue    → stop local attempts, settle Retry (needs AcceptRetryReject; otherwise do not settle)
   │ DeadLetter → stop local attempts, publish the dead-letter, then settle Reject; a forwarding failure stops the subscription and leaves the source token unsettled
   │ Discard    → stop local attempts, settle Reject (needs AcceptRetryReject; otherwise do not settle)
   └────────────────────────────────────────────────────────────────────
```

Details:

- **Aggregating error handlers.** Every handler runs, so each can log or record metrics.
  The directive rule is: **the first non-`Retry` return wins**. Retry continues only when
  every handler returns `Retry`. A panicking handler is recorded as `Diagnostic::InternalFailure`
  and treated as `Discard`. With no handlers configured, a `retry_policy` means `Retry`
  and the absence of a policy means `Discard`. A decision to stop retrying always wins,
  so any single handler can stop the loop.
- **Local retry.** The facade retries only when `retry_policy` is set. After each failure
  the error handlers run first. The `qubit-retry` rule chain continues only when the
  directive is `Retry` (caller `RetryRule`, then `DeliveryAttemptError::retryable`,
  then the policy default). If the synchronous retry rule itself panics, the outcome
  becomes `Requeue`, handing the decision back to the provider. With no policy, `Retry`
  is the same as `Discard`. Synchronous backoff uses `qubit_retry::Retry` and sleeps
  on a handler-pool thread. Asynchronous backoff uses `AsyncRetry` and the `Timer`.
- **`DeliveryFailureAction`** is the directive combined with settlement capability:
  - `DeadLetter`: when a `DeadLetterPolicy` exists and the message is **not** already
    a dead-letter, an internal publish sends `DeadLetterEvent<T>`. That publish
    **skips** global publish interceptors, so a dead-letter record is not rewritten
    or dropped by them. Success then `Reject`s. If forwarding fails or no
    dead-letter policy is configured, the subscription stops and the source token
    remains unsettled so receiver close can apply the provider's recovery contract.
    A message that already carries the reserved header is not published again; it
    is `Reject`ed.
  - `Requeue`: settlement `AcceptRetryReject` becomes `Retry`.
  - `Discard`, or local retry exhausted: `AcceptRetryReject` becomes `Reject`.
  - When settlement is `None` or `AcceptOnly`, **no failure is settled**
    (`SubscriberPipeline::failure_disposition` returns `None`). If the message has
    a token, `Diagnostic::SettlementUnavailable { requested }` records what the
    facade wanted to do. Under `AcceptOnly`, whether an unsettled message is
    redelivered is the provider's decision.
  - Internal failures while building or publishing a dead-letter (missing policy,
    envelope construction failure, publish failure) are recorded as
    `Diagnostic::InternalFailure` and stop the subscription. Ephemeral loss is
    counted where the facade can identify it.
- Every terminal failure emits `Diagnostic::DeliveryFailed { event_id, topic, subscription_id, subscriber_id, attempts, error }`.

### 7.6 Dead-letter recursion

A dead-letter message carries the header `x-qubit-event-bus-dead-letter: v1`.
When `SubscriberPipeline` reaches a terminal failure for a message that already
has the header, it does not publish a second-level dead-letter; it `Reject`s.
Dead-letter header protection prevents recursive forwarding. It does not limit
provider duplicates: forwarding and source settlement are not atomic. A lost
forward reply or failed source settlement may cause duplicate logical records.
The publish uncertainty gate also applies here; failed/uncertain forwarding stops
the source subscription and retains durable recovery state. Consumers deduplicate.

### 7.7 Relationship to `qubit-retry`

- This crate depends on `RetryPolicy`, `RetryRule`, `RetryCancellationToken`,
  `Retry` / `AsyncRetry`, `RetryError`, and `RetryFallback` from `qubit-retry`.
  It does **not** re-export them (P9).
- `PublishAttemptError` and `DeliveryAttemptError` are internal wrappers that pass
  `SpiError::retryable()` and the `DeliveryError` classification into a `RetryRule`.
- Cancellation: `retry_cancellation_token` can stop a retry early. A synchronous
  `Immediate` shutdown also cancels in-flight retry.

---

## 8. Synchronous facade: `EventBus`

### 8.1 Structure

```
EventBus (Arc<Inner>)
 ├─ spi: Arc<dyn EventBusSpi>
 ├─ config: EventBusFacadeConfig
 ├─ lifecycle: LifecycleTracker            Running → Closing → Closed
 ├─ operations: OperationGate              counts in-flight publish/subscribe; shutdown waits for zero
 ├─ scheduler: SyncDeliveryScheduler       shared handler pool (started lazily)
 ├─ subscriptions: Mutex<HashMap<Id, SubscriptionControl>>
 ├─ shutdown: ShutdownCoordinator
 ├─ diagnostics: DiagnosticObservers
 └─ metrics: PublishMetrics
```

Threads, named so a debugger can tell them apart:

| Thread | Count | Role |
| --- | --- | --- |
| `event-bus-subscription-{id}` | one per subscription | Coordinator: loops `receive(50 ms)`, decodes and filters, hands handler work to the scheduler, and **performs settlement** |
| `event-bus-handler-{i}` | `max_running_handlers` (default 4), started lazily on the first `subscribe` | Runs middleware, the handler, local retry, error handlers, and dead-letter publish |
| `event-bus-shutdown` | at most one, spawned at shutdown | Drains, joins, and calls provider shutdown in the background |

`EventSubscriptionSpi` is a single owner (`&mut self`), so each subscription needs
**one** thread that calls `receive` and `settle`. Handlers can be slow. Running them
serially on that same thread would give neither concurrency nor a global limit.
The coordinator therefore does I/O and settlement, and handlers run in a shared
bounded pool. `max_running_handlers` limits running handlers; owned work has its own budget.

### 8.2 `SyncDeliveryScheduler`

Sync and async adapters share a metadata-only `DeliverySchedulerCore`; each owner retains its receiver, payloads, and handler futures. Before receive, reserve global and per-subscription owned capacity atomically. Backpressure occurs before pulling a message. A single owned lease transfers through reserved, queued, running, and settling stages, with no extra pending message outside the limits.

Eligible work rotates across subscriptions, then keys. Only an executor actually taking a ready candidate acquires a handler slot and lane. Same-key queues and settlement backoff consume no handler slot. Handler completion releases its execution slot; the lane remains owned until settlement succeeds or the subscription terminates. `OrderingPolicy::None` makes each delivery independently eligible. `PerKey` preserves FIFO within `(topic, key, subscription_id)`; keyless messages share that subscription/topic's None lane.

Fairness requires already-received eligible candidates and executors that keep progressing. Receiving B still needs owned capacity; an infinite upstream backlog of A cannot be bypassed. With H=4, D=256, S=256: running≤H, owned≤D, receivers≤S, lanes≤D and waiters≤S. Count bounds do not bound payload bytes.

### 8.3 Coordinator thread (`run_subscription_worker`)

One owner serializes receive, settle, and close for each receiver. It reserves before receiving and retains an owned map plus ready queue; timeout, Gap and Closed release reservations. Handlers send immutable token/disposition intents and completion notifications through `OwnerSettlementRouter`, without waiting on a zero-capacity settlement reply. The owner correlates both messages without releasing ownership early because of their arrival order.

Settlement failures use shared `SettlementRetryState`, preserving token/disposition without rerunning the handler. Permanent errors, unknown retryability, exhausted budgets, panic, invalid token, or infrastructure failure publish the first terminal cause and stop receiving/starting handlers for that subscription. Unstarted durable work follows provider close recovery; ephemeral work is counted as abandoned. Started handlers may finish, but a terminated receiver receives no new settlement attempts. Close errors do not replace the first cause. A blocked SPI call or handler cannot be forcibly terminated.

### 8.4 `subscribe`

1. `OperationGate::enter()`. If the bus is not `Running`, return `SubscribeError::Closed`.
2. Reject `async_interceptors`, whether they are on the request or registered on the facade for this `T`.
3. Check capabilities (§5).
4. Resolve the codec (`Encoded` mode requires one).
5. `spi.subscribe(SpiSubscriptionRequest)`, with panic isolation.
6. Build `SubscriberPipeline<T>`, `SubscriptionControl`, and an `Arc<AtomicBool>` cancel flag.
7. Start scheduler workers lazily and spawn the coordinator thread.
8. Return the `Subscription` handle.

`Subscription` exposes `id()`, `subscriber_id()`, `is_cancelled()`, and `cancel()`.
**Drop does not cancel.** The subscription lives with the bus until `cancel()` or
`shutdown()`. `cancel()` first returns queued tasks through the scheduler, then sets
the cancel flag and `join`s the coordinator. If `cancel()` is called from bus context
(a handler, middleware, or the coordinator thread), the join is skipped so the thread
does not join itself.

### 8.5 Wait primitives

Both have the signature `(&Topic<T>, Option<Duration>) -> Result<WaitOutcome, LifecycleError>`.
`WaitOutcome` is `Idle` or `TimedOut`. `None` means wait without a deadline.

- `wait_for_idle` forwards to `spi.wait_for_topic_idle`. A provider result of `None`
  becomes `LifecycleError::IdleWaitUnsupported`. This is the **provider view**:
  that topic has no pending or in-flight work.
- `wait_for_received_deliveries` waits until deliveries of that topic which the
  facade has **received but not finished** reach zero (`LifecycleTracker` counts by
  topic name). This is the **facade view** and does not ask the provider.
  The two complement each other: the first says the provider queues are empty, the
  second says the handlers have finished. A test that publishes and then asserts
  should call one of them first.

### 8.6 Deadlock detection: `BusContextGuard`

Coordinator and handler threads install a thread-local `BusContextGuard` before
entering user code. From that context, an operation that would block waiting for
itself (`shutdown` waiting synchronously, `cancel()` joining, `wait_for_*`) returns
`LifecycleError::WouldDeadlock { operation }` (wrapped in `ShutdownError::Lifecycle`
for shutdown) instead of deadlocking. Shutdown can still be **started** from bus
context, because it moves onto a background thread. It cannot be waited for there.

### 8.7 Shutdown: `ShutdownCoordinator`

```
request_shutdown(mode: ShutdownMode) -> Result<EventBusShutdown, ShutdownError>
  ├─ lifecycle: Running → Closing; close OperationGate admission
  ├─ coordinator.begin(mode) → exact generation; Immediate strengthens active Graceful
  ├─ scheduler.request_stop(immediate); request cancellation on subscription controls
  ├─ start one `event-bus-shutdown` coordinator thread when this generation needs a leader
  │     wait for OperationGate → finish/clear owner work → wait and join receiver owners
  │     → scheduler.join() → spi.shutdown(mode) → cache report and mark Closed
  └─ return the generation ticket without waiting for handlers, joins, or SPI

EventBusShutdown::wait(Some(timeout)) → bounded blocking observation
EventBusShutdown::wait_async()         → runtime-neutral Waker observation
shutdown(mode)                         → request_shutdown(mode) + ticket.wait(mode timeout)
```

`request_shutdown` closes admission and signals the fixed-pool scheduler; it does not run queued handler or settlement callbacks on the requesting thread. The scheduler marks graceful drain or immediate cancellation, then wakes receiver owners. Owners release their own delivery leases and retained payloads; already granted handler callbacks continue on the fixed pool. A separate coordinator joins owners and pool workers before the single provider shutdown call. Immediate strengthens a running Graceful generation, but cannot interrupt a callback or provider call already in progress.

Each ticket retains one exact generation's result until it is dropped. Concurrent requests can join the current generation, and a closed bus returns a ready ticket with the cached report. `wait` timeout limits only that observation; background cleanup continues. `wait_async` registers an independent cancelable waker and neither blocks a thread nor requires a runtime. Cancelling an observation future removes only its waker registration; a retained ticket can be observed again. A coordinator start failure remains associated with tickets for that generation even if a later request starts a retry generation.

`request_shutdown` is safe from bus-owned callbacks because it returns without waiting. Synchronous `shutdown` and `EventBusShutdown::wait` return `WouldDeadlock` when invoked from a callback that would need to finish first. The report includes facade-known abandoned ephemeral deliveries and whether provider-owned work may also have been abandoned without an exact count. `EventBus` has no `Drop` shutdown; dropping either the bus handle or ticket does not cancel cleanup.

---

## 9. Asynchronous facade: `AsyncEventBus` and `AsyncSubscription`

### 9.1 Staying runtime-neutral

- **No spawn.** The facade does not know whether an executor exists. `subscribe` returns
  an `AsyncSubscription`. The consume loop starts when the caller `.await`s `subscription.run()`.
- **Time is injected.** Constructors are `from_spi(provider_id, spi)`,
  `with_config(provider_id, spi, config)`, `with_timer(provider_id, spi, timer)`,
  `with_config_and_timer(...)`, and `AsyncEventBus::local(config).await`. Every sleep
  (retry backoff, settlement backoff, `wait_for_received_deliveries` timeout, shutdown
  deadline) goes through `Arc<dyn Timer>`. The default is `qubit_clock::StdTimer`.
  Tests advance time deterministically with the manual timer in `tests/support/manual_async.rs`.
- **Wakeups use `Waker`.** `AsyncSignal` (one-shot and resettable) and the shared scheduling core register and wake `Waker`s. They do not use channels or threads.
- **Every SPI call is a boxed `Send` future**, and `catch_spi_future` captures a provider panic.

### 9.2 Shared ownership and execution capacity

Async uses the same `DeliverySchedulerCore` and four-parameter configuration as §8.2. Registered sessions, including paused sessions, count toward `max_subscriptions` until close/terminal cleanup actually finishes. Each subscription has at most one receive waiter; cancellation removes it and released capacity wakes waiters. Conditions are rechecked after wake registration to avoid lost wakeups.

### 9.3 Per-key lanes

Lanes are keyed by `(topic, key, subscription_id)`. Queued same-key messages consume no handler slot, and successors cannot bypass an unsettled predecessor. `OrderingPolicy::None` uses independently eligible deliveries. The fairness conditions and resource bounds are those in §8.2.

### 9.4 `AsyncSubscription`: resumable sessions

`AsyncSession` owns the receiver and retains `buffered`, `tasks`, `completed`, and completions arising during settlement. `PendingDelivery` retains payload, token, immutable disposition, settlement timing state, and one owned lease. A handler factory is not called before a runnable grant; started futures remain in the session.

Each turn polls started tasks in rotation, advances one receiver operation, then receives/dispatches when capacity allows. Settlement backoff and an in-flight async settle still allow polling started handlers, while that receiver never receives concurrently. Dropping `run` pauses: owned work, lanes, tokens, and timing remain in the session. Resuming run or shutdown takes over without implicit spawn.

`close().await` stops and closes one subscription; Drop disposes its receiver under the provider's durable/ephemeral semantics. This differs from the non-cancelling synchronous handle Drop. Cancelling receive is not destroying the receiver. Cancelling settlement fabricates neither Accept nor Reject; a started attempt remains charged to the budget.

### 9.5 `AsyncEventBus::subscribe` and subscriptions that never start

`subscribe` returns as soon as the SPI receiver exists. If the caller never calls
`run()` and then `shutdown`s, `close_unstarted_subscriptions` `lease()`s each control
and `close_inner()`s it, so the provider receiver is closed and no subscription is
left hanging. If `subscribe` completes when the bus is no longer `Running`, the
receiver is closed immediately and the call returns `SubscribeError::Closed`.

### 9.6 Finite settlement retries

`SettlementRetryConfig::new(max_attempts, max_elapsed, initial_backoff, max_backoff)` takes `NonZeroU32` and three `Duration`s. Defaults are 5 attempts including the first, 5 seconds, 10 ms initial backoff, and a 1-second cap. Elapsed and initial backoff must be nonzero and max must be at least initial; one attempt is valid.

The shared state is `Ready → Attempting → Settled | Waiting(deadline) | Terminal`. Only `retryable()==Some(true)` retries; false and None stop as `PermanentError` and `RetryabilityUnknown`. Other terminal classifications are `AttemptsExhausted`, `DeadlineExceeded`, `ProviderPanicked`, `InvalidToken`, and `InfrastructureFailure`. Failure n waits `min(initial * 2^(n-1), max)` with saturating arithmetic. Recheck attempts and monotonic elapsed budget before entering SPI; timer-registration failure does not count a SPI call.

Each failure emits `SettlementFailed` with `attempt` and `Arc<SpiError>`; first termination emits `SettlementStopped` and is retained by `terminal_failure()`. Budgets do not interrupt an in-flight call: a success returned after the deadline is still success. A cancelled in-flight attempt remains charged; pause itself is not termination, and resume checks the budget again.

### 9.7 Shutdown

The async facade has no provider-view `wait_for_idle`. It has `wait_for_received_deliveries`.
`AsyncSubscription` exposes `id()`, `subscriber_id()`, `run(handler).await`, and `close().await`.

```
shutdown(mode: ShutdownMode).await
  ├─ CAS elects a leader; other callers wait and receive the same result
  ├─ lifecycle Closing; OperationGate closes admission
  ├─ tell every control to stop (Graceful: finish in-flight; Immediate: return as soon as possible)
  ├─ for each control: shutdown(mode) — lease the session → drain (Graceful) → close_inner
  ├─ wait for runners_stopped (a timeout is measured with Timer; expiry is TimedOut)
  ├─ catch_spi_future(spi.shutdown(mode))
  └─ lifecycle Closed
```

`Immediate` can also strengthen a `Graceful` shutdown already in progress. A control's stop level only rises.

A bounded caller wait is not forced process exit. Use two `Graceful` waits with individual timeouts, handle `ShutdownError::TimedOut`, and hand incomplete cleanup to an external supervisor. `Immediate` can still wait for an uncooperative handler/SPI and is not a timeout rescue. Cancelled futures resume through coordinator state. See the [compiled sync/async policy](user_guide.md#shut-down-a-sync-bus).

### 9.8 `BusContextFuture`: deadlock detection at poll scope

The async side has no thread to bind, so it uses a **poll scope**. A delivery task
enters bus context while polling the handler or middleware and leaves it when the
poll returns. Inside that scope, `wait_for_received_deliveries` or `shutdown().await`
returns `WouldDeadlock` immediately, because waiting for the delivery that is currently being polled cannot succeed.

---

## 10. Built-in `local` provider

`local` is the only provider shipped with the crate. It is **in-process, low-latency,
broadcast per topic, and bounded**, and it covers enough of the capability matrix
to exercise the facade:

| Capability | Declared value | Notes |
| --- | --- | --- |
| `payload_modes` | `Native` | Forwards `Arc<dyn Any>` only. An `Encoded` payload returns `unsupported_payload_mode` (the facade chooses the payload from the capability, so this path is not taken in normal use) |
| `settlement` | `AcceptRetryReject` | Full settlement, including manual ACK |
| `ordering` | `PerKey` | Follows from the lane structure |
| `delayed_delivery` | `Native` | Delay heap |
| `durability` | `Ephemeral` | Close discards state |
| `consumer_groups` | `false` | Every subscription receives every message (broadcast) |
| `replay` | `None` | History is not retained |
| `publish_guarantee` | `Accepted` | Returns once the message is queued |
| `publish_visibility` | `DestinationAdmissions` | Reports admission per subscription |

The `Encoded` payload path is covered by test providers in `tests/support/provider_shapes.rs`
and `fake_spi.rs` that declare `Encoded` or `NativeAndEncoded`, so the codec pipeline
does not depend on `local`.

### 10.1 Shared queue: `LocalQueueState`

Each subscription has one `LocalQueue`:

```
LocalQueueState
 ├─ lanes: HashMap<QueueKey, QueueLane { events: VecDeque<LocalEvent>, version, delayed_version }>
 │                                                   one lane per ordering_key (the absent key is a lane too)
 ├─ ready_lanes: VecDeque<(QueueKey, version)>       round-robin over lanes that can deliver
 ├─ delayed_lanes: BinaryHeap<Reverse<DelayedQueueHead>> min-heap of due times; versions invalidate stale entries
 ├─ delayed_live_count / delayed_stale_count         counts that trigger heap compaction
 ├─ in_flight: HashMap<Box<str>, LocalInFlight>      key = "{event_id}:{seq}", delivered and not yet settled
 ├─ pending_count / next_delivery_token
 └─ closed
```

`LocalEvent.payload` is `SharedPayload::Native(Arc<dyn Any>)`. The same `Arc` is
broadcast into every subscription queue. Publish does not clone the payload.

- **A lane preserves receive order.** Same-key messages enter one queue and leave
  from its head, respecting delay. After a pop, `pop_ready` schedules the next head
  whenever the queue is nonempty; it does not wait for the previous settlement.
  Successors can become facade-owned early. The facade PerKey lane enforces the
  handler-start and settlement boundary.
- **`Retry` returns to the head** (`enqueue_front`) and keeps the original order.
  `Accept` and `Reject` release outstanding budget. `Retry` does not.
- **The delay heap is compacted lazily.** When a lane is consumed or reordered,
  old heap entries become stale. When stale entries exceed `max(live, 8)`, the heap is rebuilt so it cannot grow without bound.
- Synchronous bus state keeps a **`change_version`** (incremented on every route or
  queue change) so `wait_for_topic_idle` can wait on a condition variable for "any
  change" and then recheck `pending == 0 && in_flight == 0`. The async provider
  uses `AsyncSignal` for the same effect.

### 10.2 Capacity and budget

| Parameter | Default | Role |
| --- | --- | --- |
| `queue_capacity` | 1024 | Maximum `pending + in_flight` messages for one subscription |
| `max_total_outstanding` | 65,536 | Provider-wide `OutstandingBudget`: queued plus in-flight across every subscription |

Publish decides per destination. A full queue or an exhausted budget rejects that
destination (counted as rejected in `AdmissionSummary`, with a reason). A topic with
no subscriptions is `NoDestinations`. Publish **never blocks**. Backpressure is
visible on the receipt (P7).

### 10.3 `LocalEventBusSpi` (synchronous)

- Bus state is bucketed by topic. Each bucket stores `payload_type_id: Option<TypeId>`
  and `queues: BTreeMap<subscription::Id, Weak<LocalQueue>>`. Queues are held by
  `Weak`, so a dropped subscription disappears from routing, and publish cleans up dead entries.
- **Type binding.** The first publish or subscribe on a topic records its `TypeId`.
  A later use with a different type returns `SpiError` with kind `InvalidArgument`
  and reason `"topic_type_conflict"`. This restores the `TypeId` half of `Topic<T>`
  equality after type erasure.
- `receive(timeout)` waits on a condition variable until a message can be delivered,
  a delayed message is due, the wait times out, or the queue closes.
- `settle` parses `"{event_id}:{seq}"` from the token, finds the in-flight entry,
  and applies the disposition. An unknown token is `InvalidSettlementToken`.
  Repeating `Accept` for the same token returns `Ok` (idempotent).
- `close` marks the queue closed and clears pending and in-flight. That is Ephemeral
  semantics: unsettled messages are discarded. The queue is removed from routing.
- `wait_for_topic_idle` walks every live queue of the topic and waits until `pending == 0 && in_flight == 0`.
- `shutdown` with `Graceful` waits until every queue is empty or the timeout fires,
  then closes every queue. The call is idempotent. Later publish and subscribe return `Closed`.
- A duplicate `subscription_id` returns `duplicate_subscription`. The facade does not produce this; the check is defensive.

### 10.4 `AsyncLocalEventBusSpi` (asynchronous)

The structure matches the synchronous provider. The waiting primitive and the index differ:

- Waiting uses a `Waker` instead of a condition variable. A `receive` future registers
  a waker when the queue is empty, and publish or settle wakes it. A delayed message
  becoming due is a sleep future on the injected `Timer`.
- Routing uses a primary `HashMap<MailboxKey { subscription_id }, Arc<AsyncMailbox>>`
  and a secondary `HashMap<TopicAddress, BTreeSet<subscription_id>>`. Publish snapshots
  only the target topic's mailboxes, ordered by subscription ID. Both indexes are
  updated under the bus-state lock; close removes only the matching mailbox instance,
  so reusing an ID cannot let an old handle remove a newer mailbox.
- Different subscription instances may share a `(topic, subscriber_id)` and each
  receives its own broadcast copy, matching synchronous local behavior.
- Capacity and budget share `LocalQueue` and `OutstandingBudget`. `AsyncLocalShared`
  also holds `AsyncSignal changed` and `Arc<dyn Timer>`.
- `close_mailbox` is Ephemeral as well.
- `AsyncLocalEventBusSpi::with_timer(config, timer)` lets tests inject a manual timer;
  it validates both queue and provider-wide capacities and returns a configuration
  error for invalid zero limits.
  `AsyncLocalEventBusProvider`, when created through the registry, uses `qubit_clock::StdTimer`.
  That provider is **not** submitted to the async inventory catalog (§6.4).

### 10.5 How the facade and `local` divide one message

Take a delayed `PerKey` message. The facade checks capabilities and puts
`ordering_key` and `delay` on `OutboundMessage`; local queues it in the matching
lane and schedules its delay. Once due, receive returns it in queue order.
Local can then return a same-key successor before the predecessor settles.
The facade can prefetch those messages within its owned budget, but its PerKey
lane prevents starting the successor until the predecessor settles successfully;
termination does not start successors. This facade constraint is necessary on
local as well: receive FIFO is not serialized handler execution or settlement.

### 10.6 Resources and benchmarks

`benches/local_scale.rs` (repeatable hot-path measurements that do not depend on a
benchmark harness) and `benches/local_threads.rs` (thread and resource cost of
creating and tearing down synchronous and asynchronous subscriptions) give an order
of magnitude. `benches/encoded_publish.rs` checks synchronous encoded-payload
allocation reuse across publish retries, while `benches/facade_delivery.rs` measures
caller-driven asynchronous publication through the local SPI. Numbers move with the
machine. Run them directly:

```bash
cargo bench --bench local_scale
cargo bench --bench local_threads
cargo bench --bench encoded_publish
cargo bench --bench facade_delivery
```

The synchronous facade starts one blocking receiver thread per subscription and
enforces `max_subscriptions` (default 256) before calling the provider.
The handler pool is sized by `max_running_handlers`; shutdown may also briefly start a
coordinator thread. The async facade creates no thread per subscription. The
`local` provider itself creates no threads.

---

## 11. `NotificationPublisher`

The `notification` module covers one case: **hot-path code wants to publish a `T`
to a fixed topic and must not block on `EventBus::publish` retry, interceptors, or
the provider.** It is a bounded, never-blocking queue in front of a synchronous
`EventBus`. It is not a second bus.

```rust
pub struct NotificationPublisher<T: Send + Sync + 'static> { ... }
impl<T: Send + Sync + 'static> NotificationPublisher<T> {
    pub fn new<F>(bus: EventBus, topic: Topic<T>, capacity: NonZeroUsize, observer: F) -> io::Result<Self>
    where F: Fn(NotificationOutcome) + Send + Sync + 'static;
    pub const fn default_capacity() -> NonZeroUsize;                     // 256
    pub fn try_publish(&self, payload: T) -> Result<(), TryPublishError<T>>; // Full(T) | Closed(T); the payload is returned
    pub fn stats(&self) -> NotificationStatsSnapshot;
    pub fn close(&self) -> io::Result<()>;
    pub fn close_with_timeout(&self, timeout: Duration) -> io::Result<()>;
}

pub enum NotificationOutcome {
    Published(PublishReceipt),
    PublishFailed(PublishFailure),
    RequestFailed(EventIdGenerationError),
}
```

- **A bounded `sync_channel(capacity)` and one worker thread named `event-notification-publisher`.**
  The worker builds `PublishRequest::new(topic.clone(), payload)` and calls `bus.publish`,
  so facade semantics (interceptors, retry, receipts) still apply. They run on the background thread.
- **`try_publish` never blocks.** A full queue returns `TryPublishError::Full(payload)`.
  A closed publisher returns `Closed(payload)`. In both cases the payload is given
  back so the caller can decide what to do with it (P7).
- **Results are pushed to an observer.** Each `NotificationOutcome` is handed to the
  observer on the worker thread, so the observer should return quickly. An observer
  panic is isolated, counted in `observer_panicked`, and does not stop later publishes (P8).
- **Stats.** `NotificationStatsSnapshot` reports `enqueued`, `queue_full`, `queue_closed`,
  `published`, `publish_errors`, `request_errors`, `observer_panicked`, and `worker_panicked`.
- **`close()`** closes the sender, waits for the worker to drain, and joins it.
  Calling it from the worker thread (that is, from the observer) returns an error
  instead of joining itself, the same idea as §8.6. If the worker has panicked, `close()` returns an error.
- **`close_with_timeout(timeout)`** closes enqueue admission and waits at most the
  caller's deadline for the same drain and resource cleanup. A deadline returns
  `io::ErrorKind::TimedOut`; the worker continues and later close calls can wait
  again. The timeout cannot interrupt provider calls or running user code.
- **`Drop`** closes the sender and does not wait. The worker exits after it drains what is left.
- The publisher does not own the `EventBus` lifetime. After the bus shuts down the
  worker receives `PublishError::Closed` and reports it through the observer. The caller still calls `close()`.

---

### 11.1 Worker exit and concurrent close

A completion guard publishes exactly one `Drained` or `Panicked` exit after the
processing loop and user-owned resource cleanup finish. The outer unwind boundary
includes observer captures and other worker resources; cleanup panic increments
`worker_panicked` once. Observer-call panics retain their narrower isolation.
Thread identity is retained separately from the optional JoinHandle. A worker
calling close on itself receives `io::ErrorKind::Other` without closing admission.
External callers take the sender under its lock and drop it outside the lock;
all waiters observe the same exit. A timed-out caller can wait again; only one
caller joins, and timed close also respects the thread's actual finished state.
This handles `panic=unwind`, not abort or indefinitely blocking user destructors.

## 12. Error model

**Each public operation has its own error enum. `EventBusError` only aggregates them.**
Errors carry enough context (provider, operation, resource, retryability) and do not carry the business payload.

| Type | Produced by | Principal variants |
| --- | --- | --- |
| `PublishFailure` | `publish` / `publish_all` | Original event ID and aggregate effect wrapping `PublishError`: `Configuration`, `Capability`, `Codec`, `Spi`, `Retry(Box<RetryError<PublishAttemptError>>)`, `InterceptorPanicked { .. }`, `ErrorHandlerPanicked { .. }`, `Closed` |
| `EventBusError` | Aggregate operation error | Transparent `PublishFailure(PublishFailure)` conversion preserves publication identity/effect; `Publish(PublishError)` remains a cause-only conversion |
| `AdmissionCheckError` | `PublishReceipt::check_admission` | `VisibilityUnavailable`, `Dropped`, `NoAcceptedDestination`, `RejectedDestinations { .. }` |
| `SubscribeError` | `subscribe` | `Configuration`, `Capability`, `Spi`, `Closed` (a missing codec is `Capability`) |
| `DeliveryError` | handler return / pipeline | `Handler { source }`, `Codec`, `Spi`, `Retry(Box<RetryError<DeliveryAttemptError>>)` |
| `LifecycleError` | `wait_for_*`, `cancel`, shutdown internals | `Timer(TimeError)`, `WouldDeadlock { operation }`, `Closed`, `IdleWaitUnsupported`, `Spi`, `SubscriptionClose(Arc<SubscriptionCloseErrors>)` |
| `ShutdownError` | `request_shutdown`, ticket waits, `shutdown` | `TimedOut { .. }`, `CoordinatorStart(io::Error)`, `Lifecycle(LifecycleError)` (including `WouldDeadlock`), `Spi`, `SubscriptionClose` |
| `ProviderError` | registry | `Resolution` (provider not found, or an illegal selection), `Creation` (provider construction failed, or `RequiredCapabilities` are missing) |
| `EventBusProviderError` | provider authors | The error wrapper a provider `create` returns, aggregated by `qubit-spi` |
| `SpiError` | provider | `Publish { provider_id, resource, kind, retryable, effect, source }`, `Operation { provider_id, operation, resource, kind, retryable, source }`, `InvalidSettlementToken { .. }` |
| `CapabilityError` / `CodecError` / `ConfigurationError` / `EventIdGenerationError` | construction or validation | see each type |

`EventBusError` implements `From<PublishFailure>` through its transparent
`PublishFailure` variant, so `bus.publish(request)?` in an application function
returning `Result<_, EventBusError>` retains the original event ID, aggregate
effect, and structured cause. `Publish(PublishError)` remains available for a
cause without an assigned publication identity. Do not reduce a public publish
failure to `into_cause()` merely to convert it to the aggregate error, because
that discards the wrapper's identity/effect. Transparent error propagation
preserves the underlying source chain.

- `SpiError::retryable()` is the only channel a provider uses to tell the facade
  "this is worth retrying". Publish and delivery retry both consult it.
- `SpiError::Operation::kind` is a `&'static str` classification (for example
  `closed`, `invalid_argument`, `provider_panicked`, `worker_panicked`,
  `topic_type_conflict`) used in logs and test assertions. Each facade layer's `Closed`
  variant comes from that layer's own lifecycle gate. A closed-style `SpiError`
  returned by a provider after shutdown is passed through as the `Spi` variant and is not remapped.
- Enums are `#[non_exhaustive]` so a new variant can be added later.
- Every error is `Send + Sync + 'static` and can cross threads and tasks.
- Internal `PipelineFailure { origin, error, publish_effect }` is not public. It carries the failing stage to diagnostics and tests.

---

## 13. Diagnostics and metrics

`observe_diagnostics(observer) -> DiagnosticObserverHandle` registers an
`Fn(&Diagnostic) + Send + Sync` observer. Dropping the handle unregisters it.
An observer panic is isolated and does not affect other observers or the main path.

| `Diagnostic` variant | When it is emitted |
| --- | --- |
| `AdmissionRejected` | A destination in a publish receipt was rejected (only `DestinationAdmissions` providers) |
| `ReceiveGap` | Provider `receive` returned `Gap` |
| `DeliveryFailed` | A delivery reached a terminal failure (includes attempts, the `DeliveryFailureAction`, and the error) |
| `SettlementFailed` | An attempt failed; structured error and attempt are preserved, retry depends on policy |
| `SettlementStopped` | The first terminal settlement cause stops this subscription |
| `SettlementUnavailable` | Settlement was required but the capability does not allow it, or it was abandoned (for example an `Immediate` shutdown) |
| `InternalFailure` | An internal facade failure that should not happen (for example the coordinator's `receive` returned `Err`) |

`PublishMetricsSnapshot` (`publish_metrics()`) is the publish-side counters from
step 10 of §7.3. It is for tests and light monitoring. It is not a full metrics system.
On the async side, `attempts` increments when the future is first polled. A publish
future that is constructed and dropped without being polled is not counted.

Diagnostics are a **push** model, not a log. This crate does not depend on `log` or
`tracing`. Wiring them into a monitoring system is the caller's choice.

---

`delivery_metrics()` returns `DeliveryMetricsSnapshot`; subscriptions return `SubscriptionDeliveryMetricsSnapshot` with subscription/subscriber IDs. Reservations, queued/running/settling/lane-waiting work, attempts/retries/termination, completion/abandonment, durations and oldest-owned age are observable. Snapshots retain no payload/token or per-event/per-key history and are not transactionally consistent across threads. Closed handles retain final subscription counts. Forward synchronous observer records through an application-owned bounded nonblocking queue; panic isolation is not latency isolation. See the [user guide](user_guide.md#diagnose-settlement-termination-and-restore-consumption) for full fields and recovery.

## 14. Concurrency invariants

1. At any moment only one owner calls methods on a given `EventSubscriptionSpi` or `AsyncEventSubscriptionSpi` (synchronous: the coordinator thread; asynchronous: the `run` or `shutdown` that holds the lease).
2. A token/disposition pair may be retried idempotently; the provider applies its terminal effect once even if more than one call returns success.
3. The first `Acknowledgement` decision wins and is then immutable.
4. A same-key lane remains owned until settlement succeeds or the subscription terminates; successors never bypass FIFO.
5. Running≤max_running_handlers, owned≤max_owned_deliveries, and per-subscription owned≤max_owned_per_subscription.
6. Reserve owned before receive; registered subscriptions≤max_subscriptions, with no extra pending item outside capacity.
7. After `Closing`, `publish` and `subscribe` return `Closed`. Shutdown is idempotent and at most one provider shutdown call is in flight; a failed or cancelled call may be retried.
8. Callback panics are contained. Codec panic stops its subscription; notification
   cleanup panic publishes a failed worker exit. User code that aborts the process
   or blocks indefinitely cannot be recovered or forcibly interrupted.
9. Dead-letter is at most one level deep. The dead-letter header cannot be set or altered from outside the pipeline.
10. Nonterminal durable work follows the provider's close/recovery protocol. Ephemeral work may be discarded and counted; the facade also flags provider abandonment it cannot count. Graceful drains already-owned work, while terminal/cancel/Immediate prevents new handler starts. Sync nonterminal cancellation may send Retry when supported; async non-Graceful close releases unstarted work and closes the receiver without universally sending Retry. Terminal stop starts no new settlement and never fabricates Accept for a handler that did not run.
11. User code is not called while a facade-internal lock is held. Diagnostic observers
    are snapshotted under the `observers` lock, and `emit` calls them after releasing it.
    Handlers, middleware, and error handlers run on scheduler threads or async delivery
    tasks and do not hold the subscription catalog, ordering lane, or tracker lock.
    A callback that re-enters the bus API therefore cannot deadlock on the same lock.
    This pairs with the `WouldDeadlock` checks in §8.6 and §9.8.

`tests/concurrency_contract_tests.rs` model-checks primitives related to items 4, 5, 7, and 10 under `loom`.

---

## 15. Testing and verification

### 15.1 Test layers

| Location | What it covers |
| --- | --- |
| Unit tests in `src/**/*.rs` | Model validation, builder rejection rules, capability enums, queue and heap behavior, admission and lane primitives |
| `tests/sync_facade_tests.rs`, `tests/async_facade_tests.rs` | End to end: publish and subscribe, ACK matrix, retry, dead-letter, ordering, backpressure, shutdown, pause and resume, deadlock detection |
| `tests/*_contract_tests.rs` (model / registry / spi / spi_error / public_error_trait) | Public contracts and invariants |
| `tests/*_coverage_tests.rs` (sync / async / local / pipeline / publisher / registry / model / error / spi) | Extra branch coverage |
| `tests/local_provider_tests.rs`, `tests/async_local_provider_tests.rs` | `local` behavior exercised at the SPI layer |
| `tests/publish_admission_tests.rs`, `tests/request_builder_tests.rs`, `tests/notification_publisher_tests.rs` | Admission receipts, builder validation, the notification publisher |
| `tests/support/fake_spi.rs` | A programmable fake provider: injected errors, panics, capability matrices, settlement failures |
| `tests/support/flume_spi.rs` | A second bounded channel transport implemented with the standard library, so the SPI is not tailored to `local` |
| `tests/support/manual_async.rs` | A manual executor and manual timer so async tests advance deterministically |
| `tests/support/provider_shapes.rs` | Provider shapes for combinations of capabilities |
| `tests/support/{scheduler_race,spawn_failure,panic_hook}.rs` | Scheduler races, thread-spawn failure, panic-hook isolation |
| `tests/discovery_{sync,async,conflict}_tests.rs` plus `tests/fixtures/discovery_{provider,consumer}` | Cross-crate linking and conflict detection for the `discovery` feature |
| `tests/spi_conformance_tests.rs` | Runs the `conformance` module against `local` and flume |
| `tests/concurrency_contract_tests.rs` | `loom` models: an admission permit is released exactly once, cancelling a lane wakes the next waiter, cancel races receive, graceful shutdown and publish share one admission linearization point |
| `fuzz/fuzz_targets/{provider_options,transport_envelope}.rs` | Fuzz of parse and construction edges |
| `benches/` | See §10.6 |

### 15.2 CI

`.github/workflows/ci.yml` uses the repository's shared `rs-infra` orchestration (`ci-check.sh`):
dependency baseline, rustfmt and clippy (including the coverage cfg), the feature and
dependency matrix, `cargo +1.94.0 test --doc`, the README dependency-version check, and
a strict documentation build with
`RUSTDOCFLAGS="-D warnings -D missing-docs" cargo doc --all-features`.

### 15.3 The `conformance` feature

`qubit_event_bus::spi::conformance::{run_sync, run_async}` takes a provider factory
(`Fn() -> Arc<dyn EventBusSpi>`, or an async factory that returns a future; each case
builds a fresh instance so cases do not contaminate each other) and `ConformanceHooks`.
The hooks cover optional settlement, receive/settlement/close/shutdown cancellation,
and durable recovery. Strict runs turn missing required hooks into failures while
typed skips remain available for unsupported capabilities. Async hooks are awaited by
the runner and do not block its executor. It returns a `ConformanceReport`
(`Vec<ConformanceCase::{Passed, Failed, Skipped}>`). Current cases cover capability and
payload-mode consistency, `subscribe`, `publish`, `receive-payload`, settlement
idempotence and conflicts, and `shutdown`. A provider that does not support a case
should record it as `Skipped`, not return a vague error after the test has already
called the operation. Structural runs are smoke checks; strict runs are the fixture-backed provider acceptance gate.

Checks the public runner does not cover, which a provider author supplies:

- descriptor, provider selection, and creation;
- capability declarations that stay stable and match real behavior;
- native and encoded payload contracts;
- `receive` after `publish`;
- `receive` timeout;
- `receive` returns `Closed` after `close`;
- a gap mapped to `ReceiveOutcome::Gap`;
- settlement-token subscription ownership;
- repeated settlement is idempotent, and a conflicting disposition does not cause an extra delivery;
- `shutdown` is idempotent;
- dropping an async `receive` does not lose the message;
- errors carry provider context and a traceable `source`;
- `Debug` output does not leak sensitive provider options.

Strict recovery checks follow the provider's declared subscription modes. A
durable mode requires a durable-recovery fixture; an ephemeral mode requires an
ephemeral-cleanup fixture. Unsupported modes are reported with a typed skip,
and a missing required fixture fails the report. Synchronous cancellation cases
are `NotApplicable` because synchronous methods return no cancellable future.
Cancelling an asynchronous receive future is distinct from destroying its
receiver: a message already consumed by the provider must remain available to a
later receive or recovery attempt. Closing or dropping a durable receiver must
preserve accepted, unsettled deliveries; ephemeral providers may discard them.
Neither close nor drop implicitly acknowledges an unsettled delivery.

### 15.4 Documentation checks

Public traits and the main types have runnable rustdoc examples. The README and the
user guide list the `qubit-retry` dependency and use `qubit_retry::*` paths when they
show advanced retry configuration. Basic examples do not import retry types they do
not use. The documents keep provider acceptance, facade admission, and handler
completion as three separate facts (P6).

---

## 16. Public API stability

These types are the stable boundary and should change deliberately:

- `EventBus` and `AsyncEventBus`;
- `Topic<T>`, `EventEnvelope<T>`, both request types and their builders, and `Delivery<T>`;
- `EventBusSpi`, `AsyncEventBusSpi`, and both subscription SPIs;
- transport messages and the settlement contract;
- capability types;
- the provider spec and the registry;
- the per-operation error types and their accessors.

Public error enums, capability enums, and `Diagnostic` are `#[non_exhaustive]`.
SPI input structs use private fields, constructors, and accessors, so adding a field
is not a breaking change. Backend-specific extensions go through namespaced
`ProviderOptions` or a separate extension trait. They do not keep adding methods to the minimal SPI.

---

## 17. Non-goals and known limits

- **No exactly-once.** The facade can only work within the settlement capability the provider declared. Under `AcceptOnly`, a failed message may be redelivered or lost, depending on the provider.
- **Ordering scope.** Handlers for one subscription and one key are serialized. Across subscriptions and topics there is no order.
- **Synchronous retry occupies a handler thread.** `Retry` backoff sleeps on a pool thread, so a long backoff reduces effective concurrency. A long backoff should use `Requeue` and let the provider redeliver, or use the async facade.
- **Async `local` does not participate in discovery** (§6.4).
- **`wait_for_idle` depends on the provider.** When the provider does not support it, the call returns `IdleWaitUnsupported`. Use `wait_for_received_deliveries` or an application-level signal instead.
- **`provider_attempt` is always `None` today.** `InboundMessage` has no source for a provider redelivery count.

---

*This document is maintained with `qubit-event-bus` 0.18.x. A change to facade or SPI behavior should update the matching section here and in the [Chinese document](design.zh_CN.md).*

## Provider specification compile probe

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/provider_spec.rs -->
```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider service aliases and subscription calls compiled by documentation checks.

use std::time::Duration;

use qubit_event_bus::EventBusSpec;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::SpiFuture;
use qubit_spi::AsyncServiceSpec;
use qubit_spi::ServiceSpec;
use qubit_spi::SyncServiceSpec;

/// Configuration type selected by the event-bus provider service specification.
pub type ProviderConfig = <EventBusSpec as ServiceSpec>::Config;
/// Output returned by the synchronous event-bus provider.
pub type SyncOutput = <EventBusSpec as SyncServiceSpec>::Output;
/// Output returned by the asynchronous event-bus provider.
pub type AsyncOutput = <EventBusSpec as AsyncServiceSpec>::Output;

/// Performs one nonblocking receive attempt on a synchronous subscription.
pub fn receive_once(receiver: &mut dyn EventSubscriptionSpi) -> Result<ReceiveOutcome, SpiError> {
    receiver.receive(Duration::ZERO)
}

/// Starts accepting a token without tying the returned future to the token borrow.
///
/// The future borrows the receiver for `'a`; the caller may release the token
/// borrow independently after this method returns.
pub fn settle_without_borrowing_token<'a>(
    receiver: &'a mut dyn AsyncEventSubscriptionSpi,
    token: &SettlementToken,
) -> SpiFuture<'a, Result<(), SpiError>> {
    receiver.settle(token, DeliveryDisposition::Accept)
}
```

## Validate a single crate or the coordinated ecosystem

`./scripts/project-ci-check.sh` checks this crate's resolved dependency metadata on its own.
An independent single-crate checkout does not need every downstream repository.
For a coordinated migration, run `./scripts/project-ci-check.sh --ecosystem-root <repos-dir>`
with `rs-event-bus`, `rs-event-bus-redis`, `rs-task`, `rs-ioc`, and
`rs-execution-services` below that directory. The gate requires all five roots
and the seven declared consumer fixtures, resolves locked all-feature Cargo
metadata, and rejects a graph mixing old event-bus minors with 0.18. Missing
inputs fail explicitly; this metadata check supplements each project's CI and
does not prove delivery behavior by itself.
