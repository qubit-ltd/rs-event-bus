# Qubit Event Bus Design (0.14)

> This document describes `qubit-event-bus` 0.14.0 as implemented.
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
  settle one message". They do not reimplement retry, dead-letter, middleware,
  per-key ordering, backpressure, or shutdown.
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
are implemented once in the facade. A provider delivers, receives, and settles.
Every backend then shares the same processing behavior, and each backend stays small.

**(P3) Capabilities are declared honestly. There is no lowest-common-denominator API and no silent downgrade.**
A provider reports what it can do through `EventBusCapabilities`. The facade checks
a publish or subscribe request against those capabilities (for example, a delay when
delayed delivery is unsupported) and returns `CapabilityError` instead of pretending
to support the request. The facade also does not shrink every backend's API down to
whatever the weakest backend can do.

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
`PublishReceipt` means the provider accepted the message. What that acceptance
promises is `PublishGuarantee`. It does not mean any subscriber finished handling it.
`Acknowledgement` is the handler's business decision (`AckMode::Auto` or `Manual`).
`SettlementToken` plus `DeliveryDisposition` is the transport-level confirmation
between the facade and the provider. Keeping them apart avoids reading
"publish returned" as "the handler finished".

**(P7) Every queue is bounded.**
The synchronous handler pool, handler queues, async admission, local queue capacity,
the provider-wide outstanding budget, and the notification publisher queue all have
explicit limits. Overflow behavior (block, reject, or fall back to `Retry`) is defined.
Work does not accumulate without a bound.

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
      │ · OperationGate    │             │ · AsyncAdmission       │
      │ · one coordinator  │             │ · AsyncOrderingLanes   │
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
| `pipeline` | Processing shared by both facades | `PublisherPipeline`, `SubscriberPipeline`, `DeliveryFailureAction`, `AdmissionTracker`, `OrderingLanes`, `DeadLetter*`, retry adapters, `Diagnostic` |
| `facade` | User-facing bus | `EventBus`, `Subscription`, `AsyncEventBus`, `AsyncSubscription`, `EventBusFacadeConfig`, `SyncDeliverySchedulerConfig`, `DeliveryAdmissionConfig`, `PublishMetricsSnapshot`, `WaitOutcome`, internal `SyncDeliveryScheduler` / `ShutdownCoordinator` / `LifecycleTracker` |
| `local` | Built-in in-process provider | `LocalEventBusConfig`, `LocalEventBusProvider`, `AsyncLocalEventBusProvider`, `LocalEventBusSpi`, `AsyncLocalEventBusSpi`, `LocalQueue`, `OutstandingBudget` |
| `notification` | A bounded, never-blocking publish queue in front of `EventBus` | `NotificationPublisher<T>`, `NotificationOutcome`, `TryPublishError<T>`, `NotificationStatsSnapshot` |
| `error` | Layered error types | `EventBusError`, `PublishError`, `SubscribeError`, `DeliveryError`, `LifecycleError`, `ShutdownError`, `ProviderError`, `SpiError`, `CapabilityError`, `CodecError`, `ConfigurationError` |

Dependencies point `facade → pipeline → {model, codec, spi, error}`,
`registry → spi`, and `local → spi`. `pipeline` and `spi` do not know about the
facade. `local` does not know about any layer above the registry.

### 2.3 Crate metadata, features, and dependencies

- Package `qubit-event-bus`, version `0.14.0`, edition 2024, `rust-version = 1.94`.
- Features:
  - `discovery = ["qubit-spi/inventory"]` enables inventory-driven provider
    registration (see §6.4).
  - `conformance` exposes `qubit_event_bus::spi::conformance` so provider authors
    can run the contract cases in their own tests (see §15.3).
- Qubit crates actually used at runtime: `qubit-spi` (catalog and discovery),
  `qubit-retry` (`worker` and `async` features), `qubit-clock` (`Timer` / `TimeError`),
  and `qubit-id` (UUID `EventId`s). Error types use `thiserror`.
- Dev-dependencies: `flume` (the second real transport in `tests/support/flume_spi.rs`)
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
(`SubscribeError::Configuration`). The asynchronous facade accepts both kinds of middleware.

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

`Acknowledgement` is atomic and **the first decision wins**. The first `ack()` or
`nack()` is stored. Later calls return `false` and leave the state unchanged.
`Delivery` and the facade share it. After the handler returns, the facade reads it
to choose settlement (see the ACK matrix in §7.4). Moving the `Delivery` to another
thread before acknowledging it still produces exactly one terminal state.

### 3.5 `PublishReceipt` and admission visibility

```rust
pub struct PublishReceipt {
    input_event_id: EventId,             // envelope id supplied by the caller
    dispatched_event_id: Option<EventId>, // id actually sent after interceptors; None when dropped
    provider_id: ProviderId,
    acknowledgement: PublishAcknowledgement,
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

This is P6. `publish` returning `Ok(receipt)` means the provider accepted the message.
Whether anyone was actually queued is `admission_outcome()`, and only a provider with
`PublishVisibility::DestinationAdmissions` can supply that detail.
`publish_all` returns `BatchPublishResult`, preserving each request's
`Result<PublishReceipt, PublishError>` in input order. One failure does not stop the later requests.

### 3.6 `DeadLetterEvent<T>`

The payload type of a dead-letter topic is `DeadLetterEvent<T>`: `original_event: Arc<EventEnvelope<T>>`,
`subscriber_id`, and `reason` (the `Display` of the final `DeliveryError`).
`DeadLetterPolicy::Topic(name)` names the topic. The facade builds
`Topic::<DeadLetterEvent<T>>::new(name)` and reuses the original topic's codec when one exists.

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
    #[doc(hidden)] fn provider_id(&self) -> Option<&ProviderId> { None }
}

pub trait EventSubscriptionSpi: Send + 'static {
    fn id(&self) -> subscription::Id;
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError>;
    fn settle(&mut self, token: SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError>;
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
    fn wait_for_topic_idle<'a>(&'a self, topic: &'a TopicAddress, timeout: Option<Duration>) -> SpiFuture<'a, Result<Option<bool>, SpiError>>;
    #[doc(hidden)] fn provider_id(&self) -> Option<&ProviderId> { None }
}

pub trait AsyncEventSubscriptionSpi: Send + 'static {
    fn id(&self) -> subscription::Id;
    fn receive(&mut self, timeout: Duration) -> SpiFuture<'_, Result<ReceiveOutcome, SpiError>>;
    fn settle(&mut self, token: SettlementToken, disposition: DeliveryDisposition) -> SpiFuture<'_, Result<(), SpiError>>;
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

- **Not `Clone`.** The facade settles a message once, so the provider does not have to accept concurrent settlement of the same token.
- `belongs_to(subscription_id)`: the facade checks ownership before settling. A provider may also reject a mismatched token with `SpiError::InvalidSettlementToken`.
- **Idempotent.** Settling the same `(token, disposition)` again must return `Ok(())`. The async facade retries the same disposition after a failure.
- **Conflict.** Settling the same token with a different disposition should error or be ignored. It must not corrupt state beyond a duplicate delivery.
- `DeliveryDisposition::{Accept, Retry, Reject}`: `Retry` asks the provider to deliver again and requires `SettlementCapabilities::AcceptRetryReject`. `Reject` is a terminal drop. Dead-letter publication, when it happens, has already been done by the facade.

### 4.6 Shutdown contract

`shutdown(ShutdownMode)`:

- `ShutdownMode::Graceful { timeout }` waits for accepted messages to finish being delivered. On timeout it returns `ShutdownOutcome::TimedOut`. `timeout` covers the **whole** facade shutdown, including receiver close, handler drain, and the provider shutdown.
- `ShutdownMode::Immediate` closes every subscription immediately and drops or retains unprocessed messages according to the provider.
- The call is idempotent. After shutdown, `publish` and `subscribe` return `SpiError::Operation` whose `kind` is closed.

The facade calls provider `shutdown` **once**, after it has drained handlers.
The synchronous side uses `ShutdownCoordinator`. The asynchronous side uses a leader CAS.

---

## 5. Capability model

```rust
pub struct EventBusCapabilities {
    payload_modes: PayloadModes,                 // Native | Encoded | NativeAndEncoded
    settlement: SettlementCapabilities,          // None | AcceptOnly | AcceptRetryReject
    ordering: OrderingCapability,                // None | PerSubscription | PerKey | PerPartition
    delayed_delivery: DelayedDeliveryCapability, // None | Native
    durability: DurabilityCapability,            // Ephemeral | Durable
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
| `consumer_group` | Subscribe | Rejected when `consumer_groups == false` |
| `StartPosition::Earliest` | Subscribe | Rejected when replay is `ReplayCapability::None` |
| `PayloadModes::Encoded` | Publish / subscribe | `CapabilityError::CodecRequired` when no codec is available (an encode or decode failure is a `CodecError`) |
| `FailureDirective::Requeue` | Delivery failure | Falls back to `Discard` / `Reject` when settlement cannot `Retry` (see §7.5) |
| `Diagnostic::SettlementUnavailable` | Delivery failure | Emitted when settlement is required but cannot be performed, instead of failing silently |

`RequiredCapabilities` (§6.2) reuses the same enums so a caller can demand
"at least these capabilities" at **creation** time and learn about a mismatch before the bus is used.

---

## 6. Registry, discovery, and provider assembly

### 6.1 `EventBusSpec` and `qubit-spi`

```rust
pub struct EventBusSpec;                    // synchronous
impl ServiceSpec for EventBusSpec {
    type Config = EventBusConfig;
    type Output = Arc<dyn EventBusSpi>;
    type Error = EventBusProviderError;
}
pub struct AsyncEventBusSpec;               // asynchronous; Output = Arc<dyn AsyncEventBusSpi>
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
| `sync_scheduler` | `SyncDeliverySchedulerConfig { max_in_flight: 4, handler_queue_capacity: 32 }` | Synchronous handler-pool size and queue depth |
| `delivery_admission` | `DeliveryAdmissionConfig { max_in_flight: 4 }` | Global in-flight limit of the async facade |

Global interceptors and middleware are **added to** request-level ones. On publish,
typed request interceptors run first and global metadata interceptors run after them.
On subscribe, global middleware wraps the request middleware: global, then typed, then the handler.

### 7.2 Codec resolution

`resolve_codec::<T>(topic, registry)`:

1. Use `topic.codec()` when it is present.
2. Otherwise use `registry.get::<T>()`.
3. Otherwise `None`.

Only a `PayloadModes::Encoded` provider **requires** a codec (missing codec →
`CapabilityError::CodecRequired`). `Native` and `NativeAndEncoded` both use
`TransportPayload::Native`. The subscribe side decodes an `Encoded` payload with
the same rule. A decode failure becomes `DeliveryError::Codec`, is `Reject`ed when
the provider supports it, and emits `Diagnostic::DeliveryFailed { attempts: 0 }`.

### 7.3 Publish pipeline (`PublisherPipeline`)

Steps of `publish(request)`. `publish_all` runs them for each request in order and collects every result:

1. **Lifecycle gate.** If the bus is not `Running`, return `PublishError::Closed`.
2. **Request-level typed interceptors.** `Fn(EventEnvelope<T>) -> Result<Option<EventEnvelope<T>>, PublishError>`,
   chained in registration order. An interceptor may replace the whole envelope
   (which is why the receipt distinguishes `input_event_id` from `dispatched_event_id`).
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
   A provider panic is captured as `SpiError::Operation` with `kind = "spi_panic"`.
8. **Error handlers.** On failure, `PublishErrorHandler<T>(&PublishFailureContext<T>, &PublishError)`
   runs in registration order. If any handler panics, the final error becomes
   `PublishError::ErrorHandlerPanicked` and the remaining handlers still run.
9. **Admission diagnostics.** When the receipt carries `DestinationAdmissions`, each
   rejected destination emits `Diagnostic::AdmissionRejected { event_id, topic, subscriber_id, reason }`.
10. **Metrics.** Update `PublishMetricsSnapshot` (`attempts`, `errors`, `dropped`,
    `opaque_accepted`, `zero_destinations`, `accepted_destinations`,
    `filtered_destinations`, `rejected_destinations`), readable through `publish_metrics()`.

Internally, `PipelineFailure { origin, error }` records **which step** failed
(interceptor, capability, codec, SPI, error handler, and so on) for tests and logs.
Callers see only `PublishError`.

### 7.4 Subscribe pipeline (`SubscriberPipeline`): one message

`SubscriberPipeline<T>` is called by the synchronous worker and by the asynchronous run loop:

```
InboundMessage
  │ into_parts()
  ├─ TransportPayload → Arc<T> (Native downcast / Encoded decode) ── failure → Reject + DeliveryFailed(attempts=0)
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
   │ DeadLetter → stop local attempts, publish the dead-letter, then settle Reject; a dead-letter failure follows Requeue
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
    or dropped by them. Success then `Reject`s. If publishing fails and the provider
    supports `Retry`, the disposition is `Retry`; otherwise `Reject`. With no
    dead-letter policy the action degrades to `Requeue`. A message that already
    carries the reserved header is not published again; it is `Reject`ed.
  - `Requeue`: settlement `AcceptRetryReject` becomes `Retry`.
  - `Discard`, or local retry exhausted: `AcceptRetryReject` becomes `Reject`.
  - When settlement is `None` or `AcceptOnly`, **no failure is settled**
    (`SubscriberPipeline::failure_disposition` returns `None`). If the message has
    a token, `Diagnostic::SettlementUnavailable { requested }` records what the
    facade wanted to do. Under `AcceptOnly`, whether an unsettled message is
    redelivered is the provider's decision.
  - Internal failures while building or publishing a dead-letter (missing policy,
    envelope construction failure, publish failure) are recorded as
    `Diagnostic::InternalFailure` and then handled as `Requeue`.
- Every terminal failure emits `Diagnostic::DeliveryFailed { event_id, topic, subscription_id, subscriber_id, attempts, error }`.

### 7.6 Dead-letter recursion

A dead-letter message carries the header `x-qubit-event-bus-dead-letter: v1`.
When `SubscriberPipeline` reaches a terminal failure for a message that already
has the header, it does not publish a second-level dead-letter; it `Reject`s.
Together with dead-letter header protection on the publish path, one message
produces at most one dead-letter record.

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
| `event-bus-handler-{i}` | `max_in_flight` (default 4), started lazily on the first `subscribe` | Runs middleware, the handler, local retry, error handlers, and dead-letter publish |
| `event-bus-shutdown` | at most one, spawned at shutdown | Drains, joins, and calls provider shutdown in the background |

`EventSubscriptionSpi` is a single owner (`&mut self`), so each subscription needs
**one** thread that calls `receive` and `settle`. Handlers can be slow. Running them
serially on that same thread would give neither concurrency nor a global limit.
The coordinator therefore does I/O and settlement, and handlers run in a shared
bounded pool. `max_in_flight` is the global in-flight limit.

### 8.2 `SyncDeliveryScheduler`

- Each subscription has a `VecDeque` of tasks (`handler_queue_capacity`, default 32).
  Workers walk the subscription queues round-robin so one busy subscription cannot starve the others.
- `try_reserve()` is admission: the coordinator may enqueue only while `queued < capacity`.
  Otherwise it keeps the message in `pending` and **stops calling `receive`**, which
  backpressures the provider. The local provider's queue then fills, and that shows
  up as an `AdmissionOutcome` on publish. `handler_queue_capacity = 0` degenerates
  to a direct handoff: a task is accepted only when idle workers outnumber reserved
  tasks and the key is not active.
- **Per-key order.** A `OrderingPolicy::PerKey` task carries `ordering_key`. The
  scheduler keeps `active_keys: HashSet<(subscription_id, key)>`. `take_ready()`
  skips a task whose key is already active and takes the first runnable task in that
  subscription's queue. The queue is FIFO and each take is "the earliest runnable
  task", so tasks for one key run in arrival order while different keys run in parallel.
- `AdmissionTracker` records issued permits. `wait_for_received_deliveries` uses it
  to see whether the facade still has unfinished deliveries.
- `cancel_subscription(id)` takes every queued task for that subscription and runs
  them with `cancelled = true`, which produces a `Retry` settlement back to the provider, so cancel does not drop a message.
- `stop_admission(immediate)`: `Graceful` leaves queued tasks to drain. `Immediate`
  clears the queues and runs them as `cancelled`, so they `Retry`.

### 8.3 Coordinator thread (`run_subscription_worker`)

```
loop {
  if cancel_requested { return pending as Retry; break }
  apply settlements sent back by handler threads (OwnerSettlementRouter)
  if let Some(job) = pending.take() {
     if let Some(r) = scheduler.try_reserve(...) { r.submit(job) } else { pending = Some(job); sleep 1ms; continue }
  }
  match receiver.receive(50ms) {
     Message(m)  → process_inbound_parts → build a job → try reserve/submit, else keep it pending
     Gap(g)      → Diagnostic::ReceiveGap
     TimedOut    → continue
     Closed      → break
     Err(e)      → Diagnostic::InternalFailure, brief sleep, continue
  }
}
drain remaining settlements → receiver.close() → mark stopped
```

**Settlement flows back to the owner.** A handler thread cannot call `receiver.settle`
because it does not hold `&mut` on the receiver. It sends `(token, disposition)`
through an mpsc channel. The coordinator settles them at the start of each loop.
A failed settlement emits `Diagnostic::SettlementFailed` and is tried **once more**
on the next loop, relying on the idempotence contract in §4.5.

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
shutdown(mode: ShutdownMode)
  ├─ lifecycle: Running → Closing (already Closing/Closed joins the shutdown in progress)
  ├─ OperationGate::close_admission()          new publish/subscribe returns Closed immediately
  ├─ coordinator.begin(mode) → generation       Immediate can strengthen an in-progress Graceful
  ├─ scheduler.stop_admission(immediate); every subscription request_cancel()
  ├─ spawn `event-bus-shutdown` to run perform_shutdown:
  │     wait for OperationGate to reach zero → wait for or clear scheduler tasks → join every coordinator
  │     → scheduler.join() → spi.shutdown(mode) (once) → lifecycle Closed
  └─ caller outside bus context → wait until the Graceful.timeout deadline and return ShutdownOutcome
     caller inside bus context → return ShutdownError::Lifecycle(WouldDeadlock) immediately
                                 (shutdown continues in the background)
```

- A generation lets concurrent `shutdown()` calls all wait for the **same** shutdown.
  An arriving `Immediate` upgrades a `Graceful` generation. Tasks still queued are returned as `Retry`.
- A `Graceful` timeout returns `ShutdownOutcome::TimedOut` or `ShutdownError::TimedOut`,
  depending on which step timed out. The bus stays `Closing` and the background thread finishes the rest.
- The background shutdown thread runs inside `catch_unwind`. If it panics, the facade
  emits `Diagnostic::InternalFailure` and advances the state to `Closed`, so the caller is not left waiting forever.
- `EventBus` is a `Clone` of an `Arc` handle and has **no `Drop` shutdown**. Coordinator
  threads hold `Arc<EventBusInner>`, so dropping the last caller handle does not stop them.
  The caller must call `shutdown`.

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
- **Wakeups use `Waker`.** `AsyncSignal` (one-shot and resettable), `AsyncAdmission`,
  and `AsyncOrderingLanes` register and wake `Waker`s. They do not use channels or threads.
- **Every SPI call is a boxed `Send` future**, and `catch_spi_future` captures a provider panic.

### 9.2 `AsyncAdmission`: a global in-flight limit

- Capacity comes from `DeliveryAdmissionConfig::max_in_flight` (default 4).
- `acquire()` returns a future. With no free slot it enqueues its `Waker` in a **FIFO**
  waiter list. Releasing a permit wakes the head, so waiters are fair and cannot starve.
- The permit is RAII and releases on drop. A delivery task holds it until the handler
  finishes and a settlement intent has been produced.
- `AsyncTracker` counts in-flight work for `wait_for_received_deliveries` and shutdown drain.

### 9.3 `AsyncOrderingLanes`

Lanes are keyed by `(subscription_id, ordering_key)`. A delivery task `acquire`s the
lane before entering the handler. If the previous holder has not released it, the
task suspends and registers a `Waker`. Release wakes the next waiter in FIFO order.
An `OrderingPolicy::Unordered` subscription does not use a lane.

### 9.4 `AsyncSubscription`: session, lease, and a resumable `run`

```
AsyncSubscription<T>
 ├─ control: Arc<AsyncSubscriptionControl<T>>   stop signal (SessionSignals), runner count, SessionSlot
 └─ SessionSlot → Mutex<Option<AsyncSession<T>>>
        AsyncSession
         ├─ receiver: Option<Box<dyn AsyncEventSubscriptionSpi>>   single-owner receiver
         ├─ pending / waiting_admission: Option<PendingDelivery>    at most one received, not-yet-admitted message
         ├─ tasks: Vec<OwnedDeliveryTask>                          delivery futures in progress
         ├─ completed: VecDeque<PendingDelivery>                    finished items waiting to settle
         ├─ admission_waiter: Option<AsyncAdmissionFuture>
         └─ handler: Option<SharedAsyncHandler<T>>                  supplied by run(), kept across a pause
```

`PendingDelivery` is the facade-side lifetime of one message: the event, the token,
`settlement_intent`, `settlement_failures`, a pending failure diagnostic, the admission
permit, and the ordering-lane guard. The permit and the guard drop with the
`PendingDelivery`, so they cannot be forgotten.

- `run()` `lease()`s the `Session` (waiting if another `run` already holds it) and
  executes `run_loop`. If the subscription has not been disposed, the `Session` goes
  back into the slot. Therefore:
  - **Dropping the `run()` future pauses.** The receiver stays in the session.
    Unsettled messages stay with the provider (this depends on `receive` being
    cancellation-safe). A later `run()` continues from that state.
  - **Shutdown can take over a paused session.** `AsyncEventBus::shutdown` calls
    `shutdown(mode)` on each control. That call `lease()`s the session, finishes
    pending deliveries when the mode is `Graceful`, then `close_inner()`.
- Each `run_loop` turn checks the stop signal, applies settlement intents, and if a
  message is pending (received but not yet admitted) `acquire`s a permit; otherwise
  it calls `receive(Duration::MAX)`. **Each subscription holds at most one message
  that has not been admitted.** That is the backpressure (the facade does not pull
  more from the provider) and it means pause or shutdown has only one message to return.
- A delivery task (`OwnedDeliveryTask`) is a `'static` owned future. `run_loop` polls
  the `tasks` with `select` semantics: any task completing, `receive` returning, or a
  stop signal produces an `AsyncRunnerEvent`. After the handler finishes, the
  `PendingDelivery` enters `completed` with a `settlement_intent`. The next turn of
  `run_loop` settles it through `&mut receiver`. This is the same idea as the
  synchronous "send settlement back to the owner thread".
- `close()` stops `run` (`Immediate`), `lease`s, calls `receiver.close()`, and unregisters from the bus.
- `Drop` is `dispose()`: stop immediately, drop the session (the receiver is dropped
  and the provider applies Ephemeral or Durable semantics), and unregister the control.
  **This differs from the synchronous handle.** Dropping the async handle disposes it,
  because no background thread is left to keep consuming.

### 9.5 `AsyncEventBus::subscribe` and subscriptions that never start

`subscribe` returns as soon as the SPI receiver exists. If the caller never calls
`run()` and then `shutdown`s, `close_unstarted_subscriptions` `lease()`s each control
and `close_inner()`s it, so the provider receiver is closed and no subscription is
left hanging. If `subscribe` completes when the bus is no longer `Running`, the
receiver is closed immediately and the call returns `SubscribeError::Closed`.

### 9.6 Settlement retry backoff

A failed settlement emits `Diagnostic::SettlementFailed` and stays on the intent queue.
The retry delay is `10 ms × 2^n`, capped at 1 s (the exponent is at most 7), using
the injected `Timer`. Retries rely on the idempotence contract in §4.5. An `Immediate`
shutdown abandons the remaining intents and emits `SettlementUnavailable`.

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

- **A lane is the order.** Messages with the same key enter the tail of one lane.
  Only the head of a lane can be delivered, and the lane does not dequeue again
  until that message is settled. That is the `PerKey` order, with no extra lock.
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
| `queue_capacity` | 1024 | Messages that may be queued on one subscription (not counting in-flight) |
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
- Routing is a **mailbox**:
  `AsyncBusState { mailboxes: HashMap<MailboxKey { topic, subscriber }, Arc<AsyncMailbox>>, payload_types: HashMap<TopicAddress, TypeId>, closed, outcome }`.
  Subscribing again while a mailbox for the same `(topic, subscriber_id)` is live
  returns `duplicate_subscriber`. The synchronous provider indexes by `subscription_id`,
  so the same `SubscriberId` may have several concurrent subscriptions on one topic.
  **This difference is known.** Callers should not depend on subscribing the same
  subscriber to the same topic more than once.
- Capacity and budget share `LocalQueue` and `OutstandingBudget`. `AsyncLocalShared`
  also holds `AsyncSignal changed` and `Arc<dyn Timer>`.
- `close_mailbox` is Ephemeral as well.
- `AsyncLocalEventBusSpi::with_timer(config, timer)` lets tests inject a manual timer.
  `AsyncLocalEventBusProvider`, when created through the registry, uses `qubit_clock::StdTimer`.
  That provider is **not** submitted to the async inventory catalog (§6.4).

### 10.5 How the facade and `local` divide one message

Take a delayed `PerKey` message. The facade checks capabilities and puts
`ordering_key` and `delay` on the `OutboundMessage`. `local` places it on the
matching lane and pushes the delay heap. When it is due, `receive` returns it.
The synchronous scheduler then uses `active_keys` so the same key is not handled
concurrently. On `local` that second check is redundant, because `local` already
dequeues one key serially. It is necessary for a provider that only guarantees
partition order and does not itself serialize a key.

### 10.6 Resources and benchmarks

`benches/local_scale.rs` (repeatable hot-path measurements that do not depend on a
benchmark harness) and `benches/local_threads.rs` (thread and resource cost of
creating and tearing down synchronous and asynchronous subscriptions) give an order
of magnitude. Numbers move with the machine. Run them directly:

```bash
cargo bench --bench local_scale
cargo bench --bench local_threads
```

By design, the synchronous facade's thread count is the subscription count plus
`max_in_flight` plus one during shutdown. The asynchronous facade creates no threads.
The `local` provider itself creates no threads.

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
}

pub enum NotificationOutcome {
    Published(PublishReceipt),
    PublishFailed(PublishError),
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
- **`Drop`** closes the sender and does not wait. The worker exits after it drains what is left.
- The publisher does not own the `EventBus` lifetime. After the bus shuts down the
  worker receives `PublishError::Closed` and reports it through the observer. The caller still calls `close()`.

---

## 12. Error model

**Each public operation has its own error enum. `EventBusError` only aggregates them.**
Errors carry enough context (provider, operation, resource, retryability) and do not carry the business payload.

| Type | Produced by | Principal variants |
| --- | --- | --- |
| `PublishError` | `publish` / `publish_all` | `Configuration`, `Capability`, `Codec`, `Spi`, `Retry(Box<RetryError<PublishAttemptError>>)`, `InterceptorPanicked { .. }`, `ErrorHandlerPanicked { .. }`, `Closed` |
| `AdmissionCheckError` | `PublishReceipt::check_admission` | `VisibilityUnavailable`, `Dropped`, `NoAcceptedDestination`, `RejectedDestinations { .. }` |
| `SubscribeError` | `subscribe` | `Configuration`, `Capability`, `Spi`, `Closed` (a missing codec is `Capability`) |
| `DeliveryError` | handler return / pipeline | `Handler { source }`, `Codec`, `Spi`, `Retry(Box<RetryError<DeliveryAttemptError>>)` |
| `LifecycleError` | `wait_for_*`, `cancel`, shutdown internals | `Timer(TimeError)`, `WouldDeadlock { operation }`, `Closed`, `IdleWaitUnsupported`, `Spi`, `SubscriptionClose(Arc<SubscriptionCloseErrors>)` |
| `ShutdownError` | `shutdown` | `TimedOut { .. }`, `CoordinatorStart(io::Error)`, `Lifecycle(LifecycleError)` (including `WouldDeadlock`), `Spi`, `SubscriptionClose` |
| `ProviderError` | registry | `Resolution` (provider not found, or an illegal selection), `Creation` (provider construction failed, or `RequiredCapabilities` are missing) |
| `EventBusProviderError` | provider authors | The error wrapper a provider `create` returns, aggregated by `qubit-spi` |
| `SpiError` | provider | `Operation { provider_id, operation, resource, kind, retryable, source }`, `InvalidSettlementToken { .. }` |
| `CapabilityError` / `CodecError` / `ConfigurationError` / `EventIdGenerationError` | construction or validation | see each type |

- `SpiError::retryable()` is the only channel a provider uses to tell the facade
  "this is worth retrying". Publish and delivery retry both consult it.
- `SpiError::Operation::kind` is a `&'static str` classification (for example
  `closed`, `invalid_argument`, `spi_panic`, `worker_panicked`, `duplicate_subscriber`,
  `topic_type_conflict`) used in logs and test assertions. Each facade layer's `Closed`
  variant comes from that layer's own lifecycle gate. A closed-style `SpiError`
  returned by a provider after shutdown is passed through as the `Spi` variant and is not remapped.
- Enums are `#[non_exhaustive]` so a new variant can be added later.
- Every error is `Send + Sync + 'static` and can cross threads and tasks.
- Internal `PipelineFailure { origin, error }` is not public. It carries the failing stage to diagnostics and tests.

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
| `SettlementFailed` | `settle` returned an error (it will be retried) |
| `SettlementUnavailable` | Settlement was required but the capability does not allow it, or it was abandoned (for example an `Immediate` shutdown) |
| `InternalFailure` | An internal facade failure that should not happen (for example the coordinator's `receive` returned `Err`) |

`PublishMetricsSnapshot` (`publish_metrics()`) is the publish-side counters from
step 10 of §7.3. It is for tests and light monitoring. It is not a full metrics system.
On the async side, `attempts` increments when the future is first polled. A publish
future that is constructed and dropped without being polled is not counted.

Diagnostics are a **push** model, not a log. This crate does not depend on `log` or
`tracing`. Wiring them into a monitoring system is the caller's choice.

---

## 14. Concurrency invariants

1. At any moment only one owner calls methods on a given `EventSubscriptionSpi` or `AsyncEventSubscriptionSpi` (synchronous: the coordinator thread; asynchronous: the `run` or `shutdown` that holds the lease).
2. A message's `SettlementToken` produces at most one successful settlement. A retry repeats the same disposition.
3. The first `Acknowledgement` decision wins and is then immutable.
4. Handlers for the same subscription and the same `ordering_key` do not run concurrently (synchronous: `active_keys`; asynchronous: `AsyncOrderingLanes`).
5. The number of handlers running at once is at most `max_in_flight` (synchronous pool size, or asynchronous admission capacity).
6. Each subscription has at most one message that has been taken from the provider and has not yet entered a handler (synchronous `pending`; asynchronous pending plus a single permit).
7. After `Closing`, `publish` and `subscribe` return `Closed`. `shutdown` is idempotent and provider `shutdown` is called once.
8. A panic in user code does not exit a thread or task and does not corrupt bus state.
9. Dead-letter is at most one level deep. The dead-letter header cannot be set or altered from outside the pipeline.
10. Cancel and shutdown do not drop a message: a handler task that has not started is returned to the provider as `Retry` when the capability allows it.
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
| `tests/support/flume_spi.rs` | A second real transport on `flume`, so the SPI is not tailored to `local` |
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
builds a fresh instance so cases do not contaminate each other) and `ConformanceHooks`
(optional `settlement` and `receive_cancellation` hooks, so an author can add idempotence
or cancellation checks only they can verify). It returns a `ConformanceReport`
(`Vec<ConformanceCase::{Passed, Failed, Skipped}>`). Current cases cover capability and
payload-mode consistency, `subscribe`, `publish`, `receive-payload`, settlement
idempotence and conflicts, and `shutdown`. A provider that does not support a case
should record it as `Skipped`, not return a vague error after the test has already
called the operation. The runner is the minimum acceptance gate for provider authors
and the executable statement of this crate's SPI contract.

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
- **Async `local` `duplicate_subscriber` differs from synchronous `local`** (§10.4).
- **Async `local` does not participate in discovery** (§6.4).
- **`wait_for_idle` depends on the provider.** When the provider does not support it, the call returns `IdleWaitUnsupported`. Use `wait_for_received_deliveries` or an application-level signal instead.
- **`provider_attempt` is always `None` today.** `InboundMessage` has no source for a provider redelivery count.

---

*This document is maintained with `qubit-event-bus` 0.14.x. A change to facade or SPI behavior should update the matching section here and in the [Chinese document](design.zh_CN.md).*
