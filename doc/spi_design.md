# Qubit Event Bus SPI Architecture Design

[English user guide](user_guide.md) · [Chinese SPI design](spi_design.zh_CN.md) · [Architecture status (English)](design.md)

## Document status

This is the formal SPI and facade design for `qubit-event-bus` 0.12, targeting Rust 1.94 or later. It describes behavior implemented by this version. A capability or extension described as provider-specific is not automatically guaranteed by every backend. The built-in provider is synchronous and in-process; this crate does not ship a broker adapter.

## Goals and boundaries

The crate provides typed, portable event models and synchronous and runtime-neutral asynchronous facades. Providers implement small object-safe SPI contracts for publishing, receiving, settlement, capabilities, and transport shutdown. The facade owns portable policy: validation, codec selection, middleware, filtering, retry coordination, dead letters, ordering, admission bounds, diagnostics, and lifecycle sequencing.

The provider boundary transports erased payloads and provider-issued settlement tokens. It does not execute application handlers or own facade middleware. The application uses `Topic<T>`, `PublishRequest<T>`, `SubscribeRequest<T>`, and `Delivery<T>` rather than provider payload types.

`SpiSubscriptionRequest` carries the topic's Rust `TypeId` so a native provider can reject same-name topics with conflicting payload types before routing. Encoded providers may ignore this process-local type identity. The synchronous `EventBusSpi::wait_for_topic_idle` method defaults to `Ok(None)`; providers that implement it report topic outstanding work as `Some(true)` when idle and `Some(false)` on timeout.

The crate does not promise persistence, cross-process delivery, transactional batches, or exactly-once processing. It does not bundle Tokio, crossbeam, flume, RabbitMQ, Kafka, or Redis adapters. Provider-specific guarantees must be declared through capabilities where representable and documented by the adapter.

## Architecture

```text
Application
  ├─ EventBus / AsyncEventBus
  │    typed API, processing policies, lifecycle coordination
  ├─ model / codec / pipeline
  │    envelopes, type erasure, middleware, delivery policy
  ├─ EventBusSpi / AsyncEventBusSpi
  │    object-safe transport contracts
  └─ qubit-spi registry
       provider discovery, selection, creation, creation-time fallback
           └─ LocalEventBusProvider (sync, in-process)
```

`EventBus` and `AsyncEventBus` are concrete, cloneable facades. The asynchronous facade is runtime-neutral: it does not spawn a task or require Tokio. An application drives `AsyncSubscription::run` on its executor. Sync and async middleware use distinct types; a facade rejects middleware belonging to the other execution model.

The registry may select a fallback provider while creating a backend. A runtime publish, subscribe, receive, settlement, or shutdown failure does not silently switch providers because the operation may already have side effects.

## Identity and domain model

- `Topic<T>` validates a topic name and associates it with a Rust payload type.
- `SubscriberId` is an application-named logical identity.
- `qubit_id::Id` identifies a subscription object within one bus instance. It is not a portable event identity.
- `EventId` is carried by the event envelope and transport message. The default is a UUID v4 and is suitable for cross-process correlation and deduplication.
- `EventEnvelope<T>` carries the event ID, topic, payload, timestamp, headers, ordering key, and delay metadata.
- `PublishRequest<T>` and `SubscribeRequest<T>` are validated facade inputs. Their builders configure metadata and policies without exposing SPI payload details.
- `Delivery<T>` contains the event, delivery context, and acknowledgement handle. It is not itself the provider settlement token.
- `PublishReceipt` reports the provider identity and acknowledgement for one publish attempt. It does not mean a handler has started or completed.

## Codec and transport payload

`EventCodec<T>` converts typed payloads to and from the SPI's erased `Payload` representation. Providers advertise supported payload modes through capabilities. A facade performs codec and capability checks before issuing an SPI operation. Codec failures are publish/receive errors, not provider selection signals.

A provider must preserve the event identity and transport metadata required by the advertised contract. Provider-specific options are namespaced opaque values; applications should not place secrets in values that may be logged or debug-formatted.

## Provider SPI contracts

The synchronous `EventBusSpi` contract provides capability reporting, publish, subscription creation, and shutdown. Each subscription implements `EventSubscriptionSpi`, which exposes bounded-time receive, settlement, and close. The facade owns worker threads for sync subscriptions and calls `receive` with a finite timeout so it can observe cancellation.

The asynchronous `AsyncEventBusSpi` contract exposes equivalent operations as runtime-neutral futures. Async providers must not assume a specific executor. Dropping an operation future may happen after the provider has performed a side effect; retry-sensitive operations must therefore follow their idempotency contract.

SPI methods return structured `SpiError` values. Preserve provider and operation context and retain the underlying source error. Do not panic for expected transport failures. `shutdown(mode)` closes provider transport resources only; facade delivery and receiver coordination happens before it is called.

## Capabilities

Capabilities report the backend contract, including payload mode, settlement, ordering, delayed delivery, durability, consumer groups, replay, publish guarantee, and publish visibility. `RequiredCapabilities` lets an application fail provider creation when a required property is absent.

A declared capability is a provider promise, not a behavior synthesized by the facade. An adapter must report only properties it actually satisfies and describe any limits that cannot be expressed by the capability model. Runtime failures do not cause provider failover.

## Publication and acknowledgement

`publish` returns `Result<PublishReceipt, PublishError>`. `Ok(receipt)` means the provider returned its acknowledgement; it does not prove that a subscriber handler ran. `publish_all` attempts requests independently in input order and retains each result. It is best-effort and non-atomic.

`PublishAcknowledgement` is non-exhaustive and currently has these forms:

- `Accepted { provider_message_id, metadata }`: a broker accepted the message. Consumer identities may be unavailable.
- `DestinationAdmissions(Vec<DestinationAdmission>)`: the provider reports destination-level admission. An empty vector means no destinations were reported. Each destination can be `Accepted`, `Filtered`, or `Rejected(reason)`.
- `DroppedByInterceptor`: a publisher interceptor intentionally stopped dispatch before the provider publish.

A local provider may accept one destination and reject another because its bounded queue is full. Such a mixed result remains a successful receipt: changing it to a whole-request error would hide already accepted work and encourage duplicate delivery. `Filtered` is intentional exclusion, not capacity rejection. Inspect the acknowledgement and apply an application-owned compensation policy. Do not blindly resend an entire partially admitted event; use an idempotency key or retry only work that is safe to repeat.

## Subscription and delivery pipeline

A subscription setup validates topic, payload mode, capability requirements, middleware execution model, and provider options before creating the receiver. The facade then owns the receiver lifecycle. Sync facade workers receive messages and coordinate handler execution; async subscriptions are driven by the caller.

The delivery path can include receive, gap reporting, decode, filtering, bounded admission, ordering, middleware, handler, retry/error policy, dead-letter publication, and final settlement. Ordering keys may progress concurrently with other keys while preserving order within a key. In-flight limits are bus-wide and cover admitted delivery work through settlement. A message received but not admitted remains bounded per subscription and is returned through the provider's retry/recovery mechanism when admission is unavailable.

`EventBus::wait_for_idle` asks the synchronous provider whether the topic has queued or unsettled messages. The SPI returns `Ok(Some(true))` when none remain, `Ok(Some(false))` on timeout, and `Ok(None)` when this capability is unsupported; the facade maps the last case to `LifecycleError::IdleWaitUnsupported`. This does not prove handler success or global activity on a remote broker. `wait_for_received_deliveries` retains the old facade-local received-work tracking. The async facade exposes that operation as `wait_for_received_deliveries` and does not claim provider queue emptiness.

## Acknowledgement, NACK, and settlement

Automatic acknowledgement settles a successful handler result according to configured policy. Manual acknowledgement requires the handler to decide through the delivery acknowledgement handle before it returns. An omitted decision is a delivery failure.

Providers issue opaque `SettlementToken` values. Repeating the same token with the same disposition must be idempotent and return a consistent result. A conflicting disposition for one token must fail. Async settlement can be retried with the original token and disposition after future cancellation; providers must handle both in-progress and completed duplicate requests safely.

Receiver close or drop must not implicitly acknowledge outstanding work. An adapter must make unsettled deliveries recoverable according to its advertised contract.

## Retry, errors, and dead letters

Retry policy and classification types come from the application's direct `qubit-retry` dependency; the event bus does not re-export that crate. A classification rule alone does not enable retries: a retry policy is also required. Cancellation can prevent a later attempt but cannot interrupt a synchronous handler already running.

Terminal publish-error handlers receive `PublishFailureContext<T>` after direct SPI publish failure when no retry policy applies, or when configured retries terminate. Preflight validation, capability, codec, and interceptor errors do not invoke that handler. The context shares payload ownership and does not require `T: Clone`.

Dead-letter handling is at-least-once around uncertain asynchronous outcomes. A resumed runner may publish the same dead letter again if cancellation occurred after provider side effects. Consumers should deduplicate by event ID or a business idempotency key where required.

## Backpressure, ordering, and delay

The local provider uses a bounded queue per subscription. Its capacity counts queued and unsettled messages, including messages already received by a consumer; `Retry` preserves the message's reservation. Queue capacity is not a global broker quota. The facade also applies a bus-wide limit to admitted delivery work; sync configuration sets in-flight and handler-queue bounds, while async configuration sets `max_in_flight` (default 4). Limits cover queued or running facade work through final settlement.

Ordering is a facade/provider contract described by capability. The facade coordinates order where supported; providers must preserve the ordering guarantees they advertise. Delayed delivery is likewise capability-gated and may be provider-native or represented through transport metadata only when the adapter can honor the contract.

## Diagnostics and error model

Errors are separated by operation, including publish, subscribe, receive, lifecycle, and shutdown. Provider sources should remain available through the error chain. Diagnostics are observations, not replacements for operation results.

Diagnostic observers run synchronously on the thread emitting the event, outside registry locks. Panics are contained, but a blocking observer can delay that thread. Diagnostics are not buffered on a separate queue. Applications should keep observers short and nonblocking.

## Lifecycle and shutdown

A facade shutdown coordinates its own admission, subscriptions, workers, settlements, and provider resources. Applications should not bypass this sequence by shutting down the SPI directly.

For synchronous `EventBus::shutdown`, `Graceful { timeout }` bounds the caller's wait for the complete close sequence, including public operations admitted earlier, subscription close, active delivery work, and provider shutdown. One background coordinator is used per bus. If the deadline expires, the call returns `ShutdownError::TimedOut`; the bus remains `Closing` and rejects new publish/subscribe calls while cleanup continues. Call shutdown again to await the result, or request `Immediate` to strengthen an active attempt. A blocked synchronous SPI call or user handler cannot be forcibly interrupted and may keep the coordinator thread alive. A coordinator thread creation failure is returned as a structured error and a later call can retry startup.

`Immediate` stops new intake and discards work that has not started, but waits for active handlers, settlement, receiver close, and provider shutdown so the facade can report errors. It does not mean that Rust user code is forcibly terminated. A synchronous callback or worker that calls blocking shutdown on its own bus receives `WouldDeadlock`.

Async shutdown is awaited through its future and uses the configured `qubit-clock` timer for graceful deadlines. Dropping or timing out the future does not roll back provider side effects; retryable close and shutdown operations must be idempotent. The caller can invoke shutdown again while the bus remains `Closing`.

## `qubit-spi` registry and fallback

Provider definitions are registered in the corresponding registry and selected through `qubit-spi` selection configuration. Creation-time fallback can choose another compatible definition when a preferred provider is unavailable or lacks required capabilities. Once an instance is created, runtime errors are returned to the caller and are not transparently replayed through another provider.

Provider configuration belongs to the adapter. The facade owns portable settings such as codec registration, middleware, retry, admission, and diagnostic observers. Keep transport credentials in a secret manager or secure configuration path rather than opaque debug-friendly provider options.

## Built-in local provider

`LocalEventBusProvider` is an in-process synchronous provider with bounded per-subscription queues. It is useful for tests and applications that need local dispatch. It does not provide persistence, cross-process routing, broker durability, or a distributed acknowledgement. Destination-level admissions can show accepted, filtered, and rejected local subscribers, including partial admission when a queue is full.

## Conformance and verification

An adapter should verify at least the following:

1. Advertised payload, settlement, ordering, replay, delay, durability, and publish-visibility capabilities match observed behavior.
2. Publish acknowledgement describes provider admission and does not claim handler completion.
3. Destination admission reports empty, accepted, filtered, and rejected cases accurately where supported.
4. `receive` respects the requested timeout and cancellation/close behavior.
5. Settlement with the same token and disposition is idempotent; conflicting dispositions fail.
6. Close and shutdown are safe to retry after uncertain or cancelled async operations.
7. Receiver close/drop does not silently acknowledge unsettled work.
8. Provider errors preserve operation context and their source.
9. Shutdown is safe when repeated and returns a stable outcome after completion.

Facade contract tests should cover empty, partial, and fully rejected admission results; handler success/failure and manual settlement; cancellation and shutdown races; async future cancellation; error-source preservation; and the declared capability boundary. Concurrency tests should use barriers or channels rather than timing-only sleeps when checking ordering and races. Rustdoc examples, bilingual README and guide links, and project CI scripts should be checked before release.

## Public API stability and migration

The public API is non-exhaustive in selected enums so new acknowledgement and error cases can be added. Consumers should match known variants and include a fallback arm. Treat `EventId` as portable identity and `qubit_id::Id` as bus-local identity; they are not interchangeable.

Applications adopting 0.12 should inspect `PublishReceipt::acknowledgement()` rather than infer subscriber completion from `publish` success. Sync graceful shutdown timeout now describes caller wait for the complete close sequence: after `TimedOut`, the bus is still closing in the background and no new operations are admitted. Review any shutdown code that assumed timeout cancelled provider work. Async applications should continue to treat future cancellation as an uncertain provider outcome.

## Acceptance criteria

A conforming implementation must keep typed facade behavior independent of transport details; accurately expose admission and settlement outcomes; enforce capability declarations; preserve structured errors; bound facade-owned queues and in-flight work; coordinate close and shutdown without hidden provider failover; document cancellation and retry limits; and pass provider conformance, facade contract, Rustdoc, and project CI checks.
