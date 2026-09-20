# Event bus design

## Scope

`qubit-event-bus` is an in-process, typed dispatch layer. `LocalEventBus` owns lifecycle and runtime state; `LocalEventBusFactory` owns defaults and immutable configuration copied into each new bus. The implementation intentionally does not persist events, coordinate processes, or provide transactional publication.

## Dispatch path

```text
publisher
  -> EventEnvelope<T>
  -> publisher interceptors (global, then typed)
  -> subscriber snapshot
  -> filter and delivery admission
  -> PublishReceipt / BatchPublishResult
  -> local worker pool
  -> subscriber interceptors
  -> handler + ACK/NACK
  -> retry / error handler / dead-letter
  -> DeliveryFailure observer
```

Admission and execution are separate. A receipt's `Accepted` status means a delivery task acquired admission and was submitted; it does not mean the handler ran or succeeded. A rejected task is represented in the receipt and can be observed through error observers. The best-effort batch path retains input order and captures per-event global errors rather than rolling back earlier submissions.

## Type boundaries

`Topic<T>` couples a topic name to a payload type. `EventEnvelope<T>` carries payload and metadata. The `EventBus` trait keeps backend ownership explicit with an associated `Subscription<T>: SubscriptionHandle<T>`; local inherent methods return the concrete local subscription handle.

`PublishReceipt` contains the input event ID, the post-interceptor dispatched ID, and either `Dropped` or a list of `SubscriberDispatchResult`. `BatchPublishResult::accepted_count()` counts items with at least one accepted subscriber status; `failure_count()` includes global errors and any rejected subscriber status. This intentionally permits both counts to include one item.

## Capacity model

`DeliveryLimits` separates two controls:

| Control | Effect |
| --- | --- |
| `max_in_flight` | Maximum number of admitted subscriber deliveries held by the local runtime. |
| `handler_queue_capacity` | Optional bound supplied to the handler executor queue. |

`DeliveryLimits::bounded` configures both values, while `DeliveryLimits::unbounded` leaves the executor queue unbounded for a chosen in-flight limit. Zero values are rejected. `Default` uses `DEFAULT_MAX_IN_FLIGHT_DELIVERIES` (4096) and no explicit handler queue bound. `LocalEventBusFactory::set_delivery_limits` validates and copies the value into the runtime options of a newly created bus.

The permit is acquired before scheduling, and released when processing reaches its terminal path. This makes backpressure visible at admission while keeping handler completion asynchronous to the publisher.

## Ordering, delay, and retries

An envelope with an `ordering_key` selects a lane keyed by topic, ordering key, and subscription ID. Work in that lane is serialized. Work without a key is submitted directly and may run concurrently. Delayed work waits outside handler workers; if queue admission fails when delay expires, the handler is skipped and an `ExecutionRejected` error is reported.

Retry policy execution is synchronous within the publishing or handler execution path. Backoff therefore occupies the current thread and ordering slot. Retry cancellation prevents the next attempt and wakes backoff, but cannot interrupt a synchronous handler already running.

## Acknowledgement and terminal handling

Automatic acknowledgement follows a successful handler result. In manual mode, the handler must ACK or NACK before returning. A missing decision and an explicit NACK are failures. Retry classification then determines whether another attempt runs; after retries, error handlers and dead-letter strategy execute. A terminal `DeliveryFailure` is emitted after that flow completes.

## Lifecycle invariants

The runtime has stopped, starting, started, and stopping boundaries. Registration is rejected once shutdown starts, and restart is rejected until old work has drained. Blocking shutdown from the same bus's subscriber worker would deadlock, so the runtime detects this for `wait_for_idle`. A handler should request shutdown with `shutdown_nonblocking()`. Because the current handler remains active, `shutdown_with_timeout()` reports a timeout there and is reserved for callers that require bounded waiting.

## Configuration ownership

Factory defaults are type keyed for publish/subscribe options and dead-letter strategies, with global variants for metadata-only interceptors and dead-letter handling. Interceptors are copied into a bus at creation; runtime mutation is intentionally absent. This keeps dispatch snapshots stable while allowing runtime error observers to be registered through the bus API.

## Non-goals and extension points

The crate does not promise durable delivery, cross-process routing, handler interruption, or atomic batches. Backends may implement `EventBus` with stronger guarantees, but those guarantees must be documented by the backend. Applications needing transactions must coordinate publication with their own transaction manager or backend.
