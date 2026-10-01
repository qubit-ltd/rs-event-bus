# Migration guide

[简体中文](migration.zh_CN.md) · [User guide](user_guide.md) · [Design](design.md)

## Upgrade from 0.17 to 0.18

Coordinate `qubit-event-bus = "0.18.0"`, `qubit-event-bus-redis = "0.6.0"`, and `qubit-task = "0.9.0"`. Every root and consumer fixture must resolve one local core 0.18. IoC/execution-service fixture migration does not bump production versions. Update lockfiles and run the ecosystem metadata gate.

### Separate ownership from handler execution

The old sync `max_in_flight` coupled queued and running work, while async used a separate admission configuration:

```rust
// Before: 0.17; historical API, removed in 0.18.
let sync = EventBusFacadeConfig::new()
    .with_sync_delivery_scheduler(SyncDeliverySchedulerConfig::new(4, 0)?);
let asynchronous = EventBusFacadeConfig::new()
    .with_delivery_admission(DeliveryAdmissionConfig::new(4)?);
```

Both facades now share this four-parameter configuration and a separate settlement budget:

```rust
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::time::Duration;

use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SettlementRetryConfig;

let scheduling = DeliverySchedulingConfig::new(
    NonZeroUsize::new(4).unwrap(),   // running handlers
    NonZeroUsize::new(256).unwrap(), // globally owned deliveries
    NonZeroUsize::new(32).unwrap(),  // owned per subscription
    NonZeroUsize::new(256).unwrap(), // registered subscriptions
)?;
let settlement = SettlementRetryConfig::new(
    NonZeroU32::new(5).unwrap(),
    Duration::from_secs(5),
    Duration::from_millis(10),
    Duration::from_secs(1),
)?;
let config = EventBusFacadeConfig::new()
    .with_delivery_scheduling(scheduling)
    .with_settlement_retry(settlement);
```

All four scheduling values are nonzero; running and per-subscription capacity must not exceed global owned capacity. Owned includes receive reservations, queued, running and settling work; paused sessions still count as subscriptions. Old `queue=0` direct handoff has no identical replacement: `owned=running=4` with `per_subscription=4` reduces prefetch, but still counts receive/settlement and is not the old semantics. Defaults are 4/256/32/256. Removed config types, builders and getters have no compatibility aliases.

### Adopt finite settlement retry and structured diagnostics

The old settlement path had no explicit public total-attempt/elapsed budget. Defaults now are 5 attempts including the first, 5 seconds, 10 ms initial backoff, and a 1-second cap. Only `retryable()==Some(true)` retries; `None` stops by default and false stops immediately. Settlement does not rerun the handler or change token/disposition. The budget does not forcibly interrupt an in-flight call. Termination stops only the affected subscription and retains its first cause.

```rust
// Before: error was a display string; do not classify by its text.
if let Diagnostic::SettlementFailed { error, .. } = diagnostic {
    eprintln!("{error}");
}
```

```rust
use std::error::Error;

use qubit_event_bus::pipeline::Diagnostic;

// After: borrow the real SpiError and its original source chain.
match diagnostic {
    Diagnostic::SettlementFailed { attempt, error, .. } => {
        eprintln!("attempt={attempt}, retryable={:?}, source={:?}", error.retryable(), error.source());
    }
    Diagnostic::SettlementStopped { attempts, termination, error, .. } => {
        eprintln!("stopped after {attempts}: {termination:?}, source={:?}", error.source());
    }
    _ => {}
}
```

`SettlementFailed.error` changes from a string to `Arc<SpiError>` and adds `attempt`; `SettlementStopped` includes attempts, termination and the same structured error. Retain `terminal_failure()`, inspect bus/subscription `delivery_metrics()`, fix the cause, close the old subscription, then recreate the same durable group to recover nonterminal work. Settlement may already have applied, so failure is not a promise of redelivery; local resubscription cannot restore discarded messages.

### Update republish and shutdown decisions

Replace “final ACK accepted nowhere, so retry whole” with a history-first check of `receipt.duplicate_possible()`. True means reconcile by event ID; only false plus `NoDestinations`/`NoneAccepted` permits considering whole-event retry. Partial acceptance repairs rejected destinations only; interceptor `Dropped` does not automatically retry. `check_admission` does not check history.

Replace “Graceful timeout then Immediate rescue” with two individually bounded Graceful waits. Treat `TimedOut` as incomplete, propagate other errors, and hand final false plus snapshots to an external supervisor. This does not prove an uncooperative handler will end or the process will exit. See the complete compiled [user-guide examples](user_guide.md). Redis `XADD` Accepted does not promise fsync; existing-group StartPosition does not reset its cursor; PEL is affected by trimming/claim policy. Task notification gaps in `state_version` still require an authoritative query and cannot roll back persisted state.

For synchronous callbacks or async applications that must start shutdown without blocking, use `EventBus::request_shutdown(mode)` and retain its `EventBusShutdown` ticket. Observe it with `wait_async().await` on the application executor or with `wait(Some(timeout))`; cancelling an async observation or timing out a synchronous observer does not cancel cleanup. Dropping the ticket only releases that observer. `AsyncEventBus` keeps its caller-driven async `shutdown` API. See [request shutdown without waiting](user_guide.md#request-shutdown-without-waiting).

Earlier migration steps follow for applications upgrading across multiple versions.

## Upgrade from 0.16 to 0.17

Upgrade the application and every provider together: `qubit-event-bus` 0.17,
`qubit-event-bus-redis` 0.5, and `qubit-task` 0.8 when task notifications are used.
An old minor and 0.17 are different Rust type/SPI generations. Update direct,
optional, and fixture dependencies and lockfiles; run the ecosystem metadata gate
and each affected project's CI. IoC and execution-service fixture changes do not
require a production version bump when their production APIs are unchanged.
There are no compatibility aliases for the removed APIs below.

### Migrate codecs and existing messages

Change `EventCodec::decode(&[u8])` to `decode(&EncodedPayload)` and read bytes
through `payload.bytes()`. Metadata is available through `content_type()` and
`schema_id()`. The facade calls `validate_metadata` before decode. Its default
requires exact content type text and exact `Option<SchemaId>` equality; `None`
never implicitly matches `Some`, and MIME text is not normalized.

If a codec intentionally supports older schemas, override validation with an
explicit documented allowlist and dispatch decoding by the accepted schema.
The task JSON codec in the task guide writes `task-event-v1` and explicitly
accepts historical `None` with the same `application/json` content type; this is
that codec's compatibility rule, not a relaxed global default. Redis wire
version 1 remains readable: upgrading the public Rust API does not delete old
stream entries. Test a retained old record through the new codec before rollout.

### Replace unlimited encoded limits

Replace `with_max_encoded_payload_bytes(Option<NonZeroUsize>)` with
`with_payload_limits(PayloadLimits::new(publish_limit, receive_limit))`.
Both limits must be positive and default to 1,048,576 bytes; exactly the limit
is allowed. There is no unlimited setting. Publication checks after encode and
before SPI admission; receiving checks before metadata or decode callbacks.
`CodecError::PayloadTooLarge` now carries `PayloadDirection::Publish` or
`Receive`. These limits do not cap codec allocations, native object memory,
or Redis's initial RESP-frame allocation.

Redis separately defaults to 8 MiB `redis.max_wire_bytes`, 1 MiB
`redis.max_payload_bytes`, and 64 KiB `redis.max_headers_bytes`. Set positive
provider options deliberately when existing records require more. All remain
independent, so a payload below its limit can still exceed the serialized wire
limit. Provider-side publication errors from these checks occur before `XADD`.

### Handle uncertain publication before choosing retry

`EventBus::publish`, `AsyncEventBus::publish`, batch items,
`NotificationOutcome::PublishFailed`, and `PublishErrorHandler` use
`PublishFailure`, preserving the original `event_id()`, aggregate `effect()`,
and structured `cause()` with its source chain. Pattern-match on the cause when
needed, and inspect effect before deciding whether another attempt is safe.
When an application returns `Result<_, EventBusError>`, propagate
`bus.publish(request)?` directly: `From<PublishFailure>` selects the new
transparent `EventBusError::PublishFailure` variant and keeps event ID, effect,
and cause. The existing `Publish(PublishError)` variant is still a cause-only
conversion for errors without a publication identity. Do not call `into_cause()`
just to satisfy aggregate conversion; it would discard the publication wrapper.

Providers should return `SpiError::Publish` with `PublishEffect::NotAccepted`
only when no admission is known to have occurred. Generic operation errors and
provider panics conservatively mean `MayHaveBeenAccepted`.

`PublishAttemptError::new` also takes an explicit effect argument; migrate
custom attempt wrappers rather than inferring effect from error text.

`DuplicateRiskPolicy::Forbid` is the default hard gate: custom `RetryRule` cannot
force another uncertain attempt. Set `AllowDuplicates` only when the business
can tolerate duplicates; configured retry policy and retryability still apply.
A prior uncertain attempt stays uncertain after a later definite rejection;
if later publication succeeds, `PublishReceipt::duplicate_possible()` reports
that earlier risk. Typed interceptors cannot change the event ID. Retries retain
the same ID, timestamp, and encoded bytes; Redis does not deduplicate by EventId.

If retry cancellation interrupts an already-polled SPI future and returns a
failure, its effect is uncertain. Pre-SPI cancellation has no admission effect.
Dropping the public publish future produces no failure value: preserve the
request ID and reconcile a started operation as possibly published. An unpolled
future makes no attempt. RetryPolicy budgets are soft budgets, not universal
hard deadlines for in-flight provider commands or synchronous user code.

Redis distinguishes failure stages: opening a connection before submission and
explicit server rejection are `NotAccepted`; a lost reply, timeout, or response
conversion failure after entering `XADD` query is `MayHaveBeenAccepted`.
DLQ forwarding applies the same gate. Forward and source acknowledgement are
not one transaction; a failed source settlement after successful forwarding can
produce a duplicate logical dead-letter. Make consumers idempotent.

### Recover fail-stop subscriptions

Metadata mismatch, receive overflow, validate/decode panic, and native type
mismatch stop the subscription without `Accept`, `Reject`, or `Retry`.
Ordinary `CodecError::Decode` still rejects a bad message. Inspect
`terminal_failure()` for the first `Arc<SubscriptionStopReason>`; async `run`
returns `ReceiveError::Stopped` and a repeated run on that handle returns the
same cause without another receive. Started handlers finish and close errors
remain independently visible. Cancelling run/close does not erase the cause.

For durable providers, fix the codec/version or limits, close the old handle,
and create a new subscription in the same group to reclaim unsettled work.
Redis `receive_limit_exceeded` and `unsupported_wire_version` retain the PEL
entry without `XACK`, `XDEL`, or quarantine. Malformed in-limit version 1 records
continue to use the existing quarantine path. Do not delete pending data to
hide incompatibility. Ephemeral cleanup may discard messages and only known
loss can be counted; a new local subscription cannot recover discarded work.

Notification close now waits for cleanup as well as the processing loop. A
worker cleanup panic publishes one failed exit and all close callers observe
it. A timed-out caller may wait again; an observer-call panic remains isolated.
The library cannot recover `panic=abort` or forcibly stop blocked destructors.

### Validate the rollout

Use `./scripts/project-ci-check.sh` for standalone core metadata validation. For the
coordinated migration, use `./scripts/project-ci-check.sh --ecosystem-root <repos-dir>`;
the five repository roots and seven declared consumer fixtures are mandatory.
The gate resolves locked all-feature metadata and rejects mixed event-bus
minor generations. It does not replace each project's alignment, CI,
conformance, codec round-trip, durable recovery, and fault-injection checks.
Deploy repaired consumers against retained wire data and inspect their terminal
causes, publish effects, task `uncertain_publish`, and Redis `XPENDING` before
reopening business traffic. Task events remain best effort; compare per-TaskId
`state_version` and query the task service for authoritative state.

## Earlier migration: 0.14/0.15 to 0.16

This guide covers upgrades from `qubit-event-bus` 0.14 or 0.15 to 0.16.0.
Version 0.16 is a breaking release; update direct dependencies and provider
implementations together.

## Update the dependency

```toml
qubit-event-bus = "0.16"
```

Provider crates must also depend on the matching SPI generation. Run the
provider's conformance tests against 0.16 before deploying the application.

## Update dead-letter policy constructors

The constructors were renamed to make the admission guarantee explicit:

| Older call | 0.16 call |
| --- | --- |
| `DeadLetterPolicy::topic(name)` | `DeadLetterPolicy::with_topic_name(name)` |
| `DeadLetterPolicy::known_destination(name)` | `DeadLetterPolicy::with_known_destination(name)` |

`with_topic` and `with_admission` remain available. Choose the known-destination
policy only when the provider can report destination admission; opaque
acknowledgements cannot satisfy it.

## Update conformance hooks

`ConformanceHooks` and `AsyncConformanceHooks` now include
`ephemeral_cleanup`. Add `..Default::default()` to partial struct literals, or
initialize every hook explicitly. Strict conformance now fails when a durable
provider omits the durable recovery fixture or an ephemeral provider omits the
cleanup fixture. A skipped case is not a pass.

The synchronous conformance runner reports future-cancellation checks as
`NotApplicable`; a synchronous API has no cancellable future. Keep independent
tests for receiver close, shutdown, duplicate settlement, and provider-specific
recovery. Async providers must retain real barrier-controlled fixtures for
operations whose futures can be cancelled.

## Recheck delivery recovery assumptions

Durable subscriptions must keep accepted, unsettled deliveries recoverable after
receiver close or drop. Ephemeral subscriptions may discard unsettled deliveries
when their receiver is destroyed. Neither behavior acknowledges an unsettled
delivery. Cancelling a receive future differs from destroying the receiver: a
message consumed before cancellation must remain available to a later receive
or provider recovery. Review shutdown and dead-letter failure handling against
the actual provider durability and settlement capabilities.

For the complete contract and runnable examples, see the
[user guide](user_guide.md), [design document](design.md), and
[API documentation](https://docs.rs/qubit-event-bus/0.16.0/qubit_event_bus/).


## Delivery gaps and admission checks

Subscriptions stop receiving after a provider-reported delivery gap by default. Inspect the stable `SubscriptionStopReason::Gap` through `Subscription::terminal_failure()` (sync) or the `ReceiveError::Stopped` returned by async `run()`. Set `GapPolicy::Continue` only when the consumer accepts missed messages and wants later messages to continue. The gap diagnostic is emitted in either mode.

Use `publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)` when the caller requires destination admission. It publishes once and returns the complete receipt in `CheckedPublishError::Admission` if the requirement fails. Partial admission can mean some destinations already accepted the event; retrying may duplicate those deliveries. Admission does not mean handler completion or durable storage. Sync subscriptions use one coordinator thread each; the default limit is 256, so configure capacity for the expected subscription count.
