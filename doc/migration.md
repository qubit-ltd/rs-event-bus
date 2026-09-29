# Migration guide

[简体中文](migration.zh_CN.md) · [User guide](user_guide.md) · [Design](design.md)

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

Use `./project-ci-check.sh` for standalone core metadata validation. For the
coordinated migration, use `./project-ci-check.sh --ecosystem-root <repos-dir>`;
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
