# Migration guide

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
