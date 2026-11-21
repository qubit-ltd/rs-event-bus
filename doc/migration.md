# Migration guide

[简体中文](migration.zh_CN.md) · [User guide](user_guide.md)

This page is a stable entry point for core API changes. It summarizes selected
historical transitions and links to the current upgrade guide; it does not
claim that older provider releases are compatible with the current core.

## Unconfirmed publish acknowledgements (source changes after v0.21.4)

The source changes after core v0.21.4 add `SentUnconfirmed` to
`PublishAcknowledgement` and `AdmissionOutcome`. Providers declaring
`FireAndForget` should return this variant instead of legacy `Accepted`; the
facade also normalizes legacy acknowledgements. For opaque providers, a checked
publish requiring per-destination visibility fails before publication with
`CheckedPublishError::UnsupportedVisibility`; no receipt is produced and no
publish side effect occurs. This preflight applies to synchronous and
asynchronous `publish_checked`.

If publication does proceed and returns `SentUnconfirmed`, ordinary publish
returns its receipt. `PublishReceipt::check_admission` then returns
`AdmissionCheckError::VisibilityUnavailable`. Checked publish returns
`CheckedPublishError::Admission` with the complete receipt and
`VisibilityUnavailable`. The send may already have caused an external effect:
reconcile by event ID before deciding whether to retry. Providers declaring
`Accepted` or stronger, and providers reporting per-destination admissions,
retain their existing acknowledgement behavior.

## Codec registration (core 0.18 to 0.19)

In core 0.19, `CodecRegistry::register` returns
`Result<(), CodecRegistrationError>` and rejects a duplicate payload type
without replacing its existing codec. Handle the result, and use
`CodecRegistry::replace` when replacement is intentional. This records the
0.18-to-0.19 transition documented in this repository's history; check the
provider's own compatibility notes before upgrading a provider.

## Encoded payload limits (core 0.16 to 0.17)

Core 0.17 replaced `with_max_encoded_payload_bytes(Option<NonZeroUsize>)` with
`with_payload_limits(PayloadLimits::new(publish_limit, receive_limit))`.
Both limits are positive; the then-current documented defaults were 1 MiB,
and there is no unlimited setting. Publication checks encoded bytes after
encoding and before provider admission; receive checks before codec callbacks.
These limits do not cap codec allocations or native Rust object memory. Check
the version-specific guide before carrying these values into another release.

## Facade header limits (core 0.21.1 and later)

Core 0.21.1 introduced finite facade header limits by default: at most 1,024
headers and 65,536 aggregate UTF-8 bytes across keys and values. Publications
outside either limit fail with `PublishError::Headers`; received deliveries stop
with `SubscriptionStopReason::Headers` before decoding or handler invocation.
`SubscriptionStopReason` is not `non_exhaustive`, so downstream exhaustive
matches must add a `Headers` arm. A fallback arm is recommended for future
compatibility.

Applications that require larger headers should set
`EventBusFacadeConfig::with_header_limits(HeaderLimits::new(count, bytes))` with
positive `NonZeroUsize` values. This facade boundary counts header text only. It
is distinct from `PayloadLimits` for encoded message bytes and from
Redis/provider transport limits: increasing Redis JSON or wire payload limits
does not raise the core header limit. Configure both layers if both boundaries
need adjustment. Repair incompatible producers or configure compatible limits,
then recreate stopped subscriptions. Durable recovery follows the provider
contract; local delivery is ephemeral and pending/unsettled work can be lost on
close/drop.

## Delivery metric snapshots after core 0.21.4

The current unreleased source tree makes `delivery_metrics()` snapshots
observational only. An invalid or incomparable clock sample leaves
`oldest_owned_age` unavailable; it does not stop receiving or emit a diagnostic.
This behavior is newer than the `v0.21.4` release, and no later release tag is
available to assign it a published version. Monitoring that previously treated
a metrics sampling failure as a subscription failure should instead inspect
`Subscription::terminal_failure()` (or the async `run()` result) for actual stop
causes, and treat a missing age as unavailable data. Snapshot collection can
then remain on the regular monitoring path without controlling subscription
lifecycle.

## Async zero-budget shutdown after core 0.21.4

The current development source documents the zero-budget boundary exercised by
the async facade regressions. Shutdown stops admission and signals active runners to stop first. Before
polling a non-empty receiver-close batch, it checks the call's deadline. If
registered receivers remain and a zero budget is already exhausted, that call
returns `ShutdownError::TimedOut` without starting their close operations. An
active runner that the application continues to poll can finish its receive loop
and close its own receiver after the shutdown call returns. With an empty receiver
batch, cleanup may proceed to provider shutdown, which can still time out if it
must wait. This source is newer than `v0.21.4`, and
there is no later release tag to assign it a published version. Apply this
guidance when using a release that contains the behavior; do not infer a
published version from the current source tree.

## Current upgrade guide

For changes accumulated since core 0.20, follow [Migrate from 0.20 to 0.21](user_guide.md#migrate-from-020-to-021). Earlier transitions are recorded in the sections above; verify the exact source and provider versions used by your application. Adopt the unreleased metric snapshot change only when upgrading to a release that contains it; do not infer a published version from the current source tree.
