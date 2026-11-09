# Migration guide

[简体中文](migration.zh_CN.md) · [User guide](user_guide.md)

This page is a stable entry point for core API changes. It summarizes selected
historical transitions and links to the current upgrade guide; it does not
claim that older provider releases are compatible with the current core.

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

## Current upgrade guide

For changes accumulated since core 0.20, follow [Migrate from 0.20 to 0.21](user_guide.md#migrate-from-020-to-021). Earlier transitions are recorded in the sections above; verify the exact source and provider versions used by your application.
