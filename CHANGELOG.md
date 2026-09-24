# Changelog

[中文更新日志](CHANGELOG.zh_CN.md)

All notable changes to `qubit-event-bus` are documented here.

## 0.12.0 - 2026-09-24

### Breaking changes

- Replaced the former bus/factory API with typed `EventBus` and `AsyncEventBus` facades, request builders, and object-safe provider SPI contracts.
- Added `qubit-spi` provider registries and the built-in synchronous local provider. Third-party transport adapters are not bundled.
- Split transport settlement from application acknowledgement and made repeated settlement with the same token/disposition idempotent by contract.
- Added provider capability declarations, creation-time capability validation/fallback, typed event codecs, delivery diagnostics, and explicit shutdown modes.
- Retry APIs use `qubit-retry` types directly. Applications configuring retry policies must depend on `qubit-retry`; event-bus does not re-export its members.
- `EventId` is a portable validated identifier generated as UUID v4 by default; subscription object IDs use `qubit_id::Id` and are bus-local.
- Publish receipts report provider admission, not subscriber completion; `publish_all` remains best-effort and non-atomic.

### Migration

- Replace old `LocalEventBus` / factory usage with `EventBus::local(LocalEventBusConfig::default())` or create through `EventBusRegistry`.
- Build `PublishRequest<T>` and `SubscribeRequest<T>` directly; handlers receive `Delivery<T>`.
- Implement `EventBusSpi` or `AsyncEventBusSpi` for a backend and register its `qubit-spi` provider definition. No broker adapter is included in this release.
- See the [user guide](doc/user_guide.md), [Chinese user guide](doc/user_guide.zh_CN.md), and [formal SPI design](doc/spi_design.zh_CN.md) for behavioral and migration details.

## 0.11.0 - 2026-09-21

### Breaking changes

- Removed `TransactionalEventBus`, `TransactionalPublisher`, `StagedEvent`, and `StagedEventEnvelope`.
- `publish_all` is best-effort and non-atomic.
- `EventBus` uses a backend-owned associated `Subscription<T>`.
- `BatchPublishResult::accepted_count()` counts items with at least one accepted subscriber admission.
- `DeliveryLimits` separates in-flight admission from executor queue capacity.

### Migration

- Replace transactional publication only where non-atomic best-effort delivery is acceptable.
- Use `B::Subscription<T>` in generic code.
- Inspect per-subscriber receipt statuses together with `failure_count()`.
