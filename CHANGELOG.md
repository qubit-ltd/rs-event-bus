# Changelog

[中文更新日志](CHANGELOG.zh_CN.md)

All notable changes to `qubit-event-bus` are documented here.

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
