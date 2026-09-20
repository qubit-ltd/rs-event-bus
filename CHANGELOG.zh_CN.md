# 更新日志

[Changelog](CHANGELOG.md)

本文档记录 `qubit-event-bus` 的重要变更。

## 0.11.0 - 2026-09-21

### 破坏性变更

- 删除 `TransactionalEventBus`、`TransactionalPublisher`、`StagedEvent` 和 `StagedEventEnvelope`。
- `publish_all` 采用 best-effort、非原子语义。
- `EventBus` 使用由后端拥有的关联类型 `Subscription<T>`。
- `BatchPublishResult::accepted_count()` 统计至少有一个订阅者准入成功的输入项。
- `DeliveryLimits` 将 in-flight 准入上限与 executor 队列容量分开配置。

### 迁移

- 只有能接受非原子 best-effort 投递时，才以普通发布替换事务发布。
- 泛型代码改用 `B::Subscription<T>`。
- 将各订阅者的回执状态与 `failure_count()` 结合检查。
