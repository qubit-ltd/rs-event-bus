# 更新日志

[Changelog](CHANGELOG.md)

本文档记录 `qubit-event-bus` 的重要变更。

## 未发布

### 破坏性变更

- 将 `Topic::with_codec` 重命名为 `Topic::new_with_codec`，将 `Topic::with_shared_codec` 重命名为 `Topic::new_with_shared_codec`。

### 新增

- 为 `Topic`、`SubscriberId`、`ProviderId` 和 `SchemaId` 增加 const `new_static` 构造函数；`EventId` 保持不变。

## 0.12.0 - 2026-09-24

### 破坏性变更

- 删除订阅 priority 选项和 builder 方法；它们从未影响投递调度。`PerKey` 订阅现在要求 provider 声明 `PerKey` 或 `PerSubscription` 顺序能力。
- 以类型化的 `EventBus`、`AsyncEventBus` facade、request builder 和对象安全 provider SPI 替代旧 bus/factory API。
- 引入 `qubit-spi` provider registry 和内置同步 local provider；本 crate 不附带第三方传输适配器。
- 将应用层 acknowledgement 与传输层 settlement 分开；同一 token 和 disposition 的重复 settlement 按契约幂等。
- 增加 provider capability 声明、创建时能力校验与 fallback、类型化事件 codec、投递诊断和显式 shutdown mode。
- retry API 直接使用 `qubit-retry` 类型。配置重试策略的应用必须自行依赖 `qubit-retry`；event-bus 不重新导出其成员。
- `EventId` 是可跨 provider 传输的已验证标识，默认以 UUID v4 生成；subscription 对象 ID 使用仅在 bus 内有效的 `qubit_id::Id`。
- Publish receipt 表示 provider 准入而不是订阅处理完成；`publish_all` 仍为 best-effort、非原子操作。

### 迁移

- 从订阅 options/builder 中移除 priority 设置。保留同步 `Subscription` 句柄并显式调用 `cancel()`；丢弃句柄不会取消 worker。使用 `PublishReceipt::check_admission` 检查已返回的回执，使用 `publish_metrics()` 查看 provider 准入计数；两者都不代表 handler 已完成。
- 将旧 `LocalEventBus` / factory 用法迁移到 `EventBus::local(LocalEventBusConfig::default())`，或通过 `EventBusRegistry` 创建。
- 使用 `PublishRequest<T>` 和 `SubscribeRequest<T>` 构造请求；handler 接收 `Delivery<T>`。
- 后端实现 `EventBusSpi` 或 `AsyncEventBusSpi` 并注册 `qubit-spi` provider definition。本版本没有随 crate 提供 broker adapter。
- 行为与迁移细节见[中文用户指南](doc/user_guide.zh_CN.md)、[英文用户指南](doc/user_guide.md)和[正式 SPI 设计](doc/spi_design.zh_CN.md)。

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
