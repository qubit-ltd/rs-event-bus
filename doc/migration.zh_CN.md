# 迁移指南

[English](migration.md) · [用户指南](user_guide.zh_CN.md)

本文是核心库迁移说明的稳定入口，概述部分历史 API 变更并链接当前升级指南；
不表示较早发布的 provider 与当前 core 兼容。

## 未确认的发布回执（v0.21.4 之后的源码变更）

core v0.21.4 之后的源码变更在 `PublishAcknowledgement` 和
`AdmissionOutcome` 中加入 `SentUnconfirmed`。声明 `FireAndForget` 的 provider
应返回该变体，而不是旧版 `Accepted`；facade 也会归一化旧回执。对于 opaque provider，
要求逐目标可见性的 checked publish 会在发布前以
`CheckedPublishError::UnsupportedVisibility` 失败；不会生成回执，也不会产生发布副作用。
同步和异步 `publish_checked` 都会进行此预检。

如果发布已经执行并返回 `SentUnconfirmed`，普通发布会返回该回执。
`PublishReceipt::check_admission` 随后返回
`AdmissionCheckError::VisibilityUnavailable`。checked publish 则以
`CheckedPublishError::Admission` 返回完整回执和 `VisibilityUnavailable`。发送可能已经产生
外部副作用；决定是否重试前，应先按事件 ID 对账。声明 `Accepted` 或更强保证的 provider，
以及返回逐目标接纳结果的 provider，行为保持不变。

## Codec 注册（core 0.18 到 0.19）

从 core 0.19 起，`CodecRegistry::register` 返回
`Result<(), CodecRegistrationError>`；同一载荷类型重复注册会报错，并保留原有
codec。请处理返回结果；确实要替换时使用 `CodecRegistry::replace`。这是根据本仓库
历史记录的 0.18 到 0.19 变更；升级 provider 时还应核对其自身的兼容说明。

## 编码载荷限额（core 0.16 到 0.17）

Core 0.17 将 `with_max_encoded_payload_bytes(Option<NonZeroUsize>)` 替换为
`with_payload_limits(PayloadLimits::new(publish_limit, receive_limit))`。两个限额都必须
为正数；当时文档记录的默认值均为 1 MiB，且不再提供无限模式。发布端在编码完成后、
provider 接纳前检查；接收端在调用 codec 回调前检查。这些限额不限制 codec 自身分配或
原生 Rust 对象内存。跨版本升级时，请核对对应版本指南中的值。

## Facade headers 限额（core 0.21.1 及以后）

Core 0.21.1 引入默认有限的 facade headers 限额：最多 1,024 项，key/value 的
UTF-8 累计最多 65,536 字节。超限发布返回 `PublishError::Headers`；接收投递会在
解码或调用 handler 前以 `SubscriptionStopReason::Headers` 停止。
`SubscriptionStopReason` 没有 `non_exhaustive` 标记，因此下游穷尽匹配必须增加
`Headers` 分支。建议保留 fallback arm 以兼容未来变更。

需要更大 headers 的应用可通过
`EventBusFacadeConfig::with_header_limits(HeaderLimits::new(count, bytes))` 设置正数
`NonZeroUsize`。此 facade 边界只计算 header 文本，与编码消息字节使用的
`PayloadLimits`、Redis/provider transport 限额相互独立：提高 Redis JSON 或 wire
payload 限额不会提高 core header 限额。需要时应分别配置两层。修复不兼容生产方或
设置兼容限额后，重新创建已停止订阅。持久恢复遵循 provider 契约；local 投递是
ephemeral，close/drop 可能丢失排队或未结算消息。

## Core 0.21.4 之后的投递指标快照

当前尚未发布的源码树使 `delivery_metrics()` 快照仅用于观察。时钟样本无效或不可比较
时，`oldest_owned_age` 保持不可用；这不会停止接收或发出诊断。该行为晚于
`v0.21.4`，目前没有更晚的发行标签可将其标记为已发布版本。若监控此前把指标采样失败
视为订阅失败，应改为通过 `Subscription::terminal_failure()`（异步则检查 `run()` 结果）
判断真实停止原因，并把年龄缺失视为数据不可用。指标采样可以留在常规监控路径中，
不再控制订阅生命周期。

## Core 0.21.4 之后的异步零预算停机

当前开发源码记录了异步 facade 回归测试覆盖的零预算边界：停机先关闭新接纳并通知
活动 runner 停止。轮询非空 receiver 关闭批次前，会检查当前调用的 deadline。若仍有
已登记 receiver 且零预算已耗尽，该调用会返回 `ShutdownError::TimedOut`，不会启动
receiver close。应用继续轮询活动 runner 时，runner 可在 shutdown 调用返回后结束接收并
关闭自己的 receiver。若 receiver 批次为空，清理可继续进入 provider shutdown，但需要
等待时仍可能超时。该源码晚于
`v0.21.4`，目前没有更晚的发行标签可将其标记为已发布版本。使用包含此行为的发行版时
再应用这条说明；不要仅凭当前源码树推断已发布版本。

## 当前升级指南

从 core 0.20 升级时，请查看[从 0.20 迁移到 0.21](user_guide.zh_CN.md#从-020-迁移到-021)。
更早的变更见上文；请确认应用实际使用的 core 与 provider 版本。指标快照行为的
未发布变化需在升级到包含它的发行版时采用；不要仅凭当前源码树推断已发布版本。
