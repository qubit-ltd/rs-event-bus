# 迁移指南

[English](migration.md) · [用户指南](user_guide.zh_CN.md)

本文是核心库迁移说明的稳定入口，概述部分历史 API 变更并链接当前升级指南；
不表示较早发布的 provider 与当前 core 兼容。

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

## Headers 边界与后续不兼容版本

后续不兼容 core 版本计划按 0.22 处理；本说明不代表发布或宣布该版本。默认 facade headers 限额将为最多 1,024 项，且 key/value UTF-8 累计最多 65,536 字节。超限发布返回 `PublishError::Headers`；接收投递会在解码或调用 handler 前以 `SubscriptionStopReason::Headers` 停止。`SubscriptionStopReason` 当前没有 `non_exhaustive` 标记，因此适用的下游穷尽匹配必须增加 `Headers` 分支。建议保留 fallback arm 以兼容未来变更。

需要更大 headers 的应用可通过 `EventBusFacadeConfig::with_header_limits(HeaderLimits::new(count, bytes))` 设置正数 `NonZeroUsize`。此 facade 边界只计算 header 文本，与编码消息字节使用的 `PayloadLimits`、Redis/provider transport 限额相互独立：提高 Redis JSON 或 wire payload 限额不会提高 core header 限额。需要时应分别配置两层。修复不兼容生产方或设置兼容限额后，重新创建已停止订阅。持久恢复遵循 provider 契约；local 投递是 ephemeral，close/drop 可能丢失排队或未结算消息。

## 当前升级指南

从 core 0.20 升级时，请查看[从 0.20 迁移到 0.21](user_guide.zh_CN.md#从-020-迁移到-021)。
更早的变更见上文；请确认应用实际使用的 core 与 provider 版本。
