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

## 当前升级指南

从 core 0.20 升级时，请查看[从 0.20 迁移到 0.21](user_guide.zh_CN.md#从-020-迁移到-021)。
更早的变更见上文；请确认应用实际使用的 core 与 provider 版本。
