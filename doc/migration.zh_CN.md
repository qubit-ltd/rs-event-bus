# 迁移指南

本文说明如何从 `qubit-event-bus` 0.14 或 0.15 升级到 0.16.0。0.16 是
破坏性版本；应用和 provider 实现需要一起升级。

## 更新依赖

```toml
qubit-event-bus = "0.16"
```

provider crate 也应依赖对应的 SPI 版本。部署前，使用 0.16 运行 provider
自己的 conformance 测试。

## 更新死信策略构造函数

这些构造函数已重命名，以明确表达接纳保证：

| 旧调用 | 0.16 调用 |
| --- | --- |
| `DeadLetterPolicy::topic(name)` | `DeadLetterPolicy::with_topic_name(name)` |
| `DeadLetterPolicy::known_destination(name)` | `DeadLetterPolicy::with_known_destination(name)` |

`with_topic` 和 `with_admission` 仍可使用。只有 provider 能报告目标接纳情况时，
才选择 known-destination 策略；opaque acknowledgement 无法满足这一要求。

## 更新 conformance hooks

`ConformanceHooks` 和 `AsyncConformanceHooks` 新增了 `ephemeral_cleanup`。
部分结构体初始化应补上 `..Default::default()`，或显式初始化全部 hook。Strict
模式下，durable provider 缺少恢复 fixture、ephemeral provider 缺少清理 fixture
都会失败；跳过的用例不代表通过。

同步 conformance runner 会把 future cancellation 检查标记为 `NotApplicable`，
因为同步 API 没有可取消的 future。请继续单独测试 receiver close、shutdown、重复
settlement 和 provider 恢复。异步 provider 对可取消的操作仍需保留由 barrier 控制的
真实测试 fixture。

## 重新核对消息恢复假设

Durable 订阅在 receiver close 或 drop 后，必须保留已接纳但尚未终结的消息，使其
能够恢复。Ephemeral 订阅在 receiver 销毁时可以丢弃未结算消息。两种清理行为都不
会隐式确认消息。取消 receive future 与销毁 receiver 不同：如果消息已被消费，之后
的 receive 或 provider 恢复流程仍必须能取回它。请根据 provider 实际的 durability
和 settlement 能力，复核 shutdown 与死信失败处理。

完整契约和可运行示例见[用户手册](user_guide.zh_CN.md)、[设计文档](design.zh_CN.md)
和 [API 文档](https://docs.rs/qubit-event-bus/0.16.0/qubit_event_bus/)。
