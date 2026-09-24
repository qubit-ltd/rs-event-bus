# Qubit Event Bus 0.12 架构说明

[English 架构状态页](design.md) · [正式 SPI 设计（中文）](spi_design.zh_CN.md)

本文说明 0.12 代码中实际落地的架构。扩展能力和未来后端只有在明确标注“后续扩展”时才表示尚未实现；正式 SPI 契约及迁移细节见[正式 SPI 设计](spi_design.zh_CN.md)。

## 架构边界

```text
应用
  ├─ EventBus / AsyncEventBus：类型安全 facade、策略执行与生命周期
  ├─ model / codec / pipeline：事件模型、类型擦除、拦截与投递策略
  ├─ EventBusSpi / AsyncEventBusSpi：对象安全的最小传输契约
  └─ qubit-spi registry：provider 发现、选择、创建与创建期 fallback
       └─ LocalEventBusProvider（本 crate 内置的同步进程内实现）
```

应用通过 `Topic<T>`、`PublishRequest<T>` 和 `SubscribeRequest<T>` 操作总线，不需要直接处理 transport payload。Provider SPI 只负责发布、创建接收端、接收消息、结算和关闭，不接管 handler 或应用中间件。扩展 provider 可以位于独立 crate，但本 crate 当前只提供 local 同步 provider；任何其他后端都不得被误认为已随包发布。

## 发布与订阅

发布请求由 facade 校验并执行 publisher interceptors，再根据 provider capability、codec 和 payload mode 进行检查/转换，之后调用 SPI。`PublishReceipt` 说明 provider 对发布的确认以及实际使用的 provider ID，不代表 handler 已执行或完成。`publish_all` 按输入顺序独立提交请求并保留各项结果，不提供事务或回滚。

订阅建立后，provider receiver 由 facade 持有：同步 facade 为其管理 worker；异步 facade 返回由调用方 executor 驱动的 `AsyncSubscription::run`，本身不 spawn。异步消费仍是 runtime-neutral，但其 owned delivery future 使用 bus-wide `max_in_flight` 准入；不同 ordering key 可以并行，同一 key 保序。取消 `run` 只暂停并保留在途 future 与 permit；再次 `run` 会续跑旧任务，并用新 handler 处理新消息，bus shutdown 也可接管并收敛暂停 session。消费路径包括接收、gap/错误处理、解码、过滤、准入与 ordering、middleware、handler、重试、错误策略、死信和 settlement。idle wait、graceful deadline 和异步 retry 使用 `qubit-clock` 的 timer。`AsyncEventBus::new` 使用标准单调 timer，`with_timer` / `with_config_and_timer` 允许注入共享 `qubit_clock::Timer`，deadline 需要由其 future 在到期时唤醒 executor。

同步 `SubscriberInterceptor<T>` 与异步 `AsyncSubscriberInterceptor<T>` 都可保存在同一 `SubscribeOptions<T>` 中，但 facade 不会跨执行模型适配：sync bus 配置了 async middleware，或 async bus 配置了 sync middleware时，订阅配置会被拒绝。

## 标识与传输

`SubscriberId` 是经验证、可由调用方命名的逻辑订阅者 ID。每条订阅的内部对象 ID 为 `qubit_id::Id`，只在 bus 实例内关联 subscription、delivery 和诊断。`EventId` 则写入事件 envelope 和传输消息，默认使用 UUID v4，面向跨进程/跨后端传递与重放。它不能简化成内部递增 `u64`：后者无法提供跨进程唯一性，也无法在进程重启后保留事件身份。

Provider capability 显式描述 payload 模式、settlement、ordering、延迟、durability、consumer group、replay、发布保证与发布可见性。Registry 可要求 capability 并在创建阶段 fallback；backend 创建成功后，运行期发布、订阅、接收或结算错误不会静默切换 provider。能力声明不是自动获得的保证，具体 provider 必须如实实现并记录其边界。

## 结算和关闭

Sync/async SPI 都借用 `SettlementToken`。同一 token 与 disposition 重复结算必须幂等并返回一致结果；冲突 disposition 必须失败。Async settle future 取消后，facade 可用原 token 和相同 disposition 重试，provider 必须让执行中及完成后的请求均满足幂等合同。

丢弃 `AsyncSubscription::run` future 会暂停并保留 delivery task，后续 `run` 可续跑；丢弃 subscription handle 则会释放暂停的 session 与 receiver。provider 必须在 receiver 被丢弃时恢复未结算消息。需要确定性异步清理并读取 close 错误时，应调用 `AsyncSubscription::close().await`。

Facade shutdown 先停止准入和接收，再按所选模式协调 delivery、handler、settlement 和 receiver close，最终调用 SPI shutdown。`Immediate` 丢弃尚未开始的队列工作，但仍等待当前 handler/投递完成、close 和 SPI shutdown，以便完整返回错误；它不会强行终止用户代码。SPI 的 shutdown 方法只负责 provider 传输资源本身，不能替代 facade 的停机协调。`Graceful` 对 receiver close、活跃工作排空及 SPI shutdown 使用同一个总 deadline；超时后 bus 保持 Closing，可再次调用 shutdown 继续清理。异步 close/shutdown Future 被取消或超时并不代表 provider 副作用已回滚，因此 provider 必须支持幂等重试。同步 worker 中等待自身 shutdown 或 idle 会返回 `WouldDeadlock`。

## 发布失败上下文

配置的 publish error handler 在 SPI publish 直接失败（没有 retry policy）或重试达到终态失败时接收 `PublishFailureContext<T>`。该上下文通过 `Arc<T>` 共享 payload，并提供事件 ID、topic、headers、ordering key、timestamp 和 delay，因此 payload 不需要实现 `Clone`。请求构造、能力检查、编解码或 interceptor 等预检失败不会触发它。回调不能改变已结束的发布结果。

## 非目标

0.12 不内置 Tokio、crossbeam、flume、bus、RabbitMQ、Kafka 或 Redis provider，也不承诺持久投递、跨进程路由、事务批量发布或恰好一次。第三方 provider 可具备更强语义，但应通过 capability 和自身文档明确说明。`qubit-event-bus` 不重新导出 `qubit-retry`；高级 retry 配置由应用显式直接依赖 `qubit-retry`。
