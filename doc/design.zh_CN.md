# Qubit Event Bus 0.14 架构说明

[Architecture status (English)](design.md) · [正式 SPI 设计（中文）](spi_design.zh_CN.md) · [SPI design (English)](spi_design.md)

本文说明 0.14 代码中实际落地的架构。扩展能力和未来后端只有在明确标注“后续扩展”时才表示尚未实现；正式 SPI 契约及迁移细节见[正式 SPI 设计](spi_design.zh_CN.md)。

## 架构边界

```text
应用
  ├─ EventBus / AsyncEventBus：类型安全 facade、策略执行与生命周期
  ├─ model / codec / pipeline：事件模型、类型擦除、拦截与投递策略
  ├─ EventBusSpi / AsyncEventBusSpi：对象安全的最小传输契约
  └─ qubit-spi registry：provider 发现、选择、创建与创建期 fallback
       └─ LocalEventBusProvider（本 crate 内置的同步进程内实现）
```

应用通过 `Topic<T>`、`PublishRequest<T>` 和 `SubscribeRequest<T>` 操作总线，不需要直接处理 transport payload。Provider SPI 只负责发布、创建接收端、接收消息、结算和关闭，不接管 handler 或应用中间件。本 crate 提供同步和异步 local provider；任何其他后端都不得被误认为已随包发布。

## 启动装配与 provider 目录

启用 `discovery` feature 后，应用在装配模块显式链接外部 provider crate（`use provider_crate as _;`），再调用 `EventBusRegistry::discover()` 收集已链接的同步提交。内置 `local` 已提交到同步目录。启动代码设置默认 `ProviderSelection`、调用 `seal()` 固定目录，再通过 `create(&EventBusConfig::default())` 创建一条总线。应用持有总线，并将 `bus.clone()` 交给业务服务；订阅句柄由应用保存，在停机时取消订阅并关闭总线。

`AsyncEventBusRegistry::discover()` 使用独立的异步目录，其 provider 的 `create(...).await` 在创建阶段运行。同步 local 和异步 local 分别注册在独立 catalog，均使用 provider ID `local`。未启用 `discovery` 时，两个 registry 仍可通过 `with_local()` 或 `register()` 显式注册。发现只收集 provider 定义，能力校验与按策略回退发生在创建阶段；运行期故障不会触发切换。

local provider 在同名 Topic 的活跃订阅期间绑定唯一的原生 Rust payload 类型。每个订阅的队列容量同时覆盖排队和未 settlement 的投递，Retry 会保留原有额度。同步 `EventBus::wait_for_idle` 查询 provider 队列及结算状态；`wait_for_received_deliveries` 只等待当前 facade 已接收的工作。异步 facade 只提供后一种保证。

## Local provider 的路由与资源

0.14 实现中的 `BusState` 按 Topic 索引活跃订阅。发布时只快照目标 Topic 的队列，并按订阅 ID 顺序处理；释放全局状态锁后才访问各队列锁。idle wait 也只查询对应 Topic，shutdown 则快照全部活跃队列。这种分桶方式减少了发布和 idle 检查对无关 Topic 的扫描，不改变 SPI 契约。

每个订阅持有自己的有界队列。配置容量同时计入排队消息和已接收但尚未 settlement 的消息；重试复用原来的容量占用，不会额外增加配额。默认每订阅者上限为 1024，单个 provider 实例的总投递上限默认为 65,536 条；总量限额只计算接纳的投递条数，不计算 payload 字节，满额时对应目标会被拒绝。0.14 内部按 ordering key 维护 FIFO lane，轮转调度已就绪的队首，并用带版本号的最小堆排列延迟队首。同一 lane 的延迟队首会阻塞其后消息，其他已就绪 lane 仍可继续。过期的延迟堆条目数量超过活跃队首数与固定余量 8 中的较大值时会重建堆，从而限制过期调度元数据的增长，同时保留高效的延迟队首查询。这些索引增加了维护成本，换取避免在很长的阻塞队列前缀上反复扫描。

接收 SPI 仍是阻塞式：每个同步订阅都会启动一个接收 worker 线程。在一台 6 CPU Linux 主机的样本中，1/16/128 个订阅对应的进程线程峰值为 6/21/133；创建耗时中位数为 0.226/1.592/7.551 ms，取消订阅并立即关闭的耗时中位数为 0.285/651.543/5359.302 ms。关闭耗时范围较大，测量时主机也有其他负载；这些数值仅描述一次观测，可运行 `cargo bench --bench local_threads` 重新测量。

本次保留 Topic 分桶和索引队列，是基于本机对比结果做出的实现选择，并非服务等级保证。运行 `cargo bench --bench local_scale -- publish` 时，两个“32 个 Topic × 16 个订阅”场景的 p95 改善 70.8% 和 67.2%；“1 个 Topic × 1 个订阅”改善 4.6%；一次“1 个 Topic × 128 个订阅”样本则出现 21.3% 的 p95 退化。对于 depth-1024 队列中“长阻塞前缀 + 就绪后缀”的接收负载，`cargo bench --bench local_scale -- receive` 测得 16 个就绪 key 的中位 p95 从 42,654 ns 降至 590 ns，1 个就绪 key 从 41,590 ns 降至 539 ns。这些是特定主机上的对比结果，不能直接推断其他机器或负载。local provider 只在进程内工作，不持久化消息；本 crate 同时内置 sync 和 async local provider。同步 provider 每个订阅使用一个阻塞接收线程，异步 provider 使用 waker 等待且不为每个订阅创建接收线程。

## 发布与订阅

发布请求由 facade 校验并执行 publisher interceptors，再根据 provider capability、codec 和 payload mode 进行检查/转换，之后调用 SPI。`PublishReceipt` 说明 provider 对发布的确认以及实际使用的 provider ID，不代表 handler 已执行或完成。`DestinationAdmissions` 可能为空，也可能包含 accepted、filtered 和 rejected 目的地；部分拒绝仍是成功回执，重发整条事件可能让已接纳目的地重复收到消息。`publish_all` 按输入顺序独立提交请求并保留各项结果，不提供事务或回滚。

订阅建立后，provider receiver 由 facade 持有：同步 facade 为其管理 worker；异步 facade 返回由调用方 executor 驱动的 `AsyncSubscription::run`，本身不 spawn。异步消费仍是 runtime-neutral，但其 owned delivery future 使用 bus-wide `max_in_flight` 准入；不同 ordering key 可以并行，同一 key 保序。取消 `run` 只暂停并保留在途 future 与 permit；再次 `run` 会续跑旧任务，并用新 handler 处理新消息，bus shutdown 也可接管并收敛暂停 session。消费路径包括接收、gap/错误处理、解码、过滤、准入与 ordering、middleware、handler、重试、错误策略、死信和 settlement。idle wait、graceful deadline 和异步 retry 使用 `qubit-clock` 的 timer。`AsyncEventBus::from_spi` 使用标准单调 timer，`with_timer` / `with_config_and_timer` 允许注入共享 `qubit_clock::Timer`，deadline 需要由其 future 在到期时唤醒 executor。

同步 `SubscriberInterceptor<T>` 与异步 `AsyncSubscriberInterceptor<T>` 都可保存在同一 `SubscribeOptions<T>` 中，但 facade 不会跨执行模型适配：sync bus 配置了 async middleware，或 async bus 配置了 sync middleware时，订阅配置会被拒绝。

## 标识与传输

`SubscriberId` 是经验证、可由调用方命名的逻辑订阅者 ID。每条订阅的内部对象 ID 为 `qubit_id::Id`，只在 bus 实例内关联 subscription、delivery 和诊断。`EventId` 则写入事件 envelope 和传输消息，默认使用 UUID v4，面向跨进程/跨后端传递与重放。它不能简化成内部递增 `u64`：后者无法提供跨进程唯一性，也无法在进程重启后保留事件身份。

Provider capability 显式描述 payload 模式、settlement、ordering、延迟、durability、consumer group、replay、发布保证与发布可见性。Registry 可要求 capability 并在创建阶段 fallback；backend 创建成功后，运行期发布、订阅、接收或结算错误不会静默切换 provider。能力声明不是自动获得的保证，具体 provider 必须如实实现并记录其边界。

顺序是 provider 契约。请求 `OrderingPolicy::PerKey` 的订阅只有在 provider 声明 `OrderingCapability::PerKey` 或更强的 `PerSubscription` 时才会建立；facade 会在调用 provider 的 `subscribe` 之前检查。旧订阅 priority 已删除，因为 facade 和 provider 都没有定义它的调度语义。

`PublishReceipt::check_admission` 仅按当前回执报告的准入结果检查“至少一个目的地已接纳”或“至少一个已接纳且没有拒绝”这两种要求；它不会产生新的发布副作用，也无法撤销或重试原发布。每个 facade 的 `PublishMetricsSnapshot` 统计公开 publish 调用次数、错误、拦截器丢弃、不可见目的地的接纳、零目的地回执，以及 provider 报告的已接纳/过滤/拒绝目的地数。各字段独立读取，因此并发快照不保证来自同一个瞬间；这些计数均不表示 handler 已完成。同步 `Subscription` 句柄丢弃时不会取消订阅，调用方必须显式 `cancel()` 或关闭总线。
`PublishReceipt::admission_outcome` 统一分类不公开目的地的接纳、目的地接纳、部分接纳、无目的地接纳、空目的地快照和拦截器丢弃。它只描述已经返回的回执，不表示 handler 结果，也不是重试指令。需要逐个订阅者身份与拒绝原因时，读取 `acknowledgement()`。

每个同步 local 订阅都会占用一个阻塞式接收 worker；async local 使用 waker 和 timer future，不按订阅数增加接收线程。同步 provider 的一次 Linux 样本在 128 个订阅时观测到 133 个进程线程，取消订阅并立即关闭的中位数为 5.36 秒。相同主机对异步 provider 进行七次测量，128 个空闲订阅的进程线程为 1，关闭 p95 为 0.085 毫秒。它们仅是特定主机上的观察结果，不是容量保证。异步 provider 的 close/drop 会丢弃未结算消息，同 ID 重订阅会获得空队列；这符合 ephemeral 语义，不代表向 durable provider 确认了消息。

## 结算和关闭

Sync/async SPI 都借用 `SettlementToken`。同一 token 与 disposition 重复结算必须幂等并返回一致结果；冲突 disposition 必须失败。Async settle future 取消后，facade 可用原 token 和相同 disposition 重试，provider 必须让执行中及完成后的请求均满足幂等合同。

丢弃 `AsyncSubscription::run` future 会暂停并保留 delivery task，后续 `run` 可续跑；丢弃 subscription handle 则会释放暂停的 session 与 receiver。内置 async local provider 在 receiver close/drop 时会丢弃 pending 和 in-flight 队列消息。需要确定性异步清理并读取 close 错误时，应调用 `AsyncSubscription::close().await`。

Facade shutdown 先停止准入和接收，再按所选模式协调 delivery、handler、settlement 和 receiver close，最终调用 SPI shutdown。`Immediate` 丢弃尚未开始的队列工作，但仍等待当前 handler/投递完成、close 和 SPI shutdown，以便完整返回错误；它不会强行终止用户代码。同步 graceful shutdown 使用每个 bus 唯一的后台协调者：调用方 deadline 覆盖已准入的 publish/subscribe、worker drain 与 close、以及 provider shutdown。返回 `TimedOut` 后 bus 保持 Closing 并拒绝新操作，后台协调者继续清理；再次调用可等待结果，或用 `Immediate` 加强当前尝试。阻塞的同步 SPI 方法或 handler 可能让协调线程持续存在。异步 deadline 使用 `qubit-clock` timer；丢弃 async shutdown future 可能让 provider 副作用继续进行，因此 provider 必须支持幂等重试。SPI 的 shutdown 方法只负责 provider 传输资源本身，不能替代 facade 的停机协调。同步 worker 中等待自身 shutdown 或 idle 会返回 `WouldDeadlock`。

## 发布失败上下文

配置的 publish error handler 在 SPI publish 直接失败（没有 retry policy）或重试达到终态失败时接收 `PublishFailureContext<T>`。该上下文通过 `Arc<T>` 共享 payload，并提供事件 ID、topic、headers、ordering key、timestamp 和 delay，因此 payload 不需要实现 `Clone`。请求构造、能力检查、编解码或 interceptor 等预检失败不会触发它。回调不能改变已结束的发布结果。

## 非目标

0.14 不内置 Tokio、crossbeam、bus、RabbitMQ、Kafka 或 Redis provider；flume 仅用于开发期同步 SPI conformance 夹具，不是生产 adapter。本 crate 不承诺持久投递、跨进程路由、事务批量发布或恰好一次。第三方 provider 可具备更强语义，但应通过 capability 和自身文档明确说明。`qubit-event-bus` 不重新导出 `qubit-retry`；高级 retry 配置由应用显式直接依赖 `qubit-retry`。
