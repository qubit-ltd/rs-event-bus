# 事件总线设计说明

## 范围

`qubit-event-bus` 是进程内的类型化分发层。`LocalEventBus` 管理生命周期和运行时状态；`LocalEventBusFactory` 管理默认值及不可变配置，并将配置复制到新建的 bus。实现有意不持久化事件、不协调进程间投递，也不提供事务发布。

## 分发路径

```text
publisher
  -> EventEnvelope<T>
  -> 发布拦截器（typed，再 global）
  -> 订阅快照
  -> 过滤与投递准入
  -> PublishReceipt / BatchPublishResult
  -> 本地 worker 池
  -> 订阅拦截器
  -> handler + ACK/NACK
  -> 重试 / 错误处理器 / 死信
  -> DeliveryFailure 观察器
```

准入和执行是两个阶段。回执中的 `Accepted` 表示投递任务取得准入并已提交，不表示 handler 已执行或成功。被拒绝的任务会体现在回执中，也可以通过错误观察器观测。best-effort 批量路径保留输入顺序，并记录每个事件的全局错误，不会回滚此前已提交的事件。

## 类型边界

`Topic<T>` 将 Topic 名称与 payload 类型绑定。`EventEnvelope<T>` 携带 payload 和元数据。`EventBus` trait 通过关联类型 `Subscription<T>: SubscriptionHandle<T>` 明确句柄由后端拥有；本地固有方法返回具体的本地订阅句柄。

`PublishReceipt` 包含输入事件 ID、拦截器执行后的分发 ID，以及 `Dropped` 或 `SubscriberDispatchResult` 列表。`BatchPublishResult::accepted_count()` 统计至少有一个订阅状态为 accepted 的项；`failure_count()` 包含全局错误和任一订阅被拒绝的项。因此一个项可以同时计入两个数量，这属于设计语义。

## 容量模型

`DeliveryLimits` 分离两个控制项：

| 控制项 | 作用 |
| --- | --- |
| `max_in_flight` | 本地运行时同时持有准入订阅投递的最大数量。 |
| `handler_queue_capacity` | 传给 handler executor 队列的可选容量上限。 |

`DeliveryLimits::bounded` 同时配置两个值，`DeliveryLimits::unbounded` 则为指定的 in-flight 上限保留不设界的 executor 队列。零值会被拒绝。`Default` 使用 `DEFAULT_MAX_IN_FLIGHT_DELIVERIES`（4096），且不显式限制 handler 队列。`LocalEventBusFactory::set_delivery_limits` 会校验该值，并将其复制到新建 bus 的运行时配置中。

准入 permit 在调度前取得，在处理进入终态路径时释放。因此背压会在准入阶段可见，而发布者仍无需同步等待 handler 完成。

## 顺序、延迟和重试

带 `ordering_key` 的 envelope 选择由 Topic、顺序键和订阅 ID 组成的 lane；同一 lane 中的工作串行执行。没有顺序键的工作直接提交，可以并发执行。延迟工作在 handler worker 之外等待；如果到期时队列准入失败，则跳过 handler 并报告 `ExecutionRejected`。

重试策略在发布或 handler 执行路径中同步运行，因此退避会占用当前线程和顺序位置。取消重试会阻止下一次尝试并唤醒退避，但不能打断已经运行的同步 handler。

## 确认和终态处理

自动确认在 handler 成功返回后完成。手动模式要求 handler 在返回前 ACK 或 NACK。缺少确认决策和显式 NACK 都属于失败；随后由重试分类决定是否再次尝试，重试结束后运行错误处理器和死信策略。该流程完成后发出终态 `DeliveryFailure`。

## 生命周期不变量

运行时有 stopped、starting、started 和 stopping 边界。关闭开始后拒绝新注册，旧工作排空前拒绝重新启动。从同一个 bus 的订阅 worker 调用阻塞式 shutdown 会造成死锁，因此 `wait_for_idle` 会检测该情况；handler 中应使用非阻塞或带超时的关闭方法。

## 配置归属

Factory 按类型保存发布/订阅选项和死信策略的默认值，并提供面向元数据的全局拦截器及死信处理。拦截器在创建时复制到 bus，运行时不提供修改入口；这样可以保持分发快照稳定，同时仍可通过 bus API 注册运行时错误观察器。

## 非目标和扩展点

本 crate 不承诺持久投递、跨进程路由、打断 handler 或批量原子性。其他后端可以实现 `EventBus` 并提供更强保证，但必须由后端文档明确说明。需要事务的应用应由自己的事务管理器或具体后端协调发布。
