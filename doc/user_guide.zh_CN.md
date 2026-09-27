# Qubit Event Bus 用户手册

[中文 README](../README.zh_CN.md) · [English user guide](user_guide.md) · [API 文档](https://docs.rs/qubit-event-bus)

本文适用于 `qubit-event-bus` 0.14.0，要求 Rust 1.94 或更高版本。它面向在 Rust 应用中需要让多个模块响应同一业务事件的开发者；编写底层传递实现的开发者只需查阅[自己开发一种传递实现](#自己开发一种传递实现)。读到[检查发布结果](#检查发布结果)，就能在项目中接入内置的进程内事件总线；后面章节供你按需查阅消息元数据、顺序保证、失败处理、配置、异步用法和第三方实现。

## 它解决什么问题

以一个订单服务为例。订单事务提交后，系统还有几项后续工作：写审计记录、更新供客服查询的客户订单视图；以后可能还要发送通知、同步数据仓库。如果由订单服务逐个调用这些模块，订单模块就要依赖每一个下游模块，每增加一项后续工作都要改动下单流程，还要在下单的请求路径上处理各个下游的失败和延迟。

引入事件总线后，订单服务在事务提交后只做一件事：向主题 `orders.created` 发布一个 `OrderCreated` 事件，携带订单号、客户号和金额。审计模块和客户视图模块在应用启动时各自订阅这个主题，收到事件后独立处理。发布方和订阅方之间只共享事件类型，互不依赖；以后增加新的消费者，也无须改动订单服务。

本库通过统一的发布和订阅接口，把业务代码与消息的实际传递方式隔开：搭配不同的后端实现，可以得到进程内、跨进程或跨节点的事件总线。本库自带的 `local` 实现只在**同一个程序进程内**传递消息；如果程序在订单提交后、消息发出前退出，或者在处理过程中退出，消息不会被自动补发。审计等重要工作需要可靠完成时，仍要由应用自行设计持久记录和补偿机制。需要跨进程传递时，见[选择和接入第三方实现](#选择和接入第三方实现)；本手册也会在相关步骤说明这一边界。

## 从哪里开始

1. 在[接入订单服务](#接入订单服务)中了解事件定义、订阅和发布三步。
2. 在[检查发布结果](#检查发布结果)看到成功时的回执长什么样，并理解“消息已被接纳”和“业务已做完”的区别。到这里，基础接入完成。
3. 按需要查[按需补充消息信息](#按需补充消息信息)、[保证同一对象的处理顺序](#保证同一对象的处理顺序)、[处理失败和重试](#处理失败和重试)、[配置内置 local 事件总线](#配置内置-local-事件总线)、[异步用法](#异步总线与订阅)或[接入第三方实现](#选择和接入第三方实现)。

第三方实现、编码和内部扩展接口放在基础用法之后，按需查阅。

## 接入订单服务

在依赖中加入：

```toml
[dependencies]
qubit-event-bus = "0.14"
```

下面沿用前面的订单场景。订单、审计、客户视图分属应用的不同模块，数据库访问对象由应用注入；`OrderRepository`、`AuditStore` 和 `CustomerViewStore` 代表应用连接实际存储的接口。接入分三步：定义共用的事件，在启动时注册两个订阅模块，在订单事务提交后发布事件。

### 定义共用的事件数据

```rust
// src/orders/events.rs
use qubit_event_bus::model::Topic;

pub struct OrderCreated {
    pub order_id: String,
    pub customer_id: String,
    pub total_cents: u64,
}

impl OrderCreated {
    pub const TOPIC: Topic<Self> = Topic::new_static("orders.created");
}
```

`OrderCreated` 是事件的载荷类型，`orders.created` 是它的主题（`Topic`）。`Topic<T>` 带有载荷类型参数，发布方和订阅方共用同一个 `Topic<OrderCreated>`，载荷类型在编译期即可校验。主题名在代码中固定时用 `Topic::new_static`；来自配置时用 `Topic::<T>::new(name)?`，它会校验名称是否合法。修改事件字段或语义时，需同时评估所有订阅方是否兼容。

### 注册两个处理模块

```rust
// src/audit.rs
use std::sync::Arc;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::{DeliveryError, EventBus, Subscription};
use crate::orders::events::OrderCreated;

pub trait AuditStore: Send + Sync {
    fn append_order_created(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}

pub fn subscribe(
    bus: &EventBus,
    store: Arc<dyn AuditStore>,
) -> Result<Subscription, Box<dyn std::error::Error>> {
    let request = SubscribeRequest::new("audit-log", OrderCreated::TOPIC)?;
    Ok(bus.subscribe(request, move |delivery| {
        store.append_order_created(delivery.payload())
    })?)
}
```

```rust
// src/customer_view.rs
use std::sync::Arc;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::{DeliveryError, EventBus, Subscription};
use crate::orders::events::OrderCreated;

pub trait CustomerViewStore: Send + Sync {
    fn upsert_order(&self, event: &OrderCreated) -> Result<(), DeliveryError>;
}

pub fn subscribe(
    bus: &EventBus,
    store: Arc<dyn CustomerViewStore>,
) -> Result<Subscription, Box<dyn std::error::Error>> {
    let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?;
    Ok(bus.subscribe(request, move |delivery| {
        store.upsert_order(delivery.payload())
    })?)
}
```

应用启动时先创建总线和订阅，再开放订单请求入口：

```rust
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;

let bus = EventBus::local(LocalEventBusConfig::default())?;
let audit_subscription = audit::subscribe(&bus, audit_store)?;
let view_subscription = customer_view::subscribe(&bus, view_store)?;
let order_bus = bus.clone(); // 注入订单服务；应用保留 bus 与订阅句柄
```

`audit_store` 和 `view_store` 是应用构造好的存储访问对象。`subscribe` 返回的 `Subscription` 是订阅句柄，应用应持有它并在关闭时调用 `cancel()`；仅丢弃同步订阅句柄不会取消订阅。传给 `bus.subscribe` 的闭包是处理函数，收到事件后调用相应的存储；存储失败时返回错误，由订阅策略决定重试还是记录失败，见[处理失败和重试](#处理失败和重试)。

### 提交事务后发布

```rust
// src/orders/service.rs
use qubit_event_bus::model::{PublishReceipt, PublishRequest};
use qubit_event_bus::EventBus;
use super::events::OrderCreated;

pub struct CreateOrder {
    pub customer_id: String,
    pub total_cents: u64,
}

pub struct CommittedOrder {
    pub order_id: String,
    pub customer_id: String,
    pub total_cents: u64,
}

pub trait OrderRepository: Send + Sync {
    // 成功返回表示订单事务已经提交。
    fn create_and_commit(
        &self,
        command: CreateOrder,
    ) -> Result<CommittedOrder, Box<dyn std::error::Error>>;
}

pub fn create_order(
    repository: &dyn OrderRepository,
    bus: &EventBus,
    command: CreateOrder,
) -> Result<PublishReceipt, Box<dyn std::error::Error>> {
    let order = repository.create_and_commit(command)?;
    let event = OrderCreated {
        order_id: order.order_id,
        customer_id: order.customer_id,
        total_cents: order.total_cents,
    };
    Ok(bus.publish(PublishRequest::new(OrderCreated::TOPIC, event)?)?)
}
```

`create_and_commit` 成功返回后才发布 `OrderCreated`。调用方还应检查回执，并记录事件 ID、订单 ID 与失败原因：

```rust
use qubit_event_bus::model::AdmissionRequirement;

let receipt = orders::service::create_order(repository, &order_bus, command)?;
receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected)?;
```

这项检查只能说明至少有一个订阅者接纳了消息，且没有订阅者明确拒绝；它**不能**说明审计记录和客户视图已经写入数据库。回执里还有哪些信息、失败时如何区分处理，见下一节[检查发布结果](#检查发布结果)。订单提交与事件发布是两个独立操作：提交后进程退出或发布失败，会留下没有事件的订单。若审计记录必须可靠保存，应在订单事务中同时写入一条待发布事件（outbox），由后台任务发布并补偿；重试整个下单请求不能弥补这一缺口。

### 涉及的核心类型

| 类型 | 作用 |
| --- | --- |
| `EventBus` | 总线句柄，负责发布、订阅和关闭；可 `clone` 后注入各模块。 |
| `Topic<OrderCreated>` | 主题 `orders.created`，载荷类型为 `OrderCreated`。 |
| `SubscribeRequest` | 订阅请求，包含订阅者 ID、主题和可选的订阅选项。 |
| `PublishRequest` | 发布请求，包含主题、载荷和可选的事件 ID、header 等元数据。 |
| `Subscription` | 订阅句柄，用于持有和取消订阅。 |
| `PublishReceipt` | 发布回执，报告 provider 的接纳情况，不代表处理完成。 |
| `local` | 内置的进程内 provider；进程退出后消息不可恢复。 |

API 中的 `provider` 指负责实际传递消息的后端实现，`local` 是其中之一。上面的业务代码只依赖总线接口，不直接接触 provider。

## 检查发布结果

`bus.publish(...)` 返回的 `Err(PublishError)` 表示这次调用没有正常拿到接收情况报告。即使返回 `Ok(receipt)`，也可能只有部分处理方收到消息。`receipt` 就是这份报告；调用 `receipt.admission_outcome()` 可区分以下情况：

| 结果 | 含义 |
| --- | --- |
| `Accepted(summary)` | 至少一个处理方接收，没有处理方拒绝；某些处理方仍可能按规则跳过。 |
| `PartiallyAccepted(summary)` | 有人接收，也有人拒绝；直接重发可能使已接收者处理两次。 |
| `NoneAccepted(summary)` | 找到了处理方，但无人接收；查看跳过或拒绝原因。 |
| `NoDestinations` | 没找到处理方；检查是否先完成订阅，主题名是否一致。 |
| `OpaqueAccepted` | 消息传递实现说已接收，但不告诉你具体是哪些处理方。 |
| `Dropped` | 发布前的拦截规则主动丢弃了消息。 |

### 成功时能看到什么

在订单示例中，审计和客户视图两个订阅都已建立，订单事务提交后发布一条 `OrderCreated`，正常情况下回执如下：

```rust
use qubit_event_bus::model::{AdmissionOutcome, PublishAcknowledgement, PublishRequest};

let receipt = bus.publish(PublishRequest::new(OrderCreated::TOPIC, event)?)?;
// 内置 local 的 provider ID 是 "local"
println!("provider = {}", receipt.provider_id().as_str());
match receipt.admission_outcome() {
    AdmissionOutcome::Accepted(summary) => {
        // 订单示例中 summary.accepted == 2，summary.filtered == 0，summary.rejected == 0
        println!(
            "事件 {} 已交给 {} 个订阅者",
            receipt.input_event_id().as_str(),
            summary.accepted
        );
    }
    other => eprintln!("接纳结果异常：{other:?}"),
}
if let PublishAcknowledgement::DestinationAdmissions(destinations) = receipt.acknowledgement() {
    for destination in destinations {
        // 两行输出：audit-log -> Accepted、customer-view -> Accepted（先后顺序不固定）
        println!("{} -> {:?}", destination.subscriber_id().as_str(), destination.status());
    }
}
```

看到 `Accepted`，且两个目标的状态都是 `Accepted`，订单场景的接入就完成了。这是总线层面的成功标志：消息已经进入两个订阅者的队列，处理函数随后会在总线的工作线程上运行。审计记录和客户视图是否真的写入数据库，要看各自存储的返回值和日志，回执不会告诉你。

回执只说明各订阅者是否**接纳**了这条消息，处理函数此时通常还没执行。`check_admission` 按你给出的条件判断这次发布算不算成功，它只读回执：不会重发，也不会等待处理函数跑完。

以订单事件同时发给审计和客户视图为例，两种条件的差别是：

- `AtLeastOneAccepted`：至少有一个订阅者接纳即可。审计接纳、客户视图因队列已满拒绝时，检查仍通过。
- `AtLeastOneAcceptedAndNoRejected`：至少一个接纳，并且没有人拒绝。上面那种情况会返回 `RejectedDestinations`。前面订单示例用的就是这个条件。

订单服务在发布后按较严的条件检查，并按失败原因决定怎么处理：

```rust
use qubit_event_bus::model::{AdmissionCheckError, AdmissionRequirement};

let receipt = bus.publish(PublishRequest::new(OrderCreated::TOPIC, event)?)?;
match receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected) {
    Ok(()) => {}
    Err(AdmissionCheckError::RejectedDestinations { count }) => {
        // 审计可能已经接纳。不要整条重发，先查出拒绝者，只补做被拒绝的部分。
        eprintln!("{count} 个订阅者拒绝接纳订单 {}", receipt.input_event_id().as_str());
    }
    Err(AdmissionCheckError::NoAcceptedDestination) => {
        // 没人订阅，或找到了订阅者但无人接纳。可以整条重发。
        eprintln!("订单事件未被任何订阅者接纳");
    }
    Err(AdmissionCheckError::VisibilityUnavailable) => {
        // 传递实现没有列出订阅者，无法判断是否有人拒绝。
        eprintln!("回执未提供各订阅者的接纳情况");
    }
    Err(AdmissionCheckError::Dropped) => {
        eprintln!("发布拦截器在送达前丢弃了订单事件");
    }
}
```

回执中的 `Filtered` 表示传递实现在接纳阶段就判定这条消息不属于该订阅者。它不算拒绝：只要另有订阅者接纳，两种条件都通过。但它也不算接纳：如果所有目标都是 `Filtered`，`accepted` 为 0，结果是 `NoneAccepted`，两种条件都返回 `NoAcceptedDestination`。队列已满属于拒绝（`Rejected`），与跳过不要混淆。注意，只有能在接纳阶段评估过滤条件的传递实现才会报告 `Filtered`；内置 local 不会。在 local 上，订阅选项里的 `filter` 是总线取到消息后才执行的：被过滤掉的订单在回执里仍显示为 `Accepted`，只是它的处理函数不会被调用。

有些传递实现只报告“消息已收下”，不列出具体订阅者，对应表中的 `OpaqueAccepted`。这时库无法核对上面两个条件，`check_admission` 返回 `VisibilityUnavailable`。内置 local 会逐个报告订阅者，不会出现这种情况。

要知道是谁拒绝的，遍历回执里每个订阅者的状态：

```rust
use qubit_event_bus::model::{AdmissionStatus, PublishAcknowledgement};

if let PublishAcknowledgement::DestinationAdmissions(destinations) = receipt.acknowledgement() {
    for destination in destinations {
        if let AdmissionStatus::Rejected(reason) = destination.status() {
            eprintln!("{} 拒绝接纳：{reason}", destination.subscriber_id().as_str());
        }
    }
}
```

部分接纳时不能把整条事件原样重发：审计已经接纳，重发会让它再处理一次。应记录事件 ID、谁接纳、谁拒绝，只补做被拒绝的部分。订阅者自己也要能承受重复：审计模块可以用“订单 ID + 审计事件类型”作为唯一键，再次收到同一订单的创建事件时不再插入第二条记录。这种“重复执行与执行一次结果相同”的处理叫**幂等**。

`receipt.input_event_id()` 是发布请求里的事件 ID。发布拦截器在送达前丢弃消息时，消息没有被发出，`dispatched_event_id()` 为 `None`。

到这里，订单场景的基础接入已经完成。下面各节按需查阅：先是发布侧可附加的元数据和顺序保证，然后是订阅侧的失败处理、拦截过滤，最后是容量配置、第三方实现、异步用法和停机。

## 按需补充消息信息

前面的 `PublishRequest::new(topic, payload)?` 已能完成发布，并会为事件生成 ID。需要自己指定 ID，或附上用于串联日志的请求编号、控制同一客户的处理顺序时，使用请求构造器（builder）：

```rust
use qubit_event_bus::model::{EventId, PublishRequest};

let request = PublishRequest::builder()
    .topic(OrderCreated::TOPIC)
    .payload(event)
    .event_id(EventId::new("order-created-42")?)
    .header("correlation-id", "request-42")
    .ordering_key("customer-7")
    .build()?;
let receipt = bus.publish(request)?;
```

事件 ID 帮助追踪“这是否是同一条事件”。如果失败后重发，同一业务事件应保留同一 ID；不同事件不要误用同一 ID。`header` 是随消息附带的文字信息，适合放请求编号，不适合放密码。`x-qubit-event-bus-dead-letter` 由库保留使用。`ordering_key` 是顺序键，用于让同一业务对象的事件按顺序处理，需要与订阅方配合使用，详见[保证同一对象的处理顺序](#保证同一对象的处理顺序)。构造器还可设置时间、延迟、重试和拦截器；这些进阶选项见后文。

一次要发布多条同类型事件时，可调用 `publish_all`。它按输入顺序逐条尝试，在 `BatchPublishResult::items()` 中保留每条的成功或错误。前一条失败不会阻止后续条目，它不会把多条消息组成一个数据库事务。

## 保证同一对象的处理顺序

默认情况下，总线**不保证**订阅者按发布顺序处理事件。订阅选项 `ordering_policy` 的默认值是 `OrderingPolicy::Unordered`：以内置 local 为例，同步和异步总线默认都允许最多 4 个处理函数同时执行，同一订阅者先后收到的两条事件可能并发处理，后发布的一条也可能先处理完。其他传递实现的行为由其自身决定，同样不能假设有序。

很多场景依赖顺序。例如客户视图模块为每个客户维护“最近一笔订单”。客户 `customer-7` 在短时间内先后下了 `order-42` 和 `order-43`，两条 `OrderCreated` 若被并发处理，`order-43` 可能先写入视图，随后被 `order-42` 覆盖，视图就把较早的订单当成了最近一笔。账户余额变更、订单状态流转等“后一条依赖前一条结果”的处理都有同样的问题。

要保证顺序，发布方和订阅方必须同时配置。

**发布方：为事件设置顺序键。** 顺序键是应用自定的字符串，取值应标识“需要保持顺序的业务对象”；它不能为空，也不能含首尾空白或控制字符，否则 `build()` 返回 `InvalidOrderingKey`。这里要让同一客户的订单按序进入视图，就用客户号作键：

```rust
let request = PublishRequest::builder()
    .topic(OrderCreated::TOPIC)
    .payload(event)
    .ordering_key("customer-7") // 实际代码中取 order.customer_id
    .build()?;
bus.publish(request)?;
```

**订阅方：请求按键保序。** 在订阅选项中把 `ordering_policy` 设为 `PerKey`：

```rust
use qubit_event_bus::model::{OrderingPolicy, SubscribeOptions, SubscribeRequest};

let options = SubscribeOptions::builder()
    .ordering_policy(OrderingPolicy::PerKey)
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, handler)?;
```

两边都配置后，客户视图订阅的行为如下：

- **同一个键**：键为 `customer-7` 的 `order-42` 和 `order-43` 按进入该订阅的先后逐条处理；`order-43` 的处理函数要等 `order-42` 的处理函数返回后才开始。
- **不同的键**：键为 `customer-8` 的事件走另一条顺序通道，不必等待 `customer-7`。即使 `customer-7` 的某个处理函数卡住，`customer-8` 的事件仍会继续处理。
- **其他订阅**：保序只对请求了 `PerKey` 的订阅生效，每个订阅各自排序。审计模块如果没有请求 `PerKey`，它收到的 `customer-7` 事件仍可能乱序；两个订阅之间谁先处理也不作保证。
- **其他主题**：顺序通道按主题划分，`orders.created` 与其他主题上同一个键的事件之间不保证顺序。需要跨事件类型保序时，应把这些事件放在同一主题中，例如用枚举类型作为载荷。

使用时还要注意以下几点：

- **只配一边不起作用。** 发布方带了键、订阅方没有请求 `PerKey` 时，键被忽略，仍按默认的无序方式处理。订阅方请求了 `PerKey`、事件却没带键时，该订阅中所有无键事件归入同一条通道逐条处理：顺序得到保证，但失去了并行。
- **传递实现必须支持按键保序。** 创建订阅时，总线会检查所用实现的能力；不支持时，`subscribe` 返回 `SubscribeError::Capability(CapabilityError::Unsupported { capability: "ordering.per_key" })`，不会静默降级为无序。内置同步和异步 local 都支持。
- **顺序指进入订阅的先后。** 同一线程依次发布时，就是发布调用的先后；多个线程并发发布同一键的事件时，谁先进入由竞争决定，需要由发布侧自行串行化。
- **键的粒度决定并行度。** 用客户号作键，同一客户串行、不同客户并行；用订单号作键，只保证同一订单的事件有序；用固定值作键，等于让整个订阅串行处理。同一键的处理函数变慢或阻塞时，会拖住该键后续的所有事件，处理函数应避免长时间阻塞。

## 处理失败和重试

前面的 `SubscribeRequest::new(subscriber_id, topic)?` 用默认设置：只接收订阅后发出的新消息，不过滤，也不自动重试处理函数。处理成功时，库自动向消息传递实现确认；这叫 **ACK**。如果要改变这些设置，可先用 `SubscribeOptions::<T>::builder()` 构造选项，再用 `request.with_options(options)` 放进订阅请求；也可以直接使用 `SubscribeRequest::builder()`。

| 目标 | 入口 | 注意事项 |
| --- | --- | --- |
| 过滤事件 | `filter` | 总线取到消息后、处理函数运行前，查看事件内容并决定是否跳过。在 local 上，被跳过的消息在发布回执里仍是 `Accepted`；跳过不等于拒绝。 |
| 由处理函数决定何时确认 | `ack_mode(AckMode::Manual)` | 写入业务数据后调用 `delivery.acknowledgement().ack()`；处理失败可调用 `nack()`。未作决定就返回会被视为失败。示例见[由处理函数决定何时确认](#由处理函数决定何时确认)。 |
| 失败后重试 | `retry_policy`，可配 `retry_rule` / `retry_cancellation_token` | 重试次数和间隔由策略决定；单独设置错误分类规则不会启动重试。使用这些类型时需直接依赖 `qubit-retry = "0.25"`。示例见[数据库写入失败后自动重试](#数据库写入失败后自动重试)。 |
| 失败后选择动作 | `error_handler` | 可要求重试、重新放回队列、转入失败消息主题或放弃；重新入队需要所用实现支持。 |
| 保存最终处理失败的事件 | `dead_letter(DeadLetterPolicy::topic(name)?)` | 把失败消息转发到另一个主题（死信主题），还需有人订阅并处理它。示例见[保存最终处理失败的事件](#保存最终处理失败的事件)。 |
| 同一客户的消息按顺序处理 | `ordering_policy(OrderingPolicy::PerKey)` | 发布方须为事件设置顺序键，所用实现还须支持此能力；见[保证同一对象的处理顺序](#保证同一对象的处理顺序)。 |
| 多个实例分工或读取旧消息 | `consumer_group`、`durability`、`start_position` | 只有支持这些能力的传递实现可使用；local 不支持持久订阅和历史读取。 |
| 底层实现专有参数 | `provider_option` | 具体含义由该实现说明；不要放密码。 |

处理函数可返回 `()` 或 `Result<(), DeliveryError>`。如果数据库写入失败，应返回错误，让库按订阅策略处理。`delivery.payload()` 取得业务数据，`delivery.event()` 可取得事件 ID 和附带信息，`delivery.context()` 可取得传递实现、订阅者以及当前是第几次尝试（`retry_attempt()`，从 1 开始）。重试可能带来重复处理，因此处理方应使用业务唯一键。下面以客户视图和审计两个订阅为例，给出三种最常用的配置。

### 数据库写入失败后自动重试

客户视图写入偶发超时时，让库自动重试。重试策略类型来自 `qubit-retry`，需要在应用中直接依赖 `qubit-retry = "0.25"`：

```rust
use std::time::Duration;
use qubit_event_bus::model::{SubscribeOptions, SubscribeRequest};
use qubit_retry::{BackoffPolicy, RetryPolicy};

let options = SubscribeOptions::<OrderCreated>::builder()
    .retry_policy(
        RetryPolicy::builder()
            .max_attempts(3)
            .backoff(BackoffPolicy::fixed(Duration::from_millis(200)))
            .build()?,
    )
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, move |delivery| store.upsert_order(delivery.payload()))?;
```

处理函数返回 `Err` 时，库间隔 200 毫秒再次调用它，总共最多尝试 3 次。三次都失败后，这条消息按 `Discard` 处理：不再重试，也不会进入死信主题，只向 `observe_diagnostics` 登记的回调发出一条 `Diagnostic::DeliveryFailed`，其中带有尝试次数和最终错误。只设置 `retry_policy` 就会重试；`retry_rule` 用于按错误类型决定哪些失败值得重试，单独设置它不会启动重试。重试期间这条消息一直占用 local 的积压名额。

### 由处理函数决定何时确认

默认情况下，处理函数正常返回就算确认（ACK）。如果审计模块希望只在记录真正落库后才确认，把 `ack_mode` 设为 `Manual`，并在写入成功后调用 `ack()`：

```rust
use qubit_event_bus::model::{AckMode, SubscribeOptions, SubscribeRequest};
use qubit_event_bus::DeliveryError;

let options = SubscribeOptions::<OrderCreated>::builder()
    .ack_mode(AckMode::Manual)
    .build();
let request = SubscribeRequest::new("audit-log", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, move |delivery| {
    store.append_order_created(delivery.payload())?;
    delivery
        .acknowledgement()
        .ack()
        .map_err(|error| DeliveryError::Handler { source: Box::new(error) })?;
    Ok(())
})?;
```

手动模式下，处理函数返回 `Ok` 但没有调用 `ack()`，这次处理仍被视为失败；调用 `nack()` 表示明确拒绝。同一条消息重复 `ack()` 是安全的，`ack()` 之后再 `nack()`（或反过来）会返回 `AlreadyCompleted`。ACK 只是处理函数给总线的确认，不能代替数据库事务。

### 保存最终处理失败的事件

如果客户视图写入失败后不想丢弃事件，而是留下来事后补处理，配置死信（dead letter）主题。失败方通过 `error_handler` 返回 `FailureDirective::DeadLetter`，并用 `dead_letter` 指定主题；读取方订阅这个主题，收到的载荷类型是 `DeadLetterEvent<OrderCreated>`，不是原来的 `OrderCreated`：

```rust
use qubit_event_bus::model::{
    DeadLetterEvent, DeadLetterPolicy, FailureDirective, SubscribeOptions, SubscribeRequest, Topic,
};

// 失败方：处理失败时转入死信主题
let options = SubscribeOptions::<OrderCreated>::builder()
    .error_handler(|_event, _error| FailureDirective::DeadLetter)
    .dead_letter(DeadLetterPolicy::topic("orders.created.dead")?)
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let view_subscription = bus.subscribe(request, move |delivery| store.upsert_order(delivery.payload()))?;

// 读取方：订阅死信主题，记录失败原因供人工或后台任务补处理
let dead_letter_topic = Topic::<DeadLetterEvent<OrderCreated>>::new("orders.created.dead")?;
let dead_letter_subscription = bus.subscribe(
    SubscribeRequest::new("dead-letter-reader", dead_letter_topic)?,
    |delivery| {
        let dead = delivery.payload();
        eprintln!(
            "订阅者 {} 放弃了订单 {}：{}",
            dead.subscriber_id().as_str(),
            dead.original_event().payload().order_id,
            dead.reason()
        );
    },
)?;
```

`error_handler` 返回 `DeadLetter` 时，这条消息不再重试，直接转发到死信主题；即使同时配置了 `retry_policy` 也是如此。死信主题只是另一个普通主题，必须先有人订阅并处理；没有订阅者时，转发出去的死信同样没有接收方，事件仍然丢失。如果使用需要编码的外部传递实现，还要为 `DeadLetterEvent<OrderCreated>` 配置编码器。死信转发本身也可能失败，应用应监控诊断信息。

## 需要拦截或过滤消息时

**过滤**发生在接收方：处理函数运行前，先判断这条消息要不要交给它。**拦截器**则是一段放在发布或处理路径中的自定义代码，适合添加关联信息、记录日志或主动停止后续步骤。普通接入不需要先配置拦截器。

请求中的发布拦截器只影响这一次发布；`EventBusFacadeConfig::publisher_interceptor` 会影响通过该总线对象发出的所有消息。后者可修改 header，返回 `Ok(false)` 会停止发布，报告显示为 `Dropped`。发布错误处理器只负责观察最终错误，不会把错误变成成功。

订阅方先按 `filter` 判断是否接收，再执行拦截器和处理函数。同步订阅用 `SubscribeOptions::builder().interceptor(...)`，异步订阅用 `async_interceptor(...)`；也可在 `EventBusFacadeConfig` 中为整个总线对象配置。拦截器拿到一个 `next` 回调，必须调用它，后面的处理函数才会执行。同步总线不能配置异步订阅拦截器，异步总线也不能配置同步订阅拦截器；用错会在创建订阅时得到配置错误。

## 配置内置 local 事件总线

### 直接创建

`LocalEventBusConfig` 管理传输层容量：

```rust
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;

let local = LocalEventBusConfig::new()
    .queue_capacity(2_048)
    .max_total_outstanding(20_000);
let bus = EventBus::local(local)?;
```

`queue_capacity` 限制**每个订阅者**最多积压多少条消息，默认 1,024；`max_total_outstanding` 限制**这个 local 实例的所有订阅者合计**最多积压多少次投递，默认 65,536。已经取走但还没处理完的消息也算在内，重试期间仍占名额。两个值都要大于零。计数单位是消息投递次数，不是字节；队列满时，某个订阅者可能拒绝，其他订阅者仍可接收。处理结束或关闭会释放名额。请结合处理速度和内存实测调整，不能只看条数推断内存占用。

除 local 的积压上限外，事件总线对象还限制自己同时接手多少条消息。同步总线默认最多接手 4 条（包括正在处理和等待处理的消息），处理函数等待队列另有容量 32；实际能排队多少仍受前面的 4 条上限约束。异步总线默认最多同时处理 4 条。需要修改这些值时，用 `EventBusFacadeConfig`。这个类型名中的 `Facade` 是 API 名称；在本文把它理解成“总线的通用设置”即可。例如把同步总线接手消息的上限设为 8、等待队列容量设为 64：

```rust
use qubit_event_bus::{EventBusConfig, EventBusFacadeConfig, EventBusRegistry};
use qubit_event_bus::facade::SyncDeliverySchedulerConfig;
use qubit_event_bus::local::LocalEventBusConfig;

let local = LocalEventBusConfig::new()
    .queue_capacity(2_048)
    .max_total_outstanding(20_000);
let bus_settings = EventBusFacadeConfig::new()
    .with_sync_delivery_scheduler(SyncDeliverySchedulerConfig::new(8, 64)?);
let config = EventBusConfig::default()
    .with_provider_options(local.provider_options())
    .with_facade_config(bus_settings);
let bus = EventBusRegistry::with_local()?.create(&config)?;
```

`SyncDeliverySchedulerConfig::new` 的第一个值必须大于零；第二个值可以为 0，表示只能把消息立即交给空闲工作线程。使用注册表创建 local 时，要把 `local.provider_options()` 传给 `EventBusConfig`；它包含 `local.queue_capacity` 和 `local.max_total_outstanding` 两个配置键。未知的键、非数字值或零值会在创建时被拒绝。直接调用 `EventBus::local` 只能设置 local 的积压容量；要修改处理并发、编码器或拦截器，就通过 `EventBusRegistry::with_local()` 创建。

异步总线也使用 `LocalEventBusConfig`。例如把同时处理的消息数设为 8：

```rust
use qubit_event_bus::{AsyncEventBusRegistry, DeliveryAdmissionConfig};
use qubit_event_bus::{EventBusConfig, EventBusFacadeConfig};
use qubit_event_bus::local::LocalEventBusConfig;

let local = LocalEventBusConfig::new().queue_capacity(2_048);
let bus_settings = EventBusFacadeConfig::new()
    .with_delivery_admission(DeliveryAdmissionConfig::new(8)?);
let config = EventBusConfig::default()
    .with_provider_options(local.provider_options())
    .with_facade_config(bus_settings);
let bus = AsyncEventBusRegistry::with_local()?.create(&config).await?;
```

同步 local 为每个订阅者建立一个接收线程，处理函数由总线共享的工作线程执行；异步 local 不为每个订阅者建立接收线程，但应用必须持续运行 `AsyncSubscription::run`。订阅很多时，在实际部署机器上运行 `cargo bench --bench local_threads` 和 `cargo bench --bench local_scale` 测量，不能把别人的测试结果当作固定容量。

## 选择和接入第三方实现

按照消息能够传递的范围，事件总线大致可以分为三类：

- **进程内**：发布者和订阅者位于同一个程序进程中，消息通过内存传递。它无需部署额外服务，延迟低；但进程退出时，尚未处理完的消息会随之丢失。
- **跨进程**：发布者和订阅者是同一台机器上的不同进程，消息借助操作系统的进程间通信机制或本机运行的消息服务传递。
- **跨节点**：发布者和订阅者分布在不同机器上，消息经网络传递，通常依靠 Kafka、RabbitMQ、Redis 等消息中间件。这类中间件往往还提供持久化、重投等能力。

本库的核心是一层统一的抽象：业务代码只面向同一套发布和订阅接口编写，消息实际如何传递，由可替换的后端实现（本库称为 provider）负责。搭配不同范围的后端，就能得到进程内、跨进程或跨节点的事件总线，而发布和订阅的业务代码无须随之改写。需要注意的是，不同后端的能力并不相同，例如是否持久化消息、支持哪些投递策略，选用时仍要逐项确认。

本库目前只自带 `local` 实现，属于进程内事件总线，没有内置 Kafka、RabbitMQ、Redis 等后端。需要跨进程或跨节点传递时，有两条路：使用别人已经写好的实现，或者自己开发一种。下面分别说明。

### 使用别人已经写好的实现

假设有人提供了连接消息服务器的实现，你的应用先把它的 crate 加进依赖，再让总线知道“要用这个实现”。一个做法是显式注册：建立 `EventBusRegistry::new()`，调用 `register(那个实现)`，再调用 `create(&config)` 创建总线。同步实现放在 `EventBusRegistry`，异步实现放在 `AsyncEventBusRegistry`；异步创建要 `.await`。可以用 `provider_ids()` 查看已注册实现的 ID，再用 `EventBusConfig::with_selection` 指定其中一个。

有些第三方 crate 支持自动登记。它在程序链接时把自己的定义放入一个目录，这项机制叫 `discovery`（发现）。这种情况下，应用启用 feature，并确保该 crate 被链接：

```toml
qubit-event-bus = { version = "0.14", features = ["discovery"] }
qubit-spi = "0.13"
# 再加入所选 provider crate 的实际包名和版本。
```

```rust
use provider_crate as _; // 用实际 crate 名替换，确保 provider 定义链接进可执行程序
use qubit_event_bus::{EventBusConfig, EventBusRegistry};
use qubit_spi::ProviderSelection;

let registry = EventBusRegistry::discover()?;
registry.set_default_selection(ProviderSelection::named("your-provider-id")?)?;
registry.seal();
let bus = registry.create(&EventBusConfig::default())?;
```

把示例中的 `provider_crate` 和 `your-provider-id` 换成真实 crate 名称及其文档给出的 ID。如果程序找不到实现，先查看 `provider_ids()`；若两个实现使用同一选择名称，`discover()` 会报错。内置 `local` 只自动登记在**同步**目录；异步 local 要用 `AsyncEventBusRegistry::with_local()` 显式加入。同步与异步目录互不通用。两种 local 实现的 ID 都是 `local`，还可用 `memory` 或 `in-process` 选择。同步 local 也可通过 `EventBusRegistry::with_local()` 显式加入。

`EventBusConfig::with_provider_options` 传递这个实现自己的配置，`with_facade_config` 配置总线的处理方式，`with_required_capabilities` 说明应用必须具备什么能力。例如 `RequiredCapabilities::new().durable()` 表示“消息必须能持久保存”；内置 local 做不到，创建时就会报错。注册表只会在**创建总线时**尝试其他候选；运行中发布或接收失败不会自动换实现。`ProviderOptions` 可能出现在调试输出中，不要放密码或 token。

### 自己开发一种传递实现

这一节面向**编写底层传递库**的开发者。如果你只是使用现成实现，上一节已经够用。一个新实现需要同时解决“怎样传递消息”和“怎样交给应用选择”，因此分成两层：

1. **实现消息传递。** 库把这组接口叫 SPI。同步实现要提供 `EventBusSpi` 的能力说明、发布、订阅和关闭方法；每次订阅还要返回一个 `EventSubscriptionSpi`，负责收消息、确认处理结果和关闭。异步实现对应 `AsyncEventBusSpi` 与 `AsyncEventSubscriptionSpi`。能力说明要如实填写，不能把不支持的持久化或排序能力报成支持。
2. **让应用能创建它。** 实现 `ProviderMetadata`，给它一个唯一 ID；再实现 `ServiceProvider<EventBusSpec>` 或异步的 `AsyncServiceProvider<EventBusSpec>`。创建方法 `create_configured` 读取配置、连接后端，返回上一步的实现。配置错误要明确报告。
3. **加入应用并验证。** 应用可显式调用 `registry.register(MyProvider)`；如果需要自动发现，再提交到相应目录。启用 `conformance` feature 可运行库提供的接口契约检查。还要测试队列满、取消、重复确认、重复关闭和错误信息。

进程内实现可以直接传递 Rust 对象（`TransportPayload::Native`）；跨进程传输通常要先把对象编码成字节（`Encoded`）。`publish(OutboundMessage)` 的返回值要反映实际接收情况：能判断每个处理方是否接收，才返回 `DestinationAdmissions`；只知道消息服务器接收时，返回不列出处理方的 `Accepted`。不要把“只写入本地缓冲”描述成“已经持久保存”。`receive` 要能区分收到消息、等待超时、消息缺口和已关闭。如果支持确认处理结果，重复提交同一结果应安全，提交冲突结果要报错。异步操作被取消时，尚未确认的消息不能被当成成功处理。

用于创建实现的代码要先校验配置，再建立连接；无法连接、配置无效或暂时不可用时，返回对应的 `ProviderFailure<EventBusProviderError>`。应用的处理函数、过滤、重试和死信由事件总线对象管理；底层实现只负责消息传递及它明确声明支持的确认方式。

下面是同步实现向应用提供的注册代码骨架。`MyTransport::connect` 由你的传输库实现，负责连接和错误转换；这个骨架展示的是接入点，不是完整的消息服务器客户端：

```rust
use std::sync::Arc;
use qubit_event_bus::{EventBusConfig, EventBusProviderError, EventBusSpec};
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::{ProviderDescriptor, ProviderId, ProviderMetadata, ServiceProvider};
use qubit_spi::error::ProviderFailure;

struct MyProvider;

impl ProviderMetadata for MyProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(ProviderId::new("my-transport").unwrap())
    }
}

impl ServiceProvider<EventBusSpec> for MyProvider {
    fn create_configured(
        &self,
        config: &EventBusConfig,
    ) -> Result<Arc<dyn EventBusSpi>, ProviderFailure<EventBusProviderError>> {
        // MyTransport::connect 是你的后端实现：校验配置、建立连接，
        // 并把错误映射为 ProviderFailure<EventBusProviderError>。
        MyTransport::connect(config)
    }
}

// 显式注册：
// let registry = EventBusRegistry::new();
// registry.register(MyProvider)?;
```

若要支持自动发现，在实现 crate 中启用 `discovery`，并提交到同步目录：

```rust
qubit_spi::submit_sync_provider! {
    inventory_entry = qubit_event_bus::registry::sync_provider_inventory::Entry;
    spec = qubit_event_bus::EventBusSpec;
    provider = MyProvider;
}
```

异步实现使用独立的异步目录和 `submit_async_provider!`；可参考[内置异步实现源码](../src/local/async_local_event_bus_provider.rs)。接入生产系统前，应重点验证：等待超时或关闭时的结果是否正确；重复确认是否安全；异步操作被取消后消息是否仍可重新处理；关闭后是否仍保留未确认消息；重复关闭是否安全。详细接口契约见 [SPI 设计](spi_design.zh_CN.md) 和 [SPI API](https://docs.rs/qubit-event-bus/latest/qubit_event_bus/spi/)。

### 跨进程实现需要编码时

内置 local 直接传递 Rust 对象，不需要转换。消息要跨进程传递时，通常需要先把对象转换成字节，接收时再还原；负责这件事的组件叫**编码器**（codec）。可以用 `Topic::new_with_codec` / `new_with_shared_codec` 为某类事件指定编码器，也可以把编码器放进 `CodecRegistry`，再通过 `EventBusFacadeConfig::with_codec_registry` 配给总线。主题自带编码器优先；没有才查总线的注册表。创建订阅时会选定编码器，两处都没有时会报错。应用还要约定数据格式与版本兼容方式；本库不内置通用 JSON 编码器。

## 异步总线与订阅

如果应用本身用异步 Rust，可选择 `AsyncEventBus`。它不依赖某个固定运行时，但应用仍需有 Tokio 等任务执行环境来运行异步代码。`publish`、`subscribe`、`publish_all`、`shutdown` 都需要 `.await`。与同步版不同，异步 `subscribe` 只创建订阅；应用还必须启动一个长期运行的任务去执行 `subscription.run(...)`，消息才会交给处理函数：

```rust
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::SubscribeRequest;

let bus = AsyncEventBus::local(LocalEventBusConfig::default()).await?;
let mut subscription = bus
    .subscribe(SubscribeRequest::new("audit-log", OrderCreated::TOPIC)?)
    .await?;

// 放在应用 executor 的长生命周期任务中；store 是异步存储实现。
subscription.run(move |delivery| {
    let store = store.clone();
    async move { store.append_order_created(delivery.payload()).await }
}).await?;
```

应用启动时应把 `run(...)` 放进后台任务，再开放业务入口；如果在启动函数中直接等待它，后面的启动步骤就不会执行。取消这次 `run` 但保留订阅句柄，之后还能再次运行；调用 `close().await` 或丢弃句柄则结束订阅。本地异步实现关闭时会丢弃仍在排队和未处理完的消息，再次用同一 ID 订阅也会从空队列开始。`wait_for_received_deliveries` 只等待总线已取到的消息，不检查传递实现里是否还有排队消息；异步总线没有同步版的 `wait_for_idle`。

## 非阻塞通知入口

如果产生消息的代码不能停下来等待同步发布，可使用 `NotificationPublisher<T>`：它先把消息放进一个有容量上限的队列，再由一个后台线程逐条发布。默认最多排队 256 条。`try_publish(payload)` 只说明**成功入队**，并非已经发布；队列满时返回 `TryPublishError::Full(payload)`，关闭后返回 `Closed(payload)`，原数据会还给调用方。观察回调收到 `Published(receipt)`、`PublishFailed(error)` 或 `RequestFailed(error)`；这里的 `Published` 仍不表示处理函数完成。`stats()` 可查看计数。

`close()` 停止接收新通知，处理完已入队消息并等待后台线程退出。停机需要限定等待时间时，可调用 `close_with_timeout(Duration::from_secs(30))`。期限到达会返回 `io::ErrorKind::TimedOut`；worker 会继续运行，可能继续发布已经接纳的通知。此后 `try_publish` 返回 `Closed`，之后可以再次调用 `close()` 或 `close_with_timeout()` 等待 worker 结束。超时不能中断正在执行的同步 provider 调用。两种关闭方法都不会关闭通知发布器使用的事件总线。观察回调运行在后台线程上，应尽快返回；不要从回调内部调用同一个通知发布器的任一关闭方法。只丢弃句柄不会等待队列处理完，停机时应显式关闭通知发布器，再关闭总线。

## 生命周期、等待与停机

启动顺序是：创建总线 → 登记所有处理函数 → 开始接收业务请求。停机时先停止新业务请求，再关闭通知发布器等消息来源，最后处理订阅和总线。同步订阅用 `cancel()`，异步订阅用 `close().await`。如果需要尽量完成已经接收的消息，要根据所用传递实现决定取消订阅和关闭总线的先后顺序，并在该实现上验证；取消订阅本身不表示业务已经写入成功。

`ShutdownMode::Graceful { timeout }` 停止接收新工作，并尝试完成已经接收的工作。超时只表示等待时间到了；同步总线可能还在后台清理，之后可以再次调用 `shutdown` 获取结果。`Immediate` 也不能强行停止正在运行的业务代码。不要在本总线的处理函数内部调用会等待自身完成的 `shutdown`、`wait_for_idle` 或 `wait_for_received_deliveries`；这些调用会返回 `WouldDeadlock`。由程序最外层的停机流程发起关闭。

同步 `wait_for_idle(&topic, timeout)` 等待所用传递实现报告这个主题已没有排队或未处理完的消息；不支持这项查询时返回 `IdleWaitUnsupported`。`wait_for_received_deliveries` 只等待总线已经取到的消息。两者返回空闲，都不能代替检查数据库和失败记录。

## 错误、诊断与排障

| 现象 | 检查顺序 |
| --- | --- |
| `PublishError` | 查看具体错误是配置、编码、传递失败、重试结束还是总线已关闭；重试前确认是否可能已有处理方收到。 |
| `SubscribeError` | 检查订阅设置、所用实现是否支持相关能力、是否需要编码器、总线是否已关闭；启动失败时不要开放业务入口。 |
| `subscribe` 成功、回执也是 `Accepted`，处理函数却始终不执行 | 异步总线：确认已在一个长期任务中运行 `AsyncSubscription::run`，且该任务没有被提前取消，也没有阻塞在它前面的启动步骤上。同步总线：确认处理函数没有被前一条消息长期阻塞，工作线程上限见[配置内置 local 事件总线](#配置内置-local-事件总线)。 |
| `NoDestinations` | 检查主题名、载荷类型、订阅是否先于发布建立、同步订阅句柄是否已被 `cancel()`。 |
| `PartiallyAccepted` / `NoneAccepted` | 逐个检查目标的 `Accepted`、`Filtered`、`Rejected`，再看 local 容量和补偿方案。 |
| 等待空闲但业务无结果 | 查处理函数错误、重试/死信和业务数据库；接收报告与空闲均不代表写入成功。 |
| `IdleWaitUnsupported` | 所用实现不能报告整个主题是否空闲；可以由业务代码记录完成状态，不能把总线已取到的工作当作全部工作。 |
| 平稳关闭超时 | 查是否有处理函数或底层读写一直没返回、是否还有未完成的消息；稍后再获取关闭结果。 |

用 `observe_diagnostics` 可登记一个接收内部问题通知的回调。要持续接收通知，就保留返回的 `DiagnosticObserverHandle`；丢弃它会停止观察。回调会占用触发问题的线程，应尽快返回。`publish_metrics()` 统计发布和接收情况，不统计业务写入成功次数。日志建议同时记录订单 ID、事件 ID、处理方 ID、重试次数和最终错误。

## 边界与实践清单

- local 只适合允许进程退出时丢失消息、由应用自己补偿的进程内工作；它不提供持久恢复与跨进程通信。
- `publish_all` 无事务保证；发布回执、ACK、空闲和业务提交是不同阶段。
- 总线 API 提供消费组、持久订阅、历史重放、顺序和延迟等选项，并不表示所有传递实现都支持；使用前检查其能力并测试。
- 重要业务应设计持久移交、幂等、失败记录和补偿；特别覆盖部分接纳和重复投递。
- 测试应覆盖正常接收、无人订阅、队列满、处理函数失败、重试/死信和停机；容量与性能要在目标主机测量。

## 延伸阅读

- [中文 README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-event-bus)
