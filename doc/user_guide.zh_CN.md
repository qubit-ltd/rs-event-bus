# Qubit Event Bus 用户手册

[中文 README](../README.zh_CN.md) · [English user guide](user_guide.md) · [API 文档](https://docs.rs/qubit-event-bus)

本文适用于 `qubit-event-bus` 0.18.0，要求 Rust 1.94 或更高版本。它面向在 Rust 应用中需要让多个模块响应同一业务事件的开发者；编写底层传递实现的开发者只需查阅[自己开发一种传递实现](#自己开发一种传递实现)。读到[检查发布结果](#检查发布结果)，就能在项目中接入内置的进程内事件总线；后面章节供你按需查阅消息元数据、顺序保证、失败处理、配置、异步用法和第三方实现。

## 目录

- [它解决什么问题](#它解决什么问题)
- [从哪里开始](#从哪里开始)
- [接入订单服务](#接入订单服务)
  - [定义共用的事件数据](#定义共用的事件数据)
  - [注册两个处理模块](#注册两个处理模块)
  - [提交事务后发布](#提交事务后发布)
  - [涉及的核心类型](#涉及的核心类型)
- [检查发布结果](#检查发布结果)
  - [成功时能看到什么](#成功时能看到什么)
- [按需补充消息信息](#按需补充消息信息)
- [保证同一对象的处理顺序](#保证同一对象的处理顺序)
- [处理失败和重试](#处理失败和重试)
  - [数据库写入失败后自动重试](#数据库写入失败后自动重试)
  - [由处理函数决定何时确认](#由处理函数决定何时确认)
  - [保存最终处理失败的事件](#保存最终处理失败的事件)
- [需要拦截或过滤消息时](#需要拦截或过滤消息时)
  - [在处理函数之前跳过消息](#在处理函数之前跳过消息)
  - [在处理函数前后插入代码](#在处理函数前后插入代码)
  - [改变或停止一次发布](#改变或停止一次发布)
  - [改变或停止每次发布](#改变或停止每次发布)
- [配置内置 local 事件总线](#配置内置-local-事件总线)
  - [直接创建](#直接创建)
- [选择和接入第三方实现](#选择和接入第三方实现)
  - [使用别人已经写好的实现](#使用别人已经写好的实现)
  - [自己开发一种传递实现](#自己开发一种传递实现)
  - [跨进程实现需要编码时](#跨进程实现需要编码时)
- [异步总线与订阅](#异步总线与订阅)
- [非阻塞通知入口](#非阻塞通知入口)
  - [在启动时创建通知发布器](#在启动时创建通知发布器)
  - [在请求路径上入队](#在请求路径上入队)
  - [停机时排空队列](#停机时排空队列)
- [生命周期、等待与停机](#生命周期等待与停机)
  - [同步总线的停机流程](#同步总线的停机流程)
  - [请求关闭并异步观察](#请求关闭并异步观察)
  - [等待某个主题空闲](#等待某个主题空闲)
  - [异步总线的停机流程](#异步总线的停机流程)
- [错误、诊断与排障](#错误诊断与排障)
- [边界与实践清单](#边界与实践清单)
- [延伸阅读](#延伸阅读)

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

<!-- event-bus-source: examples/local_delivery.rs -->
```rust
// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = EventBus::local(LocalEventBusConfig::new())?;
    let topic = Topic::<String>::new("orders.created")?;
    let (sender, receiver) = mpsc::channel();
    let _subscription = bus.subscribe(SubscribeRequest::new("audit", topic.clone())?, move |delivery| {
        sender.send(delivery.payload().clone()).unwrap();
    })?;
    bus.publish(PublishRequest::new(topic, "order-42".to_owned())?)?;
    assert_eq!(receiver.recv_timeout(Duration::from_secs(3))?, "order-42");
    bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })?;
    Ok(())
}
```


0.18.0 尚未发布。若应用目录与 `rs-event-bus` 检出目录并列，
在应用的 `Cargo.toml` 中同时设置直接依赖和传递依赖的本地解析：

```toml
[dependencies]
qubit-event-bus = { version = "0.18.0", path = "../rs-event-bus" }

[patch.crates-io]
qubit-event-bus = { path = "../rs-event-bus" }
```

按实际目录调整路径。只有 0.18 发布后，才能移除 path/patch 并从注册表使用
`qubit-event-bus = "0.18"`。详见[0.18 迁移说明](migration.zh_CN.md#从-017-升级到-018)。

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

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::SubscribeRequest;

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

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::SubscribeRequest;

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
use qubit_event_bus::EventBus;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;

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

`bus.publish(...)` 返回的 `Err(PublishFailure)` 表示这次调用没有正常拿到接收情况报告。即使返回 `Ok(receipt)`，也可能只有部分处理方收到消息。`receipt` 就是这份报告；调用 `receipt.admission_outcome()` 可区分以下情况：

| 结果 | 含义 |
| --- | --- |
| `Accepted(summary)` | 至少一个处理方接收，没有处理方拒绝；某些处理方仍可能按规则跳过。 |
| `PartiallyAccepted(summary)` | 有人接收，也有人拒绝；直接重发可能使已接收者处理两次。 |
| `NoneAccepted(summary)` | 找到了处理方，但无人接收；查看跳过或拒绝原因。 |
| `NoDestinations` | 没找到处理方；检查是否先完成订阅，主题名是否一致。 |
| `OpaqueAccepted` | 消息传递实现说已接收，但不告诉你具体是哪些处理方。 |
| `Dropped` | 发布前的拦截规则主动丢弃了消息。 |

### 判断发布失败后能否重发

`publish` 及 `publish_all` 的每项错误都是 `PublishFailure`；用 `event_id()` 追踪原事件，用 `effect()` 判断接纳是否确定，用 `cause()` 和 `source()` 查看保留的错误链。`NotificationOutcome::PublishFailed` 和 `PublishErrorHandler` 也接收该包装。`NotAccepted` 表示确定没有接纳；`MayHaveBeenAccepted` 表示即使调用报错，provider 仍可能已接纳。应用应记录事件 ID，并通过业务存储核对或由幂等消费者收敛结果。

`PublishOptions::builder().duplicate_risk_policy(...)` 默认选择 `DuplicateRiskPolicy::Forbid`。发生未知效果的尝试后，安全门先于自定义 `RetryRule` 停止重试。`AllowDuplicates` 只是允许原有 `qubit-retry` 策略继续判断；它本身既不启用重试，也不保证一定重试。各次尝试共用事件 ID、时间戳和编码字节。typed interceptor 可以转换 envelope，但不能改变事件 ID。

一条逻辑发布曾经出现未知效果，后续即使确定拒绝，最终错误也仍是 `MayHaveBeenAccepted`。允许重复后最终成功时，若前序尝试效果未知，`receipt.duplicate_possible()` 为 true。provider 接纳成功仍不代表 handler 完成或记录已经持久化。

取消也影响结果判断。`RetryCancellationToken` 取消已经开始轮询的 SPI 尝试并使调用返回错误时，接纳效果未知；在 SPI 调用前取消则没有接纳效果。丢弃公开 publish future 不会产生返回的错误；未轮询 future 被丢弃时没有尝试，已启动操作被丢弃后，应用须保留事件 ID 并按可能已发布处理。RetryPolicy 预算是软预算，不承诺所有执行中的 provider 操作都能被硬超时中断，尤其是同步 I/O。

死信转发使用相同安全门。转发失败或结果未知时，源订阅停止，持久源消息不结算。转发与源确认是两个操作：转发成功后源结算失败，可能再次产生相同逻辑死信。消费者必须去重；Redis EventId 和该策略均不提供恰好一次保证。

### 成功时能看到什么

在订单示例中，审计和客户视图两个订阅都已建立，订单事务提交后发布一条 `OrderCreated`，正常情况下回执如下：

```rust
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;

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
use qubit_event_bus::model::AdmissionCheckError;
use qubit_event_bus::model::AdmissionRequirement;

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
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::PublishAcknowledgement;

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
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::PublishRequest;

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
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;

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
| 过滤事件 | `filter` | 总线取到消息后、处理函数运行前，查看事件内容并决定是否跳过。在 local 上，被跳过的消息在发布回执里仍是 `Accepted`；跳过不等于拒绝。示例见[需要拦截或过滤消息时](#需要拦截或过滤消息时)。 |
| 由处理函数决定何时确认 | `ack_mode(AckMode::Manual)` | 写入业务数据后调用 `delivery.acknowledgement().ack()`；处理失败可调用 `nack()`。未作决定就返回会被视为失败。示例见[由处理函数决定何时确认](#由处理函数决定何时确认)。 |
| 失败后重试 | `retry_policy`，可配 `retry_rule` / `retry_cancellation_token` | 重试次数和间隔由策略决定；单独设置错误分类规则不会启动重试。使用这些类型时需直接依赖 `qubit-retry = "0.25"`。示例见[数据库写入失败后自动重试](#数据库写入失败后自动重试)。 |
| 失败后选择动作 | `error_handler` | 可要求重试、重新放回队列、转入失败消息主题或放弃；重新入队需要所用实现支持。 |
| 保存最终处理失败的事件 | `dead_letter(DeadLetterPolicy::with_topic_name(name)?)` | 把失败消息转发到另一个主题（死信主题），还需有人订阅并处理它。示例见[保存最终处理失败的事件](#保存最终处理失败的事件)。 |
| 同一客户的消息按顺序处理 | `ordering_policy(OrderingPolicy::PerKey)` | 发布方须为事件设置顺序键，所用实现还须支持此能力；见[保证同一对象的处理顺序](#保证同一对象的处理顺序)。 |
| 多个实例分工或读取旧消息 | `consumer_group`、`durability`、`start_position` | 只有支持这些能力的传递实现可使用；local 不支持持久订阅和历史读取。 |
| 底层实现专有参数 | `provider_option` | 具体含义由该实现说明；不要放密码。 |

处理函数可返回 `()` 或 `Result<(), DeliveryError>`。如果数据库写入失败，应返回错误，让库按订阅策略处理。`delivery.payload()` 取得业务数据，`delivery.event()` 可取得事件 ID 和附带信息，`delivery.context()` 可取得传递实现、订阅者以及当前是第几次尝试（`retry_attempt()`，从 1 开始）。重试可能带来重复处理，因此处理方应使用业务唯一键。下面以客户视图和审计两个订阅为例，给出三种最常用的配置。

### 数据库写入失败后自动重试

客户视图写入偶发超时时，让库自动重试。重试策略类型来自 `qubit-retry`，需要在应用中直接依赖 `qubit-retry = "0.25"`：

```rust
use std::time::Duration;

use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_retry::BackoffPolicy;
use qubit_retry::RetryPolicy;

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
use qubit_event_bus::DeliveryError;
use qubit_event_bus::model::AckMode;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;

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
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

// 死信主题的载荷类型是 DeadLetterEvent<OrderCreated>，不是 OrderCreated。
let dead_letter_topic = Topic::<DeadLetterEvent<OrderCreated>>::new("orders.created.dead")?;

// 失败方：处理失败时转入上面的死信主题。
let options = SubscribeOptions::<OrderCreated>::builder()
    .error_handler(|_event, _error| FailureDirective::DeadLetter)
    .dead_letter(DeadLetterPolicy::with_topic(&dead_letter_topic))
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let view_subscription = bus.subscribe(request, move |delivery| store.upsert_order(delivery.payload()))?;

// 读取方：订阅同一个死信主题，记录失败原因供人工或后台任务补处理。
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

`error_handler` 返回 `DeadLetter` 时，facade 会把失败消息转发到死信主题，即使同时配置了 `retry_policy` 也是如此。如果转发失败，异步 runner 会以 `ReceiveError::DeadLetterForwardFailed` 停止；同步 facade 会取消该订阅。源 token 保持未结算，诊断中会报告该失败。恢复方式取决于 provider 的 durability 和 close 语义：durable provider 可在恢复或重建订阅后重新投递未结算消息；ephemeral provider 关闭时可能丢弃消息。不要假设转发持续失败期间 facade 会重新运行 handler。同一原始事件和 subscriber 的死信事件 ID 保持稳定，可帮助消费者去重，但不保证 exactly-once。死信主题仍是普通主题，必须有人订阅；空主题仍可能丢失记录。使用编码传递实现时，还要为 `DeadLetterEvent<OrderCreated>` 注册 codec。`NoDestinations`、`NoneAccepted`、`Dropped` 和发布错误都不算转发成功。已知的部分接纳会结束源投递并记录诊断，因为整体重发可能造成重复。Redis 等 opaque provider 只有在发布保证至少为 Accepted 时才满足默认策略。需要确认具体消费者接纳时，配置 `DeadLetterPolicy::with_known_destination(topic_name)`；opaque provider 会在订阅创建时拒绝该配置。请监控转发失败诊断。

## 需要拦截或过滤消息时

**过滤**发生在接收方：处理函数运行前，先判断这条消息要不要交给它。**拦截器**则是一段放在发布或处理路径中的自定义代码，适合添加关联信息、记录日志或主动停止后续步骤。普通接入不需要先配置拦截器。

订阅侧先执行 `filter`。返回 `true` 时继续；返回 `false` 时按已接纳结算这次投递，订阅拦截器和处理函数都不会运行。过滤器通过之后，顺序是总线级订阅拦截器、该订阅自己的拦截器、最后才是处理函数。每个拦截器都会拿到 `next`，只有调用它，后面的步骤才会执行。同步订阅用 `interceptor` 登记，异步订阅用 `async_interceptor`。同步总线上配置异步拦截器，或异步总线上配置同步拦截器，都会在创建订阅时失败。

### 在处理函数之前跳过消息

客户视图可以不保存金额为零的订单：

```rust
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;

let options = SubscribeOptions::<OrderCreated>::builder()
    .filter(|event| event.payload().total_cents > 0)
    .build();
let request = SubscribeRequest::new("customer-view", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, move |delivery| store.upsert_order(delivery.payload()))?;
```

金额为零时不会调用 `upsert_order`。内置 local 上，该目标在发布回执里仍是 `Accepted`。`filter` 发生 panic 时，按处理函数失败报告。

### 在处理函数前后插入代码

审计订阅可以先记下订单号，再通过 `next` 进入处理函数：

```rust
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;

let options = SubscribeOptions::<OrderCreated>::builder()
    .interceptor(|delivery, next| {
        eprintln!("审计收到 {}", delivery.payload().order_id);
        next(delivery)
    })
    .build();
let request = SubscribeRequest::new("audit-log", OrderCreated::TOPIC)?.with_options(options);
let subscription = bus.subscribe(request, move |delivery| store.append_order_created(delivery.payload()))?;
```

`next(delivery)` 表示继续后面的链。日志输出之后才会写存储。拦截器直接返回、没有调用 `next` 时，处理函数不会执行。拦截器返回的值就是这次投递的结果。

同一段同步回调也可以包住这条总线上所有 `OrderCreated` 订阅。它位于该订阅自己的拦截器外侧，而且只在这条订阅的 `filter` 返回 `true` 之后运行。它要在组装总线配置时登记；`EventBus::local` 接不了这份配置：

```rust
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::SubscriberNext;

let bus_settings = EventBusFacadeConfig::new().subscriber_interceptor(
    |delivery: Delivery<OrderCreated>, next: SubscriberNext<OrderCreated>| next(delivery),
);
```

异步订阅要 `await` 自己的 `next`，返回类型是 `SpiFuture`：

```rust
use qubit_event_bus::DeliveryError;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::spi::SpiFuture;

let options = SubscribeOptions::<OrderCreated>::builder()
    .async_interceptor(|delivery: Delivery<OrderCreated>, next: AsyncSubscriberNext<OrderCreated>| {
        Box::pin(async move { next(delivery).await }) as SpiFuture<'static, Result<(), DeliveryError>>
    })
    .build();
```

把 `options` 放进 `SubscribeRequest::with_options`，再 `subscribe(...).await`。对应的总线级方法是 `EventBusFacadeConfig::async_subscriber_interceptor`。

### 改变或停止一次发布

挂在单次请求上的发布拦截器只看这一次的事件信封。返回 `Ok(None)` 表示停止，返回 `Ok(Some(envelope))` 表示带着你返回的信封继续：

```rust
use qubit_event_bus::model::PublishRequest;

let request = PublishRequest::builder()
    .topic(OrderCreated::TOPIC)
    .payload(event)
    .interceptor(|mut envelope| {
        if envelope.payload().total_cents == 0 {
            return Ok(None);
        }
        envelope.set_header("request-id", "req-42")?;
        Ok(Some(envelope))
    })
    .build()?;
let receipt = bus.publish(request)?;
```

`Ok(None)` 会在调用 provider 之前结束。`receipt.admission_outcome()` 为 `Dropped`，这次调用也不会进入总线级发布拦截器。`Ok(Some(envelope))` 会带着返回信封上的 header 继续。返回 `Err` 则这次发布失败。

### 改变或停止每次发布

`EventBusFacadeConfig::publisher_interceptor` 作用于通过该总线对象发出的每条消息，并且排在“已经返回信封”的请求级拦截器之后。它可以修改 header，不能修改载荷或事件 ID。`Ok(false)` 停止发布，`Ok(true)` 继续：

```rust
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishMetadata;

let local = LocalEventBusConfig::default();
let bus_settings = EventBusFacadeConfig::new().publisher_interceptor(|metadata: &mut PublishMetadata| {
    if metadata.header("suppress") == Some("true") {
        return Ok(false);
    }
    metadata.set_header("service", "orders")?;
    Ok(true)
});
let config = EventBusConfig::default()
    .with_provider_options(local.provider_options())
    .with_facade_config(bus_settings);
let bus = EventBusRegistry::with_local()?.create(&config)?;
```

`Ok(false)` 时接纳结果为 `Dropped`，provider 不会被调用。发布错误处理器只观察最终的发布失败，不能把失败改成成功；被拦截器丢掉的发布也不会调用它。

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
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusRegistry;
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

`SyncDeliverySchedulerConfig::new` 的第一个值必须大于零；第二个值可以为 0，表示只能把消息立即交给空闲工作线程。同步 facade 默认最多创建 256 个活跃订阅接收线程。可以用 `SyncDeliverySchedulerConfig::with_max_subscription_workers(NonZeroUsize::new(64).unwrap())` 调整上限；超过上限时会在调用 provider 创建订阅前失败。此设置限制线程数，不会降低每个阻塞接收线程的开销；订阅量更大时可评估异步总线。使用注册表创建 local 时，要把 `local.provider_options()` 传给 `EventBusConfig`；它包含 `local.queue_capacity` 和 `local.max_total_outstanding` 两个配置键。未知的键、非数字值或零值会在创建时被拒绝。直接调用 `EventBus::local` 只能设置 local 的积压容量；要修改处理并发、编码器或拦截器，就通过 `EventBusRegistry::with_local()` 创建。

编码传输使用 `EventBusFacadeConfig::with_payload_limits(PayloadLimits::new(publish_limit, receive_limit))`，两个参数均为正数 `NonZeroUsize`，默认各 1,048,576 字节；恰好达到上限仍允许。发布在编码完成后、调用 provider 前检查，接收在任何 codec 回调前检查。没有无限额配置。该检查不限制 codec 内部或传输客户端的预先分配。facade 无法可靠计算原生 Rust payload 的递归占用，本地队列仍按投递条数限流。

异步总线也使用 `LocalEventBusConfig`。例如把同时处理的消息数设为 8：

```rust
use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::DeliveryAdmissionConfig;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
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

有些第三方 crate 支持自动登记。它在程序链接时把自己的定义放入一个目录，这项机制叫 `discovery`（发现）。这种情况下，应用启用 feature，并确保该 crate 被链接。尚未发布时，
仍须保留上面的本地 path 和 `[patch.crates-io]`：

```toml
qubit-event-bus = { version = "0.18.0", path = "../rs-event-bus", features = ["discovery"] }
qubit-spi = "0.13"
# 再加入所选 provider crate 的实际包名和版本。
```

```rust
use provider_crate as _; // 用实际 crate 名替换，确保 provider 定义链接进可执行程序
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
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

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusProviderError;
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::spi::EventBusSpi;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderId;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
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
use qubit_event_bus::EventBusSpec;
use qubit_event_bus::registry::sync_provider_inventory::Entry;
use qubit_spi::submit_sync_provider;

submit_sync_provider! {
    inventory_entry = Entry;
    spec = EventBusSpec;
    provider = MyProvider;
}
```

异步实现使用独立的异步目录和 `submit_async_provider!`；可参考[内置异步实现源码](../src/local/async_local_event_bus_provider.rs)。接入生产系统前，应重点验证：等待超时或关闭时的结果是否正确；重复确认是否安全；异步操作被取消后消息是否仍可重新处理；关闭后是否仍保留未确认消息；重复关闭是否安全。详细接口契约见 [架构设计](design.zh_CN.md) 和 [SPI API](https://docs.rs/qubit-event-bus/latest/qubit_event_bus/spi/)。

### 跨进程实现需要编码时

内置 local 直接传递 Rust 对象，不需要转换。消息要跨进程传递时，通常需要先把对象转换成字节，接收时再还原；负责这件事的组件叫**编码器**（codec）。可以用 `Topic::new_with_codec` / `new_with_shared_codec` 为某类事件指定编码器，也可以把编码器放进 `CodecRegistry`，再通过 `EventBusFacadeConfig::with_codec_registry` 配给总线。主题自带编码器优先；没有才查总线的注册表。创建订阅时会选定编码器，两处都没有时会报错。应用还要约定数据格式与版本兼容方式；本库不内置通用 JSON 编码器。

Codec 回调受 panic 边界保护。`encode` 返回错误或编码/元数据回调 panic 时，发布会在调用 provider 前失败；validate/decode panic 会转换为 `CodecError::Panicked` 并停止该订阅，不结算源消息。元数据不兼容、接收字节超限或 Native 类型不匹配也会停止接收。普通 `CodecError::Decode` 仍作为无效消息拒绝。可运行的最小 `String` 实现见[codec 往返示例](../examples/codec_round_trip.rs)。下面的片段为订单事件接上编码器。字节格式由应用自己约定：三行依次是 `order_id`、`customer_id` 和 `total_cents`，且字段中不含换行。本库不提供这种格式。

下面的应用模块从前文定义的 `orders::events` 导入 `OrderCreated`。此完整 codec 模块由文档 fixture 编译验证：

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/order_created_codec.rs -->
```rust
// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Order-event codec compiled from the bilingual user guides.

use std::sync::Arc;

use qubit_event_bus::CodecError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::spi::EncodedPayload;

use crate::orders::events::OrderCreated;

pub struct OrderCreatedCodec(pub ContentType);

impl EventCodec<OrderCreated> for OrderCreatedCodec {
    fn content_type(&self) -> &ContentType {
        &self.0
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &OrderCreated) -> Result<Arc<[u8]>, CodecError> {
        let text = format!("{}\n{}\n{}", value.order_id, value.customer_id, value.total_cents);
        Ok(Arc::from(text.into_bytes()))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<OrderCreated, CodecError> {
        let text = std::str::from_utf8(payload.bytes()).map_err(|source| CodecError::Decode { source: Box::new(source) })?;
        let mut lines = text.lines();
        let order_id = lines.next().unwrap_or("").to_owned();
        let customer_id = lines.next().unwrap_or("").to_owned();
        let total_cents = lines
            .next()
            .unwrap_or("")
            .parse::<u64>()
            .map_err(|source| CodecError::Decode { source: Box::new(source) })?;
        if lines.next().is_some() || order_id.is_empty() || customer_id.is_empty() {
            return Err(CodecError::Decode {
                source: Box::new(std::io::Error::other("expected order_id, customer_id, and total_cents")),
            });
        }
        Ok(OrderCreated {
            order_id,
            customer_id,
            total_cents,
        })
    }
}
```

`encode` 交出共享字节和内容类型；`decode` 还原 `OrderCreated`，无法还原时返回 `CodecError::Decode`。发布方和订阅方使用同一份带编码器的主题。常量 `OrderCreated::TOPIC` 没有编码器，编码型 provider 不会用它来转换字节：

```rust
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

let topic = Topic::new_with_codec(
    "orders.created",
    OrderCreatedCodec(ContentType::new("text/plain")?),
)?;
let subscription = bus.subscribe(
    SubscribeRequest::new("audit-log", topic.clone())?,
    move |delivery| store.append_order_created(delivery.payload()),
)?;
let receipt = bus.publish(PublishRequest::new(topic, event)?)?;
```

发布时，provider 收到的是 `encode` 的字节；投递时，`decode` 还原的值会交给处理函数。内置 local 仍直接传递 Rust 值，不会调用这个编码器。

同一载荷类型的多个主题可以共用注册到总线上的编码器。主题自带的编码器仍然优先；注册表只在主题没有编码器时使用。编码型订阅若两处都没有编码器，会在 provider 创建订阅之前失败，错误是 `SubscribeError::Capability(CapabilityError::CodecRequired)`：

```rust
use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::codec::CodecRegistry;
use qubit_event_bus::model::ContentType;

let mut codecs = CodecRegistry::new();
codecs.register::<OrderCreated>(Arc::new(OrderCreatedCodec(ContentType::new("text/plain")?)));
let bus_settings = EventBusFacadeConfig::new().with_codec_registry(Arc::new(codecs));
let config = EventBusConfig::default().with_facade_config(bus_settings);
```

把 `config` 交给编码型 provider 的注册表 `create`，方式和前面把 facade 设置交给 local 一样。此时 `Topic::new("orders.created")` 会从总线找到 `OrderCreatedCodec`。如果手上已有 `Arc<dyn EventCodec<OrderCreated>>`，也可以用 `Topic::new_with_shared_codec` 挂到主题上。

## 异步总线与订阅

<!-- event-bus-source: tests/fixtures/documentation_consumer/src/bin/async_local.rs -->
```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous local delivery example compiled by the user-guide checks.

use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = AsyncEventBus::local(LocalEventBusConfig::new()).await?;
    let topic = Topic::<String>::new("orders.created")?;
    let mut subscription = bus
        .subscribe(SubscribeRequest::new("audit", topic.clone())?)
        .await?;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let runner = tokio::spawn(async move {
        subscription
            .run(move |delivery| {
                let sender = sender.clone();
                async move {
                    sender.send(delivery.payload().clone()).unwrap();
                    Ok(())
                }
            })
            .await
    });
    bus.publish(PublishRequest::new(topic, "order-42".to_owned())?)
        .await?;
    let delivered = tokio::time::timeout(Duration::from_secs(3), receiver.recv()).await?;
    assert_eq!(delivered.as_deref(), Some("order-42"));
    bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(3),
    })
    .await?;
    runner.await??;
    Ok(())
}
```


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

应用启动时应把 `run(...)` 放进后台任务，再开放业务入口；如果在启动函数中直接等待它，后面的启动步骤就不会执行。取消这次 `run` 但保留订阅句柄，之后还能再次运行；调用 `close().await` 或丢弃句柄则结束订阅。本地异步实现关闭时会丢弃仍在排队和未处理完的消息，再次用同一 ID 订阅也会从空队列开始。直接使用 local SPI 时，如果等待中的 `receive` future 被取消，它尚未取走消息，之后调用 `receive` 仍可收到该消息；关闭 receiver 会唤醒等待中的 `receive`，返回 `Closed`。`wait_for_received_deliveries` 只等待总线已取到的消息，不检查传递实现里是否还有排队消息；异步总线没有同步版的 `wait_for_idle`。

### 恢复因编码边界失败而停止的订阅

`decode(&EncodedPayload)` 可以读取 `payload.bytes()`、`content_type()` 和 `schema_id()`。默认 `validate_metadata` 精确比较 content type 文本和 `Option<SchemaId>`；`None` 与具名 schema 不兼容，也不会自动规范化 MIME 文本。需要读取历史版本时，须明确重写验证方法、记录允许的版本集合，并选用对应解码逻辑。直接调用 codec 时由调用方验证元数据；facade 会自动验证。

接收顺序是字节限额、元数据验证、解码，最后才运行 filter/middleware/handler。超限输入不会进入任何 codec 回调，元数据不兼容时不会调用 decode。`MetadataMismatch`、接收 `PayloadTooLarge`、`Panicked` 和 `NativeTypeMismatch` 会停止订阅，不执行 `Accept`、`Reject` 或 `Retry`。普通 `CodecError::Decode` 仍作为坏消息拒绝，不能用它表达可恢复的 schema 不兼容。

通过 `subscription.terminal_failure()` 查看首个保留的 `Arc<SubscriptionStopReason>`。`Codec` 包含事件 ID 和结构化 codec 错误；无法可信解析事件 ID 时，`Provider` 包含 provider 错误。异步 `run()` 返回 `ReceiveError::Stopped`；在同一 handle 上再次运行会返回同一原因，不再接收或解码。已经启动的 handler 按原生命周期完成；关闭错误单独报告，不覆盖终止原因。取消 run 或 close future 也不会清除原因。

Redis 等持久 provider 的恢复步骤是：停止旧 handle，修复 codec/版本或容量配置，再使用同一个 durable group 创建新订阅，由 provider 认领未结算记录。不要为消除错误直接确认或删除记录。临时 provider 销毁 receiver 时可能丢弃相关消息，facade 对已知损失计数一次；重新订阅无法取回已经丢弃的工作。其他健康订阅继续运行。

## 非阻塞通知入口

如果产生消息的代码不能停下来等待同步发布，可使用 `NotificationPublisher<T>`：它先把消息放进一个有容量上限的队列，再由一个后台线程逐条发布。默认最多排队 256 条。`try_publish(payload)` 只说明**成功入队**，并非已经发布；队列满时返回 `TryPublishError::Full(payload)`，关闭后返回 `Closed(payload)`，原数据会还给调用方。观察回调收到 `Published(receipt)`、`PublishFailed(error)` 或 `RequestFailed(error)`；这里的 `Published` 仍不表示处理函数完成。`stats()` 可查看计数。

### 在启动时创建通知发布器

回到订单场景：订单服务的请求线程只想在事务提交后把 `OrderCreated` 交出去，不想在请求路径上等待 `bus.publish` 逐个询问订阅者。应用启动时，在总线和两个订阅建立之后创建一个通知发布器；一个发布器只服务一个主题。原本在请求路径上做的回执检查，改到观察回调里完成：

```rust
use std::num::NonZeroUsize;

use qubit_event_bus::NotificationOutcome;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::model::AdmissionRequirement;

// bus 是已经建立好 audit-log 和 customer-view 订阅的 EventBus。
// 不想自定义容量时，第三个参数可以传 NotificationPublisher::<OrderCreated>::default_capacity()。
let notifier = NotificationPublisher::new(
    bus.clone(),
    OrderCreated::TOPIC,
    NonZeroUsize::new(1_024).expect("capacity is non-zero"),
    |outcome| match outcome {
        NotificationOutcome::Published(receipt) => {
            if let Err(error) =
                receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected)
            {
                eprintln!("订单事件 {} 接纳异常：{error}", receipt.input_event_id().as_str());
            }
        }
        NotificationOutcome::PublishFailed(error) => eprintln!("发布订单事件失败：{error}"),
        NotificationOutcome::RequestFailed(error) => eprintln!("无法为订单事件生成 ID：{error}"),
        // NotificationOutcome 标记为 non_exhaustive，需要兜底分支。
        _ => {}
    },
)?;
```

`new` 会启动名为 `event-notification-publisher` 的后台线程；线程无法创建时返回 `io::Error`。观察回调在这个后台线程上运行，每发布一条就被调用一次，`receipt` 与直接调用 `bus.publish` 得到的回执相同，可以按[检查发布结果](#检查发布结果)中的方法判断。回调里只应做记录日志、更新指标这类很快返回的工作。

### 在请求路径上入队

订单事务提交后，请求线程把事件交给 `try_publish`，这一步不等待任何订阅者：

```rust
use qubit_event_bus::TryPublishError;

match notifier.try_publish(event) {
    Ok(()) => {}
    Err(TryPublishError::Full(event)) => {
        // 队列已满，event 原样归还。可以记录为待补发，或者退回同步 bus.publish。
        eprintln!("通知队列已满，订单 {} 的事件未入队", event.order_id);
    }
    Err(TryPublishError::Closed(event)) => {
        // 通知发布器已经开始关闭，说明应用正在停机。
        eprintln!("通知发布器已关闭，订单 {} 的事件未入队", event.order_id);
    }
}
```

`Ok(())` 表示事件已经进入队列；后台线程随后调用 `bus.publish`，结果通过上面的观察回调交给应用。队列满或已关闭时，原来的 `event` 会随错误一起还回来，调用方可以自行决定补发或记录，不必再复制一份。队列长度按条数计，处理函数变慢会让队列逐渐填满，`Full` 是应用需要关注的信号。

运行期间可用 `stats()` 查看计数：

```rust
let stats = notifier.stats();
println!(
    "enqueued={} published={} queue_full={} publish_errors={}",
    stats.enqueued(),
    stats.published(),
    stats.queue_full(),
    stats.publish_errors()
);
```

`enqueued` 是进入队列的条数，`published` 是拿到回执的条数，`queue_full` 与 `queue_closed` 分别是因队列满和已关闭而被拒绝的次数，`publish_errors` 和 `request_errors` 对应两种失败结果。各计数独立读取，不是同一瞬间的一致快照；`published` 计的是回执，不是处理函数完成的次数。

### 停机时排空队列

`close()` 停止接收新通知，处理完已入队消息并等待后台线程退出。停机需要限定等待时间时，可调用 `close_with_timeout(Duration::from_secs(30))`。期限到达会返回 `io::ErrorKind::TimedOut`；worker 会继续运行，可能继续发布已经接纳的通知。此后 `try_publish` 返回 `Closed`，之后可以再次调用 `close()` 或 `close_with_timeout()` 等待 worker 结束。超时不能中断正在执行的同步 provider 调用。两种关闭方法都不会关闭通知发布器使用的事件总线。观察回调运行在后台线程上，应尽快返回；不要从回调内部调用同一个通知发布器的任一关闭方法。只丢弃句柄不会等待队列处理完，停机时应显式关闭通知发布器，再关闭总线：

```rust
use std::io;
use std::time::Duration;

match notifier.close_with_timeout(Duration::from_secs(30)) {
    // 队列已排空，后台线程已退出。
    Ok(()) => {}
    Err(error) if error.kind() == io::ErrorKind::TimedOut => {
        // 后台线程仍在发布已入队的通知；此时 try_publish 已返回 Closed。
        // 可以记录日志后继续停机，或再次调用 close() 一直等到它结束。
        eprintln!("等待通知队列排空超时：{error}");
    }
    Err(error) => eprintln!("通知发布器关闭失败：{error}"),
}
// 之后再取消订阅、关闭总线，见下一节。
```

后台线程持有 `bus` 的一个副本，因此要先关闭通知发布器，再关闭总线；顺序反过来，排队中的通知会在发布时得到总线已关闭的错误，只能在观察回调里看到 `PublishFailed`。

worker 完成状态包含资源清理，也包含 observer 捕获对象的析构。清理过程 unwind 会使 `worker_panicked` 增加一次；所有关闭调用者都观察到同一个失败终态，不会因 worker 已退出而永久等待。worker 自己调用 close 会返回 `io::ErrorKind::Other`，不关闭入队入口。observer 调用 panic 仍独立隔离，后续通知继续处理。关闭超时后可再次等待，但无法强制中断用户代码。

## 生命周期、等待与停机

启动顺序是：创建总线 → 登记所有处理函数 → 开始接收业务请求。停机时先停止新业务请求，再关闭通知发布器等消息来源，最后处理订阅和总线。同步订阅用 `cancel()`，异步订阅用 `close().await`。如果需要尽量完成已经接收的消息，要根据所用传递实现决定取消订阅和关闭总线的先后顺序，并在该实现上验证；取消订阅本身不表示业务已经写入成功。

### 同步总线的停机流程

下面把订单服务的同步停机流程写出来。`bus`、`audit_subscription`、`view_subscription` 和 `notifier` 都是启动时创建并由应用保留的句柄；这段代码应放在程序最外层的关闭流程中，例如收到终止信号之后：

```rust
use std::time::Duration;

use qubit_event_bus::ShutdownError;
use qubit_event_bus::WaitOutcome;
use qubit_event_bus::spi::ShutdownMode;

// 1. 应用先停止接收新的下单请求（HTTP 监听器等由应用自己控制）。
// 2. 关闭消息来源：排空通知队列，见上一节。
notifier.close_with_timeout(Duration::from_secs(30))?;
// 3. 等待总线已经取到的 OrderCreated 处理完，最多等 10 秒。
let outcome = bus.wait_for_received_deliveries(&OrderCreated::TOPIC, Some(Duration::from_secs(10)))?;
if outcome == WaitOutcome::TimedOut {
    eprintln!("仍有 OrderCreated 处理函数未返回，继续停机");
}
// 4. 取消订阅：不再从 local 取新消息。
audit_subscription.cancel()?;
view_subscription.cancel()?;
// 5. 关闭总线，最多等 30 秒。
match bus.shutdown(ShutdownMode::Graceful { timeout: Duration::from_secs(30) }) {
    Ok(report) => {
        println!(
            "provider 关闭结果 {:?}，放弃了 {} 条已接纳的投递",
            report.outcome, report.known_abandoned_deliveries
        );
        if report.provider_may_have_abandoned_deliveries {
            eprintln!("传递实现可能还丢弃了无法计数的消息");
        }
    }
    Err(ShutdownError::TimedOut { timeout }) => {
        // 总线仍在后台清理，并且已经拒绝新操作。再调一次 shutdown 可等到最终报告；
        // 改用 Immediate 会把仍在排队的消息退回，但同样要等正在运行的处理函数返回。
        eprintln!("平稳关闭在 {timeout:?} 内未完成");
        let ticket = bus.request_shutdown(ShutdownMode::Immediate)?;
        let report = ticket.wait(Some(Duration::from_secs(30)))?;
        eprintln!("最终报告：{report:?}");
    }
    Err(error) => return Err(error.into()),
}
```

订单示例正常停机时，`report.outcome` 为 `ShutdownOutcome::Complete`，`known_abandoned_deliveries` 为 0。`provider_may_have_abandoned_deliveries` 在内置 local 上总是 `true`：local 是非持久实现，无法证明进程内没有消息随关闭丢失，这个标志只是提醒，不是错误。`known_abandoned_deliveries` 大于 0，说明有已经接纳但没来得及处理的消息被放弃了；这些订单的审计记录和客户视图需要由应用的补偿机制补齐。

`ShutdownMode::Graceful { timeout }` 会停止接收新工作，并尽量完成已经接收的工作。调用方 deadline 到期会返回 `ShutdownError::TimedOut`，不表示 bus 已关闭；同步 bus 仍可能在后台清理，后续再次调用 `shutdown` 可观察最终 `ShutdownReport`。`Immediate` 无法强制终止已经运行的业务代码。不要在同一 bus 的 handler 中调用可能等待该 bus 自身工作的 `shutdown`、`wait_for_idle` 或 `wait_for_received_deliveries`；这些调用会返回 `WouldDeadlock`。应从程序最外层的关闭流程发起停机。

### 请求关闭并异步观察

异步应用如果使用同步 `EventBus`，可以先发起短暂的关闭请求，再异步观察完成。
这组 API 属于 `EventBus`；`AsyncEventBus` 仍通过自身的关闭 future 驱动清理。

```rust
use std::time::Duration;

use qubit_event_bus::{EventBus, EventBusShutdown, ShutdownError, ShutdownReport};
use qubit_event_bus::spi::ShutdownMode;

fn request_graceful(bus: &EventBus) -> Result<EventBusShutdown, ShutdownError> {
    bus.request_shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    })
}

async fn observe(ticket: &EventBusShutdown) -> Result<ShutdownReport, ShutdownError> {
    ticket.wait_async().await
}
```

把 ticket 保留在可取消的观察 future 外。`request_graceful` 关闭接纳入口，启动
或加入后台协调器，不等待 handler、worker 或 provider。`observe` 只借用 ticket：
取消时移除自己的 waker 登记；之后再调用 `observe(&ticket)` 就能继续观察，
无须重发关闭请求。`wait_async` 不阻塞线程，也不创建辅助线程。应用应在自身
executor 上驱动它，并明确设置外层观察期限；上面 30 秒的 mode timeout 传给
provider，不限制 `wait_async`。

同步观察可调用 `ticket.wait(Some(Duration::from_secs(30)))`。观察超时不会消费
ticket，也不停止后台关闭。宽限期结束后，可调用
`bus.request_shutdown(ShutdownMode::Immediate)` 加强当前关闭，并继续观察原
ticket；不需要的新 ticket 可以直接丢弃。每个 ticket 绑定确切的关闭代次，后来
启动重试产生新代时，原 ticket 仍保留原代的结果。对已关闭 bus 发起请求会立即
返回缓存结果。丢弃 ticket 只释放观察登记，丢弃 bus 句柄不会自动发起关闭。

`request_shutdown` 不等待当前回调，因此可以在 bus 回调中调用；同步
`shutdown` 和 `ticket.wait` 在这种上下文里返回 `WouldDeadlock`。异步回调也不能
等待包含自身工作的关闭完成。普通调用方执行同步 `shutdown(Immediate)` 时，
仍会等待正在运行的 handler、协调线程 join 和 provider 关闭，且没有调用方期限。
取消、回滚和 Drop 集成应使用请求加有界观察；期限和 Immediate 都不能杀死阻塞
的同步代码。

IoC 0.3 适配器在 graceful 回调中保存 ticket，abort 回调请求 Immediate；它拥有
的资源 wait 取出原 ticket，释放槽锁后再调用 `wait_async`。
`WaitPolicy::bounded` 为宽限和终止阶段提供预算。取消外层
`ShutdownHandle::wait` 会保留已有资源 wait，恢复观察不会重复请求。
终止超时通过 `incomplete` 报告，并允许继续关闭依赖；这不表示资源已被强行
终止，也不能继续保证未结束消费者的依赖可用。构建失败时应检查
`BuildFailure::cause`，取出 cleanup handle，再显式观察 Immediate 清理报告。
迁移细节见 [0.18 迁移说明](migration.zh_CN.md#从-017-升级到-018)。

### 等待某个主题空闲

同步 `wait_for_idle(&topic, timeout)` 等待所用传递实现报告这个主题已没有排队或未处理完的消息；不支持这项查询时返回 `IdleWaitUnsupported`。`wait_for_received_deliveries` 只等待总线已经取到的消息。两者返回空闲，都不能代替检查数据库和失败记录。

如果停机前想把 local 队列里排队的订单事件也尽量处理完，而不只是总线已取到的那几条，可以在取消订阅之前先等待主题空闲。内置 local 支持这项查询；换成其他传递实现时，用 `IdleWaitUnsupported` 分支退回到只等待已取到的消息：

```rust
use std::time::Duration;

use qubit_event_bus::LifecycleError;
use qubit_event_bus::WaitOutcome;

let timeout = Some(Duration::from_secs(10));
let outcome = match bus.wait_for_idle(&OrderCreated::TOPIC, timeout) {
    Err(LifecycleError::IdleWaitUnsupported) => {
        // 所用传递实现不能报告主题是否空闲，只能等总线已经取到的消息。
        bus.wait_for_received_deliveries(&OrderCreated::TOPIC, timeout)?
    }
    other => other?,
};
if outcome == WaitOutcome::TimedOut {
    eprintln!("10 秒内 orders.created 仍有消息排队或未处理完");
}
```

`Idle` 表示此刻传递实现里这个主题没有排队消息，总线也没有正在处理的投递；停止新业务请求之后得到 `Idle`，再取消订阅就不会留下已接纳却未处理的订单事件。`TimedOut` 只说明等待期满，处理函数可能还在运行。这两个方法只看本进程内的这个总线对象，对于连接消息服务器的实现，它不能说明其他进程的消费者也处理完了。

### 异步总线的停机流程

异步总线的步骤相同，只是等待和关闭都要 `.await`。[异步总线与订阅](#异步总线与订阅)中的 `subscription.run(...)` 由一个后台任务持有，`bus.shutdown` 会关闭订阅接收端，`run` 随之返回 `Ok(())`，因此不必先取回订阅句柄：

```rust
use std::time::Duration;

use qubit_event_bus::WaitOutcome;
use qubit_event_bus::spi::ShutdownMode;

// 1. 停止接收新的下单请求。
// 2. 等待总线已取到的 OrderCreated 处理完。
if let WaitOutcome::TimedOut = bus
    .wait_for_received_deliveries(&OrderCreated::TOPIC, Some(Duration::from_secs(10)))
    .await?
{
    eprintln!("仍有 OrderCreated 处理函数未完成，继续停机");
}
// 3. 关闭总线；运行 subscription.run(...) 的任务会在订阅关闭后结束。
let report = bus
    .shutdown(ShutdownMode::Graceful { timeout: Duration::from_secs(30) })
    .await?;
println!("provider 关闭结果 {:?}", report.outcome);
// 4. 等待 run 任务结束，方式取决于所用执行器，例如 Tokio 的 JoinHandle。
```

如果应用自己保留了 `AsyncSubscription` 句柄，也可以在关闭总线前调用 `subscription.close().await` 单独结束某个订阅。异步 `shutdown` 由返回的 future 驱动：丢弃这个 future 会中断关闭过程，应等它完成。异步总线没有 `wait_for_idle`；`wait_for_received_deliveries` 同样只等待总线已取到的消息，异步 local 关闭时会丢弃仍在排队的消息。

## 错误、诊断与排障

| 现象 | 检查顺序 |
| --- | --- |
| `PublishFailure` | 查看具体错误是配置、编码、传递失败、重试结束还是总线已关闭；重试前确认是否可能已有处理方收到。 |
| `SubscribeError` | 检查订阅设置、所用实现是否支持相关能力、是否需要编码器、总线是否已关闭；启动失败时不要开放业务入口。 |
| `subscribe` 成功、回执也是 `Accepted`，处理函数却始终不执行 | 异步总线：确认已在一个长期任务中运行 `AsyncSubscription::run`，且该任务没有被提前取消，也没有阻塞在它前面的启动步骤上。同步总线：确认处理函数没有被前一条消息长期阻塞，工作线程上限见[配置内置 local 事件总线](#配置内置-local-事件总线)。 |
| `NoDestinations` | 检查主题名、载荷类型、订阅是否先于发布建立、同步订阅句柄是否已被 `cancel()`。 |
| `PartiallyAccepted` / `NoneAccepted` | 逐个检查目标的 `Accepted`、`Filtered`、`Rejected`，再看 local 容量和补偿方案。 |
| 等待空闲但业务无结果 | 查处理函数错误、重试/死信和业务数据库；接收报告与空闲均不代表写入成功。 |
| `IdleWaitUnsupported` | 所用实现不能报告整个主题是否空闲；可以由业务代码记录完成状态，不能把总线已取到的工作当作全部工作。 |
| 平稳关闭超时 | 查是否有处理函数或底层读写一直没返回、是否还有未完成的消息；稍后再获取关闭结果。 |

用 `observe_diagnostics` 可登记一个接收内部问题通知的回调。要持续接收通知，就保留返回的 `DiagnosticObserverHandle`；丢弃它会停止观察。回调会占用触发问题的线程，应尽快返回。`publish_metrics()` 统计发布尝试和 provider 报告的接纳结果，不统计消息接收或业务写入成功次数。日志建议同时记录订单 ID、事件 ID、处理方 ID、重试次数和最终错误。

## 边界与实践清单

- local 只适合允许进程退出时丢失消息、由应用自己补偿的进程内工作；它不提供持久恢复与跨进程通信。
- `publish_all` 无事务保证；发布回执、ACK、空闲和业务提交是不同阶段。
- 总线 API 提供消费组、持久订阅、历史重放、顺序和延迟等选项，并不表示所有传递实现都支持；使用前检查其能力并测试。
- 重要业务应设计持久移交、幂等、失败记录和补偿；特别覆盖部分接纳和重复投递。
- 测试应覆盖正常接收、无人订阅、队列满、处理函数失败、重试/死信和停机；容量与性能要在目标主机测量。

## 延伸阅读

- [中文 README](../README.zh_CN.md) · [迁移指南](migration.zh_CN.md) · [API 文档](https://docs.rs/qubit-event-bus)

## 单仓验证与五仓整体验证

`./project-ci-check.sh` 默认只检查当前 crate 的依赖解析 metadata；独立单仓用户
无须下载全部下游。协调迁移时，运行
`./project-ci-check.sh --ecosystem-root <repos-dir>`，目录下须包含
`rs-event-bus`、`rs-event-bus-redis`、`rs-task`、`rs-ioc` 和
`rs-execution-services`。门禁强制要求五个根目录及声明的七个 consumer fixture，
使用 locked/all-features Cargo metadata 验证，并拒绝同一依赖图混用旧 minor 与
0.18；缺失输入会明确失败。本轮 IoC/EventBus/消费者的三仓更新不升级 rs-task 与
rs-event-bus-redis 的 0.17 依赖，完整五仓门禁须待它们另行迁移到 0.18 后
才能通过。metadata 检查补充各项目 CI，不能单独证明投递行为。
