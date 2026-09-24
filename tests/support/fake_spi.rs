// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Deterministic sync and async transport fakes for SPI contract tests.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

type SyncQueue = Arc<(Mutex<QueueState>, Condvar)>;
type AsyncQueue = Arc<Mutex<QueueState>>;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderMessageMetadata;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DeliveryGap;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;

#[derive(Default)]
struct QueueState {
    messages: VecDeque<InboundMessage>,
    in_flight: Option<InboundMessage>,
    unsettled: usize,
    gaps: usize,
    closed: bool,
    settled: HashMap<String, DeliveryDisposition>,
    settlement_dispositions: Vec<DeliveryDisposition>,
    wakers: Vec<Waker>,
    fail_next_receive: bool,
    fail_next_settle: bool,
    fail_settle_always: bool,
    pause_next_settle: bool,
    panic_next_receive: bool,
    manual_time: Duration,
    receive_waiters: usize,
    wake_observations: usize,
}

pub(crate) fn full_capabilities() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::PerSubscription,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}

pub(crate) fn native_no_settlement_capabilities() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::None,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}

fn spi_error(operation: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "fake".into(),
        operation,
        resource: None,
        kind: "fake_failure",
        retryable: Some(false),
        source: Box::new(std::io::Error::other("injected fake SPI failure")),
    }
}

fn conflicting_settlement_error() -> SpiError {
    SpiError::InvalidSettlementToken {
        provider_id: "fake".into(),
        operation: "settle",
        resource: None,
        reason: "conflicting_disposition",
        retryable: Some(false),
        source: Box::new(std::io::Error::other("settlement disposition is already fixed")),
    }
}

fn settlement_token_key(token: &SettlementToken) -> String {
    token
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| token.downcast_ref::<&str>().map(|value| (*value).to_owned()))
        .unwrap_or_else(|| "unknown".to_owned())
}

pub(crate) fn subscription_request() -> SpiSubscriptionRequest {
    subscription_request_with_id(1)
}

pub(crate) fn subscription_request_with_id(id: u64) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new("test.topic").unwrap(),
        SubscriberId::new("test-subscriber").unwrap(),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        std::any::TypeId::of::<u32>(),
    )
}

pub(crate) fn inbound_message(settlement: Option<SettlementToken>) -> InboundMessage {
    InboundMessage::new(
        TopicAddress::new("test.topic").unwrap(),
        EventId::new("event-test").unwrap(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        TransportPayload::Native(Arc::new(42_u32)),
        settlement,
        Default::default(),
    )
}

pub(crate) fn outbound_message() -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("test.topic").unwrap(),
        EventId::new("event-outbound").unwrap(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(42_u32)),
    )
}

#[derive(Clone)]
pub(crate) struct FakeEventBusSpi {
    capabilities: EventBusCapabilities,
    queues: Arc<Mutex<Vec<(Id, SyncQueue)>>>,
    shutdown_transitions: Arc<Mutex<usize>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
    fail_next_publish: Arc<Mutex<bool>>,
}

impl FakeEventBusSpi {
    pub(crate) fn new() -> Self {
        Self::with_capabilities(full_capabilities())
    }

    pub(crate) fn with_capabilities(capabilities: EventBusCapabilities) -> Self {
        Self {
            capabilities,
            queues: Arc::default(),
            shutdown_transitions: Arc::default(),
            calls: Arc::default(),
            fail_next_publish: Arc::default(),
        }
    }

    pub(crate) fn enqueue(&self, message: InboundMessage) {
        let queues = self.queues.lock().unwrap().clone();
        for (_, queue) in queues {
            let (lock, ready) = &*queue;
            lock.lock().unwrap().messages.push_back(message_for_copy(&message));
            ready.notify_one();
        }
    }

    pub(crate) fn shutdown_transition_count(&self) -> usize {
        *self.shutdown_transitions.lock().unwrap()
    }
    pub(crate) fn operation_log(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
    pub(crate) fn fail_next_publish(&self) {
        *self.fail_next_publish.lock().unwrap() = true;
    }
    pub(crate) fn inject_gap(&self) {
        for (_, queue) in self.queues.lock().unwrap().iter() {
            queue.0.lock().unwrap().gaps += 1;
            queue.1.notify_one();
        }
    }

    fn first_queue(&self) -> Arc<(Mutex<QueueState>, Condvar)> {
        self.queues.lock().unwrap()[0].1.clone()
    }

    pub(crate) fn wait_until_receive_is_blocked(&self) {
        let queue = self.first_queue();
        let (lock, ready) = &*queue;
        let mut state = lock.lock().unwrap();
        while state.receive_waiters == 0 {
            state = ready.wait(state).unwrap();
        }
    }

    pub(crate) fn wake_receivers_spuriously(&self) {
        let queue = self.first_queue();
        queue.1.notify_all();
    }

    pub(crate) fn wait_until_spurious_wake_is_observed(&self) {
        let queue = self.first_queue();
        let (lock, ready) = &*queue;
        let mut state = lock.lock().unwrap();
        while state.wake_observations == 0 {
            state = ready.wait(state).unwrap();
        }
    }
}

// The test payload is a scalar; reconstructing avoids imposing Clone on the SPI
// message.
fn message_for_copy(_: &InboundMessage) -> InboundMessage {
    inbound_message(None)
}

fn inbound_from_outbound(
    message: &OutboundMessage,
    subscription_id: Id,
    settlement: SettlementCapabilities,
) -> InboundMessage {
    let payload = match message.payload() {
        TransportPayload::Native(value) => TransportPayload::Native(value.clone()),
        TransportPayload::Encoded(value) => TransportPayload::Encoded(EncodedPayload::new(
            Arc::from(value.bytes()),
            value.content_type().clone(),
            value.schema_id().cloned(),
        )),
        _ => panic!("fake provider received an unknown transport payload variant"),
    };
    let token = (!matches!(settlement, SettlementCapabilities::None))
        .then(|| SettlementToken::new(subscription_id, message.id().as_str().to_owned()));
    InboundMessage::new(
        message.topic().clone(),
        message.id().clone(),
        message.timestamp(),
        message.headers().clone(),
        message.ordering_key().cloned(),
        payload,
        token,
        ProviderMessageMetadata::default(),
    )
}

impl EventBusSpi for FakeEventBusSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.calls.lock().unwrap().push("publish");
        if std::mem::take(&mut *self.fail_next_publish.lock().unwrap()) {
            return Err(spi_error("publish"));
        }
        let queues = self.queues.lock().unwrap().clone();
        for (subscription_id, queue) in queues {
            let (lock, ready) = &*queue;
            lock.lock().unwrap().messages.push_back(inbound_from_outbound(
                &message,
                subscription_id,
                self.capabilities.settlement(),
            ));
            ready.notify_one();
        }
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        self.calls.lock().unwrap().push("subscribe");
        let queue = Arc::new((Mutex::new(QueueState::default()), Condvar::new()));
        self.queues
            .lock()
            .unwrap()
            .push((request.subscription_id(), queue.clone()));
        Ok(Box::new(FakeEventSubscriptionSpi {
            id: request.subscription_id(),
            settlement: self.capabilities.settlement(),
            queue,
            calls: self.calls.clone(),
        }))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.calls.lock().unwrap().push("shutdown");
        let mut n = self.shutdown_transitions.lock().unwrap();
        if *n == 0 {
            *n = 1;
        }
        Ok(ShutdownOutcome::Complete)
    }
}

pub(crate) struct FakeEventSubscriptionSpi {
    id: Id,
    settlement: SettlementCapabilities,
    queue: Arc<(Mutex<QueueState>, Condvar)>,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl FakeEventSubscriptionSpi {
    #[allow(dead_code)]
    pub(crate) fn settlement_count(&self) -> usize {
        self.queue.0.lock().unwrap().settled.len()
    }
    #[allow(dead_code)]
    pub(crate) fn fail_next_receive(&self) {
        self.queue.0.lock().unwrap().fail_next_receive = true;
    }
    #[allow(dead_code)]
    pub(crate) fn inject_gap(&self) {
        self.queue.0.lock().unwrap().gaps += 1;
        self.queue.1.notify_one();
    }
}

impl EventSubscriptionSpi for FakeEventSubscriptionSpi {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        self.calls.lock().unwrap().push("receive");
        let (lock, ready) = &*self.queue;
        let mut state = lock.lock().unwrap();
        if std::mem::take(&mut state.fail_next_receive) {
            return Err(spi_error("receive"));
        }
        let started = Instant::now();
        while state.messages.is_empty() && state.gaps == 0 && !state.closed && !timeout.is_zero() {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            state.receive_waiters += 1;
            ready.notify_all();
            let (next, _) = ready.wait_timeout(state, remaining).unwrap();
            state = next;
            state.receive_waiters -= 1;
            state.wake_observations += 1;
            ready.notify_all();
        }
        if let Some(message) = state.messages.pop_front() {
            return Ok(ReceiveOutcome::Message(message));
        }
        if state.gaps > 0 {
            state.gaps -= 1;
            return Ok(ReceiveOutcome::Gap(DeliveryGap::new("fake gap", Some(1))));
        }
        if state.closed {
            return Ok(ReceiveOutcome::Closed);
        }
        Ok(ReceiveOutcome::TimedOut)
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        self.calls.lock().unwrap().push("settle");
        if !token.belongs_to(self.id) {
            return Err(spi_error("settle"));
        }
        if matches!(self.settlement, SettlementCapabilities::None) {
            return Err(spi_error("settle"));
        }
        let key = settlement_token_key(token);
        let mut state = self.queue.0.lock().unwrap();
        match state.settled.get(&key) {
            Some(previous) if *previous == disposition => Ok(()),
            Some(_) => Err(conflicting_settlement_error()),
            None => {
                state.settled.insert(key, disposition);
                Ok(())
            }
        }
    }
    fn close(&mut self) -> Result<(), SpiError> {
        self.calls.lock().unwrap().push("close");
        self.queue.0.lock().unwrap().closed = true;
        self.queue.1.notify_all();
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct FakeAsyncEventBusSpi {
    capabilities: EventBusCapabilities,
    queues: Arc<Mutex<Vec<(Id, AsyncQueue)>>>,
    shutdown_transitions: Arc<Mutex<usize>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
    fail_next_publish: Arc<Mutex<bool>>,
    subscribe_paused: Arc<AtomicBool>,
    subscribe_wakers: Arc<Mutex<Vec<Waker>>>,
    close_paused: Arc<AtomicBool>,
    close_wakers: Arc<Mutex<Vec<Waker>>>,
    shutdown_paused: Arc<AtomicBool>,
    shutdown_wakers: Arc<Mutex<Vec<Waker>>>,
    receiver_drop_recoveries: Arc<AtomicUsize>,
}

impl FakeAsyncEventBusSpi {
    pub(crate) fn new() -> Self {
        Self::with_capabilities(full_capabilities())
    }

    pub(crate) fn with_capabilities(capabilities: EventBusCapabilities) -> Self {
        Self {
            capabilities,
            queues: Arc::default(),
            shutdown_transitions: Arc::default(),
            calls: Arc::default(),
            fail_next_publish: Arc::default(),
            subscribe_paused: Arc::default(),
            subscribe_wakers: Arc::default(),
            close_paused: Arc::default(),
            close_wakers: Arc::default(),
            shutdown_paused: Arc::default(),
            shutdown_wakers: Arc::default(),
            receiver_drop_recoveries: Arc::default(),
        }
    }
    pub(crate) fn shutdown_transition_count(&self) -> usize {
        *self.shutdown_transitions.lock().unwrap()
    }
    pub(crate) fn operation_log(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
    pub(crate) fn fail_next_publish(&self) {
        *self.fail_next_publish.lock().unwrap() = true;
    }
    pub(crate) fn panic_next_receive(&self) {
        for (_, queue) in self.queues.lock().unwrap().iter() {
            queue.lock().unwrap().panic_next_receive = true;
        }
    }
    pub(crate) fn enqueue(&self, message: InboundMessage) {
        if let Some((_, queue)) = self.queues.lock().unwrap().first() {
            let mut state = queue.lock().unwrap();
            state.messages.push_back(message);
            for waker in state.wakers.drain(..) {
                waker.wake();
            }
        }
    }
    pub(crate) fn settlement_count(&self) -> usize {
        self.queues
            .lock()
            .unwrap()
            .iter()
            .map(|(_, queue)| queue.lock().unwrap().settled.len())
            .sum()
    }
    pub(crate) fn receiver_drop_recoveries(&self) -> usize {
        self.receiver_drop_recoveries.load(Ordering::Acquire)
    }
    pub(crate) fn settlement_dispositions(&self) -> Vec<DeliveryDisposition> {
        self.queues
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(_, queue)| queue.lock().unwrap().settlement_dispositions.clone())
            .collect()
    }
    pub(crate) fn fail_next_settle(&self) {
        for (_, queue) in self.queues.lock().unwrap().iter() {
            queue.lock().unwrap().fail_next_settle = true;
        }
    }

    pub(crate) fn fail_all_settles(&self) {
        for (_, queue) in self.queues.lock().unwrap().iter() {
            queue.lock().unwrap().fail_settle_always = true;
        }
    }

    pub(crate) fn pause_next_settle(&self) {
        for (_, queue) in self.queues.lock().unwrap().iter() {
            queue.lock().unwrap().pause_next_settle = true;
        }
    }
    pub(crate) fn pause_subscribe(&self) {
        self.subscribe_paused.store(true, Ordering::Release);
    }
    pub(crate) fn release_subscribe(&self) {
        self.subscribe_paused.store(false, Ordering::Release);
        for waker in std::mem::take(&mut *self.subscribe_wakers.lock().unwrap()) {
            waker.wake();
        }
    }
    pub(crate) fn pause_close(&self) {
        self.close_paused.store(true, Ordering::Release);
    }
    pub(crate) fn release_close(&self) {
        self.close_paused.store(false, Ordering::Release);
        for waker in std::mem::take(&mut *self.close_wakers.lock().unwrap()) {
            waker.wake();
        }
    }
    /// Makes provider shutdown remain pending after recording its state change.
    pub(crate) fn pause_shutdown(&self) {
        self.shutdown_paused.store(true, Ordering::Release);
    }
    /// Wakes provider shutdown calls paused by [`Self::pause_shutdown`].
    pub(crate) fn release_shutdown(&self) {
        self.shutdown_paused.store(false, Ordering::Release);
        for waker in std::mem::take(&mut *self.shutdown_wakers.lock().unwrap()) {
            waker.wake();
        }
    }
    pub(crate) fn inject_gap(&self) {
        for (_, queue) in self.queues.lock().unwrap().iter() {
            let mut s = queue.lock().unwrap();
            s.gaps += 1;
            for w in s.wakers.drain(..) {
                w.wake();
            }
        }
    }

    pub(crate) fn advance_time(&self, by: Duration) {
        for (_, queue) in self.queues.lock().unwrap().iter() {
            let mut state = queue.lock().unwrap();
            state.manual_time += by;
            for waker in state.wakers.drain(..) {
                waker.wake();
            }
        }
    }
}

impl AsyncEventBusSpi for FakeAsyncEventBusSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.calls.lock().unwrap().push("publish");
        if std::mem::take(&mut *self.fail_next_publish.lock().unwrap()) {
            return Box::pin(async { Err(spi_error("publish")) });
        }
        for (subscription_id, queue) in self.queues.lock().unwrap().clone() {
            let mut state = queue.lock().unwrap();
            state.messages.push_back(inbound_from_outbound(
                &message,
                subscription_id,
                self.capabilities.settlement(),
            ));
            for waker in state.wakers.drain(..) {
                waker.wake();
            }
        }
        Box::pin(async {
            Ok(PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            })
        })
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.calls.lock().unwrap().push("subscribe");
        let paused = self.subscribe_paused.clone();
        let wakers = self.subscribe_wakers.clone();
        Box::pin(async move {
            std::future::poll_fn(|cx| {
                if !paused.load(Ordering::Acquire) {
                    return std::task::Poll::Ready(());
                }
                let mut waiters = wakers.lock().unwrap();
                if !waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
                    waiters.push(cx.waker().clone());
                }
                std::task::Poll::Pending
            })
            .await;
            let queue = Arc::new(Mutex::new(QueueState::default()));
            self.queues
                .lock()
                .unwrap()
                .push((request.subscription_id(), queue.clone()));
            Ok(Box::new(FakeAsyncEventSubscriptionSpi {
                id: request.subscription_id(),
                settlement: self.capabilities.settlement(),
                queue,
                calls: self.calls.clone(),
                close_paused: self.close_paused.clone(),
                close_wakers: self.close_wakers.clone(),
                receiver_drop_recoveries: self.receiver_drop_recoveries.clone(),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.calls.lock().unwrap().push("shutdown");
        let mut n = self.shutdown_transitions.lock().unwrap();
        if *n == 0 {
            *n = 1;
        }
        drop(n);
        let paused = self.shutdown_paused.clone();
        let wakers = self.shutdown_wakers.clone();
        Box::pin(async move {
            std::future::poll_fn(|cx| {
                if !paused.load(Ordering::Acquire) {
                    return std::task::Poll::Ready(());
                }
                let mut waiters = wakers.lock().unwrap();
                if !waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
                    waiters.push(cx.waker().clone());
                }
                std::task::Poll::Pending
            })
            .await;
            Ok(ShutdownOutcome::Complete)
        })
    }
}

struct FakeAsyncEventSubscriptionSpi {
    id: Id,
    settlement: SettlementCapabilities,
    queue: Arc<Mutex<QueueState>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
    close_paused: Arc<AtomicBool>,
    close_wakers: Arc<Mutex<Vec<Waker>>>,
    receiver_drop_recoveries: Arc<AtomicUsize>,
}

impl Drop for FakeAsyncEventSubscriptionSpi {
    fn drop(&mut self) {
        let mut state = self.queue.lock().unwrap();
        self.receiver_drop_recoveries
            .fetch_add(state.unsettled, Ordering::AcqRel);
        state.unsettled = 0;
    }
}

impl FakeAsyncEventSubscriptionSpi {
    #[allow(dead_code)]
    pub(crate) fn inject_gap(&self) {
        let mut s = self.queue.lock().unwrap();
        s.gaps += 1;
        for w in s.wakers.drain(..) {
            w.wake();
        }
    }
    #[allow(dead_code)]
    pub(crate) fn settlement_count(&self) -> usize {
        self.queue.lock().unwrap().settled.len()
    }
    #[allow(dead_code)]
    pub(crate) fn fail_next_receive(&self) {
        self.queue.lock().unwrap().fail_next_receive = true;
    }
}

impl AsyncEventSubscriptionSpi for FakeAsyncEventSubscriptionSpi {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.calls.lock().unwrap().push("receive");
        if std::mem::take(&mut self.queue.lock().unwrap().panic_next_receive) {
            return Box::pin(async {
                panic!("fake async receive poll panic");
                #[allow(unreachable_code)]
                Ok(ReceiveOutcome::TimedOut)
            });
        }
        Box::pin(ReceiveFuture {
            queue: self.queue.clone(),
            timeout,
            deadline: None,
        })
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.calls.lock().unwrap().push("settle");
        if std::mem::take(&mut self.queue.lock().unwrap().pause_next_settle) {
            return Box::pin(async {
                std::future::pending::<()>().await;
                Ok(())
            });
        }
        let result = if !token.belongs_to(self.id) || matches!(self.settlement, SettlementCapabilities::None) {
            Err(spi_error("settle"))
        } else {
            let mut state = self.queue.lock().unwrap();
            let should_fail = state.fail_settle_always || std::mem::take(&mut state.fail_next_settle);
            drop(state);
            if should_fail {
                return Box::pin(async { Err(spi_error("settle")) });
            }
            let key = settlement_token_key(token);
            let mut state = self.queue.lock().unwrap();
            match state.settled.get(&key) {
                Some(previous) if *previous == disposition => Ok(()),
                Some(_) => Err(conflicting_settlement_error()),
                None => {
                    state.settled.insert(key, disposition);
                    state.settlement_dispositions.push(disposition);
                    state.unsettled = state.unsettled.saturating_sub(1);
                    Ok(())
                }
            }
        };
        Box::pin(async move { result })
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.calls.lock().unwrap().push("close");
        let queue = self.queue.clone();
        let paused = self.close_paused.clone();
        let wakers = self.close_wakers.clone();
        Box::pin(async move {
            {
                let mut state = queue.lock().unwrap();
                state.closed = true;
            }
            std::future::poll_fn(|context| {
                if !paused.load(Ordering::Acquire) {
                    return std::task::Poll::Ready(());
                }
                let mut waiters = wakers.lock().unwrap();
                if !waiters.iter().any(|waker| waker.will_wake(context.waker())) {
                    waiters.push(context.waker().clone());
                }
                std::task::Poll::Pending
            })
            .await;
            let mut state = queue.lock().unwrap();
            state.closed = true;
            for waker in state.wakers.drain(..) {
                waker.wake();
            }
            Ok(())
        })
    }
}

struct ReceiveFuture {
    queue: Arc<Mutex<QueueState>>,
    timeout: Duration,
    deadline: Option<Duration>,
}
impl Future for ReceiveFuture {
    type Output = Result<ReceiveOutcome, SpiError>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        let queue = this.queue.clone();
        let timeout = this.timeout;
        let result = {
            let mut state = queue.lock().unwrap();
            if std::mem::take(&mut state.fail_next_receive) {
                Some(Err(spi_error("receive")))
            } else if let Some(message) = state.in_flight.take() {
                if message.settlement().is_some() {
                    state.unsettled += 1;
                }
                Some(Ok(ReceiveOutcome::Message(message)))
            } else if let Some(message) = state.messages.pop_front() {
                state.in_flight = Some(message);
                if !state.wakers.iter().any(|w| w.will_wake(cx.waker())) {
                    state.wakers.push(cx.waker().clone());
                }
                cx.waker().wake_by_ref();
                None
            } else if state.gaps > 0 {
                state.gaps -= 1;
                Some(Ok(ReceiveOutcome::Gap(DeliveryGap::new("fake gap", Some(1)))))
            } else if state.closed {
                Some(Ok(ReceiveOutcome::Closed))
            } else if timeout.is_zero() {
                Some(Ok(ReceiveOutcome::TimedOut))
            } else {
                let deadline = *this.deadline.get_or_insert(state.manual_time + timeout);
                if state.manual_time >= deadline {
                    Some(Ok(ReceiveOutcome::TimedOut))
                } else {
                    if !state.wakers.iter().any(|w| w.will_wake(cx.waker())) {
                        state.wakers.push(cx.waker().clone());
                    }
                    None
                }
            }
        };
        match result {
            Some(result) => std::task::Poll::Ready(result),
            None => std::task::Poll::Pending,
        }
    }
}
use std::future::Future;
