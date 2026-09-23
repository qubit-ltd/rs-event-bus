//! Deterministic sync and async transport fakes for SPI contract tests.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Waker;
use std::time::{Duration, Instant, SystemTime};

use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::{
    EventId, Headers, ProviderOptions, PublishAcknowledgement, StartPosition, SubscriberId,
    SubscriptionDurability,
};
use qubit_event_bus::spi::{
    AsyncEventBusSpi, AsyncEventSubscriptionSpi, DelayedDeliveryCapability, DeliveryDisposition,
    DeliveryGap, DurabilityCapability, EventBusCapabilities, EventBusSpi, EventSubscriptionSpi,
    InboundMessage, OrderingCapability, OutboundMessage, PayloadModes, PublishGuarantee,
    PublishVisibility, ReceiveOutcome, ReplayCapability, SettlementCapabilities, SettlementToken,
    ShutdownMode, ShutdownOutcome, SpiFuture, SpiSubscriptionRequest, TopicAddress,
    TransportPayload,
};
use qubit_id::Id;

#[derive(Default)]
struct QueueState {
    messages: VecDeque<InboundMessage>,
    in_flight: Option<InboundMessage>,
    gaps: usize,
    closed: bool,
    settled: HashSet<String>,
    wakers: Vec<Waker>,
    fail_next_receive: bool,
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
    queues: Arc<Mutex<Vec<(Id, Arc<(Mutex<QueueState>, Condvar)>)>>>,
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
            lock.lock()
                .unwrap()
                .messages
                .push_back(message_for_copy(&message));
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

// The test payload is a scalar; reconstructing avoids imposing Clone on the SPI message.
fn message_for_copy(_: &InboundMessage) -> InboundMessage {
    inbound_message(None)
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
        self.enqueue(inbound_message(None));
        let _ = message;
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
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
    fn settle(&mut self, token: SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
        self.calls.lock().unwrap().push("settle");
        if !token.belongs_to(self.id) {
            return Err(spi_error("settle"));
        }
        if matches!(self.settlement, SettlementCapabilities::None) {
            return Err(spi_error("settle"));
        }
        let key = token
            .downcast_ref::<&str>()
            .copied()
            .unwrap_or("unknown")
            .to_owned();
        if !self.queue.0.lock().unwrap().settled.insert(key) {
            return Err(spi_error("settle"));
        }
        Ok(())
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
    queues: Arc<Mutex<Vec<(Id, Arc<Mutex<QueueState>>)>>>,
    shutdown_transitions: Arc<Mutex<usize>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
    fail_next_publish: Arc<Mutex<bool>>,
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
    fn publish<'a>(
        &'a self,
        _: OutboundMessage,
    ) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.calls.lock().unwrap().push("publish");
        if std::mem::take(&mut *self.fail_next_publish.lock().unwrap()) {
            return Box::pin(async { Err(spi_error("publish")) });
        }
        for (_, queue) in self.queues.lock().unwrap().clone() {
            let mut state = queue.lock().unwrap();
            state.messages.push_back(inbound_message(None));
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
        Box::pin(async move {
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
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.calls.lock().unwrap().push("shutdown");
        Box::pin(async move {
            let mut n = self.shutdown_transitions.lock().unwrap();
            if *n == 0 {
                *n = 1;
            }
            Ok(ShutdownOutcome::Complete)
        })
    }
}

struct FakeAsyncEventSubscriptionSpi {
    id: Id,
    settlement: SettlementCapabilities,
    queue: Arc<Mutex<QueueState>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
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
    fn receive<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.calls.lock().unwrap().push("receive");
        Box::pin(ReceiveFuture {
            queue: self.queue.clone(),
            timeout,
            deadline: None,
        })
    }
    fn settle<'a>(
        &'a mut self,
        token: SettlementToken,
        _: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.calls.lock().unwrap().push("settle");
        let result = if !token.belongs_to(self.id)
            || matches!(self.settlement, SettlementCapabilities::None)
        {
            Err(spi_error("settle"))
        } else {
            let key = token
                .downcast_ref::<&str>()
                .copied()
                .unwrap_or("unknown")
                .to_owned();
            if self.queue.lock().unwrap().settled.insert(key) {
                Ok(())
            } else {
                Err(spi_error("settle"))
            }
        };
        Box::pin(async move { result })
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.calls.lock().unwrap().push("close");
        let mut s = self.queue.lock().unwrap();
        s.closed = true;
        for w in s.wakers.drain(..) {
            w.wake();
        }
        Box::pin(async { Ok(()) })
    }
}

struct ReceiveFuture {
    queue: Arc<Mutex<QueueState>>,
    timeout: Duration,
    deadline: Option<Duration>,
}
impl Future for ReceiveFuture {
    type Output = Result<ReceiveOutcome, SpiError>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        let queue = this.queue.clone();
        let timeout = this.timeout;
        let result = {
            let mut state = queue.lock().unwrap();
            if std::mem::take(&mut state.fail_next_receive) {
                Some(Err(spi_error("receive")))
            } else if let Some(message) = state.in_flight.take() {
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
                Some(Ok(ReceiveOutcome::Gap(DeliveryGap::new(
                    "fake gap",
                    Some(1),
                ))))
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
