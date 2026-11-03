// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fault injection limited to a shutdown following close with an unconsumed
//! publish.

use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;

use super::fake_spi::FakeAsyncEventBusSpi;
use super::fake_spi::FakeEventBusSpi;

const INJECTED_ERROR: &str = "pending-close-shutdown-injected";

#[derive(Clone, Copy, Debug)]
pub(crate) enum InjectedShutdown {
    Complete,
    TimedOut,
    Error,
}

#[derive(Default)]
struct State {
    published: bool,
    consumed: bool,
    pending_close: bool,
}

fn injected_error() -> SpiError {
    SpiError::Operation {
        provider_id: "pending-close-fixture".into(),
        operation: "shutdown",
        resource: None,
        kind: "injected",
        retryable: Some(false),
        source: Box::new(io::Error::other(INJECTED_ERROR)),
    }
}

fn injected(result: Result<ShutdownOutcome, SpiError>, mode: InjectedShutdown) -> Result<ShutdownOutcome, SpiError> {
    let _ = result?;
    match mode {
        InjectedShutdown::Complete => Ok(ShutdownOutcome::Complete),
        InjectedShutdown::TimedOut => Ok(ShutdownOutcome::TimedOut),
        InjectedShutdown::Error => Err(injected_error()),
    }
}

pub(crate) struct SyncFixture {
    inner: FakeEventBusSpi,
    state: Arc<Mutex<State>>,
    mode: InjectedShutdown,
}

impl SyncFixture {
    pub(crate) fn new(mode: InjectedShutdown) -> Self {
        Self {
            inner: FakeEventBusSpi::with_capabilities(super::fake_spi::full_capabilities()),
            state: Arc::default(),
            mode,
        }
    }
}

impl EventBusSpi for SyncFixture {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let result = self.inner.publish(message);
        if result.is_ok() {
            self.state.lock().unwrap().published = true;
        }
        result
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(SyncSubscription {
            inner: self.inner.subscribe(request)?,
            state: self.state.clone(),
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        let pending = self.state.lock().unwrap().pending_close;
        let result = self.inner.shutdown(mode);
        if pending { injected(result, self.mode) } else { result }
    }
}

struct SyncSubscription {
    inner: Box<dyn EventSubscriptionSpi>,
    state: Arc<Mutex<State>>,
}

impl EventSubscriptionSpi for SyncSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        let result = self.inner.receive(timeout);
        if matches!(result, Ok(ReceiveOutcome::Message(_))) {
            self.state.lock().unwrap().consumed = true;
        }
        result
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        self.inner.settle(token, disposition)
    }
    fn close(&mut self) -> Result<(), SpiError> {
        let result = self.inner.close();
        let mut state = self.state.lock().unwrap();
        state.pending_close = state.published && !state.consumed;
        result
    }
}

pub(crate) struct AsyncFixture {
    inner: FakeAsyncEventBusSpi,
    state: Arc<Mutex<State>>,
    mode: InjectedShutdown,
}

impl AsyncFixture {
    pub(crate) fn new(mode: InjectedShutdown) -> Self {
        Self {
            inner: FakeAsyncEventBusSpi::with_capabilities(super::fake_spi::full_capabilities()),
            state: Arc::default(),
            mode,
        }
    }
}

impl AsyncEventBusSpi for AsyncFixture {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            let result = self.inner.publish(message).await;
            if result.is_ok() {
                self.state.lock().unwrap().published = true;
            }
            result
        })
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            let inner = self.inner.subscribe(request).await?;
            Ok(Box::new(AsyncSubscription {
                inner,
                state: self.state.clone(),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async move {
            let pending = self.state.lock().unwrap().pending_close;
            let result = self.inner.shutdown(mode).await;
            if pending { injected(result, self.mode) } else { result }
        })
    }
}

struct AsyncSubscription {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    state: Arc<Mutex<State>>,
}

impl AsyncEventSubscriptionSpi for AsyncSubscription {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async move {
            let result = self.inner.receive(timeout).await;
            if matches!(result, Ok(ReceiveOutcome::Message(_))) {
                self.state.lock().unwrap().consumed = true;
            }
            result
        })
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        self.inner.settle(token, disposition)
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async move {
            let result = self.inner.close().await;
            let mut state = self.state.lock().unwrap();
            state.pending_close = state.published && !state.consumed;
            result
        })
    }
}
