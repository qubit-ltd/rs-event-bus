// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Independent bounded-channel fixture for public synchronous SPI checks.

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use qubit_event_bus::SpiError;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
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
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;

const QUEUE_CAPACITY: usize = 1;

struct Route {
    topic: qubit_event_bus::spi::TopicAddress,
    subscriber: qubit_event_bus::model::SubscriberId,
    sender: flume::Sender<InboundMessage>,
}

#[derive(Default)]
struct State {
    closed: bool,
    routes: HashMap<Id, Route>,
    payload_types: HashMap<qubit_event_bus::spi::TopicAddress, TypeId>,
}

#[derive(Clone, Default)]
struct FlumeSpi {
    state: Arc<Mutex<State>>,
}

/// Creates the fixture as a public SPI trait object.
pub(crate) fn create() -> Arc<dyn EventBusSpi> {
    Arc::new(FlumeSpi::default())
}

impl EventBusSpi for FlumeSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::DestinationAdmissions,
        )
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        if message.delay().is_some() {
            return Err(operation_error("publish", "unsupported_delayed_delivery"));
        }
        let TransportPayload::Native(payload) = message.payload() else {
            return Err(operation_error("publish", "unsupported_payload_mode"));
        };
        let state = self.state.lock().unwrap();
        if state.closed {
            return Err(operation_error("publish", "provider_closed"));
        }
        let payload_type = payload.as_ref().type_id();
        if state
            .payload_types
            .get(message.topic())
            .is_some_and(|expected| *expected != payload_type)
        {
            return Err(operation_error("publish", "topic_type_conflict"));
        }
        let mut admissions = Vec::new();
        for (id, route) in state.routes.iter().filter(|(_, route)| route.topic == *message.topic()) {
            let inbound = InboundMessage::new(
                message.topic().clone(),
                message.id().clone(),
                message.timestamp(),
                message.headers().clone(),
                message.ordering_key().cloned(),
                TransportPayload::Native(Arc::clone(payload)),
                None,
                Default::default(),
            );
            let status = match route.sender.try_send(inbound) {
                Ok(()) => AdmissionStatus::Accepted,
                Err(flume::TrySendError::Full(_)) => AdmissionStatus::Rejected("subscription queue is full".into()),
                Err(flume::TrySendError::Disconnected(_)) => AdmissionStatus::Rejected("subscription is closed".into()),
            };
            admissions.push(DestinationAdmission::new(*id, route.subscriber.clone(), status));
        }
        Ok(PublishAcknowledgement::DestinationAdmissions(admissions))
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let (sender, receiver) = flume::bounded(QUEUE_CAPACITY);
        let id = request.subscription_id();
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(operation_error("subscribe", "provider_closed"));
        }
        if state
            .payload_types
            .get(request.topic())
            .is_some_and(|previous| *previous != request.payload_type_id())
        {
            return Err(operation_error("subscribe", "topic_type_conflict"));
        }
        if state.routes.contains_key(&id) {
            return Err(operation_error("subscribe", "duplicate_subscription"));
        }
        state.routes.insert(
            id,
            Route {
                topic: request.topic().clone(),
                subscriber: request.subscriber_id().clone(),
                sender,
            },
        );
        state
            .payload_types
            .insert(request.topic().clone(), request.payload_type_id());
        drop(state);
        Ok(Box::new(FlumeSubscription {
            id,
            receiver,
            state: Arc::clone(&self.state),
            closed: false,
        }))
    }

    fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.routes.clear();
        state.payload_types.clear();
        Ok(ShutdownOutcome::Complete)
    }
}

struct FlumeSubscription {
    id: Id,
    receiver: flume::Receiver<InboundMessage>,
    state: Arc<Mutex<State>>,
    closed: bool,
}

impl EventSubscriptionSpi for FlumeSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(message) => Ok(ReceiveOutcome::Message(message)),
            Err(flume::RecvTimeoutError::Timeout) => Ok(ReceiveOutcome::TimedOut),
            Err(flume::RecvTimeoutError::Disconnected) => Ok(ReceiveOutcome::Closed),
        }
    }

    fn settle(
        &mut self,
        _token: &SettlementToken,
        _disposition: qubit_event_bus::spi::DeliveryDisposition,
    ) -> Result<(), SpiError> {
        Err(operation_error("settle", "settlement_unsupported"))
    }

    fn close(&mut self) -> Result<(), SpiError> {
        if self.closed {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        if let Some(route) = state.routes.remove(&self.id)
            && !state.routes.values().any(|candidate| candidate.topic == route.topic)
        {
            state.payload_types.remove(&route.topic);
        }
        self.closed = true;
        Ok(())
    }
}

impl Drop for FlumeSubscription {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn operation_error(operation: &'static str, kind: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "flume-fixture".into(),
        operation,
        resource: None,
        kind,
        retryable: Some(false),
        source: Box::new(std::io::Error::other(kind)),
    }
}
