// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider-shaped adapters used to exercise transport and settlement variants.

use std::io::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::TrySendError;
use std::time::Duration;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ProviderMessageMetadata;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DeliveryGap;
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
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;

/// One subscribed topic and its bounded delivery channel.
struct ChannelEndpoint {
    topic: TopicAddress,
    sender: SyncSender<InboundMessage>,
    gap: Arc<AtomicBool>,
}

/// Small bounded-channel provider with native payloads and no settlement.
#[derive(Clone, Default)]
pub(crate) struct ChannelShapedEventBusSpi {
    endpoints: Arc<Mutex<Vec<ChannelEndpoint>>>,
    closed: Arc<AtomicBool>,
}

impl ChannelShapedEventBusSpi {
    /// Creates a provider with no subscriptions and open admission.
    ///
    /// # Returns
    /// A fresh channel-shaped provider double.
    #[must_use]
    #[inline]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Signals a single receive gap to every current subscription.
    ///
    /// # Side Effects
    /// Sets each endpoint's gap flag so its next receive reports lag.
    pub(crate) fn inject_gap(&self) {
        for endpoint in self.endpoints.lock().unwrap().iter() {
            endpoint.gap.store(true, Ordering::Release);
        }
    }
}

impl EventBusSpi for ChannelShapedEventBusSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(provider_error("publish", "provider_closed"));
        }
        let TransportPayload::Native(payload) = message.payload() else {
            return Err(provider_error("publish", "unsupported_payload"));
        };
        for endpoint in self.endpoints.lock().unwrap().iter() {
            if endpoint.topic.as_str() != message.topic().as_str() {
                continue;
            }
            let inbound = InboundMessage::new(
                message.topic().clone(),
                message.id().clone(),
                message.timestamp(),
                message.headers().clone(),
                message.ordering_key().cloned(),
                TransportPayload::Native(payload.clone()),
                None,
                ProviderMessageMetadata::default(),
            );
            match endpoint.sender.try_send(inbound) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    return Err(provider_error("publish", "channel_capacity"));
                }
                Err(TrySendError::Disconnected(_)) => {}
            }
        }
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: ProviderMessageMetadata::default(),
        })
    }

    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(provider_error("subscribe", "provider_closed"));
        }
        let (sender, receiver) = mpsc::sync_channel(16);
        let gap = Arc::new(AtomicBool::new(false));
        self.endpoints.lock().unwrap().push(ChannelEndpoint {
            topic: request.topic().clone(),
            sender,
            gap: gap.clone(),
        });
        Ok(Box::new(ChannelSubscription {
            receiver,
            gap,
            closed: false,
            bus_closed: self.closed.clone(),
        }))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.closed.store(true, Ordering::Release);
        Ok(ShutdownOutcome::Complete)
    }
}

/// Synchronous subscription endpoint backed by a bounded channel.
struct ChannelSubscription {
    receiver: Receiver<InboundMessage>,
    gap: Arc<AtomicBool>,
    closed: bool,
    bus_closed: Arc<AtomicBool>,
}

impl EventSubscriptionSpi for ChannelSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        if self.closed || self.bus_closed.load(Ordering::Acquire) {
            return Ok(ReceiveOutcome::Closed);
        }
        if self.gap.swap(false, Ordering::AcqRel) {
            return Ok(ReceiveOutcome::Gap(DeliveryGap::new(
                "channel lag",
                Some(1),
            )));
        }
        if timeout.is_zero() {
            match self.receiver.try_recv() {
                Ok(message) => Ok(ReceiveOutcome::Message(message)),
                Err(TryRecvError::Empty) => Ok(ReceiveOutcome::TimedOut),
                Err(TryRecvError::Disconnected) => Ok(ReceiveOutcome::Closed),
            }
        } else {
            match self.receiver.recv_timeout(timeout) {
                Ok(message) => Ok(ReceiveOutcome::Message(message)),
                Err(RecvTimeoutError::Timeout) => Ok(ReceiveOutcome::TimedOut),
                Err(RecvTimeoutError::Disconnected) => Ok(ReceiveOutcome::Closed),
            }
        }
    }

    fn settle(&mut self, _: &SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
        Err(provider_error("settle", "unsupported"))
    }

    fn close(&mut self) -> Result<(), SpiError> {
        self.closed = true;
        Ok(())
    }
}

/// Capability profile for an encoded broker whose opaque tokens represent
/// commits.
///
/// # Returns
/// A capability value with encoded payloads and full settlement support.
#[inline]
pub(crate) fn encoded_settlement_capabilities() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Encoded,
        SettlementCapabilities::AcceptRetryReject,
        OrderingCapability::PerSubscription,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::EPHEMERAL,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}

/// Creates a non-retryable SPI error for a simulated provider operation.
///
/// # Parameters
/// - `operation`: SPI operation associated with the failure.
/// - `kind`: Stable provider failure classification.
///
/// # Returns
/// An operation error identifying this test provider and failure kind.
fn provider_error(operation: &'static str, kind: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "channel-shaped".into(),
        operation,
        resource: None,
        kind,
        retryable: Some(false),
        source: Box::new(Error::other(kind)),
    }
}
