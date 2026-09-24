// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous in-process transport SPI.

use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Instant;

use super::LocalEventBusConfig;
use super::state::LocalEvent;
use super::state::LocalQueue;
use super::state::LocalQueueState;
use super::state::LocalSharedState;
use super::subscription::LocalEventSubscription;
use crate::error::SpiError;
use crate::model::AdmissionStatus;
use crate::model::DestinationAdmission;
use crate::model::PublishAcknowledgement;
use crate::spi::DelayedDeliveryCapability;
use crate::spi::DurabilityCapability;
use crate::spi::EventBusCapabilities;
use crate::spi::EventBusSpi;
use crate::spi::EventSubscriptionSpi;
use crate::spi::OrderingCapability;
use crate::spi::OutboundMessage;
use crate::spi::PayloadModes;
use crate::spi::PublishGuarantee;
use crate::spi::PublishVisibility;
use crate::spi::ReplayCapability;
use crate::spi::SettlementCapabilities;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TransportPayload;

/// Synchronous local backend with one bounded queue per subscription.
pub struct LocalEventBusSpi {
    pub(super) shared: Arc<LocalSharedState>,
}

impl LocalEventBusSpi {
    /// Creates an SPI instance from local transport configuration.
    ///
    /// # Errors
    /// Returns the configuration validation error when queue capacity is zero.
    pub fn new(config: &LocalEventBusConfig) -> Result<Self, crate::error::ConfigurationError> {
        config.validate()?;
        Ok(Self {
            shared: LocalSharedState::new(config.get_queue_capacity()),
        })
    }
}

impl EventBusSpi for LocalEventBusSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::AcceptRetryReject,
            OrderingCapability::PerKey,
            DelayedDeliveryCapability::Native,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::DestinationAdmissions,
        )
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let topic = message.topic().clone();
        validate_message(&message, &topic)?;
        let event = LocalEvent::transport(topic.clone(), &message)
            .ok_or_else(|| operation_error("publish", Some(topic.as_str()), "delay_deadline_overflow"))?;
        let queues = {
            let mut state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.closed {
                return Err(operation_error("publish", Some(topic.as_str()), "provider_closed"));
            }
            state.queues.retain(|_, queue| queue.strong_count() > 0);
            state
                .queues
                .values()
                .filter_map(std::sync::Weak::upgrade)
                .collect::<Vec<_>>()
        };
        let mut admissions = Vec::new();
        let mut admitted_any = false;
        for queue in queues {
            if queue.topic != topic {
                continue;
            }
            let mut state = queue.lock();
            let status = if state.closed {
                AdmissionStatus::Rejected("subscription is closed".into())
            } else if state.messages.len() + state.deferred_retries.len() >= queue.capacity {
                AdmissionStatus::Rejected("subscription queue is full".into())
            } else {
                state.messages.push_back(event.clone());
                queue.ready.notify_one();
                admitted_any = true;
                AdmissionStatus::Accepted
            };
            drop(state);
            admissions.push(DestinationAdmission::new(queue.id, queue.subscriber_id.clone(), status));
        }
        if admitted_any {
            signal_changed(&self.shared);
        }
        Ok(PublishAcknowledgement::DestinationAdmissions(admissions))
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let id = request.subscription_id();
        let queue = Arc::new(LocalQueue {
            id,
            topic: request.topic().clone(),
            subscriber_id: request.subscriber_id().clone(),
            capacity: self.shared.capacity,
            state: Mutex::new(LocalQueueState::default()),
            ready: Default::default(),
        });
        let mut state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return Err(operation_error(
                "subscribe",
                Some(request.topic().as_str()),
                "provider_closed",
            ));
        }
        if request.durability() != crate::model::SubscriptionDurability::Ephemeral
            || request.group().is_some()
            || !matches!(request.start_position(), crate::model::StartPosition::New)
        {
            return Err(operation_error(
                "subscribe",
                Some(request.topic().as_str()),
                "unsupported_subscription_options",
            ));
        }
        if state.queues.get(&id).and_then(std::sync::Weak::upgrade).is_some() {
            return Err(operation_error(
                "subscribe",
                Some(request.topic().as_str()),
                "duplicate_subscription",
            ));
        }
        state.queues.insert(id, Arc::downgrade(&queue));
        drop(state);
        Ok(Box::new(LocalEventSubscription::new(self.shared.clone(), queue)))
    }

    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        let _gate = self.shared.shutdown_gate.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(outcome) = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .shutdown_outcome
        {
            return Ok(outcome);
        }
        self.shared.state.lock().unwrap_or_else(PoisonError::into_inner).closed = true;
        signal_changed(&self.shared);

        let outcome = match mode {
            ShutdownMode::Immediate => ShutdownOutcome::Complete,
            ShutdownMode::Graceful { timeout } => {
                let started = Instant::now();
                loop {
                    let (version, queues) = {
                        let state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                        (
                            state.change_version,
                            state
                                .queues
                                .values()
                                .filter_map(std::sync::Weak::upgrade)
                                .collect::<Vec<_>>(),
                        )
                    };
                    let busy = queues.iter().any(|queue| {
                        let state = queue.lock();
                        !state.messages.is_empty() || !state.deferred_retries.is_empty() || !state.in_flight.is_empty()
                    });
                    if !busy {
                        break ShutdownOutcome::Complete;
                    }
                    let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                        break ShutdownOutcome::TimedOut;
                    };
                    let state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                    if state.change_version != version {
                        continue;
                    }
                    let (_state, result) = self
                        .shared
                        .changed
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(PoisonError::into_inner);
                    if result.timed_out() {
                        break ShutdownOutcome::TimedOut;
                    }
                }
            }
        };

        let queues = {
            let state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            state
                .queues
                .values()
                .filter_map(std::sync::Weak::upgrade)
                .collect::<Vec<_>>()
        };
        for queue in queues {
            let mut state = queue.lock();
            state.closed = true;
            state.messages.clear();
            state.deferred_retries.clear();
            state.in_flight.clear();
            queue.ready.notify_all();
            drop(state);
        }
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .shutdown_outcome = Some(outcome);
        signal_changed(&self.shared);
        Ok(outcome)
    }
}

/// Rejects SPI payload modes that are outside the local provider capability.
fn validate_message(message: &OutboundMessage, topic: &crate::spi::TopicAddress) -> Result<(), SpiError> {
    match message.payload() {
        TransportPayload::Native(_) => Ok(()),
        TransportPayload::Encoded(_) => Err(operation_error(
            "publish",
            Some(topic.as_str()),
            "unsupported_payload_mode",
        )),
    }
}

/// Builds a provider-tagged operation error with optional topic context.
pub(super) fn operation_error(operation: &'static str, resource: Option<&str>, kind: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "local".into(),
        operation,
        resource: resource.map(Into::into),
        kind,
        retryable: Some(false),
        source: Box::new(std::io::Error::other(kind)) as Box<dyn Error + Send + Sync>,
    }
}

/// Builds a provider-tagged invalid-token failure with its stable reason.
pub(super) fn invalid_token_error(resource: Option<&str>, reason: &'static str) -> SpiError {
    SpiError::InvalidSettlementToken {
        provider_id: "local".into(),
        operation: "settle",
        resource: resource.map(Into::into),
        reason,
        retryable: Some(false),
        source: Box::new(std::io::Error::other(reason)),
    }
}

/// Advances the provider change generation before waking graceful shutdown.
pub(super) fn signal_changed(shared: &LocalSharedState) {
    let mut state = shared.state.lock().unwrap_or_else(PoisonError::into_inner);
    state.change_version = state.change_version.wrapping_add(1);
    drop(state);
    shared.changed.notify_all();
}
