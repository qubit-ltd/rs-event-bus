// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Synchronous type-safe event-bus facade over an object-safe provider SPI.

use std::sync::Arc;

pub(in crate::facade) use internal::CoordinatorMessage;
pub(in crate::facade) use internal::EventBusInner;
pub(in crate::facade) use internal::clone_spi_error;
#[cfg(test)]
pub(in crate::facade) use subscribing::cleanup_failed_worker_spawn;

use self::internal::OperationGate;
use self::internal::OwnerSettlementRouter;
use self::internal::ShutdownState;
use self::internal::SubscriptionWorkerBudget;

// Implements local-provider construction and facade configuration.
mod construction;
// Decodes messages and runs subscriber handlers.
mod delivery;
// Registers and emits bus diagnostics.
mod diagnostics;
// Applies terminal delivery failure actions.
mod failure;
// Owns shared provider, operation, and worker state.
mod internal;
// Implements wait and shutdown lifecycle operations.
mod lifecycle;
// Implements publish operations.
mod publishing;
// Implements subscription operations.
mod subscribing;
// Owns and runs provider coordinator workers.
mod worker;

/// A cloneable synchronous facade with one provider coordinator per
/// subscription and a shared bounded handler pool.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::local::LocalEventBusConfig;
/// use qubit_event_bus::model::PublishRequest;
/// use qubit_event_bus::model::SubscribeRequest;
/// use qubit_event_bus::model::Topic;
/// use qubit_event_bus::spi::ShutdownMode;
/// use qubit_event_bus::DeliveryError;
/// use qubit_event_bus::EventBus;
///
/// let bus = EventBus::local(LocalEventBusConfig::default())?;
/// let topic = Topic::<String>::new("orders.created")?;
/// let request = SubscribeRequest::new("audit", topic.clone())?;
/// let subscription = bus.subscribe(request, |_| Ok::<(), DeliveryError>(()))?;
/// let receipt = bus.publish(PublishRequest::new(topic.clone(), "order-1".to_owned())?)?;
/// assert_eq!(receipt.provider_id().as_str(), "local");
/// bus.wait_for_idle(&topic, None)?;
/// subscription.cancel()?;
/// bus.shutdown(ShutdownMode::Immediate)?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct EventBus {
    /// Shared provider, subscription, scheduler, and lifecycle state.
    pub(super) inner: Arc<EventBusInner>,
}

#[cfg(test)]
mod spawn_failure_tests {
    //! Tests cleanup behavior when provider or scheduler worker creation fails.

    use std::error::Error;
    use std::io::Error as IoError;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use crate::error::ShutdownError;
    use crate::error::SpiError;
    use crate::error::SubscribeError;
    use crate::facade::DeliverySchedulingConfig;
    use crate::facade::EventBus;
    use crate::facade::EventBusFacadeConfig;
    use crate::model::ProviderId;
    use crate::model::PublishAcknowledgement;
    use crate::model::SubscribeRequest;
    use crate::model::SubscriberId;
    use crate::model::Topic;
    use crate::spi::DelayedDeliveryCapability;
    use crate::spi::DeliveryDisposition;
    use crate::spi::DurabilityCapability;
    use crate::spi::EventBusCapabilities;
    use crate::spi::EventBusSpi;
    use crate::spi::EventSubscriptionSpi;
    use crate::spi::OrderingCapability;
    use crate::spi::OutboundMessage;
    use crate::spi::PayloadModes;
    use crate::spi::PublishGuarantee;
    use crate::spi::PublishVisibility;
    use crate::spi::ReceiveOutcome;
    use crate::spi::ReplayCapability;
    use crate::spi::SettlementCapabilities;
    use crate::spi::SettlementToken;
    use crate::spi::ShutdownMode;
    use crate::spi::ShutdownOutcome;
    use crate::spi::SpiSubscriptionRequest;
    use crate::spi::SubscriptionModes;

    struct EmptySpi;

    impl EventBusSpi for EmptySpi {
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

        fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
            unreachable!("spawn cleanup does not publish")
        }

        fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
            unreachable!("spawn cleanup uses a directly constructed receiver")
        }

        fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
            Ok(ShutdownOutcome::Complete)
        }
    }

    struct CloseCounter {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    impl EventSubscriptionSpi for CloseCounter {
        fn receive(&mut self, _: Duration) -> Result<ReceiveOutcome, SpiError> {
            Ok(ReceiveOutcome::Closed)
        }

        fn settle(&mut self, _: &SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), SpiError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            if self.fail {
                Err(SpiError::Operation {
                    provider_id: "spawn-test".into(),
                    operation: "close_subscription",
                    resource: None,
                    kind: "close_failed",
                    retryable: None,
                    source: Box::new(IoError::other("synthetic close failure")),
                })
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn test_failed_worker_spawn_closes_receiver_and_keeps_the_spawn_error_source() {
        let bus = EventBus::from_spi(
            ProviderId::new("spawn-test").expect("valid provider ID"),
            Arc::new(EmptySpi),
        )
        .expect("valid provider capabilities");
        let close_calls = Arc::new(AtomicUsize::new(0));
        let receiver: Box<dyn EventSubscriptionSpi> = Box::new(CloseCounter {
            calls: close_calls.clone(),
            fail: false,
        });
        let error = super::cleanup_failed_worker_spawn(
            &bus.inner,
            &SubscriberId::new("spawn-failure").expect("valid ID"),
            Some(receiver),
            IoError::other("synthetic thread spawn failure"),
        );

        assert_eq!(close_calls.load(Ordering::Acquire), 1);
        let source = Error::source(&error).expect("spawn I/O error remains chained");
        assert_eq!(source.to_string(), "synthetic thread spawn failure");
    }

    struct SpawnFailureSpi {
        close_calls: Arc<AtomicUsize>,
    }

    impl EventBusSpi for SpawnFailureSpi {
        fn capabilities(&self) -> EventBusCapabilities {
            EmptySpi.capabilities()
        }

        fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
            unreachable!("spawn failure test does not publish")
        }

        fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
            Ok(Box::new(CloseCounter {
                calls: self.close_calls.clone(),
                fail: true,
            }))
        }

        fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
            Ok(ShutdownOutcome::Complete)
        }
    }

    #[test]
    fn test_scheduler_spawn_failure_closes_provider_subscription_and_keeps_both_errors() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let config = EventBusFacadeConfig::new().with_delivery_scheduling(
            DeliverySchedulingConfig::new(
                NonZeroUsize::new(2).expect("worker count is nonzero"),
                NonZeroUsize::new(2).expect("queue capacity is nonzero"),
                NonZeroUsize::new(2).expect("per-subscription capacity is nonzero"),
                NonZeroUsize::new(1).expect("handler count is nonzero"),
            )
            .expect("valid scheduler config"),
        );
        let bus = EventBus::with_config(
            ProviderId::new("spawn-test").expect("valid provider ID"),
            Arc::new(SpawnFailureSpi {
                close_calls: close_calls.clone(),
            }),
            config,
        )
        .expect("valid provider capabilities");
        bus.inner.scheduler.fail_spawn_at(1);

        let request = SubscribeRequest::new(
            "scheduler-spawn-failure",
            Topic::<String>::new("spawn.test").expect("valid topic"),
        )
        .expect("valid subscriber ID");
        let error = match bus.subscribe(request, |_| ()) {
            Ok(_) => panic!("scheduler worker spawn fails"),
            Err(error) => error,
        };
        assert!(matches!(error, SubscribeError::Spi(_)));
        assert_eq!(close_calls.load(Ordering::Acquire), 1);
        let source = Error::source(&error).expect("spawn source retained");
        assert_eq!(source.to_string(), "synthetic scheduler worker spawn failure");

        bus.inner.scheduler.fail_spawn_at(usize::MAX);
        let retry_request = SubscribeRequest::new(
            "scheduler-retry",
            Topic::<String>::new("spawn.test").expect("valid topic"),
        )
        .expect("valid subscriber ID");
        let _subscription = bus
            .subscribe(retry_request, |_| ())
            .expect("scheduler can restart after partial spawn failure");
        let shutdown_error = bus
            .shutdown(ShutdownMode::Immediate)
            .expect_err("cleanup close failure remains observable");
        assert!(matches!(shutdown_error, ShutdownError::SubscriptionClose(errors) if errors.len() == 2));
        assert_eq!(close_calls.load(Ordering::Acquire), 2);
    }
}
