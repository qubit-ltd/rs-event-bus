// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Minimal provider subscription implementations using the public SPI paths.

use std::collections::HashMap;
use std::time::Duration;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::spi::{
    AsyncEventSubscriptionSpi, DeliveryDisposition, EventSubscriptionSpi, ReceiveOutcome, SettlementToken, SpiFuture,
    SpiSubscriptionRequest,
};
use qubit_id::Id;

/// A synchronous receiver that records settlement by provider token value.
pub struct SyncSubscription {
    subscription_id: Id,
    settled: HashMap<u64, DeliveryDisposition>,
    closed: bool,
}

impl SyncSubscription {
    /// Creates a receiver and retains the ID supplied in the provider request.
    pub fn from_request(request: &SpiSubscriptionRequest) -> Self {
        let subscription_id = request.subscription_id();
        Self {
            subscription_id,
            settled: HashMap::new(),
            closed: false,
        }
    }
}

impl EventSubscriptionSpi for SyncSubscription {
    fn receive(&mut self, _timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        Ok(if self.closed {
            ReceiveOutcome::Closed
        } else {
            ReceiveOutcome::TimedOut
        })
    }

    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        if !token.belongs_to(self.subscription_id) {
            return Err(invalid_token("subscription_mismatch"));
        }
        record_settlement(&mut self.settled, token, disposition)
    }

    fn close(&mut self) -> Result<(), SpiError> {
        self.closed = true;
        Ok(())
    }
}

/// An asynchronous receiver with the same token semantics as the sync example.
pub struct AsyncSubscription {
    subscription_id: Id,
    settled: HashMap<u64, DeliveryDisposition>,
    closed: bool,
}

impl AsyncSubscription {
    /// Creates a receiver and retains the ID supplied in the provider request.
    pub fn from_request(request: &SpiSubscriptionRequest) -> Self {
        let subscription_id = request.subscription_id();
        Self {
            subscription_id,
            settled: HashMap::new(),
            closed: false,
        }
    }
}

impl AsyncEventSubscriptionSpi for AsyncSubscription {
    fn receive<'a>(&'a mut self, _timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async move {
            Ok(if self.closed {
                ReceiveOutcome::Closed
            } else {
                ReceiveOutcome::TimedOut
            })
        })
    }

    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        let result = if !token.belongs_to(self.subscription_id) {
            Err(invalid_token("subscription_mismatch"))
        } else {
            record_settlement(&mut self.settled, token, disposition)
        };
        Box::pin(async move { result })
    }

    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.closed = true;
        Box::pin(async { Ok(()) })
    }
}

fn record_settlement(
    settled: &mut HashMap<u64, DeliveryDisposition>,
    token: &SettlementToken,
    disposition: DeliveryDisposition,
) -> Result<(), SpiError> {
    let Some(value) = token.downcast_ref::<u64>() else {
        return Err(invalid_token("token_type_mismatch"));
    };
    match settled.get(value) {
        Some(previous) if *previous == disposition => Ok(()),
        Some(_) => Err(invalid_token("conflicting_disposition")),
        None => {
            settled.insert(*value, disposition);
            Ok(())
        }
    }
}

fn invalid_token(reason: &'static str) -> SpiError {
    SpiError::InvalidSettlementToken {
        provider_id: "documentation".into(),
        operation: "settle",
        resource: None,
        reason,
        retryable: Some(false),
        source: Box::new(std::io::Error::other("settlement token rejected")),
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;
    use std::time::Duration;

    use qubit_event_bus::error::SpiError;
    use qubit_event_bus::model::ProviderOptions;
    use qubit_event_bus::model::StartPosition;
    use qubit_event_bus::model::SubscriberId;
    use qubit_event_bus::model::SubscriptionDurability;
    use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
    use qubit_event_bus::spi::DeliveryDisposition;
    use qubit_event_bus::spi::EventSubscriptionSpi;
    use qubit_event_bus::spi::ReceiveOutcome;
    use qubit_event_bus::spi::SettlementToken;
    use qubit_event_bus::spi::SpiSubscriptionRequest;
    use qubit_event_bus::spi::TopicAddress;
    use qubit_id::Id;

    use super::AsyncSubscription;
    use super::SyncSubscription;

    fn request(id: Id) -> SpiSubscriptionRequest {
        SpiSubscriptionRequest::new(
            id,
            TopicAddress::new("documentation.events").expect("valid topic"),
            SubscriberId::new("documentation").expect("valid subscriber id"),
            None,
            SubscriptionDurability::Ephemeral,
            StartPosition::New,
            ProviderOptions::new(),
            TypeId::of::<()>(),
        )
    }

    #[test]
    fn sync_provider_contract_is_idempotent_and_rejects_conflict() {
        let request = request(Id::new(1));
        let mut receiver = SyncSubscription::from_request(&request);
        assert!(matches!(receiver.receive(Duration::ZERO), Ok(ReceiveOutcome::TimedOut)));
        let token = SettlementToken::new(request.subscription_id(), 7_u64);
        assert!(receiver.settle(&token, DeliveryDisposition::Accept).is_ok());
        assert!(receiver.settle(&token, DeliveryDisposition::Accept).is_ok());
        assert!(matches!(
            receiver.settle(&token, DeliveryDisposition::Reject),
            Err(SpiError::InvalidSettlementToken {
                reason: "conflicting_disposition",
                ..
            })
        ));
        assert!(receiver.close().is_ok());
        assert!(matches!(receiver.receive(Duration::ZERO), Ok(ReceiveOutcome::Closed)));
    }

    #[tokio::test]
    async fn async_provider_contract_is_idempotent_and_rejects_conflict() {
        let request = request(Id::new(2));
        let mut receiver = AsyncSubscription::from_request(&request);
        assert!(matches!(
            receiver.receive(Duration::ZERO).await,
            Ok(ReceiveOutcome::TimedOut)
        ));
        let token = SettlementToken::new(request.subscription_id(), 9_u64);
        assert!(receiver.settle(&token, DeliveryDisposition::Retry).await.is_ok());
        assert!(receiver.settle(&token, DeliveryDisposition::Retry).await.is_ok());
        assert!(matches!(
            receiver.settle(&token, DeliveryDisposition::Accept).await,
            Err(SpiError::InvalidSettlementToken {
                reason: "conflicting_disposition",
                ..
            })
        ));
        assert!(receiver.close().await.is_ok());
        assert!(matches!(
            receiver.receive(Duration::ZERO).await,
            Ok(ReceiveOutcome::Closed)
        ));
    }
}
