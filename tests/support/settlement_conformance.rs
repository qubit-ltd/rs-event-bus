// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Real local-provider Retry and Reject behavior fixtures for conformance hooks.

use std::any::TypeId;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ServiceProvider;

use super::manual_async::block_on;

fn setup() -> Result<TopicAddress, String> {
    TopicAddress::new("conformance.settlement.behavior").map_err(|e| e.to_string())
}

fn request(topic: TopicAddress, id: u64) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        topic,
        SubscriberId::new(format!("settlement-fixture-{id}")).expect("valid fixture subscriber"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<u32>(),
    )
}

fn message(topic: TopicAddress, id: EventId, value: u32) -> OutboundMessage {
    OutboundMessage::new(
        topic,
        id,
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(value)),
    )
}

fn verify_sync(
    mut receiver: Box<dyn EventSubscriptionSpi>,
    spi: Arc<dyn EventBusSpi>,
    topic: TopicAddress,
) -> Result<(), String> {
    let result = verify_sync_body(receiver.as_mut(), spi.as_ref(), topic);
    let close_result = receiver.close().map_err(|e| format!("close fixture receiver: {e}"));
    let shutdown_result = spi
        .shutdown(ShutdownMode::Immediate)
        .map_err(|e| format!("shutdown fixture provider: {e}"));
    match (result, close_result, shutdown_result) {
        (Ok(()), Ok(()), Ok(ShutdownOutcome::Complete)) => Ok(()),
        (Err(error), close, shutdown) => Err(format!("{error}; cleanup close={close:?}, shutdown={shutdown:?}")),
        (Ok(()), Err(error), shutdown) => Err(format!("{error}; shutdown={shutdown:?}")),
        (Ok(()), Ok(()), Ok(outcome)) => Err(format!("fixture provider shutdown was incomplete: {outcome:?}")),
        (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

fn verify_sync_body(
    receiver: &mut dyn EventSubscriptionSpi,
    spi: &dyn EventBusSpi,
    topic: TopicAddress,
) -> Result<(), String> {
    let retry_id = EventId::generate().map_err(|e| e.to_string())?;
    let ack = spi
        .publish(message(topic.clone(), retry_id.clone(), 1))
        .map_err(|e| format!("publish Retry A: {e}"))?;
    if !matches!(ack, PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.len() == 1 && matches!(admissions[0].status(), AdmissionStatus::Accepted))
    {
        return Err("Retry A publication was not admitted".into());
    }
    let mut first = match receiver
        .receive(Duration::from_secs(1))
        .map_err(|e| format!("receive Retry A: {e}"))?
    {
        ReceiveOutcome::Message(message) => message,
        _ => return Err("receive returned a non-message outcome".into()),
    };
    if first.id() != &retry_id {
        return Err("Retry A identity changed before settlement".into());
    }
    let token = first.take_settlement().ok_or("Retry A omitted token")?;
    receiver
        .settle(&token, DeliveryDisposition::Retry)
        .map_err(|e| format!("first Retry A: {e}"))?;
    receiver
        .settle(&token, DeliveryDisposition::Retry)
        .map_err(|e| format!("repeated Retry A: {e}"))?;
    let mut retried = match receiver
        .receive(Duration::from_secs(1))
        .map_err(|e| format!("receive retried A: {e}"))?
    {
        ReceiveOutcome::Message(message) => message,
        _ => return Err("receive returned a non-message outcome".into()),
    };
    if retried.id() != &retry_id {
        return Err("Retry returned a different event".into());
    }
    let retry_token = retried.take_settlement().ok_or("retried A omitted token")?;
    receiver
        .settle(&retry_token, DeliveryDisposition::Accept)
        .map_err(|e| format!("Accept retried A: {e}"))?;
    if !matches!(
        receiver
            .receive(Duration::from_millis(10))
            .map_err(|e| format!("check extra A: {e}"))?,
        ReceiveOutcome::TimedOut
    ) {
        return Err("Retry A produced an extra copy after Accept".into());
    }

    let reject_id = EventId::generate().map_err(|e| e.to_string())?;
    let ack = spi
        .publish(message(topic.clone(), reject_id, 2))
        .map_err(|e| format!("publish Reject B: {e}"))?;
    if !matches!(ack, PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.len() == 1 && matches!(admissions[0].status(), AdmissionStatus::Accepted))
    {
        return Err("Reject B publication was not admitted".into());
    }
    let mut rejected = match receiver
        .receive(Duration::from_secs(1))
        .map_err(|e| format!("receive Reject B: {e}"))?
    {
        ReceiveOutcome::Message(message) => message,
        _ => return Err("receive returned a non-message outcome".into()),
    };
    let reject_token = rejected.take_settlement().ok_or("Reject B omitted token")?;
    receiver
        .settle(&reject_token, DeliveryDisposition::Reject)
        .map_err(|e| format!("first Reject B: {e}"))?;
    receiver
        .settle(&reject_token, DeliveryDisposition::Reject)
        .map_err(|e| format!("repeated Reject B: {e}"))?;
    let control_id = EventId::generate().map_err(|e| e.to_string())?;
    let ack = spi
        .publish(message(topic, control_id.clone(), 3))
        .map_err(|e| format!("publish control C (Reject reservation release): {e}"))?;
    if !matches!(ack, PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.len() == 1 && matches!(admissions[0].status(), AdmissionStatus::Accepted))
    {
        return Err("Reject did not release capacity for control C".into());
    }
    let mut control = match receiver
        .receive(Duration::from_secs(1))
        .map_err(|e| format!("receive control C: {e}"))?
    {
        ReceiveOutcome::Message(message) => message,
        _ => return Err("receive returned a non-message outcome".into()),
    };
    if control.id() != &control_id {
        return Err("Reject B reappeared instead of control C".into());
    }
    let control_token = control.take_settlement().ok_or("control C omitted token")?;
    receiver
        .settle(&control_token, DeliveryDisposition::Accept)
        .map_err(|e| format!("Accept control C: {e}"))?;
    Ok(())
}

pub(crate) fn sync_local_fixture() -> Result<(), String> {
    sync_local_fixture_with_fault(None)
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum BehaviorFault {
    DropRetry,
    RequeueReject,
}

pub(crate) fn sync_local_fixture_with_fault(fault: Option<BehaviorFault>) -> Result<(), String> {
    let topic = setup()?;
    let config = qubit_event_bus::registry::EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().queue_capacity(1).provider_options());
    let spi = qubit_event_bus::local::LocalEventBusProvider
        .create_configured(&config)
        .map_err(|e| e.to_string())?;
    let receiver = spi
        .subscribe(request(topic.clone(), 11))
        .map_err(|e| format!("subscribe: {e}"))?;
    let receiver: Box<dyn EventSubscriptionSpi> = match fault {
        Some(fault) => Box::new(FaultySyncReceiver { inner: receiver, fault }),
        None => receiver,
    };
    verify_sync(receiver, spi, topic)
}

pub(crate) async fn async_local_fixture() -> Result<(), String> {
    async_local_fixture_with_fault(None).await
}

pub(crate) async fn async_local_fixture_with_fault(fault: Option<BehaviorFault>) -> Result<(), String> {
    let topic = setup()?;
    let config = qubit_event_bus::registry::EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().queue_capacity(1).provider_options());
    let spi = qubit_event_bus::local::AsyncLocalEventBusProvider
        .create_configured(&config)
        .await
        .map_err(|e| e.to_string())?;
    let receiver = spi
        .subscribe(request(topic.clone(), 12))
        .await
        .map_err(|e| format!("async subscribe: {e}"))?;
    let mut receiver: Box<dyn AsyncEventSubscriptionSpi> = match fault {
        Some(fault) => Box::new(FaultyAsyncReceiver { inner: receiver, fault }),
        None => receiver,
    };
    let result: Result<(), String> = async {
    let retry_id = EventId::generate().map_err(|e| e.to_string())?;
    let ack = spi
        .publish(message(topic.clone(), retry_id.clone(), 1))
        .await
        .map_err(|e| format!("async publish Retry A: {e}"))?;
    if !matches!(ack, PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.len() == 1 && matches!(admissions[0].status(), AdmissionStatus::Accepted))
    {
        return Err("async Retry A publication was not admitted".into());
    }
    let mut first = match receiver
        .receive(Duration::from_secs(1))
        .await
        .map_err(|e| format!("async receive Retry A: {e}"))?
    {
        ReceiveOutcome::Message(message) => message,
        _ => return Err("receive returned a non-message outcome".into()),
    };
    let token = first.take_settlement().ok_or("async Retry A omitted token")?;
    receiver
        .settle(&token, DeliveryDisposition::Retry)
        .await
        .map_err(|e| format!("async first Retry A: {e}"))?;
    receiver
        .settle(&token, DeliveryDisposition::Retry)
        .await
        .map_err(|e| format!("async repeated Retry A: {e}"))?;
    let mut retried = match receiver
        .receive(Duration::from_secs(1))
        .await
        .map_err(|e| format!("async receive retried A: {e}"))?
    {
        ReceiveOutcome::Message(message) => message,
        _ => return Err("receive returned a non-message outcome".into()),
    };
    if retried.id() != &retry_id {
        return Err("async Retry returned a different event".into());
    }
    let retry_token = retried.take_settlement().ok_or("async retried A omitted token")?;
    receiver
        .settle(&retry_token, DeliveryDisposition::Accept)
        .await
        .map_err(|e| format!("async Accept retried A: {e}"))?;
    if !matches!(
        receiver
            .receive(Duration::from_millis(10))
            .await
            .map_err(|e| format!("async check extra A: {e}"))?,
        ReceiveOutcome::TimedOut
    ) {
        return Err("async Retry A produced an extra copy after Accept".into());
    }
    let reject_id = EventId::generate().map_err(|e| e.to_string())?;
    let ack = spi
        .publish(message(topic.clone(), reject_id, 2))
        .await
        .map_err(|e| format!("async publish Reject B: {e}"))?;
    if !matches!(ack, PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.len() == 1 && matches!(admissions[0].status(), AdmissionStatus::Accepted))
    {
        return Err("async Reject B publication was not admitted".into());
    }
    let mut rejected = match receiver
        .receive(Duration::from_secs(1))
        .await
        .map_err(|e| format!("async receive Reject B: {e}"))?
    {
        ReceiveOutcome::Message(message) => message,
        _ => return Err("receive returned a non-message outcome".into()),
    };
    let reject_token = rejected.take_settlement().ok_or("async Reject B omitted token")?;
    receiver
        .settle(&reject_token, DeliveryDisposition::Reject)
        .await
        .map_err(|e| format!("async first Reject B: {e}"))?;
    receiver
        .settle(&reject_token, DeliveryDisposition::Reject)
        .await
        .map_err(|e| format!("async repeated Reject B: {e}"))?;
    let control_id = EventId::generate().map_err(|e| e.to_string())?;
    let ack = spi
        .publish(message(topic.clone(), control_id.clone(), 3))
        .await
        .map_err(|e| format!("async publish control C (Reject reservation release): {e}"))?;
    if !matches!(ack, PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.len() == 1 && matches!(admissions[0].status(), AdmissionStatus::Accepted))
    {
        return Err("async Reject did not release capacity for control C".into());
    }
    let mut control = match receiver
        .receive(Duration::from_secs(1))
        .await
        .map_err(|e| format!("async receive control C: {e}"))?
    {
        ReceiveOutcome::Message(message) => message,
        _ => return Err("receive returned a non-message outcome".into()),
    };
    if control.id() != &control_id {
        return Err("async Reject B reappeared instead of control C".into());
    }
    let control_token = control.take_settlement().ok_or("async control C omitted token")?;
    receiver
        .settle(&control_token, DeliveryDisposition::Accept)
        .await
        .map_err(|e| format!("async Accept control C: {e}"))?;
    Ok(())
    }.await;
    let close_result = receiver
        .close()
        .await
        .map_err(|e| format!("async close fixture receiver: {e}"));
    let shutdown_result = spi
        .shutdown(ShutdownMode::Immediate)
        .await
        .map_err(|e| format!("async shutdown fixture provider: {e}"));
    match (result, close_result, shutdown_result) {
        (Ok(()), Ok(()), Ok(ShutdownOutcome::Complete)) => Ok(()),
        (Err(error), close, shutdown) => Err(format!("{error}; cleanup close={close:?}, shutdown={shutdown:?}")),
        (Ok(()), Err(error), shutdown) => Err(format!("{error}; shutdown={shutdown:?}")),
        (Ok(()), Ok(()), Ok(outcome)) => Err(format!("async fixture provider shutdown was incomplete: {outcome:?}")),
        (Ok(()), Ok(()), Err(error)) => Err(error),
    }
}

#[allow(dead_code)]
pub(crate) fn run_async_fixture() -> Result<(), String> {
    block_on(async_local_fixture())
}

struct FaultySyncReceiver {
    inner: Box<dyn EventSubscriptionSpi>,
    fault: BehaviorFault,
}
impl EventSubscriptionSpi for FaultySyncReceiver {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, qubit_event_bus::SpiError> {
        self.inner.receive(timeout)
    }
    fn settle(
        &mut self,
        token: &qubit_event_bus::spi::SettlementToken,
        disposition: DeliveryDisposition,
    ) -> Result<(), qubit_event_bus::SpiError> {
        match (self.fault, disposition) {
            (BehaviorFault::DropRetry, DeliveryDisposition::Retry) => Ok(()),
            (BehaviorFault::RequeueReject, DeliveryDisposition::Reject) => {
                self.inner.settle(token, DeliveryDisposition::Retry)
            }
            _ => self.inner.settle(token, disposition),
        }
    }
    fn close(&mut self) -> Result<(), qubit_event_bus::SpiError> {
        self.inner.close()
    }
}

struct FaultyAsyncReceiver {
    inner: Box<dyn AsyncEventSubscriptionSpi>,
    fault: BehaviorFault,
}
impl AsyncEventSubscriptionSpi for FaultyAsyncReceiver {
    fn receive<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> qubit_event_bus::spi::SpiFuture<'a, Result<ReceiveOutcome, qubit_event_bus::SpiError>> {
        self.inner.receive(timeout)
    }
    fn settle<'a>(
        &'a mut self,
        token: &qubit_event_bus::spi::SettlementToken,
        disposition: DeliveryDisposition,
    ) -> qubit_event_bus::spi::SpiFuture<'a, Result<(), qubit_event_bus::SpiError>> {
        match (self.fault, disposition) {
            (BehaviorFault::DropRetry, DeliveryDisposition::Retry) => Box::pin(async { Ok(()) }),
            (BehaviorFault::RequeueReject, DeliveryDisposition::Reject) => {
                self.inner.settle(token, DeliveryDisposition::Retry)
            }
            _ => self.inner.settle(token, disposition),
        }
    }
    fn close<'a>(&'a mut self) -> qubit_event_bus::spi::SpiFuture<'a, Result<(), qubit_event_bus::SpiError>> {
        self.inner.close()
    }
}
