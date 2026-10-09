// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public facade contract tests for provider admission acknowledgements.

use std::collections::VecDeque;
use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_event_bus::CheckedPublishError;
use qubit_event_bus::CodecError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::AdmissionCheckError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::AdmissionSummary;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_id::Id;

struct AdmissionProvider {
    acknowledgements: Mutex<VecDeque<PublishAcknowledgement>>,
    visibility: PublishVisibility,
    payload_modes: PayloadModes,
    publish_calls: AtomicUsize,
}

impl EventBusSpi for AdmissionProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            self.payload_modes,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            self.visibility,
        )
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.publish_calls.fetch_add(1, Ordering::AcqRel);
        Ok(self
            .acknowledgements
            .lock()
            .expect("acknowledgement queue lock")
            .pop_front()
            .expect("test acknowledgement was configured"))
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(provider_error("subscribe"))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

fn provider_error(operation: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "admission-test".into(),
        operation,
        resource: None,
        kind: "unsupported",
        retryable: Some(false),
        source: Box::new(IoError::other("unsupported in this test provider")),
    }
}

/// Sends one event through a facade and returns the configured provider result.
fn publish(acknowledgement: PublishAcknowledgement) -> PublishAcknowledgement {
    let provider = Arc::new(AdmissionProvider {
        acknowledgements: Mutex::new(VecDeque::from([acknowledgement])),
        visibility: PublishVisibility::DestinationAdmissions,
        payload_modes: PayloadModes::Native,
        publish_calls: AtomicUsize::new(0),
    });
    let bus = EventBus::from_spi(ProviderId::new("admission-test").expect("valid provider ID"), provider)
        .expect("valid provider capabilities");
    let topic = Topic::<String>::new("orders.created").expect("valid topic");
    let request = PublishRequest::new(topic, "order-1".to_owned()).expect("request builds");
    let receipt = bus
        .publish(request)
        .expect("admission is a successful publication receipt");
    receipt.acknowledgement().clone()
}

/// Builds a receipt whose admission result can be checked without a provider.
fn receipt(acknowledgement: PublishAcknowledgement) -> PublishReceipt {
    PublishReceipt::new(
        EventId::new("event-1").expect("valid event ID"),
        None,
        ProviderId::new("admission-test").expect("valid provider ID"),
        acknowledgement,
    )
}

/// Builds one destination with a stable ID for each table row.
fn destination(id: u64, status: AdmissionStatus) -> DestinationAdmission {
    DestinationAdmission::new(
        Id::new(id),
        SubscriberId::new(format!("subscriber-{id}")).expect("valid subscriber ID"),
        status,
    )
}

#[test]
fn test_admission_checks_cover_every_acknowledgement_outcome() {
    let cases = [
        (
            "opaque accepted",
            PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            },
            None,
            Err(AdmissionCheckError::VisibilityUnavailable),
            Err(AdmissionCheckError::VisibilityUnavailable),
            Ok(()),
        ),
        (
            "interceptor dropped",
            PublishAcknowledgement::DroppedByInterceptor,
            None,
            Err(AdmissionCheckError::Dropped),
            Err(AdmissionCheckError::Dropped),
            Err(AdmissionCheckError::Dropped),
        ),
        (
            "empty snapshot",
            PublishAcknowledgement::DestinationAdmissions(vec![]),
            Some(AdmissionSummary::default()),
            Err(AdmissionCheckError::NoAcceptedDestination),
            Err(AdmissionCheckError::NoAcceptedDestination),
            Err(AdmissionCheckError::NoAcceptedDestination),
        ),
        (
            "filtered only",
            PublishAcknowledgement::DestinationAdmissions(vec![destination(1, AdmissionStatus::Filtered)]),
            Some(AdmissionSummary {
                accepted: 0,
                filtered: 1,
                rejected: 0,
            }),
            Err(AdmissionCheckError::NoAcceptedDestination),
            Err(AdmissionCheckError::NoAcceptedDestination),
            Err(AdmissionCheckError::NoAcceptedDestination),
        ),
        (
            "rejected only",
            PublishAcknowledgement::DestinationAdmissions(vec![
                destination(1, AdmissionStatus::Rejected("full".into())),
                destination(2, AdmissionStatus::Rejected("closed".into())),
            ]),
            Some(AdmissionSummary {
                accepted: 0,
                filtered: 0,
                rejected: 2,
            }),
            Err(AdmissionCheckError::NoAcceptedDestination),
            Err(AdmissionCheckError::NoAcceptedDestination),
            Err(AdmissionCheckError::NoAcceptedDestination),
        ),
        (
            "accepted only",
            PublishAcknowledgement::DestinationAdmissions(vec![destination(1, AdmissionStatus::Accepted)]),
            Some(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 0,
            }),
            Ok(()),
            Ok(()),
            Ok(()),
        ),
        (
            "partial acceptance",
            PublishAcknowledgement::DestinationAdmissions(vec![
                destination(1, AdmissionStatus::Accepted),
                destination(2, AdmissionStatus::Rejected("full".into())),
            ]),
            Some(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 1,
            }),
            Ok(()),
            Err(AdmissionCheckError::RejectedDestinations { count: 1 }),
            Ok(()),
        ),
    ];

    for (name, acknowledgement, summary, at_least_one, no_rejected, provider_or_destination) in cases {
        let receipt = receipt(acknowledgement.clone());
        assert_eq!(summary, receipt.admission_summary(), "{name}");
        assert_eq!(
            at_least_one,
            receipt.check_admission(AdmissionRequirement::AtLeastOneAccepted),
            "{name}"
        );
        assert_eq!(
            no_rejected,
            receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected),
            "{name}"
        );
        assert_eq!(
            provider_or_destination,
            receipt.check_admission(AdmissionRequirement::ProviderOrDestinationAccepted),
            "{name}"
        );
        assert_eq!(
            &acknowledgement,
            receipt.acknowledgement(),
            "{name}: original receipt changed"
        );
    }
}

#[test]
fn test_checked_publish_returns_the_receipt_when_admission_fails() {
    let provider = Arc::new(AdmissionProvider {
        acknowledgements: Mutex::new(VecDeque::from([PublishAcknowledgement::DestinationAdmissions(vec![])])),
        visibility: PublishVisibility::DestinationAdmissions,
        payload_modes: PayloadModes::Native,
        publish_calls: AtomicUsize::new(0),
    });
    let bus = EventBus::from_spi(ProviderId::new("admission-test").unwrap(), provider).unwrap();
    let request = PublishRequest::new(Topic::<String>::new("orders.created").unwrap(), "order-2".to_owned()).unwrap();
    let error = bus
        .publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)
        .expect_err("no destination was admitted");
    match error {
        qubit_event_bus::CheckedPublishError::Admission { receipt, reason } => {
            assert_eq!(receipt.admission_outcome(), AdmissionOutcome::NoDestinations);
            assert_eq!(reason, AdmissionCheckError::NoAcceptedDestination);
        }
        other => panic!("expected admission error with receipt, got {other:?}"),
    }
}

/// Counts codec work so an unsupported checked publish cannot encode a payload.
struct CountingCodec {
    content_type: ContentType,
    calls: Arc<AtomicUsize>,
}

impl EventCodec<String> for CountingCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::from(value.as_bytes()))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<String, CodecError> {
        String::from_utf8(payload.bytes().to_vec()).map_err(|source| CodecError::Decode {
            source: Box::new(source),
        })
    }
}

#[test]
fn test_checked_publish_opaque_preflight_has_no_side_effects() {
    let provider_id = ProviderId::new("opaque-test").expect("valid provider ID");
    let provider = Arc::new(AdmissionProvider {
        acknowledgements: Mutex::new(VecDeque::from([PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        }])),
        visibility: PublishVisibility::Opaque,
        payload_modes: PayloadModes::Encoded,
        publish_calls: AtomicUsize::new(0),
    });
    let interceptor_calls = Arc::new(AtomicUsize::new(0));
    let global_calls = interceptor_calls.clone();
    let config = EventBusFacadeConfig::new().publisher_interceptor(move |_| {
        global_calls.fetch_add(1, Ordering::AcqRel);
        Ok(true)
    });
    let bus = EventBus::with_config(provider_id.clone(), provider.clone(), config)
        .expect("valid opaque provider capabilities");
    let codec_calls = Arc::new(AtomicUsize::new(0));
    let topic = Topic::new("orders.opaque")
        .expect("valid topic")
        .with_codec(CountingCodec {
            content_type: ContentType::TEXT_PLAIN,
            calls: codec_calls.clone(),
        });

    for requirement in [
        AdmissionRequirement::AtLeastOneAccepted,
        AdmissionRequirement::AtLeastOneAcceptedAndNoRejected,
    ] {
        let typed_calls = interceptor_calls.clone();
        let options = PublishOptions::<String>::builder()
            .interceptor(move |envelope| {
                typed_calls.fetch_add(1, Ordering::AcqRel);
                Ok(Some(envelope))
            })
            .build();
        let request = PublishRequest::new(topic.clone(), "order".to_owned())
            .expect("valid request")
            .with_options(options);
        let event_id = request.envelope().id().clone();
        let error = bus
            .publish_checked(request, requirement)
            .expect_err("opaque provider cannot report destinations");
        assert!(matches!(
            error,
            CheckedPublishError::UnsupportedVisibility {
                event_id: actual_event_id,
                provider_id: actual_provider_id
            } if actual_event_id == event_id && actual_provider_id == provider_id
        ));
        assert_eq!(interceptor_calls.load(Ordering::Acquire), 0);
        assert_eq!(codec_calls.load(Ordering::Acquire), 0);
        assert_eq!(provider.publish_calls.load(Ordering::Acquire), 0);
        assert_eq!(bus.publish_metrics(), Default::default());
    }

    let receipt = bus
        .publish_checked(
            PublishRequest::new(topic, "order".to_owned()).expect("valid request"),
            AdmissionRequirement::ProviderOrDestinationAccepted,
        )
        .expect("provider acceptance satisfies the new condition");
    assert_eq!(receipt.admission_outcome(), AdmissionOutcome::OpaqueAccepted);
    assert_eq!(provider.publish_calls.load(Ordering::Acquire), 1);

    let visible_provider = Arc::new(AdmissionProvider {
        acknowledgements: Mutex::new(VecDeque::from([PublishAcknowledgement::DestinationAdmissions(vec![])])),
        visibility: PublishVisibility::DestinationAdmissions,
        payload_modes: PayloadModes::Native,
        publish_calls: AtomicUsize::new(0),
    });
    let visible_bus = EventBus::from_spi(provider_id, visible_provider.clone()).expect("valid visible provider");
    let error = visible_bus
        .publish_checked(
            PublishRequest::new(Topic::<String>::new("orders.visible").unwrap(), "order".to_owned()).unwrap(),
            AdmissionRequirement::ProviderOrDestinationAccepted,
        )
        .expect_err("no destinations accepted");
    assert!(
        matches!(error, CheckedPublishError::Admission { receipt, reason: AdmissionCheckError::NoAcceptedDestination }
        if receipt.admission_outcome() == AdmissionOutcome::NoDestinations)
    );
    assert_eq!(visible_provider.publish_calls.load(Ordering::Acquire), 1);
}

#[test]
fn test_admission_outcome_classifies_every_provider_result() {
    let cases = [
        (
            PublishAcknowledgement::Accepted {
                provider_message_id: None,
                metadata: Default::default(),
            },
            AdmissionOutcome::OpaqueAccepted,
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(Vec::new()),
            AdmissionOutcome::NoDestinations,
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![destination(1, AdmissionStatus::Filtered)]),
            AdmissionOutcome::NoneAccepted(AdmissionSummary {
                accepted: 0,
                filtered: 1,
                rejected: 0,
            }),
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![destination(
                1,
                AdmissionStatus::Rejected("full".into()),
            )]),
            AdmissionOutcome::NoneAccepted(AdmissionSummary {
                accepted: 0,
                filtered: 0,
                rejected: 1,
            }),
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![destination(1, AdmissionStatus::Accepted)]),
            AdmissionOutcome::Accepted(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 0,
            }),
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![
                destination(1, AdmissionStatus::Accepted),
                destination(2, AdmissionStatus::Filtered),
            ]),
            AdmissionOutcome::Accepted(AdmissionSummary {
                accepted: 1,
                filtered: 1,
                rejected: 0,
            }),
        ),
        (
            PublishAcknowledgement::DestinationAdmissions(vec![
                destination(1, AdmissionStatus::Accepted),
                destination(2, AdmissionStatus::Rejected("full".into())),
            ]),
            AdmissionOutcome::PartiallyAccepted(AdmissionSummary {
                accepted: 1,
                filtered: 0,
                rejected: 1,
            }),
        ),
        (PublishAcknowledgement::DroppedByInterceptor, AdmissionOutcome::Dropped),
    ];

    for (acknowledgement, expected) in cases {
        let receipt = receipt(acknowledgement.clone());
        assert_eq!(expected, acknowledgement.admission_outcome());
        assert_eq!(expected, receipt.admission_outcome());
        let expected_summary = match expected {
            AdmissionOutcome::Accepted(summary)
            | AdmissionOutcome::PartiallyAccepted(summary)
            | AdmissionOutcome::NoneAccepted(summary) => Some(summary),
            AdmissionOutcome::NoDestinations => Some(AdmissionSummary::default()),
            AdmissionOutcome::OpaqueAccepted | AdmissionOutcome::Dropped => None,
            _ => panic!("unexpected admission outcome in test case"),
        };
        assert_eq!(expected_summary, receipt.admission_summary());
    }
}

#[test]
fn test_admission_summary_ignores_destination_order() {
    let statuses = [
        AdmissionStatus::Filtered,
        AdmissionStatus::Rejected("full".into()),
        AdmissionStatus::Accepted,
        AdmissionStatus::Rejected("closed".into()),
        AdmissionStatus::Accepted,
    ];
    let expected = Some(AdmissionSummary {
        accepted: 2,
        filtered: 1,
        rejected: 2,
    });
    for reversed in [false, true] {
        let mut destinations = statuses
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, status)| destination(index as u64 + 1, status))
            .collect::<Vec<_>>();
        if reversed {
            destinations.reverse();
        }
        let receipt = receipt(PublishAcknowledgement::DestinationAdmissions(destinations));
        assert_eq!(expected, receipt.admission_summary());
    }
}

#[test]
fn test_publish_receipt_preserves_empty_destination_snapshot() {
    assert_eq!(
        PublishAcknowledgement::DestinationAdmissions(Vec::new()),
        publish(PublishAcknowledgement::DestinationAdmissions(Vec::new()))
    );
}

#[test]
fn test_publish_receipt_preserves_partial_destination_admission() {
    let accepted_id = SubscriberId::new("accepted-subscriber").expect("valid subscriber ID");
    let rejected_id = SubscriberId::new("full-subscriber").expect("valid subscriber ID");
    let acknowledgement = publish(PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(Id::new(1), accepted_id, AdmissionStatus::Accepted),
        DestinationAdmission::new(
            Id::new(2),
            rejected_id,
            AdmissionStatus::Rejected("subscription queue is full".into()),
        ),
    ]));
    let PublishAcknowledgement::DestinationAdmissions(destinations) = acknowledgement else {
        panic!("provider destination admissions must be preserved");
    };
    assert_eq!(2, destinations.len());
    assert_eq!("accepted-subscriber", destinations[0].subscriber_id().as_str());
    assert_eq!(&AdmissionStatus::Accepted, destinations[0].status());
    assert_eq!("full-subscriber", destinations[1].subscriber_id().as_str());
    assert_eq!(
        &AdmissionStatus::Rejected("subscription queue is full".into()),
        destinations[1].status()
    );
}

#[test]
fn test_publish_receipt_is_successful_when_every_destination_rejects_admission() {
    let acknowledgement = publish(PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(
            Id::new(3),
            SubscriberId::new("full-subscriber").expect("valid subscriber ID"),
            AdmissionStatus::Rejected("subscription queue is full".into()),
        ),
    ]));
    let PublishAcknowledgement::DestinationAdmissions(destinations) = acknowledgement else {
        panic!("provider destination admissions must be preserved");
    };
    assert_eq!(1, destinations.len());
    assert!(matches!(destinations[0].status(), AdmissionStatus::Rejected(_)));
}
