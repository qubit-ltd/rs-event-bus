use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DeliveryGap;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_id::Id;

fn assert_sync_object_safe(_: Option<&dyn EventBusSpi>) {}
fn assert_async_object_safe(_: Option<&dyn AsyncEventBusSpi>) {}

#[test]
fn test_spi_traits_are_object_safe() {
    assert_sync_object_safe(None);
    assert_async_object_safe(None);
}

#[test]
fn test_settlement_token_rejects_a_different_subscription_origin() {
    let owning_subscription = Id::new(17);
    let another_subscription = Id::new(18);
    let token = SettlementToken::new(owning_subscription, "provider-token");

    assert!(token.belongs_to(owning_subscription));
    assert!(!token.belongs_to(another_subscription));
}

#[test]
fn test_external_provider_can_construct_a_gap_receive_outcome() {
    let gap = DeliveryGap::new("provider reported lag", Some(3));
    let outcome = ReceiveOutcome::Gap(gap);

    let ReceiveOutcome::Gap(gap) = outcome else {
        panic!("expected a gap outcome");
    };
    assert_eq!(gap.reason.as_ref(), "provider reported lag");
    assert_eq!(gap.missed, Some(3));
}
