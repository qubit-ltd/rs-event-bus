use qubit_event_bus::Acknowledgement;

#[test]
fn test_acknowledgement_clone_shares_state() {
    let acknowledgement = Acknowledgement::new();
    let cloned = acknowledgement.clone();

    cloned.ack();

    assert!(acknowledgement.is_completed());
    assert!(acknowledgement.is_acked());
    assert!(!acknowledgement.is_nacked());
}

#[test]
fn test_acknowledgement_latest_decision_wins() {
    let acknowledgement = Acknowledgement::default();

    acknowledgement.ack();
    acknowledgement.nack();

    assert!(acknowledgement.is_completed());
    assert!(!acknowledgement.is_acked());
    assert!(acknowledgement.is_nacked());
}
