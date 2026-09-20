use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::mpsc;

use qubit_event_bus::DispatchStatus;
use qubit_event_bus::EventBusError;
use qubit_event_bus::EventEnvelope;
use qubit_event_bus::LocalEventBusFactory;
use qubit_event_bus::PublishOutcome;
use qubit_event_bus::SubscribeOptions;
use qubit_event_bus::Topic;

#[test]
fn test_batch_publish_counts_dropped_items_separately_from_accepted_items() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_publisher_interceptor::<String, _>(|event: EventEnvelope<String>| {
            if event.payload() == "drop" {
                None
            } else {
                Some(event)
            }
        })
        .expect("publisher interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = Topic::<String>::try_new("batch-contract-dropped")
        .expect("topic should build");

    bus.subscribe("subscriber", &topic, |_| ())
        .expect("subscriber should register");
    let events = ["keep", "drop"]
        .into_iter()
        .map(|payload| EventEnvelope::create(topic.clone(), payload.to_string()))
        .collect();

    let result = bus.publish_all(events).expect("batch should publish");

    assert_eq!(result.total_count(), 2);
    assert_eq!(result.accepted_count(), 1);
    assert_eq!(result.dropped_count(), 1);
    assert_eq!(result.failure_count(), 0);
}

#[test]
fn test_batch_publish_counts_item_with_rejected_delivery_as_failure() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    factory
        .set_max_in_flight_deliveries(1)
        .expect("one in-flight delivery should be accepted");
    let bus = factory.create_started().expect("bus should start");
    let topic = Topic::<String>::try_new("batch-contract-rejected")
        .expect("topic should build");
    let (started_sender, started_receiver) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let handler_release = Arc::clone(&release);

    bus.subscribe_with_options(
        "accepted",
        &topic,
        move |_| {
            started_sender
                .send(())
                .expect("test should receive handler start");
            let (release_lock, release_condvar) = &*handler_release;
            let mut released = release_lock.lock().expect("release gate should lock");
            while !*released {
                released = release_condvar
                    .wait(released)
                    .expect("release gate should not poison");
            }
        },
        SubscribeOptions::builder().priority(1).build(),
    )
    .expect("accepted subscriber should register");
    bus.subscribe("rejected", &topic, |_| ())
        .expect("rejected subscriber should register");

    let event = EventEnvelope::create(topic.clone(), "payload".to_string());
    let result = bus
        .publish_all(vec![event])
        .expect("batch should return a receipt");
    started_receiver
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("first subscriber should start");

    assert_eq!(result.total_count(), 1);
    assert_eq!(result.accepted_count(), 1);
    assert_eq!(result.failure_count(), 1);
    assert!(!result.is_success());
    let receipt = result.items()[0]
        .result()
        .as_ref()
        .expect("publish should return a receipt");
    assert!(matches!(
        receipt.outcome(),
        PublishOutcome::Dispatched(items)
            if items.iter().any(|item| matches!(item.status(), DispatchStatus::Accepted))
                && items.iter().any(|item| matches!(
                    item.status(),
                    DispatchStatus::Rejected(EventBusError::ExecutionRejected { .. })
                ))
    ));

    let (release_lock, release_condvar) = &*release;
    *release_lock.lock().expect("release gate should lock") = true;
    release_condvar.notify_all();
    bus.wait_for_idle(&topic).expect("topic should become idle");
}
