#[cfg(coverage)]
use qubit_event_bus::{
    coverage_exercise_core_defensive_paths,
    coverage_exercise_event_bus_factory_default_regions,
};
use std::time::Duration;

use qubit_event_bus::{
    EventBus,
    EventBusError,
    EventBusFactory,
    EventBusResult,
    EventEnvelope,
    IntoEventBusResult,
    LocalEventBusFactory,
    PublishOptions,
    SubscribeOptions,
    Subscription,
    Topic,
    UnsupportedTransactionalEventBus,
};

#[derive(Clone, Debug)]
struct FailingStartBus;

#[derive(Clone, Debug)]
struct SuccessfulStartBus;

impl EventBus for FailingStartBus {
    fn start(&self) -> EventBusResult<bool> {
        Err(EventBusError::start_failed("start failed"))
    }

    fn shutdown(&self) -> bool {
        false
    }

    fn publish_envelope_with_options<T>(
        &self,
        _envelope: EventEnvelope<T>,
        _options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(())
    }

    fn subscribe_with_options<T, S, F, R>(
        &self,
        _subscriber_id: S,
        _topic: &Topic<T>,
        _handler: F,
        _options: SubscribeOptions<T>,
    ) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        Err(EventBusError::unsupported_operation("subscribe"))
    }

    fn wait_for_idle<T>(&self, _topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        Ok(())
    }

    fn wait_for_idle_timeout<T>(
        &self,
        _topic: &Topic<T>,
        _timeout: Duration,
    ) -> EventBusResult<bool>
    where
        T: 'static,
    {
        Ok(true)
    }
}

impl EventBus for SuccessfulStartBus {
    fn start(&self) -> EventBusResult<bool> {
        Ok(true)
    }

    fn shutdown(&self) -> bool {
        true
    }

    fn publish_envelope_with_options<T>(
        &self,
        _envelope: EventEnvelope<T>,
        _options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(())
    }

    fn subscribe_with_options<T, S, F, R>(
        &self,
        _subscriber_id: S,
        _topic: &Topic<T>,
        _handler: F,
        _options: SubscribeOptions<T>,
    ) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        Err(EventBusError::unsupported_operation("subscribe"))
    }

    fn wait_for_idle<T>(&self, _topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        Ok(())
    }

    fn wait_for_idle_timeout<T>(
        &self,
        _topic: &Topic<T>,
        _timeout: Duration,
    ) -> EventBusResult<bool>
    where
        T: 'static,
    {
        Ok(true)
    }
}

struct FailingStartFactory;

struct SuccessfulStartFactory;

impl EventBusFactory for FailingStartFactory {
    type Bus = FailingStartBus;
    type TransactionalBus = UnsupportedTransactionalEventBus;

    fn create(&self) -> Self::Bus {
        FailingStartBus
    }
}

impl EventBusFactory for SuccessfulStartFactory {
    type Bus = SuccessfulStartBus;
    type TransactionalBus = UnsupportedTransactionalEventBus;

    fn create(&self) -> Self::Bus {
        SuccessfulStartBus
    }
}

#[test]
fn test_event_bus_factory_create_returns_stopped_bus() {
    let factory = LocalEventBusFactory::new();
    let bus = EventBusFactory::create(&factory);
    let topic = Topic::<String>::try_new("factory-stopped").expect("topic should build");

    let error = bus
        .publish(&topic, "payload".to_string())
        .expect_err("factory-created bus should start stopped");

    assert_eq!(error, EventBusError::not_started());
}

#[test]
fn test_event_bus_factory_reports_transactions_unsupported() {
    let factory = LocalEventBusFactory::new();

    assert!(!EventBusFactory::is_transactional_supported(&factory));
    assert_eq!(
        EventBusFactory::create_transactional(&factory)
            .expect_err("local factory should not create transactional bus"),
        EventBusError::unsupported_operation("create_transactional")
    );
}

#[test]
fn test_event_bus_factory_create_started_propagates_start_error() {
    let factory = FailingStartFactory;

    assert_eq!(
        EventBusFactory::create_started(&factory).expect_err("start failure should propagate"),
        EventBusError::start_failed("start failed")
    );
}

#[test]
fn test_event_bus_factory_create_started_returns_started_bus() {
    let factory = SuccessfulStartFactory;

    let bus = EventBusFactory::create_started(&factory).expect("start should succeed");

    assert!(bus.shutdown());
}

#[cfg(coverage)]
#[test]
fn test_coverage_event_bus_factory_default_regions() {
    let errors = coverage_exercise_event_bus_factory_default_regions();
    let observations = coverage_exercise_core_defensive_paths();

    assert_eq!(errors.len(), 16);
    assert!(
        errors
            .iter()
            .all(|error| error.kind() == "unsupported_operation")
    );
    assert!(observations.into_iter().all(|observed| observed));
}
