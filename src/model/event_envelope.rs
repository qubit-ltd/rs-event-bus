//! Published event data and metadata without delivery acknowledgement state.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use super::EventId;
use super::Topic;

static NEXT_EVENT_ID: AtomicU64 = AtomicU64::new(1);

/// String headers carried with an event.
pub type Headers = BTreeMap<String, String>;

/// A type-safe event before any subscriber delivery is created.
pub struct EventEnvelope<T: 'static> {
    pub(crate) id: EventId,
    pub(crate) topic: Topic<T>,
    pub(crate) payload: T,
    pub(crate) headers: Headers,
    pub(crate) ordering_key: Option<Box<str>>,
    pub(crate) timestamp: SystemTime,
    pub(crate) delay: Option<Duration>,
}

impl<T: 'static> EventEnvelope<T> {
    /// Creates an event with an ID, current timestamp, and empty metadata.
    pub fn new(topic: Topic<T>, payload: T) -> Self {
        let sequence = NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed);
        let id =
            EventId::new(format!("event-{}-{sequence}", std::process::id())).expect("generated event IDs are valid");
        Self {
            id,
            topic,
            payload,
            headers: Headers::new(),
            ordering_key: None,
            timestamp: SystemTime::now(),
            delay: None,
        }
    }

    /// Returns the event identifier.
    pub fn id(&self) -> &EventId {
        &self.id
    }
    /// Returns the typed topic.
    pub fn topic(&self) -> &Topic<T> {
        &self.topic
    }
    /// Returns the payload without requiring it to be cloneable.
    pub fn payload(&self) -> &T {
        &self.payload
    }
    /// Returns all event headers.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
    /// Returns one header, or `None` when absent.
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(String::as_str)
    }
    /// Returns the ordering key, or `None` when delivery is unordered.
    pub fn ordering_key(&self) -> Option<&str> {
        self.ordering_key.as_deref()
    }
    /// Returns the creation timestamp.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
    /// Returns the requested delay, or `None` for immediate delivery.
    pub fn delay(&self) -> Option<Duration> {
        self.delay
    }
    /// Consumes the envelope and returns its payload.
    pub fn into_payload(self) -> T {
        self.payload
    }
}

impl<T: Clone + 'static> Clone for EventEnvelope<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            topic: self.topic.clone(),
            payload: self.payload.clone(),
            headers: self.headers.clone(),
            ordering_key: self.ordering_key.clone(),
            timestamp: self.timestamp,
            delay: self.delay,
        }
    }
}
