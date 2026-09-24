// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! FIFO execution lanes keyed by subscription, topic, and ordering key.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use qubit_id::Id;

/// Identity of one per-key ordered lane.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct OrderingLaneKey {
    topic: Box<str>,
    ordering_key: Option<Box<str>>,
    subscription_id: Id,
}

impl OrderingLaneKey {
    /// Creates a lane identity from stable event and subscription metadata.
    pub(crate) fn new(topic: &str, ordering_key: Option<&str>, subscription_id: Id) -> Self {
        Self {
            topic: topic.into(),
            ordering_key: ordering_key.map(Into::into),
            subscription_id,
        }
    }
}

/// Blocking synchronous lanes. Async code must use [`AsyncOrderingLanes`].
#[cfg(test)]
pub(crate) struct OrderingLanes<T> {
    lanes: Mutex<HashMap<OrderingLaneKey, Weak<SyncLane<T>>>>,
    next_ticket: AtomicU64,
}

#[cfg(test)]
impl<T> OrderingLanes<T> {
    /// Creates an empty lane collection.
    pub(crate) fn new() -> Self {
        Self {
            lanes: Mutex::new(HashMap::new()),
            next_ticket: AtomicU64::new(1),
        }
    }

    /// Enqueues an item and returns a blocking turn ticket.
    pub(crate) fn enqueue(&self, key: OrderingLaneKey, value: T) -> OrderingTurn<T> {
        let lane = {
            let mut lanes = self.lanes.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            lanes.retain(|_, lane| lane.strong_count() != 0);
            lanes.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
                let lane = Arc::new(SyncLane::default());
                lanes.insert(key, Arc::downgrade(&lane));
                lane
            })
        };
        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        let is_leader = {
            let mut state = lane.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let is_leader = !state.active && state.queue.is_empty();
            state.queue.push_back((ticket, Some(value)));
            is_leader
        };
        OrderingTurn {
            lane,
            ticket: Some(ticket),
            is_leader,
        }
    }
}

#[cfg(test)]
impl<T> Default for OrderingLanes<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
struct SyncLane<T> {
    state: Mutex<SyncLaneState<T>>,
    changed: Condvar,
}
#[cfg(test)]
impl<T> Default for SyncLane<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(SyncLaneState {
                active: false,
                queue: VecDeque::new(),
            }),
            changed: Condvar::new(),
        }
    }
}
#[cfg(test)]
struct SyncLaneState<T> {
    active: bool,
    queue: VecDeque<(u64, Option<T>)>,
}

/// A synchronous lane ticket that waits until earlier work has completed.
#[cfg(test)]
pub(crate) struct OrderingTurn<T> {
    lane: Arc<SyncLane<T>>,
    ticket: Option<u64>,
    is_leader: bool,
}
#[cfg(test)]
impl<T> OrderingTurn<T> {
    /// Returns whether no earlier work was present at enqueue time.
    pub(crate) fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// Waits until this ticket reaches the head and acquires its lane guard.
    pub(crate) fn take(mut self) -> Option<OrderingGuard<T>> {
        let ticket = self.ticket?;
        let lane = self.lane.clone();
        let mut state = lane.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if !state.active && state.queue.front().is_some_and(|(queued, _)| *queued == ticket) {
                let (_, value) = state.queue.pop_front()?;
                state.active = true;
                self.ticket = None;
                return value.map(|value| OrderingGuard {
                    lane: lane.clone(),
                    value: Some(value),
                });
            }
            state = self
                .lane
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}
#[cfg(test)]
impl<T> Drop for OrderingTurn<T> {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        let mut state = self
            .lane
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = state.queue.iter().position(|(queued, _)| *queued == ticket) {
            state.queue.remove(index);
        }
        self.lane.changed.notify_all();
    }
}

/// Holds a sync lane across the complete handler/retry/settlement operation.
#[cfg(test)]
pub(crate) struct OrderingGuard<T> {
    lane: Arc<SyncLane<T>>,
    value: Option<T>,
}
#[cfg(test)]
impl<T> OrderingGuard<T> {
    /// Borrows the ordered item while retaining lane ownership.
    pub(crate) fn value(&self) -> &T {
        self.value.as_ref().expect("lane guard always owns its value")
    }
}
#[cfg(test)]
impl<T> Drop for OrderingGuard<T> {
    fn drop(&mut self) {
        let mut state = self
            .lane
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = false;
        self.lane.changed.notify_all();
    }
}

/// Nonblocking future-based lanes for runtime-neutral asynchronous facades.
pub(crate) struct AsyncOrderingLanes<T> {
    lanes: Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>,
    next_ticket: AtomicU64,
}
impl<T> AsyncOrderingLanes<T> {
    /// Creates an empty async lane collection.
    pub(crate) fn new() -> Self {
        Self {
            lanes: Mutex::new(HashMap::new()),
            next_ticket: AtomicU64::new(1),
        }
    }

    /// Enqueues an item and returns its nonblocking lane-turn future.
    pub(crate) fn enqueue(&self, key: OrderingLaneKey, value: T) -> AsyncOrderingTurn<T> {
        let lane = {
            let mut lanes = self.lanes.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            lanes.retain(|_, lane| lane.strong_count() != 0);
            lanes.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
                let lane = Arc::new(AsyncLane::default());
                lanes.insert(key, Arc::downgrade(&lane));
                lane
            })
        };
        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        lane.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .queue
            .push_back((ticket, Some(value), None));
        AsyncOrderingTurn {
            lane,
            ticket: Some(ticket),
        }
    }
}
impl<T> Default for AsyncOrderingLanes<T> {
    fn default() -> Self {
        Self::new()
    }
}

struct AsyncLane<T> {
    state: Mutex<AsyncLaneState<T>>,
}
impl<T> Default for AsyncLane<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(AsyncLaneState {
                active: false,
                queue: VecDeque::new(),
            }),
        }
    }
}
struct AsyncLaneState<T> {
    active: bool,
    queue: VecDeque<(u64, Option<T>, Option<Waker>)>,
}

/// A future that resolves when its async lane turn becomes available.
pub(crate) struct AsyncOrderingTurn<T> {
    lane: Arc<AsyncLane<T>>,
    ticket: Option<u64>,
}
impl<T> Future for AsyncOrderingTurn<T> {
    type Output = Option<AsyncOrderingGuard<T>>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(ticket) = self.ticket else {
            return Poll::Ready(None);
        };
        let lane = self.lane.clone();
        let mut state = lane.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.active && state.queue.front().is_some_and(|(queued, _, _)| *queued == ticket) {
            let (_, value, _) = state.queue.pop_front().expect("front ticket was observed");
            state.active = true;
            self.ticket = None;
            return Poll::Ready(value.map(|value| AsyncOrderingGuard {
                lane: lane.clone(),
                _value: Some(value),
            }));
        }
        let Some(waiter) = state
            .queue
            .iter_mut()
            .find(|(queued, _, _)| *queued == ticket)
            .map(|(_, _, waiter)| waiter)
        else {
            return Poll::Pending;
        };
        if waiter.as_ref().is_none_or(|old| !old.will_wake(context.waker())) {
            *waiter = Some(context.waker().clone());
        }
        Poll::Pending
    }
}
impl<T> Drop for AsyncOrderingTurn<T> {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        let waker = {
            let mut state = self
                .lane
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(index) = state.queue.iter().position(|(queued, _, _)| *queued == ticket) {
                state.queue.remove(index);
            }
            front_waker(&state)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Holds an async lane without blocking the executor thread.
pub(crate) struct AsyncOrderingGuard<T> {
    lane: Arc<AsyncLane<T>>,
    _value: Option<T>,
}
#[cfg(test)]
impl<T> AsyncOrderingGuard<T> {
    /// Borrows the ordered item while the lane is held.
    pub(crate) fn value(&self) -> &T {
        self._value.as_ref().expect("lane guard always owns its value")
    }
}
impl<T> Drop for AsyncOrderingGuard<T> {
    fn drop(&mut self) {
        let waker = {
            let mut state = self
                .lane
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.active = false;
            front_waker(&state)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

fn front_waker<T>(state: &AsyncLaneState<T>) -> Option<Waker> {
    state.queue.front().and_then(|(_, _, waker)| waker.clone())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use qubit_id::Id;

    use super::AsyncOrderingLanes;
    use super::OrderingLaneKey;

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn dropping_a_waiter_wakes_the_next_lane_turn_after_waker_update() {
        let lanes = AsyncOrderingLanes::new();
        let key = OrderingLaneKey::new("orders", Some("customer-7"), Id::new(41));
        let mut leader = Box::pin(lanes.enqueue(key.clone(), "leader"));
        let mut second = Box::pin(lanes.enqueue(key.clone(), "cancelled"));
        let mut third = Box::pin(lanes.enqueue(key, "next"));

        let leader_wakes = Arc::new(WakeCounter::default());
        let second_old_wakes = Arc::new(WakeCounter::default());
        let second_new_wakes = Arc::new(WakeCounter::default());
        let third_wakes = Arc::new(WakeCounter::default());
        let leader_waker = Waker::from(leader_wakes);
        let second_old_waker = Waker::from(second_old_wakes.clone());
        let second_new_waker = Waker::from(second_new_wakes.clone());
        let third_waker = Waker::from(third_wakes.clone());
        let mut leader_context = Context::from_waker(&leader_waker);
        let mut old_context = Context::from_waker(&second_old_waker);
        let mut updated_context = Context::from_waker(&second_new_waker);
        let mut third_context = Context::from_waker(&third_waker);

        let Poll::Ready(Some(leader_guard)) = leader.as_mut().poll(&mut leader_context) else {
            panic!("first queued turn should acquire the idle lane");
        };
        assert_eq!("leader", *leader_guard.value());

        assert!(matches!(second.as_mut().poll(&mut old_context), Poll::Pending));
        assert!(matches!(second.as_mut().poll(&mut old_context), Poll::Pending));
        assert!(matches!(second.as_mut().poll(&mut updated_context), Poll::Pending));
        assert!(matches!(third.as_mut().poll(&mut third_context), Poll::Pending));
        assert_eq!(0, second_old_wakes.0.load(Ordering::Acquire));
        assert_eq!(0, second_new_wakes.0.load(Ordering::Acquire));
        assert_eq!(0, third_wakes.0.load(Ordering::Acquire));

        drop(second);
        assert_eq!(1, third_wakes.0.load(Ordering::Acquire));
        assert!(matches!(third.as_mut().poll(&mut third_context), Poll::Pending));

        drop(leader_guard);
        assert_eq!(2, third_wakes.0.load(Ordering::Acquire));
        let Poll::Ready(Some(next_guard)) = third.as_mut().poll(&mut third_context) else {
            panic!("next waiter should acquire the lane after the leader exits");
        };
        assert_eq!("next", *next_guard.value());
        assert!(matches!(third.as_mut().poll(&mut third_context), Poll::Ready(None)));
    }
}
