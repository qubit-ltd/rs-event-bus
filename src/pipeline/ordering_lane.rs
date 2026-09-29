// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! FIFO execution lanes keyed by subscription, topic, and ordering key.

mod internal;

pub(crate) use internal::AsyncOrderingGuard;
pub(crate) use internal::AsyncOrderingLanes;
#[cfg(test)]
pub(crate) use internal::AsyncOrderingTurn;
pub(crate) use internal::OrderingLaneKey;
#[cfg(test)]
pub(crate) use internal::OrderingLanes;

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
    fn test_dropping_a_waiter_wakes_the_next_lane_turn_after_waker_update() {
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
