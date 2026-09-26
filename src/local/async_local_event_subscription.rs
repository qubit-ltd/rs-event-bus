// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation-safe asynchronous receiver for one local mailbox.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::PoisonError;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use qubit_id::Id;

use super::async_local_event_bus_spi::AsyncLocalShared;
use super::async_local_event_bus_spi::AsyncMailbox;
use super::async_local_event_bus_spi::close_mailbox;
use super::local_event_bus_spi::invalid_token_error;
use super::state::LocalSettlementState;

type ReceiveTimer = Pin<Box<dyn Future<Output = Result<(), qubit_clock::TimeError>> + Send>>;
use crate::error::SpiError;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::DeliveryDisposition;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementToken;
use crate::spi::SpiFuture;

pub(super) struct AsyncLocalEventSubscription {
    shared: Arc<AsyncLocalShared>,
    mailbox: Arc<AsyncMailbox>,
    subscription_id: Id,
    closed: bool,
}

impl AsyncLocalEventSubscription {
    pub(super) fn new(shared: Arc<AsyncLocalShared>, mailbox: Arc<AsyncMailbox>, subscription_id: Id) -> Self {
        Self {
            shared,
            mailbox,
            subscription_id,
            closed: false,
        }
    }
}

impl AsyncEventSubscriptionSpi for AsyncLocalEventSubscription {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        let queue = Arc::clone(&self.mailbox.queue);
        let subscription_id = self.subscription_id;
        let timer = Arc::clone(&self.shared.timer);
        Box::pin(async move {
            if timeout.is_zero() {
                let mut state = queue.lock();
                if state.closed {
                    return Ok(ReceiveOutcome::Closed);
                }
                return Ok(pop_message(&mut state, subscription_id).unwrap_or(ReceiveOutcome::TimedOut));
            }
            let started = Instant::now();
            let mut waiter = None;
            let mut timer_future: Option<ReceiveTimer> = None;
            let mut timer_wait = None;
            std::future::poll_fn(|cx| {
                let mut state = queue.lock();
                if state.closed {
                    return Poll::Ready(Ok(ReceiveOutcome::Closed));
                }
                if let Some(message) = pop_message(&mut state, subscription_id) {
                    return Poll::Ready(Ok(message));
                }
                let remaining = if timeout == Duration::MAX {
                    None
                } else {
                    let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                        return Poll::Ready(Ok(ReceiveOutcome::TimedOut));
                    };
                    if remaining.is_zero() {
                        return Poll::Ready(Ok(ReceiveOutcome::TimedOut));
                    }
                    Some(remaining)
                };
                let now = Instant::now();
                let delay = state.next_ready_delay(now);
                let wait_for = match (delay, remaining) {
                    (Some(delay), Some(remaining)) => Some(delay.min(remaining)),
                    (Some(delay), None) => Some(delay),
                    (None, Some(remaining)) => Some(remaining),
                    (None, None) => None,
                };
                if let Some(wait_for) = wait_for {
                    let deadline = now.checked_add(wait_for);
                    if timer_wait.is_none_or(|previous| deadline.is_some_and(|current| current < previous)) {
                        timer_wait = deadline;
                        let timer = Arc::clone(&timer);
                        timer_future = Some(Box::pin(async move {
                            let timer = timer.after(wait_for)?;
                            timer.await
                        }));
                    }
                } else {
                    timer_wait = None;
                    timer_future = None;
                }
                waiter = Some(queue.async_ready.register(cx.waker()));
                drop(state);
                if let Some(timer) = timer_future.as_mut()
                    && let Poll::Ready(result) = Future::poll(timer.as_mut(), cx)
                {
                    timer_future = None;
                    timer_wait = None;
                    match result {
                        Ok(()) => {
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Err(error) => {
                            return Poll::Ready(Err(SpiError::Operation {
                                provider_id: "local".into(),
                                operation: "receive",
                                resource: Some(queue.topic.as_str().into()),
                                kind: "timer_error",
                                retryable: Some(false),
                                source: Box::new(error),
                            }));
                        }
                    }
                }
                Poll::Pending
            })
            .await
        })
    }

    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        let queue = Arc::clone(&self.mailbox.queue);
        let subscription_id = self.subscription_id;
        let belongs = token.belongs_to(subscription_id);
        let settlement = token.downcast_ref::<super::state::LocalSettlementHandle>().cloned();
        Box::pin(async move {
            if !belongs {
                return Err(invalid_token_error(Some(queue.topic.as_str()), "foreign_subscription"));
            }
            let settlement =
                settlement.ok_or_else(|| invalid_token_error(Some(queue.topic.as_str()), "unknown_token"))?;
            let mut state = queue.lock();
            let mut token_state = settlement.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(previous) = token_state.disposition {
                return if previous == disposition {
                    Ok(())
                } else {
                    Err(invalid_token_error(
                        Some(queue.topic.as_str()),
                        "conflicting_disposition",
                    ))
                };
            }
            let Some(delivery) = state.in_flight.get(token_state.token_id.as_ref()) else {
                return Err(invalid_token_error(Some(queue.topic.as_str()), "unknown_token"));
            };
            if !Arc::ptr_eq(&delivery.settlement, &settlement) {
                return Err(invalid_token_error(Some(queue.topic.as_str()), "unknown_token"));
            }
            let delivery = state
                .in_flight
                .remove(token_state.token_id.as_ref())
                .expect("validated in-flight delivery");
            if disposition == DeliveryDisposition::Retry {
                state.enqueue_front(delivery.event);
            } else {
                self.shared.outstanding.release(1);
            }
            token_state.disposition = Some(disposition);
            drop(token_state);
            drop(state);
            queue.async_ready.notify_all();
            self.shared.changed.notify_all();
            Ok(())
        })
    }

    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        let shared = Arc::clone(&self.shared);
        let mailbox = Arc::clone(&self.mailbox);
        Box::pin(async move {
            close_mailbox(&shared, &mailbox);
            self.closed = true;
            Ok(())
        })
    }
}

impl Drop for AsyncLocalEventSubscription {
    fn drop(&mut self) {
        if !self.closed {
            close_mailbox(&self.shared, &self.mailbox);
        }
    }
}

fn pop_message(state: &mut super::state::LocalQueueState, subscription_id: Id) -> Option<ReceiveOutcome> {
    let event = state.pop_ready(Instant::now())?;
    let next = state.next_delivery_token.checked_add(1)?;
    state.next_delivery_token = next;
    let token_id = format!("{}:{next}", event.event_id()).into_boxed_str();
    let settlement = Arc::new(std::sync::Mutex::new(LocalSettlementState {
        token_id: token_id.clone(),
        disposition: None,
    }));
    state.in_flight.insert(
        token_id,
        super::state::LocalInFlight {
            event: event.clone(),
            settlement: settlement.clone(),
        },
    );
    Some(ReceiveOutcome::Message(event.into_inbound(subscription_id, settlement)))
}
