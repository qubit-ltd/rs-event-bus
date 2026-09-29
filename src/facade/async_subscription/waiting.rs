// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::facade::async_subscription::SessionSignals;
use crate::facade::async_subscription::SignalRegistration;

/// Waits for an operation unless the subscription stop signal wins first.
pub(in crate::facade::async_subscription) async fn await_or_stop<F>(
    future: F,
    control: &SessionSignals,
) -> Option<F::Output>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let registration = SignalRegistration::new(control.signal());
    std::future::poll_fn(|cx| {
        if control.is_stopped() {
            return std::task::Poll::Ready(None);
        }
        registration.register(cx.waker());
        if control.is_stopped() {
            return std::task::Poll::Ready(None);
        }
        match future.as_mut().poll(cx) {
            std::task::Poll::Ready(value) => std::task::Poll::Ready(Some(value)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    })
    .await
}
