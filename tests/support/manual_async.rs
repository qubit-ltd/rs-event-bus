// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tiny executor used to prove the async SPI does not require a runtime.

use std::future::Future;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::Thread;
use std::thread::current;
use std::thread::park;

/// Wakes the executor thread by un-parking it when a future becomes ready.
struct ThreadWake(Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Drives a future to completion on the current thread without a runtime.
///
/// # Type Parameters
/// - `F`: Future to execute.
///
/// # Parameters
/// - `future`: Operation to poll until it returns `Ready`.
///
/// # Returns
/// The future's output.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => park(),
        }
    }
}

/// Polls a pinned future once using a waker for the current thread.
///
/// # Type Parameters
/// - `F`: Future to poll.
///
/// # Parameters
/// - `future`: Pinned future whose current state is requested.
///
/// # Returns
/// The result of this single poll.
pub(crate) fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(ThreadWake(current())));
    future.poll(&mut Context::from_waker(&waker))
}
