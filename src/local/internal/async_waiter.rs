// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Registration guard that removes its waker when dropped.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Waker;

pub(in crate::local) struct AsyncWaiter {
    id: u64,
    waiters: Arc<Mutex<HashMap<u64, Waker>>>,
}

impl AsyncWaiter {
    pub(in crate::local) fn new(id: u64, waiters: Arc<Mutex<HashMap<u64, Waker>>>) -> Self {
        Self { id, waiters }
    }
}

impl Drop for AsyncWaiter {
    fn drop(&mut self) {
        self.waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}
