// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal implementation of asynchronous delivery admission.

pub(in crate::facade) use self::async_admission_future::AsyncAdmissionFuture;
pub(in crate::facade) use self::async_admission_permit::AsyncAdmissionPermit;
pub(super) use self::async_admission_state::AsyncAdmissionState;

mod async_admission_future;
mod async_admission_permit;
mod async_admission_state;
