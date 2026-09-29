// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event and payload values retained by local queues.

mod local_event;
mod local_in_flight;
mod local_settlement_state;
mod shared_payload;

pub(in crate::local) use local_event::LocalEvent;
pub(in crate::local) use local_in_flight::LocalInFlight;
pub(in crate::local) use local_settlement_state::LocalSettlementHandle;
pub(in crate::local) use local_settlement_state::LocalSettlementState;
