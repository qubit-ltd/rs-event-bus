// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private attempt lifetime evidence for asynchronous publication retries.

mod in_flight_publish;

pub(super) use in_flight_publish::InFlightPublish;
