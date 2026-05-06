/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Transactional event bus contracts and unsupported fallback implementations.

mod transactional_event_bus;
mod transactional_publisher;
mod unsupported_transactional_event_bus;
mod unsupported_transactional_publisher;

pub use transactional_event_bus::TransactionalEventBus;
pub use transactional_publisher::TransactionalPublisher;
pub use unsupported_transactional_event_bus::UnsupportedTransactionalEventBus;
pub use unsupported_transactional_publisher::UnsupportedTransactionalPublisher;
