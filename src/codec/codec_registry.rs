// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed codec registry for sharing application codecs across topics.

use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

use super::EventCodec;

/// Stores at most one codec per Rust payload type.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::codec::CodecRegistry;
///
/// let registry = CodecRegistry::new();
/// assert!(registry.get::<String>().is_none());
/// ```
#[derive(Default)]
pub struct CodecRegistry {
    /// Type-indexed codec objects retained by the registry.
    codecs: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl CodecRegistry {
    /// Creates an empty registry with no registered payload codecs.
    ///
    /// # Returns
    /// An empty registry that can be populated with [`Self::register`].
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the codec for `T`, or `None` if it was not registered.
    ///
    /// # Type Parameters
    /// * `T` — payload type whose codec is requested.
    ///
    /// # Returns
    /// A cloned shared codec handle, or `None` when no codec is registered.
    #[must_use]
    #[inline]
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<dyn EventCodec<T>>> {
        self.codecs
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<Arc<dyn EventCodec<T>>>().cloned())
    }

    /// Registers a codec, replacing any previous codec for `T`.
    ///
    /// # Type Parameters
    /// * `T` — payload type encoded and decoded by `codec`.
    ///
    /// # Parameters
    /// * `codec` — shared codec implementation to retain for `T`.
    ///
    /// The codec is shared with future lookups through [`Self::get`].
    pub fn register<T: Send + Sync + 'static>(&mut self, codec: Arc<dyn EventCodec<T>>) {
        self.codecs.insert(TypeId::of::<T>(), Box::new(codec));
    }
}
