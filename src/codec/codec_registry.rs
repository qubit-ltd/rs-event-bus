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
#[derive(Default)]
pub struct CodecRegistry {
    codecs: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl CodecRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a codec, replacing any previous codec for `T`.
    pub fn register<T: Send + Sync + 'static>(&mut self, codec: Arc<dyn EventCodec<T>>) {
        self.codecs.insert(TypeId::of::<T>(), Box::new(codec));
    }

    /// Returns the codec for `T`, or `None` if it was not registered.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<dyn EventCodec<T>>> {
        self.codecs
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<Arc<dyn EventCodec<T>>>().cloned())
    }
}
