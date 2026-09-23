//! Runtime-neutral future type used by asynchronous backends.

use std::future::Future;
use std::pin::Pin;

/// A sendable boxed future without a dependency on a particular executor.
pub type SpiFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
