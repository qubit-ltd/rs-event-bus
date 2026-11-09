// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared portable header boundary validation.

use crate::error::HeaderLimitError;
use crate::facade::HeaderLimits;
use crate::model::Headers;

/// Checks a portable header collection against count and byte boundaries.
///
/// Count is checked first. Bytes are the sum of UTF-8 key and value lengths;
/// the function reads strings in place and does not copy or log their content.
///
/// # Parameters
/// - `headers`: Headers produced by the event or provider.
/// - `limits`: Inclusive count and aggregate-byte boundaries.
///
/// # Returns
/// `Ok(())` when both limits are satisfied, otherwise the exceeded dimension.
pub(crate) fn validate_headers(headers: &Headers, limits: HeaderLimits) -> Result<(), HeaderLimitError> {
    if headers.len() > limits.max_count().get() {
        return Err(HeaderLimitError::CountExceeded {
            limit: limits.max_count().get(),
        });
    }
    let mut remaining = limits.max_bytes().get();
    for (key, value) in headers {
        for len in [key.len(), value.len()] {
            remaining = remaining.checked_sub(len).ok_or(HeaderLimitError::BytesExceeded {
                limit: limits.max_bytes().get(),
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::validate_headers;
    use crate::error::HeaderLimitError;
    use crate::facade::HeaderLimits;
    use crate::model::Headers;

    fn limits(count: usize, bytes: usize) -> HeaderLimits {
        HeaderLimits::new(NonZeroUsize::new(count).unwrap(), NonZeroUsize::new(bytes).unwrap())
    }

    #[test]
    fn test_accepts_empty_and_exact_utf8_byte_boundary() {
        assert_eq!(validate_headers(&Headers::new(), limits(1, 1)), Ok(()));
        let headers = Headers::from([("é".to_owned(), "x".to_owned())]);
        assert_eq!(validate_headers(&headers, limits(1, 3)), Ok(()));
        assert_eq!(
            validate_headers(&headers, limits(1, 2)),
            Err(HeaderLimitError::BytesExceeded { limit: 2 })
        );
    }

    #[test]
    fn test_count_precedes_byte_budget_and_key_bytes_are_counted() {
        let headers = Headers::from([("long".to_owned(), "v".to_owned()), ("k".to_owned(), "v".to_owned())]);
        assert_eq!(
            validate_headers(&headers, limits(1, 1)),
            Err(HeaderLimitError::CountExceeded { limit: 1 })
        );
        assert_eq!(validate_headers(&headers, limits(2, 7)), Ok(()));
        assert_eq!(
            validate_headers(&headers, limits(2, 6)),
            Err(HeaderLimitError::BytesExceeded { limit: 6 })
        );
    }
}
