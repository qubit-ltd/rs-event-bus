// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private publisher-pipeline failure context types.

mod pipeline_failure;
mod pipeline_failure_origin;

pub(crate) use pipeline_failure::PipelineFailure;
pub(crate) use pipeline_failure_origin::PipelineFailureOrigin;
