use std::io;

/// Reports whether a child sample is a successful measurement.
pub(crate) fn sample_succeeded(timed_out: bool, status: &str) -> bool {
    !timed_out && status == "success"
}

/// Runs every measured sample and reports whether the completed batch failed.
pub(crate) fn run_measured_batch<F>(samples: usize, mut sample: F) -> io::Result<()>
where
    F: FnMut(usize) -> io::Result<bool>,
{
    let mut failed = 0;
    for iteration in 1..=samples {
        if !sample(iteration)? {
            failed += 1;
        }
    }
    if failed == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{failed} of {samples} measured samples failed"
        )))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_seven_successful_samples_return_ok_in_order() {
        let mut iterations = Vec::new();

        let result = super::run_measured_batch(7, |iteration| {
            iterations.push(iteration);
            Ok(true)
        });

        assert!(result.is_ok());
        assert_eq!(iterations, [1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_failed_sample_does_not_stop_seven_sample_batch() {
        for failed_iteration in [1, 4, 7] {
            let mut iterations = Vec::new();
            let result = super::run_measured_batch(7, |iteration| {
                iterations.push(iteration);
                Ok(iteration != failed_iteration)
            });

            let error = result.expect_err("a failed measured sample must fail the batch");
            assert_eq!(error.kind(), std::io::ErrorKind::Other);
            assert!(error.to_string().contains("1 of 7 measured samples failed"));
            assert_eq!(iterations, [1, 2, 3, 4, 5, 6, 7]);
        }
    }

    #[test]
    fn test_sample_succeeded_uses_timeout_and_status() {
        let cases = [
            (false, "success", true),
            (true, "timeout", false),
            (false, "panic", false),
            (false, "child_failed", false),
            (false, "subscribe_failed", false),
            (false, "receiver_failed", false),
        ];

        for (timed_out, status, expected) in cases {
            assert_eq!(
                super::sample_succeeded(timed_out, status),
                expected,
                "{timed_out}, {status}"
            );
        }
    }

    #[test]
    fn test_sample_io_error_is_preserved_and_stops_batch() {
        let mut iterations = Vec::new();
        let result = super::run_measured_batch(7, |iteration| {
            iterations.push(iteration);
            if iteration == 3 {
                Err(std::io::Error::from_raw_os_error(7))
            } else {
                Ok(true)
            }
        });

        let error = result.expect_err("sample I/O failure must propagate");
        assert_eq!(error.raw_os_error(), Some(7));
        assert_eq!(iterations, [1, 2, 3]);
    }
}
