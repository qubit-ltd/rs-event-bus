# Ordering measurement evidence

See [the performance decision](../ordering_performance.md). These artifacts
preserve the measurements independently of the temporary execution directory.

- `paired_samples.txt`: exact original paired-run output, including all 672
  accepted samples, all 748 rejection records and their reasons, case identity,
  variant, warmup/sample settings, wall/thread/process CPU values, wait values,
  median, MAD, load average and process scheduling observations. Original labels
  are retained; facade `input_keys` denotes the case parameter, not the actual
  unique-key count for same-key or churn. The decision explains that mapping.
- `paired_results.json`: parsed accepted records indexed by original case and
  variant, with their associated median, MAD and rejection count. The original
  text is the primary record; this JSON is a derived convenience view.
- `paired_runner.txt`: exact pairing wrapper used in this measurement. Its
  original temporary paths describe provenance, rather than an installed tool.
- `cpu_flat_profile.txt` and `cpu_line_profile.txt`: exact gprof output from the
  matching debug-bearing binary and newly generated histogram. Inline retain
  source locations identify 223 of 284 CPU samples.
- `profile_run.json`: actual CPU-profile invocation, wall/CPU duration, exit
  status, scheduling observations and background-work boundary.
- `measurement_manifest.json`: build/hardware parameters, baseline/candidate
  binary SHA-256, profile binary and histogram SHA-256, validity rule, decision
  conditions and artifact checksums. Temporary paths identify provenance and are
  not required to read the persisted evidence.
- `source_sha256.json`: complete frozen Rust source hash inventory.
- `ordering_source_snapshot.json`: exact frozen benchmark, Cargo manifest/lock,
  and ordering-module source contents with hashes. This preserves the production
  algorithm used for profile attribution and the original benchmark metadata.
- `candidate.patch`: complete experimental source change, including private
  regression checks. It was not applied to production.
- `profiling_probe.txt`: CPU profiler validation; two functions have the same
  500-call count but sample CPU fractions of 100% and 0%.
- `profiling_probe_source.txt`: exact source used by that profiler validation.
- `cargo_entrypoint.txt`: actual cached Cargo benchmark entrypoint verification;
  this additional single-case run is separate from the paired decision dataset.

The comparison used frozen 0.16.0 migration snapshots with only two ordering
files changed between variants. Final benchmark typechecking against the
integrated 0.17.0 API passed. Final reporting names the facade case budget and
actual distinct input keys separately; that metadata correction does not rewrite
the historical raw records or change the measured operations.
