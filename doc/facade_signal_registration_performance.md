# Facade signal registration performance check

## Method

The benchmark loads the production `AsyncSignal` module through a benchmark-only sibling source file. No signal algorithm is copied, and no public API was added. The baseline binary was built after restoring `register_waiter` to the implementation at HEAD `99cf32ab1b4ee517758e5b13e5f41df19573bbc8`; the SHA-256 of the HEAD signal source is `9c79d6c799d09e5f5ac485c86bf7111a82c3c7636fa29f287d002e397e2388eb`. Its benchmark binary SHA-256 is `28ffbe700af5d3f29a25a473a50e6e29aed2bb744926e1a7cd96e7ea04359498`.

The candidate binary used the temporary `will_wake` early-return implementation. Its signal source SHA-256 was `1ce1e6d1fa866f4ab18c55994c81f9c4393cb80642c69606f85f94b6bc9c1802`; its benchmark binary SHA-256 is `1293a8eadff7e3e4bc294c7c63cd2887d7c7c03a466c96bd3707b8b8b52ad77c`. The candidate source was removed after applying the decision rule below. Both binaries used the same benchmark source, `Cargo.lock`, optimized bench profile, and dependency graph.

Environment: Linux x86_64, kernel `6.8.0-147-generic`, 56 reported CPUs, Rust `rustc 1.94.0 (4a4ef493e 2026-03-02)`, Cargo `1.94.0 (85eff7c80 2026-01-15)`. Other workspace Cargo activity was visible on the host while measuring, so absolute timings are local evidence rather than a product performance guarantee.

At build time, SHA-256 identities were `f8ca4fc190bebc2e44c28d05107a8686c976568d7f9f315afe42a2c8e826e841` for `benches/facade_signal_registration.rs` and `983ce213fad5a3e461eb373daba0bbf25a1b4e610a667d6bd062d23f2c9048c3` for its path-module support source. The benchmark binary hashes above identify the exact compiled artifacts.

Each process performed two warmups and then recorded seven sample means for each scenario. Seven process pairs alternated order: baseline/candidate, candidate/baseline, baseline/candidate, candidate/baseline, baseline/candidate, candidate/baseline, baseline/candidate. Thus each version has 49 recorded sample means per scenario. Samples are nanoseconds per operation. The end-to-end scenario used the public local facade to subscribe, caller-poll its runner, perform four repeated idle polls with the same task waker before each publish, receive each publication, acknowledge it in the handler, and poll through settlement back to receive wait. It measured 250 publishes per sample. The direct scenarios registered one signal waiter 100,000 times with either one stable waker or alternating wakers.

Commands used to build each binary:

```text
cargo +1.94.0 bench --locked --bench facade_signal_registration --no-run
```

The 14 measurement processes invoked `/tmp/rs-event-bus-t2/baseline both` and `/tmp/rs-event-bus-t2/candidate both`. All process invocations exited 0. Complete process output was retained at `/tmp/rs-event-bus-t2/raw_process_samples.txt` during this implementation; all seven-sample vectors are reproduced below so the record does not depend on that temporary file.

Before applying the candidate, the same-waker clone-count regression was run against the baseline with `cargo +1.94.0 test --locked --lib facade::async_event_bus::internal::async_signal::tests::test_same_waker_registration_does_not_clone_again -- --exact --nocapture`. One test ran and failed as expected: two clones were observed where the candidate assertion expected one. After the performance decision, that optimization-specific assertion was replaced by behavior checks that pass on the retained implementation.

## Results

| Scenario | Baseline median | Baseline MAD | Candidate median | Candidate MAD | Candidate change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Same waker | 68 ns/op | 1 ns | 33 ns/op | 0 ns | -35 ns (-51.47%) |
| Alternating wakers | 68 ns/op | 0 ns | 91 ns/op | 0 ns | +23 ns (+33.82%) |
| Public end-to-end | 16,823 ns/op | 332 ns | 16,239 ns/op | 269 ns | -584 ns (-3.47%) |

MAD is the median absolute deviation from the median, computed over all 49 sample means for that version and scenario. The same-waker improvement exceeds both 5% and the larger MAD (1 ns). The alternating-waker regression exceeds 5% and both MADs (0 ns). The end-to-end change is an improvement smaller than its larger MAD (332 ns), so it does not show a clear end-to-end gain or regression.

The candidate fails the guardrail because alternating wakers became slower by 33.82%, beyond the measured spread. The production optimization is therefore not retained. This is consistent with the candidate taking an initial lookup lock and then a second insertion lock for every alternating waker, compared with the baseline's single insertion lock.

Callback counts (clone, drop, wake) were identical across the seven samples in each process:

| Scenario | Baseline | Candidate |
| --- | --- | --- |
| Same waker | (100001, 100000, 1) | (1, 0, 1) |
| Alternating wakers | (100001, 100000, 1) | (100000, 99999, 1) |
| Public end-to-end, 250 publishes | (2989, 2488, 516) | (1504, 1003, 516) |

## Raw sample means

The values in each vector are the concatenation of the seven samples from each of the seven processes for that version, in process-pair order. Every process used two warmups, omitted from these vectors.

```text
same baseline:
[68,68,67,68,68,68,67, 67,68,67,67,67,68,67, 67,67,67,67,68,67,68, 67,68,67,67,67,67,67, 86,71,68,67,67,68,68, 68,68,68,68,69,68,68, 68,68,67,67,68,68,70]
same candidate:
[33,33,33,33,33,33,33, 33,33,33,33,33,33,33, 33,33,33,33,33,33,33, 33,33,33,33,33,33,33, 33,33,33,33,33,33,33, 33,33,34,33,33,34,33, 33,33,33,33,33,33,33]
alternating baseline:
[68,68,68,68,68,68,68, 69,69,69,69,69,69,69, 68,68,68,68,68,68,68, 68,68,68,68,68,68,68, 68,68,68,68,68,68,68, 68,68,68,68,68,68,68, 68,69,69,68,68,68,68]
alternating candidate:
[91,91,91,91,91,91,91, 91,91,91,91,91,91,91, 91,91,91,91,91,91,91, 91,91,91,91,91,91,91, 91,91,91,91,91,91,91, 91,92,91,92,91,92,92, 91,92,92,92,92,92,96]
end-to-end baseline:
[19276,18884,18816,18744,18481,18666,18577, 16874,16743,16755,16680,16740,17354,16547, 16815,16706,16623,16695,16631,16714,17385, 17227,16823,16769,16578,16617,16635,16734, 16593,16601,16345,16365,16474,16491,16491, 17289,16839,17115,16647,17380,19243,17085, 18121,18289,18693,18490,18055,17947,17778]
end-to-end candidate:
[17175,16219,16120,16044,16120,16180,16215, 17139,16668,16745,16831,16614,16644,16762, 16268,16110,16239,16089,16155,16107,16201, 16436,16386,16505,16113,16216,16260,16387, 16683,16264,15951,15970,15960,15902,15892, 16476,15763,16119,15817,15926,15886,16034, 17784,17640,17588,17576,17504,17617,17229]
```

## Regression coverage retained

The performance optimization was withdrawn, but the behavior regressions remain useful: repeated same-waker registration followed by notification wakes once and can be registered again; an alternating waker replaces the previous one; notification from inside a waker clone callback does not prevent insertion; and existing tests verify clone/drop/wake callbacks run outside the registry lock. Tests use the repository's existing `RawWaker` ownership discipline.
