# Ordering lane performance decision

Measured on 2026-09-29. **Keep the current production Weak registry.** The
allocation-identity Drop cleanup candidate improves the high-key registry stress
cases by 82.12–97.97%, but fails the maximum 5% default-case regression gate.
With four live keys and a four-ticket window, churn regresses from 292 to 444
ns/op (+52.05%; MAD 1 and 2 ns). The actual public facade at default admission
also regresses for same-key delivery from 3886 to 4238 ns/op (+9.06%; MAD 73 and
21 ns). This rejects this candidate, rather than establishing that every possible
cleanup optimization must regress.

## Decision conditions

The implementation design requires all three conditions: registry retain scans
use at least 10% of enqueue-path CPU; high-key median elapsed cost improves at
least 15%; and the default four-key case regresses by no more than 5%.
The elapsed comparisons below come from uninstrumented stable release builds.
The separate CPU profile is used for CPU attribution, never as an elapsed-time
comparison or a wall-clock surrogate. No production ordering files were changed.

## CPU profile and gate result

The retain CPU condition **passes**, the high-key elapsed improvement condition
**passes**, and the default-regression condition **fails**. The three conditions
are conjunctive, so the candidate is rejected.

A matching gprof histogram and debug-bearing optimized binary attribute real
CPU samples to the original source; no production instrumentation or copied
registry state machine is involved. Only the following unmistakable retain
locations are counted, excluding other likely scan-loop/atomic samples:

| CPU sample location | Samples / 284 total | Fraction of all sampled CPU |
|---|---:|---:|
| hashbrown `HashMap::retain`, map.rs:904 | 99 | 34.86% |
| original retain predicate, async_ordering_lanes.rs:55 | 99 | 34.86% |
| hashbrown `HashMap::retain`, map.rs:905 | 25 | 8.80% |
| Conservative identified scan total | 223 | 78.52% |

The complete enqueue function accounts for 97.54% of sampled CPU (277 of 284
samples). Identified scan samples therefore account for approximately 80.50% of
that self CPU; using the larger whole-process denominator still gives a
conservative 78.52% lower bound, well above the required 10%. These percentages
are sampled CPU work, not mcount call proportions or measured wall time.
The 266,240 recorded enqueue calls equal 262,144 measured operations plus 4,096
initial lane creations. The asserted live-key count implies 1,073,741,824 retain
predicates during the steady workload, plus 8,386,560 during initial creation.
No individual scan predicate counter was added to production.

The fresh profiling process exited 0 in 2.980107 wall seconds,
using 2.979420 CPU seconds, with 15
involuntary switches. Other authors had resumed tools, and external processes
were allowed to continue; this was not an exclusive machine window. The CPU
profile is one separate attribution run with 284 CPU histogram samples,
not a substitute for the seven elapsed samples in every matrix configuration.
Profiling retains normal optimization/inlining and explicitly disables Cargo's
release debug stripping so gprof can resolve inline source locations.

```sh
cargo +nightly-2026-06-05 rustc --locked --release --bench ordering_keys \
  --manifest-path /tmp/ordering-t8-baseline/Cargo.toml \
  --target-dir /tmp/ordering-t8-profile -- \
  -Zinstrument-mcount=yes -Cdebuginfo=2 -Cstrip=none \
  -Cforce-frame-pointers=yes -Clink-arg=-pg
ORDERING_PROFILE=1 ORDERING_PROFILE_OPERATIONS=262144 taskset -c 4 \
  /tmp/ordering-t8-profile/release/deps/ordering_keys-0e8857df5b94497b
gprof /tmp/ordering-t8-profile/release/deps/ordering_keys-0e8857df5b94497b gmon.out -p -q
gprof -l /tmp/ordering-t8-profile/release/deps/ordering_keys-0e8857df5b94497b gmon.out -p
```

Profile binary SHA-256: `68ce9e69e9d8c8989a9046da9c8138978ee9180b6a7266ccda793bfc2c201603`.
Matching gmon SHA-256: `5cdb6c104a0c4e193ad2e13bfa2bca8fb0a1225e80ec84f1787c57e4f327d6c6`.
Full flat/callgraph and inline-line reports: `baseline-gprof.txt` and
`baseline-gprof-lines.txt`, with process metadata in `profile-run.json`.
The earlier stripped binary's profile established enqueue CPU concentration but
could not establish scan attribution; it is preserved separately as
`initial-stripped-gmon.out` / `initial-stripped-gprof.txt` and is excluded from final scan attribution. Only the fresh matching
histogram and symbol file listed above support the scan gate. `perf` was denied by
`perf_event_paranoid=4`; the validated mcount/gprof route required no sysctl change.


## Machine and builds

- Intel Core i5-9600K at nominal 3.70 GHz, six physical cores, no SMT; 9 MiB L3.
- x86_64 Linux 7.0.0-34-generic; CPU governor `powersave`, dynamic turbo enabled.
- Performance builds: rustc 1.94.0 (`4a4ef493e`, 2026-03-02), Cargo 1.94.0;
  Cargo bench/release defaults, optimized level 3, no LTO, default codegen units,
  locked dependencies. No RUSTFLAGS or Cargo profile overrides were set.
- CPU profiling build: rustc 1.98.0-nightly (`e7815e522`, 2026-06-04), toolchain
  `nightly-2026-06-05`; release optimization with mcount instrumentation, debug
  information and forced frame pointers. GNU gprof 2.42.
- Both comparison binaries were pinned to CPU 4. The team paused heavy tools,
  but unrelated builds and desktop activity continued. This was not machine-wide
  isolation. Each timed sample records wall time, thread CPU and process CPU.
  Registry stress accepts thread CPU / wall >=98%; the real facade accepts
  process CPU / wall >=98% so legitimate process-owned timer work is included.
  There is no intended timer, network or condition-variable wait in these timed
  fixtures. All 672 accepted samples satisfy that rule (minimum ratio
  0.980390). 748 rejected scheduling-contaminated attempts are retained.
  This screening does not remove shared-cache, frequency or thermal variation;
  the raw samples and MAD expose the remaining dispersion.

Baseline and candidate were built from frozen source copies of crate version
0.16.0 during the boundary migration, with identical non-ordering source,
benchmark input and lockfile. The final target also typechecks against the
integrated 0.17.0 API; these measurements concern ordering, not release-wide
end-to-end performance. Only `async_lane.rs` and
`async_ordering_lanes.rs` differed. Comparison source copies and binaries are
under `/tmp/ordering-t8-baseline`, `/tmp/ordering-t8-candidate`,
`/tmp/ordering-t8-target` and `/tmp/ordering-t8-candidate-target`. Raw evidence is
under `/tmp/ordering-t8-evidence`. These temporary paths identify session provenance.
The exact original sample/rejection text, parsed samples with MAD association,
profile outputs, source/binary SHA manifests and complete candidate patch are
permanently stored in [ordering_performance_artifacts](ordering_performance_artifacts/README.md).
That artifact set and the tables below preserve this decision independently of `/tmp`.

Frozen baseline SHA-256 values:

- `benches/ordering_keys.rs`: `2997d68798675de5e3701ed92e414483e22a43808e78171e01872d223d39bd96`
- `src/pipeline/ordering_lane/internal/async_lane.rs`: `e346cc1bd9dd4b736f8cc6bff623daddcd26ba6e3115d7efd0b9183d0525b4d3`
- `src/pipeline/ordering_lane/internal/async_ordering_lanes.rs`: `3d0aa6de9992b09d37d004568e2821d44e34125284443d05cf8a5c5bdd926b12`

The final benchmark makes a metadata-only naming correction: the frozen facade
log label `input_keys` was a case-size parameter. The final target prints
`case_keys` and `distinct_input_keys` explicitly. Measured operations and timing
logic are unchanged. Its Linux CPU-clock bindings are also gated so other hosts
can compile the target; CPU-validated execution requires 64-bit Linux.
The final target binds the six production async ordering source files directly
and reexports only the three types it uses, instead of importing the internal
module root and its unrelated test-only synchronous reexports. This binding-only
change removes an unused-import suppression; all workload functions and the
frozen measured source snapshots remain unchanged.
A final Clippy correction passes the formatted ordering key by value rather than
borrowing it; the builder stores the same key and request preparation remains
outside the timed region. The final benchmark source SHA-256 is
`abc6714a926005e5c6bede00dfc5c69ff640892cc2151d62c4d916485fb28da8`; the measured baseline SHA above and all measurement artifacts
retain their original bytes. No performance sample was rerun for these changes.

## Workload boundaries

Registry stress imports the **actual production internal module** using `#[path]`,
without expanding library visibility or copying its state machine. It keeps
exactly 4, 64, 1024 or 4096 guards alive, then queues up to 4 or 64 new
tickets. Balanced batches contain min(live keys, window) distinct keys;
same-key batches fill the whole window. Balanced work rotates distinct keys; same-key work really polls pending
tickets before FIFO handoff; churn drops and rebuilds a lane with fresh prebuilt
keys. The guard count is asserted after every sample. Each sample completes
32,768 operations. All keys, payload values, vectors and initial lanes are
prepared before timing. Future allocation, enqueue, poll, guard handoff and
release remain part of the measured operation. Wait timestamps introduce the
same observation overhead in both versions. Results and completion counts are
passed through `black_box`.

Retained guards are deliberately outside the new-ticket window in registry
stress. This is a high-cardinality internal stress boundary, **not** a claim
that default facade admission 4 permits 4096 simultaneous lanes. The separate
public-facade matrix applies real admission 4 and max(case keys, 64), and observes
actual gated first-wave handlers. Balanced elevated cases observe 4, 64, 1024
and 4096 simultaneous distinct-key handlers; default admission observes at most
four. Same-key cases contain one distinct key and observe one handler. Churn
contains one new input key per delivery, and observes the configured initial
window. Public-facade samples complete 2 * max(case keys, 1024) deliveries.

Publish requests are built and all publications complete before timing. The
runner is initially polled with blocked handlers, then the timed interval
releases the gate and drains deliveries. Same-key initial queue depth is fixed
at 64; remaining messages are published before timing without polling the gated
runner. This avoids a cubic, unmeasured setup caused by repeatedly polling
thousands of pending same-key tickets, while retaining real same-key queuing,
the complete message count and every admission configuration.

Every side/configuration has two warmups and seven accepted independent samples.
The wrapper alternates baseline/candidate order between adjacent cases; each
case compares the same parameters close together. Seven samples support this
decision, not a tail-latency service-level claim. Registry wait p95 is computed
within each sample; facade reports drain cost and first-wave concurrency instead
of pretending to measure per-message waiting time.

The original full-matrix pilot was discarded because only whole-process CPU
time had been recorded, including setup, which could not identify timed-region
preemption. Its incomplete final same-key setup was terminated. None of those
pilot values enter the tables or the decision.

## Registry results

Candidate change = (candidate median / baseline median - 1) * 100%; negative
means faster. MAD is the median absolute deviation of the seven elapsed
ns/op samples. Mops/s = 1000 / median ns/op.

| Case keys | Window / admission | Workload | Baseline seven ns/op samples | Candidate seven ns/op samples | Median B / C | MAD B / C | Candidate change | Mops/s B / C | Rejected B / C |
|---:|---:|---|---|---|---:|---:|---:|---:|---:|
| 4 | 4 | balanced | 244, 247, 245, 246, 244, 254, 248 | 239, 239, 239, 244, 242, 238, 241 | 246 / 239 | 2 / 1 | -2.85% | 4.0650 / 4.1841 | 12 / 4 |
| 4 | 4 | same-key | 245, 254, 262, 261, 265, 256, 255 | 239, 248, 253, 251, 255, 249, 246 | 256 / 249 | 5 / 3 | -2.73% | 3.9062 / 4.0161 | 4 / 38 |
| 4 | 4 | churn | 285, 291, 293, 292, 291, 293, 293 | 447, 445, 438, 444, 439, 446, 443 | 292 / 444 | 1 / 2 | +52.05% | 3.4247 / 2.2523 | 7 / 7 |
| 4 | 64 | balanced | 249, 258, 256, 253, 254, 253, 252 | 245, 243, 242, 244, 243, 236, 241 | 253 / 243 | 1 / 1 | -3.95% | 3.9526 / 4.1152 | 4 / 6 |
| 4 | 64 | same-key | 257, 249, 259, 260, 250, 253, 261 | 253, 253, 253, 251, 254, 250, 255 | 257 / 253 | 4 / 1 | -1.56% | 3.8911 / 3.9526 | 2 / 11 |
| 4 | 64 | churn | 299, 296, 287, 297, 295, 298, 301 | 450, 451, 446, 440, 451, 444, 441 | 297 / 446 | 2 / 5 | +50.17% | 3.3670 / 2.2422 | 16 / 8 |
| 64 | 4 | balanced | 318, 327, 325, 322, 320, 323, 319 | 259, 244, 236, 236, 239, 259, 257 | 322 / 244 | 3 / 8 | -24.22% | 3.1056 / 4.0984 | 39 / 47 |
| 64 | 4 | same-key | 317, 314, 317, 317, 320, 322, 322 | 245, 242, 240, 243, 245, 252, 244 | 317 / 244 | 3 / 1 | -23.03% | 3.1546 / 4.0984 | 22 / 32 |
| 64 | 4 | churn | 460, 461, 465, 468, 467, 461, 471 | 473, 465, 454, 472, 467, 461, 472 | 465 / 467 | 4 / 5 | +0.43% | 2.1505 / 2.1413 | 20 / 6 |
| 64 | 64 | balanced | 306, 319, 315, 318, 314, 312, 317 | 246, 249, 251, 250, 249, 248, 252 | 315 / 249 | 3 / 1 | -20.95% | 3.1746 / 4.0161 | 2 / 7 |
| 64 | 64 | same-key | 310, 321, 310, 305, 319, 316, 312 | 250, 249, 250, 260, 252, 247, 244 | 312 / 250 | 4 / 2 | -19.87% | 3.2051 / 4.0000 | 12 / 5 |
| 64 | 64 | churn | 458, 452, 457, 454, 463, 461, 450 | 472, 466, 469, 476, 479, 465, 473 | 457 / 472 | 4 / 4 | +3.28% | 2.1882 / 2.1186 | 25 / 8 |
| 1024 | 4 | balanced | 2228, 2040, 2283, 2264, 2366, 2247, 2141 | 239, 241, 246, 251, 240, 243, 239 | 2247 / 241 | 36 / 2 | -89.27% | 0.4450 / 4.1494 | 0 / 0 |
| 1024 | 4 | same-key | 2249, 2254, 2194, 2245, 2133, 2399, 2554 | 230, 234, 225, 237, 239, 243, 248 | 2249 / 237 | 55 / 6 | -89.46% | 0.4446 / 4.2194 | 2 / 0 |
| 1024 | 4 | churn | 3104, 2974, 3128, 3101, 3038, 3052, 3161 | 529, 518, 528, 493, 519, 519, 518 | 3101 / 519 | 49 / 1 | -83.26% | 0.3225 / 1.9268 | 67 / 4 |
| 1024 | 64 | balanced | 2668, 2624, 2513, 2532, 2645, 2612, 2661 | 268, 276, 278, 275, 278, 271, 267 | 2624 / 275 | 37 / 3 | -89.52% | 0.3811 / 3.6364 | 27 / 10 |
| 1024 | 64 | same-key | 2669, 2583, 2609, 2567, 2504, 2465, 2524 | 238, 244, 233, 233, 235, 237, 237 | 2567 / 237 | 43 / 2 | -90.77% | 0.3896 / 4.2194 | 2 / 0 |
| 1024 | 64 | churn | 2571, 2573, 2549, 2544, 2606, 2777, 2611 | 465, 446, 460, 459, 480, 464, 446 | 2573 / 460 | 29 / 5 | -82.12% | 0.3887 / 2.1739 | 0 / 0 |
| 4096 | 4 | balanced | 10830, 10484, 10806, 10911, 10714, 10572, 10551 | 247, 252, 257, 256, 252, 243, 247 | 10714 / 252 | 142 / 5 | -97.65% | 0.0933 / 3.9683 | 0 / 0 |
| 4096 | 4 | same-key | 10375, 10853, 11148, 10814, 10896, 10936, 11367 | 218, 222, 222, 221, 223, 218, 219 | 10896 / 221 | 82 / 2 | -97.97% | 0.0918 / 4.5249 | 8 / 0 |
| 4096 | 4 | churn | 11091, 10749, 10721, 10827, 10797, 10640, 10760 | 530, 530, 535, 534, 538, 539, 553 | 10760 / 535 | 39 / 4 | -95.03% | 0.0929 / 1.8692 | 0 / 0 |
| 4096 | 64 | balanced | 10911, 10662, 10551, 10726, 10933, 10688, 10545 | 299, 305, 319, 320, 308, 323, 311 | 10688 / 311 | 137 / 8 | -97.09% | 0.0936 / 3.2154 | 1 / 1 |
| 4096 | 64 | same-key | 10829, 10553, 10696, 10918, 10812, 10713, 10735 | 232, 235, 232, 231, 232, 237, 236 | 10735 / 232 | 77 / 1 | -97.84% | 0.0932 / 4.3103 | 0 / 1 |
| 4096 | 64 | churn | 10725, 10869, 11089, 11103, 11266, 10912, 10831 | 488, 496, 488, 492, 488, 479, 470 | 10912 / 488 | 177 / 4 | -95.53% | 0.0916 / 2.0492 | 0 / 0 |

The baseline retain predicate visits every registry entry on every enqueue.
With the asserted live guard count, the measured 32,768 enqueues therefore imply
131,072 / 2,097,152 / 33,554,432 / 134,217,728 Weak predicates per sample for
4 / 64 / 1024 / 4096 live keys, respectively, excluding setup. Churn additionally
removes one expired entry and inserts its replacement per measured operation.
These are code-derived deterministic operation counts, not hardware counters.
The candidate performs no full-table enqueue cleanup and does one identity-
checked registry deletion at final lane release; steady balanced/same-key
handoffs keep the original allocation alive, while churn pays deletion plus
new-lane bookkeeping and key ownership. That tradeoff is visible in default
churn's regression.

## Public facade results

Case keys denotes the matrix parameter. Actual distinct input keys are that
number for balanced, one for same-key, and 2 * max(case keys, 1024) for churn.
Every accepted sample's gated handler count matches the asserted first-wave
cardinality described above.

| Case keys | Window / admission | Workload | Baseline seven ns/op samples | Candidate seven ns/op samples | Median B / C | MAD B / C | Candidate change | Mops/s B / C | Rejected B / C |
|---:|---:|---|---|---|---:|---:|---:|---:|---:|
| 4 | 4 | balanced | 4242, 4112, 4049, 4271, 4345, 4256, 4192 | 4190, 4270, 4396, 4358, 4210, 4216, 4259 | 4242 / 4259 | 50 / 49 | +0.40% | 0.2357 / 0.2348 | 11 / 7 |
| 4 | 4 | same-key | 3886, 3871, 3802, 4091, 3959, 3927, 3801 | 4183, 4204, 4238, 4246, 4247, 4210, 4259 | 3886 / 4238 | 73 / 21 | +9.06% | 0.2573 / 0.2360 | 7 / 3 |
| 4 | 4 | churn | 4030, 4006, 4066, 4451, 4302, 4308, 4276 | 4529, 4552, 4730, 4645, 4401, 4433, 4495 | 4276 / 4529 | 175 / 96 | +5.92% | 0.2339 / 0.2208 | 9 / 34 |
| 4 | 64 | balanced | 4063, 4009, 3879, 4200, 4196, 4219, 4278 | 4342, 4290, 4322, 4361, 4139, 4203, 4286 | 4196 / 4290 | 82 / 52 | +2.24% | 0.2383 / 0.2331 | 31 / 18 |
| 4 | 64 | same-key | 4093, 3982, 4089, 3930, 4156, 4177, 4001 | 4537, 4526, 4416, 4324, 4311, 4391, 4386 | 4089 / 4391 | 88 / 67 | +7.39% | 0.2446 / 0.2277 | 39 / 10 |
| 4 | 64 | churn | 4233, 4397, 4346, 4302, 4415, 4407, 4490 | 4329, 4387, 4556, 4568, 4628, 4639, 4481 | 4397 / 4556 | 51 / 75 | +3.62% | 0.2274 / 0.2195 | 2 / 10 |
| 64 | 4 | balanced | 3799, 4191, 4112, 4075, 3892, 4073, 4031 | 4222, 4347, 4281, 4262, 4328, 4248, 4186 | 4073 / 4262 | 42 / 40 | +4.64% | 0.2455 / 0.2346 | 21 / 13 |
| 64 | 4 | same-key | 3871, 3912, 3932, 3761, 3858, 3696, 3876 | 4094, 3954, 4059, 4033, 4053, 4056, 4065 | 3871 / 4056 | 41 / 9 | +4.78% | 0.2583 / 0.2465 | 12 / 8 |
| 64 | 4 | churn | 3995, 4104, 4249, 4134, 4207, 4165, 4256 | 4318, 4509, 4437, 4591, 4441, 4374, 4401 | 4165 / 4437 | 61 / 63 | +6.53% | 0.2401 / 0.2254 | 5 / 1 |
| 64 | 64 | balanced | 3983, 3929, 4013, 3867, 3806, 3727, 3671 | 4373, 4421, 4463, 4476, 4445, 4320, 4329 | 3867 / 4421 | 116 / 48 | +14.33% | 0.2586 / 0.2262 | 4 / 6 |
| 64 | 64 | same-key | 3609, 3609, 3596, 3591, 3587, 3799, 3474 | 3668, 3746, 3864, 3691, 3916, 3711, 3809 | 3596 / 3746 | 13 / 63 | +4.17% | 0.2781 / 0.2670 | 1 / 0 |
| 64 | 64 | churn | 3546, 3508, 3574, 3716, 3978, 3621, 3590 | 4145, 3903, 3835, 3970, 3846, 3875, 3859 | 3590 / 3875 | 44 / 29 | +7.94% | 0.2786 / 0.2581 | 1 / 0 |
| 1024 | 4 | balanced | 3934, 3930, 4064, 4038, 4040, 4226, 4126 | 4289, 4120, 4154, 4247, 4289, 4234, 4321 | 4040 / 4247 | 86 / 42 | +5.12% | 0.2475 / 0.2355 | 0 / 0 |
| 1024 | 4 | same-key | 3624, 3437, 3459, 3631, 3561, 3474, 3592 | 3847, 3613, 3669, 3626, 3739, 3673, 3767 | 3561 / 3673 | 70 / 60 | +3.15% | 0.2808 / 0.2723 | 0 / 0 |
| 1024 | 4 | churn | 3635, 3708, 3622, 3657, 3628, 3516, 3620 | 3886, 3767, 3726, 3659, 3708, 3659, 3718 | 3628 / 3718 | 8 / 49 | +2.48% | 0.2756 / 0.2690 | 0 / 0 |
| 1024 | 1024 | balanced | 3846, 3835, 4155, 4143, 3869, 3711, 3937 | 4231, 4358, 4122, 4265, 4399, 4268, 4325 | 3869 / 4268 | 68 / 57 | +10.31% | 0.2585 / 0.2343 | 3 / 11 |
| 1024 | 1024 | same-key | 4059, 3972, 4088, 4085, 4136, 4081, 4057 | 4639, 4571, 4623, 4476, 4598, 4642, 4617 | 4081 / 4617 | 22 / 22 | +13.13% | 0.2450 / 0.2166 | 0 / 5 |
| 1024 | 1024 | churn | 3508, 3669, 4005, 3613, 3844, 4098, 4090 | 3879, 3932, 3820, 3635, 3902, 3817, 3990 | 3844 / 3879 | 231 / 59 | +0.91% | 0.2601 / 0.2578 | 1 / 3 |
| 4096 | 4 | balanced | 3686, 3674, 3742, 3681, 3666, 3613, 3620 | 3923, 4173, 4172, 4058, 4159, 4171, 4252 | 3674 / 4171 | 12 / 12 | +13.53% | 0.2722 / 0.2398 | 0 / 1 |
| 4096 | 4 | same-key | 3329, 3378, 3367, 3540, 3395, 3466, 3637 | 3733, 3971, 3697, 3648, 3715, 3698, 3698 | 3395 / 3698 | 66 / 17 | +8.92% | 0.2946 / 0.2704 | 0 / 0 |
| 4096 | 4 | churn | 3767, 3715, 3900, 3912, 3833, 3963, 3868 | 3974, 3886, 3897, 3998, 4116, 4176, 4155 | 3868 / 3998 | 44 / 112 | +3.36% | 0.2585 / 0.2501 | 0 / 0 |
| 4096 | 4096 | balanced | 3493, 3589, 3624, 3333, 3855, 3536, 3597 | 3851, 3817, 3574, 4390, 4097, 3657, 3687 | 3589 / 3817 | 53 / 160 | +6.35% | 0.2786 / 0.2620 | 1 / 0 |
| 4096 | 4096 | same-key | 3598, 3601, 3554, 3526, 3488, 3484, 3737 | 3628, 3632, 3684, 3696, 3665, 3618, 3648 | 3554 / 3648 | 47 / 20 | +2.64% | 0.2814 / 0.2741 | 0 / 0 |
| 4096 | 4096 | churn | 4070, 3703, 3439, 3451, 4136, 3466, 3436 | 3617, 3789, 3638, 4112, 3631, 3706, 3665 | 3466 / 3665 | 30 / 41 | +5.74% | 0.2885 / 0.2729 | 1 / 2 |

## Registry wait observations

Each table entry is the median across seven sample-level queue-wait medians and
sample-level p95 values. Balanced and same-key intervals run from just before
enqueue to ready guard acquisition; churn intervals run from release/rebuild
start to acquisition. These local handoff intervals include deliberate gating
and observation overhead; they are not application end-to-end latency.

| Live registry keys | Ticket window | Workload | Baseline median wait / p95 (ns) | Candidate median wait / p95 (ns) |
|---:|---:|---|---:|---:|
| 4 | 4 | balanced | 568 / 708 | 556 / 697 |
| 4 | 4 | same-key | 596 / 736 | 573 / 723 |
| 4 | 4 | churn | 255 / 331 | 397 / 492 |
| 4 | 64 | balanced | 600 / 736 | 575 / 706 |
| 4 | 64 | same-key | 8451 / 10435 | 8262 / 10141 |
| 4 | 64 | churn | 257 / 339 | 396 / 509 |
| 64 | 4 | balanced | 763 / 1015 | 565 / 717 |
| 64 | 4 | same-key | 747 / 1006 | 559 / 704 |
| 64 | 4 | churn | 411 / 524 | 409 / 534 |
| 64 | 64 | balanced | 10166 / 13783 | 7998 / 9996 |
| 64 | 64 | same-key | 10124 / 13866 | 8208 / 10114 |
| 64 | 64 | churn | 411 / 509 | 417 / 545 |
| 1024 | 4 | balanced | 6114 / 8707 | 559 / 690 |
| 1024 | 4 | same-key | 6288 / 8787 | 535 / 624 |
| 1024 | 4 | churn | 2921 / 3623 | 450 / 682 |
| 1024 | 64 | balanced | 84660 / 154849 | 8644 / 11459 |
| 1024 | 64 | same-key | 83482 / 152302 | 7798 / 9463 |
| 1024 | 64 | churn | 2555 / 2808 | 402 / 545 |
| 4096 | 4 | balanced | 30680 / 42752 | 580 / 786 |
| 4096 | 4 | same-key | 30982 / 42907 | 531 / 605 |
| 4096 | 4 | churn | 10530 / 10973 | 461 / 730 |
| 4096 | 64 | balanced | 346559 / 648788 | 9961 / 14262 |
| 4096 | 64 | same-key | 349272 / 651563 | 7690 / 9397 |
| 4096 | 64 | churn | 10669 / 11102 | 433 / 619 |

## Reproduction and checks

The public benchmark target is dependency-free beyond existing dependencies:

```sh
cargo bench --locked --bench ordering_keys
ORDERING_SMOKE=1 cargo bench --locked --bench ordering_keys
```

`ORDERING_FILTER` selects one exact output case label for paired execution;
`ORDERING_PROFILE=1` selects the real high-key registry source workload and
`ORDERING_PROFILE_OPERATIONS` controls its operation count. A full run without
filter covers every configuration. CPU-clock validity filtering lives entirely
in the standalone benchmark, never in production code or ordinary unit tests.

The measured comparison commands were `cargo bench --locked --bench
ordering_keys --no-run` for both frozen manifests followed by the same benchmark
executables through `taskset -c 4`; the exact pairing wrapper and full CPU/wall
records are in `run_pairs.py`, `paired-samples.log` and `paired-results.json`.
Both versions passed workload-invariant smoke runs. The temporary candidate
first failed the immediate-registry-reclaim regression against baseline
(one stale entry rather than zero), then passed after implementation. Its
allocation-identity old-lane Drop/recreated-key regression also passed (2 tests,
0 failures). These experiments did not add an assumed software defect or a
timing assertion to production tests.

Formatting used `align-ci.sh --dry-run` to confirm the project's formatter and
then the authorized one-file rustfmt invocation with `nightly-2026-06-05`,
`.infra/style/rustfmt.toml` and `skip_children=true`, avoiding concurrent edits
to other authors' files. Full repository style/CI is the integration task's
responsibility; this performance decision does not claim those checks passed.

Final one-file rustfmt check passed, and `cargo check --locked --bench
ordering_keys` against the integrated 0.17.0 crate exited 0 without warnings.
The cached `cargo bench --locked --bench ordering_keys` entrypoint also exited
0 with a filtered complete seven-sample case; the 48-case decision dataset
remains the separately frozen paired matrix.

## Rejected experimental candidate

The following patch is retained as experiment evidence and was **not applied
to production**. Final lane Drop runs after all strong owners and lane-state
locks release, acquires the registry lock, and deletes only a matching data
allocation. Enqueue upgrades and replacement remain under the registry lock.

```diff
--- a/src/pipeline/ordering_lane/internal/async_lane.rs
+++ b/src/pipeline/ordering_lane/internal/async_lane.rs
@@ -8,8 +8,11 @@
 //! Mutable state and synchronization for one asynchronous ordering lane.
 
 use std::collections::VecDeque;
+use std::collections::HashMap;
 use std::sync::Mutex;
+use std::sync::Weak;
 
+use super::OrderingLaneKey;
 use super::async_lane_state::AsyncLaneState;
 
 /// One asynchronous FIFO lane and its waiter state.
@@ -18,15 +21,34 @@
 pub(super) struct AsyncLane<T> {
     /// Active turn and queued values with their registered wakers.
     pub(super) state: Mutex<AsyncLaneState<T>>,
+    /// Registry identity used only after the last strong lane owner releases.
+    registry: Weak<Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>>,
+    /// Stable key paired with this allocation.
+    key: OrderingLaneKey,
 }
-impl<T> Default for AsyncLane<T> {
+impl<T> AsyncLane<T> {
     /// Creates an idle lane with an empty FIFO and no registered waiters.
-    fn default() -> Self {
+    pub(super) fn new(key: OrderingLaneKey, registry: Weak<Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>>) -> Self {
         Self {
             state: Mutex::new(AsyncLaneState {
                 active: false,
                 queue: VecDeque::new(),
             }),
+            registry,
+            key,
         }
     }
 }
+
+impl<T> Drop for AsyncLane<T> {
+    /// Removes only this allocation, after all lane-state locks are released.
+    fn drop(&mut self) {
+        if let Some(registry) = self.registry.upgrade() {
+            let mut lanes = registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
+            let allocation = self as *const Self;
+            if lanes.get(&self.key).is_some_and(|lane| lane.as_ptr() == allocation) {
+                lanes.remove(&self.key);
+            }
+        }
+    }
+}
--- a/src/pipeline/ordering_lane/internal/async_ordering_lanes.rs
+++ b/src/pipeline/ordering_lane/internal/async_ordering_lanes.rs
@@ -25,7 +25,7 @@
 #[must_use]
 pub(crate) struct AsyncOrderingLanes<T> {
     /// Weak lane registry so inactive ordering keys can be reclaimed.
-    lanes: Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>,
+    lanes: Arc<Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>>,
     /// Generates FIFO ticket identities.
     next_ticket: AtomicU64,
 }
@@ -36,7 +36,7 @@
     /// A collection with no live keys and ticket numbering starting at one.
     pub(crate) fn new() -> Self {
         Self {
-            lanes: Mutex::new(HashMap::new()),
+            lanes: Arc::new(Mutex::new(HashMap::new())),
             next_ticket: AtomicU64::new(1),
         }
     }
@@ -52,9 +52,8 @@
     pub(crate) fn enqueue(&self, key: OrderingLaneKey, value: T) -> AsyncOrderingTurn<T> {
         let lane = {
             let mut lanes = self.lanes.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
-            lanes.retain(|_, lane| lane.strong_count() != 0);
             lanes.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
-                let lane = Arc::new(AsyncLane::default());
+                let lane = Arc::new(AsyncLane::new(key.clone(), Arc::downgrade(&self.lanes)));
                 lanes.insert(key, Arc::downgrade(&lane));
                 lane
             })
@@ -78,3 +77,66 @@
         Self::new()
     }
 }
+
+#[cfg(test)]
+mod tests {
+    use std::future::Future;
+    use std::task::Context;
+    use std::task::Poll;
+    use std::task::Waker;
+    use std::sync::Arc;
+
+    use qubit_id::Id;
+
+    use super::AsyncOrderingLanes;
+    use super::OrderingLaneKey;
+    use super::AsyncLane;
+
+    /// Verifies final release reclaims the entry without a later enqueue scan.
+    #[test]
+    fn test_final_guard_release_reclaims_registry_entry() {
+        let lanes = AsyncOrderingLanes::new();
+        let key = OrderingLaneKey::new("bench", Some("same-key"), Id::new(1));
+        let guard = match Box::pin(lanes.enqueue(key, ()))
+            .as_mut()
+            .poll(&mut Context::from_waker(Waker::noop()))
+        {
+            Poll::Ready(Some(guard)) => guard,
+            _ => panic!("first turn must acquire its lane"),
+        };
+        assert_eq!(lanes.lanes.lock().unwrap().len(), 1);
+        drop(guard);
+        assert_eq!(lanes.lanes.lock().unwrap().len(), 0);
+    }
+
+    /// Forces final Drop to wait while the same key receives a new allocation.
+    #[test]
+    fn test_old_lane_drop_preserves_recreated_key_allocation() {
+        let lanes = AsyncOrderingLanes::new();
+        let key = OrderingLaneKey::new("bench", Some("same-key"), Id::new(1));
+        let guard = match Box::pin(lanes.enqueue(key.clone(), ()))
+            .as_mut()
+            .poll(&mut Context::from_waker(Waker::noop()))
+        {
+            Poll::Ready(Some(guard)) => guard,
+            _ => panic!("first turn must acquire its lane"),
+        };
+        let mut registry = lanes.lanes.lock().unwrap();
+        let old_weak = registry.get(&key).unwrap().clone();
+        let old = old_weak.upgrade().unwrap();
+        drop(guard);
+        let dropping = std::thread::spawn(move || drop(old));
+        while old_weak.strong_count() != 0 {
+            std::thread::yield_now();
+        }
+        let replacement = Arc::new(AsyncLane::new(key.clone(), Arc::downgrade(&lanes.lanes)));
+        registry.insert(key.clone(), Arc::downgrade(&replacement));
+        drop(registry);
+        dropping.join().unwrap();
+        let registry = lanes.lanes.lock().unwrap();
+        assert_eq!(registry.get(&key).unwrap().as_ptr(), Arc::as_ptr(&replacement));
+        drop(registry);
+        drop(replacement);
+        assert!(lanes.lanes.lock().unwrap().is_empty());
+    }
+}
```
