# Async receive waiter A/B

The baseline and candidate use the same public SPI harness, payload, operation counts, warmups, sample counts, release profile, lockfile, and host. Message construction and subscription setup occur outside timed regions. The only measured production change is reuse/update of a local async receive's waiter registration.

Seven baseline/candidate pairs ran serially from the saved baseline and candidate release executables. Each process emitted two warmups and seven samples for each of the three scenarios. The final decision set is in `paired-repeat/`; the earlier clean set is in `paired-initial/`. `paired-noisy-first/` preserves a rerun taken during a host load spike and is excluded from the decision. The earlier `preliminary-includes-message-construction/` set is also retained but excluded because that driver timed message construction. The Cargo benchmark outputs and source snapshots are in `baseline/` and `candidate/`. Process exit codes are recorded in every stderr file. `run_interleaved.sh` reruns the archived binaries and writes all raw output.

| Scenario | Baseline median ns / 20k ops | Baseline MAD | Candidate median ns / 20k ops | Candidate MAD | Change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Same waker repeated pending poll | 4,017,274 | 17,207 | 2,293,822 | 13,905 | -42.90% |
| Alternating wakers repeated pending poll | 4,008,623 | 14,704 | 3,272,959 | 9,199 | -18.35% |
| Publish, receive, settle | 43,625,243 | 312,647 | 43,237,192 | 349,827 | -0.89% |

Medians and MAD use all 49 samples per variant/scenario from `paired-repeat/`. The repeated-poll improvements exceed 5% and the larger of the two MADs. The end-to-end scenario did not regress. Its 0.89% median change is within measurement dispersion and is not claimed as an improvement. Waker callback totals were zero in the pending-only scenarios and 180,000 per publish/receive/settle run, matching the 20,000 operations across nine warmup and sample iterations.

The noisy set showed high dispersion and an apparent end-to-end regression while host load was elevated; repeating with identical parameters returned stable distributions and no regression. The repeated set is the decision evidence. This supports merging the narrowly scoped waiter reuse change under this harness; it does not establish general application throughput or promise the same percentage on other machines.
