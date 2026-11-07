# Rejection diagnostics benchmark

## Scope and method

This is the EB-R01 T1 optimization-before measurement. Production code remains unchanged. The benchmark uses fixed synchronous and asynchronous SPI acknowledgements with `PublishVisibility::DestinationAdmissions`; it measures facade publication and receipt validation, excluding fixture construction and cleanup. It covers 1, 16, and 128 destinations, all-rejected and alternating accepted/rejected outcomes (the one-destination partial case is omitted), zero and one observer, sync and async. Each case checks admission outcome and observer call count. Each sample contains 1,000 publishes after two warmups; seven raw samples are emitted for every case.

## Environment and execution

- Worktree: `/data/working/qubit/rust-common/.worktrees/rs-event-bus/codex-eb-r01-r02-20261106`
- Source revision: `39bee718ed6396a6d8b0e633a854958832ecac89` (production code unchanged)
- OS / architecture: Linux 6.8.0-147-generic, x86_64 (`x86_64-unknown-linux-gnu`)
- CPU: Intel(R) Xeon(R) CPU E5-2680 v4 @ 2.40GHz; benchmark processes were not pinned to a CPU.
- Rust toolchain: `rustc 1.94.0 (4a4ef493e 2026-03-02)`, LLVM 21.1.8
- Fixtures inspected: discovery provider and documentation consumer both use edition 2024; discovery fixture enables `discovery` and `qubit-spi/inventory`; documentation fixture uses `tokio` 1.53. Discovery fixture lockfile is format version 4.
- Cargo version: `cargo 1.94.0 (85eff7c80 2026-01-15)`
- Command: `cargo bench --locked --bench rejection_diagnostics`
- First attempt: `cargo bench --locked --bench rejection_diagnostics` failed with exit code 101 during compilation. The compiler reported that `SpiError` must be imported from the crate root and that `PublishRequest<u64>` is not `Clone` (the benchmark borrowed and cloned a request).
- Correction: import `SpiError` from `qubit_event_bus`; preconstruct one independently owned request batch per warmup/sample and consume each batch during timing. Request construction remains outside the measured region.
- Initial corrected run: completed with exit code 0, but emitted two `unused_must_use` warnings for benchmark receipt outcome handling. Both were corrected to `let _ = black_box(...)`, and the benchmark was rerun afterward.
- Warning-clean run: `cargo bench --locked --bench rejection_diagnostics` exited 0 with no warnings and all 20 cases. The bench subscription stubs were subsequently changed from panic to explicit unsupported SPI errors as required by the plan, then candidate and seven paired benchmark runs were executed against that final bench source.

## Raw results

The initial corrected run before warning cleanup emitted the following 20 lines (each line is raw executable output):

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1344160, 1363090, 1347987, 1351763, 1350252, 1348744, 1344702] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1173371, 1171871, 1172840, 1169973, 1181565, 1177701, 1176561] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1417907, 1422765, 1419641, 1424004, 1421370, 1420902, 1415706] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1252976, 1254699, 1260472, 1253501, 1255161, 1274255, 1260377] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3523421, 3463194, 3450049, 3452508, 3451202, 3463267, 3457202] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3256258, 3274786, 3249195, 3262237, 3260550, 3276275, 3258577] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3665645, 3684107, 3682930, 3666978, 3675441, 3681809, 3681601] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3452945, 3453896, 3461257, 3464161, 3486600, 3471330, 3498575] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2594843, 2614480, 2601208, 2613846, 2604283, 2592993, 2611169] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2408339, 2403679, 2397478, 2416051, 2407250, 2452749, 2428802] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2750502, 2732812, 2751244, 2749556, 2733779, 2750448, 2753591] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2546365, 2558257, 2539127, 2603805, 2540265, 2551115, 2536751] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24680130, 24605655, 25061057, 25687706, 24618423, 24811007, 24868929] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23866903, 23630939, 23586041, 23650402, 23642502, 23652403, 23648923] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24903517, 25599880, 25173068, 24897298, 25363576, 25329985, 25672849] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25022806, 24802426, 24780220, 24767862, 24752785, 25272796, 24847814] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15879511, 15939672, 17074845, 16442436, 17496870, 16658763, 16439815] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[17362906, 19828416, 16391822, 16456916, 15704957, 16973065, 15848122] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[21438683, 21569464, 22749172, 23906414, 23867963, 22863991, 17316448] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16209987, 16132239, 16088352, 16118396, 16104704, 16099772, 16205513] iterations=1000 boundary=facade_publish_and_receipt_validation
```

## Interpretation

## Interleaved baseline/candidate comparison

Seven process-level pairs were run in baseline→candidate order. Each command was `cargo bench --locked --bench rejection_diagnostics` and exited 0; each process performed two warmups and emitted seven raw batch samples for all 20 cases. The table reports the median of each process's seven batch samples, then the median across seven paired process runs; ranges are the min–max of those seven process medians. Units are nanoseconds per 1,000 facade publishes. The first run of each worktree was a compilation/warm-up run and is not included in the paired set. Baseline production source is `39bee718ed6396a6d8b0e633a854958832ecac89`; candidate adds only the `observers.is_empty()` early return in `PublisherPipeline::emit_rejections`. The final bench source was identical in both worktrees.

| Case | Baseline median (range) | Candidate median (range) |
| --- | ---: | ---: |
| async / 1 destinations / all rejected / 0 observer(s) | 1296292 (1256149–1395329) | 1144728 (1128959–1157230) |
| async / 1 destinations / all rejected / 1 observer(s) | 1368039 (1330513–1398178) | 1335984 (1309981–1353470) |
| async / 128 destinations / all rejected / 0 observer(s) | 24174142 (24018811–24325537) | 13158246 (13112057–13232117) |
| async / 128 destinations / all rejected / 1 observer(s) | 24810381 (24682462–25253785) | 25143529 (25044801–25476667) |
| async / 128 destinations / partial / 0 observer(s) | 15997945 (15889658–16115532) | 10472130 (10420575–10542621) |
| async / 128 destinations / partial / 1 observer(s) | 16390234 (16136322–16669708) | 16492274 (16406248–16709814) |
| async / 16 destinations / all rejected / 0 observer(s) | 3420614 (3385987–3473626) | 1978420 (1948220–1993034) |
| async / 16 destinations / all rejected / 1 observer(s) | 3565280 (3528399–3599424) | 3587079 (3566542–3625605) |
| async / 16 destinations / partial / 0 observer(s) | 2541578 (2516225–2562414) | 1788735 (1757306–1810593) |
| async / 16 destinations / partial / 1 observer(s) | 2658060 (2651600–2716189) | 2656321 (2629547–2674535) |
| sync / 1 destinations / all rejected / 0 observer(s) | 1371019 (1330845–2853216) | 1231304 (1214674–1294556) |
| sync / 1 destinations / all rejected / 1 observer(s) | 1414716 (1383320–1717851) | 1401018 (1380753–1426010) |
| sync / 128 destinations / all rejected / 0 observer(s) | 24149821 (24057395–38582561) | 13072453 (13025731–13309106) |
| sync / 128 destinations / all rejected / 1 observer(s) | 24755658 (24615339–25086528) | 25152431 (25068385–25366101) |
| sync / 128 destinations / partial / 0 observer(s) | 16103804 (15990639–20424298) | 10369547 (10247240–10434703) |
| sync / 128 destinations / partial / 1 observer(s) | 16504657 (16334322–17333588) | 16607320 (16527408–16750331) |
| sync / 16 destinations / all rejected / 0 observer(s) | 3504919 (3485704–3619358) | 2084986 (2063868–2100346) |
| sync / 16 destinations / all rejected / 1 observer(s) | 3662552 (3641037–3965650) | 3664231 (3638309–3801247) |
| sync / 16 destinations / partial / 0 observer(s) | 2641051 (2609606–2685245) | 1893054 (1873483–1918043) |
| sync / 16 destinations / partial / 1 observer(s) | 2752062 (2718785–2794018) | 2762287 (2733381–2789938) |

### Retained raw samples

#### Pair 1 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1357263, 1456659, 1363973, 1361289, 1366011, 1367500, 1361136] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1299179, 1292708, 1295001, 1296292, 1312529, 1307785, 1295752] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1419362, 1414012, 1432349, 1414164, 1421568, 1458348, 1509668] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1439027, 1367152, 1368039, 1402045, 1358224, 1449755, 1367186] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3515186, 3487271, 3513344, 3612884, 3498456, 3476826, 3504919] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3437987, 3436717, 3430657, 3434892, 3438852, 3422514, 3426408] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3705960, 3676581, 3641037, 3637062, 3943925, 3624738, 3636551] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3753347, 3879761, 3580539, 3585179, 3574530, 3588996, 3573462] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2593435, 2729383, 2629313, 2598813, 3252175, 2612368, 2621750] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2541563, 2538711, 2553069, 2529092, 2540891, 2599646, 2532400] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2718785, 2817545, 2714576, 2721330, 2706203, 2717759, 2724674] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2640468, 2651600, 2659869, 2643184, 2659531, 2652358, 2648656] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24149131, 24066832, 24314339, 24181843, 24322651, 24063151, 24265834] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24143439, 24223472, 24066197, 24076271, 24151568, 24241208, 24163244] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25086528, 25090268, 24953150, 25402073, 25094294, 25079657, 25005830] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24735252, 24765100, 25105642, 24888832, 24896590, 24954027, 25098080] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16062585, 16077731, 16103804, 16122224, 16144350, 16083274, 16569767] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16064695, 16138916, 16025707, 16013310, 16034834, 16063455, 16109165] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16431412, 16649807, 16641396, 16543274, 16709883, 16424018, 16503074] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16949970, 17576431, 16657083, 16767853, 16669708, 16582999, 16535579] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 1 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1274083, 1239684, 1318472, 1290339, 1253568, 1254357, 1243595] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1156645, 1156882, 1161344, 1165270, 1151898, 1150808, 1160668] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1476199, 1402018, 1409777, 1422100, 1399678, 1414181, 1405423] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1327794, 1344634, 1327747, 1334104, 1348607, 1338383, 1345999] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2117509, 2153852, 2139425, 2100346, 2094538, 2084244, 2090321] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1989949, 1984955, 1993034, 1989089, 1993678, 1994262, 2002720] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3664101, 3715590, 3677112, 3664231, 3649111, 3663110, 3680315] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3602597, 3582048, 3600461, 3598419, 3607005, 3609374, 3670134] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1915338, 1903271, 1911017, 1904961, 1922765, 1899185, 1905434] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1810593, 1802689, 1818105, 1813552, 1817891, 1810386, 1803718] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2762287, 2754422, 2775807, 2869021, 2790748, 2761086, 2761515] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2677284, 2662227, 2666925, 2666364, 2676372, 2721971, 2670419] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13127613, 13093188, 13027109, 13115604, 13039373, 13058124, 13072453] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13112057, 13064954, 13079022, 13077777, 13126923, 13265245, 13385387] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25116928, 25169025, 25152431, 25062018, 25072139, 25199160, 25166647] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25373822, 25185346, 25115819, 25093647, 25476974, 25375942, 26374341] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10683182, 10637744, 10373961, 10489338, 10434703, 10382978, 10352913] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10587874, 10613255, 10535998, 10461530, 10556985, 10527074, 10542621] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16561154, 16671934, 16552656, 16669736, 16778577, 16647787, 16552328] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16427572, 16632123, 16492274, 16539038, 16508747, 16477633, 16474473] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 2 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1329374, 1329759, 1345540, 1338889, 1342034, 1360533, 1346603] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1279097, 1274785, 1282687, 1267897, 1269017, 1281431, 1269416] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1401593, 1421718, 1401113, 1390034, 1409534, 1393256, 1412736] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1342250, 1346534, 1353877, 1355892, 1344589, 1366503, 1347925] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3485244, 3474807, 3516294, 3512734, 3512491, 3489320, 3510184] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3441409, 3545636, 3520341, 3552316, 3472282, 3473626, 3469952] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3644203, 3646402, 3647583, 3659031, 3657079, 3675353, 3688837] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3541756, 3571064, 3557493, 3580162, 3556175, 3551360, 3565385] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2617870, 2608710, 2618850, 2633688, 2631423, 2647326, 2639580] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2541578, 2513420, 2548599, 2533014, 2570591, 2526361, 2585925] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2711304, 2743301, 2758766, 2756477, 2750491, 2752664, 2745373] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2671791, 2645298, 2648844, 2660288, 2648682, 2667396, 2658060] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24076560, 24104040, 24137290, 24121279, 24095569, 24110772, 24375730] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24149829, 23998105, 24125546, 24329123, 24655295, 24334228, 24287291] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24654041, 24577418, 24601042, 24727557, 24615339, 24587634, 24766263] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24779104, 25067251, 24742453, 24863734, 24713946, 24807478, 24788245] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15980204, 15981973, 16126670, 16056924, 15975311, 16086048, 15990639] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15997945, 16025888, 16028417, 16135714, 15958231, 15916198, 15975829] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16473468, 16444987, 16648932, 16462447, 16466732, 16439747, 16369224] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16463756, 16544439, 16479289, 16460130, 16545889, 16484391, 16453436] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 2 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1220963, 1215127, 1215441, 1216195, 1220635, 1214457, 1211665] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1135863, 1132422, 1124572, 1126635, 1132554, 1128959, 1127788] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1383626, 1381635, 1370250, 1385737, 1389349, 1383756, 1377435] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1312301, 1305908, 1321255, 1314419, 1309981, 1304982, 1302745] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2083576, 2072625, 2071040, 2068758, 2074279, 2069171, 2071622] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1939459, 1935519, 1948220, 2032573, 2080447, 1945159, 1957829] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3656195, 3654121, 3661675, 3651787, 3648543, 3657159, 3674534] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3554535, 3551628, 3563600, 3573007, 3567967, 3566542, 3577092] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1878078, 1943543, 1882238, 1880924, 1881073, 1892771, 1892825] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1757123, 1751264, 1758366, 1748594, 1757306, 1762577, 1758502] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2722889, 2714768, 2718985, 2836421, 2808766, 2755571, 2767305] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2644845, 2635602, 2636472, 2640210, 2633858, 2626179, 2717451] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13053551, 13076358, 13007894, 13022278, 13018914, 13072782, 13025731] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13230915, 13184714, 13220183, 13206304, 13259456, 13247938, 13227403] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25000395, 25032791, 25018252, 25137168, 25110099, 25095122, 25068385] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25061291, 25067440, 25113244, 25063689, 25106064, 25103436, 25180393] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10351854, 10368558, 10333162, 10361207, 10469482, 10400558, 10397091] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10446751, 10463946, 10440133, 10491252, 10564874, 10456020, 10500871] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16428705, 16492027, 16561949, 16527408, 16532658, 16524985, 16579630] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16515150, 16554533, 16519265, 16535412, 16520488, 16587288, 16589590] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 3 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[2853216, 2507281, 2912999, 2901646, 2888850, 2695940, 2358935] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1289275, 1283341, 1283347, 1307694, 1291679, 1287507, 1294024] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1404915, 1400791, 1390888, 1403315, 1400341, 1383662, 1399052] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1359285, 1359853, 1360942, 1372682, 1367973, 1389751, 1370689] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3462236, 3478173, 3494895, 3497342, 3484556, 3485704, 3502442] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3420614, 3405379, 3411281, 3421737, 3448390, 3406547, 3458692] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3666663, 3645619, 3651093, 3673078, 3657409, 3683089, 3662552] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3557503, 3555165, 3567003, 3546165, 3558574, 3586371, 3568478] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2653697, 2650189, 2652050, 2643901, 2661412, 2655587, 2641670] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2525927, 2529858, 2563862, 2562243, 2526787, 2554603, 2551003] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2757122, 2750478, 2767632, 2742243, 2749069, 2752062, 2778045] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2655322, 2761410, 2664395, 2665840, 2671320, 2671126, 2667472] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24018442, 24013227, 24077108, 23974558, 24057395, 24122687, 24122235] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24357037, 24210726, 24243906, 24208232, 24230761, 24234898, 24211296] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24655182, 24648033, 24916872, 25011095, 24705893, 24780258, 24755658] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24742699, 24727331, 24750694, 24690859, 26142286, 25322078, 24927538] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16010346, 16038360, 15977678, 15958308, 16019879, 16026600, 15985692] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15954920, 15953299, 15930040, 15921809, 15938333, 15944868, 15955817] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16495321, 16403356, 16381602, 16334322, 16327976, 16315532, 16334018] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16272447, 16369251, 16254110, 16270109, 16246861, 16273000, 16243770] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 3 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1233622, 1231304, 1229050, 1237605, 1233135, 1229167, 1225716] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1151543, 1145804, 1144728, 1145190, 1141578, 1142362, 1142018] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1401018, 1395876, 1405351, 1400118, 1391031, 1428391, 1407308] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1320484, 1327433, 1317002, 1319514, 1322758, 1319013, 1318826] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2081958, 2072172, 2070557, 2066186, 2071695, 2073485, 2068905] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1961640, 1959363, 1972519, 1959938, 1961169, 1959237, 1966421] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3647392, 3720791, 3636833, 3650610, 3635112, 3650515, 3701400] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3598458, 3579960, 3575249, 3578286, 3575996, 3584375, 3563803] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1892161, 1888696, 1891225, 1887479, 1893049, 1894141, 1892959] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1769398, 1771674, 1777163, 1770702, 1809217, 1805654, 1796406] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2746336, 2750922, 2752976, 2744282, 2755345, 2751393, 2761127] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2655077, 2646786, 2665815, 2653439, 2647512, 2654832, 2656193] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13085473, 13111949, 13134236, 13145889, 13074584, 13109081, 13146075] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13166586, 13139476, 13157757, 13079118, 13123483, 13176357, 13166764] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25200567, 25188925, 25198760, 25218171, 25200967, 25264238, 25272102] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25068725, 25092565, 25080896, 25044801, 25031656, 25004356, 25037355] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10234504, 10247240, 10233026, 10243623, 10344163, 10474525, 10401572] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10401623, 10439633, 10410685, 10426254, 10415482, 10420575, 10430469] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16493366, 16561040, 16620053, 16607320, 16586924, 16673826, 22362876] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16420720, 16422136, 16514035, 16545619, 16471874, 16443592, 16441963] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 4 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1375701, 1366090, 1386448, 1381079, 1382417, 1410422, 1354703] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1314637, 1347464, 1329306, 1342930, 1315525, 1336351, 1314040] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1414735, 1403691, 1428980, 1446143, 1404316, 1473485, 1424108] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1413193, 1390211, 1391497, 1398178, 1386972, 1411956, 1405516] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3494052, 3486478, 3546888, 3490446, 3547114, 3494771, 3541659] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3409227, 3412236, 3432987, 3442367, 3411355, 3408073, 3409211] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3965650, 3861447, 4067386, 4034135, 3961567, 3939074, 4098155] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3567646, 3716363, 3653616, 3596945, 3594304, 3590317, 3585451] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2650465, 2629825, 2640204, 2626482, 2641051, 2833373, 2696488] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2539208, 2539358, 2543625, 2557592, 2541627, 2555597, 2541813] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2751734, 2761706, 2775944, 2744719, 2754164, 2759402, 2756488] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[3042776, 3045598, 2686861, 2673007, 2672004, 2691190, 2674186] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24113773, 24112270, 24326386, 24474919, 24056459, 24212556, 24149821] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24329457, 24126847, 24187145, 24205669, 24165411, 24057812, 24042614] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24676507, 24580331, 24680424, 24649962, 24603342, 24664205, 24660709] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24802974, 24810381, 24789913, 25159154, 24779008, 24988730, 24839719] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16038210, 16060609, 16095707, 16014296, 15999289, 16019823, 16023796] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15895064, 15907676, 15895294, 15882351, 15879256, 15889658, 15880492] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16448157, 16478423, 16485423, 16503070, 16506661, 16476121, 16440464] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16291677, 16193565, 16174549, 16226248, 16263873, 16202846, 16178820] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 4 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1217045, 1219936, 1218795, 1222163, 1218531, 1215332, 1224425] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1141734, 1138198, 1133177, 1137822, 1134145, 1137625, 1140203] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1394481, 1394145, 1388624, 1395046, 1380898, 1392832, 1378332] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1318759, 1315609, 1317425, 1315876, 1313774, 1322420, 1313214] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2068678, 2079673, 2167084, 2309975, 2155758, 2092952, 2092810] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1984092, 1980990, 1989839, 1998252, 1995646, 1988949, 1995529] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3917590, 3935492, 3795526, 3672664, 3701336, 3801247, 3829432] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3707736, 3647516, 3697228, 3577736, 3585975, 3571102, 3580769] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1897436, 1902528, 1900797, 1904898, 1907038, 1903112, 1994922] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1837156, 1777434, 1791816, 1782855, 1945898, 1798958, 1791391] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2769463, 3090937, 2867740, 2897840, 2755636, 2745095, 2743095] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2627351, 2638299, 2761252, 2629547, 2622985, 2615928, 2631354] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13018348, 13007613, 13221040, 13407328, 13034132, 13033355, 13039578] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13175753, 13158143, 13169576, 13163331, 13211232, 13260790, 13206465] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[27714959, 25317994, 25185812, 25195498, 25291698, 25263121, 25249003] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25060230, 25064373, 25063318, 25054485, 25079274, 25046733, 25092775] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10338878, 10338155, 10340009, 10353130, 10357758, 10379361, 10388393] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10412764, 10429105, 10432559, 10473008, 10435719, 10423607, 10421118] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16540017, 16533651, 16496714, 16507462, 16560433, 16554561, 16524372] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16398629, 16406248, 16426751, 16393308, 16405906, 16418925, 16412464] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 5 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1328388, 1331087, 1315896, 1341437, 1329500, 1330845, 1348355] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1256149, 1260282, 1259891, 1254860, 1254683, 1262320, 1245757] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1383320, 1392613, 1383800, 1376237, 1385036, 1376715, 1381199] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1325487, 1331762, 1322887, 1325845, 1330513, 1331031, 1355372] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3472944, 3475588, 3488886, 3503890, 3651267, 3501929, 3482223] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3363906, 3382512, 3385987, 3391477, 3396016, 3383220, 3404711] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3631788, 3639605, 3635813, 3660638, 3657889, 3666316, 3673937] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3528399, 3515045, 3515759, 3521636, 3535348, 3536669, 3555187] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2612200, 2609606, 2613087, 2607698, 2609329, 2624149, 2605487] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2511913, 2516225, 2510613, 2540242, 2501679, 2521696, 2529029] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2729242, 2709979, 2737391, 2900509, 2824870, 2726055, 2760174] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2642120, 2662486, 2687148, 2665430, 2653380, 2645607, 2631456] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24105182, 24037730, 24120753, 23978663, 24169013, 24168840, 24163189] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24042609, 23942323, 23972519, 24021095, 24018811, 24014386, 24020166] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24588892, 24557208, 24618172, 24603432, 24622254, 24766854, 24706875] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24699279, 24804131, 24712529, 24663083, 24609244, 24682307, 24682462] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16032625, 16199433, 16168257, 15975523, 15999539, 16768130, 16238730] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15899140, 15911459, 15888411, 15921195, 15916810, 15949602, 16609428] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16395810, 16419423, 16418484, 18447356, 16618898, 17284608, 16504657] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16196619, 16127142, 16121800, 16136322, 16142297, 16128301, 16140725] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 5 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1216815, 1208720, 1214674, 1223121, 1212801, 1216903, 1213023] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1155806, 1154350, 1153173, 1155094, 1181169, 1153465, 1152258] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1380753, 1376070, 1383573, 1383196, 1390548, 1380205, 1374348] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1345981, 1335745, 1335984, 1340750, 1334186, 1333074, 1340180] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2065000, 2068264, 2064028, 2063868, 2063813, 2056718, 2060737] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1994274, 1989964, 1994196, 1984582, 1994292, 1987516, 1992141] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3630290, 3638309, 3672405, 3661107, 3651762, 3637610, 3635849] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3628365, 3616774, 3618745, 3619182, 3615114, 3610840, 3615675] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1877430, 1873483, 1871869, 1871413, 1877468, 1872438, 1881554] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1777221, 1790243, 1793448, 1785446, 1788735, 1780478, 1794022] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2755101, 2740362, 2735075, 2719502, 2724215, 2726123, 2733381] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2672044, 2674535, 2674030, 2658125, 2707527, 2684143, 2676283] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13063228, 13135179, 13128512, 17581037, 13349451, 13230779, 13085656] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13110050, 13612817, 13211267, 13121501, 13158246, 13138740, 13169156] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24978214, 25343041, 25203089, 25107133, 25095481, 25052552, 25051704] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25389001, 25429356, 25437028, 25476667, 25479102, 25567067, 25493263] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10369547, 10345909, 10340044, 10363388, 10383835, 10395147, 10395272] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10525311, 10530554, 10506430, 10522159, 10486633, 10482527, 10522115] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16797181, 16592237, 16664206, 16668742, 16653737, 16616061, 16620181] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16633824, 16650685, 16674850, 16649472, 16723938, 17180460, 17273941] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 6 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1552003, 1645134, 1579094, 1509399, 2388317, 2099967, 1644308] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1439505, 1395329, 1301892, 1619852, 1442590, 1328331, 1345632] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[2026871, 1594566, 1646630, 1632238, 1717851, 2296594, 2641261] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1439812, 1379056, 1388251, 1347045, 1371713, 1384893, 1421951] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3567576, 3542453, 3619358, 3643640, 3580769, 3706419, 3646570] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3407009, 3506925, 3398198, 3466273, 3395281, 3403453, 3459080] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3710446, 3699556, 3705117, 3698899, 3771043, 3770613, 3780351] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3593934, 3561405, 3565280, 3572260, 3630585, 3561486, 3554810] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2673365, 2728517, 2676163, 2677764, 2685245, 2694066, 2728484] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2538443, 2550365, 2536799, 2534846, 2523393, 2532567, 2562782] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2797191, 2785599, 2804257, 2774372, 2775574, 2778993, 2781218] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2640184, 2678090, 2745605, 2643673, 2659186, 2652848, 2656902] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[27038083, 38582561, 42983707, 43862470, 42895580, 30389147, 28945388] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[25187074, 24549638, 24139361, 24077670, 24171771, 24174142, 24419226] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24967178, 25046934, 25049318, 24833732, 25056416, 25290020, 24953580] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25215321, 25359306, 25245929, 25114851, 26320893, 25446125, 25253785] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16241189, 16400701, 16392634, 16314643, 16139413, 16513403, 17422010] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16025755, 16052527, 16098668, 16563324, 16608984, 16115532, 16169207] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16519283, 16831749, 17147847, 17691205, 16763943, 16654289, 16720270] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[17540445, 16309292, 16386544, 16602800, 16438556, 16412161, 16258141] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 6 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1562445, 1336947, 1290009, 1291433, 1361756, 1294556, 1260644] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1134508, 1133391, 1170921, 1151846, 1127536, 1149294, 1131508] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1443972, 1424483, 1423683, 1423288, 1400620, 1410506, 1396941] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1367319, 1353470, 1354864, 1341841, 1324868, 1326271, 1372285] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2146043, 2078521, 2079769, 2087197, 2084986, 2081927, 2087960] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1960317, 1957343, 1954180, 1945358, 2023181, 1955646, 1968297] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3777177, 3696083, 3734445, 3759868, 3693166, 3722195, 3674571] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3627112, 3587079, 3621085, 3576424, 3566707, 3568956, 3636031] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1887025, 1873945, 1895123, 1893015, 1893277, 1893054, 1893592] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1759418, 1758092, 1764266, 1768318, 2618473, 3891017, 3695606] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2789938, 2810198, 2823134, 2834316, 2785856, 2777329, 2767173] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2645746, 2656321, 2701449, 2716261, 2686141, 2642929, 2647062] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13612913, 13292860, 13398466, 13333511, 13148929, 13095144, 13309106] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13232633, 13185533, 13109547, 13102736, 13268402, 13232117, 13232523] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25304050, 25433986, 25424911, 25434852, 25361436, 25242500, 25366101] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25699336, 25414378, 25278666, 25333970, 25246194, 25165260, 25267687] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10307261, 10395097, 10399133, 10344019, 10383480, 10359514, 10414911] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10524690, 10563844, 10504689, 10498022, 10502841, 10605240, 10482445] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16653144, 16767456, 16662104, 16949299, 16943664, 16750331, 16719826] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16859284, 16694343, 16760201, 16635172, 16722213, 16604146, 16709814] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 7 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1409868, 1368230, 1383276, 1356356, 1371019, 1391601, 1360566] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1301595, 1305572, 1284058, 1294338, 1292615, 1317502, 1313543] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1412585, 1399904, 1410309, 1415124, 1456929, 1467129, 1414716] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1379296, 1464534, 1393330, 1388297, 1405879, 1399831, 1355333] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3533649, 3533939, 3503467, 3555411, 3536033, 3548669, 3534855] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3441592, 3438617, 3449626, 3483159, 3458517, 3499404, 3437386] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3678885, 3684216, 3724326, 3693631, 3708506, 3709626, 3712102] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3625653, 3610381, 3598420, 3575495, 3614122, 3599424, 3584632] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2673659, 2671733, 2666736, 2651375, 2662017, 2686915, 2719516] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2567464, 2544478, 2562414, 2556366, 2547995, 2652560, 2568805] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2793327, 2763877, 3109992, 2794018, 2760436, 2864071, 2798041] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2697713, 2678069, 2716189, 2695072, 2718998, 3020123, 3174516] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24628307, 24537940, 24416755, 24480773, 24474555, 24524257, 24716904] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[25354990, 24549550, 24144705, 24257019, 24325537, 24372470, 24312510] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25073372, 24980422, 24953091, 24903500, 25187042, 25152776, 25042699] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25130909, 24996754, 25081628, 24905479, 24851732, 25053472, 25068368] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[31723375, 28487021, 20424298, 21496797, 16560132, 17938221, 18012337] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[19647945, 17174977, 16623705, 16060224, 15965491, 16056711, 16044999] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[18233020, 18569764, 16794900, 16860760, 17053409, 17333588, 17852118] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[17206472, 16521329, 16376652, 16390234, 16382828, 16369847, 16407147] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 7 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1264321, 1259975, 1258512, 1268065, 1263037, 1291057, 1286309] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1160715, 1157230, 1155387, 1169600, 1153945, 1155062, 1160296] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1444301, 1425892, 1428350, 1426010, 1428618, 1425817, 1415848] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1367926, 1345002, 1338201, 1373196, 1349091, 1348559, 1336112] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2112552, 2093734, 2105554, 2116741, 2099678, 2097136, 2100272] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1981265, 1973021, 1981445, 1977071, 1978420, 1978217, 1981573] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3698114, 3743882, 3688985, 3687888, 3689137, 3698750, 3682572] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3630800, 3609860, 3632997, 3625605, 3611209, 3633484, 3610139] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1923451, 1920358, 1939905, 1909044, 1912161, 1896667, 1918043] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1796053, 1785629, 1793490, 1783576, 1811139, 1795104, 1807828] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2775604, 2760510, 2788070, 2779932, 2774508, 2772869, 2776766] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2670223, 2663782, 2673665, 2685151, 2662954, 2677776, 2668126] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13039671, 13048351, 13072214, 13035935, 13051514, 13065289, 13188551] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13143415, 13102430, 13100478, 13098203, 13148930, 13144023, 13147413] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25086833, 25085159, 25077294, 25110975, 25103981, 25090441, 25089726] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25123226, 25108155, 25152476, 25159105, 25145891, 25114736, 25143529] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10379872, 10363403, 10376019, 10384537, 10405108, 10376291, 10406341] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10472130, 10464502, 10477698, 10534890, 10472152, 10458378, 10450314] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16486363, 16607335, 16575068, 16595846, 16589023, 16663779, 16629252] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16494374, 16463204, 16464848, 16446788, 16435799, 16498371, 16412293] iterations=1000 boundary=facade_publish_and_receipt_validation
```

### Interpretation

For zero observers, the candidate removes per-rejection diagnostic construction. The paired medians show lower measured time in the 16- and 128-destination groups; one-destination differences are smaller. Observer-present cases are the control and remain in the same general range, though some samples show scheduler noise. This is benchmark evidence for this host and setup, not a portable performance guarantee; no timing assertion or CI threshold was added.

## Final-source interleaved comparison (authoritative)

The earlier interleaved section is retained as preliminary data from before the Clippy-driven benchmark source cleanup. The following final-source measurement supersedes it. Baseline production source is `39bee718ed6396a6d8b0e633a854958832ecac89`; candidate source adds only the empty-observer guard. Both sides used byte-identical final bench code (`benches/rejection_diagnostics.rs` SHA-256 `78eeac85c1ba24f6955153e348f490874eb56bc697129969b66c979b4269f05a`); candidate pipeline source SHA-256 was `4df7cfc3c64f8dd4da704c26440c95b944bf290a8873b57e20018bb449057b92`. Commands alternated baseline then candidate for seven pairs. All 14 commands exited 0 and produced all 20 cases. Each case contains seven timed batches of 1,000 publishes after two warmups. Table values are the median of seven samples within each process, then the median across seven process-level pair values; ranges are min–max of those seven process medians. Values are nanoseconds per 1,000 publishes.

| Case | Baseline median (range) | Candidate median (range) |
| --- | ---: | ---: |
| async / 1 destinations / all rejected / 0 observer(s) | 1238510 (1222706–1404215) | 1138031 (1113888–1183436) |
| async / 1 destinations / all rejected / 1 observer(s) | 1322257 (1306947–1495959) | 1322846 (1308870–1355239) |
| async / 128 destinations / all rejected / 0 observer(s) | 23905843 (23844376–25805543) | 12996169 (12928619–13024253) |
| async / 128 destinations / all rejected / 1 observer(s) | 24968053 (24920134–25138870) | 24774681 (24558344–24931509) |
| async / 128 destinations / partial / 0 observer(s) | 15726341 (15616202–15910141) | 10305549 (10245738–11769964) |
| async / 128 destinations / partial / 1 observer(s) | 16234333 (16145627–16470000) | 16190380 (16113131–16450716) |
| async / 16 destinations / all rejected / 0 observer(s) | 3344707 (3327471–3472184) | 1962264 (1932300–2005036) |
| async / 16 destinations / all rejected / 1 observer(s) | 3560545 (3539872–3737197) | 3551781 (3508396–3580835) |
| async / 16 destinations / partial / 0 observer(s) | 2493077 (2468691–2637734) | 1776701 (1749218–1823013) |
| async / 16 destinations / partial / 1 observer(s) | 2628388 (2608193–2765842) | 2628115 (2600508–2684276) |
| sync / 1 destinations / all rejected / 0 observer(s) | 1337171 (1315281–1359807) | 1229136 (1210722–1283695) |
| sync / 1 destinations / all rejected / 1 observer(s) | 1408366 (1393609–1431372) | 1410533 (1393590–1461510) |
| sync / 128 destinations / all rejected / 0 observer(s) | 23920907 (23728747–23973648) | 12968592 (12929732–13036382) |
| sync / 128 destinations / all rejected / 1 observer(s) | 24787715 (24761687–25003648) | 24821972 (24681442–27553877) |
| sync / 128 destinations / partial / 0 observer(s) | 15856188 (15780058–15991302) | 10216845 (10172560–10323766) |
| sync / 128 destinations / partial / 1 observer(s) | 16423984 (16337512–16501513) | 16260110 (16156679–17294987) |
| sync / 16 destinations / all rejected / 0 observer(s) | 3473318 (3432508–3503439) | 2101254 (2067065–2130279) |
| sync / 16 destinations / all rejected / 1 observer(s) | 3640622 (3619207–3702424) | 3663915 (3621877–3692501) |
| sync / 16 destinations / partial / 0 observer(s) | 2617337 (2578970–2630562) | 1885417 (1872841–1931123) |
| sync / 16 destinations / partial / 1 observer(s) | 2736555 (2701406–2754505) | 2745932 (2706875–2816782) |

### Final-source raw samples

#### Pair 1 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1328988, 1344978, 1341348, 1333384, 1334868, 1330188, 1330937] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1237892, 1236435, 1236703, 1247092, 1242232, 1233693, 1249110] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1405733, 1395314, 1393258, 1409239, 1394720, 1636599, 1402899] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1354082, 1337684, 1320758, 1321053, 1333391, 1322257, 1317828] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3450650, 3440519, 3445728, 3434982, 3457068, 3444568, 3463779] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3344707, 3326300, 3323413, 3414889, 3619155, 3416343, 3339941] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3613300, 3630950, 3625529, 3617243, 3645169, 3631868, 3646070] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3544950, 3542230, 3533230, 3550004, 3538226, 3548018, 3541016] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2582956, 2594298, 2579608, 2588135, 2586031, 2587548, 2597672] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2560709, 2484125, 2483023, 2496401, 2493077, 2499071, 2486165] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2720473, 2712761, 2729577, 2720839, 2725114, 2717367, 2735406] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2611657, 2626208, 2644868, 2644944, 2630988, 2617324, 2625783] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23750847, 23775287, 23993435, 23726919, 23807508, 23850681, 23932952] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24144755, 23796094, 23813099, 23864467, 23899342, 23846916, 23992386] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24806262, 24738828, 24812790, 24827003, 24775658, 24775510, 24786913] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24915630, 25047428, 24968053, 24942956, 25009217, 25111752, 24922146] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15889905, 15907411, 16427087, 15974165, 15857184, 15786241, 15784996] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15642664, 15636453, 15626187, 15699386, 15768082, 15705481, 15702684] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16240296, 16351679, 16446199, 16337512, 16363246, 16327366, 16319373] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16144162, 16196251, 16320918, 16106406, 16183719, 16270871, 16132954] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 1 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1229538, 1235572, 1220370, 1222682, 1215585, 1211910, 1223865] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1113888, 1113633, 1113285, 1125189, 1112872, 1130592, 1124683] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1431297, 1410384, 1425742, 1408173, 1403868, 1426720, 1410533] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1316889, 1326318, 1310188, 1314158, 1320076, 1313882, 1306283] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2084142, 2090518, 2096172, 2111179, 2087149, 2090753, 2090117] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1939624, 1957714, 1945908, 1932834, 1938549, 1935371, 1937252] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3627172, 3637991, 3621877, 3626811, 3616171, 3618203, 3610036] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3505763, 3498801, 3517070, 3503523, 3518013, 3508396, 3512050] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1853852, 1859845, 1861749, 1879282, 1889347, 1888944, 1958589] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1755215, 1746244, 1757160, 1749626, 1750557, 1748218, 1753977] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2725094, 2823910, 2816782, 2819644, 2817561, 2799155, 2725029] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2601345, 2589724, 2615332, 2576230, 2600508, 2577290, 2616548] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12941880, 12943407, 12946125, 12955782, 12989623, 13047313, 13045603] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12962699, 12982319, 12961375, 12949706, 12970248, 12984996, 12980747] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24712003, 24930850, 24735091, 25482825, 24891853, 24806728, 24835774] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[26032862, 24701283, 24672332, 24633948, 24673686, 24700898, 24663617] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10172560, 10163571, 10164884, 10170932, 10210722, 10206250, 10229621] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10263990, 10262523, 10296293, 10269890, 10239289, 10215177, 10338290] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16183543, 16204754, 16215807, 16217708, 16331937, 16205115, 16239541] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16065892, 16079582, 16105307, 16117781, 16129794, 16139032, 16138150] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 2 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1334672, 1347955, 1335697, 1343798, 1342547, 1334803, 1337171] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1222703, 1223955, 1219354, 1236490, 1221142, 1223515, 1222706] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1403872, 1419333, 1403573, 1410177, 1399012, 1397663, 1413460] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1312451, 1306947, 1309226, 1306662, 1301072, 1301551, 1309354] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3481662, 3473303, 3466393, 3473254, 3468887, 3468404, 3463560] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3325323, 3324046, 3332716, 3327471, 3328074, 3331963, 3322800] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3636225, 3640622, 3628982, 3641045, 3679199, 3650669, 3635429] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3634869, 3539872, 3534890, 3528723, 3542041, 3536616, 3685687] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2634846, 2632386, 2635496, 2625709, 2621941, 2630562, 2628893] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2473564, 2473330, 2471909, 2490164, 2474119, 2472927, 2476090] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2738357, 2741073, 2736555, 2723017, 2734533, 2731738, 2737763] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2614483, 2606596, 2621092, 2608853, 2621951, 2625837, 2620140] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23893110, 23920907, 23892285, 23875026, 23960272, 23969811, 23982287] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23905843, 23769487, 23911177, 23853158, 23874709, 23990830, 23984956] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24925081, 24782765, 24829792, 24891831, 24815084, 24984991, 25009199] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24914665, 24893889, 24920134, 24881289, 26487272, 25048154, 24938199] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15874038, 15912386, 15902445, 15893246, 16030432, 15896627, 15898720] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15597936, 15615759, 15641516, 15812416, 15591079, 15616202, 15629500] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16452185, 16501513, 16501862, 16634459, 16468270, 16503742, 16449057] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16146322, 16145627, 16281706, 16116548, 16156839, 16119417, 16137364] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 2 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1222810, 1210172, 1210929, 1214812, 1209017, 1208281, 1210722] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1183436, 1187956, 1176082, 1184363, 1180116, 1180291, 1188475] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1407064, 1392776, 1415128, 1397693, 1393115, 1404213, 1389962] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1353197, 1362658, 1350120, 1355239, 1358333, 1355611, 1351966] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2067065, 2072383, 2073951, 2061481, 2065083, 2069522, 2064608] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2003811, 2007703, 2005036, 2000120, 2000697, 2005332, 2007709] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3617416, 3643602, 3624340, 3617979, 3630911, 3616265, 3623977] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3576664, 3581227, 3567861, 3580835, 3600327, 3611893, 3577484] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1885723, 1872767, 1877598, 1989858, 1897614, 1885417, 1878781] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1812571, 1820223, 1834589, 1824908, 1828055, 1819834, 1823013] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2715685, 2704289, 2726582, 2702886, 2699364, 2706875, 2713756] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2687757, 2684276, 2678976, 2729907, 2676371, 2675437, 2697102] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12926839, 12894596, 12901860, 12931441, 13216090, 13136362, 13043729] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13024253, 13023798, 13054935, 13003125, 13056970, 13037998, 13024216] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24747818, 24704739, 24698347, 24802272, 24850967, 24752099, 24801193] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24820293, 24864436, 25014116, 24895881, 24886684, 24924484, 25076914] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10171097, 10159092, 10196258, 10169989, 10265380, 10211713, 10268218] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10305549, 10297168, 10276747, 10281034, 10356244, 10416687, 10617466] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16099660, 16142701, 16200961, 16178663, 16164137, 16178024, 16198598] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16231888, 16329029, 16170886, 16182040, 16135962, 16216058, 16205159] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 3 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1309848, 1309356, 1326285, 1315281, 1311851, 1315748, 1323783] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1404215, 1390468, 1407983, 1405599, 1399476, 1415538, 1396708] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1410162, 1409539, 1414352, 1417318, 1408719, 1403480, 1415924] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1480627, 1485399, 1495959, 1496392, 1496387, 1515484, 1480811] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3481380, 3473318, 3461020, 3454782, 3515327, 3506922, 3464057] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3472627, 3472184, 3470130, 3521724, 3478174, 3472180, 3464167] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3619207, 3624731, 3603044, 3617397, 3623582, 3613474, 3751984] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3737197, 3740068, 3769682, 3745581, 3714078, 3715139, 3711582] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2574995, 2578970, 2573156, 2581183, 2582974, 2592733, 2574977] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2641932, 2633133, 2639013, 2640954, 2637734, 2637513, 2633057] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2701807, 2703952, 2701406, 2712710, 2697178, 2692831, 2700306] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2754979, 2765842, 2770277, 2764495, 2759543, 2765887, 2768231] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23880966, 23903472, 23979707, 23864023, 23997883, 23949586, 25402706] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24306712, 24019934, 24032530, 24029824, 24083003, 24075345, 24092051] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24762320, 25813435, 24757912, 24781142, 24787715, 24812964, 24860506] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25088030, 25138870, 25303849, 25243681, 25143142, 25132414, 25133179] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15788565, 15813822, 15860667, 15802673, 15917053, 15834416, 15836747] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15876524, 15910141, 15911909, 15906864, 15895534, 15970227, 16022268] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16322875, 16386847, 16459518, 16436618, 16393690, 16365563, 16351352] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16592538, 16499069, 16470000, 16468617, 16441408, 16501020, 16454062] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 3 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1229149, 1227253, 1229136, 1227512, 1238709, 1229751, 1225536] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1121305, 1119783, 1118922, 1120783, 1127237, 1118743, 1118867] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1415005, 1415816, 1421711, 1409763, 1422973, 1416722, 1413653] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1306450, 1310037, 1300933, 1308870, 1310948, 1308942, 1298991] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2073847, 2084088, 2070028, 2069607, 2066850, 2066069, 2069035] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1935012, 1967154, 1935632, 1932275, 1932300, 1924243, 1931801] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3627375, 3631516, 3626040, 3619183, 3625151, 3611825, 3623385] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3517422, 3513908, 3545964, 3514109, 3541142, 3535882, 3524116] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1866216, 1868320, 1873803, 1872841, 1874751, 1871356, 1875251] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1747837, 1749475, 1744073, 1747400, 1749218, 1753419, 1750629] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2729976, 2717443, 2722356, 2733212, 2722488, 2720339, 2751311] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2636226, 2628115, 2617850, 2639098, 2643047, 2618152, 2623439] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13063870, 12890288, 12897405, 12929732, 12959340, 12887137, 12970881] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12933906, 12911183, 12928619, 12878704, 12918769, 12947392, 12934833] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24681442, 24647029, 24642623, 24657643, 24724578, 24695253, 24744122] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24558344, 24552632, 24538330, 24563302, 24577465, 24557290, 24582313] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10205284, 10202513, 10205815, 10214596, 10253261, 10240272, 10236848] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10244277, 10259052, 10244346, 10266213, 10245738, 10239361, 10251731] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16101197, 16136800, 16138932, 16185156, 16159182, 16156679, 16178694] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16118049, 16113131, 16095170, 16340551, 16135429, 16106084, 16057867] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 4 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1330761, 1322428, 1321133, 1322681, 1343546, 1324090, 1322028] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1229706, 1220739, 1224968, 1232214, 1236666, 1229873, 1231954] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1393609, 1399793, 1395906, 1393172, 1386145, 1389913, 1397074] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1311217, 1311697, 1305616, 1314477, 1306023, 1309214, 1308457] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3432508, 3433021, 3431755, 3426425, 3519890, 3426214, 3531380] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3325306, 3340837, 3345965, 3328027, 3331508, 3331332, 3351495] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3607590, 3651673, 3660808, 3617487, 3596657, 3620025, 3654165] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3554370, 3539932, 3568164, 3584621, 3548464, 3568219, 3559219] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2607452, 2653984, 2588282, 2576228, 2584513, 2582216, 2573084] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2491897, 2461909, 2468691, 2470030, 2471982, 2460577, 2461305] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2708110, 2699975, 2710043, 2706044, 2695708, 2704680, 2702754] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2610321, 2600070, 2608193, 2613058, 2599804, 2612135, 2602570] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23689193, 23689865, 23728747, 23649759, 23802235, 24834079, 24503223] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[37828141, 27722891, 25381042, 25805543, 26592944, 24240382, 24734060] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24857243, 24978092, 25398871, 25062718, 25043667, 24890413, 25003648] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25036714, 24972746, 24985398, 25061512, 25004365, 25125874, 25135441] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16029198, 15991302, 15948723, 16025041, 15995675, 15863439, 15908454] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15725173, 15709650, 15891712, 15666657, 15694733, 15994836, 15652942] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16469741, 16437534, 16465842, 16516747, 16524505, 16441953, 16409323] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16378174, 16215315, 16299862, 16524990, 16760034, 16537385, 16294469] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 4 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1264078, 1253008, 1253152, 1257005, 1263838, 1254720, 1265204] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1158125, 1164649, 1163503, 1169984, 1162161, 1162152, 1159696] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1428864, 1462335, 1456270, 1427490, 1425758, 1432879, 1423443] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1394328, 1351474, 1331037, 1394552, 1377848, 1335489, 1333016] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2101254, 2097450, 2111004, 2123158, 2146437, 2089303, 2092313] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1977942, 1970024, 1988780, 1975353, 1984283, 1976143, 1979019] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3676318, 3782734, 3681206, 3655709, 3671264, 3711629, 3734674] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3553264, 3547949, 3610309, 3566278, 3579098, 3569469, 3560738] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1897805, 1888369, 1928800, 1898391, 1890386, 1881978, 1895553] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1781739, 1776173, 1780876, 1782869, 1788702, 1801822, 1781334] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2748410, 2733299, 2723149, 2731701, 2730748, 2735307, 2745555] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2626467, 2628840, 2633455, 2623085, 2626610, 2628436, 2635114] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12976960, 13010245, 13036382, 13033072, 13093942, 13202143, 13140570] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12992220, 13107874, 13003992, 13064932, 13019878, 12983666, 13045360] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24817373, 24791343, 24859076, 24863242, 24977168, 24876779, 24874863] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24740206, 24722462, 24834924, 25084891, 24993477, 24931509, 25075251] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10256433, 10252114, 10298780, 10236399, 10331723, 10380052, 10462904] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10293073, 10316616, 10334274, 10308193, 10296724, 10307914, 10331264] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16321941, 16300016, 16261566, 16264105, 16503482, 16299239, 16332832] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16449540, 16428528, 16472371, 16537942, 16572518, 16450716, 16416741] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 5 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1347909, 1350567, 1370472, 1351533, 1351127, 1337508, 1341622] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1278480, 1271459, 1290000, 1265256, 1269334, 1281801, 1269631] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1408366, 1414124, 1407552, 1408678, 1407359, 1405231, 1422033] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1383105, 1419412, 1356046, 1365240, 1347632, 1345920, 1357761] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3499761, 3471534, 3470915, 3470582, 3528857, 3566985, 3552036] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3401381, 3441672, 3423443, 3587191, 3445273, 3398551, 3436940] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3664136, 3684770, 3702529, 3729070, 3727243, 3682615, 3668327] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3585959, 3588283, 3581003, 3594572, 3573614, 3588394, 3595475] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2621857, 2611531, 2624648, 2625644, 2609078, 2619498, 2614084] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2583879, 2526310, 2540494, 2526606, 2543353, 2526037, 2540068] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2748699, 2755269, 2757764, 2749213, 2745489, 2754687, 2754505] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2676812, 2678173, 2695350, 2664153, 2663614, 2698501, 2686119] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23978726, 23995384, 24132768, 23924084, 23895500, 23973648, 23884501] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23963742, 23943744, 24738197, 24989942, 24026066, 23933457, 23932861] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24811879, 24748194, 24762500, 24697097, 24761687, 24724900, 24761946] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24985887, 25054260, 25037927, 25074890, 25050181, 25036158, 25070235] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15829373, 15863456, 15922446, 15822491, 15876608, 15826454, 15856188] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15795959, 15739293, 15746732, 15722188, 15740935, 15728279, 15740826] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16345236, 16427805, 16503273, 16424495, 16447301, 16395896, 16356513] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16310897, 16353634, 16339517, 16321976, 16277286, 16295438, 16319532] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 5 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1290613, 1283695, 1295742, 1277769, 1274974, 1290172, 1282051] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1148816, 1148802, 1159648, 1149655, 1143999, 1146613, 1147815] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1467703, 1460434, 1472681, 1441112, 1464141, 1458091, 1461510] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1332408, 1335911, 1335417, 1336224, 1335948, 1337773, 1339452] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2196750, 2127594, 2129555, 2124293, 2130279, 2130734, 2138182] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1950320, 1952795, 1968956, 1958334, 1964136, 1963552, 1962264] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3675836, 3692523, 3684463, 3676030, 3692501, 3771215, 3736498] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3555933, 3535975, 3543997, 3551200, 3551781, 3552235, 3630682] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1922453, 1930980, 1931123, 1926405, 2004854, 2011851, 1937021] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1769329, 1773227, 1779507, 1770455, 1767341, 1777779, 1780772] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2772880, 2764880, 2790691, 2777592, 2787625, 2766624, 2763180] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2618375, 2617298, 2640328, 2602587, 2618268, 2609854, 2637460] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13010917, 13027903, 13289123, 13071586, 13032632, 13019370, 13043758] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12977090, 12961193, 12980302, 12997868, 13011605, 13000716, 12996169] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24875067, 24794812, 24800184, 24780501, 24828505, 24828332, 24821972] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24783570, 24803121, 24825968, 24777256, 24787771, 24778743, 24831124] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10248664, 10265532, 10254810, 10259248, 10289574, 10290426, 10296974] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10338125, 10325390, 10310459, 10395909, 10374581, 10372678, 10374491] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16182930, 16236325, 16280626, 16295360, 16392443, 16359624, 16328039] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16156507, 16195870, 16214174, 16198244, 16185840, 16190380, 16179484] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 6 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1368272, 1352204, 1358565, 1359807, 1344767, 1363901, 1376828] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1328039, 1238510, 1235660, 1246096, 1233895, 1239139, 1236247] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1434086, 1402404, 1417153, 1398470, 1411389, 1410006, 1411417] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1310876, 1326697, 1309967, 1315295, 1322995, 1312968, 1318730] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3499231, 3509848, 3497762, 3501124, 3514469, 3505500, 3503439] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3358482, 3352967, 3360844, 3363728, 3349796, 3405766, 3359924] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3709291, 3702424, 3696541, 3709714, 3710608, 3701538, 3690654] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3554430, 3556849, 3560545, 3561888, 3551833, 3562345, 3560781] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2626351, 2617337, 2621035, 2614636, 2611732, 2619196, 2612048] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2490606, 2490070, 2513090, 2496521, 2512899, 2507787, 2518787] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2742790, 2726980, 2749828, 2732228, 2724435, 2747266, 2739050] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2651094, 2640455, 2635246, 2652923, 2637499, 2645897, 2656105] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23935158, 23906779, 23902544, 23836608, 23956952, 23927884, 23946103] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23940822, 23849660, 23860439, 23823908, 23821475, 23957852, 23909770] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25052294, 24818235, 24897795, 25237473, 24804977, 24775120, 24820997] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24947978, 24968318, 24977827, 24945877, 24932777, 24959479, 24939394] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15811299, 15855318, 15848963, 15858362, 15818579, 15859988, 15868807] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15914327, 15877271, 15866396, 15863266, 15858604, 15823250, 15829834] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16354984, 16414176, 16464349, 16413101, 16405413, 20808865, 24055614] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16232612, 16220303, 16204997, 16204682, 16207695, 16212363, 16205581] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 6 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1240787, 1247015, 1238175, 1248135, 1241121, 1241081, 1240878] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1138031, 1132793, 1132766, 1142186, 1132219, 1147222, 1227395] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1397886, 1394290, 1403744, 1412844, 1409582, 1407900, 1399518] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1324239, 1313065, 1303409, 1320204, 1313133, 1312764, 1326716] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2097407, 2114805, 2107377, 2103291, 2102461, 2093523, 2133569] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1959790, 1955568, 1964864, 1959628, 1972073, 1958552, 1964140] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3660519, 3661703, 3647084, 3663915, 3711549, 3681803, 3690250] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3546747, 3531241, 3526437, 3536945, 3538468, 3537133, 3534368] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1881706, 1872251, 1883408, 1874710, 1952627, 1872590, 1890616] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1758652, 1776701, 1777399, 1774860, 1769141, 1780915, 1785531] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2776120, 2751890, 2756396, 2752239, 2764417, 2755399, 2757054] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2621842, 2628398, 2628906, 2610916, 2629847, 2606471, 2632908] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12938918, 12930728, 12936092, 12968592, 13043338, 13076560, 13001661] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13014849, 13006386, 12994240, 13163182, 13005734, 13025173, 13089600] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[27293576, 27471079, 27553877, 27730130, 27800057, 27628644, 27383212] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24738711, 25028009, 24805779, 24967177, 24738960, 24769260, 24686901] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10242088, 10237626, 10191488, 10219238, 10216845, 10213984, 10216332] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10294060, 10286239, 10268561, 10325034, 10274358, 10286556, 10311530] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16185934, 16267367, 16260110, 16267528, 16247479, 16259478, 16262496] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16122994, 16134470, 16140646, 16139779, 16211366, 16133820, 16132470] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 7 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1346122, 1343447, 1361564, 1355313, 1350139, 1353108, 1347430] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1251283, 1239845, 1240655, 1246057, 1246158, 1236319, 1238306] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1429591, 1439996, 1431372, 1432247, 1426412, 1433138, 1424904] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1329460, 1330983, 1326994, 1329002, 1338011, 1322586, 1321419] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3479475, 3465577, 3488296, 3479931, 3493991, 3479507, 3499913] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3325210, 3329200, 3335147, 3323220, 3325286, 3328031, 3329034] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3655361, 3688873, 3676670, 3669345, 3675733, 3697046, 3683201] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3599544, 3630930, 3635166, 3589380, 3587643, 3546305, 3573441] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2622156, 2642957, 2628473, 2616130, 2619971, 2620254, 2623407] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2486591, 2484544, 2486041, 2500583, 2480097, 2494595, 2489797] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2742114, 2742409, 2746219, 2742619, 2738720, 2734046, 2735004] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2628388, 2629421, 2629879, 2615631, 2625288, 2622613, 2645286] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23704205, 23743955, 23732823, 23744637, 23752152, 24199315, 23853469] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24161766, 23780146, 23763951, 23844376, 23825235, 23919359, 23879563] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24758665, 24889890, 24750924, 24703684, 24776681, 24791518, 24829231] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24985984, 24901545, 24930900, 24881836, 25056629, 24926250, 24963887] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15759622, 15800330, 16045204, 15780058, 15773326, 15771487, 15782475] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15748840, 15720881, 15726341, 15792544, 15716761, 15720601, 15749678] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16343230, 16429829, 16489511, 16423984, 16451969, 16410016, 16374810] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16320725, 16240595, 16277417, 16234333, 16225214, 16196930, 16196021] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Pair 7 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1216284, 1216751, 1223513, 1224405, 1222650, 1216383, 1218514] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1129102, 1138577, 1137290, 1133222, 1126598, 1157565, 1144975] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1401539, 1392272, 1396523, 1391511, 1400729, 1393590, 1389407] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1324995, 1323440, 1308057, 1321380, 1322846, 1313537, 1328293] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2105801, 2097894, 2121954, 2098016, 2113946, 2109130, 2097959] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1968026, 1966375, 1976881, 1983393, 1986093, 1972996, 1982215] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3675546, 3665493, 3672984, 3672792, 3661853, 3670585, 3659350] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3557783, 3544833, 3564465, 3656162, 3549566, 3554343, 3536388] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1888828, 1886053, 1883386, 1886093, 1888643, 1871855, 1909856] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1778715, 1783856, 1791088, 1768175, 1784321, 1774521, 1776971] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2748383, 2736208, 2745932, 2727461, 2746827, 2720612, 3014228] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2626536, 2614738, 2616023, 2612715, 2608055, 2640834, 2627839] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12941451, 12966535, 12969854, 13099498, 13035263, 13055145, 13053438] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12977643, 12992478, 13003108, 12968480, 12990292, 12996331, 13008646] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24736509, 24746774, 24721127, 24724364, 25421822, 25455313, 25473279] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24764029, 24774681, 24785475, 24733525, 24775906, 24766203, 24927657] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10303869, 10402650, 10275384, 10289113, 10323766, 10659136, 11055190] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[12891217, 10520318, 11207437, 11579767, 12283784, 11769964, 12603607] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16317482, 17743865, 16509487, 16422674, 17294987, 18599266, 17890788] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[20094778, 20106890, 17192492, 16442884, 16254963, 16307544, 16271632] iterations=1000 boundary=facade_publish_and_receipt_validation
```

### Final-source interpretation

Zero-observer measurements are consistently lower for the 16- and 128-destination cases, in both all-rejected and partial-rejection shapes and in sync/async facades. The one-destination differences are small. One-observer controls remain near their baseline values; outliers show shared-host scheduling noise. This is evidence for the measured Linux host and toolchain, not a general performance promise. No timing assertion was added to CI.

## T5 integrated validation

- `./.infra/bin/align-ci.sh`: first run exited 1 on missing full license headers in three new Rust files; after fixing them, a second run exited 1 on five Clippy findings in the bench and test probe. Both were corrected without lint suppressions. The final alignment run exited 0.
- `./.infra/bin/ci-check.sh`: exited 0, including workspace tests, default/discovery/conformance/all-feature matrix, Rustdoc/doctests, package verification, coverage, and audit. Coverage: 95.21% functions, 92.08% lines, 93.31% regions. `coverage-cfg-clippy` was explicitly unconfigured and skipped by repository CI.
