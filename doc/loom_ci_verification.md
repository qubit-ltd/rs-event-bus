# Loom CI 核查

核查日期：2026-11-04

## 固定共享 workflow

本仓 `.github/workflows/ci.yml` 固定引用 `qubit-ltd/rs-infra-ci` commit `22e34b76a52b6a9c5c09ddf3fa1fa58b07c46f06`，未用 floating `main` 推断其行为。核查的固定文件为 [该 commit 的共享 workflow](https://github.com/qubit-ltd/rs-infra-ci/blob/22e34b76a52b6a9c5c09ddf3fa1fa58b07c46f06/.github/workflows/github-ci.yml)。该文件在 `advanced-verification` job 中包含 `loom` / `ubuntu-latest` suite（约第 347–361 行）；suite 检测会在 workspace package 依赖名为 `loom` 时启用（约第 398–407 行），运行步骤设置 `RUSTFLAGS=--cfg loom`，然后调用 `./.infra/bin/infra-tool.sh rs-infra-verify --project "$GITHUB_WORKSPACE" run --suite loom`（约第 427–436 行）。本仓 `Cargo.toml` 有 Loom 依赖，故检测条件满足。

固定 workflow 的调用 job 自有这些触发条件：push 到 `main`、任意 pull request、`workflow_dispatch`。远端 verify job 的额外脚本步骤将 `verify-extra-script` 放入环境变量，并通过 `bash -e -o pipefail -c` 执行；因此任一额外命令失败会使该步骤失败，job 也不会将 Loom suite 的失败吞掉。Loom suite 是独立的 advanced-verification job，失败会显示在 Actions job 日志中。固定 workflow 没有为该 job 设置 `timeout-minutes`；现有 Loom suite 入口也没有本仓可见的 10 分钟模型级 timeout。Linux runner 的 GNU `timeout` 可用性不构成需要改动的理由，因为现有入口已运行 Loom 模型且没有证据表明额外入口是必需的。

共享 workflow 会调用本仓 `.infra/bin/infra-tool.sh`，该脚本再经 `.infra/bootstrap.sh` 调用 rs-infra-tools 的 suite runner。本仓 bootstrap 元数据记录的 `rs-infra-tools` 基础源 revision 是 `00c7c1aa14ae103acf00dfb13bb06237ed585355`，但 bootstrap 在未配置共享根目录时还会解析该管理器仓库的 `main` 来构建工具。因此仅凭共享 workflow commit 不能声称远端本次运行使用了某个已观测的动态管理器版本；本核查不把本地执行冒充远端日志证据。

结论：固定共享 CI 已有 Loom suite 入口，且本仓 Loom 依赖会启用它，覆盖 Rust 1.94 工具链上的 `cfg(loom)` 模型测试。为避免重复入口，`.github/workflows/ci.yml` 保持不变；本次没有新增自定义 GNU timeout 或单独 job。

## 本地模型清单与执行证据

以下均在当前 Linux x86_64 workspace、Rust `1.94.0`、locked 依赖下执行。完整的 `--list` 输出共 11 个测试，按职责分组如下：

- Budget：
  - `local::outstanding_budget::tests::test_loom_failed_reservation_and_capacity_snapshot_bounds`
  - `local::outstanding_budget::tests::test_loom_production_budget_competing_owner_release`
  - `local::outstanding_budget::tests::test_loom_production_budget_racing_admission`
  - `local::outstanding_budget::tests::test_loom_production_budget_release_races_reuse`
- Operation gate：
  - `facade::event_bus::internal::operation_gate::tests::test_loom_production_gate_close_races_admission`
  - `facade::event_bus::internal::operation_gate::tests::test_loom_production_permit_drop_wakes_idle`
- Scheduler：
  - `facade::internal::delivery_scheduler_core::loom_tests::test_loom_scheduler_duplicate_release_races_handler_finish_and_reuse`
  - `facade::internal::delivery_scheduler_core::loom_tests::test_loom_scheduler_handler_release_notification_survives_concurrent_drain`
  - `facade::internal::delivery_scheduler_core::loom_tests::test_loom_scheduler_owned_release_notification_survives_concurrent_drain`
  - `facade::internal::delivery_scheduler_core::loom_tests::test_loom_scheduler_settlement_lane_release_wakes_without_handler_capacity`
  - `facade::internal::delivery_scheduler_core::loom_tests::test_loom_scheduler_stop_fences_handler_and_settlement_grants`

命令及退出码：

```text
RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg loom" cargo +1.94.0 test --locked --lib loom -- --list
exit 0; 11 tests listed, 0 benchmarks

RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg loom" cargo +1.94.0 test --locked --lib loom
exit 0; 11 passed, 0 failed, 0 ignored, 148 filtered out
```

本地环境可用 `/usr/bin/timeout`，版本为 GNU coreutils 9.4；这是环境检查，不代表共享 CI 入口已经配置该工具。以上模型执行是本地证据。当前没有本次修改触发的 GitHub Actions 运行，也没有远端新 workflow 日志，故不报告远端通过结果。
