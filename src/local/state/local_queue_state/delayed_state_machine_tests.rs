// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fixed logical-time scenarios for delayed queue state transitions.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use super::super::event::LocalEvent;
use super::LocalQueueState;
use crate::model::EventId;
use crate::model::Headers;
use crate::spi::OrderingKey;
use crate::spi::OutboundMessage;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

fn event(id: &str, key: Option<&str>, not_before: Option<Instant>, weight: usize) -> LocalEvent {
    let message = OutboundMessage::new(
        TopicAddress::new("delayed-state.test").expect("valid topic"),
        EventId::new(id).expect("valid event ID"),
        SystemTime::UNIX_EPOCH,
        Headers::default(),
        key.map(|value| OrderingKey::new(value).expect("valid ordering key")),
        None,
        TransportPayload::Native(Arc::new(1_u8)),
    );
    let message = message.with_native_payload_weight_bytes(NonZeroUsize::new(weight).expect("positive weight"));
    let mut event = LocalEvent::transport(message.topic().clone(), &message).expect("local event");
    event.not_before = not_before;
    event
}

fn pop_id(state: &mut LocalQueueState, now: Instant) -> Option<String> {
    state.pop_ready(now).map(|event| event.event_id().to_owned())
}

#[test]
fn test_delayed_head_is_unavailable_before_deadline_and_ready_at_deadline() {
    let base = Instant::now();
    let deadline = base
        .checked_add(Duration::from_secs(5))
        .expect("deadline within Instant range");
    let mut state = LocalQueueState::default();
    state.enqueue_back_at(event("deadline", Some("lane"), Some(deadline), 1), base);

    let before_deadline = deadline
        .checked_sub(Duration::from_nanos(1))
        .expect("deadline follows base");
    assert_eq!(None, pop_id(&mut state, before_deadline));
    assert_eq!(Some("deadline".to_owned()), pop_id(&mut state, deadline));
    assert_eq!(0, state.pending_count());
}

#[test]
fn test_delayed_lane_head_blocks_ready_successor_but_other_key_is_independent() {
    let base = Instant::now();
    let deadline = base
        .checked_add(Duration::from_secs(5))
        .expect("deadline within Instant range");
    let mut state = LocalQueueState::default();
    state.enqueue_back_at(event("blocked", Some("same"), Some(deadline), 1), base);
    state.enqueue_back_at(event("successor", Some("same"), None, 1), base);
    state.enqueue_back_at(event("independent", Some("other"), None, 1), base);

    assert_eq!(Some("independent".to_owned()), pop_id(&mut state, base));
    assert_eq!(None, pop_id(&mut state, base));
    assert_eq!(Some("blocked".to_owned()), pop_id(&mut state, deadline));
    assert_eq!(Some("successor".to_owned()), pop_id(&mut state, deadline));
}

#[test]
fn test_retry_restores_lane_front_and_clear_allows_key_reuse() {
    let base = Instant::now();
    let later = base
        .checked_add(Duration::from_secs(5))
        .expect("deadline within Instant range");
    let mut state = LocalQueueState::default();
    state.enqueue_back_at(event("first", Some("reused"), None, 1), base);
    state.enqueue_back_at(event("second", Some("reused"), None, 1), base);

    let mut retried = state.pop_ready(base).expect("first event is ready");
    assert_eq!("first", retried.event_id());
    retried.not_before = Some(later);
    state.enqueue_front_at(retried, base);
    assert_eq!(None, pop_id(&mut state, base));
    assert_eq!(Some("first".to_owned()), pop_id(&mut state, later));
    assert_eq!(Some("second".to_owned()), pop_id(&mut state, later));

    state.enqueue_back_at(event("discarded", Some("reused"), Some(later), 1), base);
    let discarded = state.clear_pending();
    assert_eq!(
        vec!["discarded"],
        discarded
            .iter()
            .map(|event| event.event_id().to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(0, state.pending_count());
    assert!(state.lanes.is_empty());

    state.enqueue_back_at(event("reused-key", Some("reused"), None, 1), base);
    assert_eq!(Some("reused-key".to_owned()), pop_id(&mut state, base));
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelEvent {
    id: u64,
    lane: Option<u8>,
    due_tick: u64,
    weight: usize,
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Enqueue {
        lane: Option<u8>,
        delay: u64,
        weight: usize,
    },
    Advance {
        ticks: u64,
    },
    Pop,
    Retry,
    Finish,
    Clear,
}

#[derive(Default)]
struct Model {
    pending: BTreeMap<Option<u8>, VecDeque<ModelEvent>>,
    held: HashMap<u64, ModelEvent>,
    finished: BTreeSet<u64>,
    cleared: BTreeSet<u64>,
    weights: HashMap<u64, usize>,
    next_id: u64,
    tick: u64,
    operation_counts: [usize; 6],
}

fn key_name(lane: Option<u8>) -> Option<String> {
    lane.map(|value| format!("lane-{value}"))
}

fn instant_at(base: Instant, tick: u64) -> Instant {
    base.checked_add(Duration::from_millis(tick))
        .expect("bounded logical time")
}

fn new_event(base: Instant, model_event: &ModelEvent) -> LocalEvent {
    let deadline = (model_event.due_tick > 0).then(|| instant_at(base, model_event.due_tick));
    event(
        &format!("state-{}", model_event.id),
        key_name(model_event.lane).as_deref(),
        deadline,
        model_event.weight,
    )
}

fn pending_events(model: &Model) -> Vec<ModelEvent> {
    model.pending.values().flat_map(|lane| lane.iter().cloned()).collect()
}

fn assert_invariants(
    state: &mut LocalQueueState,
    model: &Model,
    base: Instant,
    seed: u64,
    step: usize,
    prefix: &[String],
) {
    let context = format!("seed={seed} step={step} operations={prefix:?}");
    state.promote_due_heads(instant_at(base, model.tick));
    let expected_pending = pending_events(model);
    assert_eq!(state.pending_count, expected_pending.len(), "{context}");
    assert_eq!(
        state.pending_count,
        state.lanes.values().map(|lane| lane.events.len()).sum::<usize>(),
        "{context}"
    );

    let mut identities = BTreeSet::new();
    let mut actual_weight = 0;
    for event in state.lanes.values().flat_map(|lane| lane.events.iter()) {
        let id = event
            .event_id()
            .strip_prefix("state-")
            .expect("generated ID prefix")
            .parse::<u64>()
            .expect("numeric ID");
        assert!(identities.insert(id), "duplicate pending id {id}; {context}");
        actual_weight += event.weight_bytes;
    }
    let expected_ids: BTreeSet<_> = expected_pending.iter().map(|event| event.id).collect();
    assert_eq!(identities, expected_ids, "pending identity mismatch; {context}");
    assert_eq!(
        actual_weight,
        expected_pending.iter().map(|event| event.weight).sum::<usize>(),
        "pending weight mismatch; {context}"
    );

    let mut all = expected_ids;
    for id in model
        .held
        .keys()
        .chain(model.finished.iter())
        .chain(model.cleared.iter())
    {
        assert!(all.insert(*id), "identity appears in multiple buckets: {id}; {context}");
    }
    assert_eq!(all.len(), model.weights.len(), "identity conservation; {context}");
    assert_eq!(
        model.weights.values().sum::<usize>(),
        expected_pending.iter().map(|event| event.weight).sum::<usize>()
            + model.held.values().map(|event| event.weight).sum::<usize>()
            + model.finished.iter().map(|id| model.weights[id]).sum::<usize>()
            + model.cleared.iter().map(|id| model.weights[id]).sum::<usize>(),
        "total weight conservation; {context}"
    );

    let live_delayed = model
        .pending
        .values()
        .filter_map(|lane| lane.front())
        .filter(|event| event.due_tick > model.tick)
        .count();
    let expected_next_delay = model
        .pending
        .values()
        .filter_map(|lane| lane.front())
        .filter(|event| event.due_tick > model.tick)
        .map(|event| Duration::from_millis(event.due_tick - model.tick))
        .min();
    assert_eq!(
        state.next_ready_delay(instant_at(base, model.tick)),
        expected_next_delay,
        "next delayed head; {context}"
    );
    assert_eq!(state.delayed_live_count, live_delayed, "live delayed heads; {context}");
    assert_eq!(
        state.delayed_lanes.len(),
        state.delayed_live_count + state.delayed_stale_count,
        "heap accounting; {context}"
    );
    assert!(
        state.delayed_stale_count <= state.delayed_live_count.max(8),
        "stale bound at stable point; {context}"
    );
}

fn model_ready(model: &Model) -> bool {
    model
        .pending
        .values()
        .any(|lane| lane.front().is_some_and(|event| event.due_tick <= model.tick))
}

fn perform(
    state: &mut LocalQueueState,
    model: &mut Model,
    op: Operation,
    base: Instant,
    seed: u64,
    step: usize,
    prefix: &[String],
) {
    let now = instant_at(base, model.tick);
    model.operation_counts[match op {
        Operation::Enqueue { .. } => 0,
        Operation::Advance { .. } => 1,
        Operation::Pop => 2,
        Operation::Retry => 3,
        Operation::Finish => 4,
        Operation::Clear => 5,
    }] += 1;
    match op {
        Operation::Enqueue { lane, delay, weight } => {
            let item = ModelEvent {
                id: model.next_id,
                lane,
                due_tick: model.tick + delay,
                weight,
            };
            model.next_id += 1;
            model.weights.insert(item.id, weight);
            state.enqueue_back_at(new_event(base, &item), now);
            model.pending.entry(lane).or_default().push_back(item);
        }
        Operation::Advance { ticks } => model.tick = model.tick.saturating_add(ticks),
        Operation::Pop => {
            let actual = state.pop_ready(now);
            if let Some(actual) = actual {
                let id = actual
                    .event_id()
                    .strip_prefix("state-")
                    .expect("generated ID prefix")
                    .parse::<u64>()
                    .expect("numeric ID");
                let item = model
                    .pending
                    .get_mut(&model_lane(&actual))
                    .and_then(VecDeque::pop_front)
                    .expect("returned event is a pending lane head");
                assert_eq!(
                    item.id, id,
                    "returned event must be lane head; seed={seed} step={step} operations={prefix:?}"
                );
                assert!(
                    item.due_tick <= model.tick,
                    "returned event is due; seed={seed} step={step} operations={prefix:?}"
                );
                if model.pending.get(&item.lane).is_some_and(VecDeque::is_empty) {
                    model.pending.remove(&item.lane);
                }
                model.held.insert(item.id, item);
            } else {
                assert!(
                    !model_ready(model),
                    "None despite ready model lane; seed={seed} step={step} operations={prefix:?}"
                );
            }
        }
        Operation::Retry => {
            if let Some(id) = model.held.keys().next().copied() {
                let item = model.held.remove(&id).expect("selected held event");
                state.enqueue_front_at(new_event(base, &item), now);
                model.pending.entry(item.lane).or_default().push_front(item);
            }
        }
        Operation::Finish => {
            if let Some(id) = model.held.keys().next().copied() {
                let item = model.held.remove(&id).expect("selected held event");
                model.finished.insert(item.id);
            }
        }
        Operation::Clear => {
            let discarded = state.clear_pending();
            let actual_ids: BTreeSet<_> = discarded
                .iter()
                .map(|event| {
                    event
                        .event_id()
                        .strip_prefix("state-")
                        .expect("generated ID prefix")
                        .parse::<u64>()
                        .expect("numeric ID")
                })
                .collect();
            let expected_ids: BTreeSet<_> = pending_events(model).iter().map(|event| event.id).collect();
            assert_eq!(
                actual_ids, expected_ids,
                "clear returns pending identity set; seed={seed} step={step} operations={prefix:?}"
            );
            model.cleared.extend(expected_ids);
            model.pending.clear();
        }
    }
    assert_invariants(state, model, base, seed, step, prefix);
}

fn model_lane(event: &LocalEvent) -> Option<u8> {
    event.ordering_key.as_ref().map(|key| {
        key.as_str()
            .strip_prefix("lane-")
            .expect("generated lane key")
            .parse()
            .expect("numeric lane")
    })
}

#[derive(Clone, Copy)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

#[test]
fn test_generated_sequences_match_independent_delayed_queue_model() {
    for seed in 0..64_u64 {
        let base = Instant::now();
        let mut state = LocalQueueState::default();
        let mut model = Model::default();
        let mut rng = Rng(seed ^ 0xD1B5_4A32_D192_ED03);
        let mut prefix = Vec::new();
        for step in 0..256 {
            let held_count = model.held.len();
            let held = held_count > 0;
            let pending = pending_events(&model).len();
            let active = pending + held_count;
            let op = match step {
                0 => Operation::Enqueue {
                    lane: Some(0),
                    delay: 0,
                    weight: 1,
                },
                1 => Operation::Enqueue {
                    lane: Some(0),
                    delay: 100,
                    weight: 1,
                },
                2 => Operation::Enqueue {
                    lane: Some(1),
                    delay: 20,
                    weight: 1,
                },
                3..=20 if step % 2 == 1 => Operation::Pop,
                3..=20 => Operation::Retry,
                21 => Operation::Enqueue {
                    lane: None,
                    delay: 0,
                    weight: 3,
                },
                22 => Operation::Enqueue {
                    lane: Some(2),
                    delay: 7,
                    weight: 7,
                },
                _ => {
                    let choice = rng.next() % 100;
                    if active < 64 && (pending == 0 || choice < 24) {
                        Operation::Enqueue {
                            lane: if rng.next().is_multiple_of(9) {
                                None
                            } else {
                                Some((rng.next() % 7) as u8)
                            },
                            delay: rng.next() % 1001,
                            weight: (rng.next() % 64 + 1) as usize,
                        }
                    } else if choice < 40 {
                        Operation::Advance {
                            ticks: rng.next() % 64 + 1,
                        }
                    } else if choice < 57 || pending > 0 && !held {
                        Operation::Pop
                    } else if held && choice < 72 {
                        Operation::Retry
                    } else if held && choice < 80 {
                        Operation::Finish
                    } else {
                        Operation::Clear
                    }
                }
            };
            let description = format!("{op:?}");
            prefix.push(description);
            perform(&mut state, &mut model, op, base, seed, step, &prefix);
            if step == 20 {
                assert_eq!(state.delayed_stale_count, 0, "seed {seed} must compact stale heads");
                assert_eq!(state.delayed_live_count, 1, "seed {seed} must retain the guard head");
                assert_eq!(state.delayed_lanes.len(), 1, "seed {seed} heap is compacted");
            }
        }
        model.tick = model
            .pending
            .values()
            .flat_map(|lane| lane.iter())
            .map(|event| event.due_tick)
            .max()
            .unwrap_or(model.tick)
            .max(model.tick);
        while !model.pending.is_empty() {
            perform(
                &mut state,
                &mut model,
                Operation::Pop,
                base,
                seed,
                256 + prefix.len(),
                &prefix,
            );
            if !model.held.is_empty() {
                perform(
                    &mut state,
                    &mut model,
                    Operation::Finish,
                    base,
                    seed,
                    256 + prefix.len(),
                    &prefix,
                );
            }
        }
        let ids: Vec<_> = model.held.keys().copied().collect();
        for id in ids {
            let item = model.held.remove(&id).expect("held event");
            model.finished.insert(item.id);
        }
        assert!(model.operation_counts[1] > 0, "seed {seed} must advance logical time");
        assert!(model.operation_counts[3] > 0, "seed {seed} must retry a held event");
        assert!(model.operation_counts[5] > 0, "seed {seed} must clear pending events");
        assert_invariants(&mut state, &model, base, seed, 512, &prefix);
    }
}

#[test]
fn test_retrying_ready_heads_compacts_stale_delayed_entries_and_preserves_deadline() {
    let base = Instant::now();
    let guard_deadline = instant_at(base, 20);
    let deadline = instant_at(base, 100);
    let mut state = LocalQueueState::default();
    state.enqueue_back_at(event("compression-ready", Some("compression"), None, 1), base);
    state.enqueue_back_at(
        event("compression-delayed", Some("compression"), Some(deadline), 1),
        base,
    );
    state.enqueue_back_at(event("compression-guard", Some("guard"), Some(guard_deadline), 1), base);
    let mut saw_compaction = false;
    for _ in 0..16 {
        let ready = state.pop_ready(base).expect("ready head");
        if ready.event_id() == "compression-delayed" {
            panic!("delayed event became ready early");
        }
        state.enqueue_front_at(ready, base);
        if state.delayed_stale_count == 0 && state.delayed_live_count == 1 && state.delayed_lanes.len() == 1 {
            saw_compaction = true;
        }
    }
    assert!(saw_compaction, "scenario crosses stale-entry compaction threshold");
    assert_eq!(
        "compression-ready",
        state.pop_ready(base).expect("ready head").event_id()
    );
    assert_eq!(
        "compression-guard",
        state
            .pop_ready(guard_deadline)
            .expect("guard reaches its deadline")
            .event_id()
    );
    assert_eq!(
        "compression-delayed",
        state
            .pop_ready(deadline)
            .expect("delayed head reaches its deadline")
            .event_id()
    );
    assert!(state.is_pending_empty());
}
