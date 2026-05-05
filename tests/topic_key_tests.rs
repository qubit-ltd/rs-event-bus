use std::collections::HashSet;

use qubit_event_bus::Topic;

#[test]
fn test_topic_key_separates_same_name_with_different_payload_types() {
    let string_topic = Topic::<String>::try_new("same-name").expect("topic should build");
    let number_topic = Topic::<u32>::try_new("same-name").expect("topic should build");

    assert_ne!(string_topic.key(), number_topic.key());
}

#[test]
fn test_topic_key_can_be_used_in_hash_sets() {
    let topic = Topic::<String>::try_new("hash-key").expect("topic should build");
    let same = Topic::<String>::try_new("hash-key").expect("topic should build");
    let mut keys = HashSet::new();

    keys.insert(topic.key());

    assert!(keys.contains(&same.key()));
}
