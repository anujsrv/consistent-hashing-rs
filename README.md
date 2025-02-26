![CI](https://img.shields.io/github/actions/workflow/status/anujsrv/consistent-hashing-rs/rust.yml)
# Consistent Hashing with bounded loads implementation in Rust

[Consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing) with bounded loads implementation in Rust.

Reference - https://arxiv.org/pdf/1608.01350

## Example usage

```rust
use consistenthash::{ConsistentHash, Node};

let mut ch = ConsistentHash::with_load_factor(1.25);
let replication_factor = 3;

ch.add_node(&Node::new(String::from("test_node1")), replication_factor);
ch.add_node(&Node::new(String::from("test_node2")), replication_factor);

ch.assign_key(String::from("key1"));
ch.assign_key(String::from("key2"));

println!("matched_node: {} for key: key1", ch.get_node(String::from("key1")).unwrap());
```
