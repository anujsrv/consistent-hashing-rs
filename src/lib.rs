use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Included, Unbounded};

use md5;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Node {
    name: String,
}

impl Node {
    pub fn new(name: String) -> Node {
        Node{
            name,
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }
}

#[derive(Clone)]
pub struct ConsistentHash {
    nodes: BTreeMap<Vec<u8>, Node>,
    replicas: HashMap<String, u32>,

    load_per_node: HashMap<String, f64>,
    load_factor: f64,
    total_load: u64,
}

impl ConsistentHash {
    pub fn new() -> ConsistentHash {
        ConsistentHash{
            load_per_node: HashMap::new(),
            load_factor: 0.0,
            nodes: BTreeMap::new(),
            replicas: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: &Node, num_replicas: u32) {
        let name: &String = node.get_name();

        self.load_per_node.insert(name.clone(), 0.0);
        self.replicas.insert(name.clone(), num_replicas);
        for replica in 0..num_replicas {
            let identifier: String = format!("{}-{}", name, replica);
            let hash: Vec<u8> = md5::compute(identifier).to_vec();

            self.nodes.insert(hash, node.clone());
        }
    }

    pub fn get_node(&self, name: String) -> Option<Node> {
        if self.nodes.is_empty() {
            return None;
        }
        let hash: Vec<u8> = md5::compute(name).to_vec();

        // using this since BTreeMap lower_bound has been marked as an experimental API currently.
        let lower_bound = self.nodes.range((Included(hash), Unbounded)).next();
        if let Some((_k, node)) = lower_bound {
            return Some(node.clone());
        }

        // if lower_bound points to the end of the map, that means we need to go
        // around to the first element
        let first_entry = self.nodes.first_key_value();
        if let Some((_k, node)) = first_entry {
            return Some(node.clone());
        }
        return None;
    }

    fn nearest_node_under_load(self, node: Node) -> Option<Node> {
        let mut count = 0;
        let mut curr_node = node.clone();
        loop {
            if count > self.size() {
                return None;
            }
            if self.check_load(curr_node.get_name().to_string()) {
                return Some(curr_node);
            }
            count += 1;
        }
    }

    // checks if the node is below the max allowed load value
    fn check_load(&self, node_name: String) -> bool {
        let tot_nodes = self.size();
        if tot_nodes == 0 {
            return false;
        }
        let avg_load: f64 = self.total_load as f64 / tot_nodes as f64;
        let max_allowed_load: f64 = (avg_load * self.load_factor).ceil();
        
        match self.load_per_node.get(&node_name) {
            None => false,
            Some(&val) => (val + 1.0) <= max_allowed_load,
        }
    }

    pub fn remove_node(& mut self, name: String) {
        if self.nodes.is_empty() {
            return;
        }
        let node_name = name.clone();
        let num_replicas = match self.replicas.get(&node_name) {
            None => return,
            Some(&val) => val
        };
        for replica in 0..num_replicas {
            let identifier: String = format!("{}-{}", name, replica);
            let hash: Vec<u8> = md5::compute(identifier).to_vec();

            self.nodes.remove(&hash);
        }

        self.replicas.remove(&name);
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    pub fn list_nodes(&self) -> Option<Vec<Node>> {
        if self.nodes.is_empty() {
            return None;
        }
        Some(self.nodes.values().cloned().collect::<Vec<_>>())
    }
}

impl Default for ConsistentHash {
    fn default() -> Self {
        Self::new()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn nodes_fixture(n: usize) -> Vec<Node> {
        let mut test_nodes: Vec<Node> = Vec::<Node>::new();
        for i in 0..n {
            test_nodes.push(Node::new(format!("test_node_{}", i)));
        }
        return test_nodes;
    }

    fn setup(nodes: Vec<Node>, replica_count: u32) -> ConsistentHash {
        let mut ch: ConsistentHash = ConsistentHash::new();

        for node in nodes.iter() {
            ch.add_node(&node, replica_count);
        }
        ch
    }

    #[test]
    fn add_nodes() {
        let nodes_count = 5;
        let test_nodes = nodes_fixture(nodes_count);
        let ch = setup(test_nodes, 3);

        let ch_size = ch.size();
        assert!(ch_size == nodes_count * 3, "count mismatch. expected: {}, actual: {}", nodes_count * 3, ch_size);    
    }

    #[test]
    fn remove_nodes() {
        let nodes_count = 7;
        let test_nodes = nodes_fixture(nodes_count);
        let mut ch = setup(test_nodes, 5);

        let mut ch_size = ch.size();
        assert!(ch_size == nodes_count * 5, "count mismatch after add_node. expected: {}, actual: {}", nodes_count * 3, ch_size);    

        ch.remove_node("non_existant".to_string());
        ch_size = ch.size();
        assert!(ch_size == nodes_count * 5, "count mismatch after remove_node on non_existant. expected: {}, actual: {}", nodes_count * 5, ch_size);

        ch.remove_node("test_node_3".to_string());
        ch_size = ch.size();
        assert!(ch_size == (nodes_count - 1) * 5, "count mismatch after remove_node. expected: {}, actual: {}", (nodes_count - 1) * 5, ch_size);
    }

    #[test]
    fn get_nodes() {
        let nodes_count = 7;
        let test_nodes = nodes_fixture(nodes_count);
        let mut ch = setup(test_nodes, 3);

        let mut matched_node = ch.get_node(String::from("test_node")).unwrap();
        assert_eq!(matched_node, Node::new(String::from("test_node_6")));

        ch.add_node(&Node::new(String::from("test_node_8")), 3);
        matched_node = ch.get_node(String::from("test_node")).unwrap();
        assert_eq!(matched_node, Node::new(String::from("test_node_6")));
    }
}
