use std::collections::{BTreeMap, HashMap};

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

    load_per_node: HashMap<String, u64>,
    load_factor: f64,
    total_load: u64,
}

impl ConsistentHash {
    pub fn new() -> ConsistentHash {
        ConsistentHash{
            nodes: BTreeMap::new(),
            replicas: HashMap::new(),

            load_per_node: HashMap::new(),
            load_factor: 1.0,
            total_load: 0,
        }
    }

    pub fn with_load_factor(load_factor: f64) -> ConsistentHash {
        let mut ch = ConsistentHash::new();
        ch.load_factor = load_factor;
        return ch;
    }

    pub fn add_node(&mut self, node: &Node, num_replicas: u32) {
        let name: &String = node.get_name();
            let hash: Vec<u8> = md5::compute(name).to_vec();

            self.nodes.insert(hash, node.clone());

        self.load_per_node.insert(name.clone(), 0);
        self.replicas.insert(name.clone(), num_replicas);
        for replica in 1..num_replicas {
            let identifier: String = format!("{}-{}", name, replica);
            let hash: Vec<u8> = md5::compute(identifier).to_vec();

            self.nodes.insert(hash, node.clone());
        }
    }

    pub fn get_node(&self, key: String) -> Option<Node> {
        if self.nodes.is_empty() {
            return None;
        }
        self.nearest_node_under_load(key)
    }

    fn nearest_node_under_load(&self, key: String) -> Option<Node> {
        let hash: Vec<u8> = md5::compute(key).to_vec();
        // using this since BTreeMap lower_bound has been marked as an experimental API currently.
        let mut iter = self.nodes.range(hash..);
        let mut count = 0;
        loop {
            if count > self.size() {
                return None;
            }
            let curr_node: Node;
            if let Some((_k, node)) = iter.next() {
                curr_node = node.clone();
            } else {
                // initialize to the first node in the tree
                iter = self.nodes.range(vec![0]..);
                continue;
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
        let mut avg_load: f64 = self.total_load as f64 / tot_nodes as f64;
        if avg_load == 0.0 {
            avg_load = 1.0;
        }
        let max_allowed_load: u64 = (avg_load * self.load_factor).ceil() as u64;
        
        match self.load_per_node.get(&node_name) {
            None => false,
            Some(&val) => (val + 1) <= max_allowed_load,
        }
    }

    pub fn assign_key(&mut self, key: String) {
        if let Some(node) = self.get_node(key) {
            let node_name = node.get_name();
            let load = match self.load_per_node.get(node_name) {
                None => 0,
                Some(&val) => val,
            };
            self.load_per_node.insert(node_name.to_string(), load + 1);
            self.total_load += 1;
            return;
        }
        println!("ERR: no node available to be assigned")
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
        let hash: Vec<u8> = md5::compute(&node_name).to_vec();
        self.nodes.remove(&hash);
        for replica in 1..num_replicas {
            let identifier: String = format!("{}-{}", name, replica);
            let hash: Vec<u8> = md5::compute(identifier).to_vec();

            self.nodes.remove(&hash);
        }
        self.total_load -= self.load_per_node[&node_name];
        self.load_per_node.remove(&node_name);

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

    fn setup(nodes: Vec<Node>, replica_count: u32, load_factor: f64) -> ConsistentHash {
        let mut ch: ConsistentHash = ConsistentHash::with_load_factor(load_factor);

        for node in nodes.iter() {
            ch.add_node(&node, replica_count);
        }
        ch
    }

    #[test]
    fn add_nodes() {
        let nodes_count = 5;
        let test_nodes = nodes_fixture(nodes_count);
        let ch = setup(test_nodes, 3, 1.0);

        let ch_size = ch.size();
        assert!(ch_size == nodes_count * 3, "count mismatch. expected: {}, actual: {}", nodes_count * 3, ch_size);    
    }

    #[test]
    fn remove_nodes() {
        let nodes_count = 7;
        let test_nodes = nodes_fixture(nodes_count);
        let mut ch = setup(test_nodes, 5, 1.0);

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
        let mut ch = setup(test_nodes, 3, 1.0);

        let mut matched_node = ch.get_node(String::from("test_key1")).unwrap();
        assert_eq!(matched_node, Node::new(String::from("test_node_1")));

        ch.add_node(&Node::new(String::from("test_node_8")), 3);
        matched_node = ch.get_node(String::from("test_key1")).unwrap();
        assert_eq!(matched_node, Node::new(String::from("test_node_1")));
    }

    #[test]
    fn assign_key() {
        let nodes_count = 3;
        let test_nodes = nodes_fixture(nodes_count);
        let mut ch = setup(test_nodes, 0, 1.0);

        let matched_node = ch.get_node(String::from("test_key1")).unwrap();
        assert_eq!(matched_node, Node::new(String::from("test_node_1")));

        assert_eq!(ch.total_load, 0);

        ch.assign_key(String::from("test_key1"));
        assert_eq!(ch.total_load, 1);
        ch.assign_key(String::from("test_key2"));
        assert_eq!(ch.total_load, 2);
        ch.assign_key(String::from("test_key3"));
        assert_eq!(ch.total_load, 3);
        ch.assign_key(String::from("test_key4"));
        assert_eq!(ch.total_load, 3);
    }
}
