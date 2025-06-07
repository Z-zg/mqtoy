use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: Uuid,
    pub address: String,
    pub is_leader: bool,
}

#[derive(Debug)]
pub struct Coordinator {
    nodes: Arc<RwLock<HashMap<Uuid, NodeInfo>>>,
    current_leader: Arc<RwLock<Option<Uuid>>>,
}

impl Coordinator {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            current_leader: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn register_node(&self, node_id: Uuid, address: String) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(
            node_id,
            NodeInfo {
                id: node_id,
                address,
                is_leader: false,
            },
        );
    }

    pub async fn elect_leader(&self) -> Option<Uuid> {
        let nodes = self.nodes.read().await;
        if nodes.is_empty() {
            return None;
        }

        // Simple leader election: choose the node with the lowest UUID
        let leader_id = nodes.keys().min().cloned()?;
        
        let mut current_leader = self.current_leader.write().await;
        *current_leader = Some(leader_id);
        
        Some(leader_id)
    }

    pub async fn get_leader(&self) -> Option<NodeInfo> {
        let current_leader = self.current_leader.read().await;
        let leader_id = current_leader.as_ref()?;
        let nodes = self.nodes.read().await;
        nodes.get(leader_id).cloned()
    }

    pub async fn get_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[test]
    async fn test_coordinator_basic() {
        let coordinator = Coordinator::new();
        let node_id = Uuid::new_v4();
        let address = "127.0.0.1:50051".to_string();

        // Test node registration
        coordinator.register_node(node_id, address.clone()).await;
        let nodes = coordinator.get_nodes().await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, node_id);
        assert_eq!(nodes[0].address, address);
        assert!(!nodes[0].is_leader);

        // Test leader election
        let leader_id = coordinator.elect_leader().await.unwrap();
        assert_eq!(leader_id, node_id);

        // Test getting leader
        let leader = coordinator.get_leader().await.unwrap();
        assert_eq!(leader.id, node_id);
        assert_eq!(leader.address, address);
        assert!(leader.is_leader);
    }

    #[test]
    async fn test_multiple_nodes() {
        let coordinator = Coordinator::new();
        let node1_id = Uuid::new_v4();
        let node2_id = Uuid::new_v4();
        let address = "127.0.0.1:50051".to_string();

        // Register two nodes
        coordinator.register_node(node1_id, address.clone()).await;
        coordinator.register_node(node2_id, address.clone()).await;

        // Test leader election with multiple nodes
        let leader_id = coordinator.elect_leader().await.unwrap();
        assert_eq!(leader_id, std::cmp::min(node1_id, node2_id));

        let nodes = coordinator.get_nodes().await;
        assert_eq!(nodes.len(), 2);
    }
} 