use karlsen_core::info;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Represents a Proof-of-Useful-Work (Pouw) task submitted by a client.
#[derive(Clone)]
pub struct PouwTask {
    /// Unique identifier of the task.
    pub id: String,
    /// Subnet identifier (e.g., used to route tasks to specific worker groups).
    pub subnet: String,
    /// Encrypted request payload (can be plaintext, JSON, XML, etc.).
    pub encrypted_request: String,
    /// Optional encrypted response payload (set once a miner computes the result).
    pub encrypted_response: Option<String>,
}

/// Core manager that holds and handles all submitted Pouw tasks.
#[derive(Clone)]
pub struct PouwManager {
    inner: Arc<Mutex<HashMap<String, PouwTask>>>,
}

impl PouwManager {
    /// Creates a new Pouw manager instance.
    pub fn new() -> Self {
        info!("[PoUW] Task manager Started succefully");
        Self { inner: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Submits a new Pouw task to the node.
    ///
    /// # Arguments
    /// * `subnet` - The target subnet to assign the task to.
    /// * `encrypted_request` - The encrypted payload of the task.
    ///
    /// # Returns
    /// A unique task ID.
    pub async fn submit_task(&self, subnet: String, encrypted_request: String) -> String {
        let id = Uuid::new_v4().to_string();
        let task = PouwTask { id: id.clone(), subnet, encrypted_request, encrypted_response: None };
        self.inner.lock().await.insert(id.clone(), task);
        id
    }

    /// Retrieves the first available (unsolved) task for a given subnet.
    ///
    /// # Arguments
    /// * `subnet` - The subnet from which to fetch a task.
    ///
    /// # Returns
    /// An optional task ready to be processed by a miner.
    pub async fn get_task(&self, subnet: String) -> Option<PouwTask> {
        self.inner.lock().await.values().find(|t| t.subnet == subnet && t.encrypted_response.is_none()).cloned()
    }

    /// Submits the result of a task by a miner.
    ///
    /// # Arguments
    /// * `id` - The unique task ID.
    /// * `encrypted_response` - The computed response in encrypted form.
    ///
    /// # Returns
    /// `true` if the result was accepted, `false` otherwise.
    pub async fn submit_result(&self, id: String, encrypted_response: String) -> bool {
        let mut tasks = self.inner.lock().await;
        if let Some(task) = tasks.get_mut(&id) {
            if task.encrypted_response.is_none() {
                task.encrypted_response = Some(encrypted_response);
                return true;
            }
        }
        false
    }

    /// Retrieves the encrypted result of a completed task.
    ///
    /// # Arguments
    /// * `id` - The unique task ID.
    ///
    /// # Returns
    /// An optional result payload.
    pub async fn get_result(&self, id: String) -> Option<String> {
        self.inner.lock().await.get(&id).and_then(|t| t.encrypted_response.clone())
    }
}

/// Async proxy for the Pouw manager, providing an ergonomic interface to external services (e.g., RPC or gRPC).
#[derive(Clone)]
pub struct PouwManagerProxy {
    inner: Arc<PouwManager>,
}

impl PouwManagerProxy {
    /// Creates a new proxy around a PouwManager instance.
    pub fn new(inner: Arc<PouwManager>) -> Self {
        Self { inner }
    }

    /// Submits a task via the proxy interface.
    pub async fn submit_task(&self, subnet: String, data: String) -> String {
        self.inner.submit_task(subnet, data).await
    }

    /// Gets a task available for a specific subnet.
    pub async fn get_task(&self, subnet: String) -> Option<PouwTask> {
        self.inner.get_task(subnet).await
    }

    /// Submits a computed result for a given task.
    pub async fn submit_result(&self, id: String, data: String) -> bool {
        self.inner.submit_result(id, data).await
    }

    /// Retrieves the result of a task.
    pub async fn get_result(&self, id: String) -> Option<String> {
        self.inner.get_result(id).await
    }
}
