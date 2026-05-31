//! Typed errors for the reconcile path.

/// Errors surfaced by reconcile steps. The controller's `error_policy` requeues
/// on any of these with backoff.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),

    #[error("failed to (de)serialize resource: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("failed to parse embedded helm values: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
