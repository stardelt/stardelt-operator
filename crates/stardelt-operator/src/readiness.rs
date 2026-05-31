//! Status-condition construction and readiness predicates.
//!
//! We define our own [`Condition`] rather than reuse
//! `k8s_openapi::...meta::v1::Condition` because the latter does not implement
//! `schemars::JsonSchema`, which the CRD derive requires. The shape is identical
//! to `metav1.Condition`, so `kubectl wait --for=condition=Ready` and generic
//! tooling work unchanged.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A standard Kubernetes status condition.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    pub last_transition_time: String,
}

/// Build a condition stamped with the current time (RFC3339).
pub fn condition(type_: &str, status: bool, reason: &str, message: impl Into<String>) -> Condition {
    Condition {
        type_: type_.to_string(),
        status: if status { "True" } else { "False" }.to_string(),
        reason: reason.to_string(),
        message: message.into(),
        last_transition_time: now_rfc3339(),
    }
}

/// Upsert a condition into a list by `type`, replacing any existing entry.
/// Preserves the original `lastTransitionTime` when the status is unchanged.
pub fn upsert(conditions: &mut Vec<Condition>, cond: Condition) {
    if let Some(existing) = conditions.iter_mut().find(|c| c.type_ == cond.type_) {
        if existing.status == cond.status {
            let kept = existing.last_transition_time.clone();
            *existing = cond;
            existing.last_transition_time = kept;
        } else {
            *existing = cond;
        }
    } else {
        conditions.push(cond);
    }
}

/// True when the object carries `status.conditions[type=Ready].status == "True"`.
/// Used for Flux `HelmRelease` and CNPG `Cluster`, which both expose this shape.
pub fn ready_condition_true(obj: &serde_json::Value) -> bool {
    obj.get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .map(|conds| {
            conds.iter().any(|c| {
                c.get("type").and_then(serde_json::Value::as_str) == Some("Ready")
                    && c.get("status").and_then(serde_json::Value::as_str) == Some("True")
            })
        })
        .unwrap_or(false)
}

fn now_rfc3339() -> String {
    k8s_openapi::chrono::Utc::now().to_rfc3339()
}
