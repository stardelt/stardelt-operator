//! The customer's Prometheus (operator#607): the ServiceMonitors for the components that
//! expose metrics, and the condition that says why none were rendered.

use std::collections::BTreeMap;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, DynamicObject, ObjectMeta, TypeMeta};
use kube::core::{ApiResource, GroupVersionKind};
use kube::{Client, ResourceExt};
use serde_json::json;

use crate::controller::Context;
use crate::crd::{ComponentStatus, Condition, PlatformInstance};
use crate::objects;

/// The CRD that must exist before a `ServiceMonitor` can be applied.
pub const SERVICE_MONITOR_CRD: &str = "servicemonitors.monitoring.coreos.com";

/// How often Prometheus scrapes the object store.
pub const SCRAPE_INTERVAL: &str = "30s";

/// The `ServiceMonitor` resource (`monitoring.coreos.com/v1`).
pub fn service_monitor_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("monitoring.coreos.com", "v1", "ServiceMonitor"),
        "servicemonitors",
    )
}

/// One `ServiceMonitor` per object-store component that publishes a `metrics` port
/// (measured 2026-11-12: `master`, `filer`, `volume` and `s3` answer on 9327).
///
/// The selector deliberately omits `app.kubernetes.io/instance`: the umbrella release and
/// the stepwise install label the Services differently, while `name` and `component` agree
/// in both paths.
pub fn servicemonitors(namespace: &str, labels: &BTreeMap<String, String>) -> Vec<DynamicObject> {
    let mut object_labels = BTreeMap::from([(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    )]);
    object_labels.extend(labels.iter().map(|(k, v)| (k.clone(), v.clone())));

    ["master", "filer", "volume", "s3"]
        .iter()
        .map(|component| DynamicObject {
            types: Some(TypeMeta {
                api_version: "monitoring.coreos.com/v1".into(),
                kind: "ServiceMonitor".into(),
            }),
            metadata: ObjectMeta {
                name: Some(format!("seaweedfs-{component}")),
                namespace: Some(namespace.into()),
                labels: Some(object_labels.clone()),
                ..ObjectMeta::default()
            },
            data: json!({"spec": {
                "selector": {"matchLabels": {
                    "app.kubernetes.io/name": "seaweedfs",
                    "app.kubernetes.io/component": component,
                }},
                "endpoints": [{"port": "metrics", "path": "/metrics", "interval": SCRAPE_INTERVAL}],
            }}),
        })
        .collect()
}

/// Whether the cluster has the `ServiceMonitor` CRD. A missing CRD is a 404, which
/// `get_opt` reports as `Ok(None)`; anything else is treated as absent too, so a
/// reconciler never applies an object the API server would reject.
pub async fn crds_present(client: &Client) -> bool {
    match Api::<CustomResourceDefinition>::all(client.clone())
        .get_opt(SERVICE_MONITOR_CRD)
        .await
    {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(e) => {
            tracing::debug!(error = %e, "could not read the ServiceMonitor CRD; assuming absent");
            false
        }
    }
}

/// The condition that explains why nothing was rendered, or `None` when everything is
/// either rendered or deliberately off. Returning `None` is what removes a stale
/// condition: the status vector is replaced wholesale on every reconcile.
pub fn condition(
    enabled: bool,
    crds_present: bool,
    generation: Option<i64>,
    previous: Option<&Condition>,
    now: &str,
) -> Option<Condition> {
    if !enabled || crds_present {
        return None;
    }
    Some(Condition {
        type_: "MonitoringCRDsMissing".into(),
        status: "True".into(),
        reason: Some("CRDsMissing".into()),
        message: Some(format!(
            "spec.monitoring.enabled is true but {SERVICE_MONITOR_CRD} is not installed; nothing was rendered"
        )),
        last_transition_time: Some(match previous {
            Some(p) if p.status == "True" => p
                .last_transition_time
                .clone()
                .unwrap_or_else(|| now.to_string()),
            _ => now.to_string(),
        }),
        observed_generation: generation,
    })
}

/// `objects::own` is bounded to `DynamicType = ()`, which a `DynamicObject` (whose dynamic
/// type is its `ApiResource`) does not satisfy in kube 5, so the reference is stamped here.
/// Same contract: idempotent, never a duplicate.
fn own(object: &mut DynamicObject, owner: &OwnerReference) {
    let owners = object
        .metadata
        .owner_references
        .get_or_insert_with(Vec::new);
    if owners.iter().any(|existing| existing.uid == owner.uid) {
        return;
    }
    owners.push(owner.clone());
}

fn status(name: &str, ready: bool, message: impl Into<String>) -> ComponentStatus {
    ComponentStatus {
        name: name.into(),
        ready,
        message: Some(message.into()),
        applied_hash: None,
    }
}

/// Renders the object store's ServiceMonitors, or reports why it did not.
/// `None` when `spec.monitoring.enabled` is false: no object, no status row.
pub async fn ensure(
    ctx: &Context,
    instance: &PlatformInstance,
    owner: &OwnerReference,
    crds_present: bool,
) -> Option<ComponentStatus> {
    if !instance.spec.monitoring.enabled {
        return None;
    }
    if !crds_present {
        return Some(status(
            "monitoring",
            false,
            format!("MonitoringCRDsMissing: {SERVICE_MONITOR_CRD} is not installed"),
        ));
    }
    let objects_to_apply =
        servicemonitors(&instance.spec.namespace, &instance.spec.monitoring.labels);
    for mut object in objects_to_apply {
        own(&mut object, owner);
        let name = object.name_any();
        if let Err(e) = objects::apply_dynamic(
            &ctx.client,
            &instance.spec.namespace,
            &service_monitor_resource(),
            &object,
            instance.spec.dry_run,
        )
        .await
        {
            return Some(status("monitoring", false, format!("{name}: {e}")));
        }
    }
    if instance.spec.dry_run {
        Some(status(
            "monitoring",
            false,
            "planned (dry-run): 4 ServiceMonitors",
        ))
    } else {
        Some(status("monitoring", true, "applied: 4 ServiceMonitors"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_four_object_store_monitors_carry_their_selectors() {
        let monitors = servicemonitors(
            "stardelt",
            &BTreeMap::from([("release".to_string(), "kube-prometheus-stack".to_string())]),
        );
        assert_eq!(monitors.len(), 4);
        let components = ["master", "filer", "volume", "s3"];
        for (monitor, component) in monitors.iter().zip(components) {
            let metadata = &monitor.metadata;
            assert_eq!(
                metadata.name.as_deref(),
                Some(format!("seaweedfs-{component}").as_str())
            );
            assert_eq!(metadata.namespace.as_deref(), Some("stardelt"));
            assert_eq!(
                metadata.labels,
                Some(BTreeMap::from([
                    (
                        "app.kubernetes.io/part-of".to_string(),
                        "stardelt".to_string()
                    ),
                    ("release".to_string(), "kube-prometheus-stack".to_string()),
                ]))
            );
            assert_eq!(
                monitor.data["spec"]["selector"]["matchLabels"],
                json!({
                    "app.kubernetes.io/name": "seaweedfs",
                    "app.kubernetes.io/component": component,
                })
            );
            assert_eq!(
                monitor.data["spec"]["endpoints"],
                json!([{"port": "metrics", "path": "/metrics", "interval": "30s"}])
            );
            assert!(monitor.data["spec"].get("namespaceSelector").is_none());
        }
    }

    #[test]
    fn the_condition_names_only_missing_crds() {
        assert!(condition(false, false, Some(1), None, "t").is_none());
        assert!(condition(true, true, Some(1), None, "t").is_none());
        let fired = condition(true, false, Some(1), None, "t").expect("condition");
        assert_eq!(fired.type_, "MonitoringCRDsMissing");
        assert_eq!(fired.status, "True");
        assert_eq!(fired.reason.as_deref(), Some("CRDsMissing"));
        assert!(
            fired
                .message
                .as_deref()
                .is_some_and(|m| m.contains("servicemonitors.monitoring.coreos.com"))
        );
        assert_eq!(fired.last_transition_time.as_deref(), Some("t"));

        let previous = Condition {
            type_: "MonitoringCRDsMissing".into(),
            status: "True".into(),
            last_transition_time: Some("earlier".into()),
            ..Condition::default()
        };
        let kept = condition(true, false, Some(2), Some(&previous), "t").expect("condition");
        assert_eq!(kept.last_transition_time.as_deref(), Some("earlier"));
    }
}
