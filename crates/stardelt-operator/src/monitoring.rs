//! The customer's Prometheus (operator#607): the ServiceMonitors and PodMonitors for the
//! components that expose metrics, the starter alerts, and the condition that says why none
//! were rendered.

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

/// How often Prometheus scrapes the platform's endpoints.
pub const SCRAPE_INTERVAL: &str = "30s";

/// The four CloudNative-PG clusters the platform installs. They are applied by two
/// controllers, so this list is deliberately static: a PodMonitor for a cluster that is not
/// installed selects no pod and costs nothing.
pub const CNPG_CLUSTERS: [&str; 4] = ["lakekeeper-pg", "openfga-pg", "keycloak-pg", "nova-pg"];

/// The `ServiceMonitor` resource (`monitoring.coreos.com/v1`).
pub fn service_monitor_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("monitoring.coreos.com", "v1", "ServiceMonitor"),
        "servicemonitors",
    )
}

/// The `PodMonitor` resource (`monitoring.coreos.com/v1`).
pub fn pod_monitor_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("monitoring.coreos.com", "v1", "PodMonitor"),
        "podmonitors",
    )
}

/// The `PrometheusRule` resource (`monitoring.coreos.com/v1`).
pub fn prometheus_rule_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("monitoring.coreos.com", "v1", "PrometheusRule"),
        "prometheusrules",
    )
}

/// The labels every rendered object carries: `app.kubernetes.io/part-of: stardelt` first,
/// then the customer's labels, which win.
fn object_labels(labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut labels = labels.clone();
    labels.insert(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    );
    labels
}

/// One `ServiceMonitor` per object-store component that publishes a `metrics` port
/// (measured 2026-11-12: `master`, `filer`, `volume` and `s3` answer on 9327).
///
/// The selector deliberately omits `app.kubernetes.io/instance`: the umbrella release and
/// the stepwise install label the Services differently, while `name` and `component` agree
/// in both paths.
pub fn servicemonitors(namespace: &str, labels: &BTreeMap<String, String>) -> Vec<DynamicObject> {
    let labels = object_labels(labels);
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
                labels: Some(labels.clone()),
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

/// One `PodMonitor` per CloudNative-PG cluster: the instance manager serves Prometheus text
/// on the pod's `metrics` port (9187, measured 2026-11-12) and its pod carries
/// `cnpg.io/cluster: <name>`. CNPG has no Service for that port, so a ServiceMonitor cannot
/// select it.
pub fn podmonitors(namespace: &str, labels: &BTreeMap<String, String>) -> Vec<DynamicObject> {
    let labels = object_labels(labels);
    CNPG_CLUSTERS
        .iter()
        .map(|cluster| DynamicObject {
            types: Some(TypeMeta {
                api_version: "monitoring.coreos.com/v1".into(),
                kind: "PodMonitor".into(),
            }),
            metadata: ObjectMeta {
                name: Some(format!("cnpg-{cluster}")),
                namespace: Some(namespace.into()),
                labels: Some(labels.clone()),
                ..ObjectMeta::default()
            },
            data: json!({"spec": {
                "selector": {"matchLabels": {"cnpg.io/cluster": cluster}},
                "podMetricsEndpoints": [{
                    "port": "metrics",
                    "path": "/metrics",
                    "interval": SCRAPE_INTERVAL,
                }],
            }}),
        })
        .collect()
}

/// The starter alerts (#607 item 5). Every expression uses a series measured on the lab on
/// 2026-11-12; an alert whose series does not exist stays inactive instead of failing.
/// `runbook_url` points at the operations page that documents the endpoints.
pub fn prometheus_rule(namespace: &str, labels: &BTreeMap<String, String>) -> DynamicObject {
    let labels = object_labels(labels);
    let annotation = |summary: &str, description: &str| {
        json!({
            "summary": summary,
            "description": description,
            "runbook_url": "https://stardelt.io/docs/operations/monitoring/",
        })
    };
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "monitoring.coreos.com/v1".into(),
            kind: "PrometheusRule".into(),
        }),
        metadata: ObjectMeta {
            name: Some("stardelt-alerts".into()),
            namespace: Some(namespace.into()),
            labels: Some(labels),
            ..ObjectMeta::default()
        },
        data: json!({"spec": {"groups": [{
            "name": "stardelt",
            "rules": [
                {
                    "alert": "StardeltMetricsTargetDown",
                    "expr": format!(
                        "up{{namespace=\"{namespace}\"}} == 0"
                    ),
                    "for": "10m",
                    "labels": {"severity": "warning"},
                    "annotations": annotation(
                        "A platform metrics endpoint is not answering",
                        "Prometheus has failed to scrape a stardelt target for 10 minutes."
                    ),
                },
                {
                    "alert": "StardeltOperatorNoLeader",
                    "expr": format!(
                        "max by (namespace) (stardelt_operator_leader{{namespace=\"{namespace}\"}}) == 0"
                    ),
                    "for": "10m",
                    "labels": {"severity": "critical"},
                    "annotations": annotation(
                        "The operator has no leader",
                        "Every operator replica reports stardelt_operator_leader 0, so nothing reconciles."
                    ),
                },
                {
                    "alert": "StardeltOperatorReconcileErrors",
                    "expr": format!(
                        "increase(stardelt_operator_reconcile_errors_total{{namespace=\"{namespace}\"}}[15m]) > 3"
                    ),
                    "for": "5m",
                    "labels": {"severity": "warning"},
                    "annotations": annotation(
                        "The operator is failing reconciles",
                        "More than three reconciles failed in the last 15 minutes; read the operator log."
                    ),
                },
                {
                    "alert": "StardeltSeaweedFSMasterNotLeader",
                    "expr": format!(
                        "SeaweedFS_master_is_leader{{namespace=\"{namespace}\"}} == 0"
                    ),
                    "for": "5m",
                    "labels": {"severity": "critical"},
                    "annotations": annotation(
                        "The object store has no master",
                        "No SeaweedFS master reports leadership; writes and reads will fail."
                    ),
                },
                {
                    "alert": "StardeltCnpgCollectorDown",
                    "expr": format!(
                        "cnpg_collector_up{{namespace=\"{namespace}\"}} == 0"
                    ),
                    "for": "5m",
                    "labels": {"severity": "critical"},
                    "annotations": annotation(
                        "A platform database stopped exporting metrics",
                        "The CloudNative-PG instance manager of a cluster is down, so its database health is unknown."
                    ),
                },
                {
                    "alert": "StardeltCnpgClientsWaiting",
                    "expr": format!(
                        "cnpg_backends_waiting_total{{namespace=\"{namespace}\"}} > 5"
                    ),
                    "for": "10m",
                    "labels": {"severity": "warning"},
                    "annotations": annotation(
                        "A platform database has clients waiting",
                        "More than five backends have waited for a lock or a connection for 10 minutes."
                    ),
                },
            ],
        }]}}),
    }
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

/// Renders the metrics objects, or reports why it did not. `None` when
/// `spec.monitoring.enabled` is false: no object, no status row.
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
    let namespace = &instance.spec.namespace;
    let labels = &instance.spec.monitoring.labels;
    let mut rendered: Vec<(ApiResource, DynamicObject)> = Vec::new();
    for object in servicemonitors(namespace, labels) {
        rendered.push((service_monitor_resource(), object));
    }
    for object in podmonitors(namespace, labels) {
        rendered.push((pod_monitor_resource(), object));
    }
    rendered.push((
        prometheus_rule_resource(),
        prometheus_rule(namespace, labels),
    ));

    for (resource, mut object) in rendered {
        own(&mut object, owner);
        let name = object.name_any();
        if let Err(e) = objects::apply_dynamic(
            &ctx.client,
            namespace,
            &resource,
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
            "planned (dry-run): 4 ServiceMonitors, 4 PodMonitors, 1 PrometheusRule",
        ))
    } else {
        Some(status(
            "monitoring",
            true,
            "applied: 4 ServiceMonitors, 4 PodMonitors, 1 PrometheusRule",
        ))
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
    fn the_four_cnpg_clusters_get_a_pod_monitor() {
        let monitors = podmonitors("stardelt", &BTreeMap::new());
        assert_eq!(monitors.len(), 4);
        for (monitor, cluster) in monitors.iter().zip(CNPG_CLUSTERS) {
            assert_eq!(
                monitor.metadata.name.as_deref(),
                Some(format!("cnpg-{cluster}").as_str())
            );
            assert_eq!(monitor.metadata.namespace.as_deref(), Some("stardelt"));
            assert_eq!(
                monitor.data["spec"]["selector"]["matchLabels"],
                json!({"cnpg.io/cluster": cluster})
            );
            assert_eq!(
                monitor.data["spec"]["podMetricsEndpoints"],
                json!([{"port": "metrics", "path": "/metrics", "interval": "30s"}])
            );
            assert!(monitor.data["spec"].get("namespaceSelector").is_none());
        }
    }

    #[test]
    fn the_starter_rule_names_every_alert_and_its_namespace() {
        let rule = prometheus_rule("stardelt", &BTreeMap::new());
        assert_eq!(rule.metadata.name.as_deref(), Some("stardelt-alerts"));
        assert_eq!(rule.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(rule.data["spec"]["groups"][0]["name"], "stardelt");
        let rules = rule.data["spec"]["groups"][0]["rules"]
            .as_array()
            .expect("rules");
        assert_eq!(rules.len(), 6);
        let alerts: Vec<&str> = rules
            .iter()
            .filter_map(|rule| rule["alert"].as_str())
            .collect();
        for expected in [
            "StardeltMetricsTargetDown",
            "StardeltOperatorNoLeader",
            "StardeltOperatorReconcileErrors",
            "StardeltSeaweedFSMasterNotLeader",
            "StardeltCnpgCollectorDown",
            "StardeltCnpgClientsWaiting",
        ] {
            assert!(alerts.contains(&expected), "missing {expected}");
        }
        for rule in rules {
            let expr = rule["expr"].as_str().expect("expr");
            assert!(
                expr.contains("namespace=\"stardelt\""),
                "expression names its namespace: {expr}"
            );
            assert!(rule["for"].is_string());
            assert_eq!(
                rule["annotations"]["runbook_url"],
                "https://stardelt.io/docs/operations/monitoring/"
            );
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
