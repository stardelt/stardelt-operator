//! The catalog's ingress policy (`design/operator-parity.md`, row `lakekeeper`): mirrors
//! stardelt-platform/manifests/lakekeeper-network-policy.yaml. Defence in depth — Lakekeeper
//! and the OPA bridge decide *what* a caller may do; this decides *who may ask at all*.

use std::collections::BTreeMap;

use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicyPort,
    NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::ObjectMeta;

/// Name of the policy; the same string the platform uses, so a namespace cut over from
/// `make install` keeps one object.
pub const LAKEKEEPER_POLICY: &str = "lakekeeper-ingress";
/// The bootstrap Job's pod label (step 3): the Job's name is hashed per object store, so the
/// Job controller's `batch.kubernetes.io/job-name` label cannot be selected here.
pub const BOOTSTRAP_POD_LABEL: &str = "app.kubernetes.io/name";
pub const BOOTSTRAP_POD_VALUE: &str = "lakekeeper-bootstrap";

fn meta(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        ..ObjectMeta::default()
    }
}

/// Ingress to the serving Lakekeeper pod only: the chart's db-migration Job carries
/// `name=lakekeeper` too, and `component=catalog` keeps migrations (Postgres only) out.
pub fn lakekeeper_ingress(namespace: &str) -> NetworkPolicy {
    let peer = |labels: &[(&str, &str)]| NetworkPolicyPeer {
        pod_selector: Some(LabelSelector {
            match_labels: Some(BTreeMap::from_iter(
                labels
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
            )),
            match_expressions: None,
        }),
        namespace_selector: None,
        ip_block: None::<IPBlock>,
    };
    let port = |number: i32| NetworkPolicyPort {
        port: Some(IntOrString::Int(number)),
        end_port: None,
        protocol: Some("TCP".into()),
    };
    NetworkPolicy {
        metadata: meta(LAKEKEEPER_POLICY, namespace),
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([
                    (
                        "app.kubernetes.io/name".to_string(),
                        "lakekeeper".to_string(),
                    ),
                    (
                        "app.kubernetes.io/component".to_string(),
                        "catalog".to_string(),
                    ),
                ])),
                match_expressions: None,
            }),
            policy_types: Some(vec!["Ingress".to_string()]),
            ingress: Some(vec![
                NetworkPolicyIngressRule {
                    from: Some(vec![
                        peer(&[
                            ("app.kubernetes.io/name", "trino"),
                            ("app.kubernetes.io/component", "coordinator"),
                        ]),
                        peer(&[("app.kubernetes.io/name", "nova")]),
                        peer(&[("app.kubernetes.io/name", "spark-connect")]),
                        peer(&[("release", "airflow"), ("component", "worker")]),
                        peer(&[(BOOTSTRAP_POD_LABEL, BOOTSTRAP_POD_VALUE)]),
                        peer(&[("app.kubernetes.io/name", "access-sync")]),
                    ]),
                    ports: Some(vec![port(8181)]),
                },
                NetworkPolicyIngressRule {
                    // The OPA bridge's data API: the coordinator asks, the access sync writes.
                    from: Some(vec![
                        peer(&[
                            ("app.kubernetes.io/name", "trino"),
                            ("app.kubernetes.io/component", "coordinator"),
                        ]),
                        peer(&[("app.kubernetes.io/name", "access-sync")]),
                    ]),
                    ports: Some(vec![port(8282)]),
                },
            ]),
            egress: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prerequisites;

    #[test]
    fn the_policy_admits_the_six_catalog_callers_on_8181() {
        let policy = lakekeeper_ingress("data");
        let spec = policy.spec.as_ref().unwrap();
        // pod selector
        let pod_sel = spec.pod_selector.as_ref().unwrap();
        let labels = pod_sel.match_labels.as_ref().unwrap();
        let mut v: Vec<(String, String)> =
            labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        v.sort();
        assert_eq!(
            v,
            vec![
                (
                    "app.kubernetes.io/component".to_string(),
                    "catalog".to_string()
                ),
                (
                    "app.kubernetes.io/name".to_string(),
                    "lakekeeper".to_string()
                ),
            ]
        );
        assert_eq!(
            spec.policy_types.as_ref().unwrap(),
            &vec!["Ingress".to_string()]
        );
        let ingress = spec.ingress.as_ref().unwrap();
        let expected_port_8181 = NetworkPolicyPort {
            port: Some(IntOrString::Int(8181)),
            end_port: None,
            protocol: Some("TCP".into()),
        };
        assert_eq!(
            ingress[0].ports.as_ref().unwrap(),
            &vec![expected_port_8181]
        );
        // six peers
        let from = ingress[0].from.as_ref().unwrap();
        assert_eq!(from.len(), 6);
        let mut peers: Vec<Vec<(String, String)>> = from
            .iter()
            .map(|p| {
                let mut pl: Vec<(String, String)> = p
                    .pod_selector
                    .as_ref()
                    .unwrap()
                    .match_labels
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                pl.sort();
                pl
            })
            .collect();
        peers.sort();
        assert_eq!(
            peers,
            vec![
                vec![
                    (
                        "app.kubernetes.io/component".to_string(),
                        "coordinator".to_string()
                    ),
                    ("app.kubernetes.io/name".to_string(), "trino".to_string()),
                ],
                vec![(
                    "app.kubernetes.io/name".to_string(),
                    "access-sync".to_string()
                )],
                vec![(
                    "app.kubernetes.io/name".to_string(),
                    "lakekeeper-bootstrap".to_string()
                )],
                vec![("app.kubernetes.io/name".to_string(), "nova".to_string())],
                vec![(
                    "app.kubernetes.io/name".to_string(),
                    "spark-connect".to_string()
                )],
                vec![
                    ("component".to_string(), "worker".to_string()),
                    ("release".to_string(), "airflow".to_string()),
                ],
            ]
        );
    }

    #[test]
    fn the_bridge_port_admits_only_the_coordinator_and_the_sync() {
        let policy = lakekeeper_ingress("data");
        let spec = policy.spec.as_ref().unwrap();
        let ingress = spec.ingress.as_ref().unwrap();
        let expected_port_8282 = NetworkPolicyPort {
            port: Some(IntOrString::Int(8282)),
            end_port: None,
            protocol: Some("TCP".into()),
        };
        assert_eq!(
            ingress[1].ports.as_ref().unwrap(),
            &vec![expected_port_8282]
        );
        let from = ingress[1].from.as_ref().unwrap();
        assert_eq!(from.len(), 2);
    }

    #[test]
    fn the_policy_has_no_namespace_selector_and_no_ip_block() {
        let policy = lakekeeper_ingress("data");
        let spec = policy.spec.as_ref().unwrap();
        for rule in spec.ingress.as_ref().unwrap() {
            for peer in rule.from.as_ref().unwrap() {
                assert!(peer.namespace_selector.is_none());
                assert!(peer.ip_block.is_none());
            }
        }
    }

    #[test]
    fn the_bootstrap_job_pod_carries_the_label_the_policy_selects() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

        let owner = OwnerReference {
            api_version: "stardelt.io/v1alpha1".into(),
            kind: "Lakehouse".into(),
            name: "main".into(),
            uid: "u-1".into(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        };
        let job = prerequisites::bootstrap_job("main", "data", "lakekeeper-bootstrap-1", owner);
        let labels = job
            .spec
            .as_ref()
            .unwrap()
            .template
            .metadata
            .as_ref()
            .unwrap()
            .labels
            .as_ref()
            .unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/name").unwrap(),
            "lakekeeper-bootstrap"
        );
        // and the policy contains a peer with exactly that pair
        let policy = lakekeeper_ingress("data");
        let ingress = policy.spec.as_ref().unwrap().ingress.as_ref().unwrap();
        let mut found = false;
        for peer in ingress[0].from.as_ref().unwrap() {
            let pl = peer
                .pod_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap();
            if pl.len() == 1
                && pl.get("app.kubernetes.io/name").map(|s| s.as_str())
                    == Some("lakekeeper-bootstrap")
            {
                found = true;
                break;
            }
        }
        assert!(found);
    }
}
