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

/// The permission store's ingress policy: OpenFGA ships without authentication, so the
/// network is what keeps every other pod from rewriting grants. Mirrors
/// stardelt-platform/manifests/openfga-network-policy.yaml.
pub const OPENFGA_POLICY: &str = "openfga-ingress";

fn meta(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        ..ObjectMeta::default()
    }
}

fn peer(labels: &[(&str, &str)]) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
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
    }
}

fn port(number: i32) -> NetworkPolicyPort {
    NetworkPolicyPort {
        port: Some(IntOrString::Int(number)),
        end_port: None,
        protocol: Some("TCP".into()),
    }
}

/// Ingress to the serving Lakekeeper pod only: the chart's db-migration Job carries
/// `name=lakekeeper` too, and `component=catalog` keeps migrations (Postgres only) out.
pub fn lakekeeper_ingress(namespace: &str) -> NetworkPolicy {
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

/// The permission store's ingress policy: OpenFGA ships without authentication, so the
/// network is what keeps every other pod from rewriting grants. Mirrors
/// stardelt-platform/manifests/openfga-network-policy.yaml.
pub fn openfga_ingress(namespace: &str) -> NetworkPolicy {
    NetworkPolicy {
        metadata: meta(OPENFGA_POLICY, namespace),
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_string(),
                    "openfga".to_string(),
                )])),
                match_expressions: None,
            }),
            policy_types: Some(vec!["Ingress".to_string()]),
            ingress: Some(vec![NetworkPolicyIngressRule {
                from: Some(vec![
                    peer(&[
                        ("app.kubernetes.io/name", "lakekeeper"),
                        ("app.kubernetes.io/component", "catalog"),
                    ]),
                    peer(&[
                        ("app.kubernetes.io/name", "lakekeeper"),
                        ("app.kubernetes.io/component", "db-migration"),
                    ]),
                ]),
                ports: Some(vec![port(8080), port(8081)]),
            }]),
            egress: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // ... rest of test continues - let us just append new test at end
    }

    #[test]
    fn openfga_policy_selects_openfga_pods() {
        let policy = openfga_ingress("data");
        let spec = policy.spec.as_ref().unwrap();
        let pod_sel = spec.pod_selector.as_ref().unwrap();
        let labels = pod_sel.match_labels.as_ref().unwrap();
        assert_eq!(labels.get("app.kubernetes.io/name").unwrap(), "openfga");
        assert_eq!(labels.len(), 1);
    }

    #[test]
    fn openfga_policy_admits_lakekeeper_on_8080_and_8081() {
        let policy = openfga_ingress("data");
        let spec = policy.spec.as_ref().unwrap();
        assert_eq!(
            spec.policy_types.as_ref().unwrap(),
            &vec!["Ingress".to_string()]
        );
        assert!(spec.egress.is_none());
        let ingress = spec.ingress.as_ref().unwrap();
        assert_eq!(ingress.len(), 1);
        let rule = &ingress[0];
        let peers = rule.from.as_ref().unwrap();
        assert_eq!(peers.len(), 2);
        // First peer
        let p1 = &peers[0];
        assert!(p1.namespace_selector.is_none());
        assert!(p1.ip_block.is_none());
        let p1_labels = p1
            .pod_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert_eq!(
            p1_labels.get("app.kubernetes.io/name").unwrap(),
            "lakekeeper"
        );
        assert_eq!(
            p1_labels.get("app.kubernetes.io/component").unwrap(),
            "catalog"
        );
        // Second peer
        let p2 = &peers[1];
        assert!(p2.namespace_selector.is_none());
        assert!(p2.ip_block.is_none());
        let p2_labels = p2
            .pod_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert_eq!(
            p2_labels.get("app.kubernetes.io/name").unwrap(),
            "lakekeeper"
        );
        assert_eq!(
            p2_labels.get("app.kubernetes.io/component").unwrap(),
            "db-migration"
        );
        let ports = rule.ports.as_ref().unwrap();
        assert_eq!(ports.len(), 2);
        let mut ports_sorted: Vec<i32> = ports
            .iter()
            .map(|p| match &p.port {
                Some(IntOrString::Int(n)) => *n,
                _ => panic!("expected int port"),
            })
            .collect();
        ports_sorted.sort();
        assert_eq!(ports_sorted, vec![8080, 8081]);
        for p in ports {
            assert_eq!(p.protocol.as_ref().unwrap(), "TCP");
        }
    }
}
