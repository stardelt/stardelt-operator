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
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        labels: Some(BTreeMap::from([
            (
                "app.kubernetes.io/part-of".to_string(),
                "stardelt".to_string(),
            ),
            ("stardelt.io/component".to_string(), "catalog".to_string()),
        ])),
        ..Default::default()
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

    fn peer_labels(p: &NetworkPolicyPeer) -> Vec<(String, String)> {
        let sel = p.pod_selector.as_ref().unwrap();
        let mut v: Vec<(String, String)> = sel
            .match_labels
            .as_ref()
            .unwrap()
            .iter()
            .map(|(k, val)| (k.clone(), val.clone()))
            .collect();
        v.sort();
        v
    }

    #[test]
    fn the_policy_admits_the_six_catalog_callers_on_8181() {
        let pol = lakekeeper_ingress("data");
        let spec = pol.spec.as_ref().unwrap();
        assert_eq!(
            spec.pod_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap(),
            &BTreeMap::from([
                (
                    "app.kubernetes.io/name".to_string(),
                    "lakekeeper".to_string()
                ),
                (
                    "app.kubernetes.io/component".to_string(),
                    "catalog".to_string()
                ),
            ])
        );
        assert_eq!(
            spec.policy_types.as_ref().unwrap(),
            &vec!["Ingress".to_string()]
        );
        let ports = spec.ingress.as_ref().unwrap()[0].ports.as_ref().unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].port, Some(IntOrString::Int(8181)));
        assert_eq!(ports[0].protocol.as_deref(), Some("TCP"));
        let peers = spec.ingress.as_ref().unwrap()[0].from.as_ref().unwrap();
        assert_eq!(peers.len(), 6);
        let got: Vec<Vec<(String, String)>> = peers.iter().map(peer_labels).collect();
        let mut expected = vec![
            vec![
                (
                    "app.kubernetes.io/component".to_string(),
                    "coordinator".to_string(),
                ),
                ("app.kubernetes.io/name".to_string(), "trino".to_string()),
            ],
            vec![("app.kubernetes.io/name".to_string(), "nova".to_string())],
            vec![(
                "app.kubernetes.io/name".to_string(),
                "spark-connect".to_string(),
            )],
            vec![
                ("component".to_string(), "worker".to_string()),
                ("release".to_string(), "airflow".to_string()),
            ],
            vec![(
                BOOTSTRAP_POD_LABEL.to_string(),
                BOOTSTRAP_POD_VALUE.to_string(),
            )],
            vec![(
                "app.kubernetes.io/name".to_string(),
                "access-sync".to_string(),
            )],
        ];
        for e in expected.iter_mut() {
            e.sort();
        }
        for g in got.iter() {
            assert!(expected.contains(g), "unexpected peer: {:?}", g);
        }
    }

    #[test]
    fn the_bridge_port_admits_only_the_coordinator_and_the_sync() {
        let pol = lakekeeper_ingress("data");
        let spec = pol.spec.as_ref().unwrap();
        let ports = spec.ingress.as_ref().unwrap()[1].ports.as_ref().unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].port, Some(IntOrString::Int(8282)));
        let peers = spec.ingress.as_ref().unwrap()[1].from.as_ref().unwrap();
        assert_eq!(peers.len(), 2);
        let got: Vec<Vec<(String, String)>> = peers.iter().map(peer_labels).collect();
        assert!(got.contains(&vec![
            (
                "app.kubernetes.io/component".to_string(),
                "coordinator".to_string()
            ),
            ("app.kubernetes.io/name".to_string(), "trino".to_string()),
        ]));
        assert!(got.contains(&vec![(
            "app.kubernetes.io/name".to_string(),
            "access-sync".to_string(),
        )]));
    }

    #[test]
    fn the_policy_has_no_namespace_selector_and_no_ip_block() {
        let pol = lakekeeper_ingress("data");
        let spec = pol.spec.as_ref().unwrap();
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
            name: "main".into(),
            ..Default::default()
        };
        let job =
            crate::prerequisites::bootstrap_job("main", "data", "lakekeeper-bootstrap-1", owner);
        assert_eq!(
            job.spec
                .as_ref()
                .unwrap()
                .template
                .metadata
                .as_ref()
                .unwrap()
                .labels
                .as_ref()
                .unwrap()
                .get(BOOTSTRAP_POD_LABEL),
            Some(&BOOTSTRAP_POD_VALUE.to_string())
        );
        let pol = lakekeeper_ingress("data");
        let spec = pol.spec.as_ref().unwrap();
        let peers = spec.ingress.as_ref().unwrap()[0].from.as_ref().unwrap();
        assert!(peers.iter().any(|p| {
            let labels = p
                .pod_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap();
            labels.get(BOOTSTRAP_POD_LABEL) == Some(&BOOTSTRAP_POD_VALUE.to_string())
                && labels.len() == 1
        }));
    }
}
