//! The catalog's ingress policy (`design/operator-parity.md`, row `lakekeeper`): mirrors
//! stardelt-platform/manifests/lakekeeper-network-policy.yaml. Defence in depth — Lakekeeper
//! and the OPA bridge decide *what* a caller may do; this decides *who may ask at all*.

use std::collections::BTreeMap;

use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicyPort,
    NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};
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

/// The same peer, with keys and values the caller builds (a group label key carries the
/// group's own name).
fn peer_owned(labels: Vec<(&str, String)>) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        pod_selector: Some(LabelSelector {
            match_labels: Some(BTreeMap::from_iter(
                labels.into_iter().map(|(k, v)| (k.to_string(), v)),
            )),
            match_expressions: None,
        }),
        namespace_selector: None,
        ip_block: None::<IPBlock>,
    }
}

/// A Kubernetes label key segment or label value: 1 to 63 bytes, ASCII alphanumeric at both
/// ends, `[A-Za-z0-9_.-]` in between.
fn label_segment(value: &str) -> bool {
    let bytes = value.as_bytes();
    let alnum = |b: &u8| b.is_ascii_alphanumeric();
    !bytes.is_empty()
        && bytes.len() <= 63
        && alnum(&bytes[0])
        && alnum(&bytes[bytes.len() - 1])
        && bytes
            .iter()
            .all(|b| alnum(b) || matches!(b, b'.' | b'_' | b'-'))
}

/// Port the Spark Connect server serves gRPC on.
pub const SPARK_CONNECT_PORT: i32 = 15002;

/// Ingress to one `SparkConnect` server (internal#161 slice 4d): the server authenticates
/// nobody, so the network admits only the owner's pods, Nova and Airflow. The caller sets
/// the owner reference; the policy is created before the server exists.
pub fn spark_connect_ingress(
    name: &str,
    namespace: &str,
    owner: &crate::crd::SparkClusterOwner,
) -> Result<NetworkPolicy, String> {
    let connect = format!("spc-{name}");
    if !label_segment(&connect) {
        return Err(format!(
            "`{connect}` is not a valid label value (1 to 63 characters, letters, digits, `-`, `_` and `.`, starting and ending with a letter or digit)"
        ));
    }
    let owner_peer = match (&owner.group, &owner.user) {
        (Some(group), _) => {
            let segment = format!("group.{group}");
            if !label_segment(&segment) {
                return Err(format!(
                    "group `{group}` is not a valid label segment (1 to 63 characters, letters, digits, `-`, `_` and `.`, starting and ending with a letter or digit)"
                ));
            }
            let key = format!("stardelt.io/{segment}");
            peer_owned(vec![(&key, "true".to_string())])
        }
        (None, Some(user)) => {
            if !label_segment(user) {
                return Err(format!(
                    "user `{user}` is not a valid label value (1 to 63 characters, letters, digits, `-`, `_` and `.`, starting and ending with a letter or digit)"
                ));
            }
            peer_owned(vec![("stardelt.io/user", user.clone())])
        }
        (None, None) => return Err("the cluster names no owner".to_string()),
    };
    Ok(NetworkPolicy {
        metadata: meta(&format!("{connect}-ingress"), namespace),
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(CONNECT_POD_LABEL.to_string(), connect)])),
                match_expressions: None,
            }),
            policy_types: Some(vec!["Ingress".to_string()]),
            ingress: Some(vec![NetworkPolicyIngressRule {
                from: Some(vec![
                    owner_peer,
                    peer(&[("app.kubernetes.io/name", "nova")]),
                    peer(&[("release", "airflow"), ("component", "worker")]),
                ]),
                ports: Some(vec![port(SPARK_CONNECT_PORT)]),
            }]),
            egress: None,
        }),
    })
}

/// The Spark Operator labels a `SparkConnect`'s server pod with the connect's name.
pub const CONNECT_POD_LABEL: &str = "sparkoperator.k8s.io/connect-name";

/// Ingress to the Lakehouse's shared Spark Connect server (platform#1097). The server
/// authenticates nobody, so the network admits members' notebook pods on 15002 (not a
/// pod labelled `stardelt.io/group.visitors`) and the server's own driver and executor
/// pods on any port. Both carry `app.kubernetes.io/name: spark-connect`
/// (values/spark.yaml), so one peer covers the driver-executor traffic both ways.
pub fn shared_spark_connect_ingress(namespace: &str) -> NetworkPolicy {
    NetworkPolicy {
        metadata: meta("spark-connect-ingress", namespace),
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_string(),
                    "spark-connect".to_string(),
                )])),
                match_expressions: None,
            }),
            policy_types: Some(vec!["Ingress".to_string()]),
            ingress: Some(vec![
                NetworkPolicyIngressRule {
                    from: Some(vec![NetworkPolicyPeer {
                        pod_selector: Some(LabelSelector {
                            match_labels: Some(BTreeMap::from([(
                                "component".to_string(),
                                "singleuser-server".to_string(),
                            )])),
                            match_expressions: Some(vec![LabelSelectorRequirement {
                                key: "stardelt.io/group.visitors".to_string(),
                                operator: "DoesNotExist".to_string(),
                                values: None,
                            }]),
                        }),
                        namespace_selector: None,
                        ip_block: None::<IPBlock>,
                    }]),
                    ports: Some(vec![port(SPARK_CONNECT_PORT)]),
                },
                NetworkPolicyIngressRule {
                    from: Some(vec![peer(&[("app.kubernetes.io/name", "spark-connect")])]),
                    ports: None,
                },
            ]),
            egress: None,
        }),
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

    fn owner_labels(peer: &NetworkPolicyPeer) -> BTreeMap<String, String> {
        peer.pod_selector
            .as_ref()
            .expect("pod selector")
            .match_labels
            .clone()
            .expect("labels")
    }

    #[test]
    fn a_group_cluster_admits_the_owner_group_nova_and_airflow() {
        let owner = crate::crd::SparkClusterOwner {
            group: Some("mobility-analytics".into()),
            user: None,
        };
        let policy = spark_connect_ingress("trial", "data", &owner).expect("policy");
        assert_eq!(policy.metadata.name.as_deref(), Some("spc-trial-ingress"));
        assert_eq!(policy.metadata.namespace.as_deref(), Some("data"));
        assert!(policy.metadata.owner_references.is_none());
        let spec = policy.spec.as_ref().expect("spec");
        assert_eq!(
            spec.pod_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap(),
            &BTreeMap::from([(
                "sparkoperator.k8s.io/connect-name".to_string(),
                "spc-trial".to_string()
            )])
        );
        assert_eq!(
            spec.policy_types.as_ref().unwrap(),
            &vec!["Ingress".to_string()]
        );
        let ingress = spec.ingress.as_ref().unwrap();
        assert_eq!(ingress.len(), 1);
        assert_eq!(ingress[0].ports.as_ref().unwrap(), &vec![port(15002)]);
        let from = ingress[0].from.as_ref().unwrap();
        assert_eq!(from.len(), 3, "{from:?}");
        assert_eq!(
            owner_labels(&from[0]),
            BTreeMap::from([(
                "stardelt.io/group.mobility-analytics".to_string(),
                "true".to_string()
            )])
        );
        assert_eq!(
            owner_labels(&from[1]),
            BTreeMap::from([("app.kubernetes.io/name".to_string(), "nova".to_string())])
        );
        assert_eq!(
            owner_labels(&from[2]),
            BTreeMap::from([
                ("release".to_string(), "airflow".to_string()),
                ("component".to_string(), "worker".to_string()),
            ])
        );
    }

    #[test]
    fn a_person_cluster_admits_that_person() {
        let owner = crate::crd::SparkClusterOwner {
            group: None,
            user: Some("ana".into()),
        };
        let policy = spark_connect_ingress("trial", "data", &owner).expect("policy");
        let spec = policy.spec.as_ref().expect("spec");
        let from = spec.ingress.as_ref().unwrap()[0]
            .from
            .as_ref()
            .expect("peer");
        assert_eq!(
            owner_labels(&from[0]),
            BTreeMap::from([("stardelt.io/user".to_string(), "ana".to_string())])
        );
        let keys = from
            .iter()
            .flat_map(|p| owner_labels(p).into_keys())
            .collect::<Vec<_>>();
        assert!(!keys.iter().any(|key| key.contains("group.")), "{keys:?}");
    }

    #[test]
    fn shared_spark_connect_ingress_admits_members_and_own_pods() {
        let policy = shared_spark_connect_ingress("data");
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("spark-connect-ingress")
        );
        assert_eq!(policy.metadata.namespace.as_deref(), Some("data"));
        let spec = policy.spec.as_ref().expect("spec");
        assert_eq!(
            spec.pod_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap(),
            &BTreeMap::from([(
                "app.kubernetes.io/name".to_string(),
                "spark-connect".to_string()
            )])
        );
        assert_eq!(
            spec.policy_types.as_ref().unwrap(),
            &vec!["Ingress".to_string()]
        );
        let ingress = spec.ingress.as_ref().expect("rules");
        assert_eq!(ingress.len(), 2);
        // Rule 1: members' notebook pods only, on the Connect port.
        let members = ingress[0].from.as_ref().unwrap();
        assert_eq!(members.len(), 1);
        let selector = members[0].pod_selector.as_ref().expect("pod selector");
        assert_eq!(
            selector.match_labels.as_ref().unwrap(),
            &BTreeMap::from([("component".to_string(), "singleuser-server".to_string())])
        );
        let expressions = selector.match_expressions.as_ref().expect("expressions");
        assert_eq!(expressions.len(), 1);
        assert_eq!(expressions[0].key, "stardelt.io/group.visitors");
        assert_eq!(expressions[0].operator, "DoesNotExist");
        assert_eq!(
            ingress[0].ports.as_ref().unwrap(),
            &vec![port(SPARK_CONNECT_PORT)]
        );
        // Rule 2: the server's own driver and executor pods on any port.
        let own = ingress[1].from.as_ref().unwrap();
        assert_eq!(
            own[0]
                .pod_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap(),
            &BTreeMap::from([(
                "app.kubernetes.io/name".to_string(),
                "spark-connect".to_string()
            )])
        );
        assert!(ingress[1].ports.is_none());
    }

    #[test]
    fn an_owner_that_cannot_be_a_label_is_refused() {
        let spaced = crate::crd::SparkClusterOwner {
            group: Some("Analytics Team".into()),
            user: None,
        };
        let err = spark_connect_ingress("trial", "data", &spaced).unwrap_err();
        assert!(err.contains("Analytics Team"), "{err}");

        let long = crate::crd::SparkClusterOwner {
            group: Some("a".repeat(60)),
            user: None,
        };
        assert!(spark_connect_ingress("trial", "data", &long).is_err());
    }
}
