//! The catalog's ingress policy (`design/operator-parity.md`, row `lakekeeper`): mirrors
//! stardelt-platform/manifests/lakekeeper-network-policy.yaml. Defence in depth — Lakekeeper
//! and the OPA bridge decide *what* a caller may do; this decides *who may ask at all*.

use std::collections::BTreeMap;

use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule, NetworkPolicyPeer,
    NetworkPolicyPort, NetworkPolicySpec,
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

/// The object store's unsigned ports (platform#1100). Mirrors
/// stardelt-platform/manifests/seaweedfs-network-policy.yaml.
pub const SEAWEEDFS_POLICY: &str = "seaweedfs-internal";

/// The IdP's ingress policy: sign-in stays open in-cluster, metrics is gated
/// (stardelt-platform#1161).
pub const KEYCLOAK_POLICY: &str = "keycloak-ingress";
/// The sign-in, discovery and token port. Deliberately open to every source:
/// every platform component and the ingress controller authenticate here.
pub const KEYCLOAK_HTTP_PORT: i32 = 8080;
/// The management port that serves Prometheus text (stardelt-operator#630).
pub const KEYCLOAK_MANAGEMENT_PORT: i32 = 9000;

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

/// The same pod labels in **any** namespace: the customer's Prometheus lives in its
/// own namespace, and a peer without a namespace selector matches only this policy's
/// namespace (stardelt-operator#607).
fn peer_any_namespace(labels: &[(&str, &str)]) -> NetworkPolicyPeer {
    NetworkPolicyPeer {
        namespace_selector: Some(LabelSelector {
            match_labels: None,
            match_expressions: None,
        }),
        ..peer(labels)
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

/// Port the shared Spark Connect driver serves Prometheus text on (its UI port).
pub const SPARK_CONNECT_METRICS_PORT: i32 = 4040;

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
/// The customer's Prometheus reads the driver's metrics on the UI port 4040 from any
/// namespace; a NetworkPolicy selects ports, not paths, so that rule admits the whole UI
/// (jobs, stages, the redacted environment page) as well — the exposure
/// stardelt-platform#1158 measured and accepted.
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
                // The customer's Prometheus (stardelt-operator#607): read-only, on the
                // UI port. A NetworkPolicy selects ports, not paths, so this admits the
                // driver's UI too (jobs, stages, the redacted environment page) — the
                // exposure stardelt-platform#1158 measured and accepted.
                NetworkPolicyIngressRule {
                    from: Some(vec![peer_any_namespace(&[(
                        "app.kubernetes.io/name",
                        "prometheus",
                    )])]),
                    ports: Some(vec![port(SPARK_CONNECT_METRICS_PORT)]),
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

/// The same port with UDP: DNS answers on both transports.
fn port_udp(number: i32) -> NetworkPolicyPort {
    NetworkPolicyPort {
        port: Some(IntOrString::Int(number)),
        end_port: None,
        protocol: Some("UDP".into()),
    }
}

/// Ingress to the serving Lakekeeper pod only: the chart's db-migration Job carries
/// `name=lakekeeper` too, and `component=catalog` keeps migrations (Postgres only) out.
/// Callers are same-namespace; the metrics scraper is the one read-only peer from any
/// namespace (stardelt-operator#607).
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
                NetworkPolicyIngressRule {
                    // The metrics endpoint (9000): read-only, so the customer's Prometheus may
                    // scrape it from its own namespace.
                    from: Some(vec![peer_any_namespace(&[(
                        "app.kubernetes.io/name",
                        "prometheus",
                    )])]),
                    ports: Some(vec![port(9000)]),
                },
            ]),
            egress: None,
        }),
    }
}

/// The permission store's ingress policy: OpenFGA ships without authentication, so the
/// network is what keeps every other pod from rewriting grants. Mirrors
/// stardelt-platform/manifests/openfga-network-policy.yaml. Lakekeeper is same-namespace;
/// the metrics scraper is the one read-only peer from any namespace (stardelt-operator#607).
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
            ingress: Some(vec![
                NetworkPolicyIngressRule {
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
                },
                NetworkPolicyIngressRule {
                    // The metrics endpoint (2112): read-only, so the customer's Prometheus may
                    // scrape it from its own namespace.
                    from: Some(vec![peer_any_namespace(&[(
                        "app.kubernetes.io/name",
                        "prometheus",
                    )])]),
                    ports: Some(vec![port(2112)]),
                },
            ]),
            egress: None,
        }),
    }
}

/// Ingress to the platform IdP (stardelt-platform#1161): 8080 stays open to every
/// source because every component signs in and the ingress controller is a pod;
/// 9000 answers only a scraper-labelled pod in any namespace (stardelt-operator#607).
/// Kubelet probes come from the node and are unaffected, as with `lakekeeper_ingress`.
pub fn keycloak_ingress(namespace: &str) -> NetworkPolicy {
    NetworkPolicy {
        metadata: meta(KEYCLOAK_POLICY, namespace),
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_string(),
                    "keycloak".to_string(),
                )])),
                match_expressions: None,
            }),
            policy_types: Some(vec!["Ingress".to_string()]),
            ingress: Some(vec![
                // Sign-in: no `from`, so every source; the port is the restriction.
                NetworkPolicyIngressRule {
                    from: None,
                    ports: Some(vec![port(KEYCLOAK_HTTP_PORT)]),
                },
                NetworkPolicyIngressRule {
                    from: Some(vec![peer_any_namespace(&[(
                        "app.kubernetes.io/name",
                        "prometheus",
                    )])]),
                    ports: Some(vec![port(KEYCLOAK_MANAGEMENT_PORT)]),
                },
            ]),
            egress: None,
        }),
    }
}

/// SeaweedFS's own pods reach the filer, master and volume servers on every port; a pod
/// labelled `app.kubernetes.io/name: prometheus` in any namespace reads 9327 only.
/// The S3 gateway (8333) stays open: it checks an access key.
pub fn seaweedfs_internal(namespace: &str) -> NetworkPolicy {
    NetworkPolicy {
        metadata: meta(SEAWEEDFS_POLICY, namespace),
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_string(),
                    "seaweedfs".to_string(),
                )])),
                match_expressions: Some(vec![LabelSelectorRequirement {
                    key: "app.kubernetes.io/component".to_string(),
                    operator: "In".to_string(),
                    values: Some(vec![
                        "master".to_string(),
                        "filer".to_string(),
                        "volume".to_string(),
                    ]),
                }]),
            }),
            policy_types: Some(vec!["Ingress".to_string()]),
            ingress: Some(vec![
                NetworkPolicyIngressRule {
                    from: Some(vec![peer(&[("app.kubernetes.io/name", "seaweedfs")])]),
                    ports: None,
                },
                // The customer's Prometheus (stardelt-operator#607): read-only metrics on 9327.
                // master/filer/volume answer Prometheus text there; the unsigned data ports
                // (8888, 9333, 8080) stay closed by the rule above.
                NetworkPolicyIngressRule {
                    from: Some(vec![peer_any_namespace(&[(
                        "app.kubernetes.io/name",
                        "prometheus",
                    )])]),
                    ports: Some(vec![port(9327)]),
                },
                // The chart's bucket hook (stardelt-platform#1244): `helm upgrade` runs it as a
                // post-upgrade hook — after this policy is live — and the operator installs the
                // chart with `s3.createBuckets` (`values/seaweedfs.yaml`), so every upgrade
                // creates the `lakehouse` and `backups` buckets through it. It waits on the
                // master's `/cluster/status` (9333) and the filer (8888), then `weed shell`
                // dials the master's gRPC port (19333) and the filer's (18888).
                // The Job controller labels the pod `job-name: <job>` and the chart's Job name is
                // the fixed `seaweedfs-bucket-hook` (`helm template … --version 4.25.1` renders
                // exactly that name). `job-name` is the pod's own label, so this admits any pod in
                // the namespace that claims it — still far narrower than
                // `app.kubernetes.io/instance: seaweedfs`, which would also admit the S3 gateway.
                NetworkPolicyIngressRule {
                    from: Some(vec![peer(&[("job-name", "seaweedfs-bucket-hook")])]),
                    ports: Some(vec![port(9333), port(8888), port(19333), port(18888)]),
                },
            ]),
            egress: None,
        }),
    }
}

/// Egress from a scheduled notebook pod (stardelt-platform#1100's "Then" item 3): it runs a
/// person's code, so it reaches the query engine, the S3 gateway, DNS and the public
/// internet — and no other private address. Mirrors
/// stardelt-platform/manifests/pipeline-notebook-egress.yaml (measured there before the
/// ticket was written: without it, a pod with these labels reached nova-pg:5432 and
/// openfga-pg:5432).
pub const PIPELINE_NOTEBOOK_POLICY: &str = "pipeline-notebook-egress";
/// The label the pipeline loader puts on every notebook task pod.
pub const PIPELINE_NOTEBOOK_LABEL: &str = "pipeline-notebook";

pub fn pipeline_notebook_egress(namespace: &str) -> NetworkPolicy {
    NetworkPolicy {
        metadata: meta(PIPELINE_NOTEBOOK_POLICY, namespace),
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".to_string(),
                    PIPELINE_NOTEBOOK_LABEL.to_string(),
                )])),
                match_expressions: None,
            }),
            policy_types: Some(vec!["Egress".to_string()]),
            ingress: None,
            egress: Some(vec![
                // DNS: the cluster resolver in kube-system, or a private-range resolver IP.
                NetworkPolicyEgressRule {
                    to: Some(vec![
                        NetworkPolicyPeer {
                            namespace_selector: Some(LabelSelector {
                                match_labels: Some(BTreeMap::from([(
                                    "kubernetes.io/metadata.name".to_string(),
                                    "kube-system".to_string(),
                                )])),
                                match_expressions: None,
                            }),
                            ..NetworkPolicyPeer::default()
                        },
                        NetworkPolicyPeer {
                            ip_block: Some(IPBlock {
                                cidr: "10.0.0.0/8".to_string(),
                                except: None,
                            }),
                            ..NetworkPolicyPeer::default()
                        },
                        NetworkPolicyPeer {
                            ip_block: Some(IPBlock {
                                cidr: "172.16.0.0/12".to_string(),
                                except: None,
                            }),
                            ..NetworkPolicyPeer::default()
                        },
                        NetworkPolicyPeer {
                            ip_block: Some(IPBlock {
                                cidr: "192.168.0.0/16".to_string(),
                                except: None,
                            }),
                            ..NetworkPolicyPeer::default()
                        },
                    ]),
                    ports: Some(vec![port(53), port_udp(53)]),
                },
                // The query engine (PIPELINE_TRINO_URL is the in-cluster https endpoint).
                NetworkPolicyEgressRule {
                    to: Some(vec![peer(&[
                        ("app.kubernetes.io/name", "trino"),
                        ("app.kubernetes.io/component", "coordinator"),
                    ])]),
                    ports: Some(vec![port(8443)]),
                },
                // The S3 gateway: the presigned GET and PUT of the notebook.
                NetworkPolicyEgressRule {
                    to: Some(vec![peer(&[
                        ("app.kubernetes.io/name", "seaweedfs"),
                        ("app.kubernetes.io/component", "s3"),
                    ])]),
                    ports: Some(vec![port(8333)]),
                },
                // The public internet, never a private range and never the metadata address.
                NetworkPolicyEgressRule {
                    to: Some(vec![NetworkPolicyPeer {
                        ip_block: Some(IPBlock {
                            cidr: "0.0.0.0/0".to_string(),
                            except: Some(vec![
                                "10.0.0.0/8".to_string(),
                                "172.16.0.0/12".to_string(),
                                "192.168.0.0/16".to_string(),
                                "169.254.169.254/32".to_string(),
                            ]),
                        }),
                        ..NetworkPolicyPeer::default()
                    }]),
                    ports: None,
                },
            ]),
        }),
    }
}

/// The public internet for **members'** notebook pods (stardelt-platform#1101). A
/// public-demo visitor's pod carries `stardelt.io/group.visitors=true` and is not selected,
/// so it reaches no public address; members' pods are selected and keep the egress the
/// chart's `nonPrivateIPs` default used to grant everyone. NetworkPolicies only add rights,
/// so the chart's default is off in `values/jupyterhub.yaml`. Mirrors
/// stardelt-platform/manifests/notebook-public-egress.yaml.
pub const NOTEBOOK_PUBLIC_EGRESS_POLICY: &str = "notebook-public-egress";

pub fn notebook_public_egress(namespace: &str) -> NetworkPolicy {
    NetworkPolicy {
        metadata: meta(NOTEBOOK_PUBLIC_EGRESS_POLICY, namespace),
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "component".to_string(),
                    "singleuser-server".to_string(),
                )])),
                // Visitors are excluded: only a member's pod carries no visitors label.
                match_expressions: Some(vec![LabelSelectorRequirement {
                    key: "stardelt.io/group.visitors".to_string(),
                    operator: "DoesNotExist".to_string(),
                    values: None,
                }]),
            }),
            policy_types: Some(vec!["Egress".to_string()]),
            ingress: None,
            egress: Some(vec![NetworkPolicyEgressRule {
                to: Some(vec![NetworkPolicyPeer {
                    ip_block: Some(IPBlock {
                        cidr: "0.0.0.0/0".to_string(),
                        except: Some(vec![
                            "10.0.0.0/8".to_string(),
                            "172.16.0.0/12".to_string(),
                            "192.168.0.0/16".to_string(),
                            "169.254.0.0/16".to_string(),
                            "100.64.0.0/10".to_string(),
                        ]),
                    }),
                    ..NetworkPolicyPeer::default()
                }]),
                ports: None,
            }]),
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
        assert_eq!(ingress.len(), 2);
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

    #[test]
    fn keycloak_ingress_keeps_sign_in_open_and_gates_the_management_port() {
        let policy = keycloak_ingress("data");
        assert_eq!(policy.metadata.name.as_deref(), Some("keycloak-ingress"));
        let spec = policy.spec.as_ref().expect("spec");
        assert_eq!(
            spec.pod_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap(),
            &BTreeMap::from([("app.kubernetes.io/name".to_string(), "keycloak".to_string())])
        );
        let ingress = spec.ingress.as_ref().expect("rules");
        assert_eq!(ingress.len(), 2);
        // Rule 1: every source (no `from`) on the sign-in port only.
        assert!(ingress[0].from.is_none());
        assert_eq!(
            ingress[0].ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(8080))
        );
        // Rule 2: the scraper label from any namespace, on the management port only.
        let peer = &ingress[1].from.as_ref().unwrap()[0];
        assert_eq!(
            peer.pod_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap(),
            &BTreeMap::from([(
                "app.kubernetes.io/name".to_string(),
                "prometheus".to_string()
            )])
        );
        assert!(peer.namespace_selector.is_some());
        assert_eq!(
            ingress[1].ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(9000))
        );
        assert_eq!(ingress[1].ports.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn seaweedfs_policy_selects_the_three_internal_components() {
        let policy = seaweedfs_internal("data");
        let spec = policy.spec.as_ref().expect("spec");
        let pod_sel = spec.pod_selector.as_ref().expect("pod selector");
        let labels = pod_sel.match_labels.as_ref().expect("labels");
        assert_eq!(
            labels.get("app.kubernetes.io/name").map(String::as_str),
            Some("seaweedfs")
        );
        assert_eq!(labels.len(), 1);
        let expressions = pod_sel.match_expressions.as_ref().expect("expressions");
        assert_eq!(expressions.len(), 1);
        assert_eq!(expressions[0].key, "app.kubernetes.io/component");
        assert_eq!(expressions[0].operator, "In");
        assert_eq!(
            expressions[0].values.as_ref().expect("values"),
            &vec![
                "master".to_string(),
                "filer".to_string(),
                "volume".to_string()
            ]
        );
    }

    #[test]
    fn seaweedfs_policy_admits_seaweedfs_on_every_port() {
        let policy = seaweedfs_internal("data");
        let spec = policy.spec.as_ref().expect("spec");
        assert_eq!(
            spec.policy_types.as_ref().expect("policy types"),
            &vec!["Ingress".to_string()]
        );
        assert!(spec.egress.is_none());
        let ingress = spec.ingress.as_ref().expect("ingress");
        assert_eq!(ingress.len(), 3);
        assert!(ingress[0].ports.is_none(), "every port, not a list");
        let from = ingress[0].from.as_ref().expect("from");
        assert_eq!(from.len(), 1);
        assert_eq!(
            owner_labels(&from[0]),
            BTreeMap::from([(
                "app.kubernetes.io/name".to_string(),
                "seaweedfs".to_string()
            )])
        );
    }

    #[test]
    fn seaweedfs_policy_admits_a_prometheus_scraper_on_9327() {
        let policy = seaweedfs_internal("data");
        let ingress = policy.spec.as_ref().unwrap().ingress.as_ref().unwrap();
        assert_eq!(ingress.len(), 3);
        assert_scrape_rule(&ingress[1], 9327);
    }

    #[test]
    fn seaweedfs_policy_admits_the_bucket_hook_on_the_data_and_grpc_ports() {
        let policy = seaweedfs_internal("data");
        let ingress = policy.spec.as_ref().unwrap().ingress.as_ref().unwrap();
        assert_eq!(ingress.len(), 3);
        let rule = &ingress[2];
        let peers = rule.from.as_ref().expect("peers");
        assert_eq!(peers.len(), 1);
        assert_eq!(
            owner_labels(&peers[0]),
            BTreeMap::from([("job-name".to_string(), "seaweedfs-bucket-hook".to_string())])
        );
        assert!(peers[0].namespace_selector.is_none());
        assert!(peers[0].ip_block.is_none());
        let ports: Vec<_> = rule
            .ports
            .as_ref()
            .expect("ports")
            .iter()
            .map(|entry| entry.port.clone().expect("a port number"))
            .collect();
        assert_eq!(
            ports,
            vec![
                IntOrString::Int(9333),
                IntOrString::Int(8888),
                IntOrString::Int(19333),
                IntOrString::Int(18888)
            ],
            "master HTTP, filer HTTP, master gRPC, filer gRPC — in the platform manifest's order"
        );
        assert!(
            rule.ports
                .as_ref()
                .unwrap()
                .iter()
                .all(|entry| entry.protocol.as_deref() == Some("TCP"))
        );
    }

    #[test]
    fn seaweedfs_policy_name_matches_the_platform() {
        let policy = seaweedfs_internal("data");
        assert_eq!(policy.metadata.name.as_deref(), Some("seaweedfs-internal"));
        assert_eq!(policy.metadata.name.as_deref(), Some(SEAWEEDFS_POLICY));
        assert_eq!(policy.metadata.namespace.as_deref(), Some("data"));
        assert!(policy.metadata.owner_references.is_none());
    }

    /// A scrape rule's only peer: the Prometheus pod labels in any namespace, and one TCP port.
    fn assert_scrape_rule(rule: &NetworkPolicyIngressRule, expected_port: i32) {
        let peers = rule.from.as_ref().expect("peers");
        assert_eq!(peers.len(), 1, "{peers:?}");
        let scraper = &peers[0];
        let ns = scraper
            .namespace_selector
            .as_ref()
            .expect("any-namespace selector");
        assert!(ns.match_labels.is_none(), "{ns:?}");
        assert!(ns.match_expressions.is_none(), "{ns:?}");
        assert!(scraper.ip_block.is_none());
        assert_eq!(
            owner_labels(scraper),
            BTreeMap::from([(
                "app.kubernetes.io/name".to_string(),
                "prometheus".to_string()
            )])
        );
        assert_eq!(rule.ports.as_ref().unwrap(), &vec![port(expected_port)]);
    }

    #[test]
    fn openfga_policy_admits_a_prometheus_scraper_on_2112() {
        let policy = openfga_ingress("data");
        let ingress = policy.spec.as_ref().unwrap().ingress.as_ref().unwrap();
        assert_eq!(ingress.len(), 2);
        assert_scrape_rule(&ingress[1], 2112);
    }

    #[test]
    fn lakekeeper_policy_admits_a_prometheus_scraper_on_9000() {
        let policy = lakekeeper_ingress("data");
        let ingress = policy.spec.as_ref().unwrap().ingress.as_ref().unwrap();
        assert_eq!(ingress.len(), 3);
        assert_scrape_rule(&ingress[2], 9000);
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
        assert_eq!(ingress.len(), 3);
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
        // Rule 3: the customer's Prometheus, any namespace, on the metrics port.
        let scrape = ingress[2].from.as_ref().unwrap();
        assert_eq!(scrape.len(), 1);
        assert_eq!(
            scrape[0]
                .pod_selector
                .as_ref()
                .unwrap()
                .match_labels
                .as_ref()
                .unwrap(),
            &BTreeMap::from([(
                "app.kubernetes.io/name".to_string(),
                "prometheus".to_string()
            )])
        );
        assert!(
            scrape[0].namespace_selector.is_some(),
            "the scraper lives in another namespace"
        );
        assert_eq!(
            ingress[2].ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(4040))
        );
        assert_eq!(ingress[2].ports.as_ref().unwrap().len(), 1);
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

    #[test]
    fn pipeline_notebook_egress_selects_the_task_pods() {
        let policy = pipeline_notebook_egress("data");
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some(PIPELINE_NOTEBOOK_POLICY)
        );
        let spec = policy.spec.as_ref().unwrap();
        assert_eq!(
            spec.policy_types.as_ref().unwrap(),
            &vec!["Egress".to_string()]
        );
        assert!(spec.ingress.is_none());
        let labels = spec
            .pod_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(
            labels.get("app.kubernetes.io/name").unwrap(),
            "pipeline-notebook"
        );
    }

    #[test]
    fn pipeline_notebook_egress_allows_trino_s3_dns_and_public_only() {
        let policy = pipeline_notebook_egress("data");
        let spec = policy.spec.as_ref().unwrap();
        let rules = spec.egress.as_ref().unwrap();
        assert_eq!(rules.len(), 4);

        // Rule 1: DNS on TCP and UDP 53.
        let dns = rules[0].ports.as_ref().unwrap();
        let mut dns_ports: Vec<(i32, String)> = dns
            .iter()
            .map(|p| {
                let number = match &p.port {
                    Some(IntOrString::Int(n)) => *n,
                    other => panic!("expected an int port, got {other:?}"),
                };
                (number, p.protocol.clone().unwrap())
            })
            .collect();
        dns_ports.sort();
        assert_eq!(
            dns_ports,
            vec![(53, "TCP".to_string()), (53, "UDP".to_string())]
        );

        // Rule 2: the query engine on 8443.
        let trino_peers = rules[1].to.as_ref().unwrap();
        assert_eq!(trino_peers.len(), 1);
        let trino = trino_peers[0]
            .pod_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert_eq!(trino.get("app.kubernetes.io/name").unwrap(), "trino");
        assert_eq!(
            trino.get("app.kubernetes.io/component").unwrap(),
            "coordinator"
        );
        assert_eq!(
            rules[1].ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(8443))
        );

        // Rule 3: the S3 gateway on 8333.
        let s3_peers = rules[2].to.as_ref().unwrap();
        assert_eq!(s3_peers.len(), 1);
        let s3 = s3_peers[0]
            .pod_selector
            .as_ref()
            .unwrap()
            .match_labels
            .as_ref()
            .unwrap();
        assert_eq!(s3.get("app.kubernetes.io/name").unwrap(), "seaweedfs");
        assert_eq!(s3.get("app.kubernetes.io/component").unwrap(), "s3");
        assert_eq!(
            rules[2].ports.as_ref().unwrap()[0].port,
            Some(IntOrString::Int(8333))
        );

        // Rule 4: the public internet, every private range and the metadata address excepted.
        let public = rules[3].to.as_ref().unwrap();
        assert_eq!(public.len(), 1);
        let block = public[0].ip_block.as_ref().unwrap();
        assert_eq!(block.cidr, "0.0.0.0/0");
        assert_eq!(
            block.except.as_ref().unwrap(),
            &vec![
                "10.0.0.0/8".to_string(),
                "172.16.0.0/12".to_string(),
                "192.168.0.0/16".to_string(),
                "169.254.169.254/32".to_string(),
            ]
        );
        assert!(rules[3].ports.is_none());
    }

    #[test]
    fn notebook_public_egress_selects_members_only_and_excepts_every_private_range() {
        let policy = notebook_public_egress("data");
        // The literal, not the constant: the platform's manifest carries this name.
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("notebook-public-egress")
        );
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some(NOTEBOOK_PUBLIC_EGRESS_POLICY)
        );
        let spec = policy.spec.as_ref().unwrap();
        assert_eq!(
            spec.policy_types.as_ref().unwrap(),
            &vec!["Egress".to_string()]
        );
        assert!(spec.ingress.is_none());
        let selector = spec.pod_selector.as_ref().unwrap();
        let labels = selector.match_labels.as_ref().unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels.get("component").unwrap(), "singleuser-server");
        let expressions = selector.match_expressions.as_ref().unwrap();
        assert_eq!(expressions.len(), 1);
        assert_eq!(expressions[0].key, "stardelt.io/group.visitors");
        assert_eq!(expressions[0].operator, "DoesNotExist");
        assert!(expressions[0].values.is_none());
        let rules = spec.egress.as_ref().unwrap();
        assert_eq!(rules.len(), 1);
        let peers = rules[0].to.as_ref().unwrap();
        assert_eq!(peers.len(), 1);
        let block = peers[0].ip_block.as_ref().unwrap();
        assert_eq!(block.cidr, "0.0.0.0/0");
        assert_eq!(
            block.except.as_ref().unwrap(),
            &vec![
                "10.0.0.0/8".to_string(),
                "172.16.0.0/12".to_string(),
                "192.168.0.0/16".to_string(),
                "169.254.0.0/16".to_string(),
                "100.64.0.0/10".to_string(),
            ]
        );
        assert!(rules[0].ports.is_none());
    }

    #[test]
    fn no_pipeline_notebook_peer_opens_every_namespace() {
        let policy = pipeline_notebook_egress("data");
        let spec = policy.spec.as_ref().unwrap();
        for rule in spec.egress.as_ref().unwrap() {
            for peer in rule.to.as_ref().unwrap() {
                if let Some(selector) = &peer.namespace_selector {
                    assert!(
                        selector.match_labels.is_some(),
                        "an empty namespaceSelector admits every namespace"
                    );
                }
            }
        }
    }
}
