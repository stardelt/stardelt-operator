//! The Spark Connect endpoint: one long-running driver with Kubernetes as the cluster manager,
//! for compute that outgrows Trino. Pure builders; they mirror
//! stardelt-platform/manifests/spark.yaml object for object (operator#4, part 1).
//!
//! No Ingress and no authentication: Spark Connect has none of its own, so the Service stays
//! ClusterIP and only in-cluster clients reach it.

use std::collections::BTreeMap;

use k8s_openapi::api::{
    apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy},
    core::v1::{
        Capabilities, ConfigMap, ConfigMapVolumeSource, Container, ContainerPort,
        EmptyDirVolumeSource, EnvVar, EnvVarSource, LocalObjectReference, ObjectFieldSelector,
        PodSecurityContext, PodSpec, PodTemplateSpec, Probe, ResourceRequirements, SeccompProfile,
        SecretKeySelector, SecurityContext, Service, ServiceAccount, ServicePort, ServiceSpec,
        TCPSocketAction, Volume, VolumeMount,
    },
    rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
};
use k8s_openapi::apimachinery::pkg::{
    api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
};
use kube::api::ObjectMeta;

pub const NAME: &str = "spark-connect";
pub const CONFIG_MAP: &str = "spark-connect-config";

/// The digest-pinned Spark image every pod in this repo runs (the Connect server and a
/// `StreamApp`'s driver/executors share it, D42).
pub fn image() -> String {
    golden().image
}

/// The image and the `spark-defaults.conf` body live in one file, like the golden values of the
/// Helm components do (`values.rs`).
#[derive(serde::Deserialize)]
struct Golden {
    image: String,
    defaults: String,
}

fn golden() -> Golden {
    serde_yaml::from_str(include_str!("../values/spark.yaml"))
        .expect("values/spark.yaml is not valid YAML")
}

fn part_of() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    )])
}

fn name_selector() -> BTreeMap<String, String> {
    BTreeMap::from([("app.kubernetes.io/name".to_string(), NAME.to_string())])
}

fn meta_with_name_labels(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        labels: Some({
            let mut labels = name_selector();
            labels.extend(part_of());
            labels
        }),
        ..ObjectMeta::default()
    }
}

/// The identity Spark Connect's pod runs as.
pub fn service_account(namespace: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: meta_with_name_labels(NAME, namespace),
        ..ServiceAccount::default()
    }
}

/// The `deletecollection` grants are what Spark's Kubernetes client needs to clean up after a
/// driver exits.
pub fn role(namespace: &str) -> Role {
    Role {
        metadata: meta_with_name_labels(NAME, namespace),
        rules: Some(vec![
            PolicyRule {
                api_groups: Some(vec!["".into()]),
                resources: Some(vec!["pods".into()]),
                verbs: vec![
                    "get".into(),
                    "list".into(),
                    "watch".into(),
                    "create".into(),
                    "delete".into(),
                    "deletecollection".into(),
                ],
                ..PolicyRule::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".into()]),
                resources: Some(vec!["services".into()]),
                verbs: vec![
                    "get".into(),
                    "list".into(),
                    "create".into(),
                    "delete".into(),
                    "deletecollection".into(),
                ],
                ..PolicyRule::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".into()]),
                resources: Some(vec!["configmaps".into()]),
                verbs: vec![
                    "get".into(),
                    "list".into(),
                    "create".into(),
                    "update".into(),
                    "delete".into(),
                    "deletecollection".into(),
                ],
                ..PolicyRule::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".into()]),
                resources: Some(vec!["persistentvolumeclaims".into()]),
                verbs: vec![
                    "get".into(),
                    "list".into(),
                    "create".into(),
                    "delete".into(),
                    "deletecollection".into(),
                ],
                ..PolicyRule::default()
            },
        ]),
    }
}

pub fn role_binding(namespace: &str) -> RoleBinding {
    RoleBinding {
        metadata: meta_with_name_labels(NAME, namespace),
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".into(),
            name: NAME.into(),
            namespace: Some(namespace.into()),
            ..Subject::default()
        }]),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".into()),
            kind: "Role".into(),
            name: NAME.into(),
        },
    }
}

pub fn config_map(namespace: &str) -> ConfigMap {
    let mut golden = golden();
    golden.defaults = golden.defaults.replace("__NAMESPACE__", namespace);
    let mut data = BTreeMap::new();
    data.insert("spark-defaults.conf".to_string(), golden.defaults);
    ConfigMap {
        metadata: meta_with_name_labels(CONFIG_MAP, namespace),
        data: Some(data),
        ..ConfigMap::default()
    }
}

pub fn deployment(namespace: &str) -> Deployment {
    let image = golden().image;
    let labels_name_only = name_selector();

    Deployment {
        metadata: meta_with_name_labels(NAME, namespace),
        spec: DeploymentSpec {
            replicas: Some(1),
            revision_history_limit: Some(2),
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".into()),
                rolling_update: None,
            }),
            selector: LabelSelector {
                match_labels: Some(labels_name_only.clone()),
                ..LabelSelector::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels_name_only.clone()),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    service_account_name: Some(NAME.into()),
                    image_pull_secrets: Some(vec![LocalObjectReference {
                        name: "ghcr-creds".into(),
                    }]),
                    termination_grace_period_seconds: Some(60),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        run_as_user: Some(185),
                        fs_group: Some(185),
                        seccomp_profile: Some(SeccompProfile {
                            type_: "RuntimeDefault".into(),
                            ..SeccompProfile::default()
                        }),
                        ..PodSecurityContext::default()
                    }),
                    containers: vec![Container {
                        name: NAME.into(),
                        image: Some(image),
                        command: Some(vec!["/opt/spark/bin/stardelt-connect-server".into()]),
                        ports: Some(vec![ContainerPort {
                            name: Some("connect".into()),
                            container_port: 15002,
                            protocol: Some("TCP".into()),
                            host_ip: None,
                            host_port: None,
                        }]),
                        env: Some(vec![
                            EnvVar {
                                name: "POD_IP".into(),
                                value_from: Some(EnvVarSource {
                                    field_ref: Some(ObjectFieldSelector {
                                        field_path: "status.podIP".into(),
                                        ..ObjectFieldSelector::default()
                                    }),
                                    ..EnvVarSource::default()
                                }),
                                ..EnvVar::default()
                            },
                            EnvVar {
                                name: "AWS_ACCESS_KEY_ID".into(),
                                value_from: Some(EnvVarSource {
                                    secret_key_ref: Some(SecretKeySelector {
                                        name: "stardelt-s3-creds".into(),
                                        key: "access-key".into(),
                                        optional: Some(false),
                                    }),
                                    ..EnvVarSource::default()
                                }),
                                ..EnvVar::default()
                            },
                            EnvVar {
                                name: "AWS_SECRET_ACCESS_KEY".into(),
                                value_from: Some(EnvVarSource {
                                    secret_key_ref: Some(SecretKeySelector {
                                        name: "stardelt-s3-creds".into(),
                                        key: "secret-key".into(),
                                        optional: Some(false),
                                    }),
                                    ..EnvVarSource::default()
                                }),
                                ..EnvVar::default()
                            },
                        ]),
                        resources: Some(ResourceRequirements {
                            requests: Some({
                                let mut m = BTreeMap::new();
                                m.insert("cpu".to_string(), Quantity("250m".into()));
                                m.insert("memory".to_string(), Quantity("1Gi".into()));
                                m
                            }),
                            limits: Some({
                                let mut m = BTreeMap::new();
                                m.insert("cpu".to_string(), Quantity("1".into()));
                                m.insert("memory".to_string(), Quantity("3Gi".into()));
                                m
                            }),
                            ..ResourceRequirements::default()
                        }),
                        security_context: Some(SecurityContext {
                            allow_privilege_escalation: Some(false),
                            capabilities: Some(Capabilities {
                                drop: Some(vec!["ALL".into()]),
                                ..Capabilities::default()
                            }),
                            ..SecurityContext::default()
                        }),
                        readiness_probe: Some(Probe {
                            tcp_socket: Some(TCPSocketAction {
                                port: IntOrString::String("connect".into()),
                                ..TCPSocketAction::default()
                            }),
                            initial_delay_seconds: Some(20),
                            period_seconds: Some(10),
                            ..Probe::default()
                        }),
                        liveness_probe: Some(Probe {
                            tcp_socket: Some(TCPSocketAction {
                                port: IntOrString::String("connect".into()),
                                ..TCPSocketAction::default()
                            }),
                            initial_delay_seconds: Some(60),
                            period_seconds: Some(20),
                            failure_threshold: Some(6),
                            ..Probe::default()
                        }),
                        volume_mounts: Some(vec![
                            VolumeMount {
                                name: "spark-defaults".into(),
                                mount_path: "/opt/spark/conf/spark-defaults.conf".into(),
                                sub_path: Some("spark-defaults.conf".into()),
                                read_only: Some(true),
                                ..VolumeMount::default()
                            },
                            VolumeMount {
                                name: "tmp".into(),
                                mount_path: "/tmp".into(),
                                ..VolumeMount::default()
                            },
                        ]),
                        ..Container::default()
                    }],
                    volumes: Some(vec![
                        Volume {
                            name: "spark-defaults".into(),
                            config_map: Some(ConfigMapVolumeSource {
                                name: CONFIG_MAP.into(),
                                ..ConfigMapVolumeSource::default()
                            }),
                            ..Volume::default()
                        },
                        Volume {
                            name: "tmp".into(),
                            empty_dir: Some(EmptyDirVolumeSource::default()),
                            ..Volume::default()
                        },
                    ]),
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        },
        ..Deployment::default()
    }
}

pub fn service(namespace: &str) -> Service {
    Service {
        metadata: meta_with_name_labels(NAME, namespace),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".into()),
            selector: Some(name_selector()),
            ports: Some(vec![ServicePort {
                name: Some("connect".into()),
                port: 15002,
                target_port: Some(IntOrString::String("connect".into())),
                ..ServicePort::default()
            }]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

    fn container_of(deployment: &Deployment) -> k8s_openapi::api::core::v1::Container {
        let pod = deployment.spec.clone().template.spec.unwrap();
        assert_eq!(pod.containers.len(), 1);
        pod.containers[0].clone()
    }

    #[test]
    fn the_config_map_carries_the_platform_defaults() {
        let cm = config_map("stardelt-op");
        let data = cm.data.expect("data");
        let conf = data
            .get("spark-defaults.conf")
            .expect("spark-defaults.conf");
        assert!(
            conf.contains("spark.kubernetes.namespace                              stardelt-op")
        );
        assert!(!conf.contains("__NAMESPACE__"));
        assert!(conf.contains("spark.connect.grpc.binding.port                         15002"));
    }

    #[test]
    fn the_image_is_pinned_by_digest() {
        let g = golden();
        assert!(g.image.starts_with("ghcr.io/stardelt/spark:dev@sha256:"));
        assert_eq!(
            g.image.len(),
            "ghcr.io/stardelt/spark:dev@sha256:".len() + 64
        );
        let dep = deployment("stardelt");
        let container = container_of(&dep);
        assert_eq!(container.image.as_ref().unwrap(), &g.image);
    }

    #[test]
    fn the_driver_reads_the_s3_secret_and_its_own_ip() {
        let dep = deployment("stardelt");
        let container = container_of(&dep);
        let env = container.env.as_ref().unwrap();
        let find = |name: &str| env.iter().find(|e| e.name == name).unwrap();
        // POD_IP from fieldRef
        let pod_ip = find("POD_IP");
        assert_eq!(
            pod_ip
                .value_from
                .as_ref()
                .unwrap()
                .field_ref
                .as_ref()
                .unwrap()
                .field_path,
            "status.podIP"
        );
        // S3 secret keys
        let ak = find("AWS_ACCESS_KEY_ID")
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .unwrap();
        assert_eq!(ak.name, "stardelt-s3-creds");
        assert_eq!(ak.key, "access-key");
        let sk = find("AWS_SECRET_ACCESS_KEY")
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .unwrap();
        assert_eq!(sk.name, "stardelt-s3-creds");
        assert_eq!(sk.key, "secret-key");
    }

    #[test]
    fn the_driver_is_replace_only_and_capped() {
        let dep = deployment("stardelt");
        let spec = &dep.spec;
        assert_eq!(spec.replicas, Some(1));
        assert_eq!(
            spec.strategy.as_ref().unwrap().type_.as_ref().unwrap(),
            "Recreate"
        );
        let container = container_of(&dep);
        let resources = container.resources.as_ref().unwrap();
        let req = resources.requests.as_ref().unwrap();
        let lim = resources.limits.as_ref().unwrap();
        assert_eq!(req.get("cpu").unwrap(), &Quantity("250m".into()));
        assert_eq!(req.get("memory").unwrap(), &Quantity("1Gi".into()));
        assert_eq!(lim.get("cpu").unwrap(), &Quantity("1".into()));
        assert_eq!(lim.get("memory").unwrap(), &Quantity("3Gi".into()));
    }

    #[test]
    fn the_service_is_cluster_ip_on_the_connect_port() {
        let svc = service("stardelt");
        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.type_.as_ref().unwrap(), "ClusterIP");
        let ports = spec.ports.as_ref().unwrap();
        assert_eq!(ports[0].port, 15002);
        assert_eq!(ports[0].name.as_ref().unwrap(), "connect");
        assert_eq!(
            ports[0].target_port.as_ref().unwrap(),
            &IntOrString::String("connect".into())
        );
        assert_eq!(
            spec.selector
                .as_ref()
                .unwrap()
                .get("app.kubernetes.io/name")
                .unwrap(),
            "spark-connect"
        );
    }

    #[test]
    fn the_role_manages_pods_and_nothing_cluster_wide() {
        let r = role("stardelt");
        let rules = r.rules.as_ref().unwrap();
        assert_eq!(rules.len(), 4);
        // pods
        assert_eq!(rules[0].api_groups.as_ref().unwrap(), &vec!["".to_string()]);
        assert_eq!(
            rules[0].resources.as_ref().unwrap(),
            &vec!["pods".to_string()]
        );
        assert_eq!(
            rules[0].verbs,
            vec![
                "get".to_string(),
                "list".to_string(),
                "watch".to_string(),
                "create".to_string(),
                "delete".to_string(),
                "deletecollection".to_string()
            ]
        );
        // services
        assert_eq!(
            rules[1].resources.as_ref().unwrap(),
            &vec!["services".to_string()]
        );
        assert_eq!(
            rules[1].verbs,
            vec![
                "get".to_string(),
                "list".to_string(),
                "create".to_string(),
                "delete".to_string(),
                "deletecollection".to_string()
            ]
        );
        // configmaps
        assert_eq!(
            rules[2].resources.as_ref().unwrap(),
            &vec!["configmaps".to_string()]
        );
        assert_eq!(
            rules[2].verbs,
            vec![
                "get".to_string(),
                "list".to_string(),
                "create".to_string(),
                "update".to_string(),
                "delete".to_string(),
                "deletecollection".to_string()
            ]
        );
        // persistentvolumeclaims
        assert_eq!(
            rules[3].resources.as_ref().unwrap(),
            &vec!["persistentvolumeclaims".to_string()]
        );
        assert_eq!(
            rules[3].verbs,
            vec![
                "get".to_string(),
                "list".to_string(),
                "create".to_string(),
                "delete".to_string(),
                "deletecollection".to_string()
            ]
        );
        // no cluster wide
        for rule in rules {
            if let Some(groups) = &rule.api_groups {
                assert!(!groups.contains(&"*".to_string()));
            }
        }
    }
}
