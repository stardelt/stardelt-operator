//! The Spark Connect endpoint: one long-running driver with Kubernetes as the cluster manager,
//! for compute that outgrows Trino. Pure builders; they mirror
//! stardelt-platform/manifests/spark.yaml object for object (operator#4, part 1).
//!
//! No Ingress and no authentication: Spark Connect has none of its own, so the Service stays
//! ClusterIP and only in-cluster clients reach it.

use std::collections::BTreeMap;

use k8s_openapi::api::{
    admissionregistration::v1::{
        MatchCondition, MatchResources, NamedRuleWithOperations, ValidatingAdmissionPolicy,
        ValidatingAdmissionPolicyBinding, ValidatingAdmissionPolicyBindingSpec,
        ValidatingAdmissionPolicySpec, Validation,
    },
    apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy},
    batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec},
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

/// The ServiceAccount a batch `SparkApplication`'s driver runs as (platform#1208). Never the
/// Connect executor's `spark-executor`: that one mounts no token and cannot create pods.
pub const BATCH_DRIVER: &str = "spark-batch-driver";

/// The StreamApp driver's identity (platform#1210 option B). Only stardelt's own
/// `stream_job.py` runs in it, which is what makes the `sp-pipeline-*` Secret read on its
/// executors safe; the shared Connect driver keeps the narrower rule.
pub const STREAM_DRIVER: &str = "spark-stream-driver";

/// The executor pod template (platform#1096 question 5, twin of
/// stardelt-platform#1171). Spark reads this file on the driver's filesystem when it
/// creates an executor pod and merges it into the generated pod; the container name is
/// Spark's default in the pinned image (`spark-kubernetes-executor`), so no
/// `spark.kubernetes.executor.podTemplateContainerName` is needed.
pub const EXECUTOR_POD_TEMPLATE: &str = r#"apiVersion: v1
kind: Pod
metadata:
  name: executor-pod-template
spec:
  securityContext:
    runAsNonRoot: true
    runAsUser: 185
    fsGroup: 185
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: spark-kubernetes-executor
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
"#;

/// The driver's Prometheus/UI port (stardelt-platform#1158); ClusterIP only.
pub const METRICS_PORT: i32 = 4040;

/// The ConfigMap janitor (platform#1173 option A, operator twin of platform#1258): every Spark
/// Connect session leaves its `spark-exec-<16 hex>-conf-map` behind, and the driver's Role may
/// not collect them (`config_map_guard_policy`). This CronJob deletes them with its own identity.
pub const CONFMAP_JANITOR: &str = "spark-confmap-janitor";

/// The janitor script, byte for byte from stardelt-platform's
/// `scripts/spark-confmap-janitor.sh`.
pub const CONFMAP_JANITOR_SCRIPT: &str = include_str!("spark_confmap_janitor.sh");

/// The kubectl image the platform manifest runs the janitor in.
pub const CONFMAP_JANITOR_IMAGE: &str = "alpine/k8s:1.37.1";

/// Executors run user code and never call the Kubernetes API (platform#1096): no Role is
/// bound to this ServiceAccount and its token is not mounted.
pub const EXECUTOR: &str = "spark-executor";

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

/// The metadata of an object that belongs to the platform but is not itself a selector target:
/// `app.kubernetes.io/part-of: stardelt` only, the same shape as
/// `stardelt-platform/manifests/spark-batch-rbac.yaml` and `airflow::meta()` (D44,
/// stardelt-operator#687: the batch driver is not the `spark-connect` Deployment, and a name
/// label no selector reads is a difference the two install paths do not need).
fn meta_part_of_only(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        labels: Some(part_of()),
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

/// The identity the executor pods run as: no Role is bound to it and the token is not mounted,
/// so user code inside an executor holds no Kubernetes API identity.
pub fn executor_service_account(namespace: &str) -> ServiceAccount {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), EXECUTOR.to_string());
    labels.insert(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    );
    ServiceAccount {
        metadata: ObjectMeta {
            name: Some(EXECUTOR.into()),
            namespace: Some(namespace.into()),
            labels: Some(labels),
            ..ObjectMeta::default()
        },
        automount_service_account_token: Some(false),
        ..ServiceAccount::default()
    }
}

/// Pods and one ConfigMap per session; no Services, no PVCs, and no `update` or
/// `deletecollection` on ConfigMaps (platform#1121, #1096 slice 3): a client's code runs
/// in the driver, and those verbs would rewrite or remove another workload's ConfigMap.
/// The two verbs that remain are bounded to `spark-*-conf-map` by `config_map_guard_policy`.
///
/// The same rules serve the batch driver ([`batch_driver_role`]): a `SparkApplication`'s
/// driver creates executor pods and turns its state into a ConfigMap too.
fn pod_and_config_map_rules() -> Vec<PolicyRule> {
    vec![
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
            resources: Some(vec!["configmaps".into()]),
            verbs: vec![
                "get".into(),
                "list".into(),
                "create".into(),
                "delete".into(),
            ],
            ..PolicyRule::default()
        },
    ]
}

pub fn role(namespace: &str) -> Role {
    Role {
        metadata: meta_with_name_labels(NAME, namespace),
        rules: Some(pod_and_config_map_rules()),
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

/// The identity a batch `SparkApplication`'s driver runs as (platform#1208). It keeps the default
/// `automountServiceAccountToken`: unlike [`executor_service_account`], the driver creates pods.
pub fn batch_driver_service_account(namespace: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: meta_part_of_only(BATCH_DRIVER, namespace),
        ..ServiceAccount::default()
    }
}

/// The batch driver's Role: the Connect driver's rules, not a copy, so the two cannot drift.
pub fn batch_driver_role(namespace: &str) -> Role {
    Role {
        metadata: meta_part_of_only(BATCH_DRIVER, namespace),
        rules: Some(pod_and_config_map_rules()),
    }
}

pub fn batch_driver_role_binding(namespace: &str) -> RoleBinding {
    RoleBinding {
        metadata: meta_part_of_only(BATCH_DRIVER, namespace),
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".into(),
            name: BATCH_DRIVER.into(),
            namespace: Some(namespace.into()),
            ..Subject::default()
        }]),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".into()),
            kind: "Role".into(),
            name: BATCH_DRIVER.into(),
        },
    }
}

/// The identity a `StreamApp`'s driver runs as (platform#1210 option B): its own, so the
/// admission policies can tell stardelt's `stream_job.py` apart from user code in the shared
/// Connect driver. It keeps the default `automountServiceAccountToken`: the driver creates pods.
pub fn stream_driver_service_account(namespace: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: meta_part_of_only(STREAM_DRIVER, namespace),
        ..ServiceAccount::default()
    }
}

/// The stream driver's Role: the same rules as the batch driver's ([`pod_and_config_map_rules`]).
pub fn stream_driver_role(namespace: &str) -> Role {
    Role {
        metadata: meta_part_of_only(STREAM_DRIVER, namespace),
        rules: Some(pod_and_config_map_rules()),
    }
}

pub fn stream_driver_role_binding(namespace: &str) -> RoleBinding {
    RoleBinding {
        metadata: meta_part_of_only(STREAM_DRIVER, namespace),
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".into(),
            name: STREAM_DRIVER.into(),
            namespace: Some(namespace.into()),
            ..Subject::default()
        }]),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".into()),
            kind: "Role".into(),
            name: STREAM_DRIVER.into(),
        },
    }
}

pub fn config_map(namespace: &str) -> ConfigMap {
    let mut golden = golden();
    golden.defaults = golden.defaults.replace("__NAMESPACE__", namespace);
    let mut data = BTreeMap::new();
    data.insert("spark-defaults.conf".to_string(), golden.defaults);
    data.insert(
        "executor-pod-template.yaml".to_string(),
        EXECUTOR_POD_TEMPLATE.to_string(),
    );
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
                                name: "spark-defaults".into(),
                                mount_path: "/opt/spark/conf/executor-pod-template.yaml".into(),
                                sub_path: Some("executor-pod-template.yaml".into()),
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

/// The janitor's identity. It keeps the default `automountServiceAccountToken`: the pod calls
/// the Kubernetes API and holds `delete` on ConfigMaps by name only.
pub fn confmap_janitor_service_account(namespace: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: meta_part_of_only(CONFMAP_JANITOR, namespace),
        automount_service_account_token: Some(true),
        ..ServiceAccount::default()
    }
}

/// Get, list and delete ConfigMaps — no `create`, no `update`, and no `deletecollection`
/// (platform#1121): a collection delete would take every ConfigMap in the namespace, and this
/// identity exists to remove one shape of them.
pub fn confmap_janitor_role(namespace: &str) -> Role {
    Role {
        metadata: meta_part_of_only(CONFMAP_JANITOR, namespace),
        rules: Some(vec![PolicyRule {
            api_groups: Some(vec!["".into()]),
            resources: Some(vec!["configmaps".into()]),
            verbs: vec!["get".into(), "list".into(), "delete".into()],
            ..PolicyRule::default()
        }]),
    }
}

pub fn confmap_janitor_role_binding(namespace: &str) -> RoleBinding {
    RoleBinding {
        metadata: meta_part_of_only(CONFMAP_JANITOR, namespace),
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".into(),
            name: CONFMAP_JANITOR.into(),
            namespace: Some(namespace.into()),
            ..Subject::default()
        }]),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".into()),
            kind: "Role".into(),
            name: CONFMAP_JANITOR.into(),
        },
    }
}

/// The script the CronJob mounts (key `spark-confmap-janitor.sh`, the platform's
/// `--from-file=spark-confmap-janitor.sh=`).
pub fn confmap_janitor_config_map(namespace: &str) -> ConfigMap {
    ConfigMap {
        metadata: meta_part_of_only(CONFMAP_JANITOR, namespace),
        data: Some(BTreeMap::from([(
            "spark-confmap-janitor.sh".to_string(),
            CONFMAP_JANITOR_SCRIPT.to_string(),
        )])),
        ..ConfigMap::default()
    }
}

/// Every 15 minutes, one run at a time, under the restricted profile: the image is the
/// platform's kubectl base and the script deletes only maps older than an hour.
pub fn confmap_janitor_cron_job(namespace: &str) -> CronJob {
    let mut labels = part_of();
    labels.insert(
        "app.kubernetes.io/name".to_string(),
        CONFMAP_JANITOR.to_string(),
    );
    let container = Container {
        name: "janitor".to_string(),
        image: Some(CONFMAP_JANITOR_IMAGE.to_string()),
        command: Some(vec![
            "bash".to_string(),
            "/opt/scripts/spark-confmap-janitor.sh".to_string(),
        ]),
        env: Some(vec![
            EnvVar {
                name: "NAMESPACE".into(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.namespace".into(),
                        ..ObjectFieldSelector::default()
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            },
            EnvVar {
                name: "HOME".into(),
                value: Some("/tmp".into()),
                ..EnvVar::default()
            },
        ]),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("50m".into())),
                ("memory".to_string(), Quantity("64Mi".into())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("500m".into())),
                ("memory".to_string(), Quantity("256Mi".into())),
            ])),
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
        volume_mounts: Some(vec![VolumeMount {
            name: "janitor-script".into(),
            mount_path: "/opt/scripts".into(),
            read_only: Some(true),
            ..VolumeMount::default()
        }]),
        ..Container::default()
    };

    CronJob {
        metadata: ObjectMeta {
            name: Some(CONFMAP_JANITOR.into()),
            namespace: Some(namespace.into()),
            labels: Some(labels.clone()),
            ..ObjectMeta::default()
        },
        spec: CronJobSpec {
            schedule: "*/15 * * * *".to_string(),
            concurrency_policy: Some("Forbid".to_string()),
            successful_jobs_history_limit: Some(1),
            failed_jobs_history_limit: Some(3),
            job_template: JobTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..ObjectMeta::default()
                }),
                spec: Some(JobSpec {
                    backoff_limit: Some(1),
                    template: PodTemplateSpec {
                        metadata: Some(ObjectMeta {
                            labels: Some(labels),
                            ..ObjectMeta::default()
                        }),
                        spec: Some(PodSpec {
                            restart_policy: Some("Never".into()),
                            service_account_name: Some(CONFMAP_JANITOR.into()),
                            security_context: Some(PodSecurityContext {
                                run_as_non_root: Some(true),
                                run_as_user: Some(65534),
                                seccomp_profile: Some(SeccompProfile {
                                    type_: "RuntimeDefault".into(),
                                    ..SeccompProfile::default()
                                }),
                                ..PodSecurityContext::default()
                            }),
                            containers: vec![container],
                            volumes: Some(vec![Volume {
                                name: "janitor-script".into(),
                                config_map: Some(ConfigMapVolumeSource {
                                    name: CONFMAP_JANITOR.into(),
                                    default_mode: Some(0o555),
                                    ..ConfigMapVolumeSource::default()
                                }),
                                ..Volume::default()
                            }]),
                            ..PodSpec::default()
                        }),
                    },
                    ..JobSpec::default()
                }),
            },
            ..CronJobSpec::default()
        },
        status: None,
    }
}

pub fn service(namespace: &str) -> Service {
    Service {
        metadata: meta_with_name_labels(NAME, namespace),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".into()),
            selector: Some(name_selector()),
            ports: Some(vec![
                ServicePort {
                    name: Some("connect".into()),
                    port: 15002,
                    target_port: Some(IntOrString::String("connect".into())),
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some("metrics".into()),
                    port: METRICS_PORT,
                    target_port: Some(IntOrString::Int(METRICS_PORT)),
                    ..ServicePort::default()
                },
            ]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    }
}

/// The pod guard's name. The platform's manifest uses the same string, so a
/// namespace cut over from `make install` keeps one policy object.
pub const POD_GUARD: &str = "spark-connect-executor-pods";

/// Cluster-scoped policy name for StreamApp executor pods.
pub const STREAM_POD_GUARD: &str = "spark-stream-executor-pods";

pub const CONFIG_MAP_GUARD: &str = "spark-connect-configmaps";

/// The batch path's pod guard (platform#1296 family, D44): a `SparkApplication`'s driver runs the
/// pipeline's Spark code, so its executor pods may read only `stardelt-s3-creds`.
pub const BATCH_POD_GUARD: &str = "spark-batch-executor-pods";
/// The batch path's ConfigMap guard: one `spark-exec-<hash>-conf-map` per application, nothing else.
pub const BATCH_CONFIG_MAP_GUARD: &str = "spark-batch-configmaps";
/// The stream path's ConfigMap guard: one `spark-exec-<hash>-conf-map` per application, nothing else.
pub const STREAM_CONFIG_MAP_GUARD: &str = "spark-stream-configmaps";

fn validation(expression: &str, message: &str) -> Validation {
    Validation {
        expression: expression.into(),
        message: Some(message.into()),
        ..Validation::default()
    }
}

fn rule(resources: &[&str], operations: &[&str]) -> NamedRuleWithOperations {
    NamedRuleWithOperations {
        api_groups: Some(vec![String::new()]),
        api_versions: Some(vec!["v1".into()]),
        operations: Some(operations.iter().map(|s| (*s).into()).collect()),
        resources: Some(resources.iter().map(|s| (*s).into()).collect()),
        ..NamedRuleWithOperations::default()
    }
}

/// What a pod created by the Spark Connect driver may be (platform#1096 question 2).
/// The allow-list is the measured executor shape; the platform manifest is the same
/// policy and `scripts/verify-spark-pod-guard.sh` is the live proof.
pub fn pod_guard_policy() -> ValidatingAdmissionPolicy {
    ValidatingAdmissionPolicy {
        metadata: ObjectMeta {
            name: Some(POD_GUARD.into()),
            ..ObjectMeta::default()
        },
        spec: Some(ValidatingAdmissionPolicySpec {
            failure_policy: Some("Fail".into()),
            match_constraints: Some(MatchResources {
                resource_rules: Some(vec![
                    rule(&["pods"], &["CREATE"]),
                    rule(&["pods/ephemeralcontainers"], &["UPDATE"]),
                ]),
                ..MatchResources::default()
            }),
            match_conditions: Some(vec![MatchCondition {
                name: "created-by-spark-connect".into(),
                expression: "request.userInfo.username == 'system:serviceaccount:' + request.namespace + ':spark-connect'".into(),
            }]),
            validations: Some(vec![
                validation("object.spec.serviceAccountName == 'spark-executor'", "a pod created by the Spark Connect driver must run as ServiceAccount spark-executor"),
                validation("!has(object.spec.hostNetwork) || object.spec.hostNetwork == false", "the Spark Connect driver may not create a host-network pod"),
                validation("!has(object.spec.hostPID) || object.spec.hostPID == false", "the Spark Connect driver may not create a host-PID pod"),
                validation("!has(object.spec.hostIPC) || object.spec.hostIPC == false", "the Spark Connect driver may not create a host-IPC pod"),
                validation("!has(object.spec.initContainers) || object.spec.initContainers.size() == 0", "the Spark Connect driver may not create a pod with init containers"),
                validation("!has(object.spec.ephemeralContainers) || object.spec.ephemeralContainers.size() == 0", "the Spark Connect driver may not add an ephemeral container"),
                validation("object.spec.containers.all(c, c.image.startsWith('ghcr.io/stardelt/spark:'))", "every container a Spark Connect driver creates must use the stardelt Spark image"),
                validation("object.spec.containers.all(c, !has(c.securityContext) || !has(c.securityContext.privileged) || c.securityContext.privileged != true)", "the Spark Connect driver may not create a privileged container"),
                validation("object.spec.containers.all(c, !has(c.ports) || c.ports.all(p, !has(p.hostPort) || p.hostPort == 0))", "the Spark Connect driver may not bind a host port"),
                validation("!has(object.spec.volumes) || object.spec.volumes.all(v, has(v.configMap) || has(v.emptyDir))", "executor pods may mount only configMap and emptyDir volumes (no secret, hostPath, PVC, projected or csi volume)"),
                validation("object.spec.containers.all(c, !has(c.env) || c.env.all(e, !has(e.valueFrom) || !has(e.valueFrom.secretKeyRef) || e.valueFrom.secretKeyRef.name == 'stardelt-s3-creds'))", "executor pods may read only Secret stardelt-s3-creds (secretKeyRef)"),
                validation("object.spec.containers.all(c, !has(c.envFrom) || c.envFrom.all(e, !has(e.secretRef) || e.secretRef.name == 'stardelt-s3-creds'))", "executor pods may read only Secret stardelt-s3-creds (envFrom)"),
            ]),
            ..ValidatingAdmissionPolicySpec::default()
        }),
        status: None,
    }
}

/// The binding is per namespace (the policy object itself is cluster-scoped and
/// shared): the second Lakehouse in a cluster adds a second binding, never a
/// second policy.
pub fn pod_guard_binding(namespace: &str) -> ValidatingAdmissionPolicyBinding {
    ValidatingAdmissionPolicyBinding {
        metadata: ObjectMeta {
            name: Some(format!("{POD_GUARD}-{namespace}")),
            ..ObjectMeta::default()
        },
        // `spec`, `spec.policyName` and `spec.validationActions` are required in
        // k8s-openapi 0.29's `latest` (v1.37), so they are not wrapped in `Some`.
        spec: ValidatingAdmissionPolicyBindingSpec {
            policy_name: POD_GUARD.into(),
            validation_actions: vec!["Deny".into()],
            match_resources: Some(MatchResources {
                namespace_selector: Some(LabelSelector {
                    match_labels: Some(BTreeMap::from([(
                        "kubernetes.io/metadata.name".to_string(),
                        namespace.to_string(),
                    )])),
                    match_expressions: None,
                }),
                ..MatchResources::default()
            }),
            ..ValidatingAdmissionPolicyBindingSpec::default()
        },
    }
}

/// What a pod created by the StreamApp driver may be (platform#1210 option B): the Connect
/// policy's allow-list, except that the identity is [`STREAM_DRIVER`] and an executor may read
/// the stream's own `sp-pipeline-<group>` catalog Secret — its Iceberg client refreshes the
/// token there. Only stardelt's `stream_job.py` runs as that identity, which is what keeps the
/// wider rule safe; the shared Connect driver keeps [`pod_guard_policy`]'s narrow one.
pub fn stream_pod_guard_policy() -> ValidatingAdmissionPolicy {
    ValidatingAdmissionPolicy {
        metadata: ObjectMeta {
            name: Some(STREAM_POD_GUARD.into()),
            ..ObjectMeta::default()
        },
        spec: Some(ValidatingAdmissionPolicySpec {
            failure_policy: Some("Fail".into()),
            match_constraints: Some(MatchResources {
                resource_rules: Some(vec![
                    rule(&["pods"], &["CREATE"]),
                    rule(&["pods/ephemeralcontainers"], &["UPDATE"]),
                ]),
                ..MatchResources::default()
            }),
            match_conditions: Some(vec![MatchCondition {
                name: "created-by-spark-stream-driver".into(),
                expression: "request.userInfo.username == 'system:serviceaccount:' + request.namespace + ':spark-stream-driver'".into(),
            }]),
            validations: Some(vec![
                validation("object.spec.serviceAccountName == 'spark-executor'", "a pod created by the Spark Connect driver must run as ServiceAccount spark-executor"),
                validation("!has(object.spec.hostNetwork) || object.spec.hostNetwork == false", "the Spark Connect driver may not create a host-network pod"),
                validation("!has(object.spec.hostPID) || object.spec.hostPID == false", "the Spark Connect driver may not create a host-PID pod"),
                validation("!has(object.spec.hostIPC) || object.spec.hostIPC == false", "the Spark Connect driver may not create a host-IPC pod"),
                validation("!has(object.spec.initContainers) || object.spec.initContainers.size() == 0", "the Spark Connect driver may not create a pod with init containers"),
                validation("!has(object.spec.ephemeralContainers) || object.spec.ephemeralContainers.size() == 0", "the Spark Connect driver may not add an ephemeral container"),
                validation("object.spec.containers.all(c, c.image.startsWith('ghcr.io/stardelt/spark:'))", "every container a Spark Connect driver creates must use the stardelt Spark image"),
                validation("object.spec.containers.all(c, !has(c.securityContext) || !has(c.securityContext.privileged) || c.securityContext.privileged != true)", "the Spark Connect driver may not create a privileged container"),
                validation("object.spec.containers.all(c, !has(c.ports) || c.ports.all(p, !has(p.hostPort) || p.hostPort == 0))", "the Spark Connect driver may not bind a host port"),
                validation("!has(object.spec.volumes) || object.spec.volumes.all(v, has(v.configMap) || has(v.emptyDir))", "executor pods may mount only configMap and emptyDir volumes (no secret, hostPath, PVC, projected or csi volume)"),
                validation("object.spec.containers.all(c, !has(c.env) || c.env.all(e, !has(e.valueFrom) || !has(e.valueFrom.secretKeyRef) || e.valueFrom.secretKeyRef.name == 'stardelt-s3-creds' || e.valueFrom.secretKeyRef.name.startsWith('sp-pipeline-')))", "a StreamApp executor may read only Secret stardelt-s3-creds and sp-pipeline-* (secretKeyRef)"),
                validation("object.spec.containers.all(c, !has(c.envFrom) || c.envFrom.all(e, !has(e.secretRef) || e.secretRef.name == 'stardelt-s3-creds' || e.secretRef.name.startsWith('sp-pipeline-')))", "a StreamApp executor may read only Secret stardelt-s3-creds and sp-pipeline-* (envFrom)"),
            ]),
            ..ValidatingAdmissionPolicySpec::default()
        }),
        status: None,
    }
}

/// The stream policy's binding, per namespace like [`pod_guard_binding`].
pub fn stream_pod_guard_binding(namespace: &str) -> ValidatingAdmissionPolicyBinding {
    ValidatingAdmissionPolicyBinding {
        metadata: ObjectMeta {
            name: Some(format!("{STREAM_POD_GUARD}-{namespace}")),
            ..ObjectMeta::default()
        },
        spec: ValidatingAdmissionPolicyBindingSpec {
            policy_name: STREAM_POD_GUARD.into(),
            validation_actions: vec!["Deny".into()],
            match_resources: Some(MatchResources {
                namespace_selector: Some(LabelSelector {
                    match_labels: Some(BTreeMap::from([(
                        "kubernetes.io/metadata.name".to_string(),
                        namespace.to_string(),
                    )])),
                    match_expressions: None,
                }),
                ..MatchResources::default()
            }),
            ..ValidatingAdmissionPolicyBindingSpec::default()
        },
    }
}

/// What a pod created by the batch driver may be (platform#1296, D44): [`pod_guard_policy`]'s
/// allow-list, except that the identity is [`BATCH_DRIVER`]. A `SparkApplication`'s driver runs
/// the pipeline's Spark code, so the batch identity gets no `sp-pipeline-*` arm: that Secret
/// belongs to a platform-authored stream job, not to a user's batch code.
pub fn batch_pod_guard_policy() -> ValidatingAdmissionPolicy {
    ValidatingAdmissionPolicy {
        metadata: ObjectMeta {
            name: Some(BATCH_POD_GUARD.into()),
            ..ObjectMeta::default()
        },
        spec: Some(ValidatingAdmissionPolicySpec {
            failure_policy: Some("Fail".into()),
            match_constraints: Some(MatchResources {
                resource_rules: Some(vec![
                    rule(&["pods"], &["CREATE"]),
                    rule(&["pods/ephemeralcontainers"], &["UPDATE"]),
                ]),
                ..MatchResources::default()
            }),
            match_conditions: Some(vec![MatchCondition {
                name: "created-by-spark-batch-driver".into(),
                expression: "request.userInfo.username == 'system:serviceaccount:' + request.namespace + ':spark-batch-driver'".into(),
            }]),
            validations: Some(vec![
                validation("object.spec.serviceAccountName == 'spark-executor'", "a pod created by the Spark Connect driver must run as ServiceAccount spark-executor"),
                validation("!has(object.spec.hostNetwork) || object.spec.hostNetwork == false", "the Spark Connect driver may not create a host-network pod"),
                validation("!has(object.spec.hostPID) || object.spec.hostPID == false", "the Spark Connect driver may not create a host-PID pod"),
                validation("!has(object.spec.hostIPC) || object.spec.hostIPC == false", "the Spark Connect driver may not create a host-IPC pod"),
                validation("!has(object.spec.initContainers) || object.spec.initContainers.size() == 0", "the Spark Connect driver may not create a pod with init containers"),
                validation("!has(object.spec.ephemeralContainers) || object.spec.ephemeralContainers.size() == 0", "the Spark Connect driver may not add an ephemeral container"),
                validation("object.spec.containers.all(c, c.image.startsWith('ghcr.io/stardelt/spark:'))", "every container a Spark Connect driver creates must use the stardelt Spark image"),
                validation("object.spec.containers.all(c, !has(c.securityContext) || !has(c.securityContext.privileged) || c.securityContext.privileged != true)", "the Spark Connect driver may not create a privileged container"),
                validation("object.spec.containers.all(c, !has(c.ports) || c.ports.all(p, !has(p.hostPort) || p.hostPort == 0))", "the Spark Connect driver may not bind a host port"),
                validation("!has(object.spec.volumes) || object.spec.volumes.all(v, has(v.configMap) || has(v.emptyDir))", "executor pods may mount only configMap and emptyDir volumes (no secret, hostPath, PVC, projected or csi volume)"),
                validation("object.spec.containers.all(c, !has(c.env) || c.env.all(e, !has(e.valueFrom) || !has(e.valueFrom.secretKeyRef) || e.valueFrom.secretKeyRef.name == 'stardelt-s3-creds'))", "executor pods may read only Secret stardelt-s3-creds (secretKeyRef)"),
                validation("object.spec.containers.all(c, !has(c.envFrom) || c.envFrom.all(e, !has(e.secretRef) || e.secretRef.name == 'stardelt-s3-creds'))", "executor pods may read only Secret stardelt-s3-creds (envFrom)"),
            ]),
            ..ValidatingAdmissionPolicySpec::default()
        }),
        status: None,
    }
}

/// The batch policy's binding, per namespace like [`pod_guard_binding`].
pub fn batch_pod_guard_binding(namespace: &str) -> ValidatingAdmissionPolicyBinding {
    ValidatingAdmissionPolicyBinding {
        metadata: ObjectMeta {
            name: Some(format!("{BATCH_POD_GUARD}-{namespace}")),
            ..ObjectMeta::default()
        },
        spec: ValidatingAdmissionPolicyBindingSpec {
            policy_name: BATCH_POD_GUARD.into(),
            validation_actions: vec!["Deny".into()],
            match_resources: Some(MatchResources {
                namespace_selector: Some(LabelSelector {
                    match_labels: Some(BTreeMap::from([(
                        "kubernetes.io/metadata.name".to_string(),
                        namespace.to_string(),
                    )])),
                    match_expressions: None,
                }),
                ..MatchResources::default()
            }),
            ..ValidatingAdmissionPolicyBindingSpec::default()
        },
    }
}

/// What ConfigMaps the batch driver may create or delete: the batch driver runs the pipeline's
/// Spark code, so without the name filter it could delete `trino-catalog` or `keycloak-realm`.
pub fn batch_config_map_guard_policy() -> ValidatingAdmissionPolicy {
    ValidatingAdmissionPolicy {
        metadata: ObjectMeta {
            name: Some(BATCH_CONFIG_MAP_GUARD.into()),
            ..ObjectMeta::default()
        },
        spec: Some(ValidatingAdmissionPolicySpec {
            failure_policy: Some("Fail".into()),
            match_constraints: Some(MatchResources {
                resource_rules: Some(vec![rule(&["configmaps"], &["CREATE", "DELETE"])]),
                ..MatchResources::default()
            }),
            match_conditions: Some(vec![MatchCondition {
                name: "created-by-spark-batch-driver".into(),
                expression: "request.userInfo.username == 'system:serviceaccount:' + request.namespace + ':spark-batch-driver'".into(),
            }]),
            validations: Some(vec![validation(
                "request.name.startsWith('spark-') && request.name.endsWith('-conf-map')",
                "the Spark Connect driver may create or delete only its own spark-*-conf-map ConfigMaps",
            )]),
            ..ValidatingAdmissionPolicySpec::default()
        }),
        status: None,
    }
}

/// The batch ConfigMap policy's binding, per namespace like [`config_map_guard_binding`].
pub fn batch_config_map_guard_binding(namespace: &str) -> ValidatingAdmissionPolicyBinding {
    ValidatingAdmissionPolicyBinding {
        metadata: ObjectMeta {
            name: Some(format!("{BATCH_CONFIG_MAP_GUARD}-{namespace}")),
            ..ObjectMeta::default()
        },
        spec: ValidatingAdmissionPolicyBindingSpec {
            policy_name: BATCH_CONFIG_MAP_GUARD.into(),
            validation_actions: vec!["Deny".into()],
            match_resources: Some(MatchResources {
                namespace_selector: Some(LabelSelector {
                    match_labels: Some(BTreeMap::from([(
                        "kubernetes.io/metadata.name".to_string(),
                        namespace.to_string(),
                    )])),
                    match_expressions: None,
                }),
                ..MatchResources::default()
            }),
            ..ValidatingAdmissionPolicyBindingSpec::default()
        },
    }
}

/// What ConfigMaps the stream driver may create or delete: one `spark-exec-<hash>-conf-map` per
/// application and nothing else. Without the name filter the StreamApp driver could delete
/// `trino-catalog` or `keycloak-realm`.
pub fn stream_config_map_guard_policy() -> ValidatingAdmissionPolicy {
    ValidatingAdmissionPolicy {
        metadata: ObjectMeta {
            name: Some(STREAM_CONFIG_MAP_GUARD.into()),
            ..ObjectMeta::default()
        },
        spec: Some(ValidatingAdmissionPolicySpec {
            failure_policy: Some("Fail".into()),
            match_constraints: Some(MatchResources {
                resource_rules: Some(vec![rule(&["configmaps"], &["CREATE", "DELETE"])]),
                ..MatchResources::default()
            }),
            match_conditions: Some(vec![MatchCondition {
                name: "created-by-spark-stream-driver".into(),
                expression: "request.userInfo.username == 'system:serviceaccount:' + request.namespace + ':spark-stream-driver'".into(),
            }]),
            validations: Some(vec![validation(
                "request.name.startsWith('spark-') && request.name.endsWith('-conf-map')",
                "the StreamApp driver may create or delete only its own spark-*-conf-map ConfigMaps",
            )]),
            ..ValidatingAdmissionPolicySpec::default()
        }),
        status: None,
    }
}

/// The stream ConfigMap policy's binding, per namespace like [`batch_config_map_guard_binding`].
pub fn stream_config_map_guard_binding(namespace: &str) -> ValidatingAdmissionPolicyBinding {
    ValidatingAdmissionPolicyBinding {
        metadata: ObjectMeta {
            name: Some(format!("{STREAM_CONFIG_MAP_GUARD}-{namespace}")),
            ..ObjectMeta::default()
        },
        spec: ValidatingAdmissionPolicyBindingSpec {
            policy_name: STREAM_CONFIG_MAP_GUARD.into(),
            validation_actions: vec!["Deny".into()],
            match_resources: Some(MatchResources {
                namespace_selector: Some(LabelSelector {
                    match_labels: Some(BTreeMap::from([(
                        "kubernetes.io/metadata.name".to_string(),
                        namespace.to_string(),
                    )])),
                    match_expressions: None,
                }),
                ..MatchResources::default()
            }),
            ..ValidatingAdmissionPolicyBindingSpec::default()
        },
    }
}

/// What ConfigMaps the driver may create or delete (platform#1121): one
/// `spark-exec-<hash>-conf-map` per session and nothing else. A client's code runs in the
/// driver, so without the name filter a session could create or delete any ConfigMap in
/// the namespace — `trino-catalog`, `keycloak-realm`, `airflow-config`.
pub fn config_map_guard_policy() -> ValidatingAdmissionPolicy {
    ValidatingAdmissionPolicy {
        metadata: ObjectMeta {
            name: Some(CONFIG_MAP_GUARD.into()),
            ..ObjectMeta::default()
        },
        spec: Some(ValidatingAdmissionPolicySpec {
            failure_policy: Some("Fail".into()),
            match_constraints: Some(MatchResources {
                resource_rules: Some(vec![rule(&["configmaps"], &["CREATE", "DELETE"])]),
                ..MatchResources::default()
            }),
            match_conditions: Some(vec![MatchCondition {
                name: "created-by-spark-connect".into(),
                expression: "request.userInfo.username == 'system:serviceaccount:' + request.namespace + ':spark-connect'".into(),
            }]),
            validations: Some(vec![validation(
                "request.name.startsWith('spark-') && request.name.endsWith('-conf-map')",
                "the Spark Connect driver may create or delete only its own spark-*-conf-map ConfigMaps",
            )]),
            ..ValidatingAdmissionPolicySpec::default()
        }),
        status: None,
    }
}

/// Per namespace, like the pod guard: a second Lakehouse adds a binding, never a second
/// policy.
pub fn config_map_guard_binding(namespace: &str) -> ValidatingAdmissionPolicyBinding {
    ValidatingAdmissionPolicyBinding {
        metadata: ObjectMeta {
            name: Some(format!("{CONFIG_MAP_GUARD}-{namespace}")),
            ..ObjectMeta::default()
        },
        spec: ValidatingAdmissionPolicyBindingSpec {
            policy_name: CONFIG_MAP_GUARD.into(),
            validation_actions: vec!["Deny".into()],
            match_resources: Some(MatchResources {
                namespace_selector: Some(LabelSelector {
                    match_labels: Some(BTreeMap::from([(
                        "kubernetes.io/metadata.name".to_string(),
                        namespace.to_string(),
                    )])),
                    match_expressions: None,
                }),
                ..MatchResources::default()
            }),
            ..ValidatingAdmissionPolicyBindingSpec::default()
        },
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
    fn the_config_map_declares_no_static_catalog_or_executor_count() {
        // The platform's shared server has no static catalog (the client configures its own) and
        // no static executor count (dynamic allocation owns it). The operator's golden defaults
        // must not reintroduce either, nor an event-log bucket no install creates.
        let cm = config_map("stardelt-op");
        let conf = cm
            .data
            .expect("data")
            .remove("spark-defaults.conf")
            .expect("defaults");
        for forbidden in [
            "spark.sql.catalog.",
            "spark.sql.defaultCatalog",
            "hive-metastore",
            "spark.executor.instances",
            "spark.eventLog",
            "spark.history.fs.logDirectory",
            "spark-events",
        ] {
            assert!(
                !conf.contains(forbidden),
                "the golden Spark defaults still carry {forbidden}:\n{conf}"
            );
        }
        // What must stay, by key: the Iceberg extensions, dynamic allocation, the Connect
        // listener, the UI metrics and the executor pod template. Column alignment is not the
        // contract, so compare the first whitespace-separated token.
        for key in [
            "spark.sql.extensions",
            "spark.dynamicAllocation.enabled",
            "spark.dynamicAllocation.maxExecutors",
            "spark.connect.grpc.binding.port",
            "spark.ui.prometheus.enabled",
            "spark.kubernetes.executor.podTemplateFile",
        ] {
            assert!(
                conf.lines()
                    .any(|line| line.split_whitespace().next() == Some(key)),
                "the golden Spark defaults lost {key}:\n{conf}"
            );
        }
    }

    #[test]
    fn the_config_map_turns_the_ui_and_its_metrics_on() {
        let cm = config_map("stardelt-op");
        let conf = cm
            .data
            .expect("data")
            .remove("spark-defaults.conf")
            .expect("defaults");
        assert!(conf.contains("spark.ui.enabled                                       true"));
        assert!(conf.contains("spark.ui.prometheus.enabled                            true"));
        assert!(!conf.contains("spark.ui.enabled                                       false"));
    }

    #[test]
    fn the_config_map_carries_the_executor_pod_template() {
        let cm = config_map("stardelt-op");
        let data = cm.data.expect("data");
        assert!(data.contains_key("spark-defaults.conf"));
        let template = data
            .get("executor-pod-template.yaml")
            .expect("executor-pod-template.yaml");
        assert!(template.contains("spark-kubernetes-executor"));
        assert!(template.contains("runAsNonRoot: true"));
        assert!(!template.contains("__NAMESPACE__"));
    }

    #[test]
    fn the_deployment_mounts_the_executor_pod_template() {
        let dep = deployment("stardelt");
        let container = container_of(&dep);
        let mounts = container.volume_mounts.expect("volume_mounts");
        let mount = mounts
            .iter()
            .find(|m| m.mount_path == "/opt/spark/conf/executor-pod-template.yaml")
            .expect("the driver mounts the executor pod template");
        assert_eq!(
            mount.sub_path.as_deref(),
            Some("executor-pod-template.yaml")
        );
        assert_eq!(mount.read_only, Some(true));
        assert_eq!(mount.name, "spark-defaults");

        let pod = dep.spec.template.spec.expect("pod spec");
        let volumes = pod.volumes.expect("volumes");
        let volume = volumes
            .iter()
            .find(|v| v.name == "spark-defaults")
            .expect("the spark-defaults volume");
        assert_eq!(volume.config_map.as_ref().unwrap().name, CONFIG_MAP);
    }

    #[test]
    fn the_executor_pod_template_declares_the_restricted_fields() {
        let pod: k8s_openapi::api::core::v1::Pod =
            serde_yaml::from_str(EXECUTOR_POD_TEMPLATE).expect("valid Pod YAML");
        let spec = pod.spec.expect("spec");
        let sc = spec.security_context.expect("pod securityContext");
        assert_eq!(sc.run_as_non_root, Some(true));
        assert_eq!(sc.run_as_user, Some(185));
        assert_eq!(sc.fs_group, Some(185));
        assert_eq!(sc.seccomp_profile.as_ref().unwrap().type_, "RuntimeDefault");

        let containers = spec.containers;
        assert_eq!(containers.len(), 1);
        let c = &containers[0];
        assert_eq!(c.name, "spark-kubernetes-executor");
        let csc = c.security_context.as_ref().unwrap();
        assert_eq!(csc.allow_privilege_escalation, Some(false));
        assert_eq!(
            csc.capabilities.as_ref().unwrap().drop.as_ref().unwrap(),
            &vec!["ALL".to_string()]
        );
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
    fn executors_run_the_same_image_as_the_driver() {
        let g = golden();
        assert!(
            g.defaults
                .contains(&format!("spark.kubernetes.container.image {}", g.image)),
            "the spark-defaults block must name the same digest as `image:` in values/spark.yaml"
        );
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
        assert_eq!(ports.len(), 2);
        assert_eq!(ports[0].port, 15002);
        assert_eq!(ports[0].name.as_ref().unwrap(), "connect");
        assert_eq!(
            ports[0].target_port.as_ref().unwrap(),
            &IntOrString::String("connect".into())
        );
        assert_eq!(ports[1].name, Some("metrics".to_string()));
        assert_eq!(ports[1].port, 4040);
        assert_eq!(ports[1].target_port, Some(IntOrString::Int(4040)));
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
    fn the_executor_service_account_mounts_no_token() {
        let sa = executor_service_account("data");
        assert_eq!(sa.metadata.name.as_deref(), Some("spark-executor"));
        assert_eq!(sa.metadata.namespace.as_deref(), Some("data"));
        assert_eq!(sa.automount_service_account_token, Some(false));
    }

    #[test]
    fn the_executor_is_not_bound_to_the_drivers_role() {
        let binding = role_binding("data");
        let subjects = binding.subjects.expect("subjects");
        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].name, "spark-connect");
        assert_eq!(subjects[0].kind, "ServiceAccount");
    }

    #[test]
    fn the_executor_uses_its_own_service_account() {
        let cm = config_map("data");
        let data = cm.data.expect("data");
        let conf = data
            .get("spark-defaults.conf")
            .expect("spark-defaults.conf");
        assert!(
            conf.contains(
                "spark.kubernetes.authenticate.executor.serviceAccountName spark-executor"
            ),
            "the rendered defaults must name the executor ServiceAccount"
        );
    }

    #[test]
    fn the_driver_keeps_its_token() {
        let sa = service_account("data");
        assert_eq!(sa.metadata.name.as_deref(), Some("spark-connect"));
        assert_eq!(
            sa.automount_service_account_token, None,
            "the driver's ServiceAccount keeps the default and mounts its token"
        );
    }

    #[test]
    fn the_role_manages_pods_and_its_own_configmaps_only() {
        let r = role("stardelt");
        let rules = r.rules.as_ref().unwrap();
        assert_eq!(rules.len(), 2);
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
        assert_eq!(
            rules[1].resources.as_ref().unwrap(),
            &vec!["configmaps".to_string()]
        );
        assert_eq!(
            rules[1].verbs,
            vec![
                "get".to_string(),
                "list".to_string(),
                "create".to_string(),
                "delete".to_string()
            ]
        );
        for rule in rules {
            let resources = rule.resources.as_ref().unwrap();
            assert!(!resources.contains(&"services".to_string()));
            assert!(!resources.contains(&"persistentvolumeclaims".to_string()));
            assert!(!rule.verbs.contains(&"update".to_string()));
        }
    }

    #[test]
    fn the_batch_driver_has_the_executor_rules_but_keeps_its_token() {
        let sa = batch_driver_service_account("stardelt");
        assert_eq!(sa.metadata.name.as_deref(), Some("spark-batch-driver"));
        assert_eq!(sa.metadata.namespace.as_deref(), Some("stardelt"));
        let labels = sa.metadata.labels.as_ref().expect("labels");
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            None,
            "D44 (stardelt-operator#687): the platform twin carries no name label"
        );
        assert_eq!(labels["app.kubernetes.io/part-of"], "stardelt");
        assert_eq!(
            sa.automount_service_account_token, None,
            "the batch driver creates pods, so it keeps the default and mounts its token"
        );
        let batch = batch_driver_role("stardelt");
        assert_eq!(batch.metadata.name.as_deref(), Some("spark-batch-driver"));
        assert_eq!(
            batch.rules,
            role("stardelt").rules,
            "the batch driver holds the Connect driver's rules, not a copy of them"
        );
    }

    #[test]
    fn the_batch_driver_binding_names_its_own_service_account() {
        let binding = batch_driver_role_binding("stardelt");
        assert_eq!(binding.role_ref.kind, "Role");
        assert_eq!(binding.role_ref.name, "spark-batch-driver");
        let subjects = binding.subjects.expect("subjects");
        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].kind, "ServiceAccount");
        assert_eq!(subjects[0].name, "spark-batch-driver");
        assert_eq!(subjects[0].namespace.as_deref(), Some("stardelt"));
    }

    #[test]
    fn the_stream_policy_matches_only_the_stream_driver() {
        let policy = stream_pod_guard_policy();
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("spark-stream-executor-pods")
        );
        assert!(policy.metadata.namespace.is_none());
        let spec = policy.spec.as_ref().expect("spec");
        let conditions = spec.match_conditions.as_ref().expect("match conditions");
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].name, "created-by-spark-stream-driver");
        assert!(conditions[0].expression.contains("spark-stream-driver"));
        assert!(!conditions[0].expression.contains(":spark-connect'"));
        let expressions: Vec<String> = spec
            .validations
            .as_ref()
            .expect("validations")
            .iter()
            .map(|v| v.expression.clone())
            .collect();
        let secret_rules: Vec<&String> = expressions
            .iter()
            .filter(|e| e.contains("secretKeyRef") || e.contains("secretRef"))
            .collect();
        assert_eq!(secret_rules.len(), 2);
        for rule in secret_rules {
            assert!(
                rule.contains("startsWith('sp-pipeline-')"),
                "a StreamApp executor may read its own catalog Secret: {rule}"
            );
        }
    }

    #[test]
    fn the_batch_policy_matches_only_the_batch_driver() {
        let policy = batch_pod_guard_policy();
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("spark-batch-executor-pods")
        );
        assert!(policy.metadata.namespace.is_none());
        let spec = policy.spec.as_ref().expect("spec");
        let conditions = spec.match_conditions.as_ref().expect("match conditions");
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].name, "created-by-spark-batch-driver");
        assert!(conditions[0].expression.contains(":spark-batch-driver'"));
        assert!(!conditions[0].expression.contains(":spark-connect'"));
        assert!(!conditions[0].expression.contains(":spark-stream-driver'"));
        assert_eq!(
            batch_pod_guard_binding("data").metadata.name.as_deref(),
            Some("spark-batch-executor-pods-data")
        );
        assert_eq!(
            batch_pod_guard_binding("data").spec.policy_name,
            "spark-batch-executor-pods"
        );
    }

    #[test]
    fn the_batch_policy_does_not_allow_catalog_secrets() {
        fn expressions(policy: &ValidatingAdmissionPolicy) -> Vec<String> {
            policy
                .spec
                .as_ref()
                .expect("spec")
                .validations
                .as_ref()
                .expect("validations")
                .iter()
                .map(|v| v.expression.clone())
                .collect()
        }
        fn secret_expressions(policy: &ValidatingAdmissionPolicy) -> Vec<String> {
            expressions(policy)
                .into_iter()
                .filter(|e| e.contains("secretKeyRef") || e.contains("secretRef"))
                .collect()
        }
        let batch = expressions(&batch_pod_guard_policy());
        let batch_secrets = secret_expressions(&batch_pod_guard_policy());
        assert_eq!(batch_secrets.len(), 2);
        assert_eq!(
            batch_secrets,
            secret_expressions(&pod_guard_policy()),
            "the batch driver gets the Connect driver's two Secret rules unchanged"
        );
        for expression in &batch {
            assert!(
                !expression.contains("startsWith('sp-pipeline-')"),
                "a batch driver must never admit a catalog Secret: {expression}"
            );
        }
    }

    #[test]
    fn the_batch_config_map_policy_matches_only_the_batch_driver() {
        let policy = batch_config_map_guard_policy();
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("spark-batch-configmaps")
        );
        assert!(policy.metadata.namespace.is_none());
        let spec = policy.spec.as_ref().expect("spec");
        let conditions = spec.match_conditions.as_ref().expect("match conditions");
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].name, "created-by-spark-batch-driver");
        assert!(conditions[0].expression.contains(":spark-batch-driver'"));
        assert!(!conditions[0].expression.contains(":spark-connect'"));
        assert!(!conditions[0].expression.contains(":spark-stream-driver'"));
        let validations: Vec<String> = spec
            .validations
            .as_ref()
            .expect("validations")
            .iter()
            .map(|v| v.expression.clone())
            .collect();
        assert_eq!(
            validations,
            vec![
                "request.name.startsWith('spark-') && request.name.endsWith('-conf-map')"
                    .to_string()
            ]
        );
        assert_eq!(
            batch_config_map_guard_binding("data")
                .metadata
                .name
                .as_deref(),
            Some("spark-batch-configmaps-data")
        );
        assert_eq!(
            batch_config_map_guard_binding("data").spec.policy_name,
            "spark-batch-configmaps"
        );
    }

    #[test]
    fn the_stream_config_map_policy_matches_only_the_stream_driver() {
        let policy = stream_config_map_guard_policy();
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("spark-stream-configmaps")
        );
        assert!(policy.metadata.namespace.is_none());
        let spec = policy.spec.as_ref().expect("spec");
        let conditions = spec.match_conditions.as_ref().expect("match conditions");
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].name, "created-by-spark-stream-driver");
        assert!(conditions[0].expression.contains(":spark-stream-driver'"));
        assert!(!conditions[0].expression.contains(":spark-connect'"));
        assert!(!conditions[0].expression.contains(":spark-batch-driver'"));
        let validations: Vec<String> = spec
            .validations
            .as_ref()
            .expect("validations")
            .iter()
            .map(|v| v.expression.clone())
            .collect();
        assert_eq!(
            validations,
            vec![
                "request.name.startsWith('spark-') && request.name.endsWith('-conf-map')"
                    .to_string()
            ]
        );
        assert_eq!(
            stream_config_map_guard_binding("data")
                .metadata
                .name
                .as_deref(),
            Some("spark-stream-configmaps-data")
        );
        assert_eq!(
            stream_config_map_guard_binding("data").spec.policy_name,
            "spark-stream-configmaps"
        );
    }

    #[test]
    fn the_connect_policy_does_not_allow_catalog_secrets() {
        let policy = pod_guard_policy();
        let expressions: Vec<String> = policy
            .spec
            .as_ref()
            .expect("spec")
            .validations
            .as_ref()
            .expect("validations")
            .iter()
            .map(|v| v.expression.clone())
            .collect();
        for expression in &expressions {
            assert!(
                !expression.contains("sp-pipeline-"),
                "the shared Connect driver must never admit a catalog Secret: {expression}"
            );
        }
    }

    #[test]
    fn the_stream_driver_has_the_batch_driver_rules() {
        assert_eq!(
            stream_driver_role("stardelt").rules,
            batch_driver_role("stardelt").rules,
            "the stream driver holds the batch driver's rules, so the two cannot drift"
        );
        let sa = stream_driver_service_account("stardelt");
        assert_eq!(sa.metadata.name.as_deref(), Some("spark-stream-driver"));
        assert_eq!(sa.metadata.namespace.as_deref(), Some("stardelt"));
        let labels = sa.metadata.labels.as_ref().expect("labels");
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            None,
            "the platform twin carries no name label (D44)"
        );
        assert_eq!(labels["app.kubernetes.io/part-of"], "stardelt");
        assert_eq!(
            sa.automount_service_account_token, None,
            "the stream driver creates pods, so it keeps the default and mounts its token"
        );
        let binding = stream_driver_role_binding("stardelt");
        assert_eq!(binding.role_ref.kind, "Role");
        assert_eq!(binding.role_ref.name, "spark-stream-driver");
        let subjects = binding.subjects.expect("subjects");
        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].name, "spark-stream-driver");
        assert_eq!(subjects[0].namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            stream_pod_guard_binding("data").metadata.name.as_deref(),
            Some("spark-stream-executor-pods-data")
        );
    }

    #[test]
    fn pod_guard_is_cluster_scoped() {
        let policy = pod_guard_policy();
        assert!(policy.metadata.namespace.is_none());
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("spark-connect-executor-pods")
        );
    }

    #[test]
    fn pod_guard_fails_closed() {
        let policy = pod_guard_policy();
        let spec = policy.spec.as_ref().expect("spec");
        assert_eq!(spec.failure_policy.as_deref(), Some("Fail"));
        let binding = pod_guard_binding("data");
        let binding_spec = &binding.spec;
        assert_eq!(binding_spec.validation_actions, vec!["Deny".to_string()]);
        let selector = binding_spec
            .match_resources
            .as_ref()
            .expect("match resources")
            .namespace_selector
            .as_ref()
            .expect("namespace selector");
        let labels = selector.match_labels.as_ref().expect("match labels");
        assert_eq!(
            labels
                .get("kubernetes.io/metadata.name")
                .map(String::as_str),
            Some("data")
        );
    }

    #[test]
    fn pod_guard_covers_pods_and_ephemeral_containers() {
        let policy = pod_guard_policy();
        let rules = policy
            .spec
            .as_ref()
            .expect("spec")
            .match_constraints
            .as_ref()
            .expect("match constraints")
            .resource_rules
            .as_ref()
            .expect("resource rules")
            .clone();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].resources, Some(vec!["pods".to_string()]));
        assert_eq!(rules[0].operations, Some(vec!["CREATE".to_string()]));
        assert_eq!(
            rules[1].resources,
            Some(vec!["pods/ephemeralcontainers".to_string()])
        );
        assert_eq!(rules[1].operations, Some(vec!["UPDATE".to_string()]));
        for rule in &rules {
            assert_eq!(rule.api_groups, Some(vec![String::new()]));
            assert_eq!(rule.api_versions, Some(vec!["v1".to_string()]));
        }
    }

    #[test]
    fn pod_guard_has_twelve_validations() {
        let policy = pod_guard_policy();
        let actual: Vec<String> = policy
            .spec
            .as_ref()
            .expect("spec")
            .validations
            .as_ref()
            .expect("validations")
            .iter()
            .map(|v| v.expression.clone())
            .collect();
        let expected = vec![
            "object.spec.serviceAccountName == 'spark-executor'",
            "!has(object.spec.hostNetwork) || object.spec.hostNetwork == false",
            "!has(object.spec.hostPID) || object.spec.hostPID == false",
            "!has(object.spec.hostIPC) || object.spec.hostIPC == false",
            "!has(object.spec.initContainers) || object.spec.initContainers.size() == 0",
            "!has(object.spec.ephemeralContainers) || object.spec.ephemeralContainers.size() == 0",
            "object.spec.containers.all(c, c.image.startsWith('ghcr.io/stardelt/spark:'))",
            "object.spec.containers.all(c, !has(c.securityContext) || !has(c.securityContext.privileged) || c.securityContext.privileged != true)",
            "object.spec.containers.all(c, !has(c.ports) || c.ports.all(p, !has(p.hostPort) || p.hostPort == 0))",
            "!has(object.spec.volumes) || object.spec.volumes.all(v, has(v.configMap) || has(v.emptyDir))",
            "object.spec.containers.all(c, !has(c.env) || c.env.all(e, !has(e.valueFrom) || !has(e.valueFrom.secretKeyRef) || e.valueFrom.secretKeyRef.name == 'stardelt-s3-creds'))",
            "object.spec.containers.all(c, !has(c.envFrom) || c.envFrom.all(e, !has(e.secretRef) || e.secretRef.name == 'stardelt-s3-creds'))",
        ];
        assert_eq!(actual.len(), 12);
        assert_eq!(actual, expected);
        for expression in &expected {
            assert_eq!(
                actual.iter().filter(|a| a.as_str() == *expression).count(),
                1,
                "{expression} must appear exactly once"
            );
        }
    }

    #[test]
    fn pod_guard_binding_name_carries_the_namespace() {
        assert_eq!(
            pod_guard_binding("data").metadata.name.as_deref(),
            Some("spark-connect-executor-pods-data")
        );
    }

    #[test]
    fn config_map_guard_bounds_create_and_delete_by_name() {
        let policy = config_map_guard_policy();
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("spark-connect-configmaps")
        );
        assert!(policy.metadata.namespace.is_none());
        let spec = policy.spec.as_ref().expect("spec");
        assert_eq!(spec.failure_policy.as_deref(), Some("Fail"));
        assert_eq!(
            spec.match_constraints
                .as_ref()
                .unwrap()
                .resource_rules
                .as_ref()
                .unwrap(),
            &vec![rule(&["configmaps"], &["CREATE", "DELETE"])]
        );
        let expressions: Vec<&str> = spec
            .validations
            .as_ref()
            .expect("validations")
            .iter()
            .map(|v| v.expression.as_str())
            .collect();
        assert_eq!(
            expressions,
            vec!["request.name.startsWith('spark-') && request.name.endsWith('-conf-map')"]
        );
    }

    #[test]
    fn config_map_guard_applies_to_the_driver_only() {
        let policy = config_map_guard_policy();
        let conditions = policy
            .spec
            .as_ref()
            .unwrap()
            .match_conditions
            .as_ref()
            .unwrap();
        assert!(
            conditions
                .iter()
                .any(|c| c.expression.contains(":spark-connect'"))
        );
    }

    #[test]
    fn config_map_guard_binding_name_carries_the_namespace() {
        let binding = config_map_guard_binding("data");
        assert_eq!(
            binding.metadata.name.as_deref(),
            Some("spark-connect-configmaps-data")
        );
        assert_eq!(binding.spec.policy_name, "spark-connect-configmaps");
        assert_eq!(binding.spec.validation_actions, vec!["Deny".to_string()]);
        let selector = binding
            .spec
            .match_resources
            .unwrap()
            .namespace_selector
            .unwrap();
        assert_eq!(
            selector
                .match_labels
                .unwrap()
                .get("kubernetes.io/metadata.name")
                .map(String::as_str),
            Some("data")
        );
    }

    #[test]
    fn the_confmap_janitor_may_only_delete_configmaps_by_name() {
        let janitor = confmap_janitor_role("stardelt");
        assert_eq!(
            janitor.metadata.name.as_deref(),
            Some("spark-confmap-janitor")
        );
        let rules = janitor.rules.as_ref().expect("rules");
        assert_eq!(rules.len(), 1, "the janitor holds exactly one rule");
        let rule = &rules[0];
        assert_eq!(rule.api_groups, Some(vec![String::new()]));
        assert_eq!(
            rule.resources.as_ref().unwrap(),
            &vec!["configmaps".to_string()]
        );
        assert_eq!(
            rule.verbs,
            vec!["get".to_string(), "list".to_string(), "delete".to_string()]
        );
        assert!(
            !rule.verbs.contains(&"deletecollection".to_string()),
            "a collection delete would take every ConfigMap in the namespace (platform#1121)"
        );
        assert!(rule.resource_names.is_none(), "the script filters by name");
    }

    #[test]
    fn the_confmap_janitor_runs_restricted() {
        let cron = confmap_janitor_cron_job("stardelt");
        assert_eq!(cron.metadata.name.as_deref(), Some("spark-confmap-janitor"));
        let spec = &cron.spec;
        assert_eq!(spec.schedule, "*/15 * * * *");
        assert_eq!(spec.concurrency_policy.as_deref(), Some("Forbid"));
        assert_eq!(spec.successful_jobs_history_limit, Some(1));
        assert_eq!(spec.failed_jobs_history_limit, Some(3));
        let job_spec = spec.job_template.spec.as_ref().expect("job spec");
        assert_eq!(job_spec.backoff_limit, Some(1));
        let pod = job_spec.template.spec.as_ref().expect("pod spec");
        assert_eq!(
            pod.service_account_name.as_deref(),
            Some("spark-confmap-janitor")
        );
        let pod_sc = pod.security_context.as_ref().expect("pod securityContext");
        assert_eq!(pod_sc.run_as_non_root, Some(true));
        assert_eq!(pod_sc.run_as_user, Some(65534));
        assert_eq!(
            pod_sc.seccomp_profile.as_ref().unwrap().type_,
            "RuntimeDefault"
        );

        assert_eq!(pod.containers.len(), 1);
        let c = &pod.containers[0];
        assert_eq!(c.name, "janitor");
        assert_eq!(c.image.as_deref(), Some(CONFMAP_JANITOR_IMAGE));
        assert_eq!(
            c.command,
            Some(vec![
                "bash".to_string(),
                "/opt/scripts/spark-confmap-janitor.sh".to_string()
            ])
        );
        let c_sc = c
            .security_context
            .as_ref()
            .expect("container securityContext");
        assert_eq!(c_sc.allow_privilege_escalation, Some(false));
        assert_eq!(
            c_sc.capabilities.as_ref().unwrap().drop.as_ref().unwrap(),
            &vec!["ALL".to_string()]
        );

        let mount = c
            .volume_mounts
            .as_ref()
            .expect("volume_mounts")
            .iter()
            .find(|m| m.mount_path == "/opt/scripts")
            .expect("the script is mounted at /opt/scripts");
        assert_eq!(mount.read_only, Some(true));
        let volume = pod
            .volumes
            .as_ref()
            .expect("volumes")
            .iter()
            .find(|v| v.name == mount.name)
            .expect("the script volume");
        let source = volume.config_map.as_ref().expect("configMap volume");
        assert_eq!(source.name, "spark-confmap-janitor");
        assert_eq!(source.default_mode, Some(0o555));

        let data = confmap_janitor_config_map("stardelt").data.expect("data");
        let script = data
            .get("spark-confmap-janitor.sh")
            .expect("the ConfigMap key is spark-confmap-janitor.sh");
        assert!(!script.is_empty());
        assert!(script.starts_with("#!/usr/bin/env bash"));
    }
}
