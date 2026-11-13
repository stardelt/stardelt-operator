//! Airflow (the `pipelines` pillar of `design/operator-parity.md`, step O6): builders for the
//! Role/binding, Secrets, and group-roles sync CronJob applied by the controller/provision step.
//!
//! Every field mirrors `stardelt-platform`: the Role is `manifests/airflow-pipelines.yaml`, the
//! Secrets are the Airflow part of `scripts/app-secrets.sh`, and the group-roles job mirrors
//! `manifests/airflow-group-roles.yaml`.

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMap, ConfigMapVolumeSource, Container, EnvVar, EnvVarSource,
    PodSecurityContext, PodSpec, PodTemplateSpec, ResourceRequirements, SeccompProfile,
    SecretKeySelector, SecurityContext, ServiceAccount, Volume, VolumeMount,
};
use k8s_openapi::api::rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ObjectMeta;

use crate::keycloak::IdentitySecret;

/// Role that lets Airflow read the `Pipeline` and `ServicePrincipal` resources it turns into DAGs.
pub const PIPELINE_READER_ROLE: &str = "airflow-pipeline-reader";
/// The ServiceAccounts of the Airflow chart that read them: the DAG processor, which builds the
/// DAGs, the worker, which runs the tasks, and the scheduler, which parses the bundle in Airflow 3
/// (so `airflow dags test` in the scheduler pod works — platform#1170).
pub const PIPELINE_READERS: [&str; 3] = [
    "airflow-dag-processor",
    "airflow-worker",
    "airflow-scheduler",
];

fn meta(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        labels: Some(BTreeMap::from([(
            "app.kubernetes.io/part-of".to_string(),
            "stardelt".to_string(),
        )])),
        ..ObjectMeta::default()
    }
}

/// Read access to the pipelines, the service principals and the connections of the namespace,
/// nothing else. The loader also resolves a task's `connection` against the namespace's
/// `Connection` resources; the Secret is never read here.
pub fn pipeline_reader_role(namespace: &str) -> Role {
    Role {
        metadata: meta(PIPELINE_READER_ROLE, namespace),
        rules: Some(vec![PolicyRule {
            api_groups: Some(vec!["stardelt.io".into()]),
            resources: Some(vec![
                "pipelines".into(),
                "serviceprincipals".into(),
                "connections".into(),
            ]),
            verbs: vec!["get".into(), "list".into(), "watch".into()],
            ..PolicyRule::default()
        }]),
    }
}

/// The binding of [`pipeline_reader_role`] to [`PIPELINE_READERS`].
pub fn pipeline_reader_binding(namespace: &str) -> RoleBinding {
    RoleBinding {
        metadata: meta(PIPELINE_READER_ROLE, namespace),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".into()),
            kind: "Role".into(),
            name: PIPELINE_READER_ROLE.into(),
        },
        subjects: Some(
            PIPELINE_READERS
                .iter()
                .map(|name| Subject {
                    kind: "ServiceAccount".into(),
                    name: (*name).into(),
                    namespace: Some(namespace.into()),
                    ..Subject::default()
                })
                .collect(),
        ),
    }
}

/// Name of the ServiceAccount, ConfigMap, RoleBinding and CronJob that create the FAB roles.
pub const GROUP_ROLES_JOB: &str = "airflow-group-roles";

/// The image the Job runs: the same Airflow build the chart runs (`images.airflow` in the
/// golden values), by digest.
pub fn image() -> String {
    let values = crate::values::golden_values("airflow");
    let repository = values["images"]["airflow"]["repository"]
        .as_str()
        .expect("values/airflow.yaml has images.airflow.repository");
    let digest = values["images"]["airflow"]["digest"]
        .as_str()
        .expect("values/airflow.yaml has images.airflow.digest");
    format!("{repository}@{digest}")
}

pub fn group_roles_service_account(namespace: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: meta(GROUP_ROLES_JOB, namespace),
        automount_service_account_token: Some(true),
        ..ServiceAccount::default()
    }
}

pub fn group_roles_config_map(namespace: &str) -> ConfigMap {
    ConfigMap {
        metadata: meta(GROUP_ROLES_JOB, namespace),
        data: Some(BTreeMap::from([(
            "sync.py".to_string(),
            include_str!("airflow_group_roles.py").to_string(),
        )])),
        ..ConfigMap::default()
    }
}

pub fn group_roles_binding(namespace: &str) -> RoleBinding {
    RoleBinding {
        metadata: meta(GROUP_ROLES_JOB, namespace),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".into()),
            kind: "Role".into(),
            name: PIPELINE_READER_ROLE.into(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".into(),
            name: GROUP_ROLES_JOB.into(),
            namespace: Some(namespace.into()),
            ..Subject::default()
        }]),
    }
}

pub fn group_roles_cron_job(namespace: &str, image: &str) -> CronJob {
    let container = Container {
        name: "sync".to_string(),
        image: Some(image.to_string()),
        command: Some(vec![
            "python".to_string(),
            "/opt/scripts/sync.py".to_string(),
        ]),
        env: Some(vec![
            // The chart sets the FAB auth manager for the Airflow deployments; this pod mounts no
            // airflow.cfg, so without it `get_application_builder()` hands back
            // AirflowSecurityManagerV2, which has no find_role/create_permission/
            // add_permission_to_role (platform#994).
            EnvVar {
                name: "AIRFLOW__CORE__AUTH_MANAGER".to_string(),
                value: Some(
                    "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
                        .to_string(),
                ),
                ..EnvVar::default()
            },
            EnvVar {
                name: "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "airflow-metadata".to_string(),
                        key: "connection".to_string(),
                        optional: None,
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            },
            EnvVar {
                name: "AIRFLOW__API__SECRET_KEY".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "airflow-webserver-key".to_string(),
                        key: "api-secret-key".to_string(),
                        optional: None,
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            },
            EnvVar {
                name: "AIRFLOW__API_AUTH__JWT_SECRET".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "airflow-jwt-secret".to_string(),
                        key: "jwt-secret".to_string(),
                        optional: None,
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            },
            EnvVar {
                name: "AIRFLOW__CORE__FERNET_KEY".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: "airflow-fernet-key".to_string(),
                        key: "fernet-key".to_string(),
                        optional: None,
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            },
        ]),
        volume_mounts: Some(vec![VolumeMount {
            name: "scripts".to_string(),
            mount_path: "/opt/scripts".to_string(),
            read_only: Some(true),
            ..VolumeMount::default()
        }]),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("50m".to_string())),
                ("memory".to_string(), Quantity("64Mi".to_string())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("500m".to_string())),
                ("memory".to_string(), Quantity("256Mi".to_string())),
            ])),
            ..ResourceRequirements::default()
        }),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Capabilities::default()
            }),
            ..SecurityContext::default()
        }),
        ..Container::default()
    };

    CronJob {
        metadata: meta(GROUP_ROLES_JOB, namespace),
        spec: CronJobSpec {
            schedule: "*/5 * * * *".to_string(),
            concurrency_policy: Some("Forbid".to_string()),
            failed_jobs_history_limit: Some(3),
            successful_jobs_history_limit: Some(1),
            starting_deadline_seconds: None,
            suspend: None,
            time_zone: None,
            job_template: JobTemplateSpec {
                metadata: Some(meta(GROUP_ROLES_JOB, namespace)),
                spec: Some(JobSpec {
                    backoff_limit: Some(1),
                    template: PodTemplateSpec {
                        metadata: Some(meta(GROUP_ROLES_JOB, namespace)),
                        spec: Some(PodSpec {
                            restart_policy: Some("Never".to_string()),
                            service_account_name: Some(GROUP_ROLES_JOB.to_string()),
                            security_context: Some(PodSecurityContext {
                                run_as_non_root: Some(true),
                                run_as_user: Some(50000),
                                seccomp_profile: Some(SeccompProfile {
                                    type_: "RuntimeDefault".into(),
                                    localhost_profile: None,
                                }),
                                ..PodSecurityContext::default()
                            }),
                            containers: vec![container],
                            volumes: Some(vec![Volume {
                                name: "scripts".to_string(),
                                config_map: Some(ConfigMapVolumeSource {
                                    name: GROUP_ROLES_JOB.to_string(),
                                    default_mode: Some(0o444),
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
        },
        status: None,
    }
}

/// The Secret that holds the service user Nova signs in to Airflow with (keys `username`, `password`).
pub const NOVA_API_SECRET: &str = "airflow-nova-api";

/// The Secrets Airflow reads, with the names and keys of `scripts/app-secrets.sh`: the chart's
/// Secret-reference fields (`fernetKeySecretName`, `apiSecretKeySecretName`,
/// `webserverSecretKeySecretName`, `jwtSecretName`), the local administrator and the service user
/// that Nova signs in with. Created once and never rewritten: rotating the Fernet key makes every
/// connection Airflow stored unreadable.
pub const SECRETS: [IdentitySecret; 5] = [
    IdentitySecret {
        name: "airflow-fernet-key",
        fixed: &[],
        generated: &[],
        fernet: &["fernet-key"],
    },
    IdentitySecret {
        name: "airflow-webserver-key",
        fixed: &[],
        generated: &["webserver-secret-key", "api-secret-key"],
        fernet: &[],
    },
    IdentitySecret {
        name: "airflow-jwt-secret",
        fixed: &[],
        generated: &["jwt-secret"],
        fernet: &[],
    },
    IdentitySecret {
        name: "airflow-admin",
        fixed: &[("username", "admin")],
        generated: &["password"],
        fernet: &[],
    },
    IdentitySecret {
        name: NOVA_API_SECRET,
        fixed: &[("username", "nova-api")],
        generated: &["password"],
        fernet: &[],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_group_roles_job_runs_every_five_minutes_on_the_pinned_image() {
        let job = group_roles_cron_job("stardelt", &image());
        let spec = &job.spec;
        assert_eq!(spec.schedule, "*/5 * * * *");
        assert_eq!(spec.concurrency_policy.as_deref(), Some("Forbid"));
        assert_eq!(spec.successful_jobs_history_limit, Some(1));
        assert_eq!(spec.failed_jobs_history_limit, Some(3));
        let job_spec = spec.job_template.spec.as_ref().unwrap();
        assert_eq!(job_spec.backoff_limit, Some(1));
        let pod_spec = job_spec.template.spec.as_ref().unwrap();
        assert_eq!(pod_spec.restart_policy.as_deref(), Some("Never"));
        let container = &pod_spec.containers[0];
        assert_eq!(container.name, "sync");
        let expected = {
            let values = crate::values::golden_values("airflow");
            format!(
                "{}@{}",
                values["images"]["airflow"]["repository"].as_str().unwrap(),
                values["images"]["airflow"]["digest"].as_str().unwrap()
            )
        };
        assert_eq!(container.image.as_deref(), Some(expected.as_str()));
        assert_eq!(image(), expected);
    }

    #[test]
    fn the_group_roles_job_declares_the_restricted_pod_fields() {
        let job = group_roles_cron_job("stardelt", &image());
        let pod = job
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap();
        let sc = pod.security_context.as_ref().expect("pod securityContext");
        assert_eq!(sc.run_as_non_root, Some(true));
        assert_eq!(sc.run_as_user, Some(50000));
        assert_eq!(sc.seccomp_profile.as_ref().unwrap().type_, "RuntimeDefault");
        let csc = pod.containers[0]
            .security_context
            .as_ref()
            .expect("container securityContext");
        assert_eq!(csc.allow_privilege_escalation, Some(false));
        assert_eq!(
            csc.capabilities.as_ref().unwrap().drop.as_ref().unwrap(),
            &vec!["ALL".to_string()]
        );
    }

    #[test]
    fn the_group_roles_job_mounts_the_script_and_reads_the_airflow_secrets() {
        let cm = group_roles_config_map("stardelt");
        let data = cm.data.as_ref().unwrap();
        let script = data.get("sync.py").unwrap();
        assert!(script.contains("get_application_builder"));
        assert!(script.contains("ownerGroup"));
        assert!(script.contains("stardelt-group-"));
        assert!(script.contains("Task Instances"));
        assert!(script.contains("Task Logs"));
        let job = group_roles_cron_job("stardelt", &image());
        let env = job
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .env
            .as_ref()
            .unwrap();
        let names: Vec<_> = env.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"));
        assert!(names.contains(&"AIRFLOW__API__SECRET_KEY"));
        assert!(names.contains(&"AIRFLOW__API_AUTH__JWT_SECRET"));
        assert!(names.contains(&"AIRFLOW__CORE__FERNET_KEY"));
        assert_eq!(
            env.iter()
                .find(|e| e.name == "AIRFLOW__CORE__AUTH_MANAGER")
                .unwrap()
                .value
                .as_deref(),
            Some("airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager")
        );
        assert_eq!(
            env.iter()
                .find(|e| e.name == "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
                .unwrap()
                .value_from
                .as_ref()
                .unwrap()
                .secret_key_ref
                .as_ref()
                .unwrap()
                .name,
            "airflow-metadata"
        );
        assert_eq!(
            env.iter()
                .find(|e| e.name == "AIRFLOW__API__SECRET_KEY")
                .unwrap()
                .value_from
                .as_ref()
                .unwrap()
                .secret_key_ref
                .as_ref()
                .unwrap()
                .key,
            "api-secret-key"
        );
        assert_eq!(
            env.iter()
                .find(|e| e.name == "AIRFLOW__API_AUTH__JWT_SECRET")
                .unwrap()
                .value_from
                .as_ref()
                .unwrap()
                .secret_key_ref
                .as_ref()
                .unwrap()
                .key,
            "jwt-secret"
        );
        assert_eq!(
            env.iter()
                .find(|e| e.name == "AIRFLOW__CORE__FERNET_KEY")
                .unwrap()
                .value_from
                .as_ref()
                .unwrap()
                .secret_key_ref
                .as_ref()
                .unwrap()
                .key,
            "fernet-key"
        );
        let mounts = job
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .volume_mounts
            .as_ref()
            .unwrap();
        assert_eq!(mounts[0].mount_path, "/opt/scripts");
        assert_eq!(mounts[0].read_only, Some(true));
    }

    #[test]
    fn the_group_roles_binding_names_the_pipeline_reader_role() {
        let binding = group_roles_binding("stardelt");
        assert_eq!(binding.subjects.as_ref().unwrap()[0].name, GROUP_ROLES_JOB);
        assert_eq!(binding.role_ref.name, PIPELINE_READER_ROLE);
    }

    #[test]
    fn the_role_reads_pipelines_service_principals_and_connections_and_nothing_else() {
        let role = pipeline_reader_role("stardelt");
        assert_eq!(
            role.metadata.name.as_deref(),
            Some("airflow-pipeline-reader")
        );
        assert_eq!(role.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            role.metadata.labels.as_ref().expect("labels")["app.kubernetes.io/part-of"],
            "stardelt"
        );
        let rules = role.rules.expect("rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].api_groups, Some(vec!["stardelt.io".to_string()]));
        assert_eq!(
            rules[0].resources,
            Some(vec![
                "pipelines".to_string(),
                "serviceprincipals".to_string(),
                "connections".to_string()
            ])
        );
        assert_eq!(rules[0].verbs, ["get", "list", "watch"]);
    }

    #[test]
    fn the_binding_names_the_three_airflow_service_accounts_in_the_namespace() {
        let binding = pipeline_reader_binding("lab");
        assert_eq!(binding.metadata.namespace.as_deref(), Some("lab"));
        assert_eq!(binding.role_ref.kind, "Role");
        assert_eq!(binding.role_ref.name, "airflow-pipeline-reader");
        assert_eq!(
            binding.role_ref.api_group.as_deref(),
            Some("rbac.authorization.k8s.io")
        );
        let subjects = binding.subjects.expect("subjects");
        let names: Vec<_> = subjects.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "airflow-dag-processor",
                "airflow-worker",
                "airflow-scheduler"
            ]
        );
        for subject in &subjects {
            assert_eq!(subject.kind, "ServiceAccount");
            assert_eq!(subject.namespace.as_deref(), Some("lab"));
        }
    }

    #[test]
    fn the_secret_table_matches_the_platform_script() {
        let table: Vec<(&str, Vec<&str>)> = SECRETS
            .iter()
            .map(|secret| (secret.name, secret.key_names()))
            .collect();
        assert_eq!(
            table,
            [
                ("airflow-fernet-key", vec!["fernet-key"]),
                (
                    "airflow-webserver-key",
                    vec!["webserver-secret-key", "api-secret-key"]
                ),
                ("airflow-jwt-secret", vec!["jwt-secret"]),
                ("airflow-admin", vec!["username", "password"]),
                ("airflow-nova-api", vec!["username", "password"]),
            ]
        );
        assert_eq!(SECRETS[3].fixed, [("username", "admin")]);
        assert_eq!(SECRETS[4].fixed, [("username", "nova-api")]);
    }

    #[test]
    fn only_the_fernet_key_is_a_fernet_key() {
        let fernet: Vec<_> = SECRETS
            .iter()
            .flat_map(|secret| secret.fernet.iter().copied())
            .collect();
        assert_eq!(fernet, ["fernet-key"]);
        let secret = crate::keycloak::identity_secret(&SECRETS[0], "stardelt").expect("a Secret");
        let data = secret.data.expect("data");
        assert_eq!(data["fernet-key"].0.len(), 44);
        let webserver =
            crate::keycloak::identity_secret(&SECRETS[1], "stardelt").expect("a Secret");
        let data = webserver.data.expect("data");
        assert_eq!(data["webserver-secret-key"].0.len(), 48);
        assert_ne!(data["webserver-secret-key"], data["api-secret-key"]);
    }

    #[test]
    fn nova_reads_the_password_from_the_secret_the_table_creates() {
        assert_eq!(NOVA_API_SECRET, "airflow-nova-api");
        assert!(
            SECRETS
                .iter()
                .any(|s| s.name == NOVA_API_SECRET && s.key_names() == ["username", "password"])
        );
    }
}
