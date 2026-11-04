//! Superset (the `dashboards` pillar of `design/operator-parity.md`, step O7a/O7b): Secrets,
//! the `superset` ServiceAccount, the Catalogs Role/binding, and the group-roles CronJob
//! applied by the controller/provision step. The group-roles CronJob mirrors
//! `stardelt-platform/manifests/superset-group-roles.yaml`.

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMap, ConfigMapVolumeSource, Container, EnvFromSource, EnvVar, EnvVarSource,
    LocalObjectReference, PodSpec, PodTemplateSpec, ResourceRequirements, SecretEnvSource,
    SecretKeySelector, SecretVolumeSource, SecurityContext, ServiceAccount, Volume, VolumeMount,
};
use k8s_openapi::api::rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ObjectMeta;

use crate::keycloak::IdentitySecret;

/// The Secrets Superset reads before its release: the Flask secret, the local
/// administrator (the only sign-in until O7b) and the Trino service account the
/// datasource hook reads. Names and keys mirror stardelt-platform/scripts/app-secrets.sh.
pub const SECRETS: [IdentitySecret; 3] = [
    IdentitySecret {
        name: "superset-secret-key",
        fixed: &[],
        generated: &["secret-key"],
        fernet: &[],
    },
    IdentitySecret {
        name: "superset-admin",
        fixed: &[("username", "admin")],
        generated: &["password"],
        fernet: &[],
    },
    IdentitySecret {
        name: "trino-password-db",
        fixed: &[("username", "superset")],
        generated: &["password"],
        fernet: &[],
    },
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

pub const SERVICE_ACCOUNT: &str = "superset";
pub const GROUP_ROLES_JOB: &str = "superset-group-roles";

pub fn image() -> String {
    let values = crate::values::golden_values("superset");
    let repository = values["image"]["repository"]
        .as_str()
        .expect("values/superset.yaml has image.repository");
    let tag = values["image"]["tag"]
        .as_str()
        .expect("values/superset.yaml has image.tag");
    format!("{repository}:{tag}")
}

pub fn service_account(namespace: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: meta(SERVICE_ACCOUNT, namespace),
        automount_service_account_token: Some(true),
        ..ServiceAccount::default()
    }
}

pub fn catalog_reader_role(namespace: &str) -> Role {
    Role {
        metadata: meta(GROUP_ROLES_JOB, namespace),
        rules: Some(vec![PolicyRule {
            api_groups: Some(vec!["stardelt.io".to_string()]),
            resources: Some(vec!["catalogs".to_string()]),
            verbs: vec!["get".to_string(), "list".to_string()],
            ..PolicyRule::default()
        }]),
    }
}

pub fn catalog_reader_binding(namespace: &str) -> RoleBinding {
    RoleBinding {
        metadata: meta(GROUP_ROLES_JOB, namespace),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".to_string()),
            kind: "Role".to_string(),
            name: GROUP_ROLES_JOB.to_string(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: SERVICE_ACCOUNT.to_string(),
            namespace: Some(namespace.to_string()),
            ..Subject::default()
        }]),
    }
}

/// The group-roles script, a copy of `stardelt-platform/scripts/superset-group-roles.py`
/// (the platform keeps the original; copy it, never edit the copy).
pub const SCRIPT: &str = include_str!("superset_group_roles.py");

pub fn group_roles_config_map(namespace: &str) -> ConfigMap {
    ConfigMap {
        metadata: meta(GROUP_ROLES_JOB, namespace),
        data: Some(BTreeMap::from([(
            "sync.py".to_string(),
            SCRIPT.to_string(),
        )])),
        ..ConfigMap::default()
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
        env_from: Some(vec![EnvFromSource {
            secret_ref: Some(SecretEnvSource {
                name: "superset-env".to_string(),
                ..SecretEnvSource::default()
            }),
            ..EnvFromSource::default()
        }]),
        env: Some(vec![EnvVar {
            name: "SUPERSET_SECRET_KEY".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: "superset-secret-key".to_string(),
                    key: "secret-key".to_string(),
                    optional: None,
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }]),
        volume_mounts: Some(vec![
            VolumeMount {
                name: "scripts".to_string(),
                mount_path: "/opt/scripts".to_string(),
                read_only: Some(true),
                ..VolumeMount::default()
            },
            VolumeMount {
                name: "pythonpath".to_string(),
                mount_path: "/app/pythonpath".to_string(),
                read_only: Some(true),
                ..VolumeMount::default()
            },
        ]),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("100m".to_string())),
                ("memory".to_string(), Quantity("256Mi".to_string())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("1".to_string())),
                ("memory".to_string(), Quantity("1Gi".to_string())),
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
                    active_deadline_seconds: None,
                    parallelism: None,
                    completions: None,
                    manual_selector: None,
                    selector: None,
                    ttl_seconds_after_finished: None,
                    template: PodTemplateSpec {
                        metadata: Some(meta(GROUP_ROLES_JOB, namespace)),
                        spec: Some(PodSpec {
                            restart_policy: Some("Never".to_string()),
                            service_account_name: Some(SERVICE_ACCOUNT.to_string()),
                            automount_service_account_token: Some(true),
                            containers: vec![container],
                            volumes: Some(vec![
                                Volume {
                                    name: "scripts".to_string(),
                                    config_map: Some(ConfigMapVolumeSource {
                                        name: GROUP_ROLES_JOB.to_string(),
                                        default_mode: Some(0o444),
                                        ..ConfigMapVolumeSource::default()
                                    }),
                                    ..Volume::default()
                                },
                                Volume {
                                    name: "pythonpath".to_string(),
                                    secret: Some(SecretVolumeSource {
                                        secret_name: Some("superset-config".to_string()),
                                        ..SecretVolumeSource::default()
                                    }),
                                    ..Volume::default()
                                },
                            ]),
                            image_pull_secrets: Some(vec![LocalObjectReference {
                                name: "ghcr-creds".to_string(),
                            }]),
                            init_containers: None,
                            termination_grace_period_seconds: None,
                            active_deadline_seconds: None,
                            dns_policy: None,
                            node_selector: None,
                            service_account: None,
                            node_name: None,
                            host_network: None,
                            host_pid: None,
                            host_ipc: None,
                            share_process_namespace: None,
                            security_context: None,
                            hostname: None,
                            subdomain: None,
                            affinity: None,
                            tolerations: None,
                            scheduler_name: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_group_roles_job_runs_every_five_minutes_on_the_pinned_image() {
        let v = crate::values::golden_values("superset");
        let image_str = image();
        let expected = format!(
            "{}:{}",
            v["image"]["repository"].as_str().unwrap(),
            v["image"]["tag"].as_str().unwrap()
        );
        assert_eq!(image_str, expected);
        let job = group_roles_cron_job("stardelt", &image_str);
        let spec = &job.spec;
        assert_eq!(spec.schedule, "*/5 * * * *");
        assert_eq!(spec.concurrency_policy.as_deref(), Some("Forbid"));
        assert_eq!(spec.successful_jobs_history_limit, Some(1));
        assert_eq!(spec.failed_jobs_history_limit, Some(3));
        let job_spec = spec.job_template.spec.as_ref().unwrap();
        assert_eq!(job_spec.backoff_limit, Some(1));
        let pod_spec = job_spec.template.spec.as_ref().unwrap();
        assert_eq!(pod_spec.restart_policy.as_deref(), Some("Never"));
        assert_eq!(
            pod_spec.service_account_name.as_deref(),
            Some(SERVICE_ACCOUNT)
        );
        assert_eq!(pod_spec.automount_service_account_token, Some(true));
        let container = &pod_spec.containers[0];
        assert_eq!(container.name, "sync");
        assert_eq!(container.image.as_deref(), Some(expected.as_str()));
        assert_eq!(
            container.command.as_ref().unwrap(),
            &vec!["python".to_string(), "/opt/scripts/sync.py".to_string()]
        );
    }

    /// platform#514/#515: a group role must hold the grants of every catalog the group owns.
    /// Converging once per catalog left each role with only the last catalog's permissions
    /// (in practice the empty `<group>ref` one), so members saw the dashboard but no charts.
    /// The script accumulates into `desired` and converges once, after every catalog.
    #[test]
    fn the_group_roles_script_accumulates_grants_across_catalogs() {
        assert!(SCRIPT.contains("def accumulate(desired, role_name, granted_all)"));
        assert!(
            SCRIPT.contains("accumulate(desired, role_name, granted_all)"),
            "every catalog contributes to the role's wanted set"
        );
        assert!(
            SCRIPT.contains("for role_name in sorted(desired):"),
            "convergence happens once per role, after the catalog loop"
        );
        assert!(
            !SCRIPT.contains("converge(names_fn(role), want, granted_all)"),
            "the per-catalog convergence of platform#514 must not come back"
        );
        let cm = group_roles_config_map("stardelt");
        assert_eq!(cm.data.unwrap()["sync.py"], SCRIPT);
    }

    #[test]
    fn the_group_roles_job_reads_the_superset_secrets() {
        let image_str = image();
        let job = group_roles_cron_job("stardelt", &image_str);
        let spec = &job.spec;
        let job_spec = spec.job_template.spec.as_ref().unwrap();
        let pod_spec = job_spec.template.spec.as_ref().unwrap();
        let container = &pod_spec.containers[0];
        assert_eq!(
            container.env_from.as_ref().unwrap()[0]
                .secret_ref
                .as_ref()
                .unwrap()
                .name,
            "superset-env"
        );
        let envs = container.env.as_ref().unwrap();
        assert_eq!(envs[0].name, "SUPERSET_SECRET_KEY");
        assert_eq!(
            envs[0]
                .value_from
                .as_ref()
                .unwrap()
                .secret_key_ref
                .as_ref()
                .unwrap()
                .name,
            "superset-secret-key"
        );
        assert_eq!(
            envs[0]
                .value_from
                .as_ref()
                .unwrap()
                .secret_key_ref
                .as_ref()
                .unwrap()
                .key,
            "secret-key"
        );
        assert_eq!(
            pod_spec.image_pull_secrets.as_ref().unwrap()[0].name,
            "ghcr-creds"
        );
        let mounts = container.volume_mounts.as_ref().unwrap();
        assert_eq!(mounts[0].name, "scripts");
        assert_eq!(mounts[0].mount_path, "/opt/scripts");
        assert_eq!(mounts[0].read_only, Some(true));
        assert_eq!(mounts[1].name, "pythonpath");
        assert_eq!(mounts[1].mount_path, "/app/pythonpath");
        assert_eq!(mounts[1].read_only, Some(true));
        let volumes = pod_spec.volumes.as_ref().unwrap();
        assert_eq!(
            volumes[0].config_map.as_ref().unwrap().name,
            GROUP_ROLES_JOB
        );
        assert_eq!(
            volumes[0].config_map.as_ref().unwrap().default_mode,
            Some(0o444)
        );
        assert_eq!(
            volumes[1].secret.as_ref().unwrap().secret_name.as_deref(),
            Some("superset-config")
        );
        let cm = group_roles_config_map("stardelt");
        let data = cm.data.as_ref().unwrap();
        let script = data.get("sync.py").unwrap();
        assert!(script.contains("catalogs.stardelt.io"));
        assert!(script.contains("stardelt-group-"));
        assert!(script.contains("dashboard_roles"));
    }

    #[test]
    fn the_catalog_reader_role_lists_catalogs_and_nothing_else() {
        let role = catalog_reader_role("stardelt");
        let rules = role.rules.expect("rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].api_groups, Some(vec!["stardelt.io".to_string()]));
        assert_eq!(rules[0].resources, Some(vec!["catalogs".to_string()]));
        assert_eq!(rules[0].verbs, vec!["get".to_string(), "list".to_string()]);
    }

    #[test]
    fn the_service_account_is_the_one_the_chart_uses() {
        let sa = service_account("stardelt");
        assert_eq!(sa.metadata.name.as_deref(), Some(SERVICE_ACCOUNT));
        assert_eq!(sa.automount_service_account_token, Some(true));
    }
}
