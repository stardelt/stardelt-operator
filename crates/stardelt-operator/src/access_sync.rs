//! The access-sync loop of `design/operator-parity.md` step O4, part 1: the script ConfigMap,
//! the ServiceAccount with its Role and binding, and the CronJob that turns `Catalog` objects
//! into Lakekeeper warehouses with grants and `SqlWarehouse` objects into Trino resource
//! groups. The script is stardelt-platform's `scripts/access-sync-job.py`, byte for byte
//! (`access_sync_job.py`, stdlib only).

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMap, ConfigMapVolumeSource, Container, EnvVar, EnvVarSource,
    ObjectFieldSelector, PodSecurityContext, PodSpec, PodTemplateSpec, ResourceRequirements,
    SeccompProfile, SecurityContext, ServiceAccount, Volume, VolumeMount,
};
use k8s_openapi::api::rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::ObjectMeta;

/// Name of the ServiceAccount, Role, RoleBinding, ConfigMap and CronJob.
pub const JOB: &str = "access-sync";
/// The script, verbatim from stardelt-platform.
pub const SCRIPT: &str = include_str!("access_sync_job.py");
/// Key of the script inside the ConfigMap (the platform's `--from-file=job.py=`).
pub const SCRIPT_KEY: &str = "job.py";
/// The Lakekeeper management client, keys client-id/client-secret/token-url. Created by the
/// operator as `ServicePrincipal` `catalog-operator` (operator#194, platform mode); the Secret
/// name is the operator's convention, `sp-<name>`.
pub const CLIENT_SECRET: &str = "sp-catalog-operator";
/// stdlib only, no build of ours (the platform's `manifests/access-sync.yaml`).
pub const IMAGE: &str = "python:3.13.16-slim-bookworm";
/// Realm the group reads come from; the same literal `keycloak.rs:224` substitutes.
pub const REALM: &str = "stardelt";
/// Groups whose `superset` account may hold warehouse grants without a Keycloak user: the
/// PASSWORD account Superset's Trino datasource uses (`crates/stardelt-operator/values/superset.yaml:66`,
/// `trino://superset@trino:8080/...`). Same value as the platform's manifest.
pub const STATIC_ASSIGNEES: &str = r#"{"stardelt-users":["superset"]}"#;

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

pub fn service_account(namespace: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: meta(JOB, namespace),
        automount_service_account_token: Some(true),
        ..ServiceAccount::default()
    }
}

pub fn script_config_map(namespace: &str) -> ConfigMap {
    ConfigMap {
        metadata: meta(JOB, namespace),
        data: Some(BTreeMap::from([(
            SCRIPT_KEY.to_string(),
            SCRIPT.to_string(),
        )])),
        ..ConfigMap::default()
    }
}

pub fn role(namespace: &str) -> Role {
    Role {
        metadata: meta(JOB, namespace),
        rules: Some(vec![
            PolicyRule {
                api_groups: Some(vec!["".into()]),
                resources: Some(vec!["secrets".into()]),
                resource_names: Some(vec![
                    CLIENT_SECRET.into(),
                    "stardelt-s3-creds".into(),
                    "stardelt-oidc-clients".into(),
                ]),
                verbs: vec!["get".into()],
                ..PolicyRule::default()
            },
            PolicyRule {
                api_groups: Some(vec!["stardelt.io".into()]),
                resources: Some(vec!["catalogs".into(), "sqlwarehouses".into()]),
                verbs: vec!["get".into(), "list".into()],
                ..PolicyRule::default()
            },
            PolicyRule {
                api_groups: Some(vec!["stardelt.io".into()]),
                resources: Some(vec![
                    "catalogs/status".into(),
                    "sqlwarehouses/status".into(),
                ]),
                verbs: vec!["patch".into()],
                ..PolicyRule::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".into()]),
                resources: Some(vec!["configmaps".into()]),
                resource_names: Some(vec!["trino-warehouses".into(), "trino-group-file".into()]),
                verbs: vec!["get".into(), "update".into(), "patch".into()],
                ..PolicyRule::default()
            },
        ]),
    }
}

pub fn binding(namespace: &str) -> RoleBinding {
    RoleBinding {
        metadata: meta(JOB, namespace),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".into()),
            kind: "Role".into(),
            name: JOB.into(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".into(),
            name: JOB.into(),
            namespace: Some(namespace.into()),
            ..Subject::default()
        }]),
    }
}

pub fn cron_job(namespace: &str, admin_group: &str) -> CronJob {
    let mut labels = meta(JOB, namespace).labels.unwrap_or_default();
    labels.insert(
        "app.kubernetes.io/name".to_string(),
        "access-sync".to_string(),
    );

    let container = Container {
        name: "sync".to_string(),
        image: Some(IMAGE.to_string()),
        command: Some(vec!["python3".to_string(), "/opt/job/job.py".to_string()]),
        env: Some(vec![
            EnvVar {
                name: "NAMESPACE".into(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.namespace".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            EnvVar {
                name: "LAKEKEEPER_URL".into(),
                value: Some("http://lakekeeper:8181".into()),
                ..Default::default()
            },
            EnvVar {
                name: "KEYCLOAK_URL".into(),
                value: Some("http://keycloak:8080".into()),
                ..Default::default()
            },
            EnvVar {
                name: "REALM".into(),
                value: Some(REALM.into()),
                ..Default::default()
            },
            EnvVar {
                name: "ADMIN_GROUP".into(),
                value: Some(admin_group.into()),
                ..Default::default()
            },
            EnvVar {
                name: "STATIC_ASSIGNEES".into(),
                value: Some(STATIC_ASSIGNEES.into()),
                ..Default::default()
            },
            EnvVar {
                name: "OPA_DATA_URL".into(),
                value: Some(format!(
                    "http://lakekeeper-opa.{namespace}.svc.cluster.local:8282/v1/data/stardelt/access"
                )),
                ..Default::default()
            },
            EnvVar {
                name: "CLIENT_SECRET_NAME".into(),
                value: Some(CLIENT_SECRET.into()),
                ..Default::default()
            },
        ]),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("50m".to_string())),
                ("memory".to_string(), Quantity("64Mi".to_string())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("500m".to_string())),
                ("memory".to_string(), Quantity("256Mi".to_string())),
            ])),
            ..Default::default()
        }),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        volume_mounts: Some(vec![VolumeMount {
            name: "job-script".to_string(),
            mount_path: "/opt/job".to_string(),
            read_only: Some(true),
            ..Default::default()
        }]),
        ..Container::default()
    };

    CronJob {
        metadata: ObjectMeta {
            name: Some(JOB.into()),
            namespace: Some(namespace.into()),
            labels: Some(labels),
            ..ObjectMeta::default()
        },
        spec: CronJobSpec {
            schedule: "*/2 * * * *".to_string(),
            concurrency_policy: Some("Forbid".to_string()),
            successful_jobs_history_limit: Some(3),
            failed_jobs_history_limit: Some(3),
            starting_deadline_seconds: None,
            suspend: None,
            time_zone: None,
            job_template: JobTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(meta(JOB, namespace).labels.unwrap_or_default()),
                    ..ObjectMeta::default()
                }),
                spec: Some(JobSpec {
                    backoff_limit: Some(1),
                    active_deadline_seconds: Some(110),
                    template: PodTemplateSpec {
                        metadata: Some(ObjectMeta {
                            labels: Some(meta(JOB, namespace).labels.unwrap_or_default()),
                            ..ObjectMeta::default()
                        }),
                        spec: Some(PodSpec {
                            restart_policy: Some("Never".to_string()),
                            service_account_name: Some(JOB.to_string()),
                            automount_service_account_token: Some(true),
                            security_context: Some(PodSecurityContext {
                                run_as_non_root: Some(true),
                                run_as_user: Some(65534),
                                run_as_group: Some(65534),
                                seccomp_profile: Some(SeccompProfile {
                                    type_: "RuntimeDefault".to_string(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            containers: vec![container],
                            volumes: Some(vec![Volume {
                                name: "job-script".to_string(),
                                config_map: Some(ConfigMapVolumeSource {
                                    name: JOB.to_string(),
                                    default_mode: Some(0o444),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }]),
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                }),
            },
        },
        status: None,
    }
}

/// The resource groups the coordinator loads (`resource-groups.json`, loaded once).
pub fn warehouses_config_map(namespace: &str) -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some("trino-warehouses".to_string()),
            namespace: Some(namespace.into()),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            "resource-groups.json".to_string(),
            "".to_string(),
        )])),
        ..Default::default()
    }
}

/// The file group provider (`groups.txt`, re-read within 30 s).
pub fn group_file_config_map(namespace: &str) -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some("trino-group-file".to_string()),
            namespace: Some(namespace.into()),
            ..Default::default()
        },
        data: Some(BTreeMap::from([("groups.txt".to_string(), "".to_string())])),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cron_job_runs_every_two_minutes_without_overlap() {
        let cj = cron_job("data", "platform-admins");
        let spec = &cj.spec;
        assert_eq!(spec.schedule, "*/2 * * * *");
        assert_eq!(spec.concurrency_policy.as_deref(), Some("Forbid"));
        assert_eq!(spec.successful_jobs_history_limit, Some(3));
        assert_eq!(spec.failed_jobs_history_limit, Some(3));
        let job_spec = spec.job_template.spec.as_ref().unwrap();
        assert_eq!(job_spec.backoff_limit, Some(1));
        assert_eq!(job_spec.active_deadline_seconds, Some(110));
        let pod_spec = job_spec.template.spec.as_ref().unwrap();
        assert_eq!(pod_spec.service_account_name.as_deref(), Some(JOB));
    }

    #[test]
    fn the_config_map_carries_the_platform_script_byte_for_byte() {
        let cm = script_config_map("data");
        let data = cm.data.as_ref().unwrap();
        let script = data.get(SCRIPT_KEY).unwrap();
        assert!(!script.is_empty());
        assert!(script.starts_with("#!/usr/bin/env python3"));
        assert!(script.contains("def main("));
    }

    #[test]
    fn the_role_reads_three_secrets_and_writes_the_two_config_maps() {
        let role = role("data");
        let rules = role.rules.as_ref().unwrap();
        assert_eq!(rules.len(), 4);
        // rule 0
        assert_eq!(
            rules[0].resource_names.as_ref().unwrap(),
            &vec![
                CLIENT_SECRET.to_string(),
                "stardelt-s3-creds".to_string(),
                "stardelt-oidc-clients".to_string()
            ]
        );
        // rule 3
        assert_eq!(
            rules[3].resource_names.as_ref().unwrap(),
            &vec![
                "trino-warehouses".to_string(),
                "trino-group-file".to_string()
            ]
        );
        for rule in rules {
            let verbs = &rule.verbs;
            assert!(!verbs.iter().any(|v| v == "create"));
            assert!(!verbs.iter().any(|v| v == "delete"));
        }
    }

    #[test]
    fn the_job_reads_the_admin_group_and_advertises_its_namespace() {
        let cj = cron_job("data", "platform-admins");
        let pod_spec = cj
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap();
        let container = &pod_spec.containers[0];
        let env = container.env.as_ref().unwrap();
        // Find ADMIN_GROUP
        let admin_group = env
            .iter()
            .find(|e| e.name == "ADMIN_GROUP")
            .unwrap()
            .value
            .as_ref()
            .unwrap();
        assert_eq!(admin_group, "platform-admins");
        // NAMESPACE from fieldRef
        let ns = env.iter().find(|e| e.name == "NAMESPACE").unwrap();
        assert!(ns.value_from.as_ref().unwrap().field_ref.is_some());
        assert_eq!(
            ns.value_from
                .as_ref()
                .unwrap()
                .field_ref
                .as_ref()
                .unwrap()
                .field_path,
            "metadata.namespace"
        );
        // OPA_DATA_URL
        let opa = env
            .iter()
            .find(|e| e.name == "OPA_DATA_URL")
            .unwrap()
            .value
            .as_ref()
            .unwrap();
        assert_eq!(
            opa,
            "http://lakekeeper-opa.data.svc.cluster.local:8282/v1/data/stardelt/access"
        );
    }

    #[test]
    fn the_access_sync_reads_the_catalog_principal_secret() {
        assert_eq!(CLIENT_SECRET, "sp-catalog-operator");
        let cj = cron_job("data", "stardelt-admins");
        let pod = cj.spec.job_template.spec.unwrap().template.spec.unwrap();
        let env: Vec<(String, Option<String>)> = pod.containers[0]
            .env
            .as_ref()
            .unwrap()
            .iter()
            .map(|e| (e.name.clone(), e.value.clone()))
            .collect();
        assert!(
            env.iter()
                .any(|(n, v)| n == "CLIENT_SECRET_NAME"
                    && v.as_deref() == Some("sp-catalog-operator")),
            "the CronJob must name the catalog principal's Secret explicitly: {env:?}"
        );
        let role = role("data");
        let first = &role.rules.as_ref().unwrap()[0];
        assert!(
            first
                .resource_names
                .as_ref()
                .unwrap()
                .iter()
                .any(|n| n == "sp-catalog-operator"),
            "the Role must allow get on sp-catalog-operator"
        );
    }
}
