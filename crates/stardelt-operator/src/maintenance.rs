//! The per-`Catalog` maintenance CronJob (operator#476, slice b): one Job at
//! `spec.maintenance.schedule` that runs `stardelt maintenance run --catalog <name>` as the
//! team's pipeline principal. Pure builders; the `Catalog` controller that applies them and
//! writes `MaintenanceReady` is slice b2.

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, EnvVar, EnvVarSource, LocalObjectReference, PodSpec, PodTemplateSpec,
    ResourceRequirements, SeccompProfile, SecretKeySelector, SecretVolumeSource, SecurityContext,
    Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::ObjectMeta;

use crate::crd::{MaintenanceSpec, principal_client_id};
use crate::identity::PLATFORM_REALM;
use crate::objects::FIELD_MANAGER;

/// Prefix of the CronJob name; the catalog name follows (`maint-<catalog>`).
pub const JOB_PREFIX: &str = "maint-";
/// The CLI image (stardelt/stardelt-python#62). Pinned by digest when the platform's pin
/// tripwire covers it; the dev registry path is the same `:dev` convention as the other
/// stardelt images.
pub const IMAGE: &str = "ghcr.io/stardelt/stardelt-cli:dev";

pub fn job_name(catalog: &str) -> String {
    format!("{JOB_PREFIX}{catalog}")
}

/// The policy as the JSON the CLI reads from `STARDELT_MAINTENANCE_POLICY`. `MaintenanceSpec`
/// serializes to the camelCase keys `maintenance.py::_settings` expects.
pub fn policy_json(spec: &MaintenanceSpec) -> String {
    serde_json::to_string(spec).expect("MaintenanceSpec is a plain struct and always serializes")
}

/// The client-credentials token endpoint: the platform realm in the platform's own Keycloak,
/// or the external issuer's token endpoint when the platform runs against an external IdP
/// (`PlatformInstance.spec.identity.issuerUrl`).
pub fn token_url(issuer_url: Option<&str>) -> String {
    match issuer_url {
        Some(issuer) => format!(
            "{}/protocol/openid-connect/token",
            issuer.trim_end_matches('/')
        ),
        None => {
            format!("http://keycloak:8080/realms/{PLATFORM_REALM}/protocol/openid-connect/token")
        }
    }
}

fn labels(catalog: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            FIELD_MANAGER.to_string(),
        ),
        ("stardelt.io/catalog".to_string(), catalog.to_string()),
    ])
}

/// The CronJob a `Catalog` becomes. Pure: no client, no cluster state.
pub fn cron_job(
    namespace: &str,
    catalog: &str,
    owner_group: &str,
    spec: &MaintenanceSpec,
    token_url: &str,
    owner: &OwnerReference,
) -> CronJob {
    let principal = format!("pipeline-{owner_group}");
    let secret = format!("sp-{principal}");
    let job_labels = labels(catalog);

    let container = Container {
        name: "maintenance".to_string(),
        image: Some(IMAGE.to_string()),
        command: Some(vec![
            "stardelt".to_string(),
            "maintenance".to_string(),
            "run".to_string(),
            "--catalog".to_string(),
            catalog.to_string(),
        ]),
        env: Some(vec![
            EnvVar {
                name: "STARDELT_MAINTENANCE_POLICY".into(),
                value: Some(policy_json(spec)),
                ..EnvVar::default()
            },
            EnvVar {
                name: "TRINO_URL".into(),
                value_from: Some(EnvVarSource {
                    config_map_key_ref: Some(k8s_openapi::api::core::v1::ConfigMapKeySelector {
                        name: "stardelt-identity".into(),
                        key: "trinoUrl".into(),
                        optional: Some(true),
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            },
            EnvVar {
                name: "PIPELINE_TOKEN_URL".into(),
                value: Some(token_url.to_string()),
                ..EnvVar::default()
            },
            EnvVar {
                name: "PIPELINE_CLIENT_ID".into(),
                value: Some(principal_client_id(&principal)),
                ..EnvVar::default()
            },
            EnvVar {
                name: "PIPELINE_CLIENT_SECRET".into(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: secret,
                        key: "client-secret".into(),
                        optional: Some(false),
                    }),
                    ..EnvVarSource::default()
                }),
                ..EnvVar::default()
            },
            EnvVar {
                name: "REQUESTS_CA_BUNDLE".into(),
                value: Some("/etc/stardelt/trino-ca/ca.crt".into()),
                ..EnvVar::default()
            },
        ]),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("50m".to_string())),
                ("memory".to_string(), Quantity("128Mi".to_string())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("500m".to_string())),
                ("memory".to_string(), Quantity("512Mi".to_string())),
            ])),
            ..ResourceRequirements::default()
        }),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Capabilities::default()
            }),
            seccomp_profile: Some(SeccompProfile {
                type_: "RuntimeDefault".to_string(),
                ..SeccompProfile::default()
            }),
            ..SecurityContext::default()
        }),
        volume_mounts: Some(vec![VolumeMount {
            name: "trino-ca".to_string(),
            mount_path: "/etc/stardelt/trino-ca".to_string(),
            read_only: Some(true),
            ..VolumeMount::default()
        }]),
        ..Container::default()
    };

    let pod_spec = PodSpec {
        restart_policy: Some("Never".to_string()),
        image_pull_secrets: Some(vec![LocalObjectReference {
            name: "ghcr-creds".to_string(),
        }]),
        containers: vec![container],
        volumes: Some(vec![Volume {
            name: "trino-ca".to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some("trino-ca".to_string()),
                optional: Some(true),
                items: Some(vec![k8s_openapi::api::core::v1::KeyToPath {
                    key: "tls.crt".to_string(),
                    path: "ca.crt".to_string(),
                    mode: None,
                    ..k8s_openapi::api::core::v1::KeyToPath::default()
                }]),
                ..SecretVolumeSource::default()
            }),
            ..Volume::default()
        }]),
        ..PodSpec::default()
    };

    CronJob {
        metadata: ObjectMeta {
            name: Some(job_name(catalog)),
            namespace: Some(namespace.to_string()),
            labels: Some(job_labels.clone()),
            owner_references: Some(vec![owner.clone()]),
            ..ObjectMeta::default()
        },
        spec: CronJobSpec {
            schedule: spec.schedule.clone(),
            concurrency_policy: Some("Forbid".to_string()),
            successful_jobs_history_limit: Some(1),
            failed_jobs_history_limit: Some(1),
            job_template: JobTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(job_labels.clone()),
                    ..ObjectMeta::default()
                }),
                spec: Some(JobSpec {
                    backoff_limit: Some(0),
                    // One hour is long enough for a small catalog and short enough that a
                    // wedged job does not hold `Forbid` until the next schedule.
                    active_deadline_seconds: Some(3600),
                    template: PodTemplateSpec {
                        metadata: Some(ObjectMeta {
                            labels: Some(job_labels),
                            ..ObjectMeta::default()
                        }),
                        spec: Some(pod_spec),
                    },
                    ..JobSpec::default()
                }),
            },
            ..CronJobSpec::default()
        },
        ..CronJob::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::MaintenanceSpec;

    fn owner() -> OwnerReference {
        OwnerReference {
            api_version: "stardelt.io/v1alpha1".into(),
            kind: "Catalog".into(),
            name: "mobility".into(),
            uid: "uid-1".into(),
            ..OwnerReference::default()
        }
    }

    #[test]
    fn the_job_runs_daily_as_the_teams_pipeline_principal() {
        let job = cron_job(
            "stardelt",
            "mobility",
            "analytics",
            &MaintenanceSpec::default(),
            &token_url(None),
            &owner(),
        );
        assert_eq!(job.metadata.name.as_deref(), Some("maint-mobility"));
        let spec = job.spec;
        assert_eq!(spec.schedule, "0 2 * * *");
        assert_eq!(spec.concurrency_policy.as_deref(), Some("Forbid"));

        let pod = spec.job_template.spec.unwrap().template.spec.unwrap();
        let container = &pod.containers[0];
        assert_eq!(container.image.as_deref(), Some(IMAGE));
        assert_eq!(
            container.command.as_ref().unwrap(),
            &vec![
                "stardelt".to_string(),
                "maintenance".to_string(),
                "run".to_string(),
                "--catalog".to_string(),
                "mobility".to_string()
            ]
        );
        let env = container.env.as_ref().unwrap();
        let value = |name: &str| {
            env.iter()
                .find(|e| e.name == name)
                .and_then(|e| e.value.clone())
        };
        assert_eq!(
            value("PIPELINE_CLIENT_ID").as_deref(),
            Some("sp-pipeline-analytics")
        );
        assert_eq!(
            value("PIPELINE_TOKEN_URL").as_deref(),
            Some("http://keycloak:8080/realms/stardelt/protocol/openid-connect/token")
        );
        let secret = env
            .iter()
            .find(|e| e.name == "PIPELINE_CLIENT_SECRET")
            .and_then(|e| e.value_from.as_ref())
            .and_then(|v| v.secret_key_ref.as_ref())
            .unwrap();
        assert_eq!(secret.name, "sp-pipeline-analytics");
        assert_eq!(secret.key, "client-secret");
        assert_eq!(
            value("REQUESTS_CA_BUNDLE").as_deref(),
            Some("/etc/stardelt/trino-ca/ca.crt")
        );

        assert_eq!(pod.volumes.as_ref().unwrap()[0].name, "trino-ca");
        assert_eq!(
            container.volume_mounts.as_ref().unwrap()[0].mount_path,
            "/etc/stardelt/trino-ca"
        );
        assert_eq!(
            job.metadata.owner_references.as_ref().unwrap()[0].name,
            "mobility"
        );
    }

    #[test]
    fn the_policy_is_the_crd_json_the_cli_reads() {
        let json = policy_json(&MaintenanceSpec::default());
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["schedule"], "0 2 * * *");
        assert_eq!(parsed["optimize"]["minSmallFiles"], 20);
        assert_eq!(parsed["optimize"]["fileSizeThreshold"], "128MB");
        assert_eq!(parsed["expireSnapshots"]["retention"], "7d");
        assert_eq!(parsed["removeOrphanFiles"]["retention"], "7d");
        assert_eq!(parsed["excludeTables"], serde_json::json!([]));
    }

    #[test]
    fn an_external_issuer_supplies_its_own_token_endpoint() {
        assert_eq!(
            token_url(Some("https://login.example.com/tenant/")),
            "https://login.example.com/tenant/protocol/openid-connect/token"
        );
    }
}
