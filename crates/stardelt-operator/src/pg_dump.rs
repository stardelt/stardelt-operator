//! Nightly logical dumps of the two Bitnami PostgreSQL StatefulSets the `dashboards` and
//! `pipelines` pillars own, uploaded to the `backups` bucket (`design/operator-parity.md`,
//! O8). Mirrors stardelt-platform/manifests/pg-dump-backup.yaml, including its limits: the
//! unit of recovery is one day, and a restore loses the writes after the last dump.

use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, EmptyDirVolumeSource, EnvVar, EnvVarSource, KeyToPath, PodSpec,
    PodTemplateSpec, ResourceRequirements, SecretKeySelector, SecurityContext, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::BTreeMap;

/// Superset's database, the same image as StatefulSet `superset-postgresql`
/// (pg_dump must not be newer than the server it reads).
pub const SUPERSET_PG_IMAGE: &str = "docker.io/bitnamilegacy/postgresql:14.17.0-debian-12-r3";
/// Airflow's database, the same image as StatefulSet `airflow-postgresql`.
pub const AIRFLOW_PG_IMAGE: &str = "docker.io/bitnamilegacy/postgresql:16.1.0-debian-11-r15";
/// The uploader, digest-pinned: it is the only container that holds the S3 key.
pub const AWS_CLI_IMAGE: &str =
    "amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73";
/// The bucket the SeaweedFS values create (`crates/stardelt-operator/values/seaweedfs.yaml`) and the object-store
/// archive writes into; the dump lives under `pgdump/<app>/`, next to it, never over it.
pub const BACKUP_BUCKET: &str = "backups";
/// Days a dump is kept; only the app's own `pgdump/<app>/` prefix is pruned.
pub const RETENTION_DAYS: &str = "14";
/// Names of the two CronJobs (and of their `ComponentStatus` label is the step name).
pub const SUPERSET_JOB: &str = "superset-pg-dump";
pub const AIRFLOW_JOB: &str = "airflow-pg-dump";

fn meta(name: &str, namespace: &str) -> ObjectMeta {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), name.to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), name.to_string());
    labels.insert(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "stardelt-operator".to_string(),
    );
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        labels: Some(labels),
        ..Default::default()
    }
}

/// One environment variable read from Secret `stardelt-s3-creds`, never a literal.
fn s3_env(name: &str, key: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: "stardelt-s3-creds".to_string(),
                key: key.to_string(),
                optional: Some(false),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn dump_container(app: &str, image: &str, host: &str, database: &str, user: &str) -> Container {
    let script = "/bin/bash -ec '\n\
export PGPASSWORD=\"$(cat \"$PGPASSWORD_FILE\")\"\n\
pg_dump --format=custom --no-owner --file=/dump/${APP}.dump \"$@\"\n\
SIZE=$(stat -c%s /dump/${APP}.dump || echo 0)\n\
if [ \"$SIZE\" -eq 0 ]; then echo \"empty dump\" >&2; exit 1; fi\n\
' _";

    let env = vec![
        EnvVar {
            name: "APP".to_string(),
            value: Some(app.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGHOST".to_string(),
            value: Some(host.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGPORT".to_string(),
            value: Some("5432".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGDATABASE".to_string(),
            value: Some(database.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGUSER".to_string(),
            value: Some(user.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGPASSWORD_FILE".to_string(),
            value: Some("/run/pg-secret/password".to_string()),
            ..Default::default()
        },
    ];

    Container {
        name: "pg-dump".to_string(),
        image: Some(image.to_string()),
        command: Some(vec!["/bin/bash".to_string(), "-ec".to_string()]),
        args: Some(vec![script.to_string()]),
        env: Some(env),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("100m".to_string())),
                ("memory".to_string(), Quantity("128Mi".to_string())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("500m".to_string())),
                ("memory".to_string(), Quantity("512Mi".to_string())),
            ])),
            ..Default::default()
        }),
        security_context: Some(SecurityContext {
            read_only_root_filesystem: Some(true),
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        volume_mounts: Some(vec![
            VolumeMount {
                name: "dump".to_string(),
                mount_path: "/dump".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: "tmp".to_string(),
                mount_path: "/tmp".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: "pg-secret".to_string(),
                mount_path: "/run/pg-secret".to_string(),
                read_only: Some(true),
                ..Default::default()
            },
        ]),
        ..Default::default()
    }
}

fn upload_container(app: &str) -> Container {
    let script = "/bin/sh -ec '\n\
FILE=/dump/${APP}.dump\n\
SIZE=$(stat -c%s \"$FILE\" 2>/dev/null || echo 0)\n\
[ \"$SIZE\" -gt 0 ] || { echo \"empty dump\" >&2; exit 1; }\n\
KEY=pgdump/${APP}/$(date -u +%F).dump\n\
aws s3 cp \"$FILE\" s3://${BACKUP_BUCKET}/$KEY\n\
LEN=$(aws s3api head-object --bucket \"${BACKUP_BUCKET}\" --key \"$KEY\" --query ContentLength --output text)\n\
[ \"$LEN\" = \"$SIZE\" ] || { echo \"size mismatch\" >&2; exit 1; }\n\
CUTOFF=$(date -u -d \"${RETENTION_DAYS} days ago\" +%Y-%m-%d 2>/dev/null || date -u -v-${RETENTION_DAYS}d +%Y-%m-%d)\n\
aws s3api list-objects-v2 --bucket \"${BACKUP_BUCKET}\" --prefix pgdump/${APP}/ --query \"Contents[?LastModified<='\"$CUTOFF\"T23:59:59Z'].Key\" --output text | grep -v '^None$' | xargs -r -n1 -I{} aws s3 rm s3://${BACKUP_BUCKET}/{}\n\
'";

    let env = vec![
        EnvVar {
            name: "APP".to_string(),
            value: Some(app.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "BACKUP_BUCKET".to_string(),
            value: Some(BACKUP_BUCKET.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "RETENTION_DAYS".to_string(),
            value: Some(RETENTION_DAYS.to_string()),
            ..Default::default()
        },
        s3_env("AWS_ENDPOINT_URL", "endpoint"),
        s3_env("AWS_ACCESS_KEY_ID", "access-key"),
        s3_env("AWS_SECRET_ACCESS_KEY", "secret-key"),
        s3_env("AWS_DEFAULT_REGION", "region"),
    ];

    Container {
        name: "upload".to_string(),
        image: Some(AWS_CLI_IMAGE.to_string()),
        command: Some(vec!["/bin/sh".to_string(), "-ec".to_string()]),
        args: Some(vec![script.to_string()]),
        env: Some(env),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("50m".to_string())),
                ("memory".to_string(), Quantity("64Mi".to_string())),
            ])),
            limits: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("200m".to_string())),
                ("memory".to_string(), Quantity("256Mi".to_string())),
            ])),
            ..Default::default()
        }),
        security_context: Some(SecurityContext {
            read_only_root_filesystem: Some(true),
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".to_string()]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        volume_mounts: Some(vec![
            VolumeMount {
                name: "dump".to_string(),
                mount_path: "/dump".to_string(),
                ..Default::default()
            },
            VolumeMount {
                name: "tmp".to_string(),
                mount_path: "/tmp".to_string(),
                ..Default::default()
            },
        ]),
        ..Default::default()
    }
}

fn pod_security_context() -> k8s_openapi::api::core::v1::PodSecurityContext {
    k8s_openapi::api::core::v1::PodSecurityContext {
        run_as_non_root: Some(true),
        run_as_user: Some(1001),
        run_as_group: Some(1001),
        fs_group: Some(1001),
        seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
            type_: "RuntimeDefault".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn superset_cron_job(namespace: &str) -> CronJob {
    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/name".to_string(),
        SUPERSET_JOB.to_string(),
    );
    labels.insert(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "stardelt-operator".to_string(),
    );

    CronJob {
        metadata: meta(SUPERSET_JOB, namespace),
        spec: CronJobSpec {
            schedule: "30 2 * * *".to_string(),
            concurrency_policy: Some("Forbid".to_string()),
            successful_jobs_history_limit: Some(3),
            failed_jobs_history_limit: Some(3),
            starting_deadline_seconds: None,
            suspend: None,
            time_zone: None,
            job_template: JobTemplateSpec {
                metadata: None,
                spec: Some(JobSpec {
                    backoff_limit: Some(1),
                    active_deadline_seconds: Some(900),
                    template: PodTemplateSpec {
                        metadata: Some(ObjectMeta {
                            labels: Some(labels.clone()),
                            ..Default::default()
                        }),
                        spec: Some(PodSpec {
                            automount_service_account_token: Some(false),
                            restart_policy: Some("Never".to_string()),
                            security_context: Some(pod_security_context()),
                            init_containers: Some(vec![dump_container(
                                "superset",
                                SUPERSET_PG_IMAGE,
                                "superset-postgresql",
                                "superset",
                                "superset",
                            )]),
                            containers: vec![upload_container("superset")],
                            volumes: Some(vec![
                                Volume {
                                    name: "dump".to_string(),
                                    empty_dir: Some(EmptyDirVolumeSource::default()),
                                    ..Default::default()
                                },
                                Volume {
                                    name: "tmp".to_string(),
                                    empty_dir: Some(EmptyDirVolumeSource::default()),
                                    ..Default::default()
                                },
                                Volume {
                                    name: "pg-secret".to_string(),
                                    secret: Some(k8s_openapi::api::core::v1::SecretVolumeSource {
                                        secret_name: Some("superset-postgresql".to_string()),
                                        items: Some(vec![KeyToPath {
                                            key: "password".to_string(),
                                            path: "password".to_string(),
                                            ..Default::default()
                                        }]),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ]),
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

pub fn airflow_cron_job(namespace: &str) -> CronJob {
    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/name".to_string(),
        AIRFLOW_JOB.to_string(),
    );
    labels.insert(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "stardelt-operator".to_string(),
    );

    CronJob {
        metadata: meta(AIRFLOW_JOB, namespace),
        spec: CronJobSpec {
            schedule: "45 2 * * *".to_string(),
            concurrency_policy: Some("Forbid".to_string()),
            successful_jobs_history_limit: Some(3),
            failed_jobs_history_limit: Some(3),
            starting_deadline_seconds: None,
            suspend: None,
            time_zone: None,
            job_template: JobTemplateSpec {
                metadata: None,
                spec: Some(JobSpec {
                    backoff_limit: Some(1),
                    active_deadline_seconds: Some(900),
                    template: PodTemplateSpec {
                        metadata: Some(ObjectMeta {
                            labels: Some(labels.clone()),
                            ..Default::default()
                        }),
                        spec: Some(PodSpec {
                            automount_service_account_token: Some(false),
                            restart_policy: Some("Never".to_string()),
                            security_context: Some(pod_security_context()),
                            init_containers: Some(vec![dump_container(
                                "airflow",
                                AIRFLOW_PG_IMAGE,
                                "airflow-postgresql",
                                "postgres",
                                "postgres",
                            )]),
                            containers: vec![upload_container("airflow")],
                            volumes: Some(vec![
                                Volume {
                                    name: "dump".to_string(),
                                    empty_dir: Some(EmptyDirVolumeSource::default()),
                                    ..Default::default()
                                },
                                Volume {
                                    name: "tmp".to_string(),
                                    empty_dir: Some(EmptyDirVolumeSource::default()),
                                    ..Default::default()
                                },
                                Volume {
                                    name: "pg-secret".to_string(),
                                    secret: Some(k8s_openapi::api::core::v1::SecretVolumeSource {
                                        secret_name: Some("airflow-postgresql".to_string()),
                                        items: Some(vec![KeyToPath {
                                            key: "postgres-password".to_string(),
                                            path: "password".to_string(),
                                            ..Default::default()
                                        }]),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ]),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_superset_dump_runs_at_0230_and_uploads_pgdump_superset() {
        let cj = superset_cron_job("stardelt");
        assert_eq!(cj.spec.schedule, "30 2 * * *");
        assert_eq!(cj.metadata.name.as_deref(), Some(SUPERSET_JOB));
        let pod = &cj
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap();
        let init = &pod.init_containers.as_ref().unwrap()[0];
        assert_eq!(init.image.as_deref(), Some(SUPERSET_PG_IMAGE));
        let env = init.env.as_ref().unwrap();
        let get = |name: &str| {
            env.iter()
                .find(|e| e.name == name)
                .unwrap()
                .value
                .as_deref()
        };
        assert_eq!(get("PGHOST"), Some("superset-postgresql"));
        assert_eq!(get("PGDATABASE"), Some("superset"));
        assert_eq!(get("PGUSER"), Some("superset"));
        let vol = pod
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .find(|v| v.name == "pg-secret")
            .unwrap();
        let items = vol.secret.as_ref().unwrap().items.as_ref().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].key, "password");
        assert_eq!(items[0].path, "password");
        assert_eq!(
            vol.secret.as_ref().unwrap().secret_name.as_deref(),
            Some("superset-postgresql")
        );
        let up = &pod.containers[0];
        let uenv = up.env.as_ref().unwrap();
        let uget = |name: &str| {
            uenv.iter()
                .find(|e| e.name == name)
                .unwrap()
                .value
                .as_deref()
        };
        assert_eq!(uget("APP"), Some("superset"));
        assert_eq!(uget("BACKUP_BUCKET"), Some(BACKUP_BUCKET));
        let uenv = up.env.as_ref().unwrap();
        let names = [
            "AWS_ENDPOINT_URL",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION",
        ];
        for n in names {
            let ev = uenv.iter().find(|e| e.name == n).unwrap();
            assert_eq!(
                ev.value_from
                    .as_ref()
                    .unwrap()
                    .secret_key_ref
                    .as_ref()
                    .unwrap()
                    .name,
                "stardelt-s3-creds"
            );
        }
        assert_eq!(
            uenv.iter()
                .position(|e| e.name == "AWS_ENDPOINT_URL")
                .unwrap(),
            3
        );
        assert_eq!(
            uenv.iter()
                .position(|e| e.name == "AWS_ACCESS_KEY_ID")
                .unwrap(),
            4
        );
        assert_eq!(
            uenv.iter()
                .position(|e| e.name == "AWS_SECRET_ACCESS_KEY")
                .unwrap(),
            5
        );
        assert_eq!(
            uenv.iter()
                .position(|e| e.name == "AWS_DEFAULT_REGION")
                .unwrap(),
            6
        );
    }

    #[test]
    fn the_airflow_dump_runs_at_0245_against_the_bitnami_defaults() {
        let cj = airflow_cron_job("stardelt");
        assert_eq!(cj.spec.schedule, "45 2 * * *");
        let pod = &cj
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap();
        let init = &pod.init_containers.as_ref().unwrap()[0];
        let env = init.env.as_ref().unwrap();
        let get = |name: &str| {
            env.iter()
                .find(|e| e.name == name)
                .unwrap()
                .value
                .as_deref()
        };
        assert_eq!(get("PGDATABASE"), Some("postgres"));
        assert_eq!(get("PGUSER"), Some("postgres"));
        let vol = pod
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .find(|v| v.name == "pg-secret")
            .unwrap();
        let items = vol.secret.as_ref().unwrap().items.as_ref().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].key, "postgres-password");
        assert_eq!(items[0].path, "password");
        assert_eq!(
            vol.secret.as_ref().unwrap().secret_name.as_deref(),
            Some("airflow-postgresql")
        );
    }

    #[test]
    fn the_uploader_image_is_pinned_by_digest() {
        assert!(AWS_CLI_IMAGE.contains("@sha256:"));
        let cj = superset_cron_job("stardelt");
        let pod = &cj
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap();
        assert_eq!(pod.containers[0].image.as_deref(), Some(AWS_CLI_IMAGE));
    }

    #[test]
    fn neither_job_mounts_a_service_account_token() {
        let s = superset_cron_job("stardelt");
        let a = airflow_cron_job("stardelt");
        let sp = &s
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap();
        let ap = &a
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap();
        assert_eq!(sp.automount_service_account_token, Some(false));
        assert_eq!(ap.automount_service_account_token, Some(false));
    }
}
