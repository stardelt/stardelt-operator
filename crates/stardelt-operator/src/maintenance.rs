//! The per-`Catalog` maintenance CronJob (operator#476, slice b): one Job at
//! `spec.maintenance.schedule` that runs `stardelt maintenance run --catalog <name>` as the
//! team's pipeline principal. Pure builders; the `Catalog` controller that applies them and
//! writes `MaintenanceReady` is slice b2.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, Job, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Capabilities, ConfigMap, Container, EnvVar, EnvVarSource, LocalObjectReference, Pod, PodSpec,
    PodTemplateSpec, ResourceRequirements, SeccompProfile, SecretKeySelector, SecretVolumeSource,
    SecurityContext, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use kube::api::{Api, ListParams, LogParams, ObjectMeta};
use kube::runtime::controller::Action;

use crate::controller::Context;
use crate::crd::{
    Catalog, Condition, MaintenanceSpec, MaintenanceStatus, ServicePrincipal, principal_client_id,
};
use crate::identity::PLATFORM_REALM;
use crate::objects::FIELD_MANAGER;

/// Prefix of the CronJob name; the catalog name follows (`maint-<catalog>`).
pub const JOB_PREFIX: &str = "maint-";
/// The CLI image (stardelt/stardelt-python#62), pinned by digest: the digest is what runs and
/// the `:dev` tag is kept for readability only, as in `values/*.yaml`.
pub const IMAGE: &str = "ghcr.io/stardelt/stardelt-cli:dev@sha256:6b24c07c4271870be966d8bcfe85b691708d74a9cbf006263dc4bf928e220b63";

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

/// The minute a catalog's maintenance starts when it keeps the default schedule
/// (operator#490): nine catalogs on one node used to start in the same second.
///
/// FNV-1a over the name in `1..=49`, so minute 0 stays free for an explicit schedule and
/// the value is stable across releases (unlike `DefaultHasher`, which is not guaranteed
/// to keep its algorithm).
pub fn stagger_minute(catalog: &str) -> u32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in catalog.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    1 + (hash % 49) as u32
}

/// The schedule the CronJob actually carries: the catalog's own when it set one, the
/// name-staggered default otherwise.
pub fn effective_schedule(catalog: &str, spec: &MaintenanceSpec) -> String {
    if spec.schedule == MaintenanceSpec::DEFAULT_SCHEDULE {
        format!("{} 2 * * *", stagger_minute(catalog))
    } else {
        spec.schedule.clone()
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
            schedule: effective_schedule(catalog, spec),
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

/// The conditions after replacing the one of `next`'s type and keeping every other one, so a
/// controller patch never wipes `CatalogReady`/`GrantsReady`/`Drift` (the access-sync job
/// writes the whole array with a merge-patch).
pub fn merge_condition(existing: &[Condition], next: Condition) -> Vec<Condition> {
    let mut conditions: Vec<Condition> = existing
        .iter()
        .filter(|c| c.type_ != next.type_)
        .cloned()
        .collect();
    conditions.push(next);
    conditions
}

/// The summary line `stardelt maintenance run` writes last:
/// `{"catalog": …, "tablesDone": n, "tablesFailed": m}`. It is the last such line, not the last
/// line: a warning after it must not hide the result.
pub fn parse_summary(log: &str) -> Option<(i64, i64)> {
    log.lines().rev().find_map(|line| {
        let value: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
        Some((
            value.get("tablesDone")?.as_i64()?,
            value.get("tablesFailed")?.as_i64()?,
        ))
    })
}

/// When a Job finished: `status.completionTime` for a Job that completed, or the `Failed`
/// condition's `lastTransitionTime` for one that failed (a failed Job never gets a completion
/// time). `None` while it still runs.
fn finished_at(job: &Job) -> Option<String> {
    let status = job.status.as_ref()?;
    if let Some(time) = status.completion_time.as_ref() {
        return Some(time.0.to_string());
    }
    status
        .conditions
        .iter()
        .flatten()
        .find(|condition| condition.type_ == "Failed" && condition.status == "True")
        .and_then(|condition| condition.last_transition_time.as_ref())
        .map(|time| time.0.to_string())
}

/// The newest finished Job of this catalog's CronJob and its summary, if it has one. A Job is
/// finished when it completed or failed (operator#507); a failed Job has no `completionTime`.
async fn last_maintenance(
    ctx: &Context,
    namespace: &str,
    catalog: &str,
) -> Result<Option<MaintenanceStatus>, kube::Error> {
    let jobs: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);
    let selector = format!("stardelt.io/catalog={catalog}");
    let mut done: Vec<(String, Job)> = jobs
        .list(&ListParams::default().labels(&selector))
        .await?
        .items
        .into_iter()
        .filter_map(|job| finished_at(&job).map(|at| (at, job)))
        .collect();
    done.sort_by(|a, b| a.0.cmp(&b.0));
    let Some((finished, job)) = done.pop() else {
        return Ok(None);
    };
    let status = job.status.clone().unwrap_or_default();
    let result = if status.succeeded.unwrap_or(0) > 0 {
        "Succeeded"
    } else {
        "Failed"
    };
    let summary = pod_summary(
        ctx,
        namespace,
        job.metadata.name.as_deref().unwrap_or_default(),
    )
    .await;
    Ok(Some(MaintenanceStatus {
        last_run: Some(finished),
        last_result: Some(result.to_string()),
        tables_done: summary.map(|(done, _)| done),
        tables_failed: summary.map(|(_, failed)| failed),
    }))
}

/// The pod log of one Job's pod, parsed with [`parse_summary`]. A refusal (the pod was already
/// deleted, the log is gone) is not a reconcile failure: the counts are simply absent.
async fn pod_summary(ctx: &Context, namespace: &str, job: &str) -> Option<(i64, i64)> {
    let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);
    let selector = format!("job-name={job}");
    let pod = pods
        .list(&ListParams::default().labels(&selector))
        .await
        .ok()?
        .items
        .into_iter()
        .next()?;
    let log = pods
        .logs(
            &pod.metadata.name?,
            &LogParams {
                container: Some("maintenance".to_string()),
                ..LogParams::default()
            },
        )
        .await
        .ok()?;
    parse_summary(&log)
}

fn catalog_owner(catalog: &Catalog) -> OwnerReference {
    OwnerReference {
        api_version: "stardelt.io/v1alpha1".into(),
        kind: "Catalog".into(),
        name: catalog.name_any(),
        uid: catalog.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// `token_url` for this platform: the external issuer's endpoint when the contract carries
/// `provider`, the in-cluster Keycloak otherwise.
fn token_endpoint(identity: Option<&ConfigMap>) -> String {
    let data = identity.and_then(|c| c.data.as_ref());
    let external = data.is_some_and(|d| d.contains_key("provider"));
    let issuer = data.and_then(|d| d.get("issuerUrl")).map(String::as_str);
    token_url(if external { issuer } else { None })
}

/// One `Catalog` -> one `maint-<catalog>` CronJob. On a missing `pipeline-<owner>` principal the
/// condition says so and nothing is applied (the principal controller creates it).
pub async fn reconcile_catalog(
    catalog: Arc<Catalog>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    let namespace = catalog.namespace().unwrap_or_default();
    let name = catalog.name_any();
    let owner = catalog.spec.owner.clone();
    let principal = format!("pipeline-{owner}");

    let identity: Option<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace)
        .get_opt("stardelt-identity")
        .await?;

    let principals: Api<ServicePrincipal> = Api::namespaced(ctx.client.clone(), &namespace);
    let condition = match principals.get_opt(&principal).await? {
        None => Condition {
            type_: "MaintenanceReady".into(),
            status: "False".into(),
            reason: Some("NoRunner".into()),
            message: Some(format!(
                "create ServicePrincipal {principal} to run maintenance"
            )),
            last_transition_time: Some(k8s_openapi::jiff::Timestamp::now().to_string()),
            observed_generation: catalog.metadata.generation,
        },
        Some(_) => {
            let job = cron_job(
                &namespace,
                &name,
                &owner,
                &catalog.spec.maintenance,
                &token_endpoint(identity.as_ref()),
                &catalog_owner(&catalog),
            );
            crate::objects::apply(&ctx.client, &namespace, &job, false).await?;
            Condition {
                type_: "MaintenanceReady".into(),
                status: "True".into(),
                reason: Some("Scheduled".into()),
                message: Some(format!(
                    "{} on {}",
                    job_name(&name),
                    effective_schedule(&name, &catalog.spec.maintenance)
                )),
                last_transition_time: Some(k8s_openapi::jiff::Timestamp::now().to_string()),
                observed_generation: catalog.metadata.generation,
            }
        }
    };

    let maintenance = last_maintenance(&ctx, &namespace, &name).await?;

    let existing = catalog
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    let conditions = merge_condition(&existing, condition);
    let observed = catalog.metadata.generation;
    let api: Api<Catalog> = Api::namespaced(ctx.client.clone(), &namespace);
    let mut status = serde_json::json!({
        "conditions": conditions,
        "observedGeneration": observed,
    });
    if let Some(maintenance) = maintenance {
        status["maintenance"] = serde_json::to_value(&maintenance)
            .expect("MaintenanceStatus is a plain struct and always serializes");
    }
    api.patch_status(
        &name,
        &kube::api::PatchParams::default(),
        &kube::api::Patch::Merge(serde_json::json!({ "status": status })),
    )
    .await?;
    tracing::info!(catalog = %name, %namespace, "reconciled maintenance job");
    Ok(Action::requeue(Duration::from_secs(300)))
}

pub fn error_policy<K>(_object: Arc<K>, error: &kube::Error, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %error, "maintenance reconcile failed");
    Action::requeue(Duration::from_secs(30))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{CatalogDomainStatus, MaintenanceSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use kube::Client;
    use std::sync::Mutex;

    fn catalog() -> Catalog {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Catalog",
            "metadata": {
                "name": "mobility",
                "namespace": "data",
                "uid": "uid-1",
                "generation": 3
            },
            "spec": { "lakehouseRef": { "name": "main" }, "owner": "analytics" },
            "status": {
                "conditions": [
                    { "type": "CatalogReady", "status": "True" }
                ]
            }
        }))
        .expect("deserialize Catalog")
    }

    fn mock_client<F>(handler: F) -> (Client, Arc<Mutex<Vec<String>>>)
    where
        F: Fn(&str, &str, &str) -> (u16, String) + Send + Sync + 'static,
    {
        use http_body_util::BodyExt;
        let requests: Arc<Mutex<Vec<String>>> = Arc::default();
        let recorder = requests.clone();
        let handler = Arc::new(handler);
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let handler = handler.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            let query = request.uri().query().unwrap_or_default().to_string();
            async move {
                let body_bytes = request
                    .into_body()
                    .collect()
                    .await
                    .map(|b| b.to_bytes().to_vec())
                    .unwrap_or_default();
                let body = String::from_utf8_lossy(&body_bytes).to_string();
                recorder
                    .lock()
                    .expect("requests poisoned")
                    .push(format!("{method} {path}?{query} {body}"));
                let (status, document) = handler(&method, &path, &body);
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(document.into_bytes()))
                        .expect("valid mock response"),
                )
            }
        });
        (Client::new(service, "stardelt"), requests)
    }

    fn ctx(client: Client) -> Arc<Context> {
        Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        })
    }

    /// A `Catalog` with its pipeline principal applies `maint-<catalog>` and writes
    /// `MaintenanceReady` without dropping the access-sync condition.
    #[tokio::test]
    async fn a_catalog_with_a_pipeline_principal_applies_the_job_and_one_condition() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.ends_with("/configmaps/stardelt-identity") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-identity","namespace":"data"},"data":{"issuerUrl":"https://id.example.com/realms/stardelt"}}"#.into(),
                );
            }
            if method == "GET" && path.ends_with("/serviceprincipals/pipeline-analytics") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"ServicePrincipal","metadata":{"name":"pipeline-analytics","namespace":"data"},"spec":{"ownerGroup":"analytics"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/cronjobs/maint-mobility") {
                return (
                    200,
                    r#"{"apiVersion":"batch/v1","kind":"CronJob","metadata":{"name":"maint-mobility","namespace":"data"}}"#.into(),
                );
            }
            if method == "GET" && path.ends_with("/jobs") {
                return (
                    200,
                    r#"{"apiVersion":"batch/v1","kind":"JobList","items":[]}"#.into(),
                );
            }
            if method == "PATCH" && path.ends_with("/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"Catalog","metadata":{"name":"mobility","namespace":"data"},"spec":{"lakehouseRef":{"name":"main"},"owner":"analytics"},"status":{"conditions":[{"type":"CatalogReady","status":"True"},{"type":"MaintenanceReady","status":"True"}]}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let action = reconcile_catalog(Arc::new(catalog()), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(300)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(
            log.contains("PATCH /apis/batch/v1/namespaces/data/cronjobs/maint-mobility"),
            "{log}"
        );
        let status = log
            .lines()
            .find(|line| line.contains("/status"))
            .unwrap_or_else(|| panic!("no status patch\n{log}"));
        assert!(
            status.starts_with(
                "PATCH /apis/stardelt.io/v1alpha1/namespaces/data/catalogs/mobility/status"
            ),
            "{status}"
        );
        assert!(status.contains("\"MaintenanceReady\""), "{status}");
        assert!(status.contains("\"CatalogReady\""), "{status}");
        assert!(status.contains("\"observedGeneration\":3"), "{status}");
    }

    /// Without `pipeline-<owner>` the condition says so and no CronJob is applied.
    #[tokio::test]
    async fn a_missing_pipeline_principal_writes_no_runner_and_applies_nothing() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.ends_with("/configmaps/stardelt-identity") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-identity","namespace":"data"},"data":{"issuerUrl":"https://id.example.com/realms/stardelt"}}"#.into(),
                );
            }
            if method == "GET" && path.ends_with("/serviceprincipals/pipeline-analytics") {
                return (
                    404,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}"#.into(),
                );
            }
            if method == "GET" && path.ends_with("/jobs") {
                return (
                    200,
                    r#"{"apiVersion":"batch/v1","kind":"JobList","items":[]}"#.into(),
                );
            }
            if method == "PATCH" && path.ends_with("/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"Catalog","metadata":{"name":"mobility","namespace":"data"},"spec":{"lakehouseRef":{"name":"main"},"owner":"analytics"},"status":{"conditions":[]}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        reconcile_catalog(Arc::new(catalog()), ctx(client))
            .await
            .expect("reconcile");
        let log = requests.lock().expect("requests").join("\n");
        assert!(!log.contains("/cronjobs/"), "{log}");
        let status = log
            .lines()
            .find(|line| line.contains("/status"))
            .unwrap_or_else(|| panic!("no status patch\n{log}"));
        assert!(status.contains("\"NoRunner\""), "{status}");
        assert!(status.contains("\"MaintenanceReady\""), "{status}");
    }

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
    fn the_default_schedule_is_staggered_by_catalog_name() {
        // The nine catalogs on the dev cluster (operator#490): with the default schedule they
        // all fired at 02:00:00Z. They must now start in different minutes, and the values must
        // not drift between releases (a changed hash re-schedules everyone).
        let spec = MaintenanceSpec::default();
        let expected = [
            ("mobility", "44 2 * * *"),
            ("mobilitylab", "28 2 * * *"),
            ("mobilityref", "17 2 * * *"),
            ("qaenergy", "3 2 * * *"),
            ("qaenergylab", "6 2 * * *"),
            ("qaenergyref", "28 2 * * *"),
            ("retail", "44 2 * * *"),
            ("retaillab", "15 2 * * *"),
            ("retailref", "40 2 * * *"),
        ];
        for (catalog, schedule) in expected {
            assert_eq!(effective_schedule(catalog, &spec), schedule, "{catalog}");
        }
        let minutes: std::collections::BTreeSet<String> = expected
            .iter()
            .map(|(catalog, _)| effective_schedule(catalog, &spec))
            .collect();
        assert!(
            minutes.len() >= 7,
            "the nine catalogs must spread over the hour, got {minutes:?}"
        );
        assert!(
            !minutes.contains("0 2 * * *"),
            "no catalog may keep the unstaggered default"
        );
    }

    #[test]
    fn an_explicit_schedule_is_kept_verbatim() {
        let spec = MaintenanceSpec {
            schedule: "15 4 * * 1".to_string(),
            ..MaintenanceSpec::default()
        };
        assert_eq!(effective_schedule("mobility", &spec), "15 4 * * 1");
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
        assert_eq!(spec.schedule, "44 2 * * *");
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
    fn the_maintenance_cli_is_pinned_by_digest() {
        assert!(
            IMAGE.contains("@sha256:"),
            "the maintenance CronJob must run a digest, not the mutable dev tag: {IMAGE}"
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

    #[test]
    fn a_condition_patch_keeps_the_access_sync_conditions() {
        let existing = vec![
            Condition {
                type_: "CatalogReady".into(),
                status: "True".into(),
                reason: None,
                message: None,
                last_transition_time: None,
                observed_generation: None,
            },
            Condition {
                type_: "GrantsReady".into(),
                status: "True".into(),
                reason: None,
                message: None,
                last_transition_time: None,
                observed_generation: None,
            },
        ];
        let next = Condition {
            type_: "MaintenanceReady".into(),
            status: "True".into(),
            reason: Some("Scheduled".into()),
            message: None,
            last_transition_time: None,
            observed_generation: Some(3),
        };
        let merged = merge_condition(&existing, next.clone());
        assert_eq!(
            merged
                .iter()
                .filter(|c| c.type_ == "MaintenanceReady")
                .count(),
            1
        );
        assert!(merged.iter().any(|c| c.type_ == "CatalogReady"));
        assert!(merged.iter().any(|c| c.type_ == "GrantsReady"));
        assert_eq!(merged.last(), Some(&next));
    }

    #[test]
    fn an_external_contract_uses_its_issuer_and_platform_mode_the_keycloak_service() {
        let platform = ConfigMap {
            data: Some(BTreeMap::from([(
                "issuerUrl".to_string(),
                "https://id.example.com/realms/stardelt".to_string(),
            )])),
            ..ConfigMap::default()
        };
        assert_eq!(
            token_endpoint(Some(&platform)),
            "http://keycloak:8080/realms/stardelt/protocol/openid-connect/token"
        );
        let external = ConfigMap {
            data: Some(BTreeMap::from([
                (
                    "issuerUrl".to_string(),
                    "https://login.example.com/tenant".to_string(),
                ),
                ("provider".to_string(), "entra".to_string()),
            ])),
            ..ConfigMap::default()
        };
        assert_eq!(
            token_endpoint(Some(&external)),
            "https://login.example.com/tenant/protocol/openid-connect/token"
        );
    }

    #[test]
    fn the_summary_line_is_read_from_the_end() {
        let log = "{\"table\":\"silver.trips\",\"smallFiles\":14,\"optimized\":true,\"seconds\":2.1,\"error\":null}\n\
                   {\"table\":\"silver.zones\",\"smallFiles\":2,\"optimized\":false,\"seconds\":0.4,\"error\":\"boom\"}\n\
                   {\"catalog\":\"mobility\",\"tablesDone\":1,\"tablesFailed\":1}\n";
        assert_eq!(parse_summary(log), Some((1, 1)));
    }

    #[test]
    fn a_log_without_a_summary_line_has_no_counts() {
        assert_eq!(
            parse_summary("{\"table\":\"silver.trips\"}\nWARNING: slow query\n"),
            None
        );
        assert_eq!(parse_summary(""), None);
    }

    #[test]
    fn the_newest_summary_wins_when_a_log_repeats_one() {
        let log = "{\"catalog\":\"a\",\"tablesDone\":1,\"tablesFailed\":0}\n\
                   {\"catalog\":\"a\",\"tablesDone\":4,\"tablesFailed\":2}\n";
        assert_eq!(parse_summary(log), Some((4, 2)));
    }

    #[test]
    fn the_status_serializes_camel_case_maintenance() {
        let status = CatalogDomainStatus {
            maintenance: Some(MaintenanceStatus {
                last_run: Some("2026-11-11T02:44:12Z".to_string()),
                last_result: Some("Succeeded".to_string()),
                tables_done: Some(3),
                tables_failed: Some(0),
            }),
            ..CatalogDomainStatus::default()
        };
        let value = serde_json::to_value(&status).expect("serializes");
        assert_eq!(value["maintenance"]["lastRun"], "2026-11-11T02:44:12Z");
        assert_eq!(value["maintenance"]["lastResult"], "Succeeded");
        assert_eq!(value["maintenance"]["tablesDone"], 3);
        assert_eq!(
            serde_json::to_value(MaintenanceStatus::default())
                .expect("serializes")
                .get("lastRun"),
            None,
            "an absent result is left out, not sent as null"
        );
    }

    fn job_with(status: k8s_openapi::api::batch::v1::JobStatus) -> Job {
        Job {
            status: Some(status),
            ..Job::default()
        }
    }

    #[test]
    fn a_failed_job_counts_as_finished() {
        let at: k8s_openapi::jiff::Timestamp = "2026-11-10T07:23:52Z".parse().expect("a timestamp");
        let job = job_with(k8s_openapi::api::batch::v1::JobStatus {
            failed: Some(1),
            completion_time: None,
            conditions: Some(vec![k8s_openapi::api::batch::v1::JobCondition {
                type_: "Failed".to_string(),
                status: "True".to_string(),
                last_transition_time: Some(Time(at)),
                ..Default::default()
            }]),
            ..Default::default()
        });
        assert_eq!(finished_at(&job).as_deref(), Some("2026-11-10T07:23:52Z"));
    }

    #[test]
    fn a_completed_job_uses_its_completion_time() {
        let at: k8s_openapi::jiff::Timestamp = "2026-11-11T02:44:12Z".parse().expect("a timestamp");
        let job = job_with(k8s_openapi::api::batch::v1::JobStatus {
            succeeded: Some(1),
            completion_time: Some(Time(at)),
            ..Default::default()
        });
        assert_eq!(finished_at(&job).as_deref(), Some("2026-11-11T02:44:12Z"));
    }

    #[test]
    fn a_running_job_is_not_finished() {
        let job = job_with(k8s_openapi::api::batch::v1::JobStatus {
            active: Some(1),
            ..Default::default()
        });
        assert_eq!(finished_at(&job), None);
    }
}
