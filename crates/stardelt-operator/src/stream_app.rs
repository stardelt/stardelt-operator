//! A `StreamApp` (D42): the SparkApplication that runs it and the ConfigMap that carries the
//! job. Pure builders plus the reconciler that applies them.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{
    DeleteParams, DynamicObject, ListParams, ObjectMeta, Patch, PatchParams, TypeMeta,
};
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};
use serde_json::json;

use crate::controller::Context;
use crate::crd::{
    Condition, Connection, SinkMode, StreamApp, StreamAppSpec, StreamAppStatus, StreamRestartPolicy,
};
use crate::objects::{self, FIELD_MANAGER};
use crate::spark;

pub const JOB_FILE: &str = "stream.py";
pub const JOB_MOUNT: &str = "/opt/job";

/// The bus the streaming pillar installs (`kafka.rs`: `Kafka` `kafka`, plain listener 9092).
pub fn internal_bootstrap(namespace: &str) -> String {
    format!("kafka-kafka-bootstrap.{namespace}.svc.cluster.local:9092")
}

fn labels() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            FIELD_MANAGER.to_string(),
        ),
        ("stardelt.io/component".to_string(), "stream".to_string()),
    ])
}

pub fn job_config_map(namespace: &str, app: &str, owner: &OwnerReference) -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{app}-job")),
            namespace: Some(namespace.to_string()),
            labels: Some(labels()),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            JOB_FILE.to_string(),
            include_str!("stream_job.py").to_string(),
        )])),
        ..Default::default()
    }
}

pub fn checkpoint_for(spec: &StreamAppSpec, namespace: &str, app: &str) -> String {
    match spec.sink.checkpoint_prefix.as_deref() {
        Some(prefix) => format!("s3a://lakehouse/{prefix}"),
        None => format!("s3a://lakehouse/checkpoints/{namespace}/{app}"),
    }
}

fn resolve_run_as(spec: &StreamAppSpec) -> Result<String, String> {
    if let Some(run_as) = spec.run_as.as_deref() {
        return Ok(run_as.to_string());
    }
    if let Some(group) = spec.owner_group.as_deref() {
        return Ok(format!("pipeline-{group}"));
    }
    Err("runAs is required (or set ownerGroup to derive pipeline-<group>)".to_string())
}

/// A Kubernetes object name for the `KafkaTopic`. The topic itself may contain
/// characters a DNS-1123 subdomain does not allow (`_`, upper case) or may be
/// owned by another `KafkaTopic`, so the topic name lives in `spec.topicName`.
fn topic_resource_name(app: &str, topic: &str) -> String {
    let legal = !topic.is_empty()
        && topic.len() <= 63
        && topic
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
        && topic.starts_with(|c: char| c.is_ascii_alphanumeric())
        && topic.ends_with(|c: char| c.is_ascii_alphanumeric());
    if legal {
        topic.to_string()
    } else {
        format!("stream-{app}")
    }
}

pub fn kafka_topic(app: &str, topic: &str, owner: &OwnerReference) -> DynamicObject {
    let mut topic_labels = labels();
    topic_labels.insert("strimzi.io/cluster".to_string(), "kafka".to_string());
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "kafka.strimzi.io/v1".into(),
            kind: "KafkaTopic".into(),
        }),
        metadata: ObjectMeta {
            name: Some(topic_resource_name(app, topic)),
            labels: Some(topic_labels),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        data: json!({
            "spec": {
                "topicName": topic,
                "partitions": 1,
                "replicas": 1,
                "config": {
                    "retention.ms": "604800000"
                }
            }
        }),
    }
}

/// The name of an existing `KafkaTopic` that already manages `topic`, if any.
/// The topic name is `spec.topicName` when set (Strimzi), otherwise the object name.
pub fn topic_owner(existing: &[DynamicObject], topic: &str) -> Option<String> {
    existing.iter().find_map(|t| {
        let effective = t
            .data
            .pointer("/spec/topicName")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| t.metadata.name.as_deref().unwrap_or(""));
        (effective == topic).then(|| t.name_any())
    })
}

/// Whether an existing `KafkaTopic` already satisfies `desired`: it manages the same
/// topic and its `spec` is what the operator would apply. The object's name may differ
/// (the demo applies `pegelonline-continuous` for the topic `pegelonline.continuous`),
/// and the operator writes nothing in either case. A spec that differs is a real
/// conflict and must stay a warning (`#320`).
pub fn topic_satisfied(existing: &[DynamicObject], topic: &str, desired: &DynamicObject) -> bool {
    existing.iter().any(|t| {
        let effective = t
            .data
            .pointer("/spec/topicName")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| t.metadata.name.as_deref().unwrap_or(""));
        effective == topic && t.data.get("spec") == desired.data.get("spec")
    })
}

pub fn spark_application(
    namespace: &str,
    app: &str,
    spec: &StreamAppSpec,
    bootstrap: &str,
    checkpoint: &str,
    owner: &OwnerReference,
) -> Result<DynamicObject, String> {
    let run_as = resolve_run_as(spec)?;
    let sp = format!("sp-{run_as}");
    let restart = match spec.restart_policy {
        StreamRestartPolicy::Always => "Always",
        StreamRestartPolicy::OnFailure => "OnFailure",
    };
    let merge_keys = spec.sink.merge_keys.join(",");
    let catalog_uri = format!("http://lakekeeper.{namespace}.svc.cluster.local:8181/catalog");
    let mode = match spec.sink.mode {
        SinkMode::Merge => "merge",
        SinkMode::Append => "append",
    };
    let c = &spec.sink.catalog;
    let token_url = format!(
        "http://keycloak:8080/realms/{}/protocol/openid-connect/token",
        crate::identity::PLATFORM_REALM
    );
    let s3_secret = crate::prerequisites::S3_SECRET;
    let mut spark_conf = json!({
        // The image carries both jars already (`iceberg-spark-runtime.jar` and the Kafka
        // connector, platform#601). `spark.jars.packages` would resolve them from Maven
        // Central at submit time — against the "pinned artifacts, not Maven at pod start"
        // rule — and the published coordinates are Scala 2.12/3.5, while the image is
        // Spark 4.1.3/Scala 2.13, so the fetched jars could not link.
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.hadoop.fs.s3a.endpoint": "http://seaweedfs-s3:8333",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    });
    spark_conf[format!("spark.sql.catalog.{c}.s3.endpoint")] = json!("http://seaweedfs-s3:8333");
    spark_conf[format!("spark.sql.catalog.{c}.s3.path-style-access")] = json!("true");
    // The executor reads and writes Iceberg data files, so it needs the same S3 credentials
    // as the driver. The Spark operator maps `spark.kubernetes.executor.secretKeyRef.*` to
    // executor environment variables (the shape Spark Connect already uses).
    spark_conf["spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID"] =
        json!(format!("{s3_secret}:access-key"));
    spark_conf["spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY"] =
        json!(format!("{s3_secret}:secret-key"));
    spark_conf["spark.kubernetes.executor.secretKeyRef.AWS_REGION"] =
        json!(format!("{s3_secret}:region"));
    // The executor may refresh the catalog token (OAuth2Util.AuthSession), so it needs the
    // same catalog identity as the driver (#341). Client id/secret come from the principal
    // Secret; the token URL is not secret-backed (the driver passes it as a literal value),
    // so it is injected as a plain executor env var rather than a secretKeyRef.
    spark_conf["spark.kubernetes.executor.secretKeyRef.CATALOG_CLIENT_ID"] =
        json!(format!("{sp}:client-id"));
    spark_conf["spark.kubernetes.executor.secretKeyRef.CATALOG_CLIENT_SECRET"] =
        json!(format!("{sp}:client-secret"));
    spark_conf["spark.executorEnv.CATALOG_TOKEN_URL"] = json!(token_url);

    Ok(DynamicObject {
        types: Some(TypeMeta {
            api_version: "sparkoperator.k8s.io/v1beta2".into(),
            kind: "SparkApplication".into(),
        }),
        metadata: ObjectMeta {
            name: Some(app.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(labels()),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        data: json!({
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": spark::image(),
                "imagePullPolicy": "IfNotPresent",
                "sparkVersion": "4.1.3",
                "mainApplicationFile": "local:///opt/job/stream.py",
                "arguments": [
                    "--bootstrap-servers", bootstrap,
                    "--topic", spec.source.topic,
                    "--catalog", spec.sink.catalog,
                    "--catalog-uri", catalog_uri,
                    "--table", spec.sink.table,
                    "--mode", mode,
                    "--merge-keys", merge_keys,
                    "--checkpoint", checkpoint,
                ],
                "restartPolicy": restart_policy(restart),
                "sparkConf": spark_conf,
                "driver": {
                    "cores": 1,
                    "coreLimit": "1200m",
                    "memory": "2g",
                    "memoryOverhead": "1g",
                    "serviceAccount": "spark-connect",
                    "env": [
                        {"name": "CATALOG_CLIENT_ID", "valueFrom": {"secretKeyRef": {"name": sp, "key": "client-id"}}},
                        {"name": "CATALOG_CLIENT_SECRET", "valueFrom": {"secretKeyRef": {"name": sp, "key": "client-secret"}}},
                        {"name": "CATALOG_TOKEN_URL", "value": token_url},
                        {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": s3_secret, "key": "access-key"}}},
                        {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": s3_secret, "key": "secret-key"}}},
                        {"name": "AWS_REGION", "valueFrom": {"secretKeyRef": {"name": s3_secret, "key": "region"}}}
                    ],
                    "volumeMounts": [{"name": "job", "mountPath": JOB_MOUNT}],
                },
                "executor": {
                    "cores": 1,
                    "instances": 1,
                    "memory": "2g",
                    "memoryOverhead": "1g",
                    "volumeMounts": [{"name": "job", "mountPath": JOB_MOUNT}],
                },
                "volumes": [{"name": "job", "configMap": {"name": format!("{app}-job")}}],
            }
        }),
    })
}

fn owner_reference(app: &StreamApp) -> OwnerReference {
    OwnerReference {
        api_version: "stardelt.io/v1alpha1".into(),
        kind: "StreamApp".into(),
        name: app.name_any(),
        uid: app.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

fn now_rfc3339() -> String {
    k8s_openapi::jiff::Timestamp::now().to_string()
}

/// How long a just-(re)submitted application counts as `Restarting`. Spark resubmits a
/// failed streaming app within seconds; a submission that has run this long is healthy
/// again even though its cumulative `executionAttempts` stays > 1 (operator#322, this ticket).
const RESTART_WINDOW_SECS: i64 = 600;
/// A `PENDING_RERUN` SparkApplication with no pod is only resubmitted after this many
/// seconds, so a normal restart (the Spark operator recreates the driver) is not cut short.
const PENDING_RERUN_GRACE_SECS: i64 = 120;
/// Never resubmit more than this many times without the counter being reset by a run.
const PENDING_RERUN_MAX_RECOVERIES: i64 = 3;
const PENDING_RERUN_ANNOTATION: &str = "stardelt.io/pending-rerun-recoveries";

/// The `phase` and `Ready` reason for a `SparkApplication` status.
pub fn stream_state(
    state: Option<&str>,
    execution_attempts: i64,
    last_submission: Option<&str>,
    now: k8s_openapi::jiff::Timestamp,
) -> (&'static str, &'static str) {
    match state {
        Some("RUNNING") | Some("SUBMITTED") => {
            let fresh = last_submission
                .and_then(|s| s.parse::<k8s_openapi::jiff::Timestamp>().ok())
                .map(|t| {
                    let age = now.as_second() - t.as_second();
                    (0..RESTART_WINDOW_SECS).contains(&age)
                })
                .unwrap_or(false);
            if execution_attempts > 1 && fresh {
                ("Running", "Restarting")
            } else {
                ("Running", "Running")
            }
        }
        Some("COMPLETED") => ("Stopped", "Stopped"),
        Some("FAILED") | Some("SUBMISSION_FAILED") => ("Failed", "Failed"),
        _ => ("Pending", "Pending"),
    }
}

/// Whether a `PENDING_RERUN` application is wedged: no pod of any kind is left and the
/// object is older than the grace period. A missing creation timestamp is *not* wedged.
pub fn pending_rerun_is_wedged(
    state: Option<&str>,
    pod_count: usize,
    created: Option<i64>,
    now: i64,
) -> bool {
    state == Some("PENDING_RERUN")
        && pod_count == 0
        && created.is_some_and(|c| now - c > PENDING_RERUN_GRACE_SECS)
}

/// The scheduler's sentence for a pod it cannot place, or `None` when the pod is
/// schedulable or has already run. A driver can be `RUNNING` while every executor waits
/// like this (for example "0/1 nodes are available: 1 Insufficient cpu"), and Spark
/// reports the application as healthy — so the stream must say it is not doing work.
pub fn unschedulable_message(pod: &Pod) -> Option<String> {
    let status = pod.status.as_ref()?;
    if status.phase.as_deref() != Some("Pending") {
        return None;
    }
    let conditions = status.conditions.as_deref()?;
    conditions.iter().find_map(|c| {
        (c.type_ == "PodScheduled" && c.status == "False").then(|| {
            c.message
                .clone()
                .unwrap_or_else(|| "the executor is not schedulable".to_string())
        })
    })
}

/// The v2 controller refuses to retry when the interval fields are absent (`invalid retry
/// interval`), even when `type` is set, so both failure paths carry explicit values.
fn restart_policy(restart: &str) -> serde_json::Value {
    json!({
        "type": restart,
        "onFailureRetries": 3,
        "onFailureRetryInterval": 10,
        "onSubmissionFailureRetries": 3,
        "onSubmissionFailureRetryInterval": 10,
    })
}

fn ready_condition(
    phase: &str,
    reason: &str,
    generation: Option<i64>,
    previous: Option<&Condition>,
    now: &str,
) -> Condition {
    let status = if phase == "Running" && reason != "ExecutorsUnschedulable" {
        "True"
    } else {
        "False"
    };
    let last_transition_time = match previous {
        Some(p) if p.status == status => p.last_transition_time.clone(),
        _ => Some(now.to_string()),
    };
    Condition {
        type_: "Ready".into(),
        status: status.into(),
        reason: Some(reason.into()),
        message: None,
        last_transition_time,
        observed_generation: generation,
    }
}

#[allow(clippy::too_many_arguments)]
async fn patch_stream_status(
    apps: &Api<StreamApp>,
    name: &str,
    generation: Option<i64>,
    previous: Option<&Condition>,
    phase: &str,
    reason: &str,
    last_error: Option<String>,
    attempts: Option<i64>,
    last_submitted_at: Option<String>,
) -> Result<(), kube::Error> {
    let now = now_rfc3339();
    let status = StreamAppStatus {
        phase: phase.into(),
        last_checkpoint_at: None,
        last_error,
        attempts,
        last_submitted_at,
        conditions: vec![ready_condition(phase, reason, generation, previous, &now)],
        observed_generation: generation,
    };
    apps.patch_status(
        name,
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": status })),
    )
    .await?;
    Ok(())
}

fn is_not_found(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(r) if r.code == 404)
}

pub async fn reconcile_stream_app(
    app: Arc<StreamApp>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    if app.metadata.deletion_timestamp.is_some() {
        return Ok(Action::await_change());
    }
    let name = app.name_any();
    let namespace = app.namespace().unwrap_or_else(|| "default".into());
    let apps: Api<StreamApp> = Api::namespaced(ctx.client.clone(), &namespace);
    let previous = app
        .status
        .as_ref()
        .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready"));
    let generation = app.metadata.generation;

    if app.spec.stopped {
        let spark_api: Api<DynamicObject> = Api::namespaced_with(
            ctx.client.clone(),
            &namespace,
            &objects::spark_application_resource(),
        );
        match spark_api.delete(&name, &DeleteParams::default()).await {
            Ok(_) => {}
            Err(e) if is_not_found(&e) => {}
            Err(e) => return Err(e),
        }
        patch_stream_status(
            &apps, &name, generation, previous, "Stopped", "Stopped", None, None, None,
        )
        .await?;
        tracing::info!(stream = %name, %namespace, "stream stopped");
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    let (bootstrap, create_topic) = match app.spec.source.connection_ref.as_deref() {
        Some(conn_name) => {
            let conn = Api::<Connection>::namespaced(ctx.client.clone(), &namespace)
                .get(conn_name)
                .await;
            let bootstrap = match conn {
                Ok(c) => c.spec.config.get("bootstrapServers").cloned(),
                Err(e) if is_not_found(&e) => None,
                Err(e) => return Err(e),
            };
            match bootstrap {
                Some(b) => (b, false),
                None => {
                    let msg =
                        format!("source Connection {conn_name} has no config.bootstrapServers");
                    patch_stream_status(
                        &apps,
                        &name,
                        generation,
                        previous,
                        "Failed",
                        "Failed",
                        Some(msg),
                        None,
                        None,
                    )
                    .await?;
                    return Ok(Action::requeue(Duration::from_secs(60)));
                }
            }
        }
        None => (internal_bootstrap(&namespace), true),
    };

    let owner = owner_reference(&app);
    let checkpoint = checkpoint_for(&app.spec, &namespace, &name);
    if create_topic {
        let topic = kafka_topic(&name, &app.spec.source.topic, &owner);
        let topics: Api<DynamicObject> = Api::namespaced_with(
            ctx.client.clone(),
            &namespace,
            &objects::kafka_topic_resource(),
        );
        let existing = topics.list(&Default::default()).await?.items;
        let own = topic.name_any();
        match topic_owner(&existing, &app.spec.source.topic) {
            Some(other) if other != own => {
                if topic_satisfied(&existing, &app.spec.source.topic, &topic) {
                    // Nothing to apply: the topic exists with the spec we want. Debug, not
                    // warn — the reconcile runs every 60 s on a healthy stream (#320).
                    tracing::debug!(
                        stream = %name, %namespace, topic = %app.spec.source.topic, owner = %other,
                        "topic is already managed by an identical KafkaTopic; nothing to apply"
                    );
                } else {
                    tracing::warn!(
                        stream = %name, %namespace, topic = %app.spec.source.topic, owner = %other,
                        "topic is already managed by another KafkaTopic with a different spec; not applying ours"
                    );
                }
            }
            _ => {
                objects::apply_dynamic(
                    &ctx.client,
                    &namespace,
                    &objects::kafka_topic_resource(),
                    &topic,
                    false,
                )
                .await?;
            }
        }
    }
    let cm = job_config_map(&namespace, &name, &owner);
    objects::apply(&ctx.client, &namespace, &cm, false).await?;
    let spark = spark_application(
        &namespace,
        &name,
        &app.spec,
        &bootstrap,
        &checkpoint,
        &owner,
    )
    .map_err(|e| kube::Error::Service(Box::new(std::io::Error::other(e))))?;
    objects::apply_dynamic(
        &ctx.client,
        &namespace,
        &objects::spark_application_resource(),
        &spark,
        false,
    )
    .await?;

    let spark_api: Api<DynamicObject> = Api::namespaced_with(
        ctx.client.clone(),
        &namespace,
        &objects::spark_application_resource(),
    );
    let (phase, reason, last_error, attempts, last_submitted_at) = match spark_api.get(&name).await
    {
        Ok(obj) => {
            let state = obj
                .data
                .get("status")
                .and_then(|s| s.get("applicationState"))
                .and_then(|a| a.get("state"))
                .and_then(|s| s.as_str());
            let err_msg = obj
                .data
                .get("status")
                .and_then(|s| s.get("applicationState"))
                .and_then(|a| a.get("errorMessage"))
                .and_then(|s| s.as_str())
                .map(|s| {
                    let mut t = s.to_string();
                    if t.len() > 400 {
                        t.truncate(400);
                    }
                    t
                });
            let execution_attempts = obj
                .data
                .get("status")
                .and_then(|s| s.get("executionAttempts"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let last_submission = obj
                .data
                .get("status")
                .and_then(|s| s.get("lastSubmissionAttemptTime"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let now = k8s_openapi::jiff::Timestamp::now();
            let (mut phase, mut reason) =
                stream_state(state, execution_attempts, last_submission.as_deref(), now);
            let mut last_error = err_msg;

            let recoveries = app
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(PENDING_RERUN_ANNOTATION))
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            let created = obj
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| t.0.as_second());

            if state == Some("PENDING_RERUN") {
                let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &namespace);
                let selector = format!("sparkoperator.k8s.io/app-name={name}");
                let pod_count = pods
                    .list(&ListParams::default().labels(&selector))
                    .await?
                    .items
                    .len();
                if pending_rerun_is_wedged(state, pod_count, created, now.as_second()) {
                    if recoveries >= PENDING_RERUN_MAX_RECOVERIES {
                        phase = "Failed";
                        reason = "Failed";
                        last_error = Some(format!(
                            "the Spark application stayed in PENDING_RERUN with no pods after \
                             {PENDING_RERUN_MAX_RECOVERIES} resubmissions"
                        ));
                    } else {
                        let mut annotations = serde_json::Map::new();
                        annotations.insert(
                            PENDING_RERUN_ANNOTATION.to_string(),
                            serde_json::Value::String((recoveries + 1).to_string()),
                        );
                        apps.patch(
                            &name,
                            &PatchParams::default(),
                            &Patch::Merge(json!({ "metadata": { "annotations": annotations } })),
                        )
                        .await?;
                        match spark_api.delete(&name, &DeleteParams::default()).await {
                            Ok(_) => {}
                            Err(e) if is_not_found(&e) => {}
                            Err(e) => return Err(e),
                        }
                        tracing::warn!(
                            stream = %name, %namespace, recoveries = recoveries + 1,
                            "resubmitting a Spark application wedged in PENDING_RERUN"
                        );
                    }
                }
            } else if recoveries > 0 {
                apps.patch(
                    &name,
                    &PatchParams::default(),
                    &Patch::Merge(json!({
                        "metadata": { "annotations": { PENDING_RERUN_ANNOTATION: "0" } }
                    })),
                )
                .await?;
            }

            // A driver can be RUNNING while its executor cannot be scheduled. Spark keeps
            // the application healthy, so without this the stream reads as Running while
            // it processes nothing. Nova shows the Ready reason as a badge and this
            // sentence in its Last error column.
            if phase == "Running" && reason == "Running" {
                let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &namespace);
                let selector = format!("sparkoperator.k8s.io/app-name={name},spark-role=executor");
                let executors = pods.list(&ListParams::default().labels(&selector)).await?;
                if let Some(sentence) = executors.items.iter().find_map(unschedulable_message) {
                    reason = "ExecutorsUnschedulable";
                    if last_error.is_none() {
                        let mut text = sentence;
                        if text.len() > 400 {
                            text.truncate(400);
                        }
                        last_error = Some(text);
                    }
                }
            }

            (
                phase.to_string(),
                reason.to_string(),
                last_error,
                Some(execution_attempts),
                last_submission,
            )
        }
        Err(e) if is_not_found(&e) => (
            "Pending".to_string(),
            "Pending".to_string(),
            None,
            None,
            None,
        ),
        Err(e) => return Err(e),
    };

    patch_stream_status(
        &apps,
        &name,
        generation,
        previous,
        &phase,
        &reason,
        last_error,
        attempts,
        last_submitted_at,
    )
    .await?;
    tracing::info!(stream = %name, %namespace, %phase, %reason, "reconciled");
    Ok(Action::requeue(Duration::from_secs(60)))
}

pub(crate) fn error_policy(obj: Arc<StreamApp>, err: &kube::Error, ctx: Arc<Context>) -> Action {
    crate::controller::error_policy(obj, err, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        SinkMode, StreamAppSpec, StreamEngine, StreamFormat, StreamRestartPolicy, StreamSink,
        StreamSource,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::Client;
    use std::sync::{Arc, Mutex};

    fn owner() -> OwnerReference {
        OwnerReference {
            api_version: "stardelt.io/v1alpha1".into(),
            kind: "StreamApp".into(),
            name: "orders".into(),
            uid: "uid-1".into(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    fn spec(mode: SinkMode, merge_keys: Vec<&str>) -> StreamAppSpec {
        StreamAppSpec {
            source: StreamSource {
                connection_ref: Some("kafka-bus".into()),
                topic: "orders.v1".into(),
                format: StreamFormat::Json,
            },
            sink: StreamSink {
                catalog: "shared".into(),
                table: "sales.orders".into(),
                mode,
                merge_keys: merge_keys.into_iter().map(String::from).collect(),
                checkpoint_prefix: None,
            },
            engine: StreamEngine::Spark,
            owner_group: Some("analysts".into()),
            run_as: Some("pipeline-analysts".into()),
            restart_policy: StreamRestartPolicy::Always,
            stopped: false,
        }
    }

    fn arg(args: &serde_json::Value, flag: &str) -> String {
        let arr = args.as_array().expect("arguments array");
        let i = arr
            .iter()
            .position(|v| v.as_str() == Some(flag))
            .unwrap_or_else(|| panic!("missing {flag}"));
        arr[i + 1].as_str().unwrap().to_string()
    }

    #[test]
    fn the_job_config_map_carries_the_script() {
        let cm = job_config_map("data", "orders", &owner());
        assert_eq!(cm.metadata.name.as_deref(), Some("orders-job"));
        let data = cm.data.expect("data");
        let script = data.get("stream.py").expect("stream.py");
        assert!(script.starts_with("\"\"\"A StreamApp:"), "{script}");
        assert!(script.contains("from_json"));
        // Both sink modes must wait for the query: `append` used to return from main()
        // after `toTable()`, so the driver exited after the first batch (this ticket).
        assert_eq!(script.matches("awaitTermination").count(), 2, "{script}");
    }

    #[test]
    fn the_application_is_cluster_mode_and_mounts_the_job() {
        let s = spec(SinkMode::Merge, vec!["id"]);
        let app = spark_application("data", "orders", &s, "kafka:9092", "s3a://c/p", &owner())
            .expect("ok");
        assert_eq!(app.types.as_ref().unwrap().kind, "SparkApplication");
        assert_eq!(app.data["spec"]["mode"], "cluster");
        assert_eq!(
            app.data["spec"]["mainApplicationFile"],
            "local:///opt/job/stream.py"
        );
        let driver_mounts = &app.data["spec"]["driver"]["volumeMounts"];
        assert_eq!(driver_mounts[0]["name"], "job");
        assert_eq!(driver_mounts[0]["mountPath"], "/opt/job");
        let executor_mounts = &app.data["spec"]["executor"]["volumeMounts"];
        assert_eq!(executor_mounts[0]["name"], "job");
        assert_eq!(executor_mounts[0]["mountPath"], "/opt/job");
        assert_eq!(
            app.data["spec"]["volumes"][0]["configMap"]["name"],
            "orders-job"
        );
    }

    #[test]
    fn merge_sink_passes_the_keys_and_append_sink_does_not() {
        let merge = spec(SinkMode::Merge, vec!["a", "b"]);
        let m = spark_application("data", "orders", &merge, "k:9092", "s3a://c/p", &owner())
            .expect("ok");
        assert_eq!(arg(&m.data["spec"]["arguments"], "--merge-keys"), "a,b");

        let append = spec(SinkMode::Append, vec![]);
        let a = spark_application("data", "orders", &append, "k:9092", "s3a://c/p", &owner())
            .expect("ok");
        assert_eq!(arg(&a.data["spec"]["arguments"], "--merge-keys"), "");
    }

    #[test]
    fn restart_policy_maps_to_the_application() {
        let mut always = spec(SinkMode::Merge, vec!["id"]);
        always.restart_policy = StreamRestartPolicy::Always;
        let a = spark_application("data", "orders", &always, "k:9092", "s3a://c/p", &owner())
            .expect("ok");
        assert_eq!(a.data["spec"]["restartPolicy"]["type"], "Always");

        let mut failure = spec(SinkMode::Merge, vec!["id"]);
        failure.restart_policy = StreamRestartPolicy::OnFailure;
        let f = spark_application("data", "orders", &failure, "k:9092", "s3a://c/p", &owner())
            .expect("ok");
        assert_eq!(f.data["spec"]["restartPolicy"]["type"], "OnFailure");
    }

    #[test]
    fn internal_bootstrap_points_at_the_platform_bus() {
        assert_eq!(
            internal_bootstrap("stardelt"),
            "kafka-kafka-bootstrap.stardelt.svc.cluster.local:9092"
        );
    }

    #[test]
    fn kafka_topic_is_a_strimzi_topic_for_the_cluster() {
        let topic = kafka_topic("measurements", "pegelonline.measurements", &owner());
        assert_eq!(
            topic.types.as_ref().unwrap().api_version,
            "kafka.strimzi.io/v1"
        );
        assert_eq!(topic.types.as_ref().unwrap().kind, "KafkaTopic");
        assert_eq!(
            topic.metadata.name.as_deref(),
            Some("pegelonline.measurements")
        );
        assert_eq!(topic.data["spec"]["topicName"], "pegelonline.measurements");
        assert_eq!(
            topic
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("strimzi.io/cluster")),
            Some(&"kafka".to_string())
        );
        assert_eq!(topic.data["spec"]["replicas"], 1);
    }

    #[test]
    fn an_illegal_topic_name_moves_to_spec_topic_name() {
        let topic = kafka_topic("orders", "orders_v1", &owner());
        assert_eq!(topic.metadata.name.as_deref(), Some("stream-orders"));
        assert_eq!(topic.data["spec"]["topicName"], "orders_v1");
    }

    #[test]
    fn an_existing_owner_of_the_topic_is_found() {
        // Ticket #325 specified "orders.v1" here, but '.' is legal per the ticket's
        // own topic_resource_name so the resource would be named "orders.v1" (not
        // "stream-producer"). Use an illegal topic name (underscore) to exercise the
        // case where the KafkaTopic resource name differs from spec.topicName.
        let other = kafka_topic("producer", "orders_v1", &owner());
        assert_eq!(
            topic_owner(&[other], "orders_v1").as_deref(),
            Some("stream-producer")
        );
        assert_eq!(topic_owner(&[], "orders_v1"), None);
    }

    #[test]
    fn an_identical_kafka_topic_satisfies_the_requirement() {
        // What the operator would apply, and what the demo applied by hand: same topic,
        // same spec, a different object name.
        let desired = kafka_topic("pegelonline-continuous", "pegelonline.continuous", &owner());
        let mut same = kafka_topic("pegelonline-continuous", "pegelonline.continuous", &owner());
        same.metadata.name = Some("pegelonline-continuous".into());
        same.metadata.owner_references = None;
        assert!(topic_satisfied(
            &[same.clone()],
            "pegelonline.continuous",
            &desired
        ));

        // A different retention is a real conflict: the warning must stay.
        let mut different = same;
        different.data["spec"]["config"]["retention.ms"] = serde_json::json!("86400000");
        assert!(!topic_satisfied(
            &[different],
            "pegelonline.continuous",
            &desired
        ));
        assert!(!topic_satisfied(&[], "pegelonline.continuous", &desired));
    }

    #[test]
    fn restart_policy_carries_the_v2_retry_fields() {
        let s = spec(SinkMode::Merge, vec!["id"]);
        let a =
            spark_application("data", "orders", &s, "k:9092", "s3a://c/p", &owner()).expect("ok");
        let rp = &a.data["spec"]["restartPolicy"];
        assert_eq!(rp["type"], "Always");
        assert_eq!(rp["onFailureRetries"], 3);
        assert_eq!(rp["onFailureRetryInterval"], 10);
        assert_eq!(rp["onSubmissionFailureRetries"], 3);
        assert_eq!(rp["onSubmissionFailureRetryInterval"], 10);
    }

    #[test]
    fn the_checkpoint_defaults_to_the_namespace_and_name() {
        let mut s = spec(SinkMode::Merge, vec!["id"]);
        s.sink.checkpoint_prefix = None;
        assert_eq!(
            checkpoint_for(&s, "stardelt", "orders"),
            "s3a://lakehouse/checkpoints/stardelt/orders"
        );
        s.sink.checkpoint_prefix = Some("team/orders".into());
        assert_eq!(
            checkpoint_for(&s, "stardelt", "orders"),
            "s3a://lakehouse/team/orders"
        );
    }

    #[test]
    fn a_stream_without_run_as_uses_the_owner_group() {
        let mut s = spec(SinkMode::Merge, vec!["id"]);
        s.run_as = None;
        s.owner_group = Some("analysts".into());
        let a =
            spark_application("data", "orders", &s, "k:9092", "s3a://c/p", &owner()).expect("ok");
        let env = a.data["spec"]["driver"]["env"].as_array().expect("env");
        let client_id = env
            .iter()
            .find(|e| e["name"] == "CATALOG_CLIENT_ID")
            .expect("CATALOG_CLIENT_ID");
        assert_eq!(
            client_id["valueFrom"]["secretKeyRef"]["name"],
            "sp-pipeline-analysts"
        );
        let client_secret = env
            .iter()
            .find(|e| e["name"] == "CATALOG_CLIENT_SECRET")
            .expect("CATALOG_CLIENT_SECRET");
        assert_eq!(
            client_secret["valueFrom"]["secretKeyRef"]["name"],
            "sp-pipeline-analysts"
        );

        s.owner_group = None;
        let err =
            spark_application("data", "orders", &s, "k:9092", "s3a://c/p", &owner()).unwrap_err();
        assert!(err.contains("runAs"), "{err}");
    }

    #[test]
    fn the_driver_carries_catalog_and_s3_env_with_no_env_from() {
        let s = spec(SinkMode::Merge, vec!["id"]);
        let a =
            spark_application("data", "orders", &s, "k:9092", "s3a://c/p", &owner()).expect("ok");
        let driver = &a.data["spec"]["driver"];
        assert!(
            driver.get("envFrom").is_none(),
            "driver must not use envFrom"
        );
        let env = driver["env"].as_array().expect("env array");
        let names: Vec<&str> = env.iter().map(|e| e["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            vec![
                "CATALOG_CLIENT_ID",
                "CATALOG_CLIENT_SECRET",
                "CATALOG_TOKEN_URL",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_REGION",
            ]
        );
    }

    #[test]
    fn secret_backed_driver_env_uses_secret_key_refs_never_literal_values() {
        let s = spec(SinkMode::Merge, vec!["id"]);
        let a =
            spark_application("data", "orders", &s, "k:9092", "s3a://c/p", &owner()).expect("ok");
        let env = a.data["spec"]["driver"]["env"].as_array().expect("env");
        let secret_backed = [
            ("CATALOG_CLIENT_ID", "sp-pipeline-analysts", "client-id"),
            (
                "CATALOG_CLIENT_SECRET",
                "sp-pipeline-analysts",
                "client-secret",
            ),
            (
                "AWS_ACCESS_KEY_ID",
                crate::prerequisites::S3_SECRET,
                "access-key",
            ),
            (
                "AWS_SECRET_ACCESS_KEY",
                crate::prerequisites::S3_SECRET,
                "secret-key",
            ),
            ("AWS_REGION", crate::prerequisites::S3_SECRET, "region"),
        ];
        for (name, secret, key) in secret_backed {
            let entry = env
                .iter()
                .find(|e| e["name"] == name)
                .unwrap_or_else(|| panic!("missing {name}"));
            assert!(
                entry.get("value").is_none(),
                "{name} must not be a literal value"
            );
            assert_eq!(
                entry["valueFrom"]["secretKeyRef"]["name"], secret,
                "{name} secret"
            );
            assert_eq!(entry["valueFrom"]["secretKeyRef"]["key"], key, "{name} key");
        }
        let token = env
            .iter()
            .find(|e| e["name"] == "CATALOG_TOKEN_URL")
            .expect("CATALOG_TOKEN_URL");
        assert_eq!(
            token["value"],
            "http://keycloak:8080/realms/stardelt/protocol/openid-connect/token"
        );
        assert!(token.get("valueFrom").is_none());

        // The executor needs the same catalog identity for token refresh (#341).
        // Secret-backed values are injected as secretKeyRefs (never literal); the token URL
        // is not secret-backed and is injected as a plain executor env var.
        let conf = &a.data["spec"]["sparkConf"];
        let executor_secret_backed = [
            (
                "spark.kubernetes.executor.secretKeyRef.CATALOG_CLIENT_ID",
                "sp-pipeline-analysts:client-id",
            ),
            (
                "spark.kubernetes.executor.secretKeyRef.CATALOG_CLIENT_SECRET",
                "sp-pipeline-analysts:client-secret",
            ),
        ];
        for (key, ref_value) in executor_secret_backed {
            assert!(
                conf.get(key).is_some(),
                "executor catalog identity missing: {key}"
            );
            assert_eq!(
                conf[key], ref_value,
                "{key} must reference the principal Secret, not a literal secret value"
            );
        }
        assert_eq!(
            conf["spark.executorEnv.CATALOG_TOKEN_URL"],
            "http://keycloak:8080/realms/stardelt/protocol/openid-connect/token",
            "executor needs the catalog token URL"
        );
    }

    #[test]
    fn the_application_uses_the_jars_baked_in_the_image() {
        let s = spec(SinkMode::Merge, vec!["id"]);
        let a =
            spark_application("data", "orders", &s, "k:9092", "s3a://c/p", &owner()).expect("ok");
        assert_eq!(a.data["spec"]["sparkVersion"], "4.1.3");
        // The image carries the Iceberg runtime and the Kafka connector (platform#601).
        // Resolving either through `spark.jars.packages` would download Scala 2.12 / Spark
        // 3.5 coordinates into a Scala 2.13 / Spark 4.1.3 runtime at submit time.
        assert!(
            a.data["spec"]["sparkConf"]
                .get("spark.jars.packages")
                .is_none(),
            "the StreamApp driver must not fetch jars: {:?}",
            a.data["spec"]["sparkConf"]
        );
        assert_eq!(
            a.data["spec"]["sparkConf"]["spark.sql.extensions"],
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        );
    }

    #[test]
    fn the_stream_has_room_above_the_jvm_heap() {
        let s = spec(SinkMode::Merge, vec!["id"]);
        let a =
            spark_application("data", "orders", &s, "k:9092", "s3a://c/p", &owner()).expect("ok");
        for part in ["driver", "executor"] {
            assert_eq!(a.data["spec"][part]["memory"], "2g", "{part}");
            assert_eq!(a.data["spec"][part]["memoryOverhead"], "1g", "{part}");
        }
    }

    #[test]
    fn spark_conf_carries_s3a_and_sink_catalog_s3_endpoints() {
        let s = spec(SinkMode::Merge, vec!["id"]);
        let a =
            spark_application("data", "orders", &s, "k:9092", "s3a://c/p", &owner()).expect("ok");
        let conf = &a.data["spec"]["sparkConf"];
        assert_eq!(
            conf["spark.hadoop.fs.s3a.endpoint"],
            "http://seaweedfs-s3:8333"
        );
        assert_eq!(conf["spark.hadoop.fs.s3a.path.style.access"], "true");
        assert_eq!(
            conf["spark.hadoop.fs.s3a.aws.credentials.provider"],
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        );
        // The access and secret keys come from the driver environment at runtime; a
        // literal `${env:...}` here is not expanded by Hadoop and signs with a bogus key.
        assert!(conf.get("spark.hadoop.fs.s3a.access.key").is_none());
        assert!(conf.get("spark.hadoop.fs.s3a.secret.key").is_none());
        assert_eq!(
            conf["spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID"],
            "stardelt-s3-creds:access-key"
        );
        assert_eq!(
            conf["spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY"],
            "stardelt-s3-creds:secret-key"
        );
        assert_eq!(
            conf["spark.kubernetes.executor.secretKeyRef.AWS_REGION"],
            "stardelt-s3-creds:region"
        );
        assert_eq!(
            conf["spark.sql.catalog.shared.s3.endpoint"],
            "http://seaweedfs-s3:8333"
        );
        assert_eq!(
            conf["spark.sql.catalog.shared.s3.path-style-access"],
            "true"
        );
    }

    fn stream_app(stopped: bool) -> StreamApp {
        StreamApp {
            metadata: ObjectMeta {
                name: Some("orders".into()),
                namespace: Some("data".into()),
                uid: Some("uid-1".into()),
                generation: Some(1),
                ..Default::default()
            },
            spec: StreamAppSpec {
                stopped,
                ..spec(SinkMode::Merge, vec!["id"])
            },
            status: None,
        }
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

    #[tokio::test]
    async fn stopped_stream_deletes_the_application_and_reports_stopped() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "DELETE" && path.contains("/sparkapplications/") {
                return (
                    404,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"StreamApp","metadata":{"name":"orders","namespace":"data"},"spec":{"source":{"connectionRef":"kafka-bus","topic":"orders.v1"},"sink":{"catalog":"shared","table":"sales.orders","mode":"merge","mergeKeys":["id"]}},"status":{"phase":"Stopped"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"v1","kind":"Status","status":"Success"}"#.into(),
            )
        });
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let action = reconcile_stream_app(Arc::new(stream_app(true)), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(300)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("PATCH"), "{log}");
        assert!(log.contains("\"phase\":\"Stopped\""), "{log}");
    }

    #[tokio::test]
    async fn a_terminal_spark_state_becomes_failed_with_its_message() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/connections/kafka-bus") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"Connection","metadata":{"name":"kafka-bus","namespace":"data"},"spec":{"scope":"group","ownerGroup":"a","type":"kafka","config":{"bootstrapServers":"kafka:9092"},"secret":{"name":"s","keys":["token"]}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkapplications/orders") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1beta2","kind":"SparkApplication","metadata":{"name":"orders","namespace":"data"},"status":{"applicationState":{"state":"FAILED","errorMessage":"boom"}}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/streamapps/orders/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"StreamApp","metadata":{"name":"orders","namespace":"data"},"spec":{"source":{"connectionRef":"kafka-bus","topic":"orders.v1"},"sink":{"catalog":"shared","table":"sales.orders","mode":"merge","mergeKeys":["id"]}},"status":{"phase":"Failed","lastError":"boom"}}"#.into(),
                );
            }
            if method == "PATCH" || method == "POST" || method == "PUT" || method == "CREATE" {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"orders-job","namespace":"data"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"v1","kind":"Status","status":"Success"}"#.into(),
            )
        });
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let action = reconcile_stream_app(Arc::new(stream_app(false)), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(60)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("\"phase\":\"Failed\""), "{log}");
        assert!(
            log.contains("\"lastError\":\"boom\"") || log.contains("boom"),
            "{log}"
        );
    }

    #[tokio::test]
    async fn a_wedged_pending_rerun_application_is_resubmitted() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/connections/kafka-bus") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"Connection","metadata":{"name":"kafka-bus","namespace":"data"},"spec":{"scope":"group","ownerGroup":"a","type":"kafka","config":{"bootstrapServers":"kafka:9092"},"secret":{"name":"s","keys":["token"]}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkapplications/orders") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1beta2","kind":"SparkApplication","metadata":{"name":"orders","namespace":"data","creationTimestamp":"2020-01-01T00:00:00Z"},"status":{"applicationState":{"state":"PENDING_RERUN"}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/pods") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/streamapps/orders/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"StreamApp","metadata":{"name":"orders","namespace":"data"},"spec":{"source":{"connectionRef":"kafka-bus","topic":"orders.v1"},"sink":{"catalog":"shared","table":"sales.orders","mode":"merge","mergeKeys":["id"]}},"status":{"phase":"Pending"}}"#.into(),
                );
            }
            if (method == "PATCH" || method == "POST") && path.contains("/streamapps/") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"StreamApp","metadata":{"name":"orders","namespace":"data"},"spec":{"source":{"connectionRef":"kafka-bus","topic":"orders.v1"},"sink":{"catalog":"shared","table":"sales.orders","mode":"merge","mergeKeys":["id"]}}}"#.into(),
                );
            }
            if (method == "PATCH" || method == "POST") && path.contains("/configmaps/") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"orders-job","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" || method == "POST" {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1beta2","kind":"SparkApplication","metadata":{"name":"orders","namespace":"data"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"v1","kind":"Status","status":"Success"}"#.into(),
            )
        });
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let action = reconcile_stream_app(Arc::new(stream_app(false)), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(60)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(
            log.contains("DELETE") && log.contains("/sparkapplications/orders"),
            "{log}"
        );
        assert!(log.contains("pending-rerun-recoveries"), "{log}");
        assert!(log.contains("\"phase\":\"Pending\""), "{log}");
    }

    #[tokio::test]
    async fn a_wedged_pending_rerun_application_stops_after_three_resubmissions() {
        let mut app = stream_app(false);
        app.metadata.annotations = Some(std::collections::BTreeMap::from([(
            PENDING_RERUN_ANNOTATION.to_string(),
            "3".to_string(),
        )]));
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/connections/kafka-bus") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"Connection","metadata":{"name":"kafka-bus","namespace":"data"},"spec":{"scope":"group","ownerGroup":"a","type":"kafka","config":{"bootstrapServers":"kafka:9092"},"secret":{"name":"s","keys":["token"]}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkapplications/orders") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1beta2","kind":"SparkApplication","metadata":{"name":"orders","namespace":"data","creationTimestamp":"2020-01-01T00:00:00Z"},"status":{"applicationState":{"state":"PENDING_RERUN"}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/pods") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/streamapps/orders/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"StreamApp","metadata":{"name":"orders","namespace":"data"},"spec":{"source":{"connectionRef":"kafka-bus","topic":"orders.v1"},"sink":{"catalog":"shared","table":"sales.orders","mode":"merge","mergeKeys":["id"]}},"status":{"phase":"Failed"}}"#.into(),
                );
            }
            if (method == "PATCH" || method == "POST") && path.contains("/streamapps/") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"StreamApp","metadata":{"name":"orders","namespace":"data"},"spec":{"source":{"connectionRef":"kafka-bus","topic":"orders.v1"},"sink":{"catalog":"shared","table":"sales.orders","mode":"merge","mergeKeys":["id"]}}}"#.into(),
                );
            }
            if (method == "PATCH" || method == "POST") && path.contains("/configmaps/") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"orders-job","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" || method == "POST" {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1beta2","kind":"SparkApplication","metadata":{"name":"orders","namespace":"data"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"v1","kind":"Status","status":"Success"}"#.into(),
            )
        });
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let action = reconcile_stream_app(Arc::new(app), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(60)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(!log.contains("DELETE "), "{log}");
        assert!(log.contains("\"phase\":\"Failed\""), "{log}");
        assert!(log.contains("PENDING_RERUN"), "{log}");
    }

    #[test]
    fn pending_rerun_needs_no_pod_and_two_minutes() {
        assert!(pending_rerun_is_wedged(
            Some("PENDING_RERUN"),
            0,
            Some(0),
            121
        ));
        assert!(!pending_rerun_is_wedged(
            Some("PENDING_RERUN"),
            1,
            Some(0),
            999
        ));
        assert!(!pending_rerun_is_wedged(
            Some("PENDING_RERUN"),
            0,
            None,
            999
        ));
        assert!(!pending_rerun_is_wedged(Some("RUNNING"), 0, Some(0), 999));
    }

    #[test]
    fn a_pending_executor_reports_the_scheduler_message() {
        use k8s_openapi::api::core::v1::{Pod, PodCondition, PodStatus};
        let pod = |phase: &str, scheduled: &str, message: Option<&str>| Pod {
            status: Some(PodStatus {
                phase: Some(phase.into()),
                conditions: Some(vec![PodCondition {
                    type_: "PodScheduled".into(),
                    status: scheduled.into(),
                    message: message.map(str::to_string),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let why = Some("0/1 nodes are available: 1 Insufficient cpu.");
        assert_eq!(
            unschedulable_message(&pod("Pending", "False", why)).as_deref(),
            why
        );
        assert_eq!(
            unschedulable_message(&pod("Pending", "False", None)).as_deref(),
            Some("the executor is not schedulable")
        );
        assert!(unschedulable_message(&pod("Pending", "True", None)).is_none());
        assert!(unschedulable_message(&pod("Running", "True", None)).is_none());
        assert!(unschedulable_message(&Pod::default()).is_none());
    }

    #[tokio::test]
    async fn a_crash_looping_running_application_reports_restarting() {
        let recent = k8s_openapi::jiff::Timestamp::now().to_string();
        let (client, requests) = mock_client(move |method, path, _body| {
            if method == "GET" && path.contains("/connections/kafka-bus") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"Connection","metadata":{"name":"kafka-bus","namespace":"data"},"spec":{"scope":"group","ownerGroup":"a","type":"kafka","config":{"bootstrapServers":"kafka:9092"},"secret":{"name":"s","keys":["token"]}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkapplications/orders") {
                return (
                    200,
                    format!(
                        r#"{{"apiVersion":"sparkoperator.k8s.io/v1beta2","kind":"SparkApplication","metadata":{{"name":"orders","namespace":"data"}},"status":{{"applicationState":{{"state":"RUNNING","errorMessage":"boom"}},"executionAttempts":4,"lastSubmissionAttemptTime":"{recent}"}}}}"#
                    ),
                );
            }
            if method == "PATCH" && path.contains("/streamapps/orders/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"StreamApp","metadata":{"name":"orders","namespace":"data"},"spec":{"source":{"connectionRef":"kafka-bus","topic":"orders.v1"},"sink":{"catalog":"shared","table":"sales.orders","mode":"merge","mergeKeys":["id"]}},"status":{"phase":"Running","lastError":"boom"}}"#.into(),
                );
            }
            if method == "PATCH" || method == "POST" || method == "PUT" || method == "CREATE" {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"orders-job","namespace":"data"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"v1","kind":"Status","status":"Success"}"#.into(),
            )
        });
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let action = reconcile_stream_app(Arc::new(stream_app(false)), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(60)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("\"phase\":\"Running\""), "{log}");
        assert!(log.contains("\"reason\":\"Restarting\""), "{log}");
        assert!(log.contains("\"lastError\":\"boom\""), "{log}");
        assert!(log.contains("\"attempts\":4"), "{log}");
        assert!(log.contains("\"lastSubmittedAt\":\"20"), "{log}");
    }

    #[tokio::test]
    async fn a_stream_that_has_run_for_a_while_reports_running() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/connections/kafka-bus") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"Connection","metadata":{"name":"kafka-bus","namespace":"data"},"spec":{"scope":"group","ownerGroup":"a","type":"kafka","config":{"bootstrapServers":"kafka:9092"},"secret":{"name":"s","keys":["token"]}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkapplications/orders") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1beta2","kind":"SparkApplication","metadata":{"name":"orders","namespace":"data"},"status":{"applicationState":{"state":"RUNNING","errorMessage":"boom"},"executionAttempts":4,"lastSubmissionAttemptTime":"2020-01-01T00:00:00Z"}}"#.into(),
                );
            }
            if method == "GET" && path.ends_with("/pods") {
                // The executor lookup: no executor is pending, so the stream stays Running.
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"PodList","items":[]}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/streamapps/orders/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"StreamApp","metadata":{"name":"orders","namespace":"data"},"spec":{"source":{"connectionRef":"kafka-bus","topic":"orders.v1"},"sink":{"catalog":"shared","table":"sales.orders","mode":"merge","mergeKeys":["id"]}},"status":{"phase":"Running"}}"#.into(),
                );
            }
            if method == "PATCH" || method == "POST" || method == "PUT" || method == "CREATE" {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"orders-job","namespace":"data"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"v1","kind":"Status","status":"Success"}"#.into(),
            )
        });
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        });
        let action = reconcile_stream_app(Arc::new(stream_app(false)), ctx)
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(60)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("\"phase\":\"Running\""), "{log}");
        assert!(log.contains("\"reason\":\"Running\""), "{log}");
        assert!(!log.contains("\"reason\":\"Restarting\""), "{log}");
    }

    #[test]
    fn stream_state_reports_recovered_stream_as_running() {
        let now = k8s_openapi::jiff::Timestamp::now();
        assert_eq!(
            stream_state(Some("RUNNING"), 8, None, now),
            ("Running", "Running")
        );
        assert_eq!(
            stream_state(Some("RUNNING"), 1, Some(&now.to_string()), now),
            ("Running", "Running")
        );
        assert_eq!(
            stream_state(Some("FAILED"), 2, None, now),
            ("Failed", "Failed")
        );
        assert_eq!(
            stream_state(Some("COMPLETED"), 1, None, now),
            ("Stopped", "Stopped")
        );
    }

    #[test]
    fn an_unschedulable_executor_is_not_ready() {
        let now = "2026-11-08T00:00:00Z";
        let running = ready_condition("Running", "Running", Some(3), None, now);
        assert_eq!(running.status, "True");
        assert_eq!(running.reason.as_deref(), Some("Running"));

        let stalled = ready_condition("Running", "ExecutorsUnschedulable", Some(3), None, now);
        assert_eq!(stalled.status, "False");
        assert_eq!(stalled.reason.as_deref(), Some("ExecutorsUnschedulable"));

        let pending = ready_condition("Pending", "Pending", Some(3), None, now);
        assert_eq!(pending.status, "False");
    }
}
