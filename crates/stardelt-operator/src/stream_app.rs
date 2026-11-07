//! A `StreamApp` (D42): the SparkApplication that runs it and the ConfigMap that carries the
//! job. Pure builders plus the reconciler that applies them.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{DeleteParams, DynamicObject, ObjectMeta, Patch, PatchParams, TypeMeta};
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};
use serde_json::json;

use crate::controller::Context;
use crate::crd::{Condition, Connection, SinkMode, StreamApp, StreamAppSpec, StreamAppStatus, StreamRestartPolicy};
use crate::objects::{self, FIELD_MANAGER};
use crate::spark;

pub const JOB_FILE: &str = "stream.py";
pub const JOB_MOUNT: &str = "/opt/job";

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

pub fn spark_application(
    namespace: &str,
    app: &str,
    spec: &StreamAppSpec,
    bootstrap: &str,
    checkpoint: &str,
    owner: &OwnerReference,
) -> Result<DynamicObject, String> {
    let run_as = resolve_run_as(spec)?;
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
                "sparkVersion": "3.5.8",
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
                "restartPolicy": {"type": restart},
                "sparkConf": {
                    "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                },
                "driver": {
                    "cores": 1,
                    "coreLimit": "1200m",
                    "memory": "1g",
                    "serviceAccount": "spark-connect",
                    "envFrom": [{"secretRef": {"name": format!("sp-{run_as}")}}],
                    "volumeMounts": [{"name": "job", "mountPath": JOB_MOUNT}],
                },
                "executor": {
                    "cores": 1,
                    "instances": 1,
                    "memory": "1g",
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

fn ready_condition(
    phase: &str,
    generation: Option<i64>,
    previous: Option<&Condition>,
    now: &str,
) -> Condition {
    let status = if phase == "Running" { "True" } else { "False" };
    let last_transition_time = match previous {
        Some(p) if p.status == status => p.last_transition_time.clone(),
        _ => Some(now.to_string()),
    };
    Condition {
        type_: "Ready".into(),
        status: status.into(),
        reason: Some(phase.into()),
        message: None,
        last_transition_time,
        observed_generation: generation,
    }
}

async fn patch_stream_status(
    apps: &Api<StreamApp>,
    name: &str,
    generation: Option<i64>,
    previous: Option<&Condition>,
    phase: &str,
    last_error: Option<String>,
) -> Result<(), kube::Error> {
    let now = now_rfc3339();
    let status = StreamAppStatus {
        phase: phase.into(),
        last_checkpoint_at: None,
        last_error,
        conditions: vec![ready_condition(phase, generation, previous, &now)],
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
        patch_stream_status(&apps, &name, generation, previous, "Stopped", None).await?;
        tracing::info!(stream = %name, %namespace, "stream stopped");
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    let conn_name = &app.spec.source.connection_ref;
    let conn = Api::<Connection>::namespaced(ctx.client.clone(), &namespace)
        .get(conn_name)
        .await;
    let bootstrap = match conn {
        Ok(c) => c.spec.config.get("bootstrapServers").cloned(),
        Err(e) if is_not_found(&e) => None,
        Err(e) => return Err(e),
    };
    let Some(bootstrap) = bootstrap else {
        let msg = format!("source Connection {conn_name} has no config.bootstrapServers");
        patch_stream_status(&apps, &name, generation, previous, "Failed", Some(msg)).await?;
        return Ok(Action::requeue(Duration::from_secs(60)));
    };

    let owner = owner_reference(&app);
    let checkpoint = checkpoint_for(&app.spec, &namespace, &name);
    let cm = job_config_map(&namespace, &name, &owner);
    objects::apply(&ctx.client, &namespace, &cm, false).await?;
    let spark = spark_application(&namespace, &name, &app.spec, &bootstrap, &checkpoint, &owner)
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
    let (phase, last_error) = match spark_api.get(&name).await {
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
            match state {
                Some("RUNNING") | Some("SUBMITTED") => ("Running".to_string(), None),
                Some("COMPLETED") => ("Stopped".to_string(), None),
                Some("FAILED") | Some("SUBMISSION_FAILED") => ("Failed".to_string(), err_msg),
                _ => ("Pending".to_string(), None),
            }
        }
        Err(e) if is_not_found(&e) => ("Pending".to_string(), None),
        Err(e) => return Err(e),
    };

    patch_stream_status(&apps, &name, generation, previous, &phase, last_error).await?;
    tracing::info!(stream = %name, %namespace, %phase, "reconciled");
    Ok(Action::requeue(Duration::from_secs(60)))
}

pub(crate) fn error_policy(
    obj: Arc<StreamApp>,
    err: &kube::Error,
    ctx: Arc<Context>,
) -> Action {
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
                connection_ref: "kafka-bus".into(),
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
        assert_eq!(
            a.data["spec"]["driver"]["envFrom"][0]["secretRef"]["name"],
            "sp-pipeline-analysts"
        );

        s.owner_group = None;
        let err =
            spark_application("data", "orders", &s, "k:9092", "s3a://c/p", &owner()).unwrap_err();
        assert!(err.contains("runAs"), "{err}");
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
            spec: StreamAppSpec { stopped, ..spec(SinkMode::Merge, vec!["id"]) },
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
        assert!(log.contains("\"lastError\":\"boom\"") || log.contains("boom"), "{log}");
    }
}
