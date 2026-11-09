//! A `SparkCluster` (internal#161): the Spark Operator's `SparkConnect` server that runs it.
//!
//! The operator only translates. When `spec.state` is `Running` it applies one `SparkConnect`;
//! when it is `Stopped` it deletes that object again. The pods belong to the Spark Operator.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use kube::api::{Api, DynamicObject, ObjectMeta, Patch, PatchParams, TypeMeta};
use kube::runtime::controller::Action;
use serde::Deserialize;
use serde_json::json;

use crate::controller::Context;
use crate::crd::{SparkCluster, SparkClusterSize, SparkClusterSpec};
use crate::objects::{self, FIELD_MANAGER};

/// The ConfigMap that carries the runtimes the platform ships (slice 4 of internal#161).
pub const RUNTIME_CATALOGUE: &str = "stardelt-spark-runtimes";

/// One runtime entry of the catalogue: the image, the Spark version and the service account.
#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeEntry {
    pub image: String,
    #[serde(rename = "sparkVersion")]
    pub spark_version: String,
    #[serde(rename = "serviceAccount")]
    pub service_account: String,
    #[serde(default, rename = "sparkConf")]
    pub spark_conf: BTreeMap<String, String>,
}

/// Read one runtime from the catalogue ConfigMap. A ConfigMap without data, or without the
/// requested key, is an error the controller reports as `Failed`.
pub fn runtime_entry(cm: &ConfigMap, runtime: &str) -> Result<RuntimeEntry, String> {
    let Some(data) = cm.data.as_ref() else {
        return Err(format!(
            "the runtime catalogue {RUNTIME_CATALOGUE} is missing"
        ));
    };
    let Some(value) = data.get(runtime) else {
        let known = data.keys().cloned().collect::<Vec<_>>().join(", ");
        return Err(format!("unknown runtime {runtime}; known: {known}"));
    };
    serde_json::from_str(value).map_err(|e| format!("runtime {runtime} is not valid JSON: {e}"))
}

/// The numbers of a size class: driver cores and memory, executor cores and memory, and the
/// maximum number of executors.
pub fn preset(size: SparkClusterSize) -> (i32, &'static str, i32, &'static str, i32) {
    match size {
        SparkClusterSize::Small => (1, "2Gi", 1, "2Gi", 2),
        SparkClusterSize::Medium => (2, "4Gi", 2, "4Gi", 4),
        SparkClusterSize::Large => (4, "8Gi", 4, "8Gi", 8),
    }
}

/// The Spark operator's webhook wants Spark's own unit (`2g`, `512m`), not a Kubernetes
/// quantity (`2Gi`, `512Mi`). The suffixes mean the same binary multiples.
fn webhook_memory(quantity: &str) -> Result<String, String> {
    if let Some(number) = quantity.strip_suffix("Gi") {
        Ok(format!("{number}g"))
    } else if let Some(number) = quantity.strip_suffix("Mi") {
        Ok(format!("{number}m"))
    } else {
        Err(format!(
            "unsupported memory unit in `{quantity}`; use Mi or Gi"
        ))
    }
}

fn labels(name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            FIELD_MANAGER.to_string(),
        ),
        ("stardelt.io/spark-cluster".to_string(), name.to_string()),
    ])
}

/// The `SparkConnect` a `SparkCluster` becomes. Pure: no client, no cluster state.
pub fn spark_connect(
    name: &str,
    namespace: &str,
    spec: &SparkClusterSpec,
    runtime: &RuntimeEntry,
    owner: &OwnerReference,
) -> Result<DynamicObject, String> {
    let (driver_cores, driver_mem, exec_cores, exec_mem, max_executors) = preset(spec.size);
    let driver_cores = spec
        .driver
        .as_ref()
        .and_then(|d| d.cores)
        .unwrap_or(driver_cores);
    let driver_mem = spec
        .driver
        .as_ref()
        .and_then(|d| d.memory.clone())
        .unwrap_or_else(|| driver_mem.to_string());
    let exec_cores = spec
        .executor
        .as_ref()
        .and_then(|e| e.cores)
        .unwrap_or(exec_cores);
    let exec_mem = spec
        .executor
        .as_ref()
        .and_then(|e| e.memory.clone())
        .unwrap_or_else(|| exec_mem.to_string());
    let driver_mem = webhook_memory(&driver_mem)?;
    let exec_mem = webhook_memory(&exec_mem)?;
    let max_executors = spec.max_executors.unwrap_or(max_executors);

    let mut env = Vec::new();
    for var in &spec.env {
        if var.connection_ref.is_some() {
            return Err("connectionRef is not supported yet (internal#161 slice 7)".to_string());
        }
        if let Some(value) = &var.value {
            env.push(json!({ "name": var.name, "value": value }));
        }
    }

    let mut spark_conf = runtime.spark_conf.clone();
    for (key, value) in &spec.spark_conf {
        spark_conf.insert(key.clone(), value.clone());
    }
    spark_conf.insert(
        "spark.kubernetes.namespace".to_string(),
        namespace.to_string(),
    );
    spark_conf.insert(
        "spark.kubernetes.authenticate.driver.serviceAccountName".to_string(),
        runtime.service_account.clone(),
    );

    let image = spec.image.clone().unwrap_or_else(|| runtime.image.clone());

    Ok(DynamicObject {
        types: Some(TypeMeta {
            api_version: "sparkoperator.k8s.io/v1alpha1".into(),
            kind: "SparkConnect".into(),
        }),
        metadata: ObjectMeta {
            name: Some(format!("spc-{name}")),
            namespace: Some(namespace.to_string()),
            labels: Some(labels(name)),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        data: json!({
            "spec": {
                "sparkVersion": runtime.spark_version,
                "image": image,
                "sparkConf": spark_conf,
                "dynamicAllocation": {
                    "enabled": true,
                    "minExecutors": 0,
                    "maxExecutors": max_executors,
                },
                "server": {
                    "cores": driver_cores,
                    "memory": driver_mem,
                    "template": {
                        "spec": {
                            "serviceAccountName": runtime.service_account,
                            "containers": [{
                                "name": "spark-kubernetes-driver",
                                "env": env,
                            }],
                        }
                    }
                },
                "executor": {
                    "cores": exec_cores,
                    "memory": exec_mem,
                    "instances": 0,
                    "template": {
                        "spec": {
                            "containers": [{
                                "name": "spark-kubernetes-executor",
                                "env": env,
                            }],
                        }
                    }
                },
            }
        }),
    })
}

fn owner_reference(cluster: &SparkCluster) -> OwnerReference {
    OwnerReference {
        api_version: "stardelt.io/v1alpha1".into(),
        kind: "SparkCluster".into(),
        name: cluster.name_any(),
        uid: cluster.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

fn is_not_found(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(r) if r.code == 404)
}

/// The `status` half of the merge patch. Absent values are written as JSON `null` so a merge
/// patch clears them instead of leaving the previous value (see operator#362).
fn status_value(
    phase: &str,
    endpoint: Option<&str>,
    started_at: Option<&str>,
    message: Option<&str>,
    generation: Option<i64>,
) -> serde_json::Value {
    json!({
        "phase": phase,
        "endpoint": endpoint,
        "startedAt": started_at,
        "message": message,
        "observedGeneration": generation,
    })
}

async fn patch_status(
    clusters: &Api<SparkCluster>,
    name: &str,
    status: serde_json::Value,
) -> Result<(), kube::Error> {
    clusters
        .patch_status(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "status": status })),
        )
        .await?;
    Ok(())
}

/// The phase of a live `SparkConnect`, from its `status.state`.
fn phase_from_state(state: Option<&str>) -> &'static str {
    match state.map(str::to_ascii_lowercase).as_deref() {
        Some("running") | Some("ready") => "Running",
        Some("failed") => "Failed",
        _ => "Starting",
    }
}

fn endpoint_for(server_name: Option<&str>, name: &str, namespace: &str) -> String {
    let service = server_name
        .map(str::to_string)
        .unwrap_or_else(|| format!("spc-{name}-server"));
    format!("sc://{service}.{namespace}.svc.cluster.local:15002")
}

pub async fn reconcile_spark_cluster(
    cluster: Arc<SparkCluster>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    if cluster.metadata.deletion_timestamp.is_some() {
        return Ok(Action::await_change());
    }
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".into());
    let spec = &cluster.spec;
    let generation = cluster.metadata.generation;
    let previous = cluster.status.as_ref();
    let clusters: Api<SparkCluster> = Api::namespaced(ctx.client.clone(), &namespace);
    let connects: Api<DynamicObject> = Api::namespaced_with(
        ctx.client.clone(),
        &namespace,
        &objects::spark_connect_resource(),
    );

    if let Err(message) = spec.validate(&name) {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, Some(&message), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, %message, "spark cluster is invalid");
        return Ok(Action::await_change());
    }

    match spec.state {
        crate::crd::SparkClusterState::Stopped => {
            match connects
                .delete(&format!("spc-{name}"), &Default::default())
                .await
            {
                Ok(_) => {
                    patch_status(
                        &clusters,
                        &name,
                        status_value("Stopping", None, None, None, generation),
                    )
                    .await?;
                    tracing::info!(cluster = %name, %namespace, "spark cluster stopping");
                    return Ok(Action::requeue(Duration::from_secs(10)));
                }
                Err(e) if is_not_found(&e) => {
                    patch_status(
                        &clusters,
                        &name,
                        status_value("Stopped", None, None, None, generation),
                    )
                    .await?;
                    tracing::info!(cluster = %name, %namespace, "spark cluster stopped");
                    return Ok(Action::await_change());
                }
                Err(e) => return Err(e),
            }
        }
        crate::crd::SparkClusterState::Running => {}
    }

    let runtime = match Api::<ConfigMap>::namespaced(ctx.client.clone(), &namespace)
        .get(RUNTIME_CATALOGUE)
        .await
    {
        Ok(cm) => runtime_entry(&cm, &spec.runtime),
        Err(e) if is_not_found(&e) => Err(format!(
            "the runtime catalogue {RUNTIME_CATALOGUE} is missing"
        )),
        Err(e) => return Err(e),
    };
    let runtime = match runtime {
        Ok(entry) => entry,
        Err(message) => {
            patch_status(
                &clusters,
                &name,
                status_value("Failed", None, None, Some(&message), generation),
            )
            .await?;
            tracing::info!(cluster = %name, %namespace, %message, "spark cluster cannot start");
            return Ok(Action::await_change());
        }
    };

    let desired = match spark_connect(
        &name,
        &namespace,
        spec,
        &runtime,
        &owner_reference(&cluster),
    ) {
        Ok(object) => object,
        Err(message) => {
            patch_status(
                &clusters,
                &name,
                status_value("Failed", None, None, Some(&message), generation),
            )
            .await?;
            tracing::info!(cluster = %name, %namespace, %message, "spark cluster cannot start");
            return Ok(Action::await_change());
        }
    };

    objects::apply_dynamic(
        &ctx.client,
        &namespace,
        &objects::spark_connect_resource(),
        &desired,
        false,
    )
    .await?;

    let (phase, server_name) = match connects.get(&format!("spc-{name}")).await {
        Ok(live) => (
            phase_from_state(live.data.pointer("/status/state").and_then(|v| v.as_str())),
            live.data
                .pointer("/status/server/serviceName")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        ),
        Err(e) if is_not_found(&e) => ("Starting", None),
        Err(e) => return Err(e),
    };

    let endpoint =
        (phase == "Running").then(|| endpoint_for(server_name.as_deref(), &name, &namespace));
    let started_at = if phase == "Running" {
        previous
            .and_then(|s| s.started_at.clone())
            .or_else(|| Some(k8s_openapi::jiff::Timestamp::now().to_string()))
    } else {
        None
    };

    patch_status(
        &clusters,
        &name,
        status_value(
            phase,
            endpoint.as_deref(),
            started_at.as_deref(),
            None,
            generation,
        ),
    )
    .await?;
    tracing::info!(cluster = %name, %namespace, %phase, "reconciled");

    let requeue = if phase == "Starting" { 30 } else { 300 };
    Ok(Action::requeue(Duration::from_secs(requeue)))
}

pub(crate) fn error_policy(
    cluster: Arc<SparkCluster>,
    err: &kube::Error,
    ctx: Arc<Context>,
) -> Action {
    crate::controller::error_policy(cluster, err, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ConnectionKeyRef, SparkClusterOwner, SparkClusterState, SparkEnvVar, SparkResources,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::Client;
    use std::sync::Mutex;

    fn owner() -> OwnerReference {
        OwnerReference {
            api_version: "stardelt.io/v1alpha1".into(),
            kind: "SparkCluster".into(),
            name: "trial".into(),
            uid: "uid-1".into(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    fn runtime() -> RuntimeEntry {
        RuntimeEntry {
            image: "ghcr.io/example/spark@sha256:runtime".into(),
            spark_version: "4.0.1".into(),
            service_account: "spark-runtime".into(),
            spark_conf: BTreeMap::from([
                ("spark.sql.shuffle.partitions".to_string(), "8".to_string()),
                (
                    "spark.kubernetes.namespace".to_string(),
                    "wrong".to_string(),
                ),
            ]),
        }
    }

    fn spec() -> SparkClusterSpec {
        SparkClusterSpec {
            display_name: None,
            owner: SparkClusterOwner {
                group: Some("mobility-analytics".into()),
                user: None,
            },
            size: SparkClusterSize::Small,
            driver: None,
            executor: None,
            max_executors: None,
            runtime: "spark-4.0-py3.12".into(),
            image: None,
            env: vec![],
            spark_conf: BTreeMap::new(),
            idle_stop_minutes: 60,
            state: SparkClusterState::Stopped,
        }
    }

    fn cluster(state: SparkClusterState) -> SparkCluster {
        SparkCluster {
            metadata: ObjectMeta {
                name: Some("trial".into()),
                namespace: Some("data".into()),
                uid: Some("uid-1".into()),
                generation: Some(1),
                ..Default::default()
            },
            spec: SparkClusterSpec { state, ..spec() },
            status: None,
        }
    }

    fn catalogue() -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(RUNTIME_CATALOGUE.into()),
                namespace: Some("data".into()),
                ..Default::default()
            },
            data: Some(BTreeMap::from([(
                "spark-4.0-py3.12".to_string(),
                r#"{"image":"ghcr.io/example/spark@sha256:runtime","sparkVersion":"4.0.1","serviceAccount":"spark-runtime","sparkConf":{"spark.sql.shuffle.partitions":"8"}}"#.to_string(),
            )])),
            ..Default::default()
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

    fn ctx(client: Client) -> Arc<Context> {
        Arc::new(Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        })
    }

    #[test]
    fn presets_give_the_documented_numbers() {
        assert_eq!(preset(SparkClusterSize::Small), (1, "2Gi", 1, "2Gi", 2));
        assert_eq!(preset(SparkClusterSize::Medium), (2, "4Gi", 2, "4Gi", 4));
        assert_eq!(preset(SparkClusterSize::Large), (4, "8Gi", 4, "8Gi", 8));
    }

    #[test]
    fn the_spec_overrides_the_preset_field_by_field() {
        let mut s = spec();
        s.size = SparkClusterSize::Medium;
        s.driver = Some(SparkResources {
            cores: Some(3),
            memory: None,
        });
        s.executor = Some(SparkResources {
            cores: None,
            memory: Some("6Gi".into()),
        });
        s.max_executors = Some(6);
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(object.data.pointer("/spec/server/cores"), Some(&json!(3)));
        assert_eq!(
            object.data.pointer("/spec/server/memory"),
            Some(&json!("4g"))
        );
        assert_eq!(object.data.pointer("/spec/executor/cores"), Some(&json!(2)));
        assert_eq!(
            object.data.pointer("/spec/executor/memory"),
            Some(&json!("6g"))
        );
        assert_eq!(
            object.data.pointer("/spec/dynamicAllocation/maxExecutors"),
            Some(&json!(6))
        );
    }

    #[test]
    fn a_built_connect_carries_the_spark_operators_memory_unit() {
        let object = spark_connect("trial", "data", &spec(), &runtime(), &owner()).expect("build");
        assert_eq!(
            object.data.pointer("/spec/server/memory"),
            Some(&json!("2g"))
        );
        assert_eq!(
            object.data.pointer("/spec/executor/memory"),
            Some(&json!("2g"))
        );

        let mut s = spec();
        s.driver = Some(SparkResources {
            cores: None,
            memory: Some("512Mi".into()),
        });
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(
            object.data.pointer("/spec/server/memory"),
            Some(&json!("512m"))
        );
    }

    #[test]
    fn a_pinned_spec_image_beats_the_runtime_image() {
        let mut s = spec();
        s.image = Some("ghcr.io/example/custom@sha256:abc".into());
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(
            object.data.pointer("/spec/image"),
            Some(&json!("ghcr.io/example/custom@sha256:abc"))
        );

        let object = spark_connect("trial", "data", &spec(), &runtime(), &owner()).expect("build");
        assert_eq!(
            object.data.pointer("/spec/image"),
            Some(&json!("ghcr.io/example/spark@sha256:runtime"))
        );
    }

    #[test]
    fn the_platform_owned_keys_win_over_the_spec() {
        let mut s = spec();
        s.spark_conf.insert(
            "spark.kubernetes.namespace".to_string(),
            "elsewhere".to_string(),
        );
        s.spark_conf.insert(
            "spark.kubernetes.authenticate.driver.serviceAccountName".to_string(),
            "mine".to_string(),
        );
        s.spark_conf
            .insert("spark.sql.shuffle.partitions".to_string(), "16".to_string());
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(
            object
                .data
                .pointer("/spec/sparkConf/spark.kubernetes.namespace"),
            Some(&json!("data"))
        );
        assert_eq!(
            object
                .data
                .pointer("/spec/sparkConf/spark.kubernetes.authenticate.driver.serviceAccountName"),
            Some(&json!("spark-runtime"))
        );
        assert_eq!(
            object
                .data
                .pointer("/spec/sparkConf/spark.sql.shuffle.partitions"),
            Some(&json!("16"))
        );
    }

    #[test]
    fn labels_and_the_owner_reference_name_the_cluster() {
        let object = spark_connect("trial", "data", &spec(), &runtime(), &owner()).expect("build");
        assert_eq!(object.metadata.name.as_deref(), Some("spc-trial"));
        assert_eq!(object.metadata.namespace.as_deref(), Some("data"));
        let labels = object.metadata.labels.expect("labels");
        assert_eq!(labels["app.kubernetes.io/managed-by"], "stardelt-operator");
        assert_eq!(labels["stardelt.io/spark-cluster"], "trial");
        assert_eq!(
            object.metadata.owner_references.expect("owner")[0].kind,
            "SparkCluster"
        );
    }

    #[test]
    fn env_with_a_connection_ref_is_not_supported_yet() {
        let mut s = spec();
        s.env = vec![SparkEnvVar {
            name: "TOKEN".into(),
            value: None,
            connection_ref: Some(ConnectionKeyRef {
                name: "store".into(),
                key: "token".into(),
            }),
        }];
        assert_eq!(
            spark_connect("trial", "data", &s, &runtime(), &owner()).unwrap_err(),
            "connectionRef is not supported yet (internal#161 slice 7)"
        );
    }

    #[test]
    fn a_literal_env_is_passed_to_driver_and_executors() {
        let mut s = spec();
        s.env = vec![SparkEnvVar {
            name: "MODE".into(),
            value: Some("dev".into()),
            connection_ref: None,
        }];
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(
            object
                .data
                .pointer("/spec/server/template/spec/containers/0/env"),
            Some(&json!([{ "name": "MODE", "value": "dev" }]))
        );
        assert_eq!(
            object
                .data
                .pointer("/spec/executor/template/spec/containers/0/env"),
            Some(&json!([{ "name": "MODE", "value": "dev" }]))
        );
    }

    #[test]
    fn an_unknown_runtime_names_the_known_ones() {
        let err = runtime_entry(&catalogue(), "spark-4.1-py3.13").unwrap_err();
        assert_eq!(
            err,
            "unknown runtime spark-4.1-py3.13; known: spark-4.0-py3.12"
        );
    }

    #[test]
    fn a_missing_catalogue_is_reported() {
        let mut cm = catalogue();
        cm.data = None;
        assert_eq!(
            runtime_entry(&cm, "spark-4.0-py3.12").unwrap_err(),
            "the runtime catalogue stardelt-spark-runtimes is missing"
        );
    }

    #[tokio::test]
    async fn running_without_a_live_object_applies_once_and_reports_starting() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    404,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Starting"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));
        let log = requests.lock().expect("requests").join("\n");
        let applies = log
            .lines()
            .filter(|line| line.starts_with("PATCH") && line.contains("/sparkconnects/spc-trial"))
            .count();
        assert_eq!(applies, 1, "{log}");
        assert!(log.contains("fieldManager=stardelt-operator"), "{log}");
        assert!(log.contains("\"phase\":\"Starting\""), "{log}");
        assert!(log.contains("\"endpoint\":null"), "{log}");
    }

    #[tokio::test]
    async fn a_live_running_server_reports_running_with_an_endpoint() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"},"status":{"state":"Running","server":{"serviceName":"spc-trial-server"}}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Running"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(300)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("\"phase\":\"Running\""), "{log}");
        assert!(
            log.contains("\"endpoint\":\"sc://spc-trial-server.data.svc.cluster.local:15002\""),
            "{log}"
        );
        assert!(log.contains("\"startedAt\":\"20"), "{log}");
    }

    #[tokio::test]
    async fn stopped_with_a_live_object_deletes_it_and_reports_stopping() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "DELETE" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12"},"status":{"phase":"Stopping"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Stopped)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(10)));
        let log = requests.lock().expect("requests").join("\n");
        let deletes = log
            .lines()
            .filter(|line| line.starts_with("DELETE") && line.contains("/sparkconnects/spc-trial"))
            .count();
        assert_eq!(deletes, 1, "{log}");
        assert!(log.contains("\"phase\":\"Stopping\""), "{log}");
    }

    #[tokio::test]
    async fn stopped_without_a_live_object_reports_stopped() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "DELETE" && path.contains("/sparkconnects/spc-trial") {
                return (
                    404,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12"},"status":{"phase":"Stopped"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Stopped)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("\"phase\":\"Stopped\""), "{log}");
        assert!(log.contains("\"endpoint\":null"), "{log}");
        assert!(log.contains("\"startedAt\":null"), "{log}");
    }

    #[tokio::test]
    async fn an_unknown_runtime_reports_failed_with_the_message() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{}"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-9.9-py3.99"},"status":{"phase":"Failed"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let mut c = cluster(SparkClusterState::Running);
        c.spec.runtime = "spark-9.9-py3.99".into();
        let action = reconcile_spark_cluster(Arc::new(c), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(
            log.contains("unknown runtime spark-9.9-py3.99; known: spark-4.0-py3.12"),
            "{log}"
        );
        assert!(log.contains("\"phase\":\"Failed\""), "{log}");
        assert!(
            !log.contains("/sparkconnects/"),
            "nothing is applied: {log}"
        );
    }

    #[tokio::test]
    async fn a_missing_catalogue_reports_failed() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    404,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Failed"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(
            log.contains("the runtime catalogue stardelt-spark-runtimes is missing"),
            "{log}"
        );
        assert!(log.contains("\"phase\":\"Failed\""), "{log}");
    }

    #[tokio::test]
    async fn a_connection_ref_reports_failed() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Failed"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let mut c = cluster(SparkClusterState::Running);
        c.spec.env = vec![SparkEnvVar {
            name: "TOKEN".into(),
            value: None,
            connection_ref: Some(ConnectionKeyRef {
                name: "store".into(),
                key: "token".into(),
            }),
        }];
        let action = reconcile_spark_cluster(Arc::new(c), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(
            log.contains("connectionRef is not supported yet (internal#161 slice 7)"),
            "{log}"
        );
        assert!(log.contains("\"phase\":\"Failed\""), "{log}");
    }

    #[tokio::test]
    async fn an_invalid_cluster_reports_failed_before_touching_the_catalogue() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Failed"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let mut c = cluster(SparkClusterState::Running);
        c.spec.spark_conf.insert(
            "spark.kubernetes.authenticate.driver.serviceAccountName".to_string(),
            "admin".to_string(),
        );
        let action = reconcile_spark_cluster(Arc::new(c), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("is managed by the platform"), "{log}");
        assert!(!log.contains("/configmaps/"), "{log}");
    }
}
