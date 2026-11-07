//! A `StreamApp` (D42): the SparkApplication that runs it and the ConfigMap that carries the
//! job. Pure builders; the reconciler that applies them lives in `controller.rs`.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{DynamicObject, ObjectMeta, TypeMeta};
use serde_json::json;

use crate::crd::{SinkMode, StreamAppSpec, StreamRestartPolicy};
use crate::objects::FIELD_MANAGER;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        SinkMode, StreamAppSpec, StreamEngine, StreamFormat, StreamRestartPolicy, StreamSink,
        StreamSource,
    };

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
}
