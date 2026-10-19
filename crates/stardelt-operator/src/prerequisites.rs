//! The non-Helm objects a Lakehouse needs (decision D17). Pure builders: they
//! return typed objects and touch no cluster; `controller.rs` applies them with
//! [`crate::objects`].

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use k8s_openapi::{
    api::{
        batch::v1::{Job, JobSpec},
        core::v1::{
            Capabilities, ConfigMap, Container, EnvVar, EnvVarSource, PodSpec, PodTemplateSpec,
            SeccompProfile, Secret, SecretKeySelector, SecurityContext,
        },
    },
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::core::{DynamicObject, ObjectMeta, TypeMeta};
use serde_json::json;

use crate::{crd::Profile, objects::labels};

/// Secret every object-storage consumer reads (same keys as stardelt-platform).
pub const S3_SECRET: &str = "stardelt-s3-creds";
/// Secret holding the S3 identities JSON that the SeaweedFS gateway reads.
pub const S3_CONFIG_SECRET: &str = "seaweedfs-s3-config";
/// CloudNative-PG Cluster that stores the catalog.
pub const CATALOG_CLUSTER: &str = "lakekeeper-pg";
/// Default bucket when `spec.storage.bucket` is unset.
pub const DEFAULT_BUCKET: &str = "lakehouse";
/// Bootstrap script (accept terms, create the warehouse, refresh its credential).
pub const BOOTSTRAP_SCRIPT: &str = include_str!("lakekeeper_bootstrap.sh");

/// Name of the Iceberg warehouse in the catalog and of the Trino catalog in front of it.
/// `lakekeeper_bootstrap.sh` and `values.rs` (`WAREHOUSE_CATALOG`) hard-code the same word.
pub const WAREHOUSE: &str = "warehouse";

/// A generated S3 key pair.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Keys {
    pub access_key: String,
    pub secret_key: String,
}

impl std::fmt::Debug for S3Keys {
    // Never print the secret.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("S3Keys { .. }")
    }
}

fn random_hex(bytes: usize) -> Result<String, getrandom::Error> {
    let mut buf = vec![0u8; bytes];
    getrandom::fill(&mut buf)?;
    Ok(buf.iter().map(|b| format!("{b:02x}")).collect())
}

impl S3Keys {
    /// 20-character access key and 40-character secret key (hex).
    pub fn generate() -> Result<Self, getrandom::Error> {
        Ok(Self {
            access_key: random_hex(10)?,
            secret_key: random_hex(20)?,
        })
    }
}

/// In-cluster S3 endpoint of the SeaweedFS gateway the operator installs.
pub fn s3_endpoint(namespace: &str) -> String {
    format!("http://seaweedfs-s3.{namespace}.svc.cluster.local:8333")
}

fn meta(name: &str, namespace: &str, lakehouse: &str, component: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        labels: Some(labels(lakehouse, component)),
        ..Default::default()
    }
}

fn string_data(pairs: &[(&str, String)]) -> Option<BTreeMap<String, String>> {
    Some(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

/// `stardelt-s3-creds`: what Trino, Airflow, CNPG backups and the bootstrap Job read.
pub fn s3_creds_secret(lakehouse: &str, namespace: &str, bucket: &str, keys: &S3Keys) -> Secret {
    Secret {
        metadata: meta(S3_SECRET, namespace, lakehouse, "seaweedfs"),
        type_: Some("Opaque".into()),
        string_data: string_data(&[
            ("access-key", keys.access_key.clone()),
            ("secret-key", keys.secret_key.clone()),
            ("endpoint", s3_endpoint(namespace)),
            ("bucket", bucket.to_string()),
            ("region", "us-east-1".to_string()),
        ]),
        ..Default::default()
    }
}

/// `seaweedfs-s3-config`: the same key pair as the gateway's S3 identity.
pub fn seaweedfs_config_secret(lakehouse: &str, namespace: &str, keys: &S3Keys) -> Secret {
    let identities = json!({"identities": [{
        "name": "stardelt",
        "credentials": [{"accessKey": keys.access_key, "secretKey": keys.secret_key}],
        "actions": ["Admin", "Read", "Write"],
    }]});
    Secret {
        metadata: meta(S3_CONFIG_SECRET, namespace, lakehouse, "seaweedfs"),
        type_: Some("Opaque".into()),
        string_data: string_data(&[("seaweedfs_s3_config", identities.to_string())]),
        ..Default::default()
    }
}

/// The CloudNative-PG `Cluster` for the catalog (single instance, like the platform manifest).
pub fn catalog_cluster(lakehouse: &str, namespace: &str, profile: Profile) -> DynamicObject {
    let size = match profile {
        Profile::Demo => "2Gi",
        _ => "10Gi",
    };
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "postgresql.cnpg.io/v1".into(),
            kind: "Cluster".into(),
        }),
        metadata: meta(CATALOG_CLUSTER, namespace, lakehouse, "lakekeeper"),
        data: json!({"spec": {
            "instances": 1,
            "storage": {"size": size},
            "bootstrap": {"initdb": {"database": "lakekeeper", "owner": "lakekeeper"}},
            "resources": {
                "requests": {"cpu": "50m", "memory": "256Mi"},
                "limits": {"cpu": "1", "memory": "512Mi"},
            },
        }}),
    }
}

/// Where the object store of a Lakehouse is, as recorded in Secret `stardelt-s3-creds`
/// (written by the operator for `seaweedfs`, by the user for `byoS3`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Location {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
}

/// Name of the discovery ConfigMap of a Lakehouse.
pub fn discovery_config_map_name(lakehouse: &str) -> String {
    format!("{lakehouse}-discovery")
}

/// `<lakehouse>-discovery`: the connection contract of a Lakehouse. Consumers (notebooks,
/// jobs, dashboards, external tools) read these keys instead of hard-coding hostnames.
/// The URLs are fully qualified, so they resolve from any namespace. No credentials:
/// those stay in Secret `stardelt-s3-creds`.
pub fn discovery_config_map(
    lakehouse: &str,
    namespace: &str,
    s3: &S3Location,
    owner: OwnerReference,
) -> ConfigMap {
    let mut metadata = meta(
        &discovery_config_map_name(lakehouse),
        namespace,
        lakehouse,
        "discovery",
    );
    metadata.owner_references = Some(vec![owner]);
    ConfigMap {
        metadata,
        data: string_data(&[
            ("lakehouse", lakehouse.to_string()),
            (
                "catalogUri",
                format!("http://lakekeeper.{namespace}.svc.cluster.local:8181/catalog"),
            ),
            ("warehouse", WAREHOUSE.to_string()),
            ("s3Endpoint", s3.endpoint.clone()),
            ("s3Bucket", s3.bucket.clone()),
            ("s3Region", s3.region.clone()),
            (
                "trinoUrl",
                format!("http://trino.{namespace}.svc.cluster.local:8080"),
            ),
            ("trinoCatalog", WAREHOUSE.to_string()),
        ]),
        ..Default::default()
    }
}

/// Name of the bootstrap Job. It changes when the access key, bucket or
/// endpoint change, so a rotated key gets a fresh Job (Job specs are immutable).
pub fn bootstrap_job_name(keys: &S3Keys, bucket: &str, endpoint: &str) -> String {
    let mut hasher = DefaultHasher::new();
    (&keys.access_key, bucket, endpoint).hash(&mut hasher);
    format!("lakekeeper-bootstrap-{:08x}", hasher.finish() as u32)
}

fn secret_env(name: &str, key: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: S3_SECRET.into(),
                key: key.into(),
                optional: None,
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The Job that bootstraps Lakekeeper and creates or refreshes the `warehouse`.
pub fn bootstrap_job(lakehouse: &str, namespace: &str, name: &str, owner: OwnerReference) -> Job {
    let mut metadata = meta(name, namespace, lakehouse, "lakekeeper-bootstrap");
    metadata.owner_references = Some(vec![owner]);
    Job {
        metadata,
        spec: Some(JobSpec {
            ttl_seconds_after_finished: Some(600),
            backoff_limit: Some(4),
            template: PodTemplateSpec {
                spec: Some(PodSpec {
                    restart_policy: Some("OnFailure".into()),
                    automount_service_account_token: Some(false),
                    containers: vec![Container {
                        name: "bootstrap".into(),
                        image: Some("curlimages/curl:8.10.1".into()),
                        command: Some(vec!["/bin/sh".into(), "-c".into()]),
                        args: Some(vec![BOOTSTRAP_SCRIPT.to_string()]),
                        env: Some(vec![
                            EnvVar {
                                name: "LK".into(),
                                value: Some("http://lakekeeper:8181".into()),
                                ..Default::default()
                            },
                            secret_env("S3_ENDPOINT", "endpoint"),
                            secret_env("S3_BUCKET", "bucket"),
                            secret_env("S3_REGION", "region"),
                            secret_env("S3_ACCESS_KEY", "access-key"),
                            secret_env("S3_SECRET_KEY", "secret-key"),
                        ]),
                        security_context: Some(SecurityContext {
                            allow_privilege_escalation: Some(false),
                            capabilities: Some(Capabilities {
                                drop: Some(vec!["ALL".into()]),
                                add: None,
                            }),
                            seccomp_profile: Some(SeccompProfile {
                                type_: "RuntimeDefault".into(),
                                localhost_profile: None,
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> S3Keys {
        S3Keys {
            access_key: "AK".into(),
            secret_key: "SK".into(),
        }
    }

    #[test]
    fn generated_keys_have_the_documented_length_and_differ() {
        let a = S3Keys::generate().unwrap();
        let b = S3Keys::generate().unwrap();
        assert_eq!((a.access_key.len(), a.secret_key.len()), (20, 40));
        assert_ne!(a, b);
        assert_eq!(format!("{a:?}"), "S3Keys { .. }");
    }

    #[test]
    fn creds_secret_carries_the_five_consumer_keys() {
        let s = s3_creds_secret("main", "data", "lakehouse", &keys());
        let d = s.string_data.unwrap();
        for k in ["access-key", "secret-key", "endpoint", "bucket", "region"] {
            assert!(d.contains_key(k), "missing {k}");
        }
        assert_eq!(
            d["endpoint"],
            "http://seaweedfs-s3.data.svc.cluster.local:8333"
        );
        assert_eq!(s.metadata.name.as_deref(), Some("stardelt-s3-creds"));
    }

    #[test]
    fn gateway_config_has_the_same_key_pair() {
        let s = seaweedfs_config_secret("main", "data", &keys());
        let json: serde_json::Value =
            serde_json::from_str(&s.string_data.unwrap()["seaweedfs_s3_config"]).unwrap();
        let cred = &json["identities"][0]["credentials"][0];
        assert_eq!(
            (cred["accessKey"].as_str(), cred["secretKey"].as_str()),
            (Some("AK"), Some("SK"))
        );
    }

    #[test]
    fn catalog_cluster_is_sized_by_profile() {
        let demo = catalog_cluster("main", "data", Profile::Demo);
        let full = catalog_cluster("main", "data", Profile::Full);
        assert_eq!(demo.data["spec"]["storage"]["size"], "2Gi");
        assert_eq!(full.data["spec"]["storage"]["size"], "10Gi");
        assert_eq!(
            demo.data["spec"]["bootstrap"]["initdb"]["database"],
            "lakekeeper"
        );
        assert_eq!(demo.metadata.name.as_deref(), Some("lakekeeper-pg"));
    }

    #[test]
    fn job_name_changes_with_the_access_key() {
        let other = S3Keys {
            access_key: "AK2".into(),
            secret_key: "SK".into(),
        };
        let a = bootstrap_job_name(&keys(), "lakehouse", "http://s3");
        assert_eq!(a, bootstrap_job_name(&keys(), "lakehouse", "http://s3"));
        assert_ne!(a, bootstrap_job_name(&other, "lakehouse", "http://s3"));
        assert!(a.starts_with("lakekeeper-bootstrap-"));
    }

    #[test]
    fn bootstrap_job_reads_the_s3_secret_and_refreshes_the_credential() {
        let owner = OwnerReference {
            name: "main".into(),
            ..Default::default()
        };
        let job = bootstrap_job("main", "data", "lakekeeper-bootstrap-1", owner);
        let c = &job.spec.unwrap().template.spec.unwrap().containers[0];
        let envs: Vec<_> = c
            .env
            .as_ref()
            .unwrap()
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        assert_eq!(
            envs,
            [
                "LK",
                "S3_ENDPOINT",
                "S3_BUCKET",
                "S3_REGION",
                "S3_ACCESS_KEY",
                "S3_SECRET_KEY"
            ]
        );
        assert!(c.args.as_ref().unwrap()[0].contains("storage-credential"));
        assert_eq!(job.metadata.owner_references.unwrap()[0].name, "main");
    }

    fn location() -> S3Location {
        S3Location {
            endpoint: "http://seaweedfs-s3.data.svc.cluster.local:8333".into(),
            bucket: "lakehouse".into(),
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn discovery_config_map_publishes_the_connection_keys() {
        let owner = OwnerReference {
            name: "main".into(),
            ..Default::default()
        };
        let cm = discovery_config_map("main", "data", &location(), owner);
        assert_eq!(cm.metadata.name.as_deref(), Some("main-discovery"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("data"));
        let labels = cm.metadata.labels.as_ref().unwrap();
        assert_eq!(labels["stardelt.io/lakehouse"], "main");
        assert_eq!(labels["stardelt.io/component"], "discovery");
        assert_eq!(cm.metadata.owner_references.unwrap()[0].name, "main");
        let data = cm.data.unwrap();
        let expected = [
            ("lakehouse", "main"),
            (
                "catalogUri",
                "http://lakekeeper.data.svc.cluster.local:8181/catalog",
            ),
            ("warehouse", "warehouse"),
            (
                "s3Endpoint",
                "http://seaweedfs-s3.data.svc.cluster.local:8333",
            ),
            ("s3Bucket", "lakehouse"),
            ("s3Region", "us-east-1"),
            ("trinoUrl", "http://trino.data.svc.cluster.local:8080"),
            ("trinoCatalog", "warehouse"),
        ];
        assert_eq!(data.len(), expected.len(), "unexpected keys: {data:?}");
        for (key, value) in expected {
            assert_eq!(data[key], value, "key {key}");
        }
    }

    #[test]
    fn discovery_config_map_holds_no_credential() {
        let cm = discovery_config_map("main", "data", &location(), OwnerReference::default());
        for key in cm.data.unwrap().keys() {
            let key = key.to_lowercase();
            assert!(
                !key.contains("secret") && !key.contains("accesskey"),
                "{key}"
            );
        }
    }
}
