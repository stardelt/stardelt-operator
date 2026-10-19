//! Applies the prerequisites of a Lakehouse (decision D17) and reports each one as
//! a component status. The objects are built in `prerequisites.rs` and sent with
//! the helpers in `objects.rs`.

use k8s_openapi::{
    ByteString,
    api::{
        batch::v1::Job,
        core::v1::{ConfigMap, Secret},
    },
};
use kube::{Api, ResourceExt, core::DynamicObject};

use crate::{
    controller::Context,
    crd::{ComponentStatus, Lakehouse, StorageImpl},
    objects::{apply, apply_dynamic, cnpg_cluster_resource, owner_reference},
    prerequisites::{
        CATALOG_CLUSTER, DEFAULT_BUCKET, S3_CONFIG_SECRET, S3_SECRET, S3Keys, S3Location,
        bootstrap_job, bootstrap_job_name, catalog_cluster, discovery_config_map,
        discovery_config_map_name, s3_creds_secret, s3_endpoint, seaweedfs_config_secret,
    },
};

fn status(name: &str, ready: bool, message: impl Into<String>) -> ComponentStatus {
    ComponentStatus {
        name: name.into(),
        ready,
        message: Some(message.into()),
        applied_hash: None,
    }
}

fn text(secret: &Secret, key: &str) -> Option<String> {
    let ByteString(bytes) = secret.data.as_ref()?.get(key)?;
    String::from_utf8(bytes.clone()).ok()
}

/// The key pair of an existing `stardelt-s3-creds`, if it has both keys.
pub fn read_keys(secret: &Secret) -> Option<S3Keys> {
    Some(S3Keys {
        access_key: text(secret, "access-key")?,
        secret_key: text(secret, "secret-key")?,
    })
}

/// Where the object store is, if `stardelt-s3-creds` has `endpoint`, `bucket` and `region`.
pub fn read_location(secret: &Secret) -> Option<S3Location> {
    Some(S3Location {
        endpoint: text(secret, "endpoint")?,
        bucket: text(secret, "bucket")?,
        region: text(secret, "region")?,
    })
}

fn bucket(lh: &Lakehouse) -> String {
    lh.spec
        .storage
        .bucket
        .clone()
        .unwrap_or_else(|| DEFAULT_BUCKET.to_string())
}

/// `stardelt-s3-creds` and `seaweedfs-s3-config`: created only when absent, never
/// overwritten. With `byoS3` the operator expects the user to have created
/// `stardelt-s3-creds` (five keys) and creates nothing.
pub async fn ensure_s3_secrets(ctx: &Context, lh: &Lakehouse, namespace: &str) -> ComponentStatus {
    const NAME: &str = "s3-credentials";
    let dry_run = lh.spec.dry_run;
    let lakehouse = lh.name_any();
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let existing = match secrets.get_opt(S3_SECRET).await {
        Ok(found) => found,
        Err(e) => return status(NAME, false, e.to_string()),
    };

    if lh.spec.storage.implementation == StorageImpl::ByoS3 {
        return match existing.as_ref().and_then(read_keys) {
            Some(_) => status(
                NAME,
                !dry_run,
                "using the Secret stardelt-s3-creds you created",
            ),
            None => status(
                NAME,
                false,
                "byoS3: create Secret stardelt-s3-creds with access-key, secret-key, endpoint, bucket, region",
            ),
        };
    }

    let keys = match &existing {
        Some(secret) => match read_keys(secret) {
            Some(keys) => keys,
            None => {
                return status(
                    NAME,
                    false,
                    "Secret stardelt-s3-creds exists but lacks access-key or secret-key",
                );
            }
        },
        None if dry_run => {
            return status(
                NAME,
                false,
                "planned (dry-run): would generate the S3 key pair",
            );
        }
        None => match S3Keys::generate() {
            Ok(keys) => keys,
            Err(e) => return status(NAME, false, e.to_string()),
        },
    };
    if existing.is_none() {
        let secret = s3_creds_secret(&lakehouse, namespace, &bucket(lh), &keys);
        if let Err(e) = apply(&ctx.client, namespace, &secret, false).await {
            return status(NAME, false, e.to_string());
        }
    }
    match secrets.get_opt(S3_CONFIG_SECRET).await {
        Ok(Some(_)) => {}
        Ok(None) if dry_run => {
            return status(
                NAME,
                false,
                "planned (dry-run): would write seaweedfs-s3-config",
            );
        }
        Ok(None) => {
            let secret = seaweedfs_config_secret(&lakehouse, namespace, &keys);
            if let Err(e) = apply(&ctx.client, namespace, &secret, false).await {
                return status(NAME, false, e.to_string());
            }
        }
        Err(e) => return status(NAME, false, e.to_string()),
    }
    status(
        NAME,
        !dry_run,
        if dry_run {
            "planned (dry-run): present"
        } else {
            "present"
        },
    )
}

fn cluster_ready(cluster: &DynamicObject) -> bool {
    cluster.data["status"]["conditions"]
        .as_array()
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|c| c["type"] == "Ready" && c["status"] == "True")
        })
}

/// The CloudNative-PG `Cluster` that stores the catalog, applied with server-side apply.
pub async fn ensure_catalog_database(
    ctx: &Context,
    lh: &Lakehouse,
    namespace: &str,
) -> ComponentStatus {
    const NAME: &str = "catalog-database";
    let dry_run = lh.spec.dry_run;
    let cluster = catalog_cluster(&lh.name_any(), namespace, lh.spec.profile);
    match apply_dynamic(
        &ctx.client,
        namespace,
        &cnpg_cluster_resource(),
        &cluster,
        dry_run,
    )
    .await
    {
        Err(kube::Error::Api(e)) if e.code == 404 => status(
            NAME,
            false,
            "CloudNative-PG is not installed: create a PlatformInstance first",
        ),
        Err(e) => status(NAME, false, e.to_string()),
        Ok(_) if dry_run => status(
            NAME,
            false,
            format!("planned (dry-run): Cluster {CATALOG_CLUSTER}"),
        ),
        Ok(applied) if cluster_ready(&applied) => status(NAME, true, "database ready"),
        Ok(_) => status(NAME, false, "waiting for the database to become Ready"),
    }
}

/// The Job that bootstraps Lakekeeper and creates or refreshes the warehouse. Its
/// name is recorded in `applied_hash`; once a Job of that name has succeeded it is
/// not applied again (the finished Job is deleted after its TTL).
pub async fn ensure_bootstrap(
    ctx: &Context,
    lh: &Lakehouse,
    namespace: &str,
    previous: &[ComponentStatus],
) -> ComponentStatus {
    const NAME: &str = "lakekeeper-bootstrap";
    if lh.spec.dry_run {
        return status(NAME, false, "planned (dry-run)");
    }
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let keys = match secrets.get_opt(S3_SECRET).await {
        Ok(Some(secret)) => match read_keys(&secret) {
            Some(keys) => keys,
            None => {
                return status(
                    NAME,
                    false,
                    "Secret stardelt-s3-creds lacks access-key or secret-key",
                );
            }
        },
        Ok(None) => return status(NAME, false, "waiting for Secret stardelt-s3-creds"),
        Err(e) => return status(NAME, false, e.to_string()),
    };
    let job_name = bootstrap_job_name(&keys, &bucket(lh), &s3_endpoint(namespace));
    let done = previous
        .iter()
        .any(|p| p.name == NAME && p.ready && p.applied_hash.as_deref() == Some(job_name.as_str()));
    if done {
        return ComponentStatus {
            applied_hash: Some(job_name),
            ..status(NAME, true, "warehouse bootstrapped")
        };
    }
    let job = bootstrap_job(&lh.name_any(), namespace, &job_name, owner_reference(lh));
    match apply::<Job>(&ctx.client, namespace, &job, false).await {
        Err(e) => status(NAME, false, e.to_string()),
        Ok(applied)
            if applied
                .status
                .as_ref()
                .and_then(|s| s.succeeded)
                .unwrap_or(0)
                >= 1 =>
        {
            ComponentStatus {
                applied_hash: Some(job_name),
                ..status(NAME, true, "warehouse bootstrapped")
            }
        }
        Ok(_) => status(NAME, false, "bootstrap Job is running"),
    }
}

/// `<lakehouse>-discovery`: the ConfigMap that tells consumers where the catalog, the
/// object store and Trino are. Published once the warehouse is bootstrapped, with
/// server-side apply on every reconcile, and owned by the Lakehouse (it is derived
/// state: a dangling address is worse than none). It never holds a credential.
pub async fn ensure_discovery(
    ctx: &Context,
    lh: &Lakehouse,
    namespace: &str,
    bootstrapped: bool,
) -> ComponentStatus {
    const NAME: &str = "discovery";
    if lh.spec.dry_run {
        return status(NAME, false, "planned (dry-run)");
    }
    if !bootstrapped {
        return status(NAME, false, "waiting for the warehouse bootstrap");
    }
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let location = match secrets.get_opt(S3_SECRET).await {
        Ok(Some(secret)) => match read_location(&secret) {
            Some(location) => location,
            None => {
                return status(
                    NAME,
                    false,
                    "Secret stardelt-s3-creds lacks endpoint, bucket or region",
                );
            }
        },
        Ok(None) => return status(NAME, false, "waiting for Secret stardelt-s3-creds"),
        Err(e) => return status(NAME, false, e.to_string()),
    };
    let lakehouse = lh.name_any();
    let config_map = discovery_config_map(&lakehouse, namespace, &location, owner_reference(lh));
    match apply::<ConfigMap>(&ctx.client, namespace, &config_map, false).await {
        Ok(_) => status(
            NAME,
            true,
            format!(
                "published ConfigMap {}",
                discovery_config_map_name(&lakehouse)
            ),
        ),
        Err(e) => status(NAME, false, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn read_keys_needs_both_keys() {
        let mut data = BTreeMap::new();
        data.insert("access-key".to_string(), ByteString(b"AK".to_vec()));
        let mut secret = Secret {
            data: Some(data.clone()),
            ..Default::default()
        };
        assert!(read_keys(&secret).is_none());
        data.insert("secret-key".to_string(), ByteString(b"SK".to_vec()));
        secret.data = Some(data);
        assert_eq!(read_keys(&secret).unwrap().access_key, "AK");
    }

    #[test]
    fn a_cluster_is_ready_only_with_a_true_ready_condition() {
        let mut cluster = catalog_cluster("main", "data", crate::crd::Profile::Demo);
        assert!(!cluster_ready(&cluster));
        cluster.data["status"] =
            serde_json::json!({"conditions": [{"type": "Ready", "status": "False"}]});
        assert!(!cluster_ready(&cluster));
        cluster.data["status"] =
            serde_json::json!({"conditions": [{"type": "Ready", "status": "True"}]});
        assert!(cluster_ready(&cluster));
    }

    /// Every request a mock client saw: method, path, body.
    type Recorded = Arc<Mutex<Vec<(String, String, String)>>>;

    /// A kube client that answers `GET secrets/stardelt-s3-creds` with a complete
    /// Secret (or 404) and echoes every PATCH body back. Every request is recorded.
    fn mock_client(secret_present: bool) -> (kube::Client, Recorded) {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            async move {
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("mock body");
                let body = String::from_utf8_lossy(&body).to_string();
                recorder.lock().expect("requests poisoned").push((
                    method.clone(),
                    path.clone(),
                    body.clone(),
                ));
                let (status, document) = if method == "PATCH" {
                    (200, body)
                } else if secret_present {
                    // base64 of: http://seaweedfs-s3.data.svc.cluster.local:8333 / lakehouse / us-east-1
                    (
                        200,
                        r#"{"apiVersion":"v1","kind":"Secret","metadata":{"name":"stardelt-s3-creds"},"data":{"endpoint":"aHR0cDovL3NlYXdlZWRmcy1zMy5kYXRhLnN2Yy5jbHVzdGVyLmxvY2FsOjgzMzM=","bucket":"bGFrZWhvdXNl","region":"dXMtZWFzdC0x"}}"#.to_string(),
                    )
                } else {
                    (
                        404,
                        r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","code":404,"message":"not found"}"#.to_string(),
                    )
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(document.into_bytes()))
                        .expect("valid mock response"),
                )
            }
        });
        (kube::Client::new(service, "data"), requests)
    }

    fn context(client: kube::Client) -> Context {
        Context {
            client,
            helm: Arc::new(crate::helm::testing::RecordingRunner::default()),
        }
    }

    fn lakehouse() -> Lakehouse {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "data", "uid": "u-1" },
            "spec": { "dryRun": false }
        }))
        .expect("deserialize Lakehouse")
    }

    #[tokio::test]
    async fn discovery_is_published_once_the_warehouse_is_bootstrapped() {
        let (client, requests) = mock_client(true);
        let status = ensure_discovery(&context(client), &lakehouse(), "data", true).await;
        assert!(status.ready, "{status:?}");
        assert_eq!(status.name, "discovery");
        assert_eq!(
            status.message.as_deref(),
            Some("published ConfigMap main-discovery")
        );
        let requests = requests.lock().expect("requests poisoned");
        let patch = requests
            .iter()
            .find(|(method, _, _)| method == "PATCH")
            .expect("a server-side apply");
        assert_eq!(patch.1, "/api/v1/namespaces/data/configmaps/main-discovery");
        let body: serde_json::Value = serde_json::from_str(&patch.2).expect("JSON body");
        // Server-side apply needs the type in the body.
        assert_eq!(body["apiVersion"], "v1");
        assert_eq!(body["kind"], "ConfigMap");
        assert_eq!(
            body["data"]["trinoUrl"],
            "http://trino.data.svc.cluster.local:8080"
        );
        assert_eq!(
            body["data"]["s3Endpoint"],
            "http://seaweedfs-s3.data.svc.cluster.local:8333"
        );
        assert_eq!(body["data"]["s3Bucket"], "lakehouse");
        assert_eq!(body["metadata"]["ownerReferences"][0]["name"], "main");
    }

    #[tokio::test]
    async fn discovery_waits_for_the_bootstrap_and_touches_nothing() {
        let (client, requests) = mock_client(true);
        let status = ensure_discovery(&context(client), &lakehouse(), "data", false).await;
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for the warehouse bootstrap")
        );
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn discovery_waits_for_the_s3_secret() {
        let (client, requests) = mock_client(false);
        let status = ensure_discovery(&context(client), &lakehouse(), "data", true).await;
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for Secret stardelt-s3-creds")
        );
        assert!(
            requests
                .lock()
                .expect("requests poisoned")
                .iter()
                .all(|(method, _, _)| method == "GET"),
            "nothing may be applied without the Secret"
        );
    }
}
