//! Applies the prerequisites of a Lakehouse (decision D17) and reports each one as
//! a component status. The objects are built in `prerequisites.rs` and sent with
//! the helpers in `objects.rs`.

use std::collections::BTreeMap;

use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::Deployment,
        batch::v1::Job,
        core::v1::{ConfigMap, Secret, Service, ServiceAccount},
        networking::v1::{Ingress, NetworkPolicy},
        rbac::v1::{Role, RoleBinding},
    },
};
use kube::{
    Api, ResourceExt,
    api::{Patch, PatchParams},
    core::{ApiResource, DynamicObject, ObjectMeta, TypeMeta},
};
use serde_json::json;

use crate::{
    access_sync, airflow,
    controller::Context,
    crd::{
        CatalogImpl, ComponentStatus, IdentityMode, Lakehouse, PlatformInstanceSpec, SecretsMode,
        StorageImpl,
    },
    identity, ingress, kafka, keycloak, network_policy, nova,
    objects::{
        apply, apply_dynamic, apply_dynamic_cluster, cert_manager_certificate_resource,
        cert_manager_issuer_resource, cnpg_cluster_resource, cnpg_scheduled_backup_resource,
        kafka_node_pool_resource, kafka_resource, labels, owner_reference,
    },
    pg_dump,
    prerequisites::{
        CATALOG_CLUSTER, DEFAULT_BUCKET, NOVA_CLUSTER, S3_CONFIG_SECRET, S3_SECRET, S3Keys,
        S3Location, archive_bucket_job, archive_bucket_job_name, archive_cron_job, bootstrap_job,
        bootstrap_job_name, catalog_cluster, catalog_pg_backup, discovery_config_map,
        discovery_config_map_name, keycloak_pg_backup, keycloak_pg_cluster, notebooks_bucket_job,
        notebooks_bucket_job_name, nova_pg_backup, nova_pg_cluster, openfga_pg_backup,
        openfga_pg_cluster, s3_creds_secret, s3_endpoint, seaweedfs_config_secret,
    },
    secrets, spark, superset,
};
use k8s_openapi::api::batch::v1::CronJob;

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

/// The self-signed root of the Trino trust anchor (manifests/trino-tls.yaml, object 1).
pub fn trino_selfsigned_issuer(namespace: &str) -> DynamicObject {
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "cert-manager.io/v1".into(),
            kind: "Issuer".into(),
        }),
        metadata: ObjectMeta {
            name: Some("trino-selfsigned".into()),
            namespace: Some(namespace.into()),
            labels: Some(labels(namespace, "trino-tls")),
            ..Default::default()
        },
        data: json!({
            "spec": {
                "selfSigned": {}
            }
        }),
    }
}

/// The namespace-local CA certificate (object 2): isCA true, commonName
/// "stardelt internal CA (trino)", secretName "trino-ca", duration "43800h",
/// renewBefore "8760h", usages ["cert sign", "crl sign"], privateKey {algorithm: RSA, size: 2048},
/// issuerRef {name: "trino-selfsigned", kind: "Issuer"}.
pub fn trino_ca_certificate(namespace: &str) -> DynamicObject {
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "cert-manager.io/v1".into(),
            kind: "Certificate".into(),
        }),
        metadata: ObjectMeta {
            name: Some("trino-ca".into()),
            namespace: Some(namespace.into()),
            labels: Some(labels(namespace, "trino-tls")),
            ..Default::default()
        },
        data: json!({
            "spec": {
                "isCA": true,
                "commonName": "stardelt internal CA (trino)",
                "secretName": "trino-ca",
                "duration": "43800h",
                "renewBefore": "8760h",
                "usages": ["cert sign", "crl sign"],
                "privateKey": {
                    "algorithm": "RSA",
                    "size": 2048
                },
                "issuerRef": {
                    "name": "trino-selfsigned",
                    "kind": "Issuer"
                }
            }
        }),
    }
}

/// The CA issuer that signs the coordinator certificate (object 3): spec.ca.secretName "trino-ca".
pub fn trino_ca_issuer(namespace: &str) -> DynamicObject {
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "cert-manager.io/v1".into(),
            kind: "Issuer".into(),
        }),
        metadata: ObjectMeta {
            name: Some("trino-ca".into()),
            namespace: Some(namespace.into()),
            labels: Some(labels(namespace, "trino-tls")),
            ..Default::default()
        },
        data: json!({
            "spec": {
                "ca": {
                    "secretName": "trino-ca"
                }
            }
        }),
    }
}

/// The coordinator certificate (object 4), including the PKCS12 keystore Trino loads:
/// secretName "trino-coordinator-tls", duration "2160h", renewBefore "720h",
/// usages ["server auth"], dnsNames ["trino", "trino.<ns>", "trino.<ns>.svc",
/// "trino.<ns>.svc.cluster.local"], keystores.pkcs12 {create: true, profile: "Modern2023",
/// passwordSecretRef {name: "trino-tls-keystore", key: "password"}},
/// issuerRef {name: "trino-ca", kind: "Issuer"}.
pub fn trino_coordinator_certificate(namespace: &str) -> DynamicObject {
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "cert-manager.io/v1".into(),
            kind: "Certificate".into(),
        }),
        metadata: ObjectMeta {
            name: Some("trino-coordinator-tls".into()),
            namespace: Some(namespace.into()),
            labels: Some(labels(namespace, "trino-tls")),
            ..Default::default()
        },
        data: json!({
            "spec": {
                "secretName": "trino-coordinator-tls",
                "duration": "2160h",
                "renewBefore": "720h",
                "usages": ["server auth"],
                "dnsNames": [
                    "trino",
                    format!("trino.{}", namespace),
                    format!("trino.{}.svc", namespace),
                    format!("trino.{}.svc.cluster.local", namespace)
                ],
                "keystores": {
                    "pkcs12": {
                        "create": true,
                        "profile": "Modern2023",
                        "passwordSecretRef": {
                            "name": "trino-tls-keystore",
                            "key": "password"
                        }
                    }
                },
                "issuerRef": {
                    "name": "trino-ca",
                    "kind": "Issuer"
                }
            }
        }),
    }
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
    let applied = match apply_dynamic(
        &ctx.client,
        namespace,
        &cnpg_cluster_resource(),
        &cluster,
        dry_run,
    )
    .await
    {
        Err(kube::Error::Api(e)) if e.code == 404 => {
            return status(
                NAME,
                false,
                "CloudNative-PG is not installed: create a PlatformInstance first",
            );
        }
        Err(e) => return status(NAME, false, e.to_string()),
        Ok(applied) => applied,
    };
    if dry_run {
        return status(
            NAME,
            false,
            format!("planned (dry-run): Cluster {CATALOG_CLUSTER}"),
        );
    }
    if let Err(e) = apply_dynamic(
        &ctx.client,
        namespace,
        &cnpg_scheduled_backup_resource(),
        &catalog_pg_backup(&lh.name_any(), namespace),
        false,
    )
    .await
    {
        return status(NAME, false, e.to_string());
    }
    if cluster_ready(&applied) {
        status(NAME, true, "database ready")
    } else {
        status(NAME, false, "waiting for the database to become Ready")
    }
}

/// The permission store's database and its daily base backup (O5), applied by the `Lakehouse`
/// controller like the catalog database. The store is empty and unenforced until the cut-over
/// (platform#304); a missing CloudNative-PG operator is reported the same way.
pub async fn ensure_openfga_database(
    ctx: &Context,
    lh: &Lakehouse,
    namespace: &str,
) -> ComponentStatus {
    const NAME: &str = "openfga-database";
    let dry_run = lh.spec.dry_run;
    let cluster = openfga_pg_cluster(namespace);
    let applied = match apply_dynamic(
        &ctx.client,
        namespace,
        &cnpg_cluster_resource(),
        &cluster,
        dry_run,
    )
    .await
    {
        Err(kube::Error::Api(e)) if e.code == 404 => {
            return status(
                NAME,
                false,
                "CloudNative-PG is not installed: create a PlatformInstance first",
            );
        }
        Err(e) => return status(NAME, false, e.to_string()),
        Ok(applied) => applied,
    };
    if dry_run {
        return status(NAME, false, "planned (dry-run): Cluster openfga-pg");
    }
    if let Err(e) = apply_dynamic(
        &ctx.client,
        namespace,
        &cnpg_scheduled_backup_resource(),
        &openfga_pg_backup(namespace),
        false,
    )
    .await
    {
        return status(NAME, false, e.to_string());
    }
    if cluster_ready(&applied) {
        status(NAME, true, "database ready")
    } else {
        status(NAME, false, "waiting for the database to become Ready")
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

/// The versioned bucket for published notebooks, created by a Job (the object store's chart can
/// create a bucket but cannot switch versioning on). `None` with your own object store (`byoS3`):
/// the buckets there are the administrator's, and the notebooks bucket must be created and
/// versioned by hand. Remembered like the bootstrap Job: once it has succeeded for an endpoint it
/// is not applied again.
pub async fn ensure_notebooks_bucket(
    ctx: &Context,
    lh: &Lakehouse,
    namespace: &str,
    previous: &[ComponentStatus],
) -> Option<ComponentStatus> {
    const NAME: &str = "notebooks-bucket";
    if lh.spec.storage.implementation != StorageImpl::Seaweedfs {
        return None;
    }
    if lh.spec.dry_run {
        return Some(status(NAME, false, "planned (dry-run)"));
    }
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    match secrets.get_opt(S3_SECRET).await {
        Ok(Some(_)) => {}
        Ok(None) => return Some(status(NAME, false, "waiting for Secret stardelt-s3-creds")),
        Err(e) => return Some(status(NAME, false, e.to_string())),
    }
    let job_name = notebooks_bucket_job_name(&s3_endpoint(namespace));
    let done = previous
        .iter()
        .any(|p| p.name == NAME && p.ready && p.applied_hash.as_deref() == Some(job_name.as_str()));
    if done {
        return Some(ComponentStatus {
            applied_hash: Some(job_name),
            ..status(NAME, true, "bucket notebooks is versioned")
        });
    }
    let job = notebooks_bucket_job(&lh.name_any(), namespace, &job_name, owner_reference(lh));
    match apply::<Job>(&ctx.client, namespace, &job, false).await {
        Err(e) => Some(status(NAME, false, e.to_string())),
        Ok(applied)
            if applied
                .status
                .as_ref()
                .and_then(|s| s.succeeded)
                .unwrap_or(0)
                >= 1 =>
        {
            Some(ComponentStatus {
                applied_hash: Some(job_name),
                ..status(NAME, true, "bucket notebooks is versioned")
            })
        }
        Ok(_) => Some(status(NAME, false, "the bucket Job is running")),
    }
}

/// The streaming bus (D42): one Strimzi `Kafka` and its combined `KafkaNodePool` when
/// `pillars.streaming` is on. `None` when it is off, so a platform that does not stream is
/// unchanged. The Strimzi CRDs arrive with the chart the same reconcile applies, so a first
/// tick reports "waiting for Strimzi" and the next one creates the broker.
pub async fn ensure_kafka_broker(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = kafka::NAME;
    if !spec.pillars.streaming {
        return None;
    }
    let namespace = &spec.namespace;
    for (resource, object) in [
        (
            kafka_node_pool_resource(),
            kafka::node_pool(namespace, spec.profile),
        ),
        (kafka_resource(), kafka::broker(namespace)),
    ] {
        match apply_dynamic(&ctx.client, namespace, &resource, &object, spec.dry_run).await {
            Err(kube::Error::Api(e)) if e.code == 404 => {
                return Some(status(
                    NAME,
                    false,
                    format!(
                        "waiting for Strimzi: CRD {} is not installed yet",
                        resource.plural
                    ),
                ));
            }
            Err(e) => return Some(status(NAME, false, e.to_string())),
            Ok(applied) if resource.plural == "kafkas" && kafka_ready(&applied) => {
                return Some(status(NAME, true, "broker ready"));
            }
            Ok(_) => {}
        }
    }
    if spec.dry_run {
        Some(status(
            NAME,
            false,
            format!("planned (dry-run): Kafka {NAME}"),
        ))
    } else {
        Some(status(NAME, false, "waiting for Kafka to become Ready"))
    }
}

/// Strimzi reports `status.conditions[type=Ready].status == "True"`, the same shape
/// `cluster_ready` reads for CloudNative-PG.
fn kafka_ready(broker: &DynamicObject) -> bool {
    broker.data["status"]["conditions"]
        .as_array()
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|c| c["type"] == "Ready" && c["status"] == "True")
        })
}

/// The Spark Connect endpoint (operator#239's builders, O8 part 4b): applied only with
/// `spec.compute.spark.enabled`. It reads the Lakehouse's own Secret `stardelt-s3-creds`, so the
/// step waits for it like the bucket Jobs do.
pub async fn ensure_spark(
    ctx: &Context,
    lh: &Lakehouse,
    namespace: &str,
) -> Option<ComponentStatus> {
    const NAME: &str = spark::NAME;
    if !lh.spec.compute.spark.enabled {
        return None;
    }
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    match secrets.get_opt(S3_SECRET).await {
        Ok(Some(_)) => {}
        Ok(None) => return Some(status(NAME, false, "waiting for Secret stardelt-s3-creds")),
        Err(e) => return Some(status(NAME, false, e.to_string())),
    }
    let dry_run = lh.spec.dry_run;
    let client = &ctx.client;
    if let Err(e) = apply::<ServiceAccount>(
        client,
        namespace,
        &spark::service_account(namespace),
        dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<Role>(client, namespace, &spark::role(namespace), dry_run).await {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) =
        apply::<RoleBinding>(client, namespace, &spark::role_binding(namespace), dry_run).await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) =
        apply::<ConfigMap>(client, namespace, &spark::config_map(namespace), dry_run).await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<Service>(client, namespace, &spark::service(namespace), dry_run).await {
        return Some(status(NAME, false, e.to_string()));
    }
    let applied = match apply::<Deployment>(
        client,
        namespace,
        &spark::deployment(namespace),
        dry_run,
    )
    .await
    {
        Ok(applied) => applied,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    if dry_run {
        return Some(status(
            NAME,
            false,
            format!("planned (dry-run): Deployment {}", spark::NAME),
        ));
    }
    if applied
        .status
        .as_ref()
        .and_then(|s| s.available_replicas)
        .unwrap_or(0)
        >= 1
    {
        Some(status(NAME, true, "running"))
    } else {
        Some(status(
            NAME,
            false,
            "waiting for the Deployment to become ready",
        ))
    }
}

/// The object-store archive: the destination bucket (a Job, remembered like the notebooks
/// bucket) and the nightly `aws s3 sync` into it. `None` with your own object store (`byoS3`):
/// there the archive destination is the administrator's bucket to create and protect.
pub async fn ensure_archive(
    ctx: &Context,
    lh: &Lakehouse,
    namespace: &str,
    previous: &[ComponentStatus],
) -> Option<ComponentStatus> {
    const NAME: &str = "object-store-archive";
    if lh.spec.storage.implementation != StorageImpl::Seaweedfs {
        return None;
    }
    if lh.spec.dry_run {
        return Some(status(NAME, false, "planned (dry-run)"));
    }
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    match secrets.get_opt(S3_SECRET).await {
        Ok(Some(_)) => {}
        Ok(None) => return Some(status(NAME, false, "waiting for Secret stardelt-s3-creds")),
        Err(e) => return Some(status(NAME, false, e.to_string())),
    }
    let job_name = archive_bucket_job_name(&s3_endpoint(namespace));
    let done = previous
        .iter()
        .any(|p| p.name == NAME && p.ready && p.applied_hash.as_deref() == Some(job_name.as_str()));
    if !done {
        let job = archive_bucket_job(&lh.name_any(), namespace, &job_name, owner_reference(lh));
        match apply::<Job>(&ctx.client, namespace, &job, false).await {
            Err(e) => return Some(status(NAME, false, e.to_string())),
            Ok(job_applied)
                if job_applied
                    .status
                    .as_ref()
                    .and_then(|s| s.succeeded)
                    .unwrap_or(0)
                    >= 1 => {}
            Ok(_) => return Some(status(NAME, false, "the archive bucket Job is running")),
        }
    }
    // The CronJob is applied on every reconcile: server-side apply converges it.
    if let Err(e) = apply::<CronJob>(
        &ctx.client,
        namespace,
        &archive_cron_job(namespace, &lh.name_any()),
        false,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    Some(ComponentStatus {
        applied_hash: Some(job_name),
        ..status(
            NAME,
            true,
            "bucket lakehouse-backup exists; archive runs at 03:15",
        )
    })
}

/// The catalog's ingress policy, for a Lakekeeper catalog only. `None` for Polaris.
pub async fn ensure_lakekeeper_network_policy(
    ctx: &Context,
    lh: &Lakehouse,
    namespace: &str,
) -> Option<ComponentStatus> {
    const NAME: &str = "lakekeeper-network-policy";
    if !matches!(lh.spec.catalog.implementation, CatalogImpl::Lakekeeper) {
        return None;
    }
    if let Err(e) = apply::<NetworkPolicy>(
        &ctx.client,
        namespace,
        &network_policy::lakekeeper_ingress(namespace),
        lh.spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if lh.spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): NetworkPolicy lakekeeper-ingress",
        ));
    }
    Some(status(NAME, true, "applied: lakekeeper-ingress"))
}

/// The permission store's ingress policy. Applied on every reconcile: it selects pods by
/// label, so it is inert until the OpenFGA release exists.
pub async fn ensure_openfga_network_policy(
    ctx: &Context,
    lh: &Lakehouse,
    namespace: &str,
) -> Option<ComponentStatus> {
    const NAME: &str = "openfga-network-policy";
    if let Err(e) = apply::<NetworkPolicy>(
        &ctx.client,
        namespace,
        &network_policy::openfga_ingress(namespace),
        lh.spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if lh.spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): NetworkPolicy openfga-ingress",
        ));
    }
    Some(status(NAME, true, "applied: openfga-ingress"))
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
    identity: bool,
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
    let trino_url = crate::identity::trino_url(namespace, identity);
    let config_map = discovery_config_map(
        &lakehouse,
        namespace,
        &location,
        &trino_url,
        owner_reference(lh),
    );
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

/// The CloudNative-PG `Cluster` of Keycloak plus its daily base backup, for
/// `identity.mode: platform`. `None` in the other modes: without Keycloak there is no
/// identity database to keep. A missing CNPG operator is reported like
/// [`ensure_catalog_database`] does.
pub async fn ensure_keycloak_pg(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    if spec.identity.mode != IdentityMode::Platform {
        return None;
    }
    let namespace = &spec.namespace;
    Some(
        ensure_database(
            ctx,
            spec,
            "keycloak-pg",
            keycloak_pg_cluster(namespace),
            keycloak_pg_backup(namespace),
        )
        .await,
    )
}

/// The CloudNative-PG `Cluster` of Nova (its saved queries, settings and query log) plus its
/// daily base backup. `None` without the `lakehouse` pillar: Nova is the interface of the
/// lakehouse, so without one there is nothing to keep.
pub async fn ensure_nova_pg(ctx: &Context, spec: &PlatformInstanceSpec) -> Option<ComponentStatus> {
    if !spec.pillars.lakehouse {
        return None;
    }
    let namespace = &spec.namespace;
    Some(
        ensure_database(
            ctx,
            spec,
            NOVA_CLUSTER,
            nova_pg_cluster(namespace),
            nova_pg_backup(namespace),
        )
        .await,
    )
}

/// Applies a `Cluster` and then its `ScheduledBackup`, and reports whether the database is
/// Ready. A dry-run applies the Cluster only, as a plan.
async fn ensure_database(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
    name: &str,
    cluster: DynamicObject,
    backup: DynamicObject,
) -> ComponentStatus {
    let namespace = &spec.namespace;
    let applied = match apply_dynamic(
        &ctx.client,
        namespace,
        &cnpg_cluster_resource(),
        &cluster,
        spec.dry_run,
    )
    .await
    {
        Err(kube::Error::Api(e)) if e.code == 404 => {
            return status(
                name,
                false,
                "CloudNative-PG is not installed: create a PlatformInstance first",
            );
        }
        Err(e) => return status(name, false, e.to_string()),
        Ok(applied) => applied,
    };
    if spec.dry_run {
        return status(
            name,
            false,
            format!("planned (dry-run): Cluster {}", cluster.name_any()),
        );
    }
    if let Err(e) = apply_dynamic(
        &ctx.client,
        namespace,
        &cnpg_scheduled_backup_resource(),
        &backup,
        false,
    )
    .await
    {
        return status(name, false, e.to_string());
    }
    if cluster_ready(&applied) {
        status(name, true, "database ready")
    } else {
        status(name, false, "waiting for the database to become Ready")
    }
}

/// Secrets the Keycloak Deployment reads. `keycloak-pg-app` is created by CloudNative-PG; the
/// other three by [`ensure_identity_secrets`] (wave 2c-iii — before that the platform's
/// `scripts/identity-bootstrap.sh` created them). Without them Keycloak crashes on boot, so the
/// workload waits instead of applying. The names come from [`keycloak::IDENTITY_SECRETS`], so
/// there is one table of what the operator creates.
pub const KEYCLOAK_SECRETS: [&str; 4] = [
    "keycloak-pg-app",
    keycloak::IDENTITY_SECRETS[0].name,
    keycloak::IDENTITY_SECRETS[1].name,
    keycloak::IDENTITY_SECRETS[2].name,
];

/// The identity Secrets for `identity.mode: platform`, with the semantics of
/// `scripts/identity-bootstrap.sh`: a Secret is created when it is absent; an existing Secret
/// gets only the keys it lacks, and no value that is already there is ever rewritten (the
/// imported realm holds those credentials, so a reconcile must not rotate them). No owner
/// reference: the realm, not the `PlatformInstance`, owns the credentials.
pub async fn ensure_identity_secrets(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    if spec.identity.mode != IdentityMode::Platform {
        return None;
    }
    Some(ensure_generated_secrets(ctx, spec, "identity-secrets", &keycloak::IDENTITY_SECRETS).await)
}

/// Nova's session key: Secret `stardelt-nova-session`, key `session`, generated once and kept
/// (rotating it signs everybody out). `None` without the `lakehouse` pillar, like Nova itself.
pub async fn ensure_nova_session(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    if !spec.pillars.lakehouse {
        return None;
    }
    Some(ensure_generated_secrets(ctx, spec, "nova-session", &[nova::SESSION]).await)
}

/// Nova's query-log token: Secret `nova-events`, key `token`, generated once and kept. The same
/// value is read by Nova (collector) and by Trino (event listener), so it lives in one Secret.
pub async fn ensure_nova_events(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    if !spec.pillars.lakehouse {
        return None;
    }
    Some(ensure_generated_secrets(ctx, spec, "nova-events", &[nova::EVENTS]).await)
}

/// Creates the Secrets in `desired` when absent and adds the keys an existing Secret lacks;
/// never rewrites a value that is there. The status is named `name`.
async fn ensure_generated_secrets(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
    name: &str,
    desired: &[keycloak::IdentitySecret],
) -> ComponentStatus {
    let namespace = &spec.namespace;
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let (mut created, mut kept) = (0, 0);
    for desired in desired {
        let existing = match secrets.get_opt(desired.name).await {
            Ok(existing) => existing,
            Err(e) => return status(name, false, e.to_string()),
        };
        let Some(existing) = existing else {
            let secret = match keycloak::identity_secret(desired, namespace) {
                Ok(secret) => secret,
                Err(e) => return status(name, false, e.to_string()),
            };
            if let Err(e) = apply::<Secret>(&ctx.client, namespace, &secret, spec.dry_run).await {
                return status(name, false, e.to_string());
            }
            created += 1;
            continue;
        };
        // Kept, not rotated: only the keys this Secret lacks are generated and patched in.
        kept += 1;
        let missing = keycloak::missing_keys(&keys_of(&existing), &desired.key_names());
        let mut values = BTreeMap::new();
        for key in missing {
            let value = match desired.fixed.iter().find(|(name, _)| *name == key.as_str()) {
                Some((_, value)) => (*value).to_string(),
                None if desired.fernet.contains(&key.as_str()) => {
                    match keycloak::generate_fernet_key() {
                        Ok(value) => value,
                        Err(e) => return status(name, false, e.to_string()),
                    }
                }
                None => match keycloak::generate_secret() {
                    Ok(value) => value,
                    Err(e) => return status(name, false, e.to_string()),
                },
            };
            values.insert(key, value);
        }
        if values.is_empty() {
            continue;
        }
        if let Err(e) =
            patch_identity_secret(&ctx.client, namespace, desired.name, &values, spec.dry_run).await
        {
            return status(name, false, e.to_string());
        }
    }
    if spec.dry_run {
        return status(name, false, "planned (dry-run)");
    }
    status(
        name,
        true,
        format!("ready: created {created} / kept {kept}"),
    )
}

/// The static auto-unseal key of the sandbox secret store (`scripts/openbao-seal-key.sh`):
/// Secret `openbao-seal-key`, key `key`, 64 hex characters, generated once and **never rotated** —
/// changing it makes the store's data unreadable. The chart mounts it at /openbao/seal (step 2).
pub async fn ensure_openbao_seal_key(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    if spec.secrets.mode != SecretsMode::Platform {
        return None;
    }
    const NAME: &str = "openbao-seal-key";
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &spec.namespace);
    let existing = match secrets.get_opt(NAME).await {
        Ok(existing) => existing,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    if existing.is_some() {
        return Some(status(NAME, true, "ready: kept"));
    }
    let key = match keycloak::generate_hex(32) {
        Ok(key) => key,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(NAME.into()),
            namespace: Some(spec.namespace.clone()),
            labels: Some(labels(&spec.namespace, "openbao")),
            ..ObjectMeta::default()
        },
        data: Some(BTreeMap::from([(
            "key".to_string(),
            ByteString(key.into_bytes()),
        )])),
        ..Secret::default()
    };
    if let Err(e) = apply::<Secret>(&ctx.client, &spec.namespace, &secret, spec.dry_run).await {
        return Some(status(NAME, false, e.to_string()));
    }
    if spec.dry_run {
        return Some(status(NAME, false, "planned (dry-run)"));
    }
    Some(status(NAME, true, "ready: created"))
}

/// Initialises the sandbox store once (`scripts/openbao-bootstrap.sh`) and keeps the recovery key
/// and the initial root token in Secret `openbao-init`. Runs after the release is applied; when the
/// store does not answer yet the status is not ready and the next reconcile retries.
pub async fn ensure_openbao_init(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "openbao-init";
    if spec.secrets.mode != SecretsMode::Platform {
        return None;
    }
    let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &spec.namespace);
    match secrets_api.get_opt(secrets::INIT_SECRET).await {
        Ok(Some(existing)) => {
            let complete = text(&existing, "recovery-key").is_some()
                && text(&existing, "root-token").is_some();
            return Some(if complete {
                status(NAME, true, "ready: kept")
            } else {
                status(
                    NAME,
                    false,
                    "Secret openbao-init exists without both keys: the store is initialised and \
                     its root token is gone, restore it by hand",
                )
            });
        }
        Ok(None) => {}
        Err(e) => return Some(status(NAME, false, e.to_string())),
    }
    if spec.dry_run {
        return Some(status(NAME, false, "planned (dry-run)"));
    }
    let uri = secrets::store_proxy_path(&spec.namespace, "/v1/sys/init");
    let body = secrets::init_request_body().to_string();
    let request = match http::Request::builder()
        .method("POST")
        .uri(uri)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body.into_bytes())
    {
        Ok(request) => request,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    // `Client::request_text` resolves the relative path against the cluster URL and turns a
    // non-2xx answer into an error; an already initialised store answers 400, which the message
    // above covers on the next reconcile.
    let response = match ctx.client.request_text(request).await {
        Ok(response) => response,
        Err(e) => return Some(status(NAME, false, format!("init request failed: {e}"))),
    };
    let document: serde_json::Value = match serde_json::from_str(&response) {
        Ok(document) => document,
        Err(e) => return Some(status(NAME, false, format!("init answer is not JSON: {e}"))),
    };
    let root_token = document
        .get("root_token")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    if root_token.is_empty() {
        return Some(status(
            NAME,
            false,
            "init answer has no root_token; the store's state must be checked by hand",
        ));
    }
    // The platform stores `recovery_keys_b64[0]`; `recovery_keys[0]` is the same key in hex.
    let recovery_key = document
        .get("recovery_keys_b64")
        .and_then(|v| v.get(0))
        .and_then(|v| v.as_str())
        .or_else(|| {
            document
                .get("recovery_keys")
                .and_then(|v| v.get(0))
                .and_then(|v| v.as_str())
        })
        .unwrap_or_default()
        .to_string();
    if recovery_key.is_empty() {
        return Some(status(
            NAME,
            false,
            "init answer has no recovery key; the store's state must be checked by hand",
        ));
    }
    let secret = secrets::init_secret(&spec.namespace, &recovery_key, &root_token);
    match apply::<Secret>(&ctx.client, &spec.namespace, &secret, false).await {
        Ok(_) => Some(status(
            NAME,
            true,
            "initialised; recovery key and root token are in Secret openbao-init (sandbox only)",
        )),
        Err(e) => Some(status(NAME, false, e.to_string())),
    }
}

/// The key names of a Secret. Values are never read: the operator only decides which keys are
/// absent, and a credential must never be logged.
fn keys_of(secret: &Secret) -> BTreeMap<String, String> {
    secret
        .data
        .as_ref()
        .map(|data| {
            data.keys()
                .map(|key| (key.clone(), String::new()))
                .collect()
        })
        .unwrap_or_default()
}

/// Merge-patches `stringData` into an existing Secret: the API server adds exactly these keys to
/// `data` and leaves every other key — and every existing value — alone. `stringData` is
/// write-only, so the credentials never come back out of the API server.
async fn patch_identity_secret(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    keys: &BTreeMap<String, String>,
    dry_run: bool,
) -> Result<(), kube::Error> {
    let params = PatchParams {
        dry_run,
        ..PatchParams::default()
    };
    Api::<Secret>::namespaced(client.clone(), namespace)
        .patch(
            name,
            &params,
            &Patch::Merge(serde_json::json!({ "stringData": keys })),
        )
        .await?;
    Ok(())
}

/// The Keycloak workload (realm and environment ConfigMaps, Service, Deployment) for
/// `identity.mode: platform`. `None` in the other modes: without Keycloak nothing serves the
/// realm. Nothing is applied until every Secret the Deployment reads exists.
pub async fn ensure_keycloak(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "keycloak";
    if spec.identity.mode != IdentityMode::Platform {
        return None;
    }
    let namespace = &spec.namespace;
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let mut missing = Vec::new();
    for name in KEYCLOAK_SECRETS {
        match secrets.get_opt(name).await {
            Ok(Some(_)) => {}
            Ok(None) => missing.push(name),
            Err(e) => return Some(status(NAME, false, e.to_string())),
        }
    }
    if !missing.is_empty() {
        return Some(status(
            NAME,
            false,
            format!("waiting for identity secrets: {}", missing.join(", ")),
        ));
    }
    let realm = match keycloak::realm_config_map(spec) {
        Ok(realm) => realm,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    let env = match keycloak::env_config_map(spec) {
        Ok(env) => env,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    let deployment = keycloak::deployment(namespace);
    for config_map in [&realm, &env] {
        if let Err(e) = apply::<ConfigMap>(&ctx.client, namespace, config_map, spec.dry_run).await {
            return Some(status(NAME, false, e.to_string()));
        }
    }
    let service = keycloak::service(namespace);
    if let Err(e) = apply::<Service>(&ctx.client, namespace, &service, spec.dry_run).await {
        return Some(status(NAME, false, e.to_string()));
    }
    let applied = match apply::<Deployment>(&ctx.client, namespace, &deployment, spec.dry_run).await
    {
        Ok(applied) => applied,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            format!("planned (dry-run): Deployment {}", keycloak::NAME),
        ));
    }
    if applied
        .status
        .as_ref()
        .and_then(|s| s.available_replicas)
        .unwrap_or(0)
        >= 1
    {
        Some(status(NAME, true, "running"))
    } else {
        Some(status(
            NAME,
            false,
            "waiting for the Deployment to become ready",
        ))
    }
}

/// The Airflow Secrets ([`airflow::SECRETS`]), for the `pipelines` pillar. `None` without it. They
/// are created BEFORE the Airflow release: its pods read them when they start, and the database
/// migration Job gives up after a few tries if its Secret appears late.
pub async fn ensure_airflow_secrets(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    if !spec.pillars.pipelines {
        return None;
    }
    Some(ensure_generated_secrets(ctx, spec, "airflow-secrets", &airflow::SECRETS).await)
}

/// The Superset Secrets ([`superset::SECRETS`]), for the `dashboards` pillar. `None` without it.
pub async fn ensure_superset_secrets(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    if !spec.pillars.dashboards {
        return None;
    }
    Some(ensure_generated_secrets(ctx, spec, "superset-secrets", &superset::SECRETS).await)
}

/// The nightly dumps of the Airflow and Superset databases (O8). Each CronJob needs its
/// pillar; the whole step needs the `lakehouse` pillar, because the dump is uploaded to the
/// `backups` bucket with the Lakehouse's S3 keys ([`S3_SECRET`]). No waiting: a job that fires
/// while the chart is still starting fails once, and the next window is a day away.
pub async fn ensure_pg_dumps(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "pg-dumps";
    if !spec.pillars.lakehouse {
        return None;
    }
    let namespace = &spec.namespace;
    let mut applied = Vec::new();
    if spec.pillars.dashboards {
        if let Err(e) = apply::<CronJob>(
            &ctx.client,
            namespace,
            &pg_dump::superset_cron_job(namespace),
            spec.dry_run,
        )
        .await
        {
            return Some(status(NAME, false, e.to_string()));
        }
        applied.push(pg_dump::SUPERSET_JOB);
    }
    if spec.pillars.pipelines {
        if let Err(e) = apply::<CronJob>(
            &ctx.client,
            namespace,
            &pg_dump::airflow_cron_job(namespace),
            spec.dry_run,
        )
        .await
        {
            return Some(status(NAME, false, e.to_string()));
        }
        applied.push(pg_dump::AIRFLOW_JOB);
    }
    if applied.is_empty() {
        return None;
    }
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            format!("planned (dry-run): {}", applied.join(", ")),
        ));
    }
    Some(status(
        NAME,
        true,
        format!("applied: {}", applied.join(", ")),
    ))
}

/// The `superset` ServiceAccount, the Catalogs read Role and its binding. **Must be applied
/// before the Superset release**: the chart's pods and init Job run as this ServiceAccount,
/// and a Pod naming a ServiceAccount that does not exist stays Pending. `None` without the
/// `dashboards` pillar.
pub async fn ensure_superset_rbac(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "superset-rbac";
    if !spec.pillars.dashboards {
        return None;
    }
    let namespace = &spec.namespace;
    if let Err(e) = apply::<ServiceAccount>(
        &ctx.client,
        namespace,
        &superset::service_account(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<Role>(
        &ctx.client,
        namespace,
        &superset::catalog_reader_role(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<RoleBinding>(
        &ctx.client,
        namespace,
        &superset::catalog_reader_binding(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): ServiceAccount, Role and RoleBinding superset-group-roles",
        ));
    }
    Some(status(NAME, true, "applied: superset-rbac"))
}

/// The ConfigMap with the group-roles script and the CronJob that converges
/// `stardelt-group-<group>` FAB roles from the Catalogs' grants (platform#494). Applied after
/// the release, because the Job reads the chart's `superset-env`/`superset-config` Secrets.
/// `None` without the `dashboards` pillar.
pub async fn ensure_superset_group_roles(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "superset-group-roles";
    if !spec.pillars.dashboards {
        return None;
    }
    let namespace = &spec.namespace;
    let image = superset::image();
    if let Err(e) = apply::<ConfigMap>(
        &ctx.client,
        namespace,
        &superset::group_roles_config_map(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<CronJob>(
        &ctx.client,
        namespace,
        &superset::group_roles_cron_job(namespace, &image),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): ConfigMap and CronJob superset-group-roles",
        ));
    }
    Some(status(NAME, true, "applied: superset-group-roles"))
}

/// The Role that lets Airflow read the `Pipeline` and `ServicePrincipal` resources, and its
/// binding to the DAG processor and the worker. `None` without the `pipelines` pillar. The
/// ServiceAccounts come with the Airflow release; a binding may name one that does not exist yet.
pub async fn ensure_airflow_rbac(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "airflow-rbac";
    if !spec.pillars.pipelines {
        return None;
    }
    let namespace = &spec.namespace;
    if let Err(e) = apply::<Role>(
        &ctx.client,
        namespace,
        &airflow::pipeline_reader_role(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<RoleBinding>(
        &ctx.client,
        namespace,
        &airflow::pipeline_reader_binding(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): Role and RoleBinding airflow-pipeline-reader",
        ));
    }
    Some(status(NAME, true, "applied: airflow-pipeline-reader"))
}

/// The CronJob that creates `stardelt-group-<ownerGroup>` FAB roles for the owner groups of
/// Pipelines and ServicePrincipals (platform#477 S1b). `None` without the `pipelines` pillar.
pub async fn ensure_airflow_group_roles(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "airflow-group-roles";
    if !spec.pillars.pipelines {
        return None;
    }
    let namespace = &spec.namespace;
    let image = airflow::image();
    if let Err(e) = apply::<ServiceAccount>(
        &ctx.client,
        namespace,
        &airflow::group_roles_service_account(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<ConfigMap>(
        &ctx.client,
        namespace,
        &airflow::group_roles_config_map(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<RoleBinding>(
        &ctx.client,
        namespace,
        &airflow::group_roles_binding(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<CronJob>(
        &ctx.client,
        namespace,
        &airflow::group_roles_cron_job(namespace, &image),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): ServiceAccount, ConfigMap, RoleBinding and CronJob airflow-group-roles",
        ));
    }
    Some(status(NAME, true, "applied: airflow-group-roles"))
}

/// The two ConfigMaps the warehouse half of the access sync writes (`trino-warehouses`,
/// `trino-group-file`): created empty when they are absent, and **never** applied over an
/// existing one — a server-side apply with force would blank the file the sync wrote
/// (`make trino-warehouse-configmaps` in stardelt-platform has the same rule). Runs for **any**
/// identity provider (`mode != none`): the access-sync CronJob fills the two files in `platform`
/// mode, while an `external` install gets them empty — the identity overlay refuses to start
/// Trino with a *missing* file, not with an empty one. `None` when no identity provider is
/// configured: the identity overlay is not applied then, so nothing reads these files.
pub async fn ensure_trino_config_maps(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "trino-configmaps";
    if spec.identity.mode == IdentityMode::None {
        return None;
    }
    let namespace = &spec.namespace;
    let config_maps: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
    let (mut created, mut kept) = (0, 0);
    for desired in [
        access_sync::warehouses_config_map(namespace),
        access_sync::group_file_config_map(namespace),
    ] {
        match config_maps.get_opt(&desired.name_any()).await {
            Err(e) => return Some(status(NAME, false, e.to_string())),
            Ok(Some(_)) => kept += 1,
            Ok(None) => {
                if let Err(e) =
                    apply::<ConfigMap>(&ctx.client, namespace, &desired, spec.dry_run).await
                {
                    return Some(status(NAME, false, e.to_string()));
                }
                created += 1;
            }
        }
    }
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): the ConfigMaps trino-warehouses and trino-group-file when absent",
        ));
    }
    Some(status(
        NAME,
        true,
        format!("created {created} / kept {kept}"),
    ))
}

/// Secret that holds the PKCS12 keystore password of the coordinator certificate. Generated
/// once and kept: a re-issue with a different password needs a new Certificate as well
/// (stardelt-platform/scripts/trino-tls-secret.sh documents the rotation).
pub const TRINO_KEYSTORE_SECRET: &str = "trino-tls-keystore";
const TRINO_KEYSTORE: keycloak::IdentitySecret = keycloak::IdentitySecret {
    name: TRINO_KEYSTORE_SECRET,
    fixed: &[],
    generated: &["password"],
    fernet: &[],
};

/// The Trino trust anchor for any identity provider (`mode != none`): the platform's `make trino`
/// rule creates the same anchor whenever ConfigMap `stardelt-identity` exists. The keystore
/// password Secret comes first (cert-manager never issues a certificate whose keystore password
/// Secret is missing), then the two Issuers and the two Certificates of manifests/trino-tls.yaml.
pub async fn ensure_trino_tls(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "trino-tls";
    if spec.identity.mode == IdentityMode::None {
        return None;
    }
    let namespace = &spec.namespace;
    let secret = ensure_generated_secrets(ctx, spec, NAME, &[TRINO_KEYSTORE]).await;
    if !secret.ready && !spec.dry_run {
        return Some(secret);
    }
    for (resource, object) in [
        (
            cert_manager_issuer_resource(),
            trino_selfsigned_issuer(namespace),
        ),
        (
            cert_manager_certificate_resource(),
            trino_ca_certificate(namespace),
        ),
        (cert_manager_issuer_resource(), trino_ca_issuer(namespace)),
        (
            cert_manager_certificate_resource(),
            trino_coordinator_certificate(namespace),
        ),
    ] {
        if let Err(e) =
            apply_dynamic(&ctx.client, namespace, &resource, &object, spec.dry_run).await
        {
            return Some(status(NAME, false, e.to_string()));
        }
    }
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): the Trino TLS Issuers and Certificates",
        ));
    }
    Some(status(
        NAME,
        true,
        "applied: the Trino TLS Issuers and Certificates",
    ))
}

/// Secret that carries the coordinator ↔ worker shared secret (key `shared-secret`). Trino
/// refuses to start with an authenticator and no shared secret.
pub const TRINO_INTERNAL_SECRET: &str = "trino-internal-secret";
const TRINO_INTERNAL: keycloak::IdentitySecret = keycloak::IdentitySecret {
    name: TRINO_INTERNAL_SECRET,
    fixed: &[],
    generated: &["shared-secret"],
    fernet: &[],
};

/// The Trino service account for any identity provider (`mode != none`): Secret
/// `trino-password-db`, keys `password.db` (`superset:<bcrypt>` — the PASSWORD authenticator's
/// file), `password` (the plaintext, which Superset's datasource hook reads) and
/// `username=superset`. Created with the value it needs and never rewritten; a Secret created
/// before this step only gets the missing `password.db`.
pub async fn ensure_trino_auth_secrets(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "trino-auth-secrets";
    if spec.identity.mode == IdentityMode::None {
        return None;
    }
    let internal = ensure_generated_secrets(ctx, spec, NAME, &[TRINO_INTERNAL]).await;
    if !internal.ready && !spec.dry_run {
        return Some(internal);
    }
    let namespace = &spec.namespace;
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let existing = match secrets.get_opt("trino-password-db").await {
        Err(e) => return Some(status(NAME, false, e.to_string())),
        Ok(existing) => existing,
    };
    let plaintext = existing
        .as_ref()
        .and_then(|s| s.string_data.as_ref())
        .and_then(|d| d.get("password").cloned());
    let has_db_file = existing
        .as_ref()
        .and_then(|s| s.data.as_ref())
        .is_some_and(|d| d.contains_key("password.db"))
        || existing
            .as_ref()
            .and_then(|s| s.string_data.as_ref())
            .is_some_and(|d| d.contains_key("password.db"));
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): the Trino auth Secrets",
        ));
    }
    if let Some(existing) = existing {
        if has_db_file {
            return Some(status(NAME, true, "kept (created 0)"));
        }
        // The plaintext may live under `password` (this operator's key) or, for a Secret the
        // platform made, under `superset-password`. A Secret with neither cannot be completed
        // without rotating the service account: say so instead of guessing.
        let password = plaintext.or_else(|| {
            existing
                .data
                .as_ref()
                .and_then(|d| d.get("superset-password"))
                .and_then(|v| String::from_utf8(v.0.clone()).ok())
        });
        let password = match password {
            Some(password) => password,
            None => {
                return Some(status(
                    NAME,
                    false,
                    "trino-password-db exists without a password key — delete it to regenerate",
                ));
            }
        };
        // Patch only the new key; the API server keeps the others.
        let params = PatchParams {
            dry_run: false,
            ..PatchParams::default()
        };
        if let Err(e) = secrets
            .patch(
                "trino-password-db",
                &params,
                &Patch::Merge(serde_json::json!({
                    "stringData": { "password.db": password_db_line(&password) }
                })),
            )
            .await
        {
            return Some(status(NAME, false, e.to_string()));
        }
        Some(status(NAME, true, "added password.db"))
    } else {
        let mut secret = match keycloak::identity_secret(
            &keycloak::IdentitySecret {
                name: "trino-password-db",
                fixed: &[("username", "superset")],
                generated: &["password"],
                fernet: &[],
            },
            namespace,
        ) {
            Ok(s) => s,
            Err(e) => return Some(status(NAME, false, e.to_string())),
        };
        let password = secret
            .data
            .as_ref()
            .and_then(|d| d.get("password"))
            .and_then(|v| String::from_utf8(v.0.clone()).ok())
            .unwrap_or_default();
        if secret.string_data.is_none() {
            secret.string_data = Some(std::collections::BTreeMap::new());
        }
        secret
            .string_data
            .as_mut()
            .unwrap()
            .insert("password.db".to_string(), password_db_line(&password));
        if let Err(e) = apply::<Secret>(&ctx.client, namespace, &secret, spec.dry_run).await {
            return Some(status(NAME, false, e.to_string()));
        }
        Some(status(NAME, true, "created 2 / kept 0"))
    }
}

/// `superset:<bcrypt>`, in the format the live platform Secret uses (`$2y$10$…`). The Rust
/// crate emits `$2b$`; `2b` and `2y` differ only in an old PHP bug for non-ASCII passwords, so
/// the prefix is rewritten and the hash stays valid (the platform's own `htpasswd -B` output is
/// `$2y$` and Trino accepts it — checked on the dev cluster 2026-11-04).
fn password_db_line(password: &str) -> String {
    let hash = bcrypt::hash(password, 10).expect("bcrypt hashing cannot fail for a short password");
    format!("superset:{}", hash.replacen("$2b$", "$2y$", 1))
}

/// The CronJob that turns `Catalog` objects into Lakekeeper warehouses and `SqlWarehouse`
/// objects into Trino resource groups (platform#305/#306/#312). Applied only when Secret
/// `svc-catalog-operator` exists: it is the job's Lakekeeper credential, and a tick without
/// it fails every two minutes. `None` without the platform identity.
pub async fn ensure_access_sync(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "access-sync";
    if spec.identity.mode != IdentityMode::Platform {
        return None;
    }
    let namespace = &spec.namespace;
    if !spec.dry_run {
        let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
        match secrets.get_opt(access_sync::CLIENT_SECRET).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Some(status(
                    NAME,
                    false,
                    "waiting for Secret svc-catalog-operator",
                ));
            }
            Err(e) => return Some(status(NAME, false, e.to_string())),
        }
    }
    if let Err(e) = apply::<ServiceAccount>(
        &ctx.client,
        namespace,
        &access_sync::service_account(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<ConfigMap>(
        &ctx.client,
        namespace,
        &access_sync::script_config_map(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<Role>(
        &ctx.client,
        namespace,
        &access_sync::role(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<RoleBinding>(
        &ctx.client,
        namespace,
        &access_sync::binding(namespace),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if let Err(e) = apply::<CronJob>(
        &ctx.client,
        namespace,
        &access_sync::cron_job(namespace, &spec.identity.admin_group),
        spec.dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            "planned (dry-run): ServiceAccount, ConfigMap, Role, RoleBinding and CronJob access-sync",
        ));
    }
    Some(status(NAME, true, "applied: access-sync"))
}

/// The build of Nova the operator installs: the one `stardelt-platform` pins in
/// `manifests/nova-deployment.yaml`, by digest, so a reconcile never changes it by itself
/// (decision D-op-5 of `design/operator-parity.md`: a constant per operator release).
/// `stardelt-platform`'s `scripts/check-operator-pins.sh` compares this constant with its pin
/// (stardelt-platform#759).
pub const NOVA_IMAGE: &str = "ghcr.io/stardelt/nova:dev@sha256:876e8583073e4dbe4c4b25497bd44728edad5ea23625100d926fe1afcccae85c";

/// Secrets the Nova Deployment reads and cannot start without: the state store's URL
/// (created by CloudNative-PG), the object-store keys (created with the Lakehouse) and the
/// session key ([`ensure_nova_session`]). The workload waits for them instead of applying.
pub const NOVA_SECRETS: [&str; 3] = ["nova-pg-app", S3_SECRET, nova::SESSION_SECRET];

/// Nova: the ServiceAccount, the three Roles with their bindings, the Service and the
/// Deployment. `None` without the `lakehouse` pillar. Nothing is applied until every Secret in
/// [`NOVA_SECRETS`] exists; the identity contract, the OIDC client and the Trino CA are optional
/// for Nova, so the workload does not wait for them.
pub async fn ensure_nova(ctx: &Context, spec: &PlatformInstanceSpec) -> Option<ComponentStatus> {
    const NAME: &str = "nova";
    if !spec.pillars.lakehouse {
        return None;
    }
    let namespace = &spec.namespace;
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let mut missing = Vec::new();
    // With the `pipelines` pillar Nova also reads the service user's password (airflow::SECRETS).
    let mut required = NOVA_SECRETS.to_vec();
    if spec.pillars.pipelines {
        required.push(airflow::NOVA_API_SECRET);
    }
    for name in required {
        match secrets.get_opt(name).await {
            Ok(Some(_)) => {}
            Ok(None) => missing.push(name),
            Err(e) => return Some(status(NAME, false, e.to_string())),
        }
    }
    if !missing.is_empty() {
        return Some(status(
            NAME,
            false,
            format!("waiting for secrets: {}", missing.join(", ")),
        ));
    }
    let dry_run = spec.dry_run;
    let client = &ctx.client;
    if let Err(e) = apply::<ServiceAccount>(
        client,
        namespace,
        &nova::service_account(namespace),
        dry_run,
    )
    .await
    {
        return Some(status(NAME, false, e.to_string()));
    }
    for role in nova::roles(namespace) {
        if let Err(e) = apply::<Role>(client, namespace, &role, dry_run).await {
            return Some(status(NAME, false, e.to_string()));
        }
    }
    for binding in nova::role_bindings(namespace) {
        if let Err(e) = apply::<RoleBinding>(client, namespace, &binding, dry_run).await {
            return Some(status(NAME, false, e.to_string()));
        }
    }
    if let Err(e) = apply::<Service>(client, namespace, &nova::service(namespace), dry_run).await {
        return Some(status(NAME, false, e.to_string()));
    }
    let deployment = nova::deployment(
        namespace,
        NOVA_IMAGE,
        spec.base_domain.as_deref(),
        spec.pillars.pipelines,
    );
    let applied = match apply::<Deployment>(client, namespace, &deployment, dry_run).await {
        Ok(applied) => applied,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    if dry_run {
        return Some(status(
            NAME,
            false,
            format!("planned (dry-run): Deployment {}", nova::NAME),
        ));
    }
    if applied
        .status
        .as_ref()
        .and_then(|s| s.available_replicas)
        .unwrap_or(0)
        >= 1
    {
        Some(status(NAME, true, "running"))
    } else {
        Some(status(
            NAME,
            false,
            "waiting for the Deployment to become ready",
        ))
    }
}

/// The Ingress objects of the applications (`spec.ingress`): Nova on the base domain, and with
/// `identity.mode: platform` the identity provider on `id.<base domain>`. `None` unless
/// `spec.ingress.enabled`. Nova's Ingress is left out without the `lakehouse` pillar, because
/// Nova is not installed then. An Ingress has no readiness of its own: it is reported ready once
/// it is applied.
pub async fn ensure_ingress(ctx: &Context, spec: &PlatformInstanceSpec) -> Option<ComponentStatus> {
    const NAME: &str = "ingress";
    if !spec.ingress.enabled {
        return None;
    }
    let mut objects = Vec::new();
    if spec.pillars.lakehouse {
        objects.extend(ingress::nova(spec));
        objects.extend(ingress::trino(spec));
    }
    objects.extend(ingress::keycloak(spec));
    if spec.pillars.pipelines {
        objects.extend(ingress::airflow(spec));
    }
    if spec.pillars.notebooks {
        objects.extend(ingress::jupyterhub(spec));
    }
    if spec.pillars.dashboards {
        objects.extend(ingress::superset(spec));
    }
    if objects.is_empty() {
        return Some(status(
            NAME,
            false,
            "nothing to publish: ingress needs baseDomain and an application that has a host",
        ));
    }
    let namespace = &spec.namespace;
    let mut names = Vec::new();
    for object in &objects {
        if let Err(e) = apply::<Ingress>(&ctx.client, namespace, object, spec.dry_run).await {
            return Some(status(NAME, false, e.to_string()));
        }
        names.push(object.name_any());
    }
    if spec.dry_run {
        return Some(status(
            NAME,
            false,
            format!("planned (dry-run): Ingress {}", names.join(", ")),
        ));
    }
    Some(status(NAME, true, format!("applied: {}", names.join(", "))))
}

/// Publish the identity contract (ConfigMap `stardelt-identity`, decision D20) in the platform
/// namespace. `None` when sign-in is off: nothing is planned, nothing is applied, and an
/// existing ConfigMap written by the chart or the scripts is left alone.
pub async fn ensure_identity(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "identity";
    if spec.identity.mode == IdentityMode::None {
        return None;
    }
    let config_map = match identity::config_map(spec, &spec.namespace) {
        Ok(Some(config_map)) => config_map,
        Ok(None) => return None,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    if spec.dry_run {
        return Some(status(NAME, false, "planned (dry-run)"));
    }
    Some(
        match apply::<ConfigMap>(&ctx.client, &spec.namespace, &config_map, false).await {
            Ok(_) => status(
                NAME,
                true,
                format!("published ConfigMap {}", identity::CONFIGMAP_NAME),
            ),
            Err(e) => status(NAME, false, e.to_string()),
        },
    )
}

/// The secrets contract (decision D23): ConfigMap `stardelt-secrets` tells a namespace's
/// `Connection` objects which store to sync from. Published for every mode, `none` included, so
/// the value is never stale.
pub async fn ensure_secrets_mode(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    let mode = secrets::mode_label(spec.secrets.mode);
    Some(
        match apply::<ConfigMap>(
            &ctx.client,
            &spec.namespace,
            &secrets::mode_config_map(spec, &spec.namespace),
            spec.dry_run,
        )
        .await
        {
            Ok(_) if spec.dry_run => status(
                "secrets-mode",
                false,
                format!("planned (dry-run): stardelt-secrets (mode {mode})"),
            ),
            Ok(_) => status(
                "secrets-mode",
                true,
                format!("published ConfigMap stardelt-secrets (mode {mode})"),
            ),
            Err(e) => status("secrets-mode", false, e.to_string()),
        },
    )
}

/// The enterprise's store (decision D23, `secrets.mode: external`): one ClusterSecretStore.
/// It needs the External Secrets CRDs, so on a first reconcile it reports the 404 and the next
/// one succeeds — that is the documented order, not an error to retry inside the step.
pub async fn ensure_external_store(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "secret-store";
    let store = match secrets::external_store(spec) {
        Ok(None) => return None,
        Err(e) => return Some(status(NAME, false, e.to_string())),
        Ok(Some(s)) => s,
    };
    if spec.dry_run {
        return Some(status(NAME, false, "planned (dry-run)"));
    }
    let dynamic: kube::core::DynamicObject = match serde_json::from_value(store) {
        Ok(d) => d,
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    match apply_dynamic_cluster(
        &ctx.client,
        &secrets::cluster_secret_store_resource(),
        &dynamic,
        false,
    )
    .await
    {
        Err(kube::Error::Api(e)) if e.code == 404 => Some(status(
            NAME,
            false,
            "External Secrets Operator is not installed (make external-secrets)",
        )),
        Err(e) => Some(status(NAME, false, e.to_string())),
        Ok(_) => Some(status(
            NAME,
            true,
            "published ClusterSecretStore enterprise",
        )),
    }
}

/// One request to the store through the API server's service proxy. The token is a header and
/// never a log line; the caller decides what to do with the answer.
async fn store_request(
    ctx: &Context,
    namespace: &str,
    method: http::Method,
    path: &str,
    token: &str,
    body: Option<serde_json::Value>,
) -> Result<String, String> {
    let uri = secrets::store_proxy_path(namespace, path);
    let mut builder = http::Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Vault-Token", token);
    let request = if let Some(body) = body {
        builder = builder.header(http::header::CONTENT_TYPE, "application/json");
        builder
            .body(body.to_string().into_bytes())
            .map_err(|e| e.to_string())?
    } else {
        builder.body(Vec::new()).map_err(|e| e.to_string())?
    };
    ctx.client
        .request_text(request)
        .await
        .map_err(|e| e.to_string())
}

/// One secret store per owner group in `secrets.mode: platform` (SC4b, decision D27): the policy
/// and the auth role in the store, then the ServiceAccount and the `SecretStore` in the namespace.
/// Idempotent: writing a policy or a role twice sets the same value.
pub async fn ensure_group_stores(
    ctx: &Context,
    spec: &PlatformInstanceSpec,
) -> Option<ComponentStatus> {
    const NAME: &str = "group-stores";
    if spec.secrets.mode != SecretsMode::Platform || spec.secrets.groups.is_empty() {
        return None;
    }
    if spec.dry_run {
        return Some(status(NAME, false, "planned (dry-run)"));
    }

    let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &spec.namespace);
    let init_secret = match secrets_api.get_opt(secrets::INIT_SECRET).await {
        Ok(Some(secret)) => secret,
        Ok(None) => {
            return Some(status(
                NAME,
                false,
                "waiting for Secret openbao-init (the store is not initialised yet)",
            ));
        }
        Err(e) => return Some(status(NAME, false, e.to_string())),
    };
    let token = match init_secret
        .data
        .as_ref()
        .and_then(|data| data.get("root-token"))
        .and_then(|value| String::from_utf8(value.0.clone()).ok())
    {
        Some(token) if !token.is_empty() => token,
        _ => {
            return Some(status(NAME, false, "Secret openbao-init has no root-token"));
        }
    };

    // Enable the Kubernetes auth method once: absent in the sandbox until the operator does it.
    let auth = match store_request(
        ctx,
        &spec.namespace,
        http::Method::GET,
        "/v1/sys/auth",
        &token,
        None,
    )
    .await
    {
        Ok(body) => body,
        Err(e) => {
            return Some(status(
                NAME,
                false,
                format!("auth method check failed: {e}"),
            ));
        }
    };
    let kubernetes_enabled = serde_json::from_str::<serde_json::Value>(&auth)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .is_some_and(|map| map.contains_key("kubernetes/"));
    if !kubernetes_enabled {
        if let Err(e) = store_request(
            ctx,
            &spec.namespace,
            http::Method::POST,
            "/v1/sys/auth/kubernetes",
            &token,
            Some(serde_json::json!({"type": "kubernetes"})),
        )
        .await
        {
            return Some(status(
                NAME,
                false,
                format!("enabling the Kubernetes auth method failed: {e}"),
            ));
        }
        if let Err(e) = store_request(
            ctx,
            &spec.namespace,
            http::Method::POST,
            "/v1/auth/kubernetes/config",
            &token,
            Some(serde_json::json!({"kubernetes_host": "https://kubernetes.default.svc"})),
        )
        .await
        {
            return Some(status(
                NAME,
                false,
                format!("configuring the Kubernetes auth method failed: {e}"),
            ));
        }
    }

    let mut seen = std::collections::BTreeSet::new();
    for group in &spec.secrets.groups {
        if !seen.insert(group.as_str()) {
            continue;
        }
        let role = secrets::group_role_name(&spec.namespace, group);
        if let Err(e) = store_request(
            ctx,
            &spec.namespace,
            http::Method::PUT,
            &format!("/v1/sys/policies/acl/{role}"),
            &token,
            Some(serde_json::json!({"policy": secrets::group_policy(group)})),
        )
        .await
        {
            return Some(status(
                NAME,
                false,
                format!("writing policy {role} failed: {e}"),
            ));
        }
        if let Err(e) = store_request(
            ctx,
            &spec.namespace,
            http::Method::POST,
            &format!("/v1/auth/kubernetes/role/{role}"),
            &token,
            Some(serde_json::json!({
                "bound_service_account_names": [format!("eso-{group}")],
                "bound_service_account_namespaces": [&spec.namespace],
                "token_policies": [role],
                "token_ttl": "15m",
            })),
        )
        .await
        {
            return Some(status(
                NAME,
                false,
                format!("writing role {role} failed: {e}"),
            ));
        }
        let sa = match serde_json::from_value::<DynamicObject>(secrets::group_service_account(
            &spec.namespace,
            group,
        )) {
            Ok(object) => object,
            Err(e) => return Some(status(NAME, false, e.to_string())),
        };
        if let Err(e) = apply_dynamic(
            &ctx.client,
            &spec.namespace,
            &ApiResource::from_gvk_with_plural(
                &kube::core::GroupVersionKind::gvk("", "v1", "ServiceAccount"),
                "serviceaccounts",
            ),
            &sa,
            false,
        )
        .await
        {
            return Some(match e {
                kube::Error::Api(e) if e.code == 404 => status(
                    NAME,
                    false,
                    "External Secrets Operator is not installed (make external-secrets)",
                ),
                other => status(NAME, false, other.to_string()),
            });
        }
        let store = match serde_json::from_value::<DynamicObject>(secrets::group_secret_store(
            &spec.namespace,
            group,
        )) {
            Ok(object) => object,
            Err(e) => return Some(status(NAME, false, e.to_string())),
        };
        if let Err(e) = apply_dynamic(
            &ctx.client,
            &spec.namespace,
            &secrets::secret_store_resource(),
            &store,
            false,
        )
        .await
        {
            return Some(match e {
                kube::Error::Api(e) if e.code == 404 => status(
                    NAME,
                    false,
                    "External Secrets Operator is not installed (make external-secrets)",
                ),
                other => status(NAME, false, other.to_string()),
            });
        }
    }

    Some(status(
        NAME,
        true,
        format!("ready: {} group store(s)", spec.secrets.groups.len()),
    ))
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

    /// A kube client for the store: `GET …/secrets/openbao-init` is 404, `POST …/v1/sys/init`
    /// answers `init` with 200, every `PATCH` echoes its body. Requests are recorded.
    fn mock_store_client(init: &'static str) -> (kube::Client, Recorded) {
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
                let (status, document) = if method == "POST" {
                    (200, init.to_string())
                } else if method == "PATCH" {
                    (200, body)
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
        (kube::Client::new(service, "stardelt"), requests)
    }

    /// A kube client for group stores: the init Secret is present or 404, the Kubernetes auth
    /// method is not enabled yet, and every PATCH/PUT/POST to the API is accepted.
    fn mock_client_with_init(root_token: &str, init_present: bool) -> (kube::Client, Recorded) {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let root_token_b64 = serde_json::to_value(ByteString(root_token.as_bytes().to_vec()))
            .expect("serialize token")
            .as_str()
            .expect("base64 string")
            .to_string();
        let init_secret = format!(
            r#"{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"openbao-init"}},"data":{{"root-token":"{root_token_b64}","recovery-key":"cmVj"}}}}"#
        );
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let init_secret = init_secret.clone();
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
                } else if path.ends_with("/secrets/openbao-init") && method == "GET" {
                    if init_present {
                        (200, init_secret)
                    } else {
                        (
                            404,
                            r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","code":404,"message":"not found"}"#.to_string(),
                        )
                    }
                } else if path.ends_with("/proxy/v1/sys/auth") && method == "GET" {
                    (200, "{}".to_string())
                } else if method == "PUT" || method == "POST" {
                    (204, String::new())
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
        (kube::Client::new(service, "stardelt"), requests)
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

    fn platform(mode: &str, dry_run: bool) -> PlatformInstanceSpec {
        let instance: crate::crd::PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {
                "namespace": "stardelt",
                "baseDomain": "example.com",
                "dryRun": dry_run,
                "identity": { "mode": mode },
            }
        }))
        .expect("deserialize PlatformInstance");
        instance.spec
    }

    fn platform_store() -> PlatformInstanceSpec {
        let mut spec = platform("platform", false);
        spec.secrets.mode = SecretsMode::Platform;
        spec
    }

    #[tokio::test]
    async fn keycloak_pg_exists_only_for_mode_platform() {
        for mode in ["none", "external"] {
            let (client, requests) = mock_client(true);
            assert!(
                ensure_keycloak_pg(&context(client), &platform(mode, false))
                    .await
                    .is_none(),
                "{mode}"
            );
            assert!(
                requests.lock().expect("requests poisoned").is_empty(),
                "{mode} must not touch the cluster"
            );
        }
    }

    #[tokio::test]
    async fn keycloak_pg_dry_run_plans_only_the_cluster() {
        let (client, requests) = mock_client(true);
        let status = ensure_keycloak_pg(&context(client), &platform("platform", true))
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("planned (dry-run): Cluster keycloak-pg")
        );
        let requests = requests.lock().expect("requests poisoned");
        let paths: Vec<_> = requests
            .iter()
            .filter(|(method, _, _)| method == "PATCH")
            .map(|(_, path, _)| path.as_str())
            .collect();
        assert_eq!(
            paths,
            ["/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/keycloak-pg"]
        );
    }

    #[tokio::test]
    async fn keycloak_pg_applies_the_cluster_then_the_backup() {
        let (client, requests) = mock_client(true);
        let status = ensure_keycloak_pg(&context(client), &platform("platform", false))
            .await
            .expect("a status");
        assert_eq!(status.name, "keycloak-pg");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for the database to become Ready")
        );
        let requests = requests.lock().expect("requests poisoned");
        let patches: Vec<_> = requests
            .iter()
            .filter(|(method, _, _)| method == "PATCH")
            .collect();
        assert_eq!(patches.len(), 2, "cluster, then the schedule");
        assert_eq!(
            patches[0].1,
            "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/keycloak-pg"
        );
        assert_eq!(
            patches[1].1,
            "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/scheduledbackups/keycloak-pg-daily"
        );
        let backup: serde_json::Value = serde_json::from_str(&patches[1].2).expect("JSON body");
        assert_eq!(backup["kind"], "ScheduledBackup");
        assert_eq!(backup["spec"]["cluster"]["name"], "keycloak-pg");
    }

    #[tokio::test]
    async fn discovery_is_published_once_the_warehouse_is_bootstrapped() {
        let (client, requests) = mock_client(true);
        let status = ensure_discovery(&context(client), &lakehouse(), "data", true, false).await;
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
        let status = ensure_discovery(&context(client), &lakehouse(), "data", false, false).await;
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
        let status = ensure_discovery(&context(client), &lakehouse(), "data", true, false).await;
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

    /// A kube client for the Keycloak workload: `GET secrets/<name>` answers 404 for the names
    /// in `missing` and a Secret otherwise; a PATCH echoes the applied object, adding
    /// `status.availableReplicas` to the Deployment when `available_replicas` is set. Every
    /// request is recorded.
    fn keycloak_client(
        missing: &[&str],
        available_replicas: Option<i32>,
    ) -> (kube::Client, Recorded) {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let missing: Vec<String> = missing.iter().map(|name| name.to_string()).collect();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let missing = missing.clone();
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
                    match (available_replicas, path.contains("/deployments/")) {
                        (Some(replicas), true) => {
                            let mut applied: serde_json::Value =
                                serde_json::from_str(&body).expect("the applied Deployment");
                            applied["status"] =
                                serde_json::json!({ "availableReplicas": replicas });
                            (200, applied.to_string())
                        }
                        _ => (200, body),
                    }
                } else if missing.iter().any(|name| path.ends_with(name.as_str())) {
                    (
                        404,
                        r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","code":404,"message":"not found"}"#.to_string(),
                    )
                } else {
                    (
                        200,
                        r#"{"apiVersion":"v1","kind":"Secret","metadata":{"name":"present"}}"#
                            .to_string(),
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
        (kube::Client::new(service, "stardelt"), requests)
    }

    /// The paths of the server-side applies a request log holds, in order.
    fn applied_paths(requests: &Recorded) -> Vec<String> {
        requests
            .lock()
            .expect("requests poisoned")
            .iter()
            .filter(|(method, _, _)| method == "PATCH")
            .map(|(_, path, _)| path.clone())
            .collect()
    }

    #[tokio::test]
    async fn the_keycloak_workload_exists_only_for_mode_platform() {
        for mode in ["none", "external"] {
            let (client, requests) = keycloak_client(&[], None);
            assert!(
                ensure_keycloak(&context(client), &platform(mode, false))
                    .await
                    .is_none(),
                "{mode}"
            );
            assert!(
                requests.lock().expect("requests poisoned").is_empty(),
                "{mode} must not touch the cluster"
            );
        }
    }

    #[tokio::test]
    async fn keycloak_waits_for_the_identity_secrets_and_applies_nothing() {
        let (client, requests) =
            keycloak_client(&["stardelt-realm-admin", "stardelt-oidc-clients"], None);
        let status = ensure_keycloak(&context(client), &platform("platform", false))
            .await
            .expect("a status");
        assert_eq!(status.name, "keycloak");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for identity secrets: stardelt-realm-admin, stardelt-oidc-clients")
        );
        assert!(
            applied_paths(&requests).is_empty(),
            "no ConfigMap, Service or Deployment may be applied without the Secrets"
        );
    }

    #[tokio::test]
    async fn keycloak_dry_run_plans_the_config_maps_the_service_and_the_deployment() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_keycloak(&context(client), &platform("platform", true))
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("planned (dry-run): Deployment keycloak")
        );
        assert_eq!(
            applied_paths(&requests),
            [
                "/api/v1/namespaces/stardelt/configmaps/keycloak-realm",
                "/api/v1/namespaces/stardelt/configmaps/keycloak-env",
                "/api/v1/namespaces/stardelt/services/keycloak",
                "/apis/apps/v1/namespaces/stardelt/deployments/keycloak",
            ]
            .map(str::to_string)
        );
        // A server-side apply carries the type, so the API server validates a real object.
        let requests = requests.lock().expect("requests poisoned");
        let deployment = requests
            .iter()
            .find(|(_, path, _)| path.ends_with("/deployments/keycloak"))
            .expect("the Deployment");
        let body: serde_json::Value = serde_json::from_str(&deployment.2).expect("JSON body");
        assert_eq!(body["kind"], "Deployment");
        assert_eq!(
            body["spec"]["template"]["spec"]["containers"][0]["image"],
            "quay.io/keycloak/keycloak:26.8.1"
        );
    }

    #[tokio::test]
    async fn keycloak_applies_the_workload_and_reports_it_running() {
        let (client, requests) = keycloak_client(&[], Some(1));
        let status = ensure_keycloak(&context(client), &platform("platform", false))
            .await
            .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.message.as_deref(), Some("running"));
        assert_eq!(
            applied_paths(&requests).len(),
            4,
            "two ConfigMaps, the Service, then the Deployment"
        );
    }

    #[tokio::test]
    async fn keycloak_waits_for_its_deployment_to_become_ready() {
        let (client, _requests) = keycloak_client(&[], None);
        let status = ensure_keycloak(&context(client), &platform("platform", false))
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for the Deployment to become ready")
        );
    }

    /// A fake API server for the identity Secrets: GET answers from `store`, a server-side apply
    /// stores the object (as the API server would), a merge patch merges `stringData` into
    /// `data`, and a dry-run PATCH is validated but not persisted. Every request is recorded.
    type Store = Arc<Mutex<BTreeMap<String, serde_json::Value>>>;

    fn identity_client(
        initial: BTreeMap<String, serde_json::Value>,
    ) -> (kube::Client, Store, Recorded) {
        let store: Store = Arc::new(Mutex::new(initial));
        let recorder: Recorded = Arc::default();
        let service_store = store.clone();
        let service_recorder = recorder.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let store = service_store.clone();
            let recorder = service_recorder.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            let dry_run = request
                .uri()
                .query()
                .is_some_and(|query| query.contains("dryRun"));
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
                let name = path.rsplit('/').next().expect("a name").to_string();
                let (status, document) = if method == "GET" {
                    match store.lock().expect("store poisoned").get(&name) {
                        Some(object) => (200, object.to_string()),
                        None => (
                            404,
                            r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","code":404,"message":"not found"}"#
                                .to_string(),
                        ),
                    }
                } else if dry_run {
                    (200, body)
                } else {
                    let patch: serde_json::Value =
                        serde_json::from_str(&body).expect("a JSON body");
                    let mut store = store.lock().expect("store poisoned");
                    let inserted = name.clone();
                    let object = store.entry(name).or_insert_with(|| {
                        serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Secret",
                            "metadata": { "name": inserted },
                            "data": {},
                        })
                    });
                    match patch.get("stringData").and_then(|value| value.as_object()) {
                        Some(string_data) => {
                            let data = object["data"].as_object_mut().expect("data");
                            // The API server stores `stringData` base64-encoded under `data`.
                            for (key, value) in string_data {
                                data.insert(
                                    key.clone(),
                                    serde_json::Value::String(standard_base64(
                                        value.as_str().unwrap_or_default().as_bytes(),
                                    )),
                                );
                            }
                        }
                        None => *object = patch,
                    }
                    (200, object.to_string())
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
        (kube::Client::new(service, "stardelt"), store, recorder)
    }

    /// Standard base64 (RFC 4648 section 4) with padding, as the API server stores a Secret value.
    fn standard_base64(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let group = (u32::from(chunk[0]) << 16)
                | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
                | u32::from(*chunk.get(2).unwrap_or(&0));
            for position in 0..4 {
                if position <= chunk.len() {
                    out.push(char::from(
                        ALPHABET[((group >> (18 - 6 * position)) & 63) as usize],
                    ));
                } else {
                    out.push('=');
                }
            }
        }
        out
    }

    /// How many writes a request log holds.
    fn patch_count(requests: &Recorded) -> usize {
        requests
            .lock()
            .expect("requests poisoned")
            .iter()
            .filter(|(method, _, _)| method == "PATCH")
            .count()
    }

    /// The body of the first PATCH against Secret `name`.
    fn applied_secret(requests: &Recorded, name: &str) -> serde_json::Value {
        let suffix = format!("/secrets/{name}");
        requests
            .lock()
            .expect("requests poisoned")
            .iter()
            .find(|(method, path, _)| method == "PATCH" && path.ends_with(&suffix))
            .map(|(_, _, body)| serde_json::from_str(body).expect("a JSON body"))
            .expect("an applied Secret")
    }

    /// A base64 string for 48 bytes: 64 characters, no padding (`openssl rand -hex 24`).
    fn assert_base64_of_48_bytes(value: &str) {
        assert_eq!(value.len(), 64, "not 48 bytes of base64");
        assert!(
            value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'/'),
            "not base64"
        );
    }

    fn secret_object(name: &str, data: &[(&str, &str)]) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": name },
            "data": data
                .iter()
                .map(|(key, value)| {
                    ((*key).to_string(), serde_json::Value::String((*value).to_string()))
                })
                .collect::<serde_json::Map<_, _>>(),
        })
    }

    #[tokio::test]
    async fn identity_secrets_exist_only_for_mode_platform() {
        for mode in ["none", "external"] {
            let (client, _store, requests) = identity_client(BTreeMap::new());
            assert!(
                ensure_identity_secrets(&context(client), &platform(mode, false))
                    .await
                    .is_none(),
                "{mode}"
            );
            assert!(
                requests.lock().expect("requests poisoned").is_empty(),
                "{mode} must not touch the cluster"
            );
        }
    }

    #[tokio::test]
    async fn identity_secrets_are_created_with_the_script_keys_and_kept_on_a_second_reconcile() {
        let (client, store, requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("platform", false);
        let status = ensure_identity_secrets(&ctx, &spec)
            .await
            .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.name, "identity-secrets");
        assert_eq!(
            status.message.as_deref(),
            Some("ready: created 3 / kept 0"),
            "counts, never values"
        );
        assert_eq!(patch_count(&requests), 3, "one apply per Secret");

        let admin = applied_secret(&requests, "keycloak-admin");
        assert_eq!(admin["apiVersion"], "v1");
        assert_eq!(admin["kind"], "Secret");
        assert_eq!(admin["metadata"]["name"], "keycloak-admin");
        assert_eq!(
            admin["metadata"]["labels"]["app.kubernetes.io/part-of"],
            "stardelt"
        );
        assert!(
            admin["metadata"].get("ownerReferences").is_none(),
            "the realm owns the credentials"
        );
        assert_eq!(admin["data"]["username"], "YWRtaW4=", "base64 of admin");
        assert_base64_of_48_bytes(admin["data"]["password"].as_str().expect("a password"));

        let realm_admin = applied_secret(&requests, "stardelt-realm-admin");
        assert_eq!(realm_admin["data"]["username"], "YWRtaW4=");
        assert_base64_of_48_bytes(
            realm_admin["data"]["password"]
                .as_str()
                .expect("a password"),
        );

        let clients = applied_secret(&requests, "stardelt-oidc-clients");
        let data = clients["data"].as_object().expect("data");
        assert_eq!(
            data.keys().collect::<Vec<_>>(),
            [
                "airflow",
                "jupyterhub",
                "nova",
                "pipeline-runner",
                "superset",
                "trino",
                "trino-group-sync"
            ]
        );
        let mut credentials: Vec<&str> = Vec::new();
        for applied in [&admin, &realm_admin, &clients] {
            for (key, value) in applied["data"].as_object().expect("data") {
                if key != "username" {
                    credentials.push(value.as_str().expect("a base64 string"));
                }
            }
        }
        assert_eq!(credentials.len(), 9, "two passwords and seven clients");
        let unique: std::collections::BTreeSet<&str> = credentials.iter().copied().collect();
        assert_eq!(unique.len(), 9, "two credentials are identical");

        // The mock now serves the objects it stored; a second reconcile must change nothing.
        let before = store.lock().expect("store poisoned").clone();
        let status = ensure_identity_secrets(&ctx, &spec)
            .await
            .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.message.as_deref(), Some("ready: created 0 / kept 3"));
        assert_eq!(patch_count(&requests), 3, "a reconcile may not write");
        assert_eq!(
            *store.lock().expect("store poisoned"),
            before,
            "a credential was rewritten"
        );
    }

    #[tokio::test]
    async fn an_existing_identity_secret_gets_only_its_missing_keys() {
        let initial = BTreeMap::from([
            (
                "keycloak-admin".to_string(),
                secret_object(
                    "keycloak-admin",
                    &[
                        ("username", "YWRtaW4="),
                        ("password", "b2xkLWtleWNsb2FrLXBhc3N3b3Jk"),
                    ],
                ),
            ),
            (
                "stardelt-realm-admin".to_string(),
                secret_object(
                    "stardelt-realm-admin",
                    &[
                        ("username", "YWRtaW4="),
                        ("password", "b2xkLXJlYWxtLXBhc3N3b3Jk"),
                    ],
                ),
            ),
            (
                "stardelt-oidc-clients".to_string(),
                secret_object(
                    "stardelt-oidc-clients",
                    &[
                        ("nova", "b2xkLW5vdmE="),
                        ("superset", "b2xkLXN1cGVyc2V0"),
                        ("airflow", "b2xkLWFpcmZsb3c="),
                        ("jupyterhub", "b2xkLWp1cHl0ZXJodWI="),
                    ],
                ),
            ),
        ]);
        let (client, store, requests) = identity_client(initial);
        let status = ensure_identity_secrets(&context(client), &platform("platform", false))
            .await
            .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.message.as_deref(), Some("ready: created 0 / kept 3"));
        assert_eq!(
            patch_count(&requests),
            1,
            "only the Secret with missing keys"
        );

        let patch = applied_secret(&requests, "stardelt-oidc-clients");
        let added = patch["stringData"].as_object().expect("stringData");
        assert_eq!(
            added.keys().collect::<Vec<_>>(),
            ["pipeline-runner", "trino", "trino-group-sync"]
        );
        for value in added.values() {
            let value = value.as_str().expect("a plain value");
            assert_eq!(value.len(), 48);
            assert!(
                value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
                "not lower-case hex"
            );
        }

        // Every key the Secret already had kept its value.
        let store = store.lock().expect("store poisoned");
        let data = store["stardelt-oidc-clients"]["data"]
            .as_object()
            .expect("data");
        for (key, value) in [
            ("nova", "b2xkLW5vdmE="),
            ("superset", "b2xkLXN1cGVyc2V0"),
            ("airflow", "b2xkLWFpcmZsb3c="),
            ("jupyterhub", "b2xkLWp1cHl0ZXJodWI="),
        ] {
            assert_eq!(data[key], value, "{key} was rewritten");
        }
        assert_eq!(
            store["keycloak-admin"]["data"]["password"],
            "b2xkLWtleWNsb2FrLXBhc3N3b3Jk"
        );
        assert_eq!(
            store["stardelt-realm-admin"]["data"]["password"],
            "b2xkLXJlYWxtLXBhc3N3b3Jk"
        );
    }

    #[tokio::test]
    async fn identity_secrets_dry_run_writes_nothing() {
        let (client, store, requests) = identity_client(BTreeMap::new());
        let status = ensure_identity_secrets(&context(client), &platform("platform", true))
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(status.message.as_deref(), Some("planned (dry-run)"));
        assert_eq!(patch_count(&requests), 3);
        assert!(
            store.lock().expect("store poisoned").is_empty(),
            "dry-run persisted a Secret"
        );
    }

    /// A platform spec with the `lakehouse` pillar switched on or off.
    fn platform_with_lakehouse(lakehouse: bool, dry_run: bool) -> PlatformInstanceSpec {
        let instance: crate::crd::PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {
                "namespace": "stardelt",
                "dryRun": dry_run,
                "pillars": { "lakehouse": lakehouse },
            }
        }))
        .expect("deserialize PlatformInstance");
        instance.spec
    }

    #[tokio::test]
    async fn nova_pg_exists_only_with_the_lakehouse_pillar() {
        let (client, requests) = mock_client(true);
        assert!(
            ensure_nova_pg(&context(client), &platform_with_lakehouse(false, false))
                .await
                .is_none()
        );
        assert!(
            requests.lock().expect("requests poisoned").is_empty(),
            "without the pillar the cluster must not be touched"
        );
    }

    #[tokio::test]
    async fn nova_pg_dry_run_plans_only_the_cluster() {
        let (client, requests) = mock_client(true);
        let status = ensure_nova_pg(&context(client), &platform_with_lakehouse(true, true))
            .await
            .expect("a status");
        assert_eq!(status.name, "nova-pg");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("planned (dry-run): Cluster nova-pg")
        );
        assert_eq!(
            applied_paths(&requests),
            ["/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/nova-pg"]
                .map(str::to_string)
        );
    }

    #[tokio::test]
    async fn nova_pg_applies_the_cluster_then_the_backup() {
        let (client, requests) = mock_client(true);
        let status = ensure_nova_pg(&context(client), &platform_with_lakehouse(true, false))
            .await
            .expect("a status");
        assert_eq!(status.name, "nova-pg");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for the database to become Ready")
        );
        assert_eq!(
            applied_paths(&requests),
            [
                "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/nova-pg",
                "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/scheduledbackups/nova-pg-daily",
            ]
            .map(str::to_string)
        );
    }

    #[tokio::test]
    async fn the_nova_session_key_exists_only_with_the_lakehouse_pillar() {
        let (client, _store, requests) = identity_client(BTreeMap::new());
        assert!(
            ensure_nova_session(&context(client), &platform_with_lakehouse(false, false))
                .await
                .is_none()
        );
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn the_nova_session_key_is_generated_once_and_kept() {
        let (client, store, requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform_with_lakehouse(true, false);
        let status = ensure_nova_session(&ctx, &spec).await.expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.name, "nova-session");
        assert_eq!(status.message.as_deref(), Some("ready: created 1 / kept 0"));
        assert_eq!(patch_count(&requests), 1);
        let secret = applied_secret(&requests, "stardelt-nova-session");
        assert_eq!(secret["kind"], "Secret");
        assert_eq!(
            secret["metadata"]["labels"]["app.kubernetes.io/part-of"],
            "stardelt"
        );
        let data = secret["data"].as_object().expect("data");
        assert_eq!(data.keys().collect::<Vec<_>>(), ["session"]);
        assert_base64_of_48_bytes(data["session"].as_str().expect("a key"));

        // The second reconcile reads the stored Secret and writes nothing.
        let before = store.lock().expect("store poisoned").clone();
        let status = ensure_nova_session(&ctx, &spec).await.expect("a status");
        assert_eq!(status.message.as_deref(), Some("ready: created 0 / kept 1"));
        assert_eq!(patch_count(&requests), 1, "a reconcile may not write");
        assert_eq!(
            *store.lock().expect("store poisoned"),
            before,
            "the session key was rewritten"
        );
    }

    #[tokio::test]
    async fn the_nova_session_key_dry_run_writes_nothing() {
        let (client, store, requests) = identity_client(BTreeMap::new());
        let status = ensure_nova_session(&context(client), &platform_with_lakehouse(true, true))
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(status.message.as_deref(), Some("planned (dry-run)"));
        assert_eq!(patch_count(&requests), 1);
        assert!(store.lock().expect("store poisoned").is_empty());
    }

    #[tokio::test]
    async fn the_query_log_token_is_generated_once_and_kept() {
        let (client, store, requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform_with_lakehouse(true, false);
        let status = ensure_nova_events(&ctx, &spec).await.expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.name, "nova-events");
        assert_eq!(status.message.as_deref(), Some("ready: created 1 / kept 0"));
        assert_eq!(patch_count(&requests), 1);
        let secret = applied_secret(&requests, "nova-events");
        assert_eq!(secret["kind"], "Secret");
        assert_eq!(
            secret["metadata"]["labels"]["app.kubernetes.io/part-of"],
            "stardelt"
        );
        let data = secret["data"].as_object().expect("data");
        assert_eq!(data.keys().collect::<Vec<_>>(), ["token"]);
        assert_base64_of_48_bytes(data["token"].as_str().expect("a token"));

        // The second reconcile reads the stored Secret and writes nothing.
        let before = store.lock().expect("store poisoned").clone();
        let status = ensure_nova_events(&ctx, &spec).await.expect("a status");
        assert_eq!(status.message.as_deref(), Some("ready: created 0 / kept 1"));
        assert_eq!(patch_count(&requests), 1, "a reconcile may not write");
        assert_eq!(
            *store.lock().expect("store poisoned"),
            before,
            "the token was rewritten"
        );
    }

    #[tokio::test]
    async fn nova_exists_only_with_the_lakehouse_pillar() {
        let (client, requests) = keycloak_client(&[], Some(1));
        assert!(
            ensure_nova(&context(client), &platform_with_lakehouse(false, false))
                .await
                .is_none()
        );
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn nova_waits_for_its_secrets_and_applies_nothing() {
        let (client, requests) = keycloak_client(&["nova-pg-app", "stardelt-nova-session"], None);
        let status = ensure_nova(&context(client), &platform_with_lakehouse(true, false))
            .await
            .expect("a status");
        assert_eq!(status.name, "nova");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for secrets: nova-pg-app, stardelt-nova-session")
        );
        assert!(
            applied_paths(&requests).is_empty(),
            "nothing may be applied without the Secrets"
        );
    }

    #[tokio::test]
    async fn nova_dry_run_plans_every_object_in_order() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_nova(&context(client), &platform_with_lakehouse(true, true))
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("planned (dry-run): Deployment nova")
        );
        let rbac = "/apis/rbac.authorization.k8s.io/v1/namespaces/stardelt";
        assert_eq!(
            applied_paths(&requests),
            [
                "/api/v1/namespaces/stardelt/serviceaccounts/nova".to_string(),
                format!("{rbac}/roles/nova-workload-reader"),
                format!("{rbac}/roles/nova-connections"),
                format!("{rbac}/roles/nova-pipelines"),
                format!("{rbac}/rolebindings/nova-workload-reader"),
                format!("{rbac}/rolebindings/nova-connections"),
                format!("{rbac}/rolebindings/nova-pipelines"),
                "/api/v1/namespaces/stardelt/services/nova".to_string(),
                "/apis/apps/v1/namespaces/stardelt/deployments/nova".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn nova_runs_the_pinned_image_and_reports_it_running() {
        let (client, requests) = keycloak_client(&[], Some(1));
        let status = ensure_nova(&context(client), &platform_with_lakehouse(true, false))
            .await
            .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.message.as_deref(), Some("running"));
        let requests = requests.lock().expect("requests poisoned");
        let deployment = requests
            .iter()
            .find(|(_, path, _)| path.ends_with("/deployments/nova"))
            .expect("the Deployment");
        let body: serde_json::Value = serde_json::from_str(&deployment.2).expect("JSON body");
        assert_eq!(body["kind"], "Deployment");
        let image = body["spec"]["template"]["spec"]["containers"][0]["image"]
            .as_str()
            .expect("an image");
        assert_eq!(image, NOVA_IMAGE);
        assert!(
            image.contains("@sha256:"),
            "the image is pinned by digest: {image}"
        );
    }

    #[tokio::test]
    async fn nova_waits_for_its_deployment_to_become_ready() {
        let (client, _requests) = keycloak_client(&[], None);
        let status = ensure_nova(&context(client), &platform_with_lakehouse(true, false))
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for the Deployment to become ready")
        );
    }

    #[tokio::test]
    async fn nova_links_to_the_applications_under_the_base_domain() {
        let (client, requests) = keycloak_client(&[], Some(1));
        ensure_nova(&context(client), &platform("none", false))
            .await
            .expect("a status");
        let requests = requests.lock().expect("requests poisoned");
        let deployment = requests
            .iter()
            .find(|(_, path, _)| path.ends_with("/deployments/nova"))
            .expect("the Deployment");
        let body: serde_json::Value = serde_json::from_str(&deployment.2).expect("JSON body");
        let env = body["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_array()
            .expect("env");
        let domain = env
            .iter()
            .find(|var| var["name"] == "NOVA_BASE_DOMAIN")
            .expect("NOVA_BASE_DOMAIN");
        assert_eq!(domain["value"], "example.com");
    }

    #[tokio::test]
    async fn nova_reads_the_jupyterhub_service_token_and_the_same_idle_timeout_as_the_culler() {
        let (client, requests) = keycloak_client(&[], Some(1));
        ensure_nova(&context(client), &platform("none", false))
            .await
            .expect("a status");
        let requests = requests.lock().expect("requests poisoned");
        let deployment = requests
            .iter()
            .find(|(_, path, _)| path.ends_with("/deployments/nova"))
            .expect("the Deployment");
        let body: serde_json::Value = serde_json::from_str(&deployment.2).expect("JSON body");
        let env = body["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_array()
            .expect("env");
        let names: Vec<&str> = env.iter().filter_map(|var| var["name"].as_str()).collect();
        assert!(names.contains(&"NOVA_JUPYTERHUB_API_URL"), "{names:?}");
        assert!(names.contains(&"NOVA_JUPYTERHUB_API_TOKEN"), "{names:?}");
        assert!(names.contains(&"NOVA_WORKSPACE_IDLE_SECONDS"), "{names:?}");

        let token = env
            .iter()
            .find(|var| var["name"] == "NOVA_JUPYTERHUB_API_TOKEN")
            .expect("NOVA_JUPYTERHUB_API_TOKEN");
        let source = &token["valueFrom"]["secretKeyRef"];
        assert_eq!(source["name"], "hub");
        assert_eq!(source["key"], "hub.services.nova.apiToken");
        assert_eq!(source["optional"], true);

        // The countdown equals the culler: the hub's `cull.timeout` is what stops an
        // idle server, so the two numbers must not drift apart.
        let golden: serde_json::Value =
            serde_yaml::from_str(include_str!("../values/jupyterhub.yaml")).expect("golden values");
        let cull = golden["cull"]["timeout"].as_i64().expect("cull.timeout");
        let idle = env
            .iter()
            .find(|var| var["name"] == "NOVA_WORKSPACE_IDLE_SECONDS")
            .expect("NOVA_WORKSPACE_IDLE_SECONDS");
        assert_eq!(
            idle["value"],
            cull.to_string().as_str(),
            "Nova's countdown must equal the culler"
        );
    }

    /// A platform spec with ingress switched on, the given identity mode and lakehouse pillar.
    fn platform_with_ingress(mode: &str, lakehouse: bool, dry_run: bool) -> PlatformInstanceSpec {
        let instance: crate::crd::PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {
                "namespace": "stardelt",
                "baseDomain": "example.com",
                "dryRun": dry_run,
                "identity": { "mode": mode },
                "pillars": { "lakehouse": lakehouse },
                "ingress": { "enabled": true, "className": "traefik" },
            }
        }))
        .expect("deserialize PlatformInstance");
        instance.spec
    }

    const INGRESSES: &str = "/apis/networking.k8s.io/v1/namespaces/stardelt/ingresses";

    #[tokio::test]
    async fn ingress_exists_only_when_it_is_enabled() {
        let (client, requests) = keycloak_client(&[], None);
        assert!(
            ensure_ingress(&context(client), &platform("platform", false))
                .await
                .is_none()
        );
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn ingress_dry_run_plans_nova_and_the_identity_provider() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_ingress(
            &context(client),
            &platform_with_ingress("platform", true, true),
        )
        .await
        .expect("a status");
        assert_eq!(status.name, "ingress");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("planned (dry-run): Ingress nova, trino, keycloak")
        );
        assert_eq!(
            applied_paths(&requests),
            [
                format!("{INGRESSES}/nova"),
                format!("{INGRESSES}/trino"),
                format!("{INGRESSES}/keycloak")
            ]
        );
    }

    #[tokio::test]
    async fn ingress_applies_the_objects_with_the_hosts_and_the_class() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_ingress(
            &context(client),
            &platform_with_ingress("platform", true, false),
        )
        .await
        .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(
            status.message.as_deref(),
            Some("applied: nova, trino, keycloak")
        );
        let requests = requests.lock().expect("requests poisoned");
        let body = |name: &str| -> serde_json::Value {
            let found = requests
                .iter()
                .find(|(method, path, _)| {
                    method == "PATCH" && path.ends_with(&format!("/ingresses/{name}"))
                })
                .expect("an applied Ingress");
            serde_json::from_str(&found.2).expect("JSON body")
        };
        let nova = body("nova");
        assert_eq!(nova["kind"], "Ingress");
        assert_eq!(nova["spec"]["ingressClassName"], "traefik");
        assert_eq!(nova["spec"]["rules"][0]["host"], "example.com");
        let keycloak = body("keycloak");
        assert_eq!(keycloak["spec"]["rules"][0]["host"], "id.example.com");
    }

    #[tokio::test]
    async fn without_the_lakehouse_pillar_nova_gets_no_ingress() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_ingress(
            &context(client),
            &platform_with_ingress("platform", false, false),
        )
        .await
        .expect("a status");
        assert_eq!(status.message.as_deref(), Some("applied: keycloak"));
        assert_eq!(applied_paths(&requests), [format!("{INGRESSES}/keycloak")]);
    }

    #[tokio::test]
    async fn without_platform_identity_only_nova_gets_an_ingress() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_ingress(
            &context(client),
            &platform_with_ingress("none", true, false),
        )
        .await
        .expect("a status");
        assert_eq!(status.message.as_deref(), Some("applied: nova"));
        assert_eq!(applied_paths(&requests), [format!("{INGRESSES}/nova")]);
    }

    #[tokio::test]
    async fn ingress_with_nothing_to_publish_says_so_and_applies_nothing() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_ingress(
            &context(client),
            &platform_with_ingress("none", false, false),
        )
        .await
        .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("nothing to publish: ingress needs baseDomain and an application that has a host")
        );
        assert!(applied_paths(&requests).is_empty());
    }

    #[tokio::test]
    async fn ingress_publishes_airflow_and_jupyterhub_with_their_pillars() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_ingress(&context(client), &platform_with_apps(true, true, false))
            .await
            .expect("a status");
        assert_eq!(
            status.message.as_deref(),
            Some("applied: nova, airflow, jupyterhub")
        );
        assert_eq!(
            applied_paths(&requests),
            vec![
                "/apis/networking.k8s.io/v1/namespaces/stardelt/ingresses/nova".to_string(),
                "/apis/networking.k8s.io/v1/namespaces/stardelt/ingresses/airflow".to_string(),
                "/apis/networking.k8s.io/v1/namespaces/stardelt/ingresses/jupyterhub".to_string(),
            ]
        );
    }

    fn platform_with_dashboards_ingress(dashboards: bool, dry_run: bool) -> PlatformInstanceSpec {
        let instance: crate::crd::PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {
                "namespace": "stardelt",
                "baseDomain": "example.com",
                "dryRun": dry_run,
                "identity": { "mode": "none" },
                "pillars": { "lakehouse": true, "dashboards": dashboards },
                "ingress": { "enabled": true, "className": "traefik" },
            }
        }))
        .expect("deserialize PlatformInstance");
        instance.spec
    }

    #[tokio::test]
    async fn ingress_publishes_superset_with_the_dashboards_pillar() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_ingress(
            &context(client),
            &platform_with_dashboards_ingress(true, false),
        )
        .await
        .expect("a status");
        assert_eq!(status.message.as_deref(), Some("applied: nova, superset"));
        assert_eq!(
            applied_paths(&requests),
            vec![format!("{INGRESSES}/nova"), format!("{INGRESSES}/superset"),]
        );
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_ingress(
            &context(client),
            &platform_with_dashboards_ingress(false, false),
        )
        .await
        .expect("a status");
        assert_eq!(status.message.as_deref(), Some("applied: nova"));
        assert_eq!(applied_paths(&requests), vec![format!("{INGRESSES}/nova")]);
    }

    #[tokio::test]
    async fn the_airflow_secrets_are_created_once_and_the_fernet_key_has_its_format() {
        let (client, store, requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("none", false);
        let status =
            ensure_generated_secrets(&ctx, &spec, "airflow-secrets", &crate::airflow::SECRETS)
                .await;
        assert!(status.ready, "{status:?}");
        assert_eq!(status.name, "airflow-secrets");
        assert_eq!(status.message.as_deref(), Some("ready: created 5 / kept 0"));
        assert_eq!(patch_count(&requests), 5);
        let fernet = applied_secret(&requests, "airflow-fernet-key");
        let value = fernet["data"]["fernet-key"].as_str().expect("a key");
        // 44 characters of url-safe base64, stored base64 again: 60 characters, one padding sign.
        assert_eq!(value.len(), 60);
        assert!(value.ends_with('='));
        let webserver = applied_secret(&requests, "airflow-webserver-key");
        assert_base64_of_48_bytes(
            webserver["data"]["webserver-secret-key"]
                .as_str()
                .expect("a key"),
        );
        assert_base64_of_48_bytes(webserver["data"]["api-secret-key"].as_str().expect("a key"));
        let nova_api = applied_secret(&requests, "airflow-nova-api");
        assert_eq!(
            nova_api["data"]["username"], "bm92YS1hcGk=",
            "base64 of nova-api"
        );

        // A second reconcile reads the stored Secrets and rewrites nothing.
        let before = store.lock().expect("store poisoned").clone();
        let again =
            ensure_generated_secrets(&ctx, &spec, "airflow-secrets", &crate::airflow::SECRETS)
                .await;
        assert_eq!(again.message.as_deref(), Some("ready: created 0 / kept 5"));
        assert_eq!(patch_count(&requests), 5, "a reconcile may not write");
        assert_eq!(*store.lock().expect("store poisoned"), before);
    }

    #[tokio::test]
    async fn an_airflow_secret_that_lacks_its_fernet_key_gets_one_and_keeps_its_other_keys() {
        let initial = BTreeMap::from([(
            "airflow-fernet-key".to_string(),
            secret_object("airflow-fernet-key", &[("other", "b2xk")]),
        )]);
        let (client, store, _requests) = identity_client(initial);
        let status = ensure_generated_secrets(
            &context(client),
            &platform("none", false),
            "airflow-secrets",
            &crate::airflow::SECRETS[..1],
        )
        .await;
        assert!(status.ready, "{status:?}");
        let store = store.lock().expect("store poisoned");
        let data = store["airflow-fernet-key"]["data"]
            .as_object()
            .expect("data");
        assert_eq!(data["other"], "b2xk", "an existing key is untouched");
        assert_eq!(data["fernet-key"].as_str().expect("a key").len(), 60);
    }

    fn platform_with_pipelines(pipelines: bool, dry_run: bool) -> PlatformInstanceSpec {
        let instance: crate::crd::PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {
                "namespace": "stardelt",
                "dryRun": dry_run,
                "pillars": { "pipelines": pipelines },
            }
        }))
        .expect("deserialize PlatformInstance");
        instance.spec
    }

    fn platform_with_dashboards(dashboards: bool, dry_run: bool) -> PlatformInstanceSpec {
        let instance: crate::crd::PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {
                "namespace": "stardelt",
                "dryRun": dry_run,
                "pillars": { "dashboards": dashboards },
            }
        }))
        .expect("deserialize PlatformInstance");
        instance.spec
    }

    fn platform_with_apps(pipelines: bool, notebooks: bool, dry_run: bool) -> PlatformInstanceSpec {
        let instance: crate::crd::PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster" },
            "spec": {
                "namespace": "stardelt",
                "baseDomain": "example.com",
                "dryRun": dry_run,
                "identity": { "mode": "none" },
                "pillars": { "lakehouse": true, "pipelines": pipelines, "notebooks": notebooks },
                "ingress": { "enabled": true, "className": "traefik" },
            }
        }))
        .expect("deserialize PlatformInstance");
        instance.spec
    }

    #[tokio::test]
    async fn the_airflow_steps_exist_only_with_the_pipelines_pillar() {
        let (client, _store, requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform_with_pipelines(false, false);
        assert!(ensure_airflow_secrets(&ctx, &spec).await.is_none());
        assert!(ensure_airflow_rbac(&ctx, &spec).await.is_none());
        assert!(ensure_airflow_group_roles(&ctx, &spec).await.is_none());
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn airflow_secrets_are_created_for_the_pipelines_pillar() {
        let (client, _store, requests) = identity_client(BTreeMap::new());
        let status =
            ensure_airflow_secrets(&context(client), &platform_with_pipelines(true, false))
                .await
                .expect("a status");
        assert_eq!(status.name, "airflow-secrets");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.message.as_deref(), Some("ready: created 5 / kept 0"));
        assert_eq!(patch_count(&requests), 5);
    }

    #[tokio::test]
    async fn superset_secrets_exist_only_with_the_dashboards_pillar() {
        let (client, _store, requests) = identity_client(BTreeMap::new());
        assert!(
            ensure_superset_secrets(&context(client), &platform_with_dashboards(false, false))
                .await
                .is_none()
        );
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn superset_secrets_are_created_for_the_dashboards_pillar() {
        let (client, _store, requests) = identity_client(BTreeMap::new());
        let status =
            ensure_superset_secrets(&context(client), &platform_with_dashboards(true, false))
                .await
                .expect("a status");
        assert_eq!(status.name, "superset-secrets");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.message.as_deref(), Some("ready: created 2 / kept 0"));
        assert_eq!(patch_count(&requests), 2);
    }

    #[tokio::test]
    async fn pg_dumps_follow_the_two_pillars() {
        let (client, _requests) = keycloak_client(&[], None);
        let ctx = context(client);
        assert!(
            ensure_pg_dumps(&ctx, &platform_with_dashboards(false, false))
                .await
                .is_none()
        );
        let (client, requests) = keycloak_client(&[], None);
        let mut spec = platform_with_apps(false, false, false);
        spec.pillars.dashboards = true;
        let status = ensure_pg_dumps(&context(client), &spec)
            .await
            .expect("a status");
        assert_eq!(status.message.as_deref(), Some("applied: superset-pg-dump"));
        assert!(status.ready, "{status:?}");
        assert_eq!(
            applied_paths(&requests),
            vec!["/apis/batch/v1/namespaces/stardelt/cronjobs/superset-pg-dump".to_string()]
        );
        let (client, requests) = keycloak_client(&[], None);
        let mut spec = platform_with_apps(true, false, false);
        spec.pillars.dashboards = true;
        let status = ensure_pg_dumps(&context(client), &spec)
            .await
            .expect("a status");
        assert_eq!(
            status.message.as_deref(),
            Some("applied: superset-pg-dump, airflow-pg-dump")
        );
        assert!(status.ready, "{status:?}");
        assert_eq!(
            applied_paths(&requests),
            vec![
                "/apis/batch/v1/namespaces/stardelt/cronjobs/superset-pg-dump".to_string(),
                "/apis/batch/v1/namespaces/stardelt/cronjobs/airflow-pg-dump".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn pg_dumps_dry_run_applies_with_dry_run_and_writes_nothing() {
        let (client, _requests) = keycloak_client(&[], None);
        let mut spec = platform_with_apps(true, false, true);
        spec.pillars.dashboards = true;
        let status = ensure_pg_dumps(&context(client), &spec)
            .await
            .expect("a status");
        assert!(
            status
                .message
                .as_deref()
                .unwrap()
                .starts_with("planned (dry-run):")
        );
        assert!(!status.ready);
    }

    #[tokio::test]
    async fn superset_rbac_and_group_roles_exist_only_with_the_dashboards_pillar() {
        let (client, _store, requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform_with_dashboards(false, false);
        assert!(ensure_superset_rbac(&ctx, &spec).await.is_none());
        assert!(ensure_superset_group_roles(&ctx, &spec).await.is_none());
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn superset_rbac_applies_the_service_account_role_and_binding() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_superset_rbac(&context(client), &platform_with_dashboards(true, false))
            .await
            .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.name, "superset-rbac");
        assert_eq!(status.message.as_deref(), Some("applied: superset-rbac"));
        let core = "/api/v1/namespaces/stardelt";
        let rbac = "/apis/rbac.authorization.k8s.io/v1/namespaces/stardelt";
        assert_eq!(
            applied_paths(&requests),
            [
                format!("{core}/serviceaccounts/superset"),
                format!("{rbac}/roles/superset-group-roles"),
                format!("{rbac}/rolebindings/superset-group-roles"),
            ]
        );
    }

    #[tokio::test]
    async fn superset_group_roles_applies_the_config_map_and_cron_job() {
        let (client, requests) = keycloak_client(&[], None);
        let status =
            ensure_superset_group_roles(&context(client), &platform_with_dashboards(true, false))
                .await
                .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(
            status.message.as_deref(),
            Some("applied: superset-group-roles")
        );
        let core = "/api/v1/namespaces/stardelt";
        let batch = "/apis/batch/v1/namespaces/stardelt";
        assert_eq!(
            applied_paths(&requests),
            [
                format!("{core}/configmaps/superset-group-roles"),
                format!("{batch}/cronjobs/superset-group-roles"),
            ]
        );
    }

    #[tokio::test]
    async fn superset_group_roles_dry_run_says_planned_and_is_not_ready() {
        let (client, _requests) = keycloak_client(&[], None);
        let status =
            ensure_superset_group_roles(&context(client), &platform_with_dashboards(true, true))
                .await
                .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("planned (dry-run): ConfigMap and CronJob superset-group-roles")
        );
    }

    #[tokio::test]
    async fn airflow_rbac_applies_the_role_then_its_binding() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_airflow_rbac(&context(client), &platform_with_pipelines(true, false))
            .await
            .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.name, "airflow-rbac");
        assert_eq!(
            status.message.as_deref(),
            Some("applied: airflow-pipeline-reader")
        );
        let rbac = "/apis/rbac.authorization.k8s.io/v1/namespaces/stardelt";
        assert_eq!(
            applied_paths(&requests),
            [
                format!("{rbac}/roles/airflow-pipeline-reader"),
                format!("{rbac}/rolebindings/airflow-pipeline-reader"),
            ]
        );
    }

    #[tokio::test]
    async fn airflow_rbac_dry_run_says_planned_and_is_not_ready() {
        let (client, _requests) = keycloak_client(&[], None);
        let status = ensure_airflow_rbac(&context(client), &platform_with_pipelines(true, true))
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("planned (dry-run): Role and RoleBinding airflow-pipeline-reader")
        );
    }

    #[tokio::test]
    async fn airflow_group_roles_applies_the_service_account_config_map_binding_and_job() {
        let (client, requests) = keycloak_client(&[], None);
        let status =
            ensure_airflow_group_roles(&context(client), &platform_with_pipelines(true, false))
                .await
                .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(
            status.message.as_deref(),
            Some("applied: airflow-group-roles")
        );
        assert_eq!(
            applied_paths(&requests),
            [
                "/api/v1/namespaces/stardelt/serviceaccounts/airflow-group-roles",
                "/api/v1/namespaces/stardelt/configmaps/airflow-group-roles",
                "/apis/rbac.authorization.k8s.io/v1/namespaces/stardelt/rolebindings/airflow-group-roles",
                "/apis/batch/v1/namespaces/stardelt/cronjobs/airflow-group-roles",
            ]
        );
    }

    #[tokio::test]
    async fn airflow_group_roles_dry_run_says_planned_and_is_not_ready() {
        let (client, _requests) = keycloak_client(&[], None);
        let status =
            ensure_airflow_group_roles(&context(client), &platform_with_pipelines(true, true))
                .await
                .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some(
                "planned (dry-run): ServiceAccount, ConfigMap, RoleBinding and CronJob airflow-group-roles"
            )
        );
    }

    #[tokio::test]
    async fn nova_waits_for_the_airflow_service_user_password_only_with_the_pipelines_pillar() {
        // Everything else exists; only the Airflow password Secret is missing.
        let (client, requests) = keycloak_client(&["airflow-nova-api"], None);
        let with = platform_with_pipelines(true, false);
        let status = ensure_nova(&context(client), &with)
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for secrets: airflow-nova-api")
        );
        assert!(applied_paths(&requests).is_empty());

        let (client, requests) = keycloak_client(&["airflow-nova-api"], Some(1));
        let without = platform_with_lakehouse(true, false);
        let status = ensure_nova(&context(client), &without)
            .await
            .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(applied_paths(&requests).len(), 9);
    }

    #[tokio::test]
    async fn nova_points_at_airflow_when_the_pipelines_pillar_is_on() {
        let (client, requests) = keycloak_client(&[], Some(1));
        let status = ensure_nova(&context(client), &platform_with_pipelines(true, false))
            .await
            .expect("a status");
        assert!(status.ready, "{status:?}");
        let requests = requests.lock().expect("requests poisoned");
        let deployment = requests
            .iter()
            .find(|(_, path, _)| path.ends_with("/deployments/nova"))
            .expect("the Deployment");
        let body: serde_json::Value = serde_json::from_str(&deployment.2).expect("JSON body");
        let env = body["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_array()
            .expect("env");
        let names: Vec<&str> = env.iter().filter_map(|e| e["name"].as_str()).collect();
        assert!(names.contains(&"NOVA_AIRFLOW_URL"), "{names:?}");
        assert!(names.contains(&"NOVA_AIRFLOW_PASSWORD"), "{names:?}");
    }

    fn lakehouse_with_storage(storage: &str, dry_run: bool) -> Lakehouse {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "data", "uid": "u-1" },
            "spec": { "dryRun": dry_run, "storage": { "implementation": storage } }
        }))
        .expect("deserialize Lakehouse")
    }

    #[tokio::test]
    async fn no_notebooks_bucket_is_made_in_an_object_store_of_your_own() {
        let (client, requests) = mock_client(true);
        let status = ensure_notebooks_bucket(
            &context(client),
            &lakehouse_with_storage("byoS3", false),
            "data",
            &[],
        )
        .await;
        assert!(status.is_none());
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn the_notebooks_bucket_waits_for_the_object_store_secret_and_applies_nothing() {
        let (client, requests) = mock_client(false);
        let status = ensure_notebooks_bucket(
            &context(client),
            &lakehouse_with_storage("seaweedfs", false),
            "data",
            &[],
        )
        .await
        .expect("a status");
        assert_eq!(status.name, "notebooks-bucket");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for Secret stardelt-s3-creds")
        );
        assert_eq!(patch_count(&requests), 0);
    }

    #[tokio::test]
    async fn the_notebooks_bucket_job_is_applied_once_the_secret_exists() {
        let (client, requests) = mock_client(true);
        let status = ensure_notebooks_bucket(
            &context(client),
            &lakehouse_with_storage("seaweedfs", false),
            "data",
            &[],
        )
        .await
        .expect("a status");
        assert!(!status.ready);
        assert_eq!(status.message.as_deref(), Some("the bucket Job is running"));
        let name = notebooks_bucket_job_name(&s3_endpoint("data"));
        assert_eq!(
            applied_paths(&requests),
            [format!("/apis/batch/v1/namespaces/data/jobs/{name}")]
        );
    }

    #[tokio::test]
    async fn a_finished_notebooks_bucket_job_is_remembered_and_not_applied_again() {
        let name = notebooks_bucket_job_name(&s3_endpoint("data"));
        let previous = [ComponentStatus {
            name: "notebooks-bucket".into(),
            ready: true,
            message: None,
            applied_hash: Some(name.clone()),
        }];
        let (client, requests) = mock_client(true);
        let status = ensure_notebooks_bucket(
            &context(client),
            &lakehouse_with_storage("seaweedfs", false),
            "data",
            &previous,
        )
        .await
        .expect("a status");
        assert!(status.ready, "{status:?}");
        assert_eq!(status.applied_hash.as_deref(), Some(name.as_str()));
        assert_eq!(patch_count(&requests), 0);
    }

    #[tokio::test]
    async fn the_notebooks_bucket_dry_run_plans_and_touches_nothing() {
        let (client, requests) = mock_client(true);
        let status = ensure_notebooks_bucket(
            &context(client),
            &lakehouse_with_storage("seaweedfs", true),
            "data",
            &[],
        )
        .await
        .expect("a status");
        assert!(!status.ready);
        assert_eq!(status.message.as_deref(), Some("planned (dry-run)"));
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn the_archive_is_applied_once_the_s3_secret_exists() {
        let (client, requests) = mock_client(true);
        let status = ensure_archive(
            &context(client),
            &lakehouse_with_storage("seaweedfs", false),
            "data",
            &[],
        )
        .await
        .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("the archive bucket Job is running")
        );
        let name = archive_bucket_job_name(&s3_endpoint("data"));
        assert_eq!(
            applied_paths(&requests),
            [format!("/apis/batch/v1/namespaces/data/jobs/{name}")]
        );
    }

    #[tokio::test]
    async fn the_archive_waits_for_the_bucket_job_before_the_cron_job() {
        let (client, requests) = mock_client(true);
        let status = ensure_archive(
            &context(client),
            &lakehouse_with_storage("seaweedfs", false),
            "data",
            &[],
        )
        .await
        .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("the archive bucket Job is running")
        );
        let name = archive_bucket_job_name(&s3_endpoint("data"));
        let paths = applied_paths(&requests);
        assert!(paths.contains(&format!("/apis/batch/v1/namespaces/data/jobs/{name}")));
        assert!(
            !paths
                .iter()
                .any(|p| p.contains("/cronjobs/lakehouse-backup"))
        );
    }

    #[tokio::test]
    async fn the_archive_is_absent_without_the_seaweedfs_store() {
        let (client, requests) = mock_client(true);
        let status = ensure_archive(
            &context(client),
            &lakehouse_with_storage("byoS3", false),
            "data",
            &[],
        )
        .await;
        assert!(status.is_none());
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn catalog_database_applies_the_cluster_then_its_daily_backup() {
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_catalog_database(&context(client), &lakehouse(), "stardelt").await;
        assert_eq!(status.name, "catalog-database");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for the database to become Ready")
        );
        assert_eq!(
            applied_paths(&requests),
            [
                "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/lakekeeper-pg",
                "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/scheduledbackups/lakekeeper-pg-daily",
            ]
        );
    }

    #[tokio::test]
    async fn catalog_database_dry_run_plans_only_the_cluster() {
        let mut lh = lakehouse();
        lh.spec.dry_run = true;
        let (client, requests) = keycloak_client(&[], None);
        let status = ensure_catalog_database(&context(client), &lh, "stardelt").await;
        assert_eq!(status.name, "catalog-database");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("planned (dry-run): Cluster lakekeeper-pg")
        );
        assert_eq!(
            applied_paths(&requests),
            ["/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/lakekeeper-pg"]
        );
    }

    #[tokio::test]
    async fn the_openfga_database_applies_its_cluster_and_backup() {
        let (client, requests) = keycloak_client(&[], None);
        let lh = lakehouse();
        let status = ensure_openfga_database(&context(client), &lh, "stardelt").await;
        assert_eq!(status.name, "openfga-database");
        assert_eq!(
            applied_paths(&requests),
            [
                "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/openfga-pg",
                "/apis/postgresql.cnpg.io/v1/namespaces/stardelt/scheduledbackups/openfga-pg-daily",
            ]
        );
    }

    #[tokio::test]
    async fn the_openfga_database_dry_run_plans_only_the_cluster() {
        let (client, requests) = keycloak_client(&[], None);
        let mut lh = lakehouse();
        lh.spec.dry_run = true;
        let status = ensure_openfga_database(&context(client), &lh, "stardelt").await;
        assert_eq!(status.name, "openfga-database");
        assert!(!status.ready);
        assert!(
            status
                .message
                .as_deref()
                .unwrap_or("")
                .starts_with("planned (dry-run):")
        );
        assert_eq!(
            applied_paths(&requests),
            ["/apis/postgresql.cnpg.io/v1/namespaces/stardelt/clusters/openfga-pg"]
        );
    }

    #[tokio::test]
    async fn access_sync_waits_for_its_catalog_client() {
        let (client, _store, _requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("platform", false);
        let status = ensure_access_sync(&ctx, &spec).await.expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("waiting for Secret svc-catalog-operator")
        );
        assert_eq!(patch_count(&_requests), 0);
    }

    #[tokio::test]
    async fn access_sync_applies_its_five_objects_when_the_client_exists() {
        let (client, store, requests) = identity_client(BTreeMap::new());
        {
            let mut s = store.lock().expect("store poisoned");
            s.insert(
                "svc-catalog-operator".to_string(),
                serde_json::json!({"apiVersion":"v1","kind":"Secret","metadata":{"name":"svc-catalog-operator"}}),
            );
        }
        let ctx = context(client);
        let spec = platform("platform", false);
        let status = ensure_access_sync(&ctx, &spec).await.expect("a status");
        assert!(status.ready);
        let paths = applied_paths(&requests);
        assert!(
            paths[paths.len() - 5..]
                .iter()
                .zip([
                    "/serviceaccounts/access-sync",
                    "/configmaps/access-sync",
                    "/roles/access-sync",
                    "/rolebindings/access-sync",
                    "/cronjobs/access-sync",
                ])
                .all(|(p, suffix)| p.ends_with(suffix))
        );
    }

    #[tokio::test]
    async fn trino_config_maps_are_created_only_when_absent() {
        let (client, _store, requests) = identity_client(BTreeMap::new());
        let ctx = context(client.clone());
        let spec = platform("platform", false);
        let status = ensure_trino_config_maps(&ctx, &spec)
            .await
            .expect("a status");
        assert!(status.ready);
        assert_eq!(status.message.as_deref(), Some("created 2 / kept 0"));
        let count_before = patch_count(&requests);
        assert!(count_before >= 2);

        // second call with objects now in store
        let ctx2 = context(client);
        let status2 = ensure_trino_config_maps(&ctx2, &spec)
            .await
            .expect("a status");
        assert!(status2.ready);
        assert_eq!(status2.message.as_deref(), Some("created 0 / kept 2"));
        assert_eq!(patch_count(&requests), count_before);
    }

    #[tokio::test]
    async fn trino_config_maps_exist_with_an_external_identity_provider() {
        let (client, _store, _requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("external", false);
        let status = ensure_trino_config_maps(&ctx, &spec)
            .await
            .expect("a status");
        assert!(status.ready);
    }

    #[tokio::test]
    async fn access_sync_is_absent_without_the_platform_identity() {
        let (client, _store, _requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("external", false);
        assert!(ensure_access_sync(&ctx, &spec).await.is_none());
    }

    #[tokio::test]
    async fn trino_tls_creates_the_anchor_once_identity_is_platform() {
        let (client, _store, requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("platform", false);
        let status = ensure_trino_tls(&ctx, &spec).await.expect("a status");
        assert!(status.ready);
        let paths = applied_paths(&requests);
        let cert_paths: Vec<String> = paths
            .into_iter()
            .filter(|p| p.contains("/apis/cert-manager.io/v1/"))
            .collect();
        assert_eq!(cert_paths.len(), 4);
        assert!(cert_paths[0].contains("/issuers/"));
        assert!(cert_paths[1].contains("/certificates/"));
        assert!(cert_paths[2].contains("/issuers/"));
        assert!(cert_paths[3].contains("/certificates/"));
        let reqs = requests.lock().expect("requests poisoned");
        let cert_reqs: Vec<_> = reqs
            .iter()
            .filter(|(_, p, _)| p.contains("/apis/cert-manager.io/v1/"))
            .cloned()
            .collect();
        let kinds: Vec<String> = (0..4)
            .map(|i| {
                let body: serde_json::Value = serde_json::from_str(&cert_reqs[i].2).expect("json");
                body["kind"].as_str().unwrap_or("").to_string()
            })
            .collect();
        assert_eq!(
            kinds,
            vec!["Issuer", "Certificate", "Issuer", "Certificate"]
        );
        let coord: serde_json::Value = serde_json::from_str(&cert_reqs[3].2).expect("json");
        assert_eq!(
            coord["spec"]["keystores"]["pkcs12"]["passwordSecretRef"]["name"],
            "trino-tls-keystore"
        );
        let dns = coord["spec"]["dnsNames"].as_array().expect("dnsNames");
        assert_eq!(dns.len(), 4);
    }

    #[tokio::test]
    async fn trino_tls_is_skipped_without_an_identity_provider() {
        let (client, _store, _requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("none", false);
        assert!(ensure_trino_tls(&ctx, &spec).await.is_none());
    }

    #[tokio::test]
    async fn trino_tls_is_created_for_an_external_identity_provider() {
        let (client, _store, requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("external", false);
        let status = ensure_trino_tls(&ctx, &spec).await.expect("a status");
        assert!(status.ready);
        let cert_paths: Vec<String> = applied_paths(&requests)
            .into_iter()
            .filter(|p| p.contains("/apis/cert-manager.io/v1/"))
            .collect();
        assert_eq!(cert_paths.len(), 4);
    }

    #[tokio::test]
    async fn trino_auth_secrets_are_created_once() {
        let (client, store, _requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("platform", false);
        let status = ensure_trino_auth_secrets(&ctx, &spec)
            .await
            .expect("a status");
        assert!(status.ready);
        assert_eq!(status.name, "trino-auth-secrets");
        let store = store.lock().expect("store poisoned");
        let internal = store
            .get("trino-internal-secret")
            .expect("trino-internal-secret");
        assert!(
            internal
                .get("data")
                .and_then(|d| d.get("shared-secret"))
                .is_some()
        );
        let trino_pwd = store.get("trino-password-db").expect("trino-password-db");
        if let Some(s) = trino_pwd
            .get("stringData")
            .and_then(|d| d.get("password.db"))
            .and_then(|v| v.as_str())
        {
            assert!(s.starts_with("superset:$2y$"));
            assert!(s.contains("$10$"));
        } else if let Some(s) = trino_pwd
            .get("data")
            .and_then(|d| d.get("password.db"))
            .and_then(|v| v.as_str())
        {
            assert!(s.contains("$2") || s.starts_with("c3Vw"));
        } else {
            panic!("password.db not found");
        }
    }

    #[tokio::test]
    async fn trino_auth_secrets_complete_an_existing_password_secret() {
        let mut initial = BTreeMap::new();
        initial.insert(
            "trino-password-db".to_string(),
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": { "name": "trino-password-db" },
                "stringData": { "password": "s3cret" },
                "data": {}
            }),
        );
        let (client, _store, requests) = identity_client(initial);
        let ctx = context(client);
        let spec = platform("platform", false);
        let status = ensure_trino_auth_secrets(&ctx, &spec)
            .await
            .expect("a status");
        assert!(status.ready);
        assert_eq!(status.message.as_deref(), Some("added password.db"));
        let reqs = requests.lock().expect("requests poisoned");
        let patch_reqs: Vec<_> = reqs
            .iter()
            .filter(|(method, path, _)| {
                method == "PATCH" && path.ends_with("/secrets/trino-password-db")
            })
            .collect();
        assert_eq!(patch_reqs.len(), 1);
        let body: serde_json::Value = serde_json::from_str(&patch_reqs[0].2).expect("JSON body");
        assert!(body.get("data").is_none());
        assert_eq!(
            body["stringData"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            vec!["password.db"]
        );
    }

    #[tokio::test]
    async fn trino_auth_secrets_are_skipped_without_an_identity_provider() {
        let (client, _store, _requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("none", false);
        assert!(ensure_trino_auth_secrets(&ctx, &spec).await.is_none());
    }

    #[tokio::test]
    async fn trino_auth_secrets_are_created_for_an_external_identity_provider() {
        let (client, store, _requests) = identity_client(BTreeMap::new());
        let ctx = context(client);
        let spec = platform("external", false);
        let status = ensure_trino_auth_secrets(&ctx, &spec)
            .await
            .expect("a status");
        assert!(status.ready);
        let store = store.lock().expect("store poisoned");
        assert!(store.get("trino-internal-secret").is_some());
        assert!(store.get("trino-password-db").is_some());
    }

    #[tokio::test]
    async fn the_catalog_network_policy_exists_only_for_lakekeeper() {
        let (client, requests) = mock_client(false);
        let status = ensure_lakekeeper_network_policy(&context(client), &lakehouse(), "data")
            .await
            .expect("a status");
        assert_eq!(status.name, "lakekeeper-network-policy");
        let paths = applied_paths(&requests);
        assert_eq!(
            paths.last().cloned().unwrap_or_default(),
            "/apis/networking.k8s.io/v1/namespaces/data/networkpolicies/lakekeeper-ingress"
        );

        let lh_polaris: Lakehouse = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Lakehouse",
            "metadata": { "name": "main", "namespace": "data", "uid": "u-1" },
            "spec": {
                "dryRun": false,
                "catalog": { "implementation": "polaris" }
            }
        }))
        .expect("deserialize Lakehouse");
        let (client2, requests2) = mock_client(false);
        let status2 =
            ensure_lakekeeper_network_policy(&context(client2), &lh_polaris, "data").await;
        assert!(status2.is_none());
        assert!(requests2.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn the_catalog_network_policy_dry_run_writes_nothing() {
        let mut lh = lakehouse();
        lh.spec.dry_run = true;
        let (client, _requests) = mock_client(false);
        let status = ensure_lakekeeper_network_policy(&context(client), &lh, "data")
            .await
            .expect("a status");
        assert!(!status.ready);
        assert_eq!(
            status.message.as_deref(),
            Some("planned (dry-run): NetworkPolicy lakekeeper-ingress")
        );
    }

    #[tokio::test]
    async fn the_permission_store_network_policy_is_applied() {
        let (client, requests) = mock_client(false);
        let status = ensure_openfga_network_policy(&context(client), &lakehouse(), "data")
            .await
            .expect("a status");
        assert!(status.ready);
        let paths = applied_paths(&requests);
        assert_eq!(
            paths.last().cloned().unwrap_or_default(),
            "/apis/networking.k8s.io/v1/namespaces/data/networkpolicies/openfga-ingress"
        );
    }

    #[tokio::test]
    async fn spark_is_not_applied_unless_compute_spark_is_enabled() {
        let (client, requests) = mock_client(true);
        let lh = lakehouse();
        assert!(ensure_spark(&context(client), &lh, "data").await.is_none());
        assert!(requests.lock().expect("requests poisoned").is_empty());
    }

    #[tokio::test]
    async fn spark_applies_the_six_objects_and_reports_the_deployment() {
        let (client, requests) = mock_client(true);
        let mut lh = lakehouse();
        lh.spec.compute.spark.enabled = true;
        let status = ensure_spark(&context(client), &lh, "data")
            .await
            .expect("a status");
        assert_eq!(status.name, "spark-connect");
        assert!(!status.ready, "the mock Deployment has no status");
        let paths: Vec<String> = requests
            .lock()
            .expect("requests poisoned")
            .iter()
            .filter(|(method, _, _)| method == "PATCH")
            .map(|(_, path, _)| path.clone())
            .collect();
        let expected = [
            "/api/v1/namespaces/data/serviceaccounts/spark-connect",
            "/apis/rbac.authorization.k8s.io/v1/namespaces/data/roles/spark-connect",
            "/apis/rbac.authorization.k8s.io/v1/namespaces/data/rolebindings/spark-connect",
            "/api/v1/namespaces/data/configmaps/spark-connect-config",
            "/api/v1/namespaces/data/services/spark-connect",
            "/apis/apps/v1/namespaces/data/deployments/spark-connect",
        ];
        assert_eq!(paths, expected);
    }

    #[tokio::test]
    async fn the_store_is_initialised_once_and_kept_in_a_secret() {
        let (client, requests) = mock_store_client(
            r#"{"root_token":"s.roottoken","recovery_keys":["6162"],"recovery_keys_b64":["cmVjb3Zlcnk="]}"#,
        );
        let status = ensure_openbao_init(&context(client), &platform_store())
            .await
            .expect("platform mode runs the step");
        assert!(status.ready, "{:?}", status.message);
        let seen = requests.lock().expect("requests poisoned").clone();
        let post = seen
            .iter()
            .find(|(method, _, _)| method == "POST")
            .expect("the init POST");
        assert_eq!(
            post.1,
            "/api/v1/namespaces/stardelt/services/http:openbao:8200/proxy/v1/sys/init"
        );
        assert_eq!(
            post.2,
            r#"{"recovery_shares":1,"recovery_threshold":1,"secret_shares":0,"secret_threshold":0}"#
        );
        let patch = seen
            .iter()
            .find(|(method, _, _)| method == "PATCH")
            .expect("the Secret");
        assert_eq!(patch.1, "/api/v1/namespaces/stardelt/secrets/openbao-init");
        let document: serde_json::Value = serde_json::from_str(&patch.2).expect("JSON patch");
        // ByteString serializes as base64 of its bytes; recovery_key is the string
        // "cmVjb3Zlcnk=" stored as-is, so the JSON value is base64 of that string.
        assert_eq!(
            document["data"]["recovery-key"],
            serde_json::Value::String("Y21WamIzWmxjbms9".into())
        );
        assert_eq!(
            document["data"]["root-token"],
            serde_json::Value::String("cy5yb290dG9rZW4=".into())
        );
    }

    #[tokio::test]
    async fn a_kept_init_secret_is_not_written_again_and_other_modes_do_nothing() {
        let (client, requests) = mock_store_client("{}");
        let mut spec = platform_store();
        spec.dry_run = true;
        let status = ensure_openbao_init(&context(client), &spec)
            .await
            .expect("platform mode runs the step");
        assert!(!status.ready);
        assert_eq!(status.message.as_deref(), Some("planned (dry-run)"));
        assert!(
            requests
                .lock()
                .expect("requests poisoned")
                .iter()
                .all(|(method, _, _)| method != "POST"),
            "a dry run must not initialise the store"
        );
        let (client, requests) = mock_client(true);
        assert!(
            ensure_openbao_init(&context(client), &platform("platform", false))
                .await
                .is_none(),
            "secrets.mode none must not touch the store"
        );
        assert!(
            requests.lock().expect("requests poisoned").is_empty(),
            "secrets.mode none must not touch the cluster"
        );
    }

    #[tokio::test]
    async fn group_stores_only_for_platform_mode() {
        for mode in ["none", "external"] {
            let mut spec = platform(mode, false);
            spec.secrets.mode = if mode == "none" {
                SecretsMode::None
            } else {
                SecretsMode::External
            };
            spec.secrets.groups = vec!["analytics".into()];
            let (client, requests) = mock_client_with_init("probe-token", true);
            assert!(ensure_group_stores(&context(client), &spec).await.is_none());
            assert!(requests.lock().expect("requests poisoned").is_empty());
        }
    }

    #[test]
    fn a_group_store_can_read_only_its_own_path() {
        let policy = crate::secrets::group_policy("analytics");
        assert!(policy.contains("secret/data/connections/group/analytics/*"));
        assert!(!policy.contains("finance"));
        let store = crate::secrets::group_secret_store("stardelt", "analytics");
        assert_eq!(store["metadata"]["name"], "openbao-analytics");
        assert_eq!(
            store["spec"]["provider"]["vault"]["auth"]["kubernetes"]["role"],
            "eso-stardelt-analytics"
        );
        assert_eq!(
            store["spec"]["provider"]["vault"]["auth"]["kubernetes"]["serviceAccountRef"]["name"],
            "eso-analytics"
        );
        assert_eq!(
            store["spec"]["provider"]["vault"]["server"],
            "http://openbao.stardelt.svc.cluster.local:8200"
        );
        assert_eq!(store["spec"]["provider"]["vault"]["path"], "secret");
        assert_eq!(store["spec"]["provider"]["vault"]["version"], "v2");
    }

    #[tokio::test]
    async fn group_stores_write_the_policy_and_the_role() {
        let (client, requests) = mock_client_with_init("probe-token", true);
        let mut spec = platform("platform", false);
        spec.secrets.mode = SecretsMode::Platform;
        spec.secrets.groups = vec!["analytics".into(), "finance".into()];
        let status = ensure_group_stores(&context(client), &spec)
            .await
            .expect("a status");
        assert!(status.ready);
        assert!(
            status
                .message
                .as_deref()
                .unwrap_or("")
                .contains("2 group store(s)")
        );
        let reqs = requests.lock().expect("requests poisoned").clone();
        let mut index = 0usize;
        let expect =
            |reqs: &Vec<(String, String, String)>, index: &mut usize, method: &str, path: &str| {
                let pos = reqs[*index..]
                    .iter()
                    .position(|(m, p, _)| m == method && p.contains(path))
                    .unwrap_or_else(|| {
                        panic!("missing {method} containing {path} after {index}: {reqs:?}")
                    });
                *index += pos + 1;
            };
        expect(&reqs, &mut index, "POST", "/proxy/v1/sys/auth/kubernetes");
        expect(
            &reqs,
            &mut index,
            "POST",
            "/proxy/v1/auth/kubernetes/config",
        );
        expect(
            &reqs,
            &mut index,
            "PUT",
            "/proxy/v1/sys/policies/acl/eso-stardelt-analytics",
        );
        expect(
            &reqs,
            &mut index,
            "POST",
            "/proxy/v1/auth/kubernetes/role/eso-stardelt-analytics",
        );
        expect(&reqs, &mut index, "PATCH", "/serviceaccounts/eso-analytics");
        expect(
            &reqs,
            &mut index,
            "PATCH",
            "/secretstores/openbao-analytics",
        );
        expect(
            &reqs,
            &mut index,
            "PUT",
            "/proxy/v1/sys/policies/acl/eso-stardelt-finance",
        );
        expect(
            &reqs,
            &mut index,
            "POST",
            "/proxy/v1/auth/kubernetes/role/eso-stardelt-finance",
        );
        expect(&reqs, &mut index, "PATCH", "/serviceaccounts/eso-finance");
        expect(&reqs, &mut index, "PATCH", "/secretstores/openbao-finance");
        for (method, path, body) in &reqs {
            assert!(!path.contains("probe-token"), "{method} {path}");
            assert!(!body.contains("probe-token"), "{method} {path} body");
        }
    }

    #[tokio::test]
    async fn group_stores_report_a_missing_init_secret() {
        let (client, requests) = mock_client_with_init("probe-token", false);
        let mut spec = platform("platform", false);
        spec.secrets.mode = SecretsMode::Platform;
        spec.secrets.groups = vec!["analytics".into()];
        let status = ensure_group_stores(&context(client), &spec)
            .await
            .expect("a status");
        assert!(!status.ready);
        assert!(
            status
                .message
                .as_deref()
                .unwrap_or("")
                .contains("openbao-init")
        );
        let reqs = requests.lock().expect("requests poisoned");
        assert!(
            reqs.iter()
                .all(|(method, _, _)| method != "PUT" && method != "POST"),
            "no store writes expected: {reqs:?}"
        );
    }
}
