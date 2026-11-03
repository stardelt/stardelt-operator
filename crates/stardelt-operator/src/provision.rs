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
        networking::v1::Ingress,
        rbac::v1::{Role, RoleBinding},
    },
};
use kube::{
    Api, ResourceExt,
    api::{Patch, PatchParams},
    core::DynamicObject,
};

use crate::{
    airflow,
    controller::Context,
    crd::{ComponentStatus, IdentityMode, Lakehouse, PlatformInstanceSpec, StorageImpl},
    identity, ingress, keycloak, nova,
    objects::{
        apply, apply_dynamic, cnpg_cluster_resource, cnpg_scheduled_backup_resource,
        owner_reference,
    },
    prerequisites::{
        CATALOG_CLUSTER, DEFAULT_BUCKET, NOVA_CLUSTER, S3_CONFIG_SECRET, S3_SECRET, S3Keys,
        S3Location, bootstrap_job, bootstrap_job_name, catalog_cluster, catalog_pg_backup,
        discovery_config_map, discovery_config_map_name, keycloak_pg_backup, keycloak_pg_cluster,
        notebooks_bucket_job, notebooks_bucket_job_name, nova_pg_backup, nova_pg_cluster,
        s3_creds_secret, s3_endpoint, seaweedfs_config_secret,
    },
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

/// The build of Nova the operator installs: the one `stardelt-platform` pins in
/// `manifests/nova-deployment.yaml`, by digest, so a reconcile never changes it by itself
/// (decision D-op-5 of `design/operator-parity.md`: a constant per operator release).
pub const NOVA_IMAGE: &str = "ghcr.io/stardelt/nova:dev@sha256:a3f4e0450c12b4b2508e35e5a0349500b6dd8aea70e7bffd154d3af5c6c9a6ee";

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
    }
    objects.extend(ingress::keycloak(spec));
    if spec.pillars.pipelines {
        objects.extend(ingress::airflow(spec));
    }
    if spec.pillars.notebooks {
        objects.extend(ingress::jupyterhub(spec));
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
            Some("planned (dry-run): Ingress nova, keycloak")
        );
        assert_eq!(
            applied_paths(&requests),
            [format!("{INGRESSES}/nova"), format!("{INGRESSES}/keycloak")]
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
        assert_eq!(status.message.as_deref(), Some("applied: nova, keycloak"));
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
}
