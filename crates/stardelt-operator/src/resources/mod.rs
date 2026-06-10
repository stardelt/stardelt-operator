//! Resource builders applied by the reconcile steps.
//!
//! Two flavours:
//!   * **Native** objects (Secret, Job, Deployment, Service) use `k8s-openapi`
//!     structs directly and are applied with [`apply`].
//!   * **Foreign CRDs** we don't own — Flux `HelmRepository`/`HelmRelease` and
//!     CNPG `Cluster` — have no `k8s-openapi` bindings, so they are built as
//!     `serde_json::Value` and applied as `DynamicObject` via [`apply_dynamic`].
//!
//! Everything is server-side-applied with the operator's field manager, making
//! each reconcile pass idempotent (the same property `helm upgrade --install`
//! relies on).

pub mod bootstrap;
pub mod cnpg;
pub mod flux;
pub mod ingress;
pub mod keycloak;
pub mod keycloak_bootstrap;
pub mod keycloak_pg;
pub mod nova;
pub mod secret;

use kube::api::{Patch, PatchParams};
use kube::core::{DynamicObject, GroupVersionKind, NamespaceResourceScope, ObjectMeta};
use kube::discovery::ApiResource;
use kube::{Api, Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

use crate::error::Result;

/// Standard labels stamped on every operator-managed object.
pub fn labels() -> std::collections::BTreeMap<String, String> {
    std::collections::BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            "stardelt-operator".to_string(),
        ),
        (
            "app.kubernetes.io/part-of".to_string(),
            "stardelt".to_string(),
        ),
    ])
}

/// `ObjectMeta` for a namespaced object owned by the PlatformInstance.
pub fn object_meta(name: &str, namespace: &str, owner: OwnerReference) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        labels: Some(labels()),
        owner_references: Some(vec![owner]),
        ..Default::default()
    }
}

/// Server-side-apply a typed native object.
///
/// k8s-openapi structs do not serialize `apiVersion`/`kind`, but the API
/// server rejects apply requests that omit them. We therefore serialize the
/// object to JSON and inject both from the `Resource` trait before patching.
pub async fn apply<K>(client: &Client, field_manager: &str, obj: &K) -> Result<()>
where
    K: Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Serialize
        + DeserializeOwned
        + Clone
        + std::fmt::Debug,
{
    let ns = obj
        .meta()
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let name = obj.meta().name.clone().unwrap_or_default();

    let mut body = serde_json::to_value(obj)?;
    if let Value::Object(map) = &mut body {
        map.insert(
            "apiVersion".to_string(),
            Value::String(K::api_version(&()).into_owned()),
        );
        map.insert("kind".to_string(), Value::String(K::kind(&()).into_owned()));
    }

    let api: Api<K> = Api::namespaced(client.clone(), &ns);
    api.patch(
        &name,
        &PatchParams::apply(field_manager).force(),
        &Patch::Apply(&body),
    )
    .await?;
    Ok(())
}

/// Server-side-apply a foreign CRD given as a JSON body. Returns the applied
/// object so callers can read back `.status` for readiness gating.
pub async fn apply_dynamic(
    client: &Client,
    field_manager: &str,
    gvk: &GroupVersionKind,
    namespace: &str,
    name: &str,
    mut body: Value,
) -> Result<DynamicObject> {
    let ar = ApiResource::from_gvk(gvk);
    // Ensure apiVersion/kind are present for SSA.
    if let Value::Object(map) = &mut body {
        map.entry("apiVersion")
            .or_insert_with(|| Value::String(ar.api_version.clone()));
        map.entry("kind")
            .or_insert_with(|| Value::String(ar.kind.clone()));
    }
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
    let applied = api
        .patch(
            name,
            &PatchParams::apply(field_manager).force(),
            &Patch::Apply(&body),
        )
        .await?;
    Ok(applied)
}

/// Server-side-apply a **cluster-scoped** foreign CRD given as a JSON body.
/// Mirrors [`apply_dynamic`] but targets the cluster-scoped collection
/// (`Api::all_with`) — required for resources like cert-manager's ClusterIssuer,
/// where a namespaced request path returns 404.
pub async fn apply_dynamic_cluster(
    client: &Client,
    field_manager: &str,
    gvk: &GroupVersionKind,
    name: &str,
    mut body: Value,
) -> Result<DynamicObject> {
    let ar = ApiResource::from_gvk(gvk);
    if let Value::Object(map) = &mut body {
        map.entry("apiVersion")
            .or_insert_with(|| Value::String(ar.api_version.clone()));
        map.entry("kind")
            .or_insert_with(|| Value::String(ar.kind.clone()));
    }
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
    let applied = api
        .patch(
            name,
            &PatchParams::apply(field_manager).force(),
            &Patch::Apply(&body),
        )
        .await?;
    Ok(applied)
}

/// Fetch a foreign CRD object (for readiness checks). `None` if not found.
pub async fn get_dynamic(
    client: &Client,
    gvk: &GroupVersionKind,
    namespace: &str,
    name: &str,
) -> Result<Option<DynamicObject>> {
    let ar = ApiResource::from_gvk(gvk);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
    Ok(api.get_opt(name).await?)
}
