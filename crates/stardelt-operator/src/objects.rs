//! Server-side apply for the objects a Lakehouse needs besides Helm releases
//! (decision D17): Secrets, the catalog database `Cluster`, the bootstrap `Job`.

use std::collections::BTreeMap;
use std::fmt::Debug;

use k8s_openapi::{NamespaceResourceScope, apimachinery::pkg::apis::meta::v1::OwnerReference};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{DynamicObject, Patch, PatchParams},
    core::{ApiResource, GroupVersionKind},
};
use serde::{Serialize, de::DeserializeOwned};

use crate::crd::Lakehouse;

/// Field manager every server-side apply of this operator uses.
pub const FIELD_MANAGER: &str = "stardelt-operator";

/// Owner reference that makes Kubernetes delete the object with its Lakehouse.
/// Only for disposable objects (the bootstrap Job): Secrets and the database
/// keep their data when the Lakehouse is deleted (decision D18, `Retain`).
pub fn owner_reference(lh: &Lakehouse) -> OwnerReference {
    OwnerReference {
        api_version: "stardelt.io/v1alpha1".into(),
        kind: "Lakehouse".into(),
        name: lh.name_any(),
        uid: lh.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Labels on everything the operator creates (decision D18).
pub fn labels(lakehouse: &str, component: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            FIELD_MANAGER.to_string(),
        ),
        ("stardelt.io/lakehouse".to_string(), lakehouse.to_string()),
        ("stardelt.io/component".to_string(), component.to_string()),
    ])
}

fn params(dry_run: bool) -> PatchParams {
    let mut params = PatchParams::apply(FIELD_MANAGER).force();
    params.dry_run = dry_run;
    params
}

/// Server-side apply of a typed namespaced object. With `dry_run` the API server
/// validates and returns the result without persisting it.
pub async fn apply<K>(
    client: &Client,
    namespace: &str,
    object: &K,
    dry_run: bool,
) -> Result<K, kube::Error>
where
    K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
        + Clone
        + DeserializeOwned
        + Serialize
        + Debug,
{
    Api::<K>::namespaced(client.clone(), namespace)
        .patch(&object.name_any(), &params(dry_run), &Patch::Apply(object))
        .await
}

/// Same for an object whose Rust type we do not have (the CloudNative-PG `Cluster`).
pub async fn apply_dynamic(
    client: &Client,
    namespace: &str,
    resource: &ApiResource,
    object: &DynamicObject,
    dry_run: bool,
) -> Result<DynamicObject, kube::Error> {
    Api::<DynamicObject>::namespaced_with(client.clone(), namespace, resource)
        .patch(&object.name_any(), &params(dry_run), &Patch::Apply(object))
        .await
}

/// The CloudNative-PG `Cluster` resource (`clusters.postgresql.cnpg.io/v1`).
pub fn cnpg_cluster_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("postgresql.cnpg.io", "v1", "Cluster"),
        "clusters",
    )
}

/// The CloudNative-PG ScheduledBackup resource (`scheduledbackups.postgresql.cnpg.io/v1`).
pub fn cnpg_scheduled_backup_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("postgresql.cnpg.io", "v1", "ScheduledBackup"),
        "scheduledbackups",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_name_the_lakehouse_and_the_component() {
        let l = labels("main", "lakekeeper");
        assert_eq!(l["stardelt.io/lakehouse"], "main");
        assert_eq!(l["stardelt.io/component"], "lakekeeper");
        assert_eq!(l["app.kubernetes.io/managed-by"], "stardelt-operator");
    }

    #[test]
    fn cluster_resource_points_at_cnpg() {
        let r = cnpg_cluster_resource();
        assert_eq!(
            (r.group.as_str(), r.version.as_str(), r.plural.as_str()),
            ("postgresql.cnpg.io", "v1", "clusters")
        );
    }

    #[test]
    fn scheduled_backup_resource_points_at_cnpg() {
        let r = cnpg_scheduled_backup_resource();
        assert_eq!(
            (r.group.as_str(), r.version.as_str(), r.plural.as_str()),
            ("postgresql.cnpg.io", "v1", "scheduledbackups")
        );
    }

    #[test]
    fn dry_run_is_passed_through() {
        assert!(params(true).dry_run);
        assert!(!params(false).dry_run);
        assert_eq!(
            params(false).field_manager.as_deref(),
            Some("stardelt-operator")
        );
    }
}
