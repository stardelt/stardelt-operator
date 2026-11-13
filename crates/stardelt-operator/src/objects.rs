//! Server-side apply for the objects a Lakehouse needs besides Helm releases
//! (decision D17): Secrets, the catalog database `Cluster`, the bootstrap `Job`.

use std::collections::BTreeMap;
use std::fmt::Debug;

use k8s_openapi::{
    ClusterResourceScope, NamespaceResourceScope, apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{DynamicObject, Patch, PatchParams},
    core::{ApiResource, GroupVersionKind},
};
use serde::{Serialize, de::DeserializeOwned};

use crate::crd::{Lakehouse, PlatformInstance};

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

/// Owner reference that makes Kubernetes delete an object with its `PlatformInstance`
/// (D18, epic#55 definition of done 3). Only for workload objects: a Secret, a PVC and the
/// CNPG `Cluster`s are retained and must never carry it.
pub fn platform_owner(instance: &PlatformInstance) -> OwnerReference {
    OwnerReference {
        api_version: "stardelt.io/v1alpha1".into(),
        kind: "PlatformInstance".into(),
        name: instance.name_any(),
        uid: instance.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Stamp `owner` on an object before it is applied. Idempotent: an object that already
/// carries the same uid is left as it is, so re-applying never duplicates the list.
pub fn own<K>(object: &mut K, owner: &OwnerReference)
where
    K: kube::Resource<DynamicType = ()>,
{
    let owners = object.owner_references_mut();
    if owners.iter().any(|existing| existing.uid == owner.uid) {
        return;
    }
    owners.push(owner.clone());
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

/// Server-side apply of a typed cluster-scoped object (the Spark Connect pod
/// guard). `apply` above is namespaced only; `apply_dynamic_cluster` covers
/// objects we have no Rust type for.
pub async fn apply_cluster<K>(client: &Client, object: &K, dry_run: bool) -> Result<K, kube::Error>
where
    K: Resource<Scope = ClusterResourceScope, DynamicType = ()>
        + Clone
        + DeserializeOwned
        + Serialize
        + Debug,
{
    Api::<K>::all(client.clone())
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

/// Same for a cluster-scoped object (the `ClusterSecretStore`).
pub async fn apply_dynamic_cluster(
    client: &Client,
    resource: &ApiResource,
    object: &DynamicObject,
    dry_run: bool,
) -> Result<DynamicObject, kube::Error> {
    Api::<DynamicObject>::all_with(client.clone(), resource)
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

/// The Strimzi broker (`kafkas.kafka.strimzi.io/v1`).
pub fn kafka_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("kafka.strimzi.io", "v1", "Kafka"),
        "kafkas",
    )
}

/// A Strimzi node pool (`kafkanodepools.kafka.strimzi.io/v1`). Strimzi 1.x requires one.
pub fn kafka_node_pool_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("kafka.strimzi.io", "v1", "KafkaNodePool"),
        "kafkanodepools",
    )
}

/// A Strimzi topic (`kafkatopics.kafka.strimzi.io/v1`).
pub fn kafka_topic_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("kafka.strimzi.io", "v1", "KafkaTopic"),
        "kafkatopics",
    )
}

/// The Kubeflow Spark Operator application (`sparkapplications.sparkoperator.k8s.io/v1beta2`).
pub fn spark_application_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("sparkoperator.k8s.io", "v1beta2", "SparkApplication"),
        "sparkapplications",
    )
}

/// A Kubeflow Spark Operator Spark Connect server
/// (`sparkconnects.sparkoperator.k8s.io/v1alpha1`).
pub fn spark_connect_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("sparkoperator.k8s.io", "v1alpha1", "SparkConnect"),
        "sparkconnects",
    )
}

/// The CloudNative-PG ScheduledBackup resource (`scheduledbackups.postgresql.cnpg.io/v1`).
pub fn cnpg_scheduled_backup_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("postgresql.cnpg.io", "v1", "ScheduledBackup"),
        "scheduledbackups",
    )
}

/// The barman-cloud plugin's `ObjectStore` (`objectstores.barmancloud.cnpg.io/v1`).
pub fn barman_object_store_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("barmancloud.cnpg.io", "v1", "ObjectStore"),
        "objectstores",
    )
}

/// A cert-manager `Issuer` (`issuers.cert-manager.io/v1`).
pub fn cert_manager_issuer_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("cert-manager.io", "v1", "Issuer"),
        "issuers",
    )
}

/// A cert-manager `Certificate` (`certificates.cert-manager.io/v1`).
pub fn cert_manager_certificate_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("cert-manager.io", "v1", "Certificate"),
        "certificates",
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
    fn kafka_resources_point_at_strimzi() {
        for (r, plural) in [
            (kafka_resource(), "kafkas"),
            (kafka_node_pool_resource(), "kafkanodepools"),
            (kafka_topic_resource(), "kafkatopics"),
        ] {
            assert_eq!(
                (r.group.as_str(), r.version.as_str(), r.plural.as_str()),
                ("kafka.strimzi.io", "v1", plural)
            );
        }
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
    fn issuer_resource_points_at_cert_manager() {
        let r = cert_manager_issuer_resource();
        assert_eq!(
            (r.group.as_str(), r.version.as_str(), r.plural.as_str()),
            ("cert-manager.io", "v1", "issuers")
        );
    }

    #[test]
    fn certificate_resource_points_at_cert_manager() {
        let r = cert_manager_certificate_resource();
        assert_eq!(
            (r.group.as_str(), r.version.as_str(), r.plural.as_str()),
            ("cert-manager.io", "v1", "certificates")
        );
    }

    #[test]
    fn an_owned_object_carries_the_platform_instance_reference() {
        let instance: crate::crd::PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "cluster", "uid": "6f1c2e8a-0000-4000-8000-000000000001" },
            "spec": {}
        }))
        .expect("deserialize PlatformInstance");
        let mut service = crate::nova::service_account("data");
        own(&mut service, &platform_owner(&instance));
        let owners = service.owner_references();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind, "PlatformInstance");
        assert_eq!(owners[0].name, "cluster");
        assert_eq!(owners[0].uid, "6f1c2e8a-0000-4000-8000-000000000001");
        assert_eq!(owners[0].controller, Some(true));
        // A second stamp is a no-op, not a duplicate.
        own(&mut service, &platform_owner(&instance));
        assert_eq!(service.owner_references().len(), 1);
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
