//! The `ozone-s3-creds` Secret.
//!
//! Derived from `PlatformInstance.spec.s3` so the credentials always match the
//! SeaweedFS HelmRelease values. The name `ozone-s3-creds` is load-bearing —
//! Trino, Lakekeeper bootstrap, and Airflow all reference it (CLAUDE.md: do not
//! rename without coordination).

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

use crate::api::PlatformInstance;

pub const SECRET_NAME: &str = "ozone-s3-creds";

/// S3 endpoint for the in-cluster SeaweedFS S3 gateway in `namespace`.
pub fn s3_endpoint(namespace: &str) -> String {
    format!("http://seaweedfs-s3.{namespace}.svc.cluster.local:8333")
}

pub fn build(pi: &PlatformInstance, owner: OwnerReference) -> Secret {
    let s3 = &pi.spec.s3;
    let ns = &pi.spec.namespace;

    let mut data = BTreeMap::new();
    data.insert("access-key".to_string(), s3.access_key.clone());
    data.insert("secret-key".to_string(), s3.secret_key.clone());
    data.insert("endpoint".to_string(), s3_endpoint(ns));
    data.insert("bucket".to_string(), s3.bucket.clone());
    data.insert("region".to_string(), s3.region.clone());

    Secret {
        metadata: super::object_meta(SECRET_NAME, ns, owner),
        string_data: Some(data),
        type_: Some("Opaque".to_string()),
        ..Default::default()
    }
}
