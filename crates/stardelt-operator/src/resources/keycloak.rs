//! Keycloak install: Flux HelmRepository + HelmRelease (bitnami chart) wired to
//! the external `keycloak-pg` CNPG Postgres, plus the `auth.<domain>` Ingress.
//! Built only when the PlatformInstance carries `spec.sso`.
//!
// `allow(dead_code)`: consumed by controllers::platform_instance::apply_keycloak
// and resources::nova (Task 1.6). Until then only tests reference these.
#![allow(dead_code)]

use kube::core::GroupVersionKind;
use serde_json::{Value, json};

use crate::api::platform_instance::{KeycloakSsoSpec, chart_versions as cv};
use crate::resources::keycloak_pg;

pub const RELEASE: &str = "keycloak";
pub const BITNAMI_REPO: &str = "bitnami";
pub const BITNAMI_URL: &str = "https://charts.bitnami.com/bitnami";
/// Chart-managed admin Secret (Keycloak admin user `user` / `admin-password`).
pub const ADMIN_SECRET: &str = "keycloak";

pub fn ingress_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("networking.k8s.io", "v1", "Ingress")
}

/// In-cluster Keycloak HTTP URL.
pub fn service_url(namespace: &str) -> String {
    format!("http://keycloak.{namespace}.svc.cluster.local")
}

/// External issuer URL for a realm (what OIDC clients trust).
pub fn issuer_url(sso: &KeycloakSsoSpec) -> String {
    format!("https://auth.{}/realms/{}", sso.domain, sso.realm)
}

pub fn bitnami_repository(namespace: &str, owner: &Value) -> Value {
    json!({
        "apiVersion": "source.toolkit.fluxcd.io/v1",
        "kind": "HelmRepository",
        "metadata": {
            "name": BITNAMI_REPO, "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
        },
        "spec": { "interval": "1h", "url": BITNAMI_URL }
    })
}

/// Keycloak HelmRelease. Uses the external keycloak-pg Postgres (CNPG-managed
/// `keycloak-pg-app` Secret) and is fronted by the auth host (proxy mode edge).
/// The install itself is realm-agnostic — realm/broker/client config is applied
/// later by the bootstrap Job — so this takes no `sso` argument.
pub fn release(namespace: &str, owner: &Value) -> Value {
    let pg_host = format!(
        "{}-rw.{namespace}.svc.cluster.local",
        keycloak_pg::CLUSTER_NAME
    );
    json!({
        "apiVersion": "helm.toolkit.fluxcd.io/v2",
        "kind": "HelmRelease",
        "metadata": {
            "name": RELEASE, "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
        },
        "spec": {
            "interval": "5m",
            "timeout": "10m",
            "releaseName": RELEASE,
            "chart": { "spec": {
                "chart": "keycloak",
                "version": cv::KEYCLOAK,
                "sourceRef": { "kind": "HelmRepository", "name": BITNAMI_REPO, "namespace": namespace }
            }},
            "values": {
                "production": true,
                "proxy": "edge",
                "auth": { "adminUser": "admin" },
                "postgresql": { "enabled": false },
                "externalDatabase": {
                    "host": pg_host,
                    "port": 5432,
                    "database": keycloak_pg::DB_NAME,
                    "user": "keycloak",
                    "existingSecret": format!("{}-app", keycloak_pg::CLUSTER_NAME),
                    "existingSecretPasswordKey": "password"
                },
                "service": { "type": "ClusterIP" }
            }
        }
    })
}

/// Ingress exposing Keycloak at `auth.<domain>` (TLS via the wildcard cert).
pub fn auth_ingress(sso: &KeycloakSsoSpec, namespace: &str, owner: &Value) -> Value {
    let host = format!("auth.{}", sso.domain);
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "stardelt-auth", "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
            "annotations": { "traefik.ingress.kubernetes.io/router.entrypoints": "websecure" },
        },
        "spec": {
            "tls": [{ "hosts": [host.clone()], "secretName": super::ingress::WILDCARD_TLS_SECRET }],
            "rules": [{ "host": host, "http": { "paths": [{
                "path": "/", "pathType": "Prefix",
                "backend": { "service": { "name": RELEASE, "port": { "number": 80 }}}
            }]}}]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sso() -> KeycloakSsoSpec {
        serde_json::from_value(serde_json::json!({
            "domain": "lab.stardelt.io",
            "githubBrokerSecret": "keycloak-github-broker"
        }))
        .unwrap()
    }

    #[test]
    fn issuer_url_includes_realm() {
        assert_eq!(
            issuer_url(&sso()),
            "https://auth.lab.stardelt.io/realms/stardelt"
        );
    }

    #[test]
    fn release_uses_external_keycloak_pg() {
        let hr = release("stardelt", &json!({}));
        assert_eq!(hr["spec"]["values"]["postgresql"]["enabled"], false);
        assert_eq!(
            hr["spec"]["values"]["externalDatabase"]["host"],
            "keycloak-pg-rw.stardelt.svc.cluster.local"
        );
        assert_eq!(hr["spec"]["chart"]["spec"]["version"], cv::KEYCLOAK);
    }

    #[test]
    fn auth_ingress_serves_keycloak_on_auth_host() {
        let ing = auth_ingress(&sso(), "stardelt", &json!({}));
        assert_eq!(ing["spec"]["rules"][0]["host"], "auth.lab.stardelt.io");
        assert_eq!(
            ing["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"],
            "keycloak"
        );
    }
}
