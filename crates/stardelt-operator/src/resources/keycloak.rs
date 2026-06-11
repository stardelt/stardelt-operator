//! Keycloak install: Flux HelmRepository + HelmRelease (bitnami chart) wired to
//! the external `keycloak-pg` CNPG Postgres, plus the `auth.<domain>` Ingress.
//! Built only when the PlatformInstance carries `spec.sso`.

use kube::core::GroupVersionKind;
use serde_json::{Value, json};

use crate::api::platform_instance::{KeycloakSsoSpec, chart_versions as cv};
use crate::resources::keycloak_pg;

pub const RELEASE: &str = "keycloak";
pub const CODECENTRIC_REPO: &str = "codecentric";
// codecentric/keycloakx uses the OFFICIAL quay.io/keycloak/keycloak image
// (vendor-neutral, maintained) — unlike the Bitnami chart, whose images were
// paywalled/removed in Broadcom's 2025 migration. Classic HTTP Helm repo.
pub const CODECENTRIC_URL: &str = "https://codecentric.github.io/helm-charts";
/// Secret the operator creates holding the Keycloak bootstrap admin password.
/// keycloakx has no chart-managed admin secret, so we supply our own.
pub const ADMIN_SECRET: &str = "keycloak-admin";
/// Deterministic admin password key in [`ADMIN_SECRET`].
pub const ADMIN_PASSWORD_KEY: &str = "admin-password";
/// The keycloakx chart names its HTTP Service `<release>-keycloakx-http`.
pub const SERVICE_NAME: &str = "keycloak-keycloakx-http";
/// Official Keycloak image (matches keycloakx 7.2.0 appVersion). Used by the
/// realm-bootstrap Job to run `kcadm.sh`. Vendor-neutral (Quay), not Bitnami.
pub const KEYCLOAK_IMAGE: &str = "quay.io/keycloak/keycloak:26.6.2";

pub fn ingress_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("networking.k8s.io", "v1", "Ingress")
}

/// In-cluster Keycloak HTTP URL (the keycloakx chart's Service, port 80).
pub fn service_url(namespace: &str) -> String {
    format!("http://{SERVICE_NAME}.{namespace}.svc.cluster.local")
}

/// External issuer URL for a realm (what OIDC clients trust).
pub fn issuer_url(sso: &KeycloakSsoSpec) -> String {
    format!("https://auth.{}/realms/{}", sso.domain, sso.realm)
}

pub fn codecentric_repository(namespace: &str, owner: &Value) -> Value {
    json!({
        "apiVersion": "source.toolkit.fluxcd.io/v1",
        "kind": "HelmRepository",
        "metadata": {
            "name": CODECENTRIC_REPO, "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
        },
        "spec": { "interval": "1h", "url": CODECENTRIC_URL }
    })
}

/// Deterministic admin-password Secret for Keycloak's bootstrap admin user.
/// Derived from the CR uid (no RNG in the reconcile path), like the nova-oidc
/// secret. The bootstrap Job reads it to authenticate `kcadm`.
pub fn admin_secret(namespace: &str, uid: &str, owner: &Value) -> Value {
    use base64::Engine as _;
    let mut acc: u64 = 0x84222325cbf29ce4;
    let mut bytes = Vec::with_capacity(24);
    for chunk in 0..3u8 {
        for b in format!("kcadmin:{uid}:{chunk}").bytes() {
            acc ^= b as u64;
            acc = acc.wrapping_mul(0x100000001b3);
        }
        bytes.extend_from_slice(&acc.to_be_bytes());
    }
    let pw = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes);
    let data = base64::engine::general_purpose::STANDARD.encode(pw.as_bytes());
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": ADMIN_SECRET, "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
        },
        "type": "Opaque",
        "data": { ADMIN_PASSWORD_KEY: data }
    })
}

/// Keycloak HelmRelease (codecentric/keycloakx). Uses the external keycloak-pg
/// Postgres (CNPG-managed `keycloak-pg-app` Secret) and the official
/// quay.io/keycloak image. Runs in production mode behind the auth host (edge
/// proxy). The install is realm-agnostic — realm/broker/client config is applied
/// later by the bootstrap Job — so this takes no `sso` argument.
pub fn release(namespace: &str, owner: &Value) -> Value {
    let pg_host = format!(
        "{}-rw.{namespace}.svc.cluster.local",
        keycloak_pg::CLUSTER_NAME
    );
    let db_secret = format!("{}-app", keycloak_pg::CLUSTER_NAME);
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
                "chart": "keycloakx",
                "version": cv::KEYCLOAK,
                "sourceRef": { "kind": "HelmRepository", "name": CODECENTRIC_REPO, "namespace": namespace }
            }},
            "values": {
                // keycloakx ships an EMPTY command by default; without one the
                // official image just prints help and exits. Provide the prod
                // start command (the chart's README example).
                "command": [
                    "/opt/keycloak/bin/kc.sh",
                    "start",
                    "--http-enabled=true",
                    "--http-port=8080",
                    "--hostname-strict=false"
                ],
                // The chart sets KC_HEALTH_ENABLED/KC_CACHE/KC_PROXY_HEADERS/KC_DB
                // itself — do NOT duplicate them here (duplicate env key →
                // StatefulSet apply fails). Only add the admin bootstrap creds.
                "extraEnv": "- name: KEYCLOAK_ADMIN\n  value: admin\n- name: KEYCLOAK_ADMIN_PASSWORD\n  valueFrom:\n    secretKeyRef:\n      name: keycloak-admin\n      key: admin-password\n",
                // keycloakx defaults the relative path to /auth; serve at root so
                // the issuer URL (https://auth.<domain>/realms/<realm>) and OIDC
                // discovery have no /auth prefix. Keeps kcadm + Nova URLs simple.
                "http": { "relativePath": "/" },
                "database": {
                    "vendor": "postgres",
                    "hostname": pg_host,
                    "port": 5432,
                    "database": keycloak_pg::DB_NAME,
                    "username": "keycloak",
                    "existingSecret": db_secret,
                    "existingSecretKey": "password"
                },
                "service": { "type": "ClusterIP", "httpPort": 80 }
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
                "backend": { "service": { "name": SERVICE_NAME, "port": { "number": 80 }}}
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
    fn repo_is_codecentric_http() {
        let repo = codecentric_repository("stardelt", &json!({}));
        assert!(repo["spec"]["type"].is_null()); // default HTTP, not OCI
        assert_eq!(
            repo["spec"]["url"],
            "https://codecentric.github.io/helm-charts"
        );
    }

    #[test]
    fn release_uses_external_keycloak_pg() {
        let hr = release("stardelt", &json!({}));
        assert_eq!(hr["spec"]["chart"]["spec"]["chart"], "keycloakx");
        assert_eq!(hr["spec"]["values"]["database"]["vendor"], "postgres");
        assert_eq!(
            hr["spec"]["values"]["database"]["hostname"],
            "keycloak-pg-rw.stardelt.svc.cluster.local"
        );
        assert_eq!(
            hr["spec"]["values"]["database"]["existingSecret"],
            "keycloak-pg-app"
        );
        assert_eq!(hr["spec"]["chart"]["spec"]["version"], cv::KEYCLOAK);
    }

    #[test]
    fn admin_secret_is_stable_for_same_uid() {
        let a = admin_secret("stardelt", "uid-x", &json!({}));
        let b = admin_secret("stardelt", "uid-x", &json!({}));
        assert_eq!(a["data"], b["data"]);
        assert_eq!(a["metadata"]["name"], "keycloak-admin");
        assert!(a["data"]["admin-password"].as_str().unwrap().len() > 10);
    }

    #[test]
    fn auth_ingress_serves_keycloak_on_auth_host() {
        let ing = auth_ingress(&sso(), "stardelt", &json!({}));
        assert_eq!(ing["spec"]["rules"][0]["host"], "auth.lab.stardelt.io");
        assert_eq!(
            ing["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"],
            "keycloak-keycloakx-http"
        );
    }
}
