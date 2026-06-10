//! Ingress + TLS resource builders.
//!
//! Ported from `stardelt-platform/manifests/ingress/*`. Built only when the
//! PlatformInstance carries `spec.ingress`. cert-manager is installed via a Flux
//! HelmRelease (jetstack chart) into the `cert-manager` namespace; the
//! ClusterIssuer (DNS-01 via Cloudflare), the wildcard Certificate, and the UIs
//! Ingress land in the platform namespace.
//!
//! Authentication is NOT handled here — each app authenticates itself against
//! Keycloak (see `resources/keycloak.rs`). This module only does TLS + routing.

use kube::core::GroupVersionKind;
use serde_json::{Value, json};

use crate::api::platform_instance::{IngressSpec, chart_versions as cv};

pub const CERT_MANAGER_NAMESPACE: &str = "cert-manager";
pub const CLUSTER_ISSUER_NAME: &str = "letsencrypt-prod";
pub const CERTIFICATE_NAME: &str = "stardelt-wildcard";
pub const WILDCARD_TLS_SECRET: &str = "stardelt-wildcard-tls";
pub const JETSTACK_REPO: &str = "jetstack";

pub fn cluster_issuer_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("cert-manager.io", "v1", "ClusterIssuer")
}
pub fn certificate_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("cert-manager.io", "v1", "Certificate")
}

/// jetstack HelmRepository (cert-manager source). Lives in the platform namespace.
pub fn jetstack_repository(namespace: &str, owner: &Value) -> Value {
    json!({
        "apiVersion": "source.toolkit.fluxcd.io/v1",
        "kind": "HelmRepository",
        "metadata": {
            "name": JETSTACK_REPO,
            "namespace": namespace,
            "labels": super::labels(),
            "ownerReferences": [owner],
        },
        "spec": { "interval": "1h", "url": "https://charts.jetstack.io" }
    })
}

/// cert-manager HelmRelease, installed into the cert-manager namespace with CRDs.
pub fn cert_manager_release(namespace: &str, owner: &Value) -> Value {
    json!({
        "apiVersion": "helm.toolkit.fluxcd.io/v2",
        "kind": "HelmRelease",
        "metadata": {
            "name": "cert-manager",
            "namespace": namespace,
            "labels": super::labels(),
            "ownerReferences": [owner],
        },
        "spec": {
            "interval": "5m",
            "timeout": "10m",
            "releaseName": "cert-manager",
            "chart": { "spec": {
                "chart": "cert-manager",
                "version": cv::CERT_MANAGER,
                "sourceRef": { "kind": "HelmRepository", "name": JETSTACK_REPO, "namespace": namespace }
            }},
            "install": { "createNamespace": true },
            "targetNamespace": CERT_MANAGER_NAMESPACE,
            "values": { "crds": { "enabled": true } }
        }
    })
}

/// Cluster-scoped ACME issuer using DNS-01 via Cloudflare (enables wildcard certs
/// without inbound :80). The Cloudflare token Secret is supplied by the operator.
pub fn cluster_issuer(ing: &IngressSpec, owner: &Value) -> Value {
    json!({
        "apiVersion": "cert-manager.io/v1",
        "kind": "ClusterIssuer",
        "metadata": {
            "name": CLUSTER_ISSUER_NAME,
            "labels": super::labels(),
            "ownerReferences": [owner],
        },
        "spec": { "acme": {
            "server": ing.tls.acme_server,
            "email": ing.tls.email,
            "privateKeySecretRef": { "name": "letsencrypt-prod-account-key" },
            "solvers": [{ "dns01": { "cloudflare": {
                "apiTokenSecretRef": { "name": "cloudflare-api-token", "key": "api-token" }
            }}}]
        }}
    })
}

/// One wildcard cert covering every per-service subdomain plus the apex.
pub fn certificate(ing: &IngressSpec, namespace: &str, owner: &Value) -> Value {
    json!({
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": CERTIFICATE_NAME,
            "namespace": namespace,
            "labels": super::labels(),
            "ownerReferences": [owner],
        },
        "spec": {
            "secretName": WILDCARD_TLS_SECRET,
            "issuerRef": { "name": CLUSTER_ISSUER_NAME, "kind": "ClusterIssuer" },
            "commonName": format!("*.{}", ing.domain),
            "dnsNames": [format!("*.{}", ing.domain), ing.domain.clone()],
        }
    })
}

/// All UI hosts, sharing the wildcard cert. No auth middleware — each app
/// authenticates itself against Keycloak (see `resources/keycloak.rs`).
pub fn uis_ingress(ing: &IngressSpec, namespace: &str, owner: &Value) -> Value {
    let d = &ing.domain;
    let route = |sub: &str, svc: &str, port: u32| {
        json!({
            "host": format!("{sub}.{d}"),
            "http": { "paths": [{ "path": "/", "pathType": "Prefix",
                "backend": { "service": { "name": svc, "port": { "number": port }}}}]}
        })
    };
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "stardelt-uis", "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
            "annotations": {
                "traefik.ingress.kubernetes.io/router.entrypoints": "websecure",
            },
        },
        "spec": {
            "tls": [{ "hosts": [
                format!("nova.{d}"), format!("superset.{d}"),
                format!("airflow.{d}"), format!("trino.{d}")
            ], "secretName": WILDCARD_TLS_SECRET }],
            "rules": [
                route("nova", "nova", 8080),
                route("superset", "superset", 8088),
                route("airflow", "airflow-api-server", 8080),
                route("trino", "trino", 8080),
            ]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::PlatformInstance;

    fn instance() -> PlatformInstance {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "platform.stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "t" },
            "spec": {
                "namespace": "stardelt",
                "ingress": { "domain": "lab.stardelt.io", "sso": { "orgName": "stardelt" } }
            }
        }))
        .unwrap()
    }

    fn ingress(pi: &PlatformInstance) -> &IngressSpec {
        pi.spec.ingress.as_ref().unwrap()
    }

    #[test]
    fn cert_manager_release_pins_version_and_targets_namespace() {
        let owner = json!({});
        let hr = cert_manager_release("stardelt", &owner);
        assert_eq!(hr["spec"]["chart"]["spec"]["version"], cv::CERT_MANAGER);
        assert_eq!(hr["spec"]["targetNamespace"], CERT_MANAGER_NAMESPACE);
        assert_eq!(hr["spec"]["values"]["crds"]["enabled"], true);
    }

    #[test]
    fn instance_carries_ingress() {
        let pi = instance();
        assert_eq!(ingress(&pi).domain, "lab.stardelt.io");
    }

    #[test]
    fn cluster_issuer_uses_acme_server_and_cloudflare_solver() {
        let pi = instance();
        let ci = cluster_issuer(ingress(&pi), &json!({}));
        assert_eq!(
            ci["spec"]["acme"]["server"],
            "https://acme-v02.api.letsencrypt.org/directory"
        );
        assert_eq!(ci["spec"]["acme"]["email"], "admin@stardelt.io");
        assert_eq!(
            ci["spec"]["acme"]["solvers"][0]["dns01"]["cloudflare"]["apiTokenSecretRef"]["name"],
            "cloudflare-api-token"
        );
    }

    #[test]
    fn certificate_covers_wildcard_and_apex() {
        let pi = instance();
        let cert = certificate(ingress(&pi), "stardelt", &json!({}));
        assert_eq!(cert["spec"]["secretName"], WILDCARD_TLS_SECRET);
        assert_eq!(cert["spec"]["dnsNames"][0], "*.lab.stardelt.io");
        assert_eq!(cert["spec"]["dnsNames"][1], "lab.stardelt.io");
    }

    #[test]
    fn uis_ingress_covers_four_hosts() {
        let pi = instance();
        let ing_obj = uis_ingress(ingress(&pi), "stardelt", &json!({}));
        let rules = ing_obj["spec"]["rules"].as_array().unwrap();
        let hosts: Vec<&str> = rules.iter().map(|r| r["host"].as_str().unwrap()).collect();
        assert_eq!(
            hosts,
            vec![
                "nova.lab.stardelt.io",
                "superset.lab.stardelt.io",
                "airflow.lab.stardelt.io",
                "trino.lab.stardelt.io"
            ]
        );
        // no auth middleware annotation anymore
        assert!(
            ing_obj["metadata"]["annotations"]["traefik.ingress.kubernetes.io/router.middlewares"]
                .is_null()
        );
    }
}
