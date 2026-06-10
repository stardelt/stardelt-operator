//! Ingress + SSO resource builders.
//!
//! Ported from `stardelt-platform/manifests/ingress/*`. Built only when the
//! PlatformInstance carries `spec.ingress`. cert-manager is installed via a Flux
//! HelmRelease (jetstack chart) into the `cert-manager` namespace; everything
//! else (ClusterIssuer, wildcard Certificate, oauth2-proxy, Traefik Middleware,
//! Ingresses) lands in the platform namespace.

use kube::core::GroupVersionKind;
use serde_json::{Value, json};

use crate::api::platform_instance::{IngressSpec, chart_versions as cv};

pub const CERT_MANAGER_NAMESPACE: &str = "cert-manager";
pub const CLUSTER_ISSUER_NAME: &str = "letsencrypt-prod";
pub const CERTIFICATE_NAME: &str = "stardelt-wildcard";
pub const WILDCARD_TLS_SECRET: &str = "stardelt-wildcard-tls";
pub const OAUTH2_PROXY_NAME: &str = "oauth2-proxy";
pub const MIDDLEWARE_NAME: &str = "oauth2-forward-auth";
pub const ERRORS_MIDDLEWARE_NAME: &str = "oauth2-errors";
pub const JETSTACK_REPO: &str = "jetstack";

pub fn cluster_issuer_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("cert-manager.io", "v1", "ClusterIssuer")
}
pub fn certificate_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("cert-manager.io", "v1", "Certificate")
}
pub fn middleware_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("traefik.io", "v1alpha1", "Middleware")
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

/// oauth2-proxy — GitHub provider restricted to the SSO org. Fronts every UI via
/// a Traefik forward-auth middleware. Credentials read from `sso.credentials_secret`.
pub fn oauth2_proxy_deployment(ing: &IngressSpec, namespace: &str, owner: &Value) -> Value {
    let secret = &ing.sso.credentials_secret;
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": OAUTH2_PROXY_NAME, "namespace": namespace,
            "labels": { "app.kubernetes.io/name": OAUTH2_PROXY_NAME },
            "ownerReferences": [owner],
        },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app.kubernetes.io/name": OAUTH2_PROXY_NAME }},
            "template": {
                "metadata": { "labels": { "app.kubernetes.io/name": OAUTH2_PROXY_NAME }},
                "spec": { "containers": [{
                    "name": OAUTH2_PROXY_NAME,
                    "image": cv::OAUTH2_PROXY_IMAGE,
                    "args": [
                        format!("--provider={}", ing.sso.provider),
                        format!("--github-org={}", ing.sso.org_name),
                        "--http-address=0.0.0.0:4180",
                        "--reverse-proxy=true",
                        format!("--cookie-domain=.{}", ing.domain),
                        format!("--whitelist-domain=.{}", ing.domain),
                        "--cookie-secure=true",
                        "--email-domain=*",
                        "--upstream=static://202",
                        format!("--redirect-url=https://auth.{}/oauth2/callback", ing.domain),
                        "--set-xauthrequest=true",
                        "--pass-access-token=false",
                        "--skip-provider-button=false",
                    ],
                    "env": [
                        { "name": "OAUTH2_PROXY_CLIENT_ID",
                          "valueFrom": { "secretKeyRef": { "name": secret, "key": "client-id" }}},
                        { "name": "OAUTH2_PROXY_CLIENT_SECRET",
                          "valueFrom": { "secretKeyRef": { "name": secret, "key": "client-secret" }}},
                        { "name": "OAUTH2_PROXY_COOKIE_SECRET",
                          "valueFrom": { "secretKeyRef": { "name": secret, "key": "cookie-secret" }}},
                    ],
                    "ports": [{ "containerPort": 4180, "name": "http" }],
                    "resources": {
                        "requests": { "cpu": "10m", "memory": "32Mi" },
                        "limits":   { "cpu": "200m", "memory": "128Mi" }
                    }
                }]}
            }
        }
    })
}

pub fn oauth2_proxy_service(namespace: &str, owner: &Value) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": OAUTH2_PROXY_NAME, "namespace": namespace,
            "labels": { "app.kubernetes.io/name": OAUTH2_PROXY_NAME },
            "ownerReferences": [owner],
        },
        "spec": {
            "type": "ClusterIP",
            "selector": { "app.kubernetes.io/name": OAUTH2_PROXY_NAME },
            "ports": [{ "name": "http", "port": 4180, "targetPort": "http" }]
        }
    })
}

/// Traefik forward-auth: protected Ingresses delegate auth to oauth2-proxy.
pub fn forward_auth_middleware(namespace: &str, owner: &Value) -> Value {
    json!({
        "apiVersion": "traefik.io/v1alpha1",
        "kind": "Middleware",
        "metadata": {
            "name": MIDDLEWARE_NAME, "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
        },
        "spec": { "forwardAuth": {
            "address": format!("http://{OAUTH2_PROXY_NAME}.{namespace}.svc.cluster.local:4180/oauth2/auth"),
            "trustForwardHeader": true,
            "authResponseHeaders": ["X-Auth-Request-User", "X-Auth-Request-Email"],
        }}
    })
}

/// Traefik `errors` middleware: when forward-auth returns 401/403 (unauthenticated),
/// serve oauth2-proxy's sign-in page instead of a bare "Unauthorized" body. The
/// `{url}` placeholder is Traefik's — it expands to the original request URL so
/// oauth2-proxy can redirect back after login. Chained BEFORE forward-auth.
pub fn auth_errors_middleware(namespace: &str, owner: &Value) -> Value {
    json!({
        "apiVersion": "traefik.io/v1alpha1",
        "kind": "Middleware",
        "metadata": {
            "name": ERRORS_MIDDLEWARE_NAME, "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
        },
        "spec": { "errors": {
            "status": ["401-403"],
            "service": { "name": OAUTH2_PROXY_NAME, "port": 4180 },
            "query": "/oauth2/sign_in?rd={url}",
        }}
    })
}

/// The auth host itself — NOT behind the middleware (it performs the login).
pub fn auth_ingress(ing: &IngressSpec, namespace: &str, owner: &Value) -> Value {
    let host = format!("auth.{}", ing.domain);
    json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "name": "stardelt-auth", "namespace": namespace,
            "labels": super::labels(), "ownerReferences": [owner],
            "annotations": { "traefik.ingress.kubernetes.io/router.entrypoints": "websecure" },
        },
        "spec": {
            "tls": [{ "hosts": [host.clone()], "secretName": WILDCARD_TLS_SECRET }],
            "rules": [{ "host": host, "http": { "paths": [{
                "path": "/", "pathType": "Prefix",
                "backend": { "service": { "name": OAUTH2_PROXY_NAME, "port": { "number": 4180 }}}
            }]}}]
        }
    })
}

/// All protected UIs, sharing the wildcard cert + forward-auth middleware.
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
                // errors middleware first so it wraps forward-auth: a 401/403 from
                // forward-auth is caught and turned into the oauth2-proxy sign-in
                // redirect instead of a bare "Unauthorized" body.
                "traefik.ingress.kubernetes.io/router.middlewares":
                    format!("{namespace}-{ERRORS_MIDDLEWARE_NAME}@kubernetescrd,{namespace}-{MIDDLEWARE_NAME}@kubernetescrd"),
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
    fn oauth2_proxy_wires_org_domain_and_secret() {
        let pi = instance();
        let dep = oauth2_proxy_deployment(ingress(&pi), "stardelt", &json!({}));
        let args = dep["spec"]["template"]["spec"]["containers"][0]["args"]
            .as_array()
            .unwrap();
        let joined: Vec<String> = args
            .iter()
            .map(|a| a.as_str().unwrap().to_string())
            .collect();
        assert!(joined.contains(&"--github-org=stardelt".to_string()));
        assert!(joined.contains(&"--cookie-domain=.lab.stardelt.io".to_string()));
        assert!(
            joined.contains(
                &"--redirect-url=https://auth.lab.stardelt.io/oauth2/callback".to_string()
            )
        );
        assert_eq!(
            dep["spec"]["template"]["spec"]["containers"][0]["image"],
            "quay.io/oauth2-proxy/oauth2-proxy:v7.6.0"
        );
        // credentials come from the named secret
        let env = dep["spec"]["template"]["spec"]["containers"][0]["env"][0].clone();
        assert_eq!(
            env["valueFrom"]["secretKeyRef"]["name"],
            "oauth2-proxy-creds"
        );
    }

    #[test]
    fn oauth2_proxy_service_exposes_4180() {
        let svc = oauth2_proxy_service("stardelt", &json!({}));
        assert_eq!(svc["spec"]["ports"][0]["port"], 4180);
    }

    #[test]
    fn middleware_points_forward_auth_at_oauth2_proxy() {
        let mw = forward_auth_middleware("stardelt", &json!({}));
        assert_eq!(
            mw["spec"]["forwardAuth"]["address"],
            "http://oauth2-proxy.stardelt.svc.cluster.local:4180/oauth2/auth"
        );
    }

    #[test]
    fn auth_ingress_routes_to_oauth2_proxy() {
        let pi = instance();
        let ing_obj = auth_ingress(ingress(&pi), "stardelt", &json!({}));
        assert_eq!(ing_obj["spec"]["rules"][0]["host"], "auth.lab.stardelt.io");
        assert_eq!(
            ing_obj["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"],
            "oauth2-proxy"
        );
    }

    #[test]
    fn errors_middleware_redirects_401_to_sign_in() {
        let mw = auth_errors_middleware("stardelt", &json!({}));
        assert_eq!(mw["spec"]["errors"]["status"][0], "401-403");
        assert_eq!(mw["spec"]["errors"]["service"]["name"], "oauth2-proxy");
        assert_eq!(mw["spec"]["errors"]["service"]["port"], 4180);
        assert_eq!(mw["spec"]["errors"]["query"], "/oauth2/sign_in?rd={url}");
    }

    #[test]
    fn uis_ingress_covers_four_hosts_with_middleware() {
        let pi = instance();
        let ing_obj = uis_ingress(ingress(&pi), "stardelt", &json!({}));
        // errors middleware must precede forward-auth so it wraps the 401/403.
        assert_eq!(
            ing_obj["metadata"]["annotations"]["traefik.ingress.kubernetes.io/router.middlewares"],
            "stardelt-oauth2-errors@kubernetescrd,stardelt-oauth2-forward-auth@kubernetescrd"
        );
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
    }
}
