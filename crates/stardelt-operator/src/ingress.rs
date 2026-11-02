//! The Ingress objects of the platform's applications (master spec §5, step O2 of
//! `design/operator-parity.md`). Pure builders: they return typed objects and touch no cluster;
//! the controller step that applies them comes later.
//!
//! Every field mirrors `stardelt-platform` `chart/templates/ingress.yaml`: Nova answers on
//! `spec.baseDomain` itself and every other application on a subdomain (the chart's
//! `stardelt.appHost`); the class, the cert-manager issuer annotation and the TLS block come
//! from `spec.ingress`. Only Nova and the identity provider are built here: the other
//! applications arrive with their components.

use std::collections::BTreeMap;

use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressRule,
    IngressServiceBackend, IngressSpec, IngressTLS, ServiceBackendPort,
};
use kube::api::ObjectMeta;

use crate::{
    crd::{IdentityMode, PlatformInstanceSpec},
    keycloak, nova,
};

/// The host an application answers on: the base domain itself for an empty `subdomain`, else
/// `<subdomain>.<base domain>`.
pub fn host(base_domain: &str, subdomain: &str) -> String {
    if subdomain.is_empty() {
        base_domain.to_string()
    } else {
        format!("{subdomain}.{base_domain}")
    }
}

/// The name of the TLS Secret: `template` with `{host}` replaced by the host name with its dots
/// turned into dashes.
pub fn secret_name(template: &str, host: &str) -> String {
    template.replace("{host}", &host.replace('.', "-"))
}

/// What the identity provider publishes: the realm's public surface and the admin console and
/// REST API of the `stardelt` realm, never the master realm. These are ingress-nginx regular
/// expressions, each anchored to its path segment.
pub const KEYCLOAK_PATHS: [&str; 6] = [
    "/realms/stardelt(/|$)",
    "/resources(/|$)",
    "/robots\\.txt$",
    "/admin/stardelt(/|$)",
    "/admin/realms/stardelt(/|$)",
    "/admin/serverinfo$",
];

/// The Ingress of Nova, on the base domain. `None` unless `spec.ingress.enabled` and a base
/// domain is set.
pub fn nova(spec: &PlatformInstanceSpec) -> Option<Ingress> {
    build(spec, nova::NAME, nova::NAME, 8080, "", &["/"], false)
}

/// The Ingress of the identity provider, on `id.<base domain>`, with `identity.mode: platform`
/// only. `None` otherwise, like [`nova`].
pub fn keycloak(spec: &PlatformInstanceSpec) -> Option<Ingress> {
    if spec.identity.mode != IdentityMode::Platform {
        return None;
    }
    build(
        spec,
        keycloak::NAME,
        keycloak::NAME,
        8080,
        "id",
        &KEYCLOAK_PATHS,
        true,
    )
}

fn build(
    spec: &PlatformInstanceSpec,
    name: &str,
    service: &str,
    port: i32,
    subdomain: &str,
    paths: &[&str],
    regex: bool,
) -> Option<Ingress> {
    let settings = &spec.ingress;
    if !settings.enabled {
        return None;
    }
    let host = host(spec.base_domain.as_deref()?, subdomain);
    let mut annotations = BTreeMap::new();
    if settings.tls.enabled
        && let Some(issuer) = settings.tls.issuer.as_deref().filter(|i| !i.is_empty())
    {
        annotations.insert(
            "cert-manager.io/cluster-issuer".to_string(),
            issuer.to_string(),
        );
    }
    if regex {
        annotations.insert(
            "nginx.ingress.kubernetes.io/use-regex".to_string(),
            "true".to_string(),
        );
    }
    let backend = |service: &str| IngressBackend {
        service: Some(IngressServiceBackend {
            name: service.to_string(),
            port: Some(ServiceBackendPort {
                number: Some(port),
                ..ServiceBackendPort::default()
            }),
        }),
        ..IngressBackend::default()
    };
    Some(Ingress {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(spec.namespace.clone()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/part-of".to_string(),
                "stardelt".to_string(),
            )])),
            annotations: (!annotations.is_empty()).then_some(annotations),
            ..ObjectMeta::default()
        },
        spec: Some(IngressSpec {
            ingress_class_name: settings.class_name.clone(),
            tls: settings.tls.enabled.then(|| {
                vec![IngressTLS {
                    hosts: Some(vec![host.clone()]),
                    secret_name: Some(secret_name(&settings.tls.secret_name_template, &host)),
                }]
            }),
            rules: Some(vec![IngressRule {
                host: Some(host),
                http: Some(HTTPIngressRuleValue {
                    paths: paths
                        .iter()
                        .map(|path| HTTPIngressPath {
                            path: Some(path.to_string()),
                            // A path that is a regular expression is not a prefix.
                            path_type: if regex {
                                "ImplementationSpecific".to_string()
                            } else {
                                "Prefix".to_string()
                            },
                            backend: backend(service),
                        })
                        .collect(),
                }),
            }]),
            ..IngressSpec::default()
        }),
        ..Ingress::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(extra: serde_json::Value) -> PlatformInstanceSpec {
        let mut value = serde_json::json!({
            "namespace": "stardelt",
            "baseDomain": "data.example.com",
            "ingress": { "enabled": true },
        });
        for (key, item) in extra.as_object().expect("an object") {
            value[key] = item.clone();
        }
        serde_json::from_value(value).expect("a PlatformInstanceSpec")
    }

    fn rule_of(ingress: &Ingress) -> (String, Vec<(String, String)>) {
        let rule = ingress.spec.clone().expect("spec").rules.expect("rules")[0].clone();
        let paths = rule
            .http
            .expect("http")
            .paths
            .into_iter()
            .map(|p| (p.path.expect("path"), p.path_type))
            .collect();
        (rule.host.expect("host"), paths)
    }

    #[test]
    fn hosts_are_the_base_domain_or_a_subdomain_of_it() {
        assert_eq!(host("data.example.com", ""), "data.example.com");
        assert_eq!(host("data.example.com", "id"), "id.data.example.com");
    }

    #[test]
    fn the_secret_name_turns_the_dots_of_the_host_into_dashes() {
        assert_eq!(
            secret_name("{host}-tls", "id.data.example.com"),
            "id-data-example-com-tls"
        );
        assert_eq!(secret_name("tls-{host}", "a.b"), "tls-a-b");
    }

    #[test]
    fn nothing_is_built_unless_ingress_is_enabled_and_a_domain_is_set() {
        let off = spec(serde_json::json!({ "ingress": { "enabled": false } }));
        assert!(nova(&off).is_none());
        let no_domain = spec(serde_json::json!({ "baseDomain": null }));
        assert!(nova(&no_domain).is_none());
    }

    #[test]
    fn nova_answers_on_the_base_domain_and_nothing_more() {
        let ingress = nova(&spec(serde_json::json!({}))).expect("an Ingress");
        assert_eq!(ingress.metadata.name.as_deref(), Some("nova"));
        assert_eq!(ingress.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            ingress.metadata.labels.as_ref().expect("labels")["app.kubernetes.io/part-of"],
            "stardelt"
        );
        assert!(ingress.metadata.annotations.is_none(), "no TLS, no regex");
        let (host, paths) = rule_of(&ingress);
        assert_eq!(host, "data.example.com");
        assert_eq!(paths, [("/".to_string(), "Prefix".to_string())]);
        let spec = ingress.spec.expect("spec");
        assert!(
            spec.ingress_class_name.is_none(),
            "the cluster default class"
        );
        assert!(spec.tls.is_none());
        let backend = spec.rules.expect("rules")[0]
            .http
            .clone()
            .expect("http")
            .paths[0]
            .backend
            .service
            .clone()
            .expect("service");
        assert_eq!(backend.name, "nova");
        assert_eq!(backend.port.expect("port").number, Some(8080));
    }

    #[test]
    fn the_class_and_tls_come_from_the_settings() {
        let spec = spec(serde_json::json!({ "ingress": {
            "enabled": true,
            "className": "traefik",
            "tls": { "enabled": true, "issuer": "letsencrypt" }
        }}));
        let ingress = nova(&spec).expect("an Ingress");
        assert_eq!(
            ingress.metadata.annotations.as_ref().expect("annotations")["cert-manager.io/cluster-issuer"],
            "letsencrypt"
        );
        let inner = ingress.spec.expect("spec");
        assert_eq!(inner.ingress_class_name.as_deref(), Some("traefik"));
        let tls = inner.tls.expect("tls");
        assert_eq!(tls.len(), 1);
        assert_eq!(tls[0].hosts, Some(vec!["data.example.com".to_string()]));
        assert_eq!(tls[0].secret_name.as_deref(), Some("data-example-com-tls"));
    }

    #[test]
    fn the_identity_provider_publishes_only_the_stardelt_realm() {
        let ingress = keycloak(&spec(
            serde_json::json!({ "identity": { "mode": "platform" } }),
        ))
        .expect("an Ingress");
        assert_eq!(ingress.metadata.name.as_deref(), Some("keycloak"));
        assert_eq!(
            ingress.metadata.annotations.as_ref().expect("annotations")["nginx.ingress.kubernetes.io/use-regex"],
            "true"
        );
        let (host, paths) = rule_of(&ingress);
        assert_eq!(host, "id.data.example.com");
        assert_eq!(paths.len(), 6);
        assert!(
            paths
                .iter()
                .all(|(_, kind)| kind == "ImplementationSpecific")
        );
        assert!(
            paths
                .iter()
                .all(|(path, _)| path != "/" && !path.starts_with("/admin/master")),
            "the master realm and the root stay unreachable: {paths:?}"
        );
        assert!(paths.contains(&(
            "/realms/stardelt(/|$)".to_string(),
            "ImplementationSpecific".to_string()
        )));
    }

    #[test]
    fn the_identity_provider_has_an_ingress_only_with_mode_platform() {
        for mode in ["none", "external"] {
            let value = serde_json::json!({ "identity": { "mode": mode, "issuerUrl": "https://idp.example.com",
                "apiAudience": "api", "clients": { "nova": { "clientId": "nova", "clientSecretRef": { "name": "s" } } } } });
            assert!(keycloak(&spec(value)).is_none(), "{mode}");
        }
    }
}
