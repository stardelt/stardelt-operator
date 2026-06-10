//! Nova — UI + backend Deployment and its ClusterIP Service.
//!
//! Ported from `stardelt-platform/manifests/nova-deployment.yaml`. Nova proxies
//! Trino and Lakekeeper, so its env points at the in-cluster service DNS.

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

use crate::api::PlatformInstance;
use crate::resources::keycloak;
use crate::resources::keycloak_bootstrap::NOVA_OIDC_SECRET;

pub const NAME: &str = "nova";

fn selector_labels() -> serde_json::Value {
    serde_json::json!({ "app.kubernetes.io/name": "nova" })
}

pub fn deployment(pi: &PlatformInstance, owner: OwnerReference) -> Deployment {
    let ns = &pi.spec.namespace;
    let image = &pi.spec.components.nova.image;

    let json = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": NAME,
            "namespace": ns,
            "labels": super::labels(),
            "ownerReferences": [owner_json(&owner)],
        },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": selector_labels() },
            "template": {
                "metadata": { "labels": {
                    "app.kubernetes.io/name": "nova",
                    "app.kubernetes.io/part-of": "stardelt",
                }},
                "spec": {
                    "containers": [{
                        "name": "nova",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "ports": [{ "containerPort": 8080, "name": "http" }],
                        "env": [
                            { "name": "NOVA_TRINO_URL",
                              "value": format!("http://trino.{ns}.svc.cluster.local:8080") },
                            { "name": "NOVA_LAKEKEEPER_URL",
                              "value": format!("http://lakekeeper.{ns}.svc.cluster.local:8181") },
                            { "name": "NOVA_WAREHOUSE_NAME", "value": "warehouse" },
                            { "name": "NOVA_DEV_USER", "value": "stardelt-dev" },
                            { "name": "RUST_LOG", "value": "info,tower_http=info" },
                        ],
                        "readinessProbe": {
                            "httpGet": { "path": "/api/health", "port": "http" },
                            "initialDelaySeconds": 2,
                            "periodSeconds": 5
                        },
                        "resources": {
                            "requests": { "cpu": "50m", "memory": "64Mi" },
                            "limits":   { "cpu": "1",   "memory": "256Mi" }
                        }
                    }]
                }
            }
        }
    });

    // When SSO is enabled, inject Nova's OIDC config so it authenticates users
    // against Keycloak instead of using the dev-user stub.
    let mut json = json;
    if let Some(sso) = &pi.spec.sso {
        let issuer = keycloak::issuer_url(sso);
        let env = json["spec"]["template"]["spec"]["containers"][0]["env"]
            .as_array_mut()
            .expect("nova env is an array");
        env.push(serde_json::json!({ "name": "NOVA_OIDC_ISSUER", "value": issuer }));
        env.push(serde_json::json!({ "name": "NOVA_OIDC_CLIENT_ID", "value": sso.nova_client_id }));
        env.push(serde_json::json!({
            "name": "NOVA_OIDC_CLIENT_SECRET",
            "valueFrom": { "secretKeyRef": { "name": NOVA_OIDC_SECRET, "key": "client-secret" }}
        }));
        env.push(serde_json::json!({
            "name": "NOVA_PUBLIC_URL",
            "value": format!("https://nova.{}", sso.domain)
        }));
    }

    serde_json::from_value(json).expect("static Nova Deployment JSON is valid")
}

pub fn service(pi: &PlatformInstance, owner: OwnerReference) -> Service {
    let ns = &pi.spec.namespace;
    let json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": NAME,
            "namespace": ns,
            "labels": { "app.kubernetes.io/name": "nova" },
            "ownerReferences": [owner_json(&owner)],
        },
        "spec": {
            "type": "ClusterIP",
            "selector": selector_labels(),
            "ports": [{ "name": "http", "port": 8080, "targetPort": "http" }]
        }
    });
    serde_json::from_value(json).expect("static Nova Service JSON is valid")
}

fn owner_json(owner: &OwnerReference) -> serde_json::Value {
    serde_json::to_value(owner).expect("OwnerReference serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance() -> PlatformInstance {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "platform.stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "t" },
            "spec": { "namespace": "lake" }
        }))
        .unwrap()
    }

    #[test]
    fn deployment_and_service_build() {
        let pi = instance();
        let owner = OwnerReference::default();
        let dep = deployment(&pi, owner.clone());
        assert_eq!(dep.metadata.namespace.as_deref(), Some("lake"));
        let svc = service(&pi, owner);
        assert_eq!(svc.metadata.name.as_deref(), Some(NAME));
    }

    #[test]
    fn deployment_has_oidc_env_when_sso_set() {
        let pi: PlatformInstance = serde_json::from_value(serde_json::json!({
            "apiVersion": "platform.stardelt.io/v1alpha1",
            "kind": "PlatformInstance",
            "metadata": { "name": "t" },
            "spec": { "namespace": "stardelt",
                "sso": { "domain": "lab.stardelt.io", "githubBrokerSecret": "keycloak-github-broker" } }
        }))
        .unwrap();
        let dep = deployment(&pi, OwnerReference::default());
        let env = dep.spec.unwrap().template.spec.unwrap().containers[0]
            .env
            .clone()
            .unwrap();
        let names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"NOVA_OIDC_ISSUER"));
        assert!(names.contains(&"NOVA_OIDC_CLIENT_SECRET"));
    }
}
