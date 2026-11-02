//! Nova, the user interface and API (master spec §5): the objects that give it an identity in
//! the cluster — ServiceAccount, Service and the Roles that say what it may read and change.
//! Pure builders: they return typed objects and touch no cluster. The Deployment, the state
//! store and the controller step that applies them come in later pieces (operator#55,
//! `design/operator-parity.md` step O1a).
//!
//! Every field mirrors `stardelt-platform` `chart/templates/nova.yaml` (the umbrella chart; the
//! manifest `manifests/nova-deployment.yaml` grants the same set since platform#473). There is
//! one definition of Nova's permissions here, and a test that lists them all.

use std::collections::BTreeMap;

use k8s_openapi::api::{
    apps::v1::{Deployment, DeploymentSpec},
    core::v1::{
        Capabilities, ConfigMapKeySelector, Container, ContainerPort, EnvVar, EnvVarSource,
        HTTPGetAction, LocalObjectReference, ObjectFieldSelector, PodSecurityContext, PodSpec,
        PodTemplateSpec, Probe, ResourceRequirements, SeccompProfile, SecretKeySelector,
        SecurityContext, Service, ServiceAccount, ServicePort, ServiceSpec,
    },
    rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
};
use k8s_openapi::apimachinery::pkg::{
    api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
};
use kube::api::ObjectMeta;

use crate::{
    keycloak::IdentitySecret,
    prerequisites::{S3_SECRET, WAREHOUSE, s3_endpoint},
};

/// Name of the ServiceAccount, the Service, the Deployment and of `app.kubernetes.io/name`.
pub const NAME: &str = "nova";
/// Role that lets Nova read the workloads of its namespace (the pins shown on the Health page).
pub const WORKLOAD_READER_ROLE: &str = "nova-workload-reader";
/// Role for the `Connection` resources (the Connections page and the break-glass notice).
pub const CONNECTIONS_ROLE: &str = "nova-connections";
/// Role for the platform resources Nova lists and changes (pipelines, catalogs, warehouses).
pub const PIPELINES_ROLE: &str = "nova-pipelines";
/// The ConfigMap the identity contract is published in (decision D20); Nova reads its sign-in
/// settings from it.
pub const IDENTITY_CONFIG_MAP: &str = "stardelt-identity";
/// Secret that holds the key that signs Nova's session cookies (key `session`).
pub const SESSION_SECRET: &str = "stardelt-nova-session";
/// The session key as a generated Secret: one key, created once and never rotated (rotating it
/// signs everybody out). Nova refuses to start with fewer than 32 bytes; a generated value has 48.
pub const SESSION: IdentitySecret = IdentitySecret {
    name: SESSION_SECRET,
    fixed: &[],
    generated: &["session"],
    fernet: &[],
};
/// Secret that CloudNative-PG generates for the state store; its `uri` key is a complete URL.
pub const DATABASE_SECRET: &str = "nova-pg-app";
/// Bucket for published notebooks (the object store versions it).
pub const NOTEBOOK_BUCKET: &str = "notebooks";

/// The pod labels, the Deployment selector and the Service selector.
pub fn name_selector() -> BTreeMap<String, String> {
    BTreeMap::from([("app.kubernetes.io/name".to_string(), NAME.to_string())])
}

fn part_of() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    )])
}

fn meta(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        labels: Some(part_of()),
        ..ObjectMeta::default()
    }
}

/// The identity Nova's pod runs as.
pub fn service_account(namespace: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: ObjectMeta {
            labels: Some({
                let mut labels = name_selector();
                labels.extend(part_of());
                labels
            }),
            ..meta(NAME, namespace)
        },
        ..ServiceAccount::default()
    }
}

/// The Service in front of Nova: `http` for the user interface and the API, `internal` for the
/// query-log collector (a second listener that no Ingress routes; reachable inside the cluster
/// only).
pub fn service(namespace: &str) -> Service {
    let port = |name: &str, port: i32| ServicePort {
        name: Some(name.into()),
        port,
        target_port: Some(IntOrString::String(name.into())),
        ..ServicePort::default()
    };
    Service {
        metadata: ObjectMeta {
            labels: Some(name_selector()),
            ..meta(NAME, namespace)
        },
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".into()),
            selector: Some(name_selector()),
            ports: Some(vec![port("http", 8080), port("internal", 8081)]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    }
}

fn rule(resources: &[&str], verbs: &[&str], group: &str) -> PolicyRule {
    PolicyRule {
        api_groups: Some(vec![group.into()]),
        resources: Some(resources.iter().map(|r| r.to_string()).collect()),
        verbs: verbs.iter().map(|v| v.to_string()).collect(),
        ..PolicyRule::default()
    }
}

fn role(name: &str, namespace: &str, rules: Vec<PolicyRule>) -> Role {
    Role {
        metadata: meta(name, namespace),
        rules: Some(rules),
    }
}

/// The three Roles of Nova. It writes nothing outside these: no Secret, no wildcard.
pub fn roles(namespace: &str) -> Vec<Role> {
    vec![
        // The pins and the digests shown on the Health page; it writes nothing.
        role(
            WORKLOAD_READER_ROLE,
            namespace,
            vec![rule(
                &["deployments", "statefulsets"],
                &["get", "list"],
                "apps",
            )],
        ),
        // The Connections page lists the resources and, for a group connection, creates and
        // deletes them; `patch` is the break-glass notice (one annotation), without which a
        // break-glass read answers nothing. The credential itself lives in the secret store.
        role(
            CONNECTIONS_ROLE,
            namespace,
            vec![rule(
                &["connections"],
                &["get", "list", "create", "delete", "patch"],
                "stardelt.io",
            )],
        ),
        // The Pipelines page lists `Pipeline` resources and Schedule creates, replaces (`PUT`,
        // hence update not patch) and deletes them; `ServicePrincipal`, `Catalog` and
        // `SqlWarehouse` resources are read for the group, permission and compute views
        // (a catalog is also patched by the administrators' view).
        role(
            PIPELINES_ROLE,
            namespace,
            vec![
                rule(
                    &["pipelines"],
                    &["get", "list", "watch", "create", "update", "delete"],
                    "stardelt.io",
                ),
                rule(&["serviceprincipals"], &["get", "list"], "stardelt.io"),
                rule(&["catalogs"], &["get", "list", "patch"], "stardelt.io"),
                rule(&["sqlwarehouses"], &["get", "list"], "stardelt.io"),
            ],
        ),
    ]
}

/// One RoleBinding per Role, binding it to ServiceAccount `nova`; the binding has the name of
/// its Role.
pub fn role_bindings(namespace: &str) -> Vec<RoleBinding> {
    roles(namespace)
        .into_iter()
        .map(|role| {
            let name = role.metadata.name.clone().unwrap_or_default();
            RoleBinding {
                metadata: meta(&name, namespace),
                role_ref: RoleRef {
                    api_group: Some("rbac.authorization.k8s.io".into()),
                    kind: "Role".into(),
                    name,
                },
                subjects: Some(vec![Subject {
                    kind: "ServiceAccount".into(),
                    name: NAME.into(),
                    namespace: Some(namespace.into()),
                    ..Subject::default()
                }]),
            }
        })
        .collect()
}

fn value_env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: Some(value.into()),
        ..EnvVar::default()
    }
}

fn secret_env(name: &str, secret: &str, key: &str, optional: bool) -> EnvVar {
    EnvVar {
        name: name.into(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: secret.into(),
                key: key.into(),
                optional: Some(optional),
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    }
}

fn identity_env(name: &str, key: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value_from: Some(EnvVarSource {
            config_map_key_ref: Some(ConfigMapKeySelector {
                name: IDENTITY_CONFIG_MAP.into(),
                key: key.into(),
                optional: Some(true),
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    }
}

/// The environment of the Nova container: the settings a lakehouse needs. Every reference to
/// something that may not exist yet (the identity contract, the OIDC client, the session key,
/// the Trino CA, the events token) is optional, so Nova starts without it and runs in
/// development mode; only the state store and the object-store keys are required. The settings
/// for the catalog client, the secret store and Airflow arrive with those components.
fn env(namespace: &str, base_domain: Option<&str>, airflow: bool) -> Vec<EnvVar> {
    let mut env = vec![
        identity_env("NOVA_TRINO_URL", "trinoUrl"),
        secret_env("NOVA_TRINO_CA_PEM", "trino-ca", "tls.crt", true),
        value_env(
            "NOVA_LAKEKEEPER_URL",
            &format!("http://lakekeeper.{namespace}.svc.cluster.local:8181"),
        ),
        value_env("NOVA_WAREHOUSE_NAME", WAREHOUSE),
        value_env("NOVA_DEV_USER", "stardelt-dev"),
        identity_env("NOVA_AUTH_MODE", "authMode"),
        identity_env("NOVA_PUBLIC_URL", "publicUrl"),
        identity_env("NOVA_OIDC_ISSUER", "issuerUrl"),
        value_env("NOVA_OIDC_CLIENT_ID", "nova"),
        secret_env(
            "NOVA_OIDC_CLIENT_SECRET",
            "stardelt-oidc-clients",
            "nova",
            true,
        ),
        secret_env("NOVA_SESSION_SECRET", SESSION_SECRET, "session", true),
        identity_env("NOVA_OIDC_ADMIN_GROUP", "adminGroup"),
        value_env("NOVA_S3_URL", &s3_endpoint(namespace)),
        secret_env("NOVA_S3_ACCESS_KEY", S3_SECRET, "access-key", false),
        secret_env("NOVA_S3_SECRET_KEY", S3_SECRET, "secret-key", false),
        value_env("NOVA_S3_NOTEBOOK_BUCKET", NOTEBOOK_BUCKET),
        // Forward the signed-in person's own token to Trino instead of acting as one shared user.
        value_env("NOVA_FORWARD_IDENTITY", "1"),
        EnvVar {
            name: "POD_NAMESPACE".into(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.namespace".into(),
                    ..ObjectFieldSelector::default()
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        },
        // Not optional: a missing state store keeps the pod in CreateContainerConfigError
        // instead of starting a Nova that silently runs without its database.
        secret_env("NOVA_DATABASE_URL", DATABASE_SECRET, "uri", false),
        // The query-log collector: a second listener that no Ingress routes.
        value_env("NOVA_INTERNAL_ADDR", "0.0.0.0:8081"),
        secret_env("NOVA_EVENTS_TOKEN", "nova-events", "token", true),
        value_env("RUST_LOG", "info,tower_http=info"),
    ];
    if let Some(domain) = base_domain {
        env.push(value_env("NOVA_BASE_DOMAIN", domain));
    }
    // The Pipelines page reads runs and logs with the service user Airflow's install creates. The
    // address is the Service of the release `airflow` in this namespace.
    if airflow {
        env.push(value_env(
            "NOVA_AIRFLOW_URL",
            "http://airflow-api-server:8080",
        ));
        env.push(value_env("NOVA_AIRFLOW_USER", "nova-api"));
        env.push(secret_env(
            "NOVA_AIRFLOW_PASSWORD",
            crate::airflow::NOVA_API_SECRET,
            "password",
            false,
        ));
    }
    env
}

fn quantities(values: [(&str, &str); 2]) -> std::collections::BTreeMap<String, Quantity> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_string(), Quantity(value.into())))
        .collect()
}

fn container(image: &str, namespace: &str, base_domain: Option<&str>, airflow: bool) -> Container {
    let port = |name: &str, container_port: i32| ContainerPort {
        name: Some(name.into()),
        container_port,
        ..ContainerPort::default()
    };
    Container {
        name: NAME.into(),
        image: Some(image.into()),
        // Digest-pinned images never change under a tag, so a cached image stays usable when
        // the registry is unreachable.
        image_pull_policy: Some("IfNotPresent".into()),
        ports: Some(vec![port("http", 8080), port("internal", 8081)]),
        env: Some(env(namespace, base_domain, airflow)),
        readiness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/api/health".into()),
                port: IntOrString::String("http".into()),
                ..HTTPGetAction::default()
            }),
            initial_delay_seconds: Some(2),
            period_seconds: Some(5),
            ..Probe::default()
        }),
        resources: Some(ResourceRequirements {
            requests: Some(quantities([("cpu", "50m"), ("memory", "64Mi")])),
            limits: Some(quantities([("cpu", "1"), ("memory", "256Mi")])),
            ..ResourceRequirements::default()
        }),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            read_only_root_filesystem: Some(true),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".into()]),
                ..Capabilities::default()
            }),
            ..SecurityContext::default()
        }),
        ..Container::default()
    }
}

/// The Nova Deployment: one replica, running as ServiceAccount `nova` without root and with a
/// read-only filesystem. `image` is the reference to run (pinned by digest in a real install);
/// `base_domain` is the domain the applications are served under, used for the links Nova shows
/// to the other applications (none when it is `None`). `airflow` adds the three variables that
/// point Nova at the Airflow of the `pipelines` pillar.
pub fn deployment(
    namespace: &str,
    image: &str,
    base_domain: Option<&str>,
    airflow: bool,
) -> Deployment {
    let mut labels = name_selector();
    labels.extend(part_of());
    Deployment {
        metadata: ObjectMeta {
            labels: Some(labels.clone()),
            ..meta(NAME, namespace)
        },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(name_selector()),
                ..LabelSelector::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    service_account_name: Some(NAME.into()),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        run_as_user: Some(65532),
                        run_as_group: Some(65532),
                        seccomp_profile: Some(SeccompProfile {
                            type_: "RuntimeDefault".into(),
                            ..SeccompProfile::default()
                        }),
                        ..PodSecurityContext::default()
                    }),
                    // The registry credential of a private image; a missing Secret only means
                    // an anonymous pull.
                    image_pull_secrets: Some(vec![LocalObjectReference {
                        name: "ghcr-creds".into(),
                    }]),
                    containers: vec![container(image, namespace, base_domain, airflow)],
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        }),
        ..Deployment::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Every (apiGroup, resource, verb) that Nova may use. 22 entries: the list that
    /// `scripts/check-nova-rbac-parity.sh` in stardelt-platform compares between the manifest
    /// and the chart.
    const GRANTED: [(&str, &str, &str); 22] = [
        ("apps", "deployments", "get"),
        ("apps", "deployments", "list"),
        ("apps", "statefulsets", "get"),
        ("apps", "statefulsets", "list"),
        ("stardelt.io", "connections", "create"),
        ("stardelt.io", "connections", "delete"),
        ("stardelt.io", "connections", "get"),
        ("stardelt.io", "connections", "list"),
        ("stardelt.io", "connections", "patch"),
        ("stardelt.io", "pipelines", "create"),
        ("stardelt.io", "pipelines", "delete"),
        ("stardelt.io", "pipelines", "get"),
        ("stardelt.io", "pipelines", "list"),
        ("stardelt.io", "pipelines", "update"),
        ("stardelt.io", "pipelines", "watch"),
        ("stardelt.io", "serviceprincipals", "get"),
        ("stardelt.io", "serviceprincipals", "list"),
        ("stardelt.io", "catalogs", "get"),
        ("stardelt.io", "catalogs", "list"),
        ("stardelt.io", "catalogs", "patch"),
        ("stardelt.io", "sqlwarehouses", "get"),
        ("stardelt.io", "sqlwarehouses", "list"),
    ];

    fn granted() -> BTreeSet<(String, String, String)> {
        let mut pairs = BTreeSet::new();
        for role in roles("stardelt") {
            for rule in role.rules.unwrap_or_default() {
                for group in rule.api_groups.unwrap_or_default() {
                    for resource in rule.resources.clone().unwrap_or_default() {
                        for verb in &rule.verbs {
                            pairs.insert((group.clone(), resource.clone(), verb.clone()));
                        }
                    }
                }
            }
        }
        pairs
    }

    #[test]
    fn the_session_secret_has_one_generated_key_and_no_fixed_value() {
        assert_eq!(SESSION.name, "stardelt-nova-session");
        assert!(SESSION.fixed.is_empty());
        assert_eq!(SESSION.key_names(), ["session"]);
    }

    #[test]
    fn nova_may_do_exactly_what_the_platform_grants() {
        let expected: BTreeSet<(String, String, String)> = GRANTED
            .iter()
            .map(|(g, r, v)| (g.to_string(), r.to_string(), v.to_string()))
            .collect();
        assert_eq!(granted(), expected);
    }

    #[test]
    fn no_permission_is_a_wildcard_or_reaches_secrets() {
        for (_, resource, verb) in granted() {
            assert_ne!(resource, "*");
            assert_ne!(verb, "*");
            assert!(!resource.contains("secrets"), "{resource}");
        }
    }

    #[test]
    fn the_roles_have_the_platform_names() {
        let names: Vec<_> = roles("stardelt")
            .iter()
            .map(|r| r.metadata.name.clone().unwrap())
            .collect();
        assert_eq!(
            names,
            ["nova-workload-reader", "nova-connections", "nova-pipelines"]
        );
    }

    #[test]
    fn every_role_is_bound_to_the_nova_service_account_in_its_namespace() {
        let bindings = role_bindings("lab");
        assert_eq!(bindings.len(), 3);
        for binding in bindings {
            assert_eq!(binding.metadata.namespace.as_deref(), Some("lab"));
            assert_eq!(binding.role_ref.kind, "Role");
            assert_eq!(
                binding.role_ref.api_group.as_deref(),
                Some("rbac.authorization.k8s.io")
            );
            assert_eq!(
                binding.metadata.name.as_deref(),
                Some(binding.role_ref.name.as_str())
            );
            let subjects = binding.subjects.expect("subjects");
            assert_eq!(subjects.len(), 1);
            assert_eq!(subjects[0].kind, "ServiceAccount");
            assert_eq!(subjects[0].name, "nova");
            assert_eq!(subjects[0].namespace.as_deref(), Some("lab"));
        }
        for role in roles("lab") {
            assert_eq!(role.metadata.namespace.as_deref(), Some("lab"));
        }
    }

    #[test]
    fn the_service_account_carries_the_platform_labels() {
        let account = service_account("stardelt");
        assert_eq!(account.metadata.name.as_deref(), Some("nova"));
        assert_eq!(account.metadata.namespace.as_deref(), Some("stardelt"));
        let labels = account.metadata.labels.expect("labels");
        assert_eq!(labels["app.kubernetes.io/name"], "nova");
        assert_eq!(labels["app.kubernetes.io/part-of"], "stardelt");
    }

    #[test]
    fn the_service_has_the_user_port_and_the_internal_collector_port() {
        let service = service("stardelt");
        assert_eq!(service.metadata.name.as_deref(), Some("nova"));
        let spec = service.spec.expect("spec");
        assert_eq!(spec.type_.as_deref(), Some("ClusterIP"));
        assert_eq!(
            spec.selector.expect("selector")["app.kubernetes.io/name"],
            "nova"
        );
        let ports: Vec<_> = spec
            .ports
            .expect("ports")
            .into_iter()
            .map(|p| (p.name.unwrap(), p.port, p.target_port.unwrap()))
            .collect();
        assert_eq!(
            ports,
            [
                ("http".to_string(), 8080, IntOrString::String("http".into())),
                (
                    "internal".to_string(),
                    8081,
                    IntOrString::String("internal".into())
                ),
            ]
        );
    }

    const IMAGE: &str = "ghcr.io/stardelt/nova:dev@sha256:0000000000000000000000000000000000000000000000000000000000000000";

    fn container_of(deployment: &Deployment) -> Container {
        let pod = deployment
            .spec
            .clone()
            .expect("spec")
            .template
            .spec
            .expect("pod");
        assert_eq!(pod.containers.len(), 1);
        pod.containers[0].clone()
    }

    fn env_of(deployment: &Deployment) -> Vec<EnvVar> {
        container_of(deployment).env.expect("env")
    }

    fn named(env: &[EnvVar], name: &str) -> EnvVar {
        env.iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("no variable {name}"))
            .clone()
    }

    #[test]
    fn the_deployment_runs_the_given_image_as_the_nova_service_account() {
        let deployment = deployment("stardelt", IMAGE, None, false);
        assert_eq!(deployment.metadata.name.as_deref(), Some("nova"));
        assert_eq!(deployment.metadata.namespace.as_deref(), Some("stardelt"));
        let spec = deployment.spec.clone().expect("spec");
        assert_eq!(spec.replicas, Some(1));
        let pod = spec.template.spec.expect("pod");
        assert_eq!(pod.service_account_name.as_deref(), Some("nova"));
        let container = &pod.containers[0];
        assert_eq!(container.name, "nova");
        assert_eq!(container.image.as_deref(), Some(IMAGE));
        assert_eq!(container.image_pull_policy.as_deref(), Some("IfNotPresent"));
        // The selector matches the pod labels, and the Service selects the same pods.
        let selector = spec.selector.match_labels.expect("selector");
        let labels = spec
            .template
            .metadata
            .expect("pod metadata")
            .labels
            .expect("pod labels");
        for (key, value) in &selector {
            assert_eq!(labels.get(key), Some(value));
        }
        assert_eq!(selector, name_selector());
    }

    #[test]
    fn the_pod_is_locked_down() {
        let deployment = deployment("stardelt", IMAGE, None, false);
        let pod = deployment
            .spec
            .clone()
            .expect("spec")
            .template
            .spec
            .expect("pod");
        let pod_security = pod.security_context.expect("pod security");
        assert_eq!(pod_security.run_as_non_root, Some(true));
        assert_eq!(pod_security.run_as_user, Some(65532));
        assert_eq!(pod_security.run_as_group, Some(65532));
        assert_eq!(
            pod_security.seccomp_profile.expect("seccomp").type_,
            "RuntimeDefault"
        );
        let security = container_of(&deployment)
            .security_context
            .expect("security");
        assert_eq!(security.allow_privilege_escalation, Some(false));
        assert_eq!(security.read_only_root_filesystem, Some(true));
        assert_eq!(
            security.capabilities.expect("caps").drop,
            Some(vec!["ALL".to_string()])
        );
    }

    #[test]
    fn the_container_ports_are_the_ones_the_service_targets() {
        let container = container_of(&deployment("stardelt", IMAGE, None, false));
        let ports: Vec<_> = container
            .ports
            .expect("ports")
            .into_iter()
            .map(|p| (p.name.unwrap(), p.container_port))
            .collect();
        assert_eq!(
            ports,
            [("http".to_string(), 8080), ("internal".to_string(), 8081)]
        );
        let probe = container
            .readiness_probe
            .expect("probe")
            .http_get
            .expect("get");
        assert_eq!(probe.path.as_deref(), Some("/api/health"));
        assert_eq!(probe.port, IntOrString::String("http".into()));
    }

    #[test]
    fn the_environment_is_exactly_the_core_set() {
        let env = env_of(&deployment("stardelt", IMAGE, None, false));
        let names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "NOVA_TRINO_URL",
                "NOVA_TRINO_CA_PEM",
                "NOVA_LAKEKEEPER_URL",
                "NOVA_WAREHOUSE_NAME",
                "NOVA_DEV_USER",
                "NOVA_AUTH_MODE",
                "NOVA_PUBLIC_URL",
                "NOVA_OIDC_ISSUER",
                "NOVA_OIDC_CLIENT_ID",
                "NOVA_OIDC_CLIENT_SECRET",
                "NOVA_SESSION_SECRET",
                "NOVA_OIDC_ADMIN_GROUP",
                "NOVA_S3_URL",
                "NOVA_S3_ACCESS_KEY",
                "NOVA_S3_SECRET_KEY",
                "NOVA_S3_NOTEBOOK_BUCKET",
                "NOVA_FORWARD_IDENTITY",
                "POD_NAMESPACE",
                "NOVA_DATABASE_URL",
                "NOVA_INTERNAL_ADDR",
                "NOVA_EVENTS_TOKEN",
                "RUST_LOG",
            ]
        );
    }

    #[test]
    fn only_the_state_store_and_the_object_store_keys_are_required() {
        let env = env_of(&deployment("stardelt", IMAGE, None, false));
        let mut required = Vec::new();
        for var in &env {
            let Some(source) = &var.value_from else {
                continue;
            };
            let optional = source
                .secret_key_ref
                .as_ref()
                .map(|s| s.optional)
                .or_else(|| source.config_map_key_ref.as_ref().map(|c| c.optional));
            if optional == Some(Some(false)) {
                required.push(var.name.as_str());
            }
        }
        assert_eq!(
            required,
            [
                "NOVA_S3_ACCESS_KEY",
                "NOVA_S3_SECRET_KEY",
                "NOVA_DATABASE_URL"
            ]
        );
    }

    #[test]
    fn the_secrets_are_read_from_the_names_the_platform_uses() {
        let env = env_of(&deployment("stardelt", IMAGE, None, false));
        let secret = |name: &str| {
            let selector = named(&env, name)
                .value_from
                .and_then(|s| s.secret_key_ref)
                .unwrap_or_else(|| panic!("{name} is not a Secret reference"));
            (selector.name, selector.key)
        };
        assert_eq!(
            secret("NOVA_DATABASE_URL"),
            ("nova-pg-app".to_string(), "uri".to_string())
        );
        assert_eq!(
            secret("NOVA_OIDC_CLIENT_SECRET"),
            ("stardelt-oidc-clients".to_string(), "nova".to_string())
        );
        assert_eq!(
            secret("NOVA_SESSION_SECRET"),
            ("stardelt-nova-session".to_string(), "session".to_string())
        );
        assert_eq!(
            secret("NOVA_S3_ACCESS_KEY"),
            ("stardelt-s3-creds".to_string(), "access-key".to_string())
        );
        assert_eq!(
            secret("NOVA_S3_SECRET_KEY"),
            ("stardelt-s3-creds".to_string(), "secret-key".to_string())
        );
        let config = |name: &str| {
            let selector = named(&env, name)
                .value_from
                .and_then(|s| s.config_map_key_ref)
                .unwrap_or_else(|| panic!("{name} is not a ConfigMap reference"));
            (selector.name, selector.key)
        };
        for (name, key) in [
            ("NOVA_AUTH_MODE", "authMode"),
            ("NOVA_PUBLIC_URL", "publicUrl"),
            ("NOVA_OIDC_ISSUER", "issuerUrl"),
            ("NOVA_OIDC_ADMIN_GROUP", "adminGroup"),
            ("NOVA_TRINO_URL", "trinoUrl"),
        ] {
            assert_eq!(
                config(name),
                ("stardelt-identity".to_string(), key.to_string())
            );
        }
    }

    #[test]
    fn the_addresses_follow_the_namespace() {
        let env = env_of(&deployment("lab", IMAGE, None, false));
        let value = |name: &str| named(&env, name).value.expect(name);
        assert_eq!(
            value("NOVA_LAKEKEEPER_URL"),
            "http://lakekeeper.lab.svc.cluster.local:8181"
        );
        assert_eq!(
            value("NOVA_S3_URL"),
            "http://seaweedfs-s3.lab.svc.cluster.local:8333"
        );
        assert_eq!(value("NOVA_WAREHOUSE_NAME"), "warehouse");
        assert_eq!(value("NOVA_S3_NOTEBOOK_BUCKET"), "notebooks");
        assert_eq!(value("NOVA_INTERNAL_ADDR"), "0.0.0.0:8081");
        assert_eq!(value("NOVA_FORWARD_IDENTITY"), "1");
    }

    #[test]
    fn the_base_domain_is_set_only_when_given() {
        let without = env_of(&deployment("stardelt", IMAGE, None, false));
        assert!(without.iter().all(|e| e.name != "NOVA_BASE_DOMAIN"));
        let with = env_of(&deployment(
            "stardelt",
            IMAGE,
            Some("data.example.com"),
            false,
        ));
        assert_eq!(
            named(&with, "NOVA_BASE_DOMAIN").value.as_deref(),
            Some("data.example.com")
        );
        assert_eq!(with.len(), without.len() + 1);
    }

    #[test]
    fn with_the_pipelines_pillar_nova_gets_the_airflow_address_user_and_password() {
        let without = env_of(&deployment("lab", IMAGE, None, false));
        assert!(without.iter().all(|e| !e.name.starts_with("NOVA_AIRFLOW")));
        let env = env_of(&deployment("lab", IMAGE, None, true));
        let added: Vec<&str> = env
            .iter()
            .map(|e| e.name.as_str())
            .filter(|name| name.starts_with("NOVA_AIRFLOW"))
            .collect();
        assert_eq!(
            added,
            [
                "NOVA_AIRFLOW_URL",
                "NOVA_AIRFLOW_USER",
                "NOVA_AIRFLOW_PASSWORD"
            ]
        );
        assert_eq!(env.len(), without.len() + 3);
        assert_eq!(
            named(&env, "NOVA_AIRFLOW_URL").value.as_deref(),
            Some("http://airflow-api-server:8080")
        );
        assert_eq!(
            named(&env, "NOVA_AIRFLOW_USER").value.as_deref(),
            Some("nova-api")
        );
        let password = named(&env, "NOVA_AIRFLOW_PASSWORD")
            .value_from
            .and_then(|s| s.secret_key_ref)
            .expect("a Secret reference");
        assert_eq!(password.name, "airflow-nova-api");
        assert_eq!(password.key, "password");
        assert_eq!(
            password.optional,
            Some(false),
            "a missing password keeps the pod from starting instead of running without it"
        );
    }
}
