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
    core::v1::{Service, ServiceAccount, ServicePort, ServiceSpec},
    rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::ObjectMeta;

/// Name of the ServiceAccount, the Service, the Deployment and of `app.kubernetes.io/name`.
pub const NAME: &str = "nova";
/// Role that lets Nova read the workloads of its namespace (the pins shown on the Health page).
pub const WORKLOAD_READER_ROLE: &str = "nova-workload-reader";
/// Role for the `Connection` resources (the Connections page and the break-glass notice).
pub const CONNECTIONS_ROLE: &str = "nova-connections";
/// Role for the platform resources Nova lists and changes (pipelines, catalogs, warehouses).
pub const PIPELINES_ROLE: &str = "nova-pipelines";

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
}
