//! The Keycloak workload for `identity.mode: platform` (master spec §5 row 3): the realm and
//! environment ConfigMaps, the Service and the Deployment. Pure builders: they return typed
//! objects and touch no cluster; [`crate::provision::ensure_keycloak`] applies them.
//!
//! Every field mirrors `stardelt-platform` `manifests/keycloak.yaml` (the Service and the
//! Deployment) and `chart/templates/keycloak.yaml` (the two ConfigMaps), so the operator can
//! take over the identity workload without a Helm chart of its own. Hosts come from
//! `spec.baseDomain`: Nova answers on the base domain, every other application on a
//! subdomain — the chart's `stardelt.appHost`.

use std::collections::BTreeMap;

use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy},
        core::v1::{
            Capabilities, ConfigMap, ConfigMapEnvSource, ConfigMapVolumeSource, Container,
            ContainerPort, EnvFromSource, EnvVar, EnvVarSource, HTTPGetAction, PodSecurityContext,
            PodSpec, PodTemplateSpec, Probe, ResourceRequirements, SeccompProfile,
            SecretKeySelector, SecurityContext, Service, ServicePort, ServiceSpec, Volume,
            VolumeMount,
        },
    },
    apimachinery::pkg::{
        api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
    },
};
use kube::api::ObjectMeta;

use crate::{
    crd::PlatformInstanceSpec,
    identity::{self, IdentityError},
};

/// Name of the Service, the Deployment, the container and of `app.kubernetes.io/name`.
pub const NAME: &str = "keycloak";
/// ConfigMap holding the realm import.
pub const REALM_CONFIG_MAP: &str = "keycloak-realm";
/// ConfigMap holding `KC_HOSTNAME`.
pub const ENV_CONFIG_MAP: &str = "keycloak-env";
/// Key of the realm import inside [`REALM_CONFIG_MAP`].
pub const REALM_KEY: &str = "stardelt-realm.json";
/// Where Keycloak reads realm imports from.
pub const REALM_MOUNT: &str = "/opt/keycloak/data/import";
/// Image of the identity provider. `stardelt-platform` pins the same tag in the Makefile, the
/// manifest and `chart/values.yaml` (their `check-version-parity.sh` keeps the three equal).
pub const IMAGE: &str = "quay.io/keycloak/keycloak:26.8.1";

/// The realm template, verbatim from `stardelt-platform` (`chart/files/keycloak-realm.json`).
/// Hosts are rendered per instance; the client secrets stay `${STARDELT_*_SECRET}` placeholders
/// that Keycloak resolves from the Deployment's environment.
pub const REALM_TEMPLATE: &str = include_str!("keycloak_realm.json");

/// The Realm import of this instance, or the reason it cannot be rendered.
fn realm_json(spec: &PlatformInstanceSpec) -> Result<String, IdentityError> {
    let domain = identity::base_domain(spec)?;
    let mut realm = REALM_TEMPLATE.to_string();
    // Nova answers on the base domain (`stardelt.appHost` with an empty subdomain).
    for (placeholder, host) in [
        ("__BASE_DOMAIN__", domain.to_string()),
        ("__NOVA_HOST__", domain.to_string()),
        ("__TRINO_HOST__", format!("trino.{domain}")),
        ("__LAKEKEEPER_HOST__", format!("lakekeeper.{domain}")),
        ("__SUPERSET_HOST__", format!("superset.{domain}")),
        ("__AIRFLOW_HOST__", format!("airflow.{domain}")),
        ("__JUPYTER_HOST__", format!("jupyter.{domain}")),
    ] {
        realm = realm.replace(placeholder, &host);
    }
    Ok(realm)
}

/// Metadata of an object that belongs to the platform rather than to a Lakehouse.
fn meta(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        labels: Some(BTreeMap::from([(
            "app.kubernetes.io/part-of".to_string(),
            "stardelt".to_string(),
        )])),
        ..ObjectMeta::default()
    }
}

/// The realm import Keycloak reads on first start (`--import-realm`).
pub fn realm_config_map(spec: &PlatformInstanceSpec) -> Result<ConfigMap, IdentityError> {
    Ok(ConfigMap {
        metadata: meta(REALM_CONFIG_MAP, &spec.namespace),
        data: Some(BTreeMap::from([(REALM_KEY.to_string(), realm_json(spec)?)])),
        ..ConfigMap::default()
    })
}

/// The environment of the Deployment: `KC_HOSTNAME` pins the issuer to `https://id.<domain>`.
pub fn env_config_map(spec: &PlatformInstanceSpec) -> Result<ConfigMap, IdentityError> {
    let domain = identity::base_domain(spec)?;
    Ok(ConfigMap {
        metadata: meta(ENV_CONFIG_MAP, &spec.namespace),
        data: Some(BTreeMap::from([(
            "KC_HOSTNAME".to_string(),
            format!("https://id.{domain}"),
        )])),
        ..ConfigMap::default()
    })
}

/// The pod labels, the Deployment selector and the Service selector.
fn name_selector() -> BTreeMap<String, String> {
    BTreeMap::from([("app.kubernetes.io/name".to_string(), NAME.to_string())])
}

fn workload_labels() -> BTreeMap<String, String> {
    let mut labels = name_selector();
    labels.insert(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    );
    labels
}

/// The Service in front of the Deployment; the ingress (installed by the platform, not by
/// this operator) targets it by name.
pub fn service(namespace: &str) -> Service {
    Service {
        metadata: ObjectMeta {
            labels: Some(workload_labels()),
            ..meta(NAME, namespace)
        },
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".into()),
            selector: Some(name_selector()),
            ports: Some(vec![ServicePort {
                name: Some("http".into()),
                port: 8080,
                target_port: Some(IntOrString::String("http".into())),
                ..ServicePort::default()
            }]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    }
}

fn value_env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: Some(value.into()),
        ..EnvVar::default()
    }
}

fn secret_env(name: &str, secret: &str, key: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: secret.into(),
                key: key.into(),
                optional: None,
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    }
}

fn probe(path: &str, period_seconds: i32, failure_threshold: Option<i32>) -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            path: Some(path.into()),
            port: IntOrString::String("management".into()),
            ..HTTPGetAction::default()
        }),
        period_seconds: Some(period_seconds),
        failure_threshold,
        ..Probe::default()
    }
}

fn port(name: &str, container_port: i32) -> ContainerPort {
    ContainerPort {
        name: Some(name.into()),
        container_port,
        ..ContainerPort::default()
    }
}

fn resources() -> ResourceRequirements {
    let quantity = |values: [(&str, &str); 2]| {
        values
            .into_iter()
            .map(|(name, value)| (name.to_string(), Quantity(value.into())))
            .collect::<BTreeMap<_, _>>()
    };
    ResourceRequirements {
        requests: Some(quantity([("cpu", "100m"), ("memory", "512Mi")])),
        limits: Some(quantity([("cpu", "1"), ("memory", "1Gi")])),
        ..ResourceRequirements::default()
    }
}

fn container() -> Container {
    Container {
        name: NAME.into(),
        image: Some(IMAGE.into()),
        // Production mode without `--optimized`: the stock image has no pre-built Postgres
        // config, so Keycloak runs its build step on boot. `--import-realm` creates realm
        // `stardelt` from [`REALM_MOUNT`] on first start and skips it afterwards.
        args: Some(vec!["start".into(), "--import-realm".into()]),
        env_from: Some(vec![EnvFromSource {
            config_map_ref: Some(ConfigMapEnvSource {
                name: ENV_CONFIG_MAP.into(),
                optional: None,
            }),
            ..EnvFromSource::default()
        }]),
        env: Some(vec![
            value_env("KC_DB", "postgres"),
            value_env(
                "KC_DB_URL",
                "jdbc:postgresql://keycloak-pg-rw:5432/keycloak",
            ),
            secret_env("KC_DB_USERNAME", "keycloak-pg-app", "username"),
            secret_env("KC_DB_PASSWORD", "keycloak-pg-app", "password"),
            // TLS terminates at the ingress; trust its X-Forwarded-* headers.
            value_env("KC_HTTP_ENABLED", "true"),
            value_env("KC_PROXY_HEADERS", "xforwarded"),
            value_env("KC_HEALTH_ENABLED", "true"),
            value_env("KC_CACHE", "local"),
            value_env(
                "JAVA_OPTS_KC_HEAP",
                "-XX:MaxRAMPercentage=65 -XX:InitialRAMPercentage=25",
            ),
            secret_env("KC_BOOTSTRAP_ADMIN_USERNAME", "keycloak-admin", "username"),
            secret_env("KC_BOOTSTRAP_ADMIN_PASSWORD", "keycloak-admin", "password"),
            // Resolved by Keycloak's ${...} placeholders in the realm import, so no secret is
            // ever written into the ConfigMap.
            secret_env(
                "STARDELT_REALM_ADMIN_PASSWORD",
                "stardelt-realm-admin",
                "password",
            ),
            secret_env(
                "STARDELT_NOVA_CLIENT_SECRET",
                "stardelt-oidc-clients",
                "nova",
            ),
            secret_env(
                "STARDELT_SUPERSET_CLIENT_SECRET",
                "stardelt-oidc-clients",
                "superset",
            ),
            secret_env(
                "STARDELT_AIRFLOW_CLIENT_SECRET",
                "stardelt-oidc-clients",
                "airflow",
            ),
            secret_env(
                "STARDELT_JUPYTERHUB_CLIENT_SECRET",
                "stardelt-oidc-clients",
                "jupyterhub",
            ),
            // Trino authenticates its OAuth2 client with this secret (Trino 480 rejects a
            // public client at startup).
            secret_env(
                "STARDELT_TRINO_CLIENT_SECRET",
                "stardelt-oidc-clients",
                "trino",
            ),
            // The continuous group-file sync (platform#167) and the scheduled pipelines
            // (platform#118) each run as their own service-account client.
            secret_env(
                "STARDELT_TRINO_GROUP_SYNC_CLIENT_SECRET",
                "stardelt-oidc-clients",
                "trino-group-sync",
            ),
            secret_env(
                "STARDELT_PIPELINE_RUNNER_CLIENT_SECRET",
                "stardelt-oidc-clients",
                "pipeline-runner",
            ),
        ]),
        ports: Some(vec![port("http", 8080), port("management", 9000)]),
        startup_probe: Some(probe("/health/started", 5, Some(60))),
        readiness_probe: Some(probe("/health/ready", 10, None)),
        liveness_probe: Some(probe("/health/live", 20, Some(5))),
        resources: Some(resources()),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".into()]),
                ..Capabilities::default()
            }),
            ..SecurityContext::default()
        }),
        volume_mounts: Some(vec![VolumeMount {
            name: "realm".into(),
            mount_path: REALM_MOUNT.into(),
            read_only: Some(true),
            ..VolumeMount::default()
        }]),
        ..Container::default()
    }
}

/// The Keycloak Deployment. Single replica: `KC_CACHE=local`, no clustering.
pub fn deployment(namespace: &str) -> Deployment {
    Deployment {
        metadata: ObjectMeta {
            labels: Some(workload_labels()),
            ..meta(NAME, namespace)
        },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".into()),
                ..DeploymentStrategy::default()
            }),
            selector: LabelSelector {
                match_labels: Some(name_selector()),
                ..LabelSelector::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(workload_labels()),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    automount_service_account_token: Some(false),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        seccomp_profile: Some(SeccompProfile {
                            type_: "RuntimeDefault".into(),
                            ..SeccompProfile::default()
                        }),
                        ..PodSecurityContext::default()
                    }),
                    containers: vec![container()],
                    volumes: Some(vec![Volume {
                        name: "realm".into(),
                        config_map: Some(ConfigMapVolumeSource {
                            name: REALM_CONFIG_MAP.into(),
                            ..ConfigMapVolumeSource::default()
                        }),
                        ..Volume::default()
                    }]),
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

    /// The placeholder names the platform chart substitutes.
    const PLACEHOLDERS: [&str; 7] = [
        "__BASE_DOMAIN__",
        "__NOVA_HOST__",
        "__TRINO_HOST__",
        "__LAKEKEEPER_HOST__",
        "__SUPERSET_HOST__",
        "__AIRFLOW_HOST__",
        "__JUPYTER_HOST__",
    ];

    fn spec(base_domain: Option<&str>) -> PlatformInstanceSpec {
        serde_json::from_value(serde_json::json!({
            "namespace": "stardelt",
            "identity": { "mode": "platform" },
            "baseDomain": base_domain,
        }))
        .expect("a PlatformInstanceSpec")
    }

    #[test]
    fn the_realm_config_map_has_the_substituted_template_under_its_documented_key() {
        let cm = realm_config_map(&spec(Some("data.example.com"))).expect("a ConfigMap");
        assert_eq!(cm.metadata.name.as_deref(), Some("keycloak-realm"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            cm.metadata.labels.as_ref().unwrap()["app.kubernetes.io/part-of"],
            "stardelt"
        );
        let data = cm.data.expect("data");
        assert_eq!(data.len(), 1, "one key: {data:?}");
        let realm = &data["stardelt-realm.json"];
        for placeholder in PLACEHOLDERS {
            assert!(
                !realm.contains(placeholder),
                "{placeholder} was not substituted"
            );
        }
        assert!(!realm.contains("__"), "a placeholder is left in the realm");
        // The hosts the clients are registered for.
        for host in [
            "admin@data.example.com",
            "https://data.example.com/api/auth/callback",
            "https://trino.data.example.com/oauth2/callback",
            "https://lakekeeper.data.example.com/ui/*",
            "https://superset.data.example.com/oauth-authorized/keycloak",
            "https://airflow.data.example.com/auth/oauth-authorized/keycloak",
            "https://jupyter.data.example.com/hub/oauth_callback",
        ] {
            assert!(realm.contains(host), "the realm has no {host}");
        }
        // Substituting hosts keeps the template a valid realm import, and its client secrets
        // stay ${...} placeholders that Keycloak resolves from the environment.
        let parsed: serde_json::Value = serde_json::from_str(realm).expect("the realm is JSON");
        assert_eq!(parsed["realm"], "stardelt");
        assert_eq!(parsed["users"][0]["email"], "admin@data.example.com");
        assert_eq!(
            parsed["clients"][0]["secret"],
            "${STARDELT_NOVA_CLIENT_SECRET}"
        );
    }

    #[test]
    fn the_realm_follows_the_base_domain_and_needs_one() {
        let realm = realm_config_map(&spec(Some("lab.example.org")))
            .expect("a ConfigMap")
            .data
            .expect("data")
            .remove("stardelt-realm.json")
            .expect("the realm");
        assert!(realm.contains("https://lab.example.org/api/auth/callback"));
        assert!(realm.contains("https://trino.lab.example.org"));
        assert!(realm.contains("admin@lab.example.org"));
        assert!(!realm.contains("data.example.com"));
        let error = realm_config_map(&spec(None)).unwrap_err();
        assert!(error.0.contains("baseDomain"), "{error}");
    }

    #[test]
    fn the_env_config_map_pins_the_issuer_hostname() {
        let cm = env_config_map(&spec(Some("data.example.com"))).expect("a ConfigMap");
        assert_eq!(cm.metadata.name.as_deref(), Some("keycloak-env"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            cm.metadata.labels.as_ref().unwrap()["app.kubernetes.io/part-of"],
            "stardelt"
        );
        assert_eq!(
            cm.data.expect("data")["KC_HOSTNAME"],
            "https://id.data.example.com"
        );
        assert!(env_config_map(&spec(None)).is_err());
    }

    #[test]
    fn the_service_exposes_http_on_8080() {
        let service = service("stardelt");
        assert_eq!(service.metadata.name.as_deref(), Some("keycloak"));
        assert_eq!(service.metadata.namespace.as_deref(), Some("stardelt"));
        let labels = service.metadata.labels.as_ref().expect("labels");
        assert_eq!(labels["app.kubernetes.io/name"], "keycloak");
        assert_eq!(labels["app.kubernetes.io/part-of"], "stardelt");
        let spec = service.spec.expect("spec");
        assert_eq!(spec.type_.as_deref(), Some("ClusterIP"));
        assert_eq!(spec.selector.unwrap()["app.kubernetes.io/name"], "keycloak");
        let ports = spec.ports.expect("ports");
        assert_eq!(ports.len(), 1, "the management port is not exposed");
        assert_eq!(ports[0].name.as_deref(), Some("http"));
        assert_eq!(ports[0].port, 8080);
        assert_eq!(
            ports[0].target_port,
            Some(IntOrString::String("http".into()))
        );
    }

    fn pod(deployment: &Deployment) -> (Container, PodSpec) {
        let spec = deployment.spec.clone().expect("spec");
        let pod = spec.template.spec.expect("pod spec");
        (pod.containers[0].clone(), pod)
    }

    #[test]
    fn the_deployment_mirrors_the_platform_manifest() {
        let deployment = deployment("stardelt");
        assert_eq!(deployment.metadata.name.as_deref(), Some("keycloak"));
        assert_eq!(deployment.metadata.namespace.as_deref(), Some("stardelt"));
        let (container, pod) = pod(&deployment);
        for labels in [
            deployment.metadata.labels.as_ref().expect("labels"),
            deployment
                .spec
                .as_ref()
                .unwrap()
                .template
                .metadata
                .as_ref()
                .unwrap()
                .labels
                .as_ref()
                .expect("pod labels"),
        ] {
            assert_eq!(labels["app.kubernetes.io/name"], "keycloak");
            assert_eq!(labels["app.kubernetes.io/part-of"], "stardelt");
        }
        let spec = deployment.spec.clone().expect("spec");
        assert_eq!(spec.replicas, Some(1));
        assert_eq!(
            spec.strategy.and_then(|s| s.type_),
            Some("Recreate".into()),
            "one replica with KC_CACHE=local must not roll two pods"
        );
        assert_eq!(
            spec.selector.match_labels.unwrap()["app.kubernetes.io/name"],
            "keycloak"
        );

        assert_eq!(container.name, "keycloak");
        assert_eq!(
            container.image.as_deref(),
            Some("quay.io/keycloak/keycloak:26.8.1")
        );
        assert_eq!(
            container
                .args
                .as_ref()
                .map(|args| args.join(" "))
                .as_deref(),
            Some("start --import-realm")
        );
        assert_eq!(
            container.env_from.as_ref().expect("envFrom")[0]
                .config_map_ref
                .as_ref()
                .unwrap()
                .name,
            "keycloak-env"
        );
        let ports = container.ports.as_ref().expect("ports");
        assert_eq!(
            (ports[0].name.as_deref(), ports[0].container_port),
            (Some("http"), 8080)
        );
        assert_eq!(
            (ports[1].name.as_deref(), ports[1].container_port),
            (Some("management"), 9000)
        );

        let startup = container.startup_probe.as_ref().expect("startup probe");
        assert_eq!(
            (startup.period_seconds, startup.failure_threshold),
            (Some(5), Some(60))
        );
        let readiness = container.readiness_probe.as_ref().expect("readiness probe");
        assert_eq!(readiness.period_seconds, Some(10));
        let liveness = container.liveness_probe.as_ref().expect("liveness probe");
        assert_eq!(
            (liveness.period_seconds, liveness.failure_threshold),
            (Some(20), Some(5))
        );
        for probe in [startup, readiness, liveness] {
            let http = probe.http_get.as_ref().expect("an HTTP probe");
            assert_eq!(http.port, IntOrString::String("management".into()));
        }
        assert_eq!(
            startup.http_get.as_ref().unwrap().path.as_deref(),
            Some("/health/started")
        );
        assert_eq!(
            readiness.http_get.as_ref().unwrap().path.as_deref(),
            Some("/health/ready")
        );
        assert_eq!(
            liveness.http_get.as_ref().unwrap().path.as_deref(),
            Some("/health/live")
        );

        let resources = container.resources.expect("resources");
        let requests = resources.requests.expect("requests");
        assert_eq!(requests["cpu"], Quantity("100m".into()));
        assert_eq!(requests["memory"], Quantity("512Mi".into()));
        let limits = resources.limits.expect("limits");
        assert_eq!(limits["cpu"], Quantity("1".into()));
        assert_eq!(limits["memory"], Quantity("1Gi".into()));

        let security = container.security_context.expect("security context");
        assert_eq!(security.allow_privilege_escalation, Some(false));
        assert_eq!(
            security.capabilities.unwrap().drop,
            Some(vec!["ALL".into()])
        );
        assert_eq!(pod.automount_service_account_token, Some(false));
        let pod_security = pod.security_context.expect("pod security context");
        assert_eq!(pod_security.run_as_non_root, Some(true));
        assert_eq!(
            pod_security.seccomp_profile.map(|p| p.type_),
            Some("RuntimeDefault".into())
        );

        let mount = container.volume_mounts.as_ref().expect("volume mounts")[0].clone();
        assert_eq!(mount.name, "realm");
        assert_eq!(mount.mount_path, "/opt/keycloak/data/import");
        assert_eq!(mount.read_only, Some(true));
        let volumes = pod.volumes.expect("volumes");
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].name, "realm");
        assert_eq!(
            volumes[0].config_map.as_ref().unwrap().name,
            "keycloak-realm"
        );
    }

    #[test]
    fn the_deployment_reads_every_secret_the_realm_needs() {
        let (container, _) = pod(&deployment("stardelt"));
        let env = container.env.expect("env");
        let named: Vec<(&str, Option<&str>)> = env
            .iter()
            .map(|e| (e.name.as_str(), e.value.as_deref()))
            .collect();
        assert_eq!(
            named,
            [
                ("KC_DB", Some("postgres")),
                (
                    "KC_DB_URL",
                    Some("jdbc:postgresql://keycloak-pg-rw:5432/keycloak")
                ),
                ("KC_DB_USERNAME", None),
                ("KC_DB_PASSWORD", None),
                ("KC_HTTP_ENABLED", Some("true")),
                ("KC_PROXY_HEADERS", Some("xforwarded")),
                ("KC_HEALTH_ENABLED", Some("true")),
                ("KC_CACHE", Some("local")),
                (
                    "JAVA_OPTS_KC_HEAP",
                    Some("-XX:MaxRAMPercentage=65 -XX:InitialRAMPercentage=25")
                ),
                ("KC_BOOTSTRAP_ADMIN_USERNAME", None),
                ("KC_BOOTSTRAP_ADMIN_PASSWORD", None),
                ("STARDELT_REALM_ADMIN_PASSWORD", None),
                ("STARDELT_NOVA_CLIENT_SECRET", None),
                ("STARDELT_SUPERSET_CLIENT_SECRET", None),
                ("STARDELT_AIRFLOW_CLIENT_SECRET", None),
                ("STARDELT_JUPYTERHUB_CLIENT_SECRET", None),
                ("STARDELT_TRINO_CLIENT_SECRET", None),
                ("STARDELT_TRINO_GROUP_SYNC_CLIENT_SECRET", None),
                ("STARDELT_PIPELINE_RUNNER_CLIENT_SECRET", None),
            ]
        );
        // Every secret reference names the Secret and key `manifests/keycloak.yaml` uses.
        let references: Vec<(&str, &str)> = env
            .iter()
            .filter_map(|e| {
                let selector = e.value_from.as_ref()?.secret_key_ref.as_ref()?;
                Some((selector.name.as_str(), selector.key.as_str()))
            })
            .collect();
        assert_eq!(
            references,
            [
                ("keycloak-pg-app", "username"),
                ("keycloak-pg-app", "password"),
                ("keycloak-admin", "username"),
                ("keycloak-admin", "password"),
                ("stardelt-realm-admin", "password"),
                ("stardelt-oidc-clients", "nova"),
                ("stardelt-oidc-clients", "superset"),
                ("stardelt-oidc-clients", "airflow"),
                ("stardelt-oidc-clients", "jupyterhub"),
                ("stardelt-oidc-clients", "trino"),
                ("stardelt-oidc-clients", "trino-group-sync"),
                ("stardelt-oidc-clients", "pipeline-runner"),
            ]
        );
    }

    #[test]
    fn the_realm_template_is_the_platform_file() {
        assert!(REALM_TEMPLATE.starts_with("{\n  \"realm\": \"stardelt\","));
        for placeholder in PLACEHOLDERS {
            assert!(REALM_TEMPLATE.contains(placeholder), "{placeholder}");
        }
        assert!(
            !REALM_TEMPLATE.contains("__STARDELT"),
            "secrets are ${{...}} placeholders, not host placeholders"
        );
    }
}
