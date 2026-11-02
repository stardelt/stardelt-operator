//! Airflow (the `pipelines` pillar of `design/operator-parity.md`, step O6): the two pieces that
//! need no decision, as pure builders. The chart, its values, the images and the controller step
//! that applies all of it come later; nothing here is called yet.
//!
//! Every field mirrors `stardelt-platform`: the Role is `manifests/airflow-pipelines.yaml`, the
//! Secrets are the Airflow part of `scripts/app-secrets.sh`.

use std::collections::BTreeMap;

use k8s_openapi::api::rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject};
use kube::api::ObjectMeta;

use crate::keycloak::IdentitySecret;

/// Role that lets Airflow read the `Pipeline` and `ServicePrincipal` resources it turns into DAGs.
pub const PIPELINE_READER_ROLE: &str = "airflow-pipeline-reader";
/// The ServiceAccounts of the Airflow chart that read them: the DAG processor, which builds the
/// DAGs, and the worker, which runs the tasks.
pub const PIPELINE_READERS: [&str; 2] = ["airflow-dag-processor", "airflow-worker"];

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

/// Read access to the pipelines and the service principals of the namespace, nothing else.
pub fn pipeline_reader_role(namespace: &str) -> Role {
    Role {
        metadata: meta(PIPELINE_READER_ROLE, namespace),
        rules: Some(vec![PolicyRule {
            api_groups: Some(vec!["stardelt.io".into()]),
            resources: Some(vec!["pipelines".into(), "serviceprincipals".into()]),
            verbs: vec!["get".into(), "list".into(), "watch".into()],
            ..PolicyRule::default()
        }]),
    }
}

/// The binding of [`pipeline_reader_role`] to [`PIPELINE_READERS`].
pub fn pipeline_reader_binding(namespace: &str) -> RoleBinding {
    RoleBinding {
        metadata: meta(PIPELINE_READER_ROLE, namespace),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".into()),
            kind: "Role".into(),
            name: PIPELINE_READER_ROLE.into(),
        },
        subjects: Some(
            PIPELINE_READERS
                .iter()
                .map(|name| Subject {
                    kind: "ServiceAccount".into(),
                    name: (*name).into(),
                    namespace: Some(namespace.into()),
                    ..Subject::default()
                })
                .collect(),
        ),
    }
}

/// The Secret that holds the service user Nova signs in to Airflow with (keys `username`, `password`).
pub const NOVA_API_SECRET: &str = "airflow-nova-api";

/// The Secrets Airflow reads, with the names and keys of `scripts/app-secrets.sh`: the chart's
/// Secret-reference fields (`fernetKeySecretName`, `apiSecretKeySecretName`,
/// `webserverSecretKeySecretName`, `jwtSecretName`), the local administrator and the service user
/// that Nova signs in with. Created once and never rewritten: rotating the Fernet key makes every
/// connection Airflow stored unreadable.
pub const SECRETS: [IdentitySecret; 5] = [
    IdentitySecret {
        name: "airflow-fernet-key",
        fixed: &[],
        generated: &[],
        fernet: &["fernet-key"],
    },
    IdentitySecret {
        name: "airflow-webserver-key",
        fixed: &[],
        generated: &["webserver-secret-key", "api-secret-key"],
        fernet: &[],
    },
    IdentitySecret {
        name: "airflow-jwt-secret",
        fixed: &[],
        generated: &["jwt-secret"],
        fernet: &[],
    },
    IdentitySecret {
        name: "airflow-admin",
        fixed: &[("username", "admin")],
        generated: &["password"],
        fernet: &[],
    },
    IdentitySecret {
        name: NOVA_API_SECRET,
        fixed: &[("username", "nova-api")],
        generated: &["password"],
        fernet: &[],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_role_reads_pipelines_and_service_principals_and_nothing_else() {
        let role = pipeline_reader_role("stardelt");
        assert_eq!(
            role.metadata.name.as_deref(),
            Some("airflow-pipeline-reader")
        );
        assert_eq!(role.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            role.metadata.labels.as_ref().expect("labels")["app.kubernetes.io/part-of"],
            "stardelt"
        );
        let rules = role.rules.expect("rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].api_groups, Some(vec!["stardelt.io".to_string()]));
        assert_eq!(
            rules[0].resources,
            Some(vec![
                "pipelines".to_string(),
                "serviceprincipals".to_string()
            ])
        );
        assert_eq!(rules[0].verbs, ["get", "list", "watch"]);
    }

    #[test]
    fn the_binding_names_the_two_airflow_service_accounts_in_the_namespace() {
        let binding = pipeline_reader_binding("lab");
        assert_eq!(binding.metadata.namespace.as_deref(), Some("lab"));
        assert_eq!(binding.role_ref.kind, "Role");
        assert_eq!(binding.role_ref.name, "airflow-pipeline-reader");
        assert_eq!(
            binding.role_ref.api_group.as_deref(),
            Some("rbac.authorization.k8s.io")
        );
        let subjects = binding.subjects.expect("subjects");
        let names: Vec<_> = subjects.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["airflow-dag-processor", "airflow-worker"]);
        for subject in &subjects {
            assert_eq!(subject.kind, "ServiceAccount");
            assert_eq!(subject.namespace.as_deref(), Some("lab"));
        }
    }

    #[test]
    fn the_secret_table_matches_the_platform_script() {
        let table: Vec<(&str, Vec<&str>)> = SECRETS
            .iter()
            .map(|secret| (secret.name, secret.key_names()))
            .collect();
        assert_eq!(
            table,
            [
                ("airflow-fernet-key", vec!["fernet-key"]),
                (
                    "airflow-webserver-key",
                    vec!["webserver-secret-key", "api-secret-key"]
                ),
                ("airflow-jwt-secret", vec!["jwt-secret"]),
                ("airflow-admin", vec!["username", "password"]),
                ("airflow-nova-api", vec!["username", "password"]),
            ]
        );
        assert_eq!(SECRETS[3].fixed, [("username", "admin")]);
        assert_eq!(SECRETS[4].fixed, [("username", "nova-api")]);
    }

    #[test]
    fn only_the_fernet_key_is_a_fernet_key() {
        let fernet: Vec<_> = SECRETS
            .iter()
            .flat_map(|secret| secret.fernet.iter().copied())
            .collect();
        assert_eq!(fernet, ["fernet-key"]);
        let secret = crate::keycloak::identity_secret(&SECRETS[0], "stardelt").expect("a Secret");
        let data = secret.data.expect("data");
        assert_eq!(data["fernet-key"].0.len(), 44);
        let webserver =
            crate::keycloak::identity_secret(&SECRETS[1], "stardelt").expect("a Secret");
        let data = webserver.data.expect("data");
        assert_eq!(data["webserver-secret-key"].0.len(), 48);
        assert_ne!(data["webserver-secret-key"], data["api-secret-key"]);
    }

    #[test]
    fn nova_reads_the_password_from_the_secret_the_table_creates() {
        assert_eq!(NOVA_API_SECRET, "airflow-nova-api");
        assert!(
            SECRETS
                .iter()
                .any(|s| s.name == NOVA_API_SECRET && s.key_names() == ["username", "password"])
        );
    }
}
