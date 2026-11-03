//! Superset (the `dashboards` pillar of `design/operator-parity.md`, step O7a): Secrets
//! applied by the controller/provision step.

use crate::keycloak::IdentitySecret;

/// The Secrets Superset reads before its release: the Flask secret, the local
/// administrator (the only sign-in until O7b) and the Trino service account the
/// datasource hook reads. Names and keys mirror stardelt-platform/scripts/app-secrets.sh.
pub const SECRETS: [IdentitySecret; 3] = [
    IdentitySecret {
        name: "superset-secret-key",
        fixed: &[],
        generated: &["secret-key"],
        fernet: &[],
    },
    IdentitySecret {
        name: "superset-admin",
        fixed: &[("username", "admin")],
        generated: &["password"],
        fernet: &[],
    },
    IdentitySecret {
        name: "trino-password-db",
        fixed: &[("username", "superset")],
        generated: &["password"],
        fernet: &[],
    },
];
