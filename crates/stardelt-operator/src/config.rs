//! Env-only configuration, mirroring Nova's `NOVA_*` convention.
//!
//! All knobs are read once at startup. Reconcile-time behaviour that varies per
//! instance belongs on the `PlatformInstance` spec, not here — this is only for
//! operator-process settings.

/// Process-level operator configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Field-manager name used for all server-side-apply patches.
    pub field_manager: String,
    /// Steady-state resync interval, in seconds, once a PlatformInstance is Ready.
    pub resync_secs: u64,
    /// Requeue interval, in seconds, while waiting for a step to become ready.
    pub poll_secs: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            field_manager: env("STARDELT_OPERATOR_FIELD_MANAGER", "stardelt-operator"),
            resync_secs: env_parsed("STARDELT_OPERATOR_RESYNC_SECS", 300),
            poll_secs: env_parsed("STARDELT_OPERATOR_POLL_SECS", 10),
        }
    }
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parsed<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
