//! Helm apply adapter.
//!
//! The operator does not re-implement upstream charts. It computes a plan (see
//! [`crate::planner`]) and applies each component as a Helm release, exactly
//! like the bash install path — but declaratively, once, from a CR.
//!
//! The runner is a trait so reconcile logic is unit-testable without helm or a
//! cluster.

use futures::future::BoxFuture;

/// One component to install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelmRelease {
    /// Release name (also the status key), e.g. `seaweedfs`.
    pub name: String,
    /// Chart reference, e.g. `seaweedfs/seaweedfs`.
    pub chart: String,
    /// Pinned chart version.
    pub version: Option<String>,
    /// Target namespace.
    pub namespace: String,
    /// `helm --dry-run` when true: render only, change nothing.
    pub dry_run: bool,
}

impl HelmRelease {
    pub fn args(&self) -> Vec<String> {
        let mut args: Vec<String> = vec![
            "upgrade".into(),
            "--install".into(),
            self.name.clone(),
            self.chart.clone(),
            "--namespace".into(),
            self.namespace.clone(),
            "--create-namespace".into(),
        ];
        if let Some(v) = &self.version {
            args.push("--version".into());
            args.push(v.clone());
        }
        if self.dry_run {
            args.push("--dry-run".into());
        } else {
            args.push("--wait".into());
            args.push("--timeout".into());
            args.push("10m".into());
        }
        args
    }
}

/// Runs helm. `Vec<String>` args (no shell), so nothing is interpolated.
pub trait HelmRunner: Send + Sync {
    fn run(&self, args: Vec<String>) -> BoxFuture<'static, anyhow::Result<String>>;
}

/// Production runner: shells out to the `helm` binary baked into the image.
pub struct CommandHelmRunner;

impl HelmRunner for CommandHelmRunner {
    fn run(&self, args: Vec<String>) -> BoxFuture<'static, anyhow::Result<String>> {
        Box::pin(async move {
            let out = tokio::process::Command::new("helm")
                .args(&args)
                .output()
                .await?;
            if !out.status.success() {
                anyhow::bail!(
                    "helm {} failed: {}",
                    args.join(" "),
                    String::from_utf8_lossy(&out.stderr).trim()
                );
            }
            Ok(String::from_utf8_lossy(&out.stdout).to_string())
        })
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use std::sync::Mutex;

    /// Records calls instead of running helm.
    #[derive(Default)]
    pub struct RecordingRunner {
        pub calls: Mutex<Vec<Vec<String>>>,
    }

    impl HelmRunner for RecordingRunner {
        fn run(&self, args: Vec<String>) -> BoxFuture<'static, anyhow::Result<String>> {
            self.calls.lock().expect("calls poisoned").push(args);
            Box::pin(async { Ok(String::new()) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release(dry_run: bool) -> HelmRelease {
        HelmRelease {
            name: "seaweedfs".into(),
            chart: "seaweedfs/seaweedfs".into(),
            version: Some("4.25.1".into()),
            namespace: "stardelt".into(),
            dry_run,
        }
    }

    #[test]
    fn args_pin_version_and_namespace() {
        let args = release(false).args();
        assert!(args.starts_with(&["upgrade".into(), "--install".into(), "seaweedfs".into()]));
        assert!(args.contains(&"seaweedfs/seaweedfs".to_string()));
        assert!(args.contains(&"4.25.1".to_string()));
        assert!(args.contains(&"stardelt".to_string()));
        assert!(args.contains(&"--wait".to_string()));
        assert!(!args.contains(&"--dry-run".to_string()));
    }

    #[test]
    fn dry_run_changes_nothing_and_does_not_wait() {
        let args = release(true).args();
        assert!(args.contains(&"--dry-run".to_string()));
        assert!(!args.contains(&"--wait".to_string()));
        assert!(!args.contains(&"--timeout".to_string()));
    }
}
