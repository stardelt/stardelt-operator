//! Helm apply adapter.
//!
//! The operator does not re-implement upstream charts. It computes a plan (see
//! [`crate::planner`]) and applies each component as a Helm release, exactly
//! like the bash install path — but declaratively, once, from a CR.
//!
//! The runner is a trait so reconcile logic is unit-testable without helm or a
//! cluster.

use futures::future::BoxFuture;
use tokio::io::AsyncWriteExt;

/// One component to install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelmRelease {
    /// Release name (also the status key), e.g. `seaweedfs`.
    pub name: String,
    /// Chart reference, e.g. `seaweedfs/seaweedfs`.
    pub chart: String,
    /// Chart repository (`--repo`), so no `helm repo add` state is needed.
    pub repo: Option<String>,
    /// Pinned chart version.
    pub version: Option<String>,
    /// Target namespace.
    pub namespace: String,
    /// `helm --dry-run` when true: render only, change nothing.
    pub dry_run: bool,
    /// Extra values, sorted for deterministic args.
    pub set: std::collections::BTreeMap<String, String>,
    /// The merged values document (golden defaults + overrides), piped to Helm
    /// on stdin as `-f -`.
    pub values: Option<serde_json::Value>,
}

impl HelmRelease {
    pub fn args(&self) -> Vec<String> {
        // `helm upgrade --install <name> <chart>`; with `--repo`, <chart> must be
        // the bare chart name ("seaweedfs"), not "seaweedfs/seaweedfs".
        let chart_arg = match &self.repo {
            Some(_) => self
                .chart
                .rsplit('/')
                .next()
                .unwrap_or(&self.chart)
                .to_string(),
            None => self.chart.clone(),
        };
        let mut args: Vec<String> = vec![
            "upgrade".into(),
            "--install".into(),
            self.name.clone(),
            chart_arg,
            "--namespace".into(),
            self.namespace.clone(),
            "--create-namespace".into(),
        ];
        if let Some(repo) = &self.repo {
            args.push("--repo".into());
            args.push(repo.clone());
        }
        if let Some(v) = &self.version {
            args.push("--version".into());
            args.push(v.clone());
        }
        for (key, value) in &self.set {
            args.push("--set".into());
            args.push(format!("{key}={value}"));
        }
        if self.values.is_some() {
            // `-f -` reads a single values file from stdin; `--set` above always
            // beats it, which is exactly the documented precedence.
            args.push("--values".into());
            args.push("-".into());
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

    /// The YAML document for `-f -`, when this release has values.
    pub fn values_yaml(&self) -> Option<String> {
        self.values.as_ref().map(|values| {
            serde_yaml::to_string(values).expect("values documents always serialize to YAML")
        })
    }

    /// `helm status` — used to detect a release this operator does not own.
    pub fn status_args(&self) -> Vec<String> {
        vec![
            "status".into(),
            self.name.clone(),
            "--namespace".into(),
            self.namespace.clone(),
        ]
    }
}

/// Runs helm. `Vec<String>` args (no shell), so nothing is interpolated.
pub trait HelmRunner: Send + Sync {
    fn run(
        &self,
        args: Vec<String>,
        stdin: Option<String>,
    ) -> BoxFuture<'static, anyhow::Result<String>>;
}

/// Production runner: shells out to the `helm` binary baked into the image.
pub struct CommandHelmRunner;

impl HelmRunner for CommandHelmRunner {
    fn run(
        &self,
        args: Vec<String>,
        stdin: Option<String>,
    ) -> BoxFuture<'static, anyhow::Result<String>> {
        Box::pin(async move {
            let mut child = tokio::process::Command::new("helm")
                .args(&args)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()?;
            // Drop stdin immediately when there is no document, so helm sees EOF.
            if let Some(mut pipe) = child.stdin.take() {
                if let Some(document) = stdin {
                    pipe.write_all(document.as_bytes()).await?;
                }
                drop(pipe);
            }
            let out = child.wait_with_output().await?;
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

    /// One recorded helm invocation.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct HelmCall {
        pub args: Vec<String>,
        pub stdin: Option<String>,
    }

    /// Records calls instead of running helm.
    #[derive(Default)]
    pub struct RecordingRunner {
        pub calls: Mutex<Vec<HelmCall>>,
    }

    impl HelmRunner for RecordingRunner {
        fn run(
            &self,
            args: Vec<String>,
            stdin: Option<String>,
        ) -> BoxFuture<'static, anyhow::Result<String>> {
            self.calls
                .lock()
                .expect("calls poisoned")
                .push(HelmCall { args, stdin });
            Box::pin(async { Ok(String::new()) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release(dry_run: bool) -> HelmRelease {
        let mut set = std::collections::BTreeMap::new();
        set.insert("trino.enabled".to_string(), "true".to_string());
        HelmRelease {
            name: "seaweedfs".into(),
            chart: "seaweedfs/seaweedfs".into(),
            repo: Some("https://seaweedfs.github.io/seaweedfs/helm".into()),
            version: Some("4.25.1".into()),
            namespace: "stardelt".into(),
            dry_run,
            set,
            values: Some(serde_json::json!({ "s3": { "enabled": true } })),
        }
    }

    fn bare_release() -> HelmRelease {
        HelmRelease {
            values: None,
            set: Default::default(),
            ..release(false)
        }
    }

    #[test]
    fn args_pin_version_namespace_and_repo() {
        let args = release(false).args();
        assert!(args.starts_with(&["upgrade".into(), "--install".into(), "seaweedfs".into()]));
        // With --repo the chart argument must be the bare name.
        assert!(args.contains(&"seaweedfs".to_string()));
        assert!(!args.contains(&"seaweedfs/seaweedfs".to_string()));
        assert!(args.contains(&"--repo".to_string()));
        assert!(args.contains(&"https://seaweedfs.github.io/seaweedfs/helm".to_string()));
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

    #[test]
    fn set_values_are_passed_and_sorted() {
        let args = release(true).args();
        let i = args
            .iter()
            .position(|a| a == "--set")
            .expect("--set present");
        assert_eq!(args[i + 1], "trino.enabled=true");
    }

    #[test]
    fn values_documents_are_piped_on_stdin() {
        let release = release(true);
        let args = release.args();
        let i = args
            .iter()
            .position(|a| a == "--values")
            .expect("--values present");
        assert_eq!(
            args[i + 1],
            "-",
            "helm reads the merged document from stdin"
        );

        let stdin = release.values_yaml().expect("values yaml");
        assert!(stdin.contains("enabled: true"), "got: {stdin}");
    }

    #[test]
    fn releases_without_values_do_not_read_stdin() {
        let release = bare_release();
        assert!(!release.args().contains(&"--values".to_string()));
        assert_eq!(release.values_yaml(), None);
    }

    #[test]
    fn status_args_target_the_release_namespace() {
        let args = release(false).status_args();
        assert_eq!(args, vec!["status", "seaweedfs", "--namespace", "stardelt"]);
    }
}
