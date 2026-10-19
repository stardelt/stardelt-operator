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

/// Recorded as the Helm revision description on every upgrade this operator
/// performs; it is how a later reconcile tells its own releases from foreign ones.
pub const OWNER_MARKER: &str = "managed by stardelt-operator";

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
        args.push("--description".into());
        args.push(OWNER_MARKER.into());
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

    /// `helm history -o json` — used to detect a release this operator does not own.
    pub fn history_args(&self) -> Vec<String> {
        vec![
            "history".into(),
            self.name.clone(),
            "--namespace".into(),
            self.namespace.clone(),
            "-o".into(),
            "json".into(),
        ]
    }

    /// Identifies what would be applied: args (chart, version, namespace, --set)
    /// plus the values document.
    // DefaultHasher is not stable across Rust releases; after an operator upgrade
    // each component re-applies once.
    pub fn fingerprint(&self) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.args().hash(&mut h);
        self.values_yaml().hash(&mut h);
        format!("{:016x}", h.finish())
    }

    /// `helm uninstall` for this release: not found is fine, and the chart's
    /// resources are waited for so the finalizer does not remove itself while
    /// pods still terminate.
    pub fn uninstall_args(&self) -> Vec<String> {
        vec![
            "uninstall".into(),
            self.name.clone(),
            "--namespace".into(),
            self.namespace.clone(),
            "--ignore-not-found".into(),
            "--wait".into(),
            "--timeout".into(),
            "120s".into(),
        ]
    }

    /// `helm status -o json` — tells whether the release currently is `deployed`
    /// (as opposed to `failed`, `pending`, …).
    pub fn status_args(&self) -> Vec<String> {
        vec![
            "status".into(),
            self.name.clone(),
            "--namespace".into(),
            self.namespace.clone(),
            "-o".into(),
            "json".into(),
        ]
    }
}

/// True when `helm status -o json` output reports the release as `deployed`.
pub fn is_deployed(status_json: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(status_json)
        .ok()
        .is_some_and(|v| v["info"]["status"] == "deployed")
}

/// True when any revision in `helm history -o json` output carries [`OWNER_MARKER`].
pub fn is_owned(history_json: &str) -> bool {
    history_json.contains(OWNER_MARKER)
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
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// One recorded helm invocation.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct HelmCall {
        pub args: Vec<String>,
        pub stdin: Option<String>,
    }

    /// Records calls instead of running helm, answering each call from a
    /// per-verb reply (`run` returns the empty string when none is set).
    #[derive(Default)]
    pub struct RecordingRunner {
        pub calls: Mutex<Vec<HelmCall>>,
        pub replies: Mutex<HashMap<String, String>>,
        pub failures: Mutex<HashMap<String, String>>,
    }

    impl RecordingRunner {
        /// Set what the next call to `helm <verb>` returns.
        pub fn reply(&self, verb: &str, output: impl Into<String>) {
            self.replies
                .lock()
                .expect("replies poisoned")
                .insert(verb.into(), output.into());
        }

        /// Make every call to `helm <verb>` fail with `message`.
        pub fn fail(&self, verb: &str, message: impl Into<String>) {
            self.failures
                .lock()
                .expect("failures poisoned")
                .insert(verb.into(), message.into());
        }
    }

    impl HelmRunner for RecordingRunner {
        fn run(
            &self,
            args: Vec<String>,
            stdin: Option<String>,
        ) -> BoxFuture<'static, anyhow::Result<String>> {
            let verb = args.first().cloned().unwrap_or_default();
            self.calls
                .lock()
                .expect("calls poisoned")
                .push(HelmCall { args, stdin });
            let failure = self
                .failures
                .lock()
                .expect("failures poisoned")
                .get(&verb)
                .cloned();
            if let Some(message) = failure {
                return Box::pin(async move { Err(anyhow::anyhow!("{message}")) });
            }
            let output = self
                .replies
                .lock()
                .expect("replies poisoned")
                .get(&verb)
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(output) })
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
    fn history_args_target_the_release_namespace() {
        let args = release(false).history_args();
        assert_eq!(
            args,
            vec![
                "history",
                "seaweedfs",
                "--namespace",
                "stardelt",
                "-o",
                "json"
            ]
        );
    }

    #[test]
    fn uninstall_ignores_a_missing_release_and_waits() {
        assert_eq!(
            release(false).uninstall_args(),
            vec![
                "uninstall",
                "seaweedfs",
                "--namespace",
                "stardelt",
                "--ignore-not-found",
                "--wait",
                "--timeout",
                "120s",
            ]
        );
    }

    #[test]
    fn upgrades_record_the_owner_marker() {
        let args = release(false).args();
        let i = args
            .iter()
            .position(|a| a == "--description")
            .expect("--description");
        assert_eq!(args[i + 1], OWNER_MARKER);
    }

    #[test]
    fn ownership_is_read_from_revision_descriptions() {
        let mine = format!(r#"[{{"revision":1,"description":"{OWNER_MARKER}"}}]"#);
        let foreign = r#"[{"revision":1,"description":"Install complete"}]"#;
        assert!(is_owned(&mine));
        assert!(!is_owned(foreign));
    }

    #[test]
    fn fingerprint_is_stable_for_equal_releases() {
        assert_eq!(release(false).fingerprint(), release(false).fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_values() {
        let mut changed = release(false);
        changed.values = Some(serde_json::json!({ "s3": { "enabled": false } }));
        assert_ne!(release(false).fingerprint(), changed.fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_version() {
        let mut changed = release(false);
        changed.version = Some("4.25.2".into());
        assert_ne!(release(false).fingerprint(), changed.fingerprint());
    }

    #[test]
    fn is_deployed_reads_info_status() {
        assert!(is_deployed(r#"{"info":{"status":"deployed"}}"#));
        assert!(!is_deployed(r#"{"info":{"status":"failed"}}"#));
        assert!(!is_deployed("not json at all"));
    }

    #[test]
    fn status_args_target_the_release_namespace() {
        let args = release(false).status_args();
        assert_eq!(
            args,
            vec![
                "status",
                "seaweedfs",
                "--namespace",
                "stardelt",
                "-o",
                "json"
            ]
        );
    }
}
