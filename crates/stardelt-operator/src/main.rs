//! stardelt-operator — reconciles the `PlatformInstance` CRD into the full
//! stardelt core stack (SeaweedFS, CNPG/Postgres, Lakekeeper, Trino, Airflow,
//! Superset, Nova) via Flux HelmReleases + native manifests.
//!
//! Usage:
//!   stardelt-operator           run the controller (uses ambient kubeconfig / in-cluster)
//!   stardelt-operator crd       print the PlatformInstance CRD YAML to stdout
//!
//! Configuration via env (mirrors Nova's NOVA_* convention):
//!   STARDELT_OPERATOR_FIELD_MANAGER  (default: "stardelt-operator")
//!   STARDELT_OPERATOR_RESYNC_SECS    (default: 300)
//!   STARDELT_OPERATOR_POLL_SECS      (default: 10)
//!   RUST_LOG                         (default: "info")

mod api;
mod config;
mod controllers;
mod error;
mod readiness;
mod resources;

use anyhow::Result;
use kube::CustomResourceExt;
use tracing_subscriber::EnvFilter;

use crate::api::PlatformInstance;
use crate::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // Subcommand dispatch: `crd` prints the CRD and exits without a cluster.
    if std::env::args().nth(1).as_deref() == Some("crd") {
        let crd = PlatformInstance::crd();
        println!("{}", serde_yaml::to_string(&crd)?);
        return Ok(());
    }

    tracing::info!("stardelt-operator v{}", env!("CARGO_PKG_VERSION"));
    let config = Config::from_env();
    let client = kube::Client::try_default().await?;
    controllers::platform_instance::run(client, config).await
}
