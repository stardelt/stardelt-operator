use std::time::Duration;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    tracing::info!("stardelt-operator v{}", env!("CARGO_PKG_VERSION"));

    match stardelt_operator::controller::run().await {
        Ok(()) => {}
        Err(e) => {
            // In a pod an unreachable API server is a startup failure: exit non-zero
            // so the container restarts (and the failure is visible in the pod
            // status) instead of idling for a cluster that may never answer.
            if std::env::var_os("KUBERNETES_SERVICE_HOST").is_some() {
                tracing::error!(error = %e, "Kubernetes API unreachable; exiting so the container restarts");
                return Err(e);
            }
            // Locally (`cargo run`) no cluster is expected: stay alive instead of
            // crash-looping so the process can idle.
            tracing::warn!(error = %e, "no Kubernetes cluster available; operator idle");
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
    }
    Ok(())
}
