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
            // No cluster available (e.g. running `cargo run` locally): stay alive
            // instead of crash-looping so the process can idle in a pod.
            tracing::warn!(error = %e, "no Kubernetes cluster available; operator idle");
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }
    }
    Ok(())
}
