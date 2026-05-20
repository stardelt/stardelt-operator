use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!("stardelt-operator v{}", env!("CARGO_PKG_VERSION"));
    tracing::info!(
        "stardelt-operator is a placeholder — see ROADMAP Stage 4 at \
        https://docs.stardelt.io/roadmap"
    );

    // Placeholder: sleep forever so the binary stays alive if deployed in a pod.
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}
