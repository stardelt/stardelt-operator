use std::time::Duration;

use stardelt_operator::crd::Profile;
use stardelt_operator::values::effective_values;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // `values` is a pure CLI: print what the controller would hand to Helm and
    // exit, before the tracing setup or the tokio runtime exists.
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() == Some("values") {
        return values_command(args.collect());
    }
    run_controller()
}

/// `stardelt-operator values <component> <demo|full|airgap>`.
///
/// An unknown component is not an error: it prints `{}`, like
/// [`stardelt_operator::values::golden_values`]. Missing arguments and an
/// unknown profile are usage errors (exit 2).
fn values_command(args: Vec<String>) -> anyhow::Result<()> {
    let (component, profile) = match args.as_slice() {
        [component, profile] => (component.as_str(), profile.as_str()),
        _ => usage(),
    };
    let profile: Profile = serde_yaml::from_str(profile).unwrap_or_else(|_| usage());

    let values = effective_values(component, profile, None, None);
    let mut document = serde_yaml::to_string(&values)?;
    if !document.ends_with('\n') {
        document.push('\n');
    }
    print!("{document}");
    Ok(())
}

/// A CLI misuse: print the usage line on stderr and exit 2.
fn usage() -> ! {
    eprintln!("usage: stardelt-operator values <component> <demo|full|airgap>");
    std::process::exit(2)
}

#[tokio::main]
async fn run_controller() -> anyhow::Result<()> {
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
