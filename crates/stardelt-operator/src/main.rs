use std::time::Duration;

use anyhow::Context as _;
use stardelt_operator::crd::{PlatformInstanceSpec, Profile};
use stardelt_operator::identity_checklist;
use stardelt_operator::values::effective_values;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // `values` and `identity-checklist` are pure CLIs: print what the controller would hand to
    // Helm (or register in the provider) and exit, before the tracing setup or the tokio
    // runtime exists.
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("values") => return values_command(args.collect()),
        Some("identity-checklist") => return identity_checklist_command(args.collect()),
        Some("--help" | "-h") => {
            print_usage();
            return Ok(());
        }
        _ => {}
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

/// `stardelt-operator identity-checklist <platforminstance.yaml> [--format json]`.
///
/// Reads a `PlatformInstance` document (only its `spec` matters) and prints what to register in
/// the external provider, per client. A file that cannot be read, parsed or turned into a
/// checklist exits 1 with the reason on stderr; a CLI misuse exits 2.
fn identity_checklist_command(args: Vec<String>) -> anyhow::Result<()> {
    let (path, json) = match args.as_slice() {
        [path] => (path.as_str(), false),
        [path, flag, format] if flag == "--format" && format == "json" => (path.as_str(), true),
        _ => checklist_usage(),
    };

    #[derive(serde::Deserialize)]
    struct InstanceDocument {
        spec: PlatformInstanceSpec,
    }

    let document = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read the PlatformInstance file {path}"))?;
    let document: InstanceDocument = serde_yaml::from_str(&document)
        .with_context(|| format!("{path} is not a PlatformInstance with a spec"))?;

    let entries = match identity_checklist::checklist(&document.spec) {
        Ok(entries) => entries,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&entries)?);
    } else {
        print!("{}", identity_checklist::render(&entries));
    }
    Ok(())
}

/// A checklist CLI misuse: print its usage line on stderr and exit 2.
fn checklist_usage() -> ! {
    eprintln!(
        "usage: stardelt-operator identity-checklist <platforminstance.yaml> [--format json]"
    );
    std::process::exit(2)
}

/// `stardelt-operator --help`.
fn print_usage() {
    println!("usage: stardelt-operator values <component> <demo|full|airgap>");
    println!("       stardelt-operator identity-checklist <platforminstance.yaml> [--format json]");
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
