//! Generate the CRD manifests into `chart/crds/`.
//!
//! Run from the repository root: `cargo run -p stardelt-operator --bin crdgen`.

use std::path::Path;

use kube::CustomResourceExt;

use stardelt_operator::crd::{Lakehouse, PlatformInstance, Tenant};

fn main() -> anyhow::Result<()> {
    let dir = Path::new("chart/crds");
    std::fs::create_dir_all(dir)?;

    write(dir, "stardelt.io_platforminstances.yaml", &PlatformInstance::crd())?;
    write(dir, "stardelt.io_tenants.yaml", &Tenant::crd())?;
    write(dir, "stardelt.io_lakehouses.yaml", &Lakehouse::crd())?;

    println!("wrote CRDs to {}", dir.display());
    Ok(())
}

fn write<T: serde::Serialize>(dir: &Path, name: &str, crd: &T) -> anyhow::Result<()> {
    let yaml = serde_yaml::to_string(crd)?;
    std::fs::write(dir.join(name), yaml)?;
    Ok(())
}
