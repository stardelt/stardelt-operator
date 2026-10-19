//! Generate the CRD manifests into `chart/crds/` and JSON Schemas into `schemas/`.
//!
//! Run from the repository root: `cargo run -p stardelt-operator --bin crdgen`.

use std::path::Path;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::CustomResourceExt;

use stardelt_operator::crd::{IngestionSource, Lakehouse, Pipeline, PlatformInstance};

fn main() -> anyhow::Result<()> {
    let dir = Path::new("chart/crds");
    std::fs::create_dir_all(dir)?;
    let schema_dir = Path::new("schemas");
    std::fs::create_dir_all(schema_dir)?;

    let platform_crd = PlatformInstance::crd();
    write(dir, "stardelt.io_platforminstances.yaml", &platform_crd)?;
    write_schema(
        schema_dir,
        "platforminstance.schema.json",
        "PlatformInstance",
        &platform_crd,
    )?;

    let lakehouse_crd = Lakehouse::crd();
    write(dir, "stardelt.io_lakehouses.yaml", &lakehouse_crd)?;
    write_schema(
        schema_dir,
        "lakehouse.schema.json",
        "Lakehouse",
        &lakehouse_crd,
    )?;

    let ingestion_crd = IngestionSource::crd();
    write(dir, "stardelt.io_ingestionsources.yaml", &ingestion_crd)?;
    write_schema(
        schema_dir,
        "ingestionsource.schema.json",
        "IngestionSource",
        &ingestion_crd,
    )?;

    let pipeline_crd = Pipeline::crd();
    write(dir, "stardelt.io_pipelines.yaml", &pipeline_crd)?;
    write_schema(
        schema_dir,
        "pipeline.schema.json",
        "Pipeline",
        &pipeline_crd,
    )?;

    println!(
        "wrote CRDs to {} and schemas to {}",
        dir.display(),
        schema_dir.display()
    );
    Ok(())
}

fn write<T: serde::Serialize>(dir: &Path, name: &str, crd: &T) -> anyhow::Result<()> {
    let yaml = serde_yaml::to_string(crd)?;
    std::fs::write(dir.join(name), yaml)?;
    Ok(())
}

fn write_schema(
    dir: &Path,
    name: &str,
    title: &str,
    crd: &CustomResourceDefinition,
) -> anyhow::Result<()> {
    let schema = crd.spec.versions[0]
        .schema
        .as_ref()
        .and_then(|s| s.open_api_v3_schema.clone())
        .expect("CRD must carry versions[0].schema.openAPIV3Schema");
    let mut value = serde_json::to_value(&schema)?;
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "$schema".to_owned(),
            serde_json::Value::String("http://json-schema.org/draft-07/schema#".to_owned()),
        );
        obj.insert(
            "title".to_owned(),
            serde_json::Value::String(title.to_owned()),
        );
    }
    let json = serde_json::to_string_pretty(&value)?;
    std::fs::write(dir.join(name), json + "\n")?;
    Ok(())
}
