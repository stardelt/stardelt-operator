//! The `stopped` annotation (operator#64): which workloads a stop touched and their replica
//! counts. Pure, so the format is tested without a cluster.
//!
//! The controller writes the annotation before it scales anything down, so a later unstop
//! restores the counts the workloads had. Deleting the annotation by hand loses them.

use std::collections::BTreeMap;

/// The annotation that records `<kind>/<name>` → replica count.
pub const PREVIOUS_REPLICAS: &str = "stardelt.io/previous-replicas";
/// CloudNative-PG's own hibernation switch, set on the namespace's Clusters while stopped.
pub const HIBERNATION: &str = "cnpg.io/hibernation";

/// The annotation value: `{"Deployment/lakekeeper":1,...}`, keys sorted.
pub fn annotation_value(workloads: &[(String, String, i32)]) -> String {
    let mut map: BTreeMap<String, i32> = BTreeMap::new();
    for (kind, name, replicas) in workloads {
        map.insert(format!("{kind}/{name}"), *replicas);
    }
    serde_json::to_string(&map).expect("a map of integers is JSON")
}

/// Read the annotation back. An entry without a `/`, a non-integer, or a non-object is an error.
pub fn recorded(annotation: &str) -> anyhow::Result<Vec<(String, String, i32)>> {
    let value: serde_json::Value = serde_json::from_str(annotation)
        .map_err(|e| anyhow::anyhow!("{PREVIOUS_REPLICAS} is not JSON: {e}"))?;
    let Some(object) = value.as_object() else {
        anyhow::bail!("{PREVIOUS_REPLICAS} must be a JSON object of \"<kind>/<name>\": <replicas>");
    };
    let mut recorded = Vec::with_capacity(object.len());
    for (key, value) in object {
        let Some((kind, name)) = key.split_once('/') else {
            anyhow::bail!("{PREVIOUS_REPLICAS} entry {key:?} has no \"/\"");
        };
        if kind.is_empty() || name.is_empty() {
            anyhow::bail!("{PREVIOUS_REPLICAS} entry {key:?} needs a kind and a name");
        }
        let Some(replicas) = value.as_i64() else {
            anyhow::bail!("{PREVIOUS_REPLICAS} entry {key:?} is not an integer: {value}");
        };
        let Ok(replicas) = i32::try_from(replicas) else {
            anyhow::bail!("{PREVIOUS_REPLICAS} entry {key:?} is out of range: {value}");
        };
        recorded.push((kind.to_string(), name.to_string(), replicas));
    }
    Ok(recorded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workloads() -> Vec<(String, String, i32)> {
        vec![
            ("StatefulSet".into(), "seaweedfs-master".into(), 1),
            ("Deployment".into(), "lakekeeper".into(), 2),
        ]
    }

    #[test]
    fn the_value_sorts_its_keys() {
        assert_eq!(
            annotation_value(&workloads()),
            r#"{"Deployment/lakekeeper":2,"StatefulSet/seaweedfs-master":1}"#
        );
    }

    #[test]
    fn the_round_trip_keeps_every_workload() {
        let recorded = recorded(&annotation_value(&workloads())).expect("round trip");
        assert_eq!(
            recorded,
            vec![
                ("Deployment".to_string(), "lakekeeper".to_string(), 2),
                ("StatefulSet".to_string(), "seaweedfs-master".to_string(), 1),
            ]
        );
    }

    #[test]
    fn an_entry_without_a_slash_is_an_error() {
        let error = recorded(r#"{"lakekeeper":1}"#).expect_err("no slash");
        assert!(error.to_string().contains(PREVIOUS_REPLICAS), "{error}");
    }

    #[test]
    fn a_non_integer_is_an_error() {
        let error = recorded(r#"{"Deployment/lakekeeper":"one"}"#).expect_err("not a number");
        assert!(error.to_string().contains(PREVIOUS_REPLICAS), "{error}");
    }

    #[test]
    fn a_non_object_is_an_error() {
        let error = recorded("[1, 2]").expect_err("not an object");
        assert!(error.to_string().contains(PREVIOUS_REPLICAS), "{error}");
    }
}
