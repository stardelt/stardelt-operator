//! The streaming bus (D42): one KRaft Kafka on Strimzi, one combined controller and broker node pool. Pure builders; no topic, no user, no `StreamApp`.

use kube::api::{DynamicObject, ObjectMeta, TypeMeta};
use serde_json::json;

use crate::crd::Profile;

pub const NAME: &str = "kafka";

fn meta(namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(NAME.into()),
        namespace: Some(namespace.into()),
        labels: Some(std::collections::BTreeMap::from([
            (
                "app.kubernetes.io/managed-by".to_string(),
                "stardelt-operator".to_string(),
            ),
            ("stardelt.io/component".to_string(), "kafka".to_string()),
            // Strimzi pairs the pool with its Kafka through this label.
            ("strimzi.io/cluster".to_string(), NAME.to_string()),
        ])),
        ..Default::default()
    }
}

/// `KafkaNodePool` `kafka`: one node that is both controller and broker. Ephemeral storage in
/// the demo profile (a restart loses topics, D42 accepts that); a 10Gi claim in `full`.
pub fn node_pool(namespace: &str, profile: Profile) -> DynamicObject {
    let storage = match profile {
        Profile::Demo => json!({"type": "ephemeral"}),
        _ => json!({"type": "persistent-claim", "size": "10Gi", "deleteClaim": false}),
    };
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "kafka.strimzi.io/v1".into(),
            kind: "KafkaNodePool".into(),
        }),
        metadata: meta(namespace),
        data: json!({"spec": {
            "replicas": 1,
            "roles": ["controller", "broker"],
            "storage": storage,
            "resources": {
                "requests": {"cpu": "500m", "memory": "1Gi"},
                "limits": {"cpu": "1", "memory": "1536Mi"},
            },
            "jvmOptions": {"-Xms": "1g", "-Xmx": "1g"},
        }}),
    }
}

/// `Kafka` `kafka`: one plain listener on 9092, in-cluster only (no Ingress, no TLS, in step
/// with Spark Connect's ClusterIP stance). Replication factors are 1 because there is one node.
pub fn broker(namespace: &str) -> DynamicObject {
    let mut metadata = meta(namespace);
    metadata.annotations = Some(std::collections::BTreeMap::from([(
        "strimzi.io/node-pools".to_string(),
        "enabled".to_string(),
    )]));
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "kafka.strimzi.io/v1".into(),
            kind: "Kafka".into(),
        }),
        metadata,
        data: json!({
            "spec": {
                "kafka": {
                    "listeners": [{"name": "plain", "port": 9092, "type": "internal", "tls": false}],
                    "config": {
                        "offsets.topic.replication.factor": 1,
                        "transaction.state.log.replication.factor": 1,
                        "transaction.state.log.min.isr": 1,
                        "default.replication.factor": 1,
                        "min.insync.replicas": 1,
                    },
                },
                "entityOperator": {"topicOperator": {}, "userOperator": {}},
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_objects_use_the_served_api_version() {
        // Strimzi 1.3.1 (planner.rs chart_version) serves only kafka.strimzi.io/v1; v1beta2 is a
        // 404 that ensure_kafka_broker reads as "waiting for Strimzi" forever.
        assert_eq!(
            node_pool("data", Profile::Demo).types.unwrap().api_version,
            "kafka.strimzi.io/v1"
        );
        assert_eq!(
            broker("data").types.unwrap().api_version,
            "kafka.strimzi.io/v1"
        );
    }

    #[test]
    fn node_pool_is_one_combined_pool() {
        let pool = node_pool("data", Profile::Demo);
        assert_eq!(pool.data["spec"]["replicas"], 1);
        assert_eq!(pool.data["spec"]["roles"], json!(["controller", "broker"]));
        assert_eq!(pool.data["spec"]["storage"]["type"], "ephemeral");
        let labels = pool.metadata.labels.as_ref().expect("labels");
        assert_eq!(labels.get("strimzi.io/cluster"), Some(&"kafka".to_string()));
    }

    #[test]
    fn full_profile_keeps_the_log_on_a_claim() {
        let pool = node_pool("data", Profile::Full);
        assert_eq!(
            pool.data["spec"]["storage"],
            json!({"type": "persistent-claim", "size": "10Gi", "deleteClaim": false})
        );
    }

    #[test]
    fn broker_listens_plain_inside_the_cluster() {
        let broker = broker("data");
        assert_eq!(
            broker.data["spec"]["kafka"]["listeners"],
            json!([{"name": "plain", "port": 9092, "type": "internal", "tls": false}])
        );
        let config = &broker.data["spec"]["kafka"]["config"];
        assert_eq!(config["offsets.topic.replication.factor"], 1);
        assert_eq!(config["transaction.state.log.replication.factor"], 1);
        assert_eq!(config["default.replication.factor"], 1);
        assert_eq!(
            broker
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("strimzi.io/node-pools")),
            Some(&"enabled".to_string())
        );
    }
}
