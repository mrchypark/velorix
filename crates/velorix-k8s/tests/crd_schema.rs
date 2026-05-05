use kube::CustomResourceExt;
use serde_json::Value;
use velorix_k8s::crd::{
    api_group, api_version, BenchmarkArtifactRef, ConditionState, ObjectStoreAuthorityRef,
    RelationVersionRef, StreamStatus, VelorixBenchmarkGate, VelorixBenchmarkGateSpec,
    VelorixCheckpointPolicy, VelorixCheckpointPolicySpec, VelorixCondition, VelorixDatabase,
    VelorixDatabaseSpec, VelorixStream, VelorixStreamSpec, VelorixTable, VelorixTableSpec,
    VelorixWorkerShard, VelorixWorkerShardSpec,
};

#[test]
fn crd_schemas_expose_required_spec_fields_for_operator_inputs() {
    assert_required_spec_fields::<VelorixDatabase>(&["authority", "database_id"]);
    assert_required_spec_fields::<VelorixStream>(&[
        "authority",
        "database_id",
        "relation",
        "stream_id",
    ]);
    assert_required_spec_fields::<VelorixTable>(&[
        "authority",
        "query_policy_id",
        "relation",
        "table_id",
        "tenant_id",
    ]);
    assert_required_spec_fields::<VelorixWorkerShard>(&[
        "authority",
        "desired_owner_id",
        "partition_id",
        "stream_id",
        "worker_id",
    ]);
    assert_required_spec_fields::<VelorixCheckpointPolicy>(&[
        "authority",
        "database_id",
        "min_interval_ms",
        "policy_id",
        "retain_checkpoints",
        "stream_id",
    ]);
    assert_required_spec_fields::<VelorixBenchmarkGate>(&[
        "authority",
        "backend",
        "baseline_ref",
        "gate_id",
        "gate_level",
        "result_ref",
    ]);
}

#[test]
fn crd_schemas_expose_kind_specific_observed_status() {
    assert_status_fields::<VelorixDatabase>(&["observed_generation", "readiness"]);
    assert_status_fields::<VelorixStream>(&[
        "last_accepted_relation_schema_fingerprint",
        "latest_published_checkpoint",
        "observed_generation",
        "readiness",
    ]);
    assert_status_fields::<VelorixTable>(&[
        "last_accepted_relation_schema_fingerprint",
        "observed_generation",
        "readiness",
    ]);
    assert_status_fields::<VelorixWorkerShard>(&[
        "current_owner_epoch",
        "observed_generation",
        "readiness",
    ]);
    assert_status_fields::<VelorixCheckpointPolicy>(&[
        "latest_published_checkpoint",
        "observed_generation",
        "readiness",
    ]);
    assert_status_fields::<VelorixBenchmarkGate>(&[
        "latest_result",
        "observed_generation",
        "readiness",
    ]);
}

#[test]
fn crd_schemas_use_velorix_api_group_and_version() {
    let crd = VelorixDatabase::crd();

    assert_eq!(crd.spec.group, api_group());
    assert_eq!(crd.spec.versions[0].name, api_version());
    assert_eq!(crd.spec.scope, "Namespaced");
}

#[test]
fn crd_specs_round_trip_with_status_as_observed_state() {
    let database = VelorixDatabase::new(
        "analytics",
        VelorixDatabaseSpec {
            database_id: "db-analytics".to_string(),
            authority: authority(),
        },
    );
    let table = VelorixTable::new(
        "balances",
        VelorixTableSpec {
            table_id: "balances".to_string(),
            tenant_id: "tenant-a".to_string(),
            relation: relation(),
            authority: authority(),
            query_policy_id: "bounded-default".to_string(),
        },
    );
    let mut stream = VelorixStream::new(
        "deposits",
        VelorixStreamSpec {
            stream_id: "deposits".to_string(),
            database_id: "db-analytics".to_string(),
            relation: relation(),
            authority: authority(),
        },
    );
    stream.status = Some(stream_ready_status());

    let shard = VelorixWorkerShard::new(
        "deposits-p0",
        VelorixWorkerShardSpec {
            worker_id: "worker-a".to_string(),
            stream_id: "deposits".to_string(),
            partition_id: 0,
            desired_owner_id: "worker-a".to_string(),
            authority: authority(),
        },
    );
    let policy = VelorixCheckpointPolicy::new(
        "fast",
        VelorixCheckpointPolicySpec {
            policy_id: "fast".to_string(),
            database_id: "db-analytics".to_string(),
            stream_id: "deposits".to_string(),
            authority: authority(),
            min_interval_ms: 1_000,
            retain_checkpoints: 16,
        },
    );
    let gate = VelorixBenchmarkGate::new(
        "pr-smoke",
        VelorixBenchmarkGateSpec {
            gate_id: "pr-smoke".to_string(),
            gate_level: "pr-smoke".to_string(),
            backend: "s3-compatible".to_string(),
            authority: authority(),
            baseline_ref: artifact_ref("v1/benchmark/baseline.json"),
            result_ref: artifact_ref("v1/benchmark/result.json"),
        },
    );

    round_trip(database);
    round_trip(table);
    round_trip(stream);
    round_trip(shard);
    round_trip(policy);
    round_trip(gate);
}

fn assert_required_spec_fields<T>(expected: &[&str])
where
    T: CustomResourceExt,
{
    let schema = crd_schema::<T>();
    let mut required = schema
        .pointer("/properties/spec/required")
        .and_then(Value::as_array)
        .expect("spec.required")
        .iter()
        .map(|value| value.as_str().expect("required field").to_string())
        .collect::<Vec<_>>();
    required.sort();

    let mut expected = expected
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    expected.sort();

    assert_eq!(required, expected);
}

fn assert_status_fields<T>(expected: &[&str])
where
    T: CustomResourceExt,
{
    let schema = crd_schema::<T>();
    let mut fields = schema
        .pointer("/properties/status/properties")
        .and_then(Value::as_object)
        .expect("status properties")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    fields.sort();

    let mut expected = expected
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    expected.sort();

    assert_eq!(fields, expected);
    assert!(!fields.iter().any(|field| {
        matches!(
            field.as_str(),
            "checkpoint_manifest_body" | "state_payload" | "object_key_prefix" | "raw_url"
        )
    }));
}

fn crd_schema<T>() -> Value
where
    T: CustomResourceExt,
{
    serde_json::to_value(&T::crd().spec.versions[0].schema)
        .unwrap()
        .pointer("/openAPIV3Schema")
        .cloned()
        .expect("openAPIV3Schema")
}

fn authority() -> ObjectStoreAuthorityRef {
    ObjectStoreAuthorityRef {
        store_id: "primary-object-store".to_string(),
        namespace: "tenant-a".to_string(),
    }
}

fn artifact_ref(object_key: &str) -> BenchmarkArtifactRef {
    BenchmarkArtifactRef {
        object_key: object_key.to_string(),
        digest: "sha256:artifact".to_string(),
        schema_version: 1,
    }
}

fn relation() -> RelationVersionRef {
    RelationVersionRef {
        relation_id: "balances".to_string(),
        relation_version: 1,
        schema_fingerprint: "sha256:relation".to_string(),
    }
}

fn stream_ready_status() -> StreamStatus {
    StreamStatus {
        observed_generation: Some(1),
        last_accepted_relation_schema_fingerprint: Some("sha256:relation".to_string()),
        latest_published_checkpoint: None,
        readiness: Some(VelorixCondition {
            type_: "Ready".to_string(),
            status: ConditionState::True,
            reason: "AuthorityValidated".to_string(),
            message: "object-store authority records validated".to_string(),
        }),
    }
}

fn round_trip<T>(value: T)
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let json = serde_json::to_string(&value).unwrap();
    let decoded = serde_json::from_str::<T>(&json).unwrap();

    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(value).unwrap()
    );
}
