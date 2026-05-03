use std::path::PathBuf;

use velorix_core::feldera_artifact::{
    feldera_spec_hash, validate_feldera_compile_artifact, FelderaArtifactError,
    FelderaCompileArtifactMetadata, StandingViewSpec,
};

fn load_spec(name: &str) -> StandingViewSpec {
    serde_json::from_str(&std::fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
}

fn load_artifact(name: &str) -> FelderaCompileArtifactMetadata {
    serde_json::from_str(&std::fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("feldera")
        .join(format!("{name}.json"))
}

#[test]
fn feldera_artifact_accepts_valid_single_input_output_standing_view() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_valid");

    assert_eq!(
        feldera_spec_hash(&spec).unwrap(),
        "velorix-feldera-spec-fnv1a64-v1:df13f7387d35c9e1"
    );
    validate_feldera_compile_artifact(&spec, &artifact).unwrap();
}

#[test]
fn feldera_artifact_rejects_unsupported_metadata_version() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_invalid_version");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedMetadataVersion { version: 2 }
    ));
}

#[test]
fn feldera_artifact_rejects_missing_schema() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_missing_schema");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MissingSchema {
            field: "input_schemas"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_missing_artifact_id() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_missing_artifact_id");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MissingIdentityField {
            field: "artifact_id"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_mismatched_spec_hash() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_mismatched_spec_hash");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MismatchedSpecHash { .. }
    ));
}

#[test]
fn feldera_artifact_rejects_mismatched_view_id() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_mismatched_view_id");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::MismatchedViewId { .. }
    ));
}

#[test]
fn feldera_artifact_rejects_unknown_state_codec() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_unknown_state_codec");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedStateCodec { .. }
    ));
}

#[test]
fn feldera_artifact_rejects_unsupported_epoch_policy() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_unsupported_epoch_policy");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedEpochPolicy { .. }
    ));
}

#[test]
fn feldera_artifact_rejects_multi_input_shape_for_now() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_multi_input");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedShape {
            shape: "multi_input"
        }
    ));
}

#[test]
fn feldera_artifact_rejects_multi_output_shape_for_now() {
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_multi_output");

    let error = validate_feldera_compile_artifact(&spec, &artifact).unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactError::UnsupportedShape {
            shape: "multi_output"
        }
    ));
}
