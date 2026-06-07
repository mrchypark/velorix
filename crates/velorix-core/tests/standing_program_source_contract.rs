use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("core crate lives under crates/velorix-core")
        .to_path_buf()
}

fn read(path: impl AsRef<Path>) -> String {
    std::fs::read_to_string(path.as_ref())
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.as_ref().display()))
}

fn rust_sources_under(relative_dir: &str) -> Vec<PathBuf> {
    let root = repo_root().join(relative_dir);
    let mut stack = vec![root];
    let mut files = Vec::new();
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path)
            .unwrap_or_else(|error| panic!("failed to list {}: {error}", path.display()))
        {
            let entry = entry.unwrap();
            let entry_path = entry.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
            } else if entry_path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                files.push(entry_path);
            }
        }
    }
    files.sort();
    files
}

#[test]
fn standing_program_contract_does_not_import_datafusion_sql_planner() {
    let source = read(repo_root().join("crates/velorix-core/src/standing_program.rs"));

    assert!(
        !source.contains("datafusion::sql"),
        "standing_program boundary must not use DataFusion SQL parser/planner"
    );
    assert!(
        !source.contains("DFParser"),
        "standing_program boundary must not parse SQL itself"
    );
}

#[test]
fn production_sources_do_not_execute_by_interpreting_feldera_ir_nodes() {
    let forbidden = [
        "feldera_ir::Op",
        "feldera_ir::MirNode",
        "feldera_ir::LirNode",
        "use feldera_ir::{Op",
        "use feldera_ir::{MirNode",
        "use feldera_ir::{LirNode",
    ];
    let mut offenders = Vec::new();

    for file in rust_sources_under("crates") {
        if !file.to_string_lossy().contains("/src/") {
            continue;
        }
        let source = read(&file);
        if forbidden.iter().any(|needle| source.contains(needle)) {
            offenders.push(
                file.strip_prefix(repo_root())
                    .unwrap()
                    .display()
                    .to_string(),
            );
        }
    }

    assert!(
        offenders.is_empty(),
        "production code must not execute by interpreting Feldera IR nodes: {offenders:?}"
    );
}

#[test]
fn dbsp_operator_spike_remains_feature_gated_and_quarantined() {
    let lib = read(repo_root().join("crates/velorix-core/src/lib.rs"));
    let manifest = read(repo_root().join("crates/velorix-core/Cargo.toml"));

    assert!(
        lib.contains("#[cfg(feature = \"dbsp-spike\")]\npub mod dbsp_engine;"),
        "dbsp_engine must remain quarantined behind the dbsp-spike feature"
    );
    assert!(
        manifest.contains("dbsp-spike = "),
        "dbsp-spike feature must remain explicit in velorix-core"
    );
}

#[test]
fn feldera_package_compatibility_gate_is_non_default() {
    let manifest = read(repo_root().join("crates/velorix-core/Cargo.toml"));
    let lib = read(repo_root().join("crates/velorix-core/src/lib.rs"));

    assert!(
        manifest.contains("feldera-package-compat = "),
        "velorix-core must expose an explicit Feldera package compatibility gate"
    );
    assert!(
        lib.contains("pub mod feldera_package_runtime;"),
        "the lightweight Feldera package runtime adapter must stay available to default product builds"
    );
    assert!(
        lib.contains(
            "#[cfg(feature = \"feldera-package-compat\")]\npub mod feldera_program_descriptor;"
        ),
        "heavy Feldera descriptor dependencies must stay behind feldera-package-compat"
    );
    assert!(
        !manifest.contains("default = [\"feldera-package-compat\"")
            && !manifest.contains("default = [ \"feldera-package-compat\""),
        "feldera-package-compat must not be enabled by default until MSRV/runtime policy is explicit"
    );
    assert!(
        manifest.contains("dbsp-spike = [") && manifest.contains("\"feldera-package-compat\","),
        "the DBSP operator spike must depend on the broader Feldera package compatibility gate"
    );
}

#[test]
fn product_view_api_does_not_call_legacy_sql_shape_validator() {
    let api = read(repo_root().join("crates/velorix-api/src/lib.rs"));
    let docs = read(
        repo_root()
            .join("docs/superpowers/specs/2026-05-27-feldera-package-first-runtime-design.md"),
    );

    assert!(
        !api.contains("validate_supported_dbsp_view_sql"),
        "product view API must not route through the removed legacy SQL shape validator"
    );
    assert!(
        docs.contains("does not construct DBSP relational operators outside a"),
        "design must require source checks against local DBSP operator construction"
    );
}
