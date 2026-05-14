use std::{
    fs,
    path::{Path, PathBuf},
};

#[test]
fn production_sources_do_not_call_bootstrap_recovery_apis() {
    let workspace = workspace_root();

    let mut violations = Vec::new();
    for source in production_source_scan_sources(&workspace) {
        let contents = fs::read_to_string(&source).expect("read Rust source");
        let lines = contents.lines().collect::<Vec<_>>();
        for line_number in 0..lines.len() {
            if let Some(pattern) =
                forbidden_bootstrap_recovery_use(&workspace, &source, &lines, line_number)
            {
                violations.push(format!(
                    "{}:{} uses bootstrap recovery API pattern `{pattern}`",
                    source.strip_prefix(&workspace).unwrap_or(&source).display(),
                    line_number + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "production source must not call direct raw bootstrap recovery wrappers:\n{}",
        violations.join("\n")
    );
}

#[test]
fn production_recovery_contract_forbids_direct_bootstrap_recovery_callers() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("crates/velorix-runtime/src/production_surface.rs");
    for (line, pattern) in [
        (
            "    RecoveredRuntime::recover(store).await?;",
            "RecoveredRuntime::recover(",
        ),
        (
            "    RecoveredRuntime::recover_with_owner(store, owner).await?;",
            "RecoveredRuntime::recover_with_owner(",
        ),
        (
            "    RecoveredRuntime::recover_from_published_checkpoint_version(store, publisher, 7).await?;",
            "RecoveredRuntime::recover_from_published_checkpoint_version(",
        ),
        (
            "    RecoveredRuntime::recover_with_owner_and_relation_catalog_record(store, owner, relation_id, relation_version).await?;",
            "RecoveredRuntime::recover_with_owner_and_relation_catalog_record(",
        ),
        (
            "    RecoveredRuntime::recover_with_slatedb_state_store_and_relation_catalog(store, db_path, owner, catalog).await?;",
            "RecoveredRuntime::recover_with_slatedb_state_store_and_relation_catalog(",
        ),
        (
            "    RecoveredRuntime::recover_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog(store, db_path, 7, owner, catalog).await?;",
            "RecoveredRuntime::recover_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog(",
        ),
        (
            "    RecoveredRuntime::recover_from_published_checkpoint_version_with_owner_and_relation_catalog(store, 7, owner, catalog).await?;",
            "RecoveredRuntime::recover_from_published_checkpoint_version_with_owner_and_relation_catalog(",
        ),
    ] {
        assert_eq!(
            forbidden_bootstrap_recovery_use(workspace, &source, &[line], 0),
            Some(pattern)
        );
    }
}

#[test]
fn production_recovery_source_scan_includes_top_level_benchmarks() {
    let workspace = workspace_root();
    let sources = production_source_scan_sources(&workspace);

    for benchmark in top_level_rust_files(&workspace.join("benches")) {
        assert!(
            sources.iter().any(|source| source == &benchmark),
            "production recovery contract should scan {}",
            benchmark
                .strip_prefix(&workspace)
                .unwrap_or(&benchmark)
                .display()
        );
    }
}

#[test]
fn production_recovery_source_scan_includes_top_level_e2e_tests() {
    let workspace = workspace_root();
    let sources = production_source_scan_sources(&workspace);
    let local_recovery = workspace.join("tests/e2e/local_recovery.rs");

    assert!(
        sources.iter().any(|source| source == &local_recovery),
        "production recovery contract should scan {}",
        local_recovery
            .strip_prefix(&workspace)
            .unwrap_or(&local_recovery)
            .display()
    );
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("runtime crate lives under crates/velorix-runtime")
        .to_path_buf()
}

fn production_source_scan_sources(workspace: &Path) -> Vec<PathBuf> {
    let mut sources = rust_sources_under(&workspace.join("crates"));
    sources.extend(top_level_rust_files(&workspace.join("benches")));
    sources.extend(all_rust_sources_under(&workspace.join("tests")));
    sources.sort();
    sources
}

fn forbidden_bootstrap_recovery_use(
    workspace: &Path,
    source: &Path,
    lines: &[&str],
    line_number: usize,
) -> Option<&'static str> {
    let line = lines[line_number];
    forbidden_bootstrap_recovery_patterns()
        .iter()
        .copied()
        .find(|pattern| {
            line.contains(pattern)
                && !allowed_bootstrap_recovery_use(workspace, source, lines, line_number, pattern)
        })
}

fn forbidden_bootstrap_recovery_patterns() -> &'static [&'static str] {
    &[
        "RecoveredRuntime::recover(",
        "RecoveredRuntime::recover_with_owner(",
        "RecoveredRuntime::recover_from_published_checkpoint_version(",
        "RecoveredRuntime::recover_with_owner_and_relation_catalog_record(",
        "RecoveredRuntime::recover_with_slatedb_state_store_and_relation_catalog(",
        "RecoveredRuntime::recover_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog(",
        "RecoveredRuntime::recover_from_published_checkpoint_version_with_owner_and_relation_catalog(",
    ]
}

fn allowed_bootstrap_recovery_use(
    workspace: &Path,
    source: &Path,
    lines: &[&str],
    line_number: usize,
    pattern: &str,
) -> bool {
    if source == workspace.join("crates/velorix-runtime/src/recovery.rs") {
        return true;
    }

    if source == workspace.join("crates/velorix-runtime/src/query.rs")
        && pattern == "RecoveredRuntime::recover("
        && line_is_inside_function(
            lines,
            line_number,
            "pub async fn query_recovered_materialized_view_with_policy_and_limiter(",
        )
    {
        return true;
    }

    if source == workspace.join("crates/velorix-cli/src/main.rs")
        && [
            "RecoveredRuntime::recover_with_owner_and_relation_catalog_record(",
            "RecoveredRuntime::recover_from_published_checkpoint_version_with_owner_and_relation_catalog(",
        ]
        .contains(&pattern)
        && line_is_inside_function(lines, line_number, "async fn recover_local_runtime(")
    {
        return true;
    }

    if source == workspace.join("tests/e2e/local_recovery.rs") {
        return allowed_local_recovery_bootstrap_fixture(lines, line_number, pattern);
    }

    false
}

fn allowed_local_recovery_bootstrap_fixture(
    lines: &[&str],
    line_number: usize,
    pattern: &str,
) -> bool {
    match pattern {
        "RecoveredRuntime::recover(" => [
            "async fn local_recovery_rejects_json_deltabatch_ingest_object()",
            "async fn local_recovery_rejects_manifest_state_with_unexpected_owner()",
        ]
        .iter()
        .any(|signature| line_is_inside_function(lines, line_number, signature)),
        "RecoveredRuntime::recover_from_published_checkpoint_version(" => {
            ["async fn local_recovery_rejects_selected_checkpoint_when_payload_is_missing()"]
                .iter()
                .any(|signature| line_is_inside_function(lines, line_number, signature))
                || (line_is_inside_function(
                    lines,
                    line_number,
                    "async fn slatedb_local_recovery_can_use_selected_published_checkpoint()",
                ) && previous_nonempty_line_contains(lines, line_number, "let raw_error ="))
        }
        "RecoveredRuntime::recover_with_slatedb_state_store_and_relation_catalog(" => {
            line_is_inside_function(
                lines,
                line_number,
                "async fn slatedb_local_recovery_rejects_raw_state_manifest()",
            )
        }
        _ => false,
    }
}

fn previous_nonempty_line_contains(lines: &[&str], line_number: usize, needle: &str) -> bool {
    lines[..line_number]
        .iter()
        .rev()
        .find(|line| !line.trim().is_empty())
        .is_some_and(|line| line.contains(needle))
}

fn line_is_inside_function(lines: &[&str], line_number: usize, signature: &str) -> bool {
    let Some(signature_line) = lines[..=line_number]
        .iter()
        .rposition(|line| line.contains(signature))
    else {
        return false;
    };

    if line_number == signature_line {
        return true;
    }

    let mut brace_depth = 0usize;
    let mut opened = false;
    for line in &lines[signature_line..=line_number] {
        for character in line.chars() {
            match character {
                '{' => {
                    opened = true;
                    brace_depth += 1;
                }
                '}' => {
                    brace_depth = brace_depth.saturating_sub(1);
                }
                _ => {}
            }
        }
    }

    opened && brace_depth > 0
}

fn rust_sources_under(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    collect_rust_sources(root, &mut sources);
    sources
}

fn all_rust_sources_under(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    collect_all_rust_sources(root, &mut sources);
    sources
}

fn top_level_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    let entries = fs::read_dir(root).expect("read benchmark directory");
    for entry in entries {
        let path = entry.expect("read benchmark directory entry").path();
        if path.is_file() && path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
    sources.sort();
    sources
}

fn collect_rust_sources(path: &Path, sources: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(path).expect("read source tree");
    for entry in entries {
        let path = entry.expect("read directory entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            collect_rust_sources(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && path
                .components()
                .any(|component| component.as_os_str() == "src")
        {
            sources.push(path);
        }
    }
}

fn collect_all_rust_sources(path: &Path, sources: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(path).expect("read source tree");
    for entry in entries {
        let path = entry.expect("read directory entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            collect_all_rust_sources(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
}
