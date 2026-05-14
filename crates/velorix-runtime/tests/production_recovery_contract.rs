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
fn recovery_public_api_names_make_bootstrap_recovery_explicit() {
    let workspace = workspace_root();
    let source = workspace.join("crates/velorix-runtime/src/recovery.rs");
    let contents = fs::read_to_string(&source).expect("read recovery source");

    let ambiguous_public_recovery_apis = contents
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            ambiguous_public_recovery_api_name(line).map(|name| {
                format!(
                    "{}:{} exposes unchecked recovery as `{name}`",
                    source.strip_prefix(&workspace).unwrap_or(&source).display(),
                    index + 1
                )
            })
        })
        .collect::<Vec<_>>();

    assert!(
        ambiguous_public_recovery_apis.is_empty(),
        "unchecked public recovery APIs must use explicit bootstrap names; production recovery APIs must be checked:\n{}",
        ambiguous_public_recovery_apis.join("\n")
    );
}

#[test]
fn production_recovery_public_apis_require_startup_capabilities_in_signature() {
    let workspace = workspace_root();
    let source = workspace.join("crates/velorix-runtime/src/recovery.rs");
    let contents = fs::read_to_string(&source).expect("read recovery source");
    let lines = contents.lines().collect::<Vec<_>>();

    let violations = public_recovery_apis_without_required_capabilities(&lines)
        .into_iter()
        .map(|(line_number, name)| {
            format!(
                "{}:{} production recovery API `{name}` lacks `AuthoritativeObjectStoreCapabilitiesV1`",
                source.strip_prefix(&workspace).unwrap_or(&source).display(),
                line_number + 1
            )
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "production-looking public recovery APIs must require startup capabilities:\n{}",
        violations.join("\n")
    );
}

#[test]
fn production_recovery_contract_forbids_checked_names_without_capability_parameter() {
    let lines = [
        "    pub async fn recover_with_owner_and_relation_catalog_checked(",
        "        store: Arc<dyn ObjectStore>,",
        "    ) -> Result<Self, RecoveryError> {",
        "        Ok(todo!())",
        "    }",
    ];

    assert_eq!(
        public_recovery_apis_without_required_capabilities(&lines),
        vec![(0, "recover_with_owner_and_relation_catalog_checked")]
    );
}

#[test]
fn production_recovery_public_apis_do_not_call_unchecked_authority_constructors() {
    let workspace = workspace_root();
    let source = workspace.join("crates/velorix-runtime/src/recovery.rs");
    let contents = fs::read_to_string(&source).expect("read recovery source");
    let lines = contents.lines().collect::<Vec<_>>();

    let mut violations = Vec::new();
    for line_number in 0..lines.len() {
        if let Some(pattern) = forbidden_unchecked_authority_constructor_use_in_public_recovery_api(
            &lines,
            line_number,
        ) {
            violations.push(format!(
                "{}:{} public production recovery API uses unchecked authority constructor `{pattern}`",
                source.strip_prefix(&workspace).unwrap_or(&source).display(),
                line_number + 1
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "non-bootstrap public recovery APIs must use startup-capability-backed constructors:\n{}",
        violations.join("\n")
    );
}

#[test]
fn production_recovery_contract_forbids_unchecked_authority_constructors_in_checked_public_apis() {
    let lines = [
        "    pub async fn recover_with_owner_and_relation_catalog_checked(",
        "        store: Arc<dyn ObjectStore>,",
        "        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,",
        "    ) -> Result<Self, RecoveryError> {",
        "        let publisher = CheckpointPublisher::new(Arc::clone(&store));",
        "        Ok(todo!())",
        "    }",
    ];

    assert_eq!(
        forbidden_unchecked_authority_constructor_use_in_public_recovery_api(&lines, 4),
        Some("CheckpointPublisher::new(")
    );
}

#[test]
fn production_recovery_contract_forbids_bootstrap_delegation_in_checked_public_apis() {
    let lines = [
        "    pub async fn recover_with_owner_and_relation_catalog_checked(",
        "        store: Arc<dyn ObjectStore>,",
        "        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,",
        "    ) -> Result<Self, RecoveryError> {",
        "        Self::recover_bootstrap_with_owner_and_relation_catalog(store, owner, catalog).await",
        "    }",
    ];

    assert_eq!(
        forbidden_unchecked_authority_constructor_use_in_public_recovery_api(&lines, 4),
        Some("Self::recover_bootstrap")
    );
}

#[test]
fn production_recovery_contract_allows_unchecked_authority_constructors_in_bootstrap_public_apis() {
    let lines = [
        "    pub async fn recover_bootstrap_with_owner_and_relation_catalog(",
        "        store: Arc<dyn ObjectStore>,",
        "    ) -> Result<Self, RecoveryError> {",
        "        let publisher = CheckpointPublisher::new(Arc::clone(&store));",
        "        Ok(todo!())",
        "    }",
    ];

    assert_eq!(
        forbidden_unchecked_authority_constructor_use_in_public_recovery_api(&lines, 3),
        None
    );
}

#[test]
fn production_recovery_contract_forbids_direct_bootstrap_recovery_callers() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("crates/velorix-runtime/src/production_surface.rs");
    for (line, pattern) in [
        (
            "    RecoveredRuntime::recover_bootstrap(store).await?;",
            "RecoveredRuntime::recover_bootstrap(",
        ),
        (
            "    RecoveredRuntime::recover_bootstrap_with_owner(store, owner).await?;",
            "RecoveredRuntime::recover_bootstrap_with_owner(",
        ),
        (
            "    RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version(store, publisher, 7).await?;",
            "RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version(",
        ),
        (
            "    RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(store, owner, relation_id, relation_version).await?;",
            "RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(",
        ),
        (
            "    RecoveredRuntime::recover_bootstrap_with_slatedb_state_store_and_relation_catalog(store, db_path, owner, catalog).await?;",
            "RecoveredRuntime::recover_bootstrap_with_slatedb_state_store_and_relation_catalog(",
        ),
        (
            "    RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog(store, db_path, 7, owner, catalog).await?;",
            "RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog(",
        ),
        (
            "    RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(store, 7, owner, catalog).await?;",
            "RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(",
        ),
    ] {
        assert_eq!(
            forbidden_bootstrap_recovery_use(workspace, &source, &[line], 0),
            Some(pattern)
        );
    }
}

#[test]
fn production_recovery_contract_forbids_bootstrap_recovered_query_helper_callers() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("crates/velorix-runtime/src/production_surface.rs");
    for (line, pattern) in [
        (
            "    query_bootstrap_recovered_materialized_view(store, sql).await?;",
            "query_bootstrap_recovered_materialized_view",
        ),
        (
            "    query_bootstrap_recovered_materialized_view_with_policy(store, sql, policy).await?;",
            "query_bootstrap_recovered_materialized_view_with_policy",
        ),
        (
            "    query_bootstrap_recovered_materialized_view_with_policy_and_limiter(store, sql, policy, limiter).await?;",
            "query_bootstrap_recovered_materialized_view_with_policy_and_limiter",
        ),
        (
            "    query_bootstrap_persisted_recovered_materialized_view(store, query_id).await?;",
            "query_bootstrap_persisted_recovered_materialized_view",
        ),
        (
            "    query_bootstrap_persisted_recovered_materialized_view_with_limiter(store, query_id, limiter).await?;",
            "query_bootstrap_persisted_recovered_materialized_view_with_limiter",
        ),
        (
            "use velorix_runtime::query::query_bootstrap_recovered_materialized_view as recover;",
            "query_bootstrap_recovered_materialized_view",
        ),
    ] {
        assert_eq!(
            forbidden_bootstrap_recovered_query_use(workspace, &source, &[line], 0),
            Some(pattern)
        );
    }
}

#[test]
fn production_sources_do_not_call_bootstrap_recovered_query_helpers() {
    let workspace = workspace_root();

    let mut violations = Vec::new();
    for source in production_source_scan_sources(&workspace) {
        let contents = fs::read_to_string(&source).expect("read Rust source");
        let lines = contents.lines().collect::<Vec<_>>();
        for line_number in 0..lines.len() {
            if let Some(pattern) =
                forbidden_bootstrap_recovered_query_use(&workspace, &source, &lines, line_number)
            {
                violations.push(format!(
                    "{}:{} calls bootstrap recovered-query helper `{pattern}`",
                    source.strip_prefix(&workspace).unwrap_or(&source).display(),
                    line_number + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "production source must not call bootstrap recovered-query helpers:\n{}",
        violations.join("\n")
    );
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

fn forbidden_bootstrap_recovered_query_use(
    workspace: &Path,
    source: &Path,
    lines: &[&str],
    line_number: usize,
) -> Option<&'static str> {
    let line = lines[line_number];
    forbidden_bootstrap_recovered_query_patterns()
        .iter()
        .copied()
        .find(|pattern| {
            line_mentions_bootstrap_recovered_query_helper(line, pattern)
                && !line.trim_start().starts_with("pub async fn")
                && !allowed_bootstrap_recovered_query_use(
                    workspace,
                    source,
                    lines,
                    line_number,
                    pattern,
                )
        })
}

fn forbidden_bootstrap_recovered_query_patterns() -> &'static [&'static str] {
    &[
        "query_bootstrap_recovered_materialized_view_with_policy_and_limiter",
        "query_bootstrap_recovered_materialized_view_with_policy",
        "query_bootstrap_recovered_materialized_view",
        "query_bootstrap_persisted_recovered_materialized_view_with_limiter",
        "query_bootstrap_persisted_recovered_materialized_view",
    ]
}

fn line_mentions_bootstrap_recovered_query_helper(line: &str, pattern: &str) -> bool {
    let trimmed = line.trim_start();
    if trimmed.starts_with("//") || trimmed.starts_with("///") {
        return false;
    }

    trimmed.match_indices(pattern).any(|(start, _)| {
        let before = trimmed[..start].chars().next_back();
        let after = trimmed[start + pattern.len()..].chars().next();
        !is_rust_ident_char(before) && !is_rust_ident_char(after)
    })
}

fn is_rust_ident_char(character: Option<char>) -> bool {
    character.is_some_and(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn allowed_bootstrap_recovered_query_use(
    workspace: &Path,
    source: &Path,
    lines: &[&str],
    line_number: usize,
    pattern: &str,
) -> bool {
    if source == workspace.join("crates/velorix-runtime/src/query.rs") {
        return [
            "pub async fn query_bootstrap_recovered_materialized_view(",
            "pub async fn query_bootstrap_recovered_materialized_view_with_policy(",
            "pub async fn query_bootstrap_recovered_materialized_view_with_policy_and_limiter(",
        ]
        .iter()
        .any(|signature| line_is_inside_function(lines, line_number, signature));
    }

    if source == workspace.join("crates/velorix-runtime/src/persisted_query.rs") {
        if pattern == "query_bootstrap_recovered_materialized_view_with_policy_and_limiter"
            && lines[line_number].trim() == format!("{pattern},")
        {
            return true;
        }

        return match pattern {
            "query_bootstrap_persisted_recovered_materialized_view_with_limiter" => {
                line_is_inside_function(
                    lines,
                    line_number,
                    "pub async fn query_bootstrap_persisted_recovered_materialized_view(",
                )
            }
            "query_bootstrap_recovered_materialized_view_with_policy_and_limiter" => {
                line_is_inside_function(
                    lines,
                    line_number,
                    "pub async fn query_bootstrap_persisted_recovered_materialized_view_with_limiter(",
                )
            }
            _ => false,
        };
    }

    false
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
        "RecoveredRuntime::recover_bootstrap(",
        "RecoveredRuntime::recover_bootstrap_with_owner(",
        "RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version(",
        "RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(",
        "RecoveredRuntime::recover_bootstrap_with_slatedb_state_store_and_relation_catalog(",
        "RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog(",
        "RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(",
    ]
}

fn ambiguous_public_recovery_api_name(line: &str) -> Option<&str> {
    let full_name = public_recovery_api_name(line)?;

    if full_name.starts_with("recover_bootstrap") || full_name.ends_with("_checked") {
        None
    } else {
        Some(full_name)
    }
}

fn public_recovery_apis_without_required_capabilities<'a>(
    lines: &'a [&'a str],
) -> Vec<(usize, &'a str)> {
    lines
        .iter()
        .enumerate()
        .filter_map(|(line_number, line)| {
            let name = public_recovery_api_name(line)?;
            if name.starts_with("recover_bootstrap") {
                return None;
            }
            (!function_signature_has_startup_capabilities(lines, line_number))
                .then_some((line_number, name))
        })
        .collect()
}

fn function_signature_has_startup_capabilities(lines: &[&str], signature_line: usize) -> bool {
    lines[signature_line..]
        .iter()
        .take_while(|line| !line.contains('{'))
        .chain(
            lines[signature_line..]
                .iter()
                .find(|line| line.contains('{')),
        )
        .any(|line| line.contains("AuthoritativeObjectStoreCapabilitiesV1"))
}

fn forbidden_unchecked_authority_constructor_use_in_public_recovery_api(
    lines: &[&str],
    line_number: usize,
) -> Option<&'static str> {
    let line = lines[line_number].trim_start();
    if line.starts_with("//") || line.starts_with("///") {
        return None;
    }

    let pattern = unchecked_authority_constructor_patterns()
        .iter()
        .copied()
        .find(|pattern| line.contains(pattern))?;
    let signature_line = lines[..=line_number]
        .iter()
        .rposition(|line| public_recovery_api_name(line).is_some())?;
    let api_name = public_recovery_api_name(lines[signature_line])?;

    if api_name.starts_with("recover_bootstrap")
        || !line_is_inside_function_starting_at(lines, signature_line, line_number)
    {
        return None;
    }

    Some(pattern)
}

fn public_recovery_api_name(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let name = trimmed
        .strip_prefix("pub async fn ")
        .and_then(|rest| rest.split_once('('))
        .map(|(name, _)| name)?;

    (name == "recover" || name.starts_with("recover_")).then_some(name)
}

fn unchecked_authority_constructor_patterns() -> &'static [&'static str] {
    &[
        "CheckpointPublisher::new(",
        "CheckpointPublisher::with_slatedb_state_store(",
        "IngestLog::new(",
        "RelationCatalogRegistry::new(",
        "Self::recover_bootstrap",
        "RecoveredRuntime::recover_bootstrap",
    ]
}

fn line_is_inside_function_starting_at(
    lines: &[&str],
    signature_line: usize,
    line_number: usize,
) -> bool {
    let mut brace_depth = 0usize;
    let mut opened = false;
    for (offset, line) in lines[signature_line..=line_number].iter().enumerate() {
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
        if opened && brace_depth == 0 && signature_line + offset < line_number {
            return false;
        }
    }

    opened && brace_depth > 0
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
        && pattern == "RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record("
        && line_is_inside_function(
            lines,
            line_number,
            "pub async fn query_bootstrap_recovered_materialized_view_with_policy_and_limiter(",
        )
    {
        return true;
    }

    if source == workspace.join("crates/velorix-cli/src/main.rs")
        && [
            "RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(",
            "RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(",
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
        "RecoveredRuntime::recover_bootstrap(" => [
            "async fn local_recovery_rejects_json_deltabatch_ingest_object()",
            "async fn local_recovery_rejects_manifest_state_with_unexpected_owner()",
        ]
        .iter()
        .any(|signature| line_is_inside_function(lines, line_number, signature)),
        "RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version(" => {
            ["async fn local_recovery_rejects_selected_checkpoint_when_payload_is_missing()"]
                .iter()
                .any(|signature| line_is_inside_function(lines, line_number, signature))
                || line_is_inside_function(
                    lines,
                    line_number,
                    "async fn slatedb_local_recovery_can_use_selected_published_checkpoint()",
                )
        }
        "RecoveredRuntime::recover_bootstrap_with_slatedb_state_store_and_relation_catalog(" => {
            line_is_inside_function(
                lines,
                line_number,
                "async fn slatedb_local_recovery_rejects_raw_state_manifest()",
            )
        }
        _ => false,
    }
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
