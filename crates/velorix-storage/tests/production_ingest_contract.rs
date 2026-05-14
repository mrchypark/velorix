use std::{
    fs,
    path::{Path, PathBuf},
};

#[test]
fn production_sources_do_not_call_bootstrap_ingest_apis() {
    let workspace = workspace_root();
    let forbidden = [
        "IngestLog::new(",
        "IngestLog::append(",
        ".append_validated_envelope(",
        ".append_validated_envelope_single_writer(",
        ".replay_from(",
        ".replay_validated_envelopes_from(",
        "IngestBatch::new(",
    ];

    let mut violations = Vec::new();
    for source in production_source_scan_sources(&workspace) {
        let contents = fs::read_to_string(&source).expect("read Rust source");
        let lines = contents.lines().collect::<Vec<_>>();
        for (line_number, line) in lines.iter().enumerate() {
            for pattern in forbidden {
                if (line.contains(pattern)
                    || (pattern == "IngestLog::append("
                        && line_calls_ingest_log_append(&lines, line_number)))
                    && !allowed_bootstrap_ingest_use(
                        &workspace,
                        &source,
                        &lines,
                        line_number,
                        pattern,
                    )
                {
                    violations.push(format!(
                        "{}:{} uses bootstrap ingest API pattern `{pattern}`",
                        source.strip_prefix(&workspace).unwrap_or(&source).display(),
                        line_number + 1
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "production source must use catalog-aware ingest/recovery APIs:\n{}",
        violations.join("\n")
    );
}

#[test]
fn production_source_scan_includes_top_level_benchmarks() {
    let workspace = workspace_root();
    let sources = production_source_scan_sources(&workspace);

    for benchmark in top_level_rust_files(&workspace.join("benches")) {
        assert!(
            sources.iter().any(|source| source == &benchmark),
            "production ingest source contract should scan {}",
            benchmark
                .strip_prefix(&workspace)
                .unwrap_or(&benchmark)
                .display()
        );
    }
}

#[test]
fn production_source_scan_includes_top_level_e2e_tests() {
    let workspace = workspace_root();
    let sources = production_source_scan_sources(&workspace);

    for test in rust_sources_under(&workspace.join("tests")) {
        assert!(
            sources.iter().any(|source| source == &test),
            "production ingest source contract should scan {}",
            test.strip_prefix(&workspace).unwrap_or(&test).display()
        );
    }
}

#[test]
fn production_like_ingest_harnesses_use_process_local_coordinator() {
    let workspace = workspace_root();
    for source in [
        workspace.join("benches/local_incremental.rs"),
        workspace.join("benches/s3_incremental.rs"),
        workspace.join("crates/velorix-runtime/tests/persisted_query.rs"),
        workspace.join("crates/velorix-runtime/tests/s3_compat_query.rs"),
        workspace.join("tests/e2e/local_recovery.rs"),
    ] {
        let contents = fs::read_to_string(&source).expect("read production-like ingest harness");
        assert!(
            contents.contains("IngestAdmissionCoordinator::new("),
            "{} should construct the process-local ingest admission coordinator",
            source.strip_prefix(&workspace).unwrap_or(&source).display()
        );
        let append_call_violations = append_ingest_call_violations(&contents);
        assert!(
            append_call_violations.is_empty(),
            "{} should pass normal ingest appends through the process-local coordinator:\n{}",
            source.strip_prefix(&workspace).unwrap_or(&source).display(),
            append_call_violations.join("\n")
        );

        let catalog_append_violations = catalog_append_call_violations(&contents);
        assert!(
            catalog_append_violations.is_empty(),
            "{} should not bypass the process-local coordinator for catalog-aware appends:\n{}",
            source.strip_prefix(&workspace).unwrap_or(&source).display(),
            catalog_append_violations.join("\n")
        );

        assert!(
            helper_append_receiver_is_coordinator(&contents),
            "{} should call append_catalog_validated_envelope on the coordinator helper receiver",
            source.strip_prefix(&workspace).unwrap_or(&source).display()
        );
    }
}

#[test]
fn production_like_authority_harnesses_use_checked_object_store_constructors() {
    let workspace = workspace_root();
    for source in [
        workspace.join("benches/local_incremental.rs"),
        workspace.join("benches/s3_incremental.rs"),
        workspace.join("crates/velorix-runtime/tests/s3_compat_query.rs"),
    ] {
        let contents = fs::read_to_string(&source).expect("read production-like authority harness");
        for required in [
            "probe_authoritative_object_store_capabilities(",
            "IngestLog::new_checked(",
            "CheckpointPublisher::new_checked(",
            "RelationCatalogRegistry::new_checked(",
        ] {
            assert!(
                contents.contains(required),
                "{} should use startup capability evidence before `{required}` authority setup",
                source.strip_prefix(&workspace).unwrap_or(&source).display()
            );
        }
    }
}

fn append_ingest_call_violations(contents: &str) -> Vec<String> {
    let lines = contents.lines().collect::<Vec<_>>();
    lines
        .iter()
        .enumerate()
        .filter_map(|(line_number, line)| {
            if !line.contains("append_ingest_envelope(")
                || line.contains("fn append_ingest_envelope(")
            {
                return None;
            }

            let uses_coordinator = lines
                .iter()
                .skip(line_number)
                .take(4)
                .any(|candidate| candidate.contains("&ingest_coordinator"));
            (!uses_coordinator)
                .then(|| format!("line {} starts `{}`", line_number + 1, line.trim()))
        })
        .collect()
}

#[test]
fn production_source_contract_forbids_direct_ingest_log_append_method_callers() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("tests/e2e/production_like.rs");
    let lines = [
        "async fn production_path() {",
        "    let ingest_log = IngestLog::new(store);",
        "    ingest_log.append(&legacy_batch).await?;",
        "}",
    ];

    assert!(!allowed_bootstrap_ingest_use(
        workspace,
        &source,
        &lines,
        2,
        "IngestLog::append("
    ));
    assert!(line_calls_ingest_log_append(&lines, 2));
}

#[test]
fn production_source_contract_forbids_unchecked_ingest_log_constructors() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("crates/velorix-runtime/src/production_path.rs");
    let lines = [
        "async fn production_path() {",
        "    let ingest_log = IngestLog::new(store);",
        "}",
    ];

    assert!(!allowed_bootstrap_ingest_use(
        workspace,
        &source,
        &lines,
        1,
        "IngestLog::new("
    ));
}

#[test]
fn production_source_contract_allows_intentional_json_deltabatch_rejection_fixture() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("tests/e2e/local_recovery.rs");
    let lines = [
        "#[tokio::test]",
        "async fn local_recovery_rejects_json_deltabatch_ingest_object() {",
        "    let ingest_log = IngestLog::new(store);",
        "    let legacy_batch = IngestBatch::new(\"orders\", 0, 0, 1, bytes).unwrap();",
        "    ingest_log.append(&legacy_batch).await.unwrap();",
        "}",
    ];

    assert!(allowed_bootstrap_ingest_use(
        workspace,
        &source,
        &lines,
        3,
        "IngestBatch::new("
    ));
    assert!(allowed_bootstrap_ingest_use(
        workspace,
        &source,
        &lines,
        4,
        "IngestLog::append("
    ));
}

#[test]
fn ingest_harness_contract_forbids_direct_catalog_aware_append_receiver() {
    let contents = r#"
async fn append_ingest_envelope(
    ingest_coordinator: &IngestAdmissionCoordinator,
    ingest_log: &IngestLog,
    bytes: Bytes,
) {
    IngestLog::append_catalog_validated_envelope_single_writer(ingest_log, bytes).await?;
}
"#;

    let violations = catalog_append_call_violations(contents);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("append_catalog_validated_envelope_single_writer"));
    assert!(violations[0].contains("receiver `IngestLog`"));
}

#[test]
fn ingest_harness_contract_allows_multiline_coordinator_catalog_append() {
    let contents = r#"
async fn append_ingest_envelope(
    ingest_coordinator: &IngestAdmissionCoordinator,
    bytes: Bytes,
) {
    ingest_coordinator
        .append_catalog_validated_envelope(bytes)
        .await?;
}
"#;

    assert!(catalog_append_call_violations(contents).is_empty());
}

#[test]
fn ingest_harness_contract_allows_inline_coordinator_catalog_append() {
    let contents = r#"
async fn append_ingest_envelope(ingest_coordinator: &IngestAdmissionCoordinator, bytes: Bytes) {
    ingest_coordinator.append_catalog_validated_envelope(bytes).await?;
}
"#;

    assert!(catalog_append_call_violations(contents).is_empty());
}

#[test]
fn ingest_harness_contract_allows_ufcs_coordinator_catalog_append() {
    let contents = r#"
async fn append_ingest_envelope(ingest_coordinator: &IngestAdmissionCoordinator, bytes: Bytes) {
    IngestAdmissionCoordinator::append_catalog_validated_envelope(ingest_coordinator, bytes)
        .await?;
}
"#;

    assert!(catalog_append_call_violations(contents).is_empty());
}

fn catalog_append_call_violations(contents: &str) -> Vec<String> {
    let lines = contents.lines().collect::<Vec<_>>();
    lines
        .iter()
        .enumerate()
        .filter_map(|(line_number, line)| {
            if !line_calls_catalog_append(line) {
                return None;
            }

            let receiver = catalog_append_receiver(&lines, line_number);
            (receiver.as_deref() != Some("ingest_coordinator")).then(|| {
                format!(
                    "line {} starts `{}` with receiver `{}`",
                    line_number + 1,
                    line.trim(),
                    receiver.as_deref().unwrap_or("<unknown>")
                )
            })
        })
        .collect()
}

fn line_calls_catalog_append(line: &str) -> bool {
    catalog_append_dot_pattern(line).is_some()
        || [
            "IngestLog::append_catalog_validated_envelope(",
            "IngestLog::append_catalog_validated_envelope_single_writer(",
            "IngestAdmissionCoordinator::append_catalog_validated_envelope(",
        ]
        .into_iter()
        .any(|pattern| line.contains(pattern))
}

fn catalog_append_dot_pattern(line: &str) -> Option<&'static str> {
    [
        ".append_catalog_validated_envelope(",
        ".append_catalog_validated_envelope_single_writer(",
    ]
    .into_iter()
    .find(|pattern| line.contains(pattern))
}

fn catalog_append_receiver(lines: &[&str], line_number: usize) -> Option<String> {
    let line = lines[line_number].trim();
    if line.contains("IngestLog::append_catalog_validated_envelope(")
        || line.contains("IngestLog::append_catalog_validated_envelope_single_writer(")
    {
        return Some("IngestLog".to_string());
    }
    if line_uses_coordinator_ufcs_receiver(line) {
        return Some("ingest_coordinator".to_string());
    }

    let pattern = catalog_append_dot_pattern(line)?;
    line.split_once(pattern)
        .and_then(|(receiver, _)| receiver.split_whitespace().last())
        .filter(|receiver| !receiver.is_empty())
        .or_else(|| previous_non_empty_line(lines, line_number))
        .map(str::to_string)
}

fn line_uses_coordinator_ufcs_receiver(line: &str) -> bool {
    line.split_once("IngestAdmissionCoordinator::append_catalog_validated_envelope(")
        .and_then(|(_, arguments)| arguments.split(',').next())
        .is_some_and(|receiver| {
            matches!(
                receiver.trim(),
                "ingest_coordinator" | "&ingest_coordinator"
            )
        })
}

fn helper_append_receiver_is_coordinator(contents: &str) -> bool {
    let Some(helper_start) = contents.find("fn append_ingest_envelope(") else {
        return false;
    };
    let helper = &contents[helper_start..];
    if !helper.contains("ingest_coordinator: &IngestAdmissionCoordinator") {
        return false;
    }

    let lines = helper.lines().collect::<Vec<_>>();
    lines.iter().enumerate().any(|(line_number, line)| {
        line.contains(".append_catalog_validated_envelope(")
            && lines
                .iter()
                .take(line_number)
                .rev()
                .find(|candidate| !candidate.trim().is_empty())
                .is_some_and(|receiver| receiver.trim() == "ingest_coordinator")
    })
}

fn previous_non_empty_line<'a>(lines: &'a [&str], line_number: usize) -> Option<&'a str> {
    lines[..line_number]
        .iter()
        .rev()
        .map(|line| line.trim())
        .find(|line| !line.is_empty())
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("storage crate lives under crates/velorix-storage")
        .to_path_buf()
}

fn production_source_scan_sources(workspace: &Path) -> Vec<PathBuf> {
    let mut sources = rust_sources_under(&workspace.join("crates"));
    sources.extend(top_level_rust_files(&workspace.join("benches")));
    sources.extend(rust_sources_under(&workspace.join("tests")));
    sources.sort();
    sources
}

fn allowed_bootstrap_ingest_use(
    workspace: &Path,
    source: &Path,
    lines: &[&str],
    line_number: usize,
    pattern: &str,
) -> bool {
    let storage_log = workspace.join("crates/velorix-storage/src/log.rs");
    if source == storage_log {
        return true;
    }

    let runtime_recovery = workspace.join("crates/velorix-runtime/src/recovery.rs");
    if source == runtime_recovery && pattern == "IngestLog::new(" {
        return line_is_inside_function(
            lines,
            line_number,
            "async fn recover_with_publisher_and_relation_catalog(",
        ) || line_is_inside_function(
            lines,
            line_number,
            "async fn recover_with_selected_manifest_and_relation_catalog(",
        );
    }
    if source == runtime_recovery && pattern == ".replay_validated_envelopes_from(" {
        let has_expected_call =
            lines[line_number].contains(".replay_validated_envelopes_from(&replay_checkpoints)");
        let following_context = lines
            .iter()
            .skip(line_number)
            .take(16)
            .copied()
            .collect::<Vec<_>>()
            .join("\n");
        return has_expected_call
            && following_context.contains(
                "prototype_delta_batch_from_arrow_envelope(&envelope, &relation_catalog)",
            );
    }

    let local_recovery_e2e = workspace.join("tests/e2e/local_recovery.rs");
    if source == local_recovery_e2e && pattern == "IngestBatch::new(" {
        return line_is_inside_function(
            lines,
            line_number,
            "async fn local_recovery_rejects_json_deltabatch_ingest_object()",
        );
    }
    if source == local_recovery_e2e && pattern == "IngestLog::append(" {
        return line_is_inside_function(
            lines,
            line_number,
            "async fn local_recovery_rejects_json_deltabatch_ingest_object()",
        );
    }

    false
}

fn line_calls_ingest_log_append(lines: &[&str], line_number: usize) -> bool {
    let line = lines[line_number];
    let Some((receiver, _)) = line.split_once(".append(") else {
        return false;
    };
    let receiver = receiver
        .split_whitespace()
        .last()
        .or_else(|| previous_non_empty_line(lines, line_number));
    let Some(receiver) = receiver else {
        return false;
    };

    if ingest_log_initializer(receiver) {
        return true;
    }

    current_function_previous_lines(lines, line_number).any(|previous| {
        binding_name(previous) == Some(receiver) && ingest_log_initializer(previous)
    })
}

fn ingest_log_initializer(line: &str) -> bool {
    line.contains("IngestLog::new(") || line.contains("IngestLog::new_checked(")
}

fn binding_name(line: &str) -> Option<&str> {
    let (binding, _) = line.split_once('=')?;
    binding
        .trim()
        .split(':')
        .next()
        .and_then(|binding| binding.split_whitespace().last())
}

fn current_function_previous_lines<'a>(
    lines: &'a [&str],
    line_number: usize,
) -> impl Iterator<Item = &'a str> {
    lines[..line_number]
        .iter()
        .rev()
        .map(|line| *line)
        .take_while(|line| !is_function_signature(line))
}

fn is_function_signature(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with("fn ")
        || line.starts_with("async fn ")
        || line.starts_with("pub fn ")
        || line.starts_with("pub async fn ")
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
