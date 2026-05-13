use std::{
    fs,
    path::{Path, PathBuf},
};

#[test]
fn production_sources_do_not_call_bootstrap_ingest_apis() {
    let workspace = workspace_root();
    let forbidden = [
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
                if line.contains(pattern)
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
fn production_like_ingest_harnesses_use_process_local_coordinator() {
    let workspace = workspace_root();
    for source in [
        workspace.join("benches/local_incremental.rs"),
        workspace.join("benches/s3_incremental.rs"),
        workspace.join("crates/velorix-runtime/tests/s3_compat_query.rs"),
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

        assert!(
            helper_append_receiver_is_coordinator(&contents),
            "{} should call append_catalog_validated_envelope on the coordinator helper receiver",
            source.strip_prefix(&workspace).unwrap_or(&source).display()
        );
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

    false
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
