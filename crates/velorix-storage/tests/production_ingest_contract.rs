use std::{
    fs,
    path::{Path, PathBuf},
};

#[test]
fn production_sources_do_not_call_bootstrap_ingest_apis() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("storage crate lives under crates/velorix-storage");
    let forbidden = [
        "IngestLog::append(",
        ".append_validated_envelope(",
        ".append_validated_envelope_single_writer(",
        ".replay_from(",
        ".replay_validated_envelopes_from(",
        "IngestBatch::new(",
    ];

    let mut violations = Vec::new();
    for source in rust_sources_under(&workspace.join("crates")) {
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
                        source.strip_prefix(workspace).unwrap_or(&source).display(),
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
