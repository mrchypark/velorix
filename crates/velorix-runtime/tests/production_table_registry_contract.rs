use std::{
    fs,
    path::{Path, PathBuf},
};

#[test]
fn production_sources_do_not_call_bootstrap_table_scan_apis() {
    let workspace = workspace_root();
    let forbidden = [
        "query_persisted_object_backed_input_with_policy(",
        "query_object_backed_input_with_policy(",
        "query_object_backed_input_with_policy_and_limiter(",
        "query_object_backed_input_with_policy_and_metrics(",
    ];

    let mut violations = Vec::new();
    for source in production_source_scan_sources(&workspace) {
        let contents = fs::read_to_string(&source).expect("read Rust source");
        let lines = contents.lines().collect::<Vec<_>>();
        for (line_number, line) in lines.iter().enumerate() {
            for pattern in forbidden {
                if line.contains(pattern)
                    && !allowed_bootstrap_table_scan_use(&workspace, &source, &lines, line_number)
                {
                    violations.push(format!(
                        "{}:{} uses bootstrap table scan API pattern `{pattern}`",
                        source.strip_prefix(&workspace).unwrap_or(&source).display(),
                        line_number + 1
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "production source must use registry-backed production table helpers:\n{}",
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
            "production table registry contract should scan {}",
            benchmark
                .strip_prefix(&workspace)
                .unwrap_or(&benchmark)
                .display()
        );
    }
}

#[test]
fn incremental_benchmarks_use_production_table_helpers() {
    let workspace = workspace_root();
    for benchmark in [
        workspace.join("benches/local_incremental.rs"),
        workspace.join("benches/s3_incremental.rs"),
    ] {
        let contents = fs::read_to_string(&benchmark).expect("read benchmark source");
        assert!(
            contents.contains(".create_production("),
            "{} should create production table specs",
            benchmark
                .strip_prefix(&workspace)
                .unwrap_or(&benchmark)
                .display()
        );
        assert!(
            !contents.contains("query_object_backed_input_with_policy("),
            "{} should not query raw object-backed input paths",
            benchmark
                .strip_prefix(&workspace)
                .unwrap_or(&benchmark)
                .display()
        );
    }
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
    sources.sort();
    sources
}

fn allowed_bootstrap_table_scan_use(
    workspace: &Path,
    source: &Path,
    lines: &[&str],
    line_number: usize,
) -> bool {
    if source == workspace.join("crates/velorix-runtime/src/query.rs") {
        return [
            "pub async fn query_object_backed_input_with_policy(",
            "pub async fn query_object_backed_input_with_policy_and_metrics(",
            "pub async fn query_object_backed_input_with_policy_and_limiter(",
            "async fn query_object_backed_input_with_policy_and_limiter_and_meter(",
        ]
        .iter()
        .any(|signature| line_is_inside_function(lines, line_number, signature));
    }

    if source == workspace.join("crates/velorix-runtime/src/persisted_table.rs") {
        return line_is_inside_function(
            lines,
            line_number,
            "pub async fn query_persisted_object_backed_input_with_policy(",
        );
    }

    if source == workspace.join("crates/velorix-runtime/src/persisted_view.rs") {
        return line_is_inside_function(
            lines,
            line_number,
            "pub async fn query_persisted_object_backed_view(",
        );
    }

    false
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
