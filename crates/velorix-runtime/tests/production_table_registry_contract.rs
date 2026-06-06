use std::{
    fs,
    path::{Path, PathBuf},
};

#[test]
fn production_sources_do_not_call_bootstrap_table_scan_apis() {
    let workspace = workspace_root();

    let mut violations = Vec::new();
    for source in production_source_scan_sources(&workspace) {
        let contents = fs::read_to_string(&source).expect("read Rust source");
        let lines = contents.lines().collect::<Vec<_>>();
        for line_number in 0..lines.len() {
            if let Some(pattern) =
                forbidden_bootstrap_table_scan_use(&workspace, &source, &lines, line_number)
            {
                violations.push(format!(
                    "{}:{} uses bootstrap table scan API pattern `{pattern}`",
                    source.strip_prefix(&workspace).unwrap_or(&source).display(),
                    line_number + 1
                ));
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
fn production_source_contract_forbids_direct_persisted_view_bootstrap_helper_callers() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("crates/velorix-runtime/src/production_surface.rs");
    let lines = ["    query_persisted_object_backed_view("];

    assert_eq!(
        forbidden_bootstrap_table_scan_use(workspace, &source, &lines, 0),
        Some("query_persisted_object_backed_view(")
    );
}

#[test]
fn production_source_contract_forbids_unchecked_storage_registry_registration() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("benches/local_incremental.rs");
    let lines = [
        "    let mut registry = StorageRegistry::new();",
        "    registry.register(\"primary\", \"memory://velorix/\", scan_store)?;",
    ];

    assert_eq!(
        forbidden_bootstrap_table_scan_use(workspace, &source, &lines, 1),
        Some("StorageRegistry::register(")
    );
}

#[test]
fn production_source_contract_forbids_multiline_unchecked_storage_registry_registration() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("benches/local_incremental.rs");
    let lines = [
        "    let mut registry: StorageRegistry = StorageRegistry::new();",
        "    registry",
        "        .register(\"primary\", \"memory://velorix/\", scan_store)?;",
    ];

    assert_eq!(
        forbidden_bootstrap_table_scan_use(workspace, &source, &lines, 2),
        Some("StorageRegistry::register(")
    );
}

#[test]
fn production_source_contract_forbids_default_unchecked_storage_registry_registration() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("benches/local_incremental.rs");
    let lines = [
        "    let mut registry: StorageRegistry = Default::default();",
        "    registry.register(\"primary\", \"memory://velorix/\", scan_store)?;",
    ];

    assert_eq!(
        forbidden_bootstrap_table_scan_use(workspace, &source, &lines, 1),
        Some("StorageRegistry::register(")
    );
}

#[test]
fn production_source_contract_forbids_ad_hoc_production_registry_probe_callers() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("benches/local_incremental.rs");
    let lines = [
        "    let mut registry = StorageRegistry::new();",
        "    registry.register_production_with_probe(",
        "        \"primary\",",
        "        \"memory://velorix/\",",
        "        scan_store,",
        "        authority_store,",
        "        \"benchmark-table-scan\",",
        "    ).await?;",
    ];

    assert_eq!(
        forbidden_bootstrap_table_scan_use(workspace, &source, &lines, 1),
        Some("register_production_with_probe(")
    );
}

#[test]
fn production_source_contract_does_not_link_register_receivers_across_functions() {
    let workspace = Path::new("/workspace");
    let source = workspace.join("benches/local_incremental.rs");
    let lines = [
        "fn bootstrap_fixture() {",
        "    let mut registry = StorageRegistry::new();",
        "}",
        "fn production_path(other: &mut ExternalRegistry) {",
        "    let registry = other;",
        "    registry.register(\"primary\")?;",
        "}",
    ];

    assert_eq!(
        forbidden_bootstrap_table_scan_use(workspace, &source, &lines, 5),
        None
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
fn production_source_scan_includes_top_level_e2e_tests() {
    let workspace = workspace_root();
    let sources = production_source_scan_sources(&workspace);

    for test_source in all_rust_files_under(&workspace.join("tests")) {
        assert!(
            sources.iter().any(|source| source == &test_source),
            "production table registry contract should scan {}",
            test_source
                .strip_prefix(&workspace)
                .unwrap_or(&test_source)
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
    sources.extend(all_rust_files_under(&workspace.join("tests")));
    sources.sort();
    sources
}

fn forbidden_bootstrap_table_scan_use(
    workspace: &Path,
    source: &Path,
    lines: &[&str],
    line_number: usize,
) -> Option<&'static str> {
    let line = lines[line_number];
    forbidden_bootstrap_table_scan_patterns()
        .iter()
        .copied()
        .find(|pattern| {
            (line.contains(pattern)
                || (pattern == &"StorageRegistry::register("
                    && line_calls_unchecked_storage_registry_register(lines, line_number)))
                && !allowed_bootstrap_table_scan_use(workspace, source, lines, line_number)
        })
}

fn forbidden_bootstrap_table_scan_patterns() -> &'static [&'static str] {
    &[
        "query_persisted_object_backed_input_with_policy(",
        "query_persisted_object_backed_view(",
        "query_object_backed_input_with_policy(",
        "query_object_backed_input_with_policy_and_limiter(",
        "query_object_backed_input_with_policy_and_metrics(",
        "StorageRegistry::register(",
        "register_production_with_probe(",
    ]
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

    if source == workspace.join("crates/velorix-runtime/src/storage_registry.rs") {
        return true;
    }

    false
}

fn line_calls_unchecked_storage_registry_register(lines: &[&str], line_number: usize) -> bool {
    let line = lines[line_number];
    if line.contains("StorageRegistry::new().register(")
        || line.contains("StorageRegistry::default().register(")
    {
        return true;
    }

    let Some((receiver, _)) = line.split_once(".register(") else {
        return false;
    };
    let registry_name = receiver
        .split_whitespace()
        .last()
        .or_else(|| previous_non_empty_line(lines, line_number));
    let Some(registry_name) = registry_name else {
        return false;
    };
    if storage_registry_initializer(registry_name) {
        return true;
    }

    current_function_previous_lines(lines, line_number).any(|previous| {
        binding_name(previous) == Some(registry_name) && storage_registry_initializer(previous)
    })
}

fn previous_non_empty_line<'a>(lines: &'a [&str], line_number: usize) -> Option<&'a str> {
    lines[..line_number]
        .iter()
        .rev()
        .map(|line| line.trim())
        .find(|line| !line.is_empty())
}

fn current_function_previous_lines<'a>(
    lines: &'a [&str],
    line_number: usize,
) -> impl Iterator<Item = &'a str> {
    lines[..line_number]
        .iter()
        .rev()
        .copied()
        .take_while(|line| !is_function_signature(line))
}

fn is_function_signature(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with("fn ")
        || line.starts_with("async fn ")
        || line.starts_with("pub fn ")
        || line.starts_with("pub async fn ")
}

fn binding_name(line: &str) -> Option<&str> {
    let (binding, _) = line.split_once('=')?;
    binding
        .trim()
        .split(':')
        .next()
        .and_then(|binding| binding.split_whitespace().last())
}

fn storage_registry_initializer(line: &str) -> bool {
    line.contains("StorageRegistry::new()")
        || line.contains("StorageRegistry::default()")
        || (line.contains("Default::default()")
            && line
                .split_once('=')
                .is_some_and(|(binding, _)| binding.contains("StorageRegistry")))
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

fn all_rust_files_under(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    collect_all_rust_files(root, &mut sources);
    sources.sort();
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

fn collect_all_rust_files(path: &Path, sources: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(path).expect("read source tree");
    for entry in entries {
        let path = entry.expect("read directory entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            collect_all_rust_files(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
}
