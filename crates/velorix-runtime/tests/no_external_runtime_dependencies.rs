use std::{fs, path::Path};

const PRODUCT_SOURCE_DIRS: &[&str] = &[
    "crates/velorix-api/src",
    "crates/velorix-core/src",
    "crates/velorix-runtime/src",
    "crates/velorix-storage/src",
];

const FORBIDDEN_RUNTIME_DEPENDENCIES: &[&str] = &[
    "feldera",
    "dbsp",
    "pipeline-manager",
    "pipeline_manager",
    "compiler-worker",
    "compiler_worker",
    "persistentvolumeclaim",
    "volumeclaimtemplates",
    "pvc",
    "rustc",
    "javac",
    ".jar",
];

#[test]
fn product_runtime_source_has_no_external_compiler_or_pvc_dependencies() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("runtime crate should be under crates/velorix-runtime");
    let mut violations = Vec::new();

    for relative_dir in PRODUCT_SOURCE_DIRS {
        collect_forbidden_source_references(
            &repo_root.join(relative_dir),
            relative_dir,
            &mut violations,
        );
    }

    assert!(
        violations.is_empty(),
        "product runtime source must not require external compiler/image/JAR/PVC paths:\n{}",
        violations.join("\n")
    );
}

fn collect_forbidden_source_references(
    directory: &Path,
    relative_dir: &str,
    violations: &mut Vec<String>,
) {
    for entry in fs::read_dir(directory).unwrap_or_else(|error| {
        panic!(
            "failed to read product source directory `{}`: {error}",
            directory.display()
        )
    }) {
        let entry = entry.expect("source directory entry should be readable");
        let path = entry.path();
        if path.is_dir() {
            let nested = format!("{relative_dir}/{}", entry.file_name().to_string_lossy());
            collect_forbidden_source_references(&path, &nested, violations);
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }

        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read `{}`: {error}", path.display()));
        let normalized = source.to_ascii_lowercase();
        for forbidden in FORBIDDEN_RUNTIME_DEPENDENCIES {
            if normalized.contains(forbidden) {
                violations.push(format!("{relative_dir}: contains `{forbidden}`"));
            }
        }
    }
}
