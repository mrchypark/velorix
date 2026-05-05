use std::{env, error::Error, io};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> BenchResult<()> {
    validate_s3_bench_env(|name| env::var(name).ok())?;

    Err(bench_error(
        "s3_incremental live workload is not implemented yet; refusing to emit local or synthetic benchmark JSON",
    ))
}

fn validate_s3_bench_env(get_env: impl Fn(&str) -> Option<String>) -> BenchResult<()> {
    if get_env("VELORIX_S3_COMPAT").as_deref() != Some("1") {
        return Err(bench_error(
            "s3_incremental is gated; set VELORIX_S3_COMPAT=1 to run against a real S3-compatible store",
        ));
    }

    let missing = required_s3_env()
        .iter()
        .copied()
        .filter(|name| get_env(name).is_none())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(bench_error(format!(
            "s3_incremental requires real S3 object store config; missing {}",
            missing.join(", ")
        )));
    }

    Ok(())
}

fn required_s3_env() -> &'static [&'static str] {
    &[
        "AWS_ENDPOINT_URL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "VELORIX_S3_BUCKET",
    ]
}

fn bench_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
