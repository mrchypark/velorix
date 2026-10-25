use std::{fs, path::PathBuf};

use anyhow::Context;
use clap::Parser;

#[path = "../tests/fixtures/ingest_payload.rs"]
mod ingest_payload_fixture;

#[derive(Debug, Parser)]
#[command(about = "Encode the ingest-writer validation payload fixture")]
struct Args {
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    schema_fingerprint: String,
    #[arg(long, default_value = "scores")]
    stream_id: String,
    #[arg(long, default_value_t = 0)]
    partition_id: u32,
    #[arg(long)]
    start_offset_inclusive: u64,
    #[arg(long)]
    rows_json: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let payload = ingest_payload_fixture::encode_default_scores_payload(
        &args.schema_fingerprint,
        &args.stream_id,
        args.partition_id,
        args.start_offset_inclusive,
        &args.rows_json,
    )?;
    fs::write(&args.output, payload)
        .with_context(|| format!("failed to write {}", args.output.display()))
}
