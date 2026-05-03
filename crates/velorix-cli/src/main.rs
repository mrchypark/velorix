#![forbid(unsafe_code)]

use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::{CommandFactory, Parser, Subcommand};
use object_store::{local::LocalFileSystem, ObjectStore};
use velorix_runtime::recovery::RecoveredRuntime;

#[derive(Debug, Parser)]
#[command(name = "velorix-cli")]
#[command(about = "Local Velorix runtime utilities")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    RecoverLocal {
        #[arg(long)]
        object_store_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::RecoverLocal { object_store_dir }) => {
            let store = LocalFileSystem::new_with_prefix(&object_store_dir).with_context(|| {
                format!(
                    "failed to open local object store at {}",
                    object_store_dir.display()
                )
            })?;
            let recovered = RecoveredRuntime::recover(Arc::new(store) as Arc<dyn ObjectStore>)
                .await
                .context("failed to recover local runtime")?;
            let materialized_records = recovered.materialized_state().records().len();

            println!(
                "recovered checkpoint={:?} replayed_batches={} materialized_records={}",
                recovered.latest_checkpoint_version(),
                recovered.replayed_batch_count(),
                materialized_records
            );
        }
        None => {
            Cli::command().print_help()?;
            println!();
        }
    }

    Ok(())
}
