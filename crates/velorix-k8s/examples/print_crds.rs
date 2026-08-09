use kube::CustomResourceExt;
use serde_json::json;
use velorix_k8s::crd::{
    VelorixBenchmarkGate, VelorixCheckpointPolicy, VelorixDatabase, VelorixStream, VelorixTable,
    VelorixWorkerShard,
};

fn main() -> Result<(), serde_json::Error> {
    let crds = json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            VelorixDatabase::crd(),
            VelorixStream::crd(),
            VelorixTable::crd(),
            VelorixWorkerShard::crd(),
            VelorixCheckpointPolicy::crd(),
            VelorixBenchmarkGate::crd(),
        ],
    });

    serde_json::to_writer_pretty(std::io::stdout(), &crds)?;
    println!();
    Ok(())
}
