//! Benchmark: clone cost for runtime state structures.
//!
//! Measures the cost of deep-cloning BTreeMap state vs Arc-wrapped clone
//! at various sizes (100, 1000, 10000 rows).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};

/// Simulates a runtime row with column values (like CrossJoinRow).
#[derive(Clone)]
#[allow(dead_code)]
struct SimRow {
    values: BTreeMap<String, Value>,
    weight: i64,
}

fn make_row(key: &str, cols: usize) -> SimRow {
    let mut values = BTreeMap::new();
    values.insert("key".to_string(), json!(key));
    for c in 0..cols {
        values.insert(format!("col_{c}"), json!(format!("value_{key}_{c}")));
    }
    SimRow { values, weight: 1 }
}

fn make_state(n: usize, cols: usize) -> BTreeMap<String, SimRow> {
    (0..n)
        .map(|i| {
            let key = format!("row_{i:08}");
            (key.clone(), make_row(&key, cols))
        })
        .collect()
}

fn bench_clone_deep(state: &BTreeMap<String, SimRow>, iters: usize) -> u128 {
    let start = Instant::now();
    for _ in 0..iters {
        let _clone = state.clone();
    }
    start.elapsed().as_nanos() / iters as u128
}

fn bench_clone_arc<T: Clone>(state: &Arc<T>, iters: usize) -> u128 {
    let start = Instant::now();
    for _ in 0..iters {
        let _clone = state.clone(); // Arc clone = O(1)
    }
    start.elapsed().as_nanos() / iters as u128
}

fn bench_make_mut_no_change(state: &Arc<BTreeMap<String, SimRow>>, iters: usize) -> u128 {
    let start = Instant::now();
    for _ in 0..iters {
        let mut s = state.clone();
        // Arc::make_mut with refcount=1: no copy needed
        let _m = Arc::make_mut(&mut s);
    }
    start.elapsed().as_nanos() / iters as u128
}

fn bench_make_mut_with_change(state: &Arc<BTreeMap<String, SimRow>>, iters: usize) -> u128 {
    let start = Instant::now();
    for _ in 0..iters {
        let mut s = state.clone();
        // Arc::make_mut with refcount=2: must deep copy
        let m = Arc::make_mut(&mut s);
        m.insert("new_key".to_string(), make_row("new_key", 5));
    }
    start.elapsed().as_nanos() / iters as u128
}

#[test]
fn benchmark_clone_cost() {
    let sizes = [100, 1000, 10000];
    let cols = 10;
    let iters = 100;

    println!("\n=== Clone Cost Benchmark ({} cols per row) ===", cols);
    println!(
        "{:>8} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "rows", "deep_ns", "arc_ns", "make_mut0", "make_mut1", "ratio"
    );
    println!("{}", "-".repeat(80));

    for &n in &sizes {
        let state = make_state(n, cols);
        let arc_state = Arc::new(state.clone());

        let deep_ns = bench_clone_deep(&state, iters);
        let arc_ns = bench_clone_arc(&arc_state, iters);
        let mm0_ns = bench_make_mut_no_change(&arc_state, iters);
        let mm1_ns = bench_make_mut_with_change(&arc_state, iters);
        let ratio = deep_ns as f64 / arc_ns as f64;

        println!(
            "{:>8} {:>12} {:>12} {:>12} {:>12} {:>12.1}x",
            n, deep_ns, arc_ns, mm0_ns, mm1_ns, ratio
        );
    }

    println!("\n=== Temporal Join Right Index Clone ===");
    println!("right_index: BTreeMap<String, BTreeMap<i64, Vec<TemporalRow>>>");

    // Simulate temporal join right_index
    let sizes = [100, 1000, 5000];
    for &n in &sizes {
        let mut right_index: BTreeMap<String, BTreeMap<i64, Vec<SimRow>>> = BTreeMap::new();
        for i in 0..n {
            let key = format!("key_{:04}", i % 100);
            let time = (i * 1000) as i64;
            right_index
                .entry(key.clone())
                .or_default()
                .entry(time)
                .or_default()
                .push(make_row(&key, 10));
        }
        let arc_index = Arc::new(right_index.clone());

        let deep_ns = bench_clone_deep_map(&right_index, iters);
        let arc_ns = bench_clone_arc(&arc_index, iters);
        let ratio = deep_ns as f64 / arc_ns as f64;

        println!(
            "  {} rows: deep={}ns arc={}ns ratio={:.1}x",
            n, deep_ns, arc_ns, ratio
        );
    }
}

fn bench_clone_deep_map(
    state: &BTreeMap<String, BTreeMap<i64, Vec<SimRow>>>,
    iters: usize,
) -> u128 {
    let start = Instant::now();
    for _ in 0..iters {
        let _clone = state.clone();
    }
    start.elapsed().as_nanos() / iters as u128
}
