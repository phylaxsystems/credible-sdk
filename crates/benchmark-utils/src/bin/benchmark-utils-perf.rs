use benchmark_utils::{
    BenchmarkMode,
    BenchmarkPackage,
    BenchmarkPhaseStats,
    BenchmarkPreset,
};
use std::{
    error::Error,
    fs,
    path::PathBuf,
    str::FromStr,
    thread,
    time::{
        Duration,
        Instant,
    },
};
use tokio::runtime::Runtime;

/// CLI parameters for the standalone profiling driver.
///
/// The extra warmup and pause options exist so `samply` can capture the hot loop
/// after benchmark fixture construction has already populated caches.
struct Args {
    preset: BenchmarkPreset,
    mode: BenchmarkMode,
    iters: usize,
    warmup_iters: usize,
    pause_after_warmup_ms: u64,
    ready_file: Option<PathBuf>,
}

fn usage() -> &'static str {
    "Usage: benchmark-utils-perf [--preset <preset>] [--mode <mode>] [--iters <n>] [--warmup-iters <n>] [--pause-after-warmup-ms <ms>] [--ready-file <path>]

Presets:
  avg_block_0_aa
  avg_block_100_aa
  erc20_transaction_aa
  uniswap_transaction_aa

Modes:
  vanilla
  trace_only
  store_read_only
  assertion_setup_only
  assertions_only
  full"
}

fn parse_args() -> Result<Args, String> {
    let mut parsed = Args {
        preset: BenchmarkPreset::AvgBlock100Aa,
        mode: BenchmarkMode::Full,
        iters: 1usize,
        warmup_iters: 0usize,
        pause_after_warmup_ms: 0u64,
        ready_file: None,
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            "--preset" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --preset".to_string())?;
                parsed.preset = BenchmarkPreset::from_str(&value)
                    .map_err(|_| format!("invalid preset: {value}"))?;
            }
            "--mode" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --mode".to_string())?;
                parsed.mode = BenchmarkMode::from_str(&value)
                    .map_err(|_| format!("invalid mode: {value}"))?;
            }
            "--iters" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --iters".to_string())?;
                parsed.iters = value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid iteration count: {value}"))?;
            }
            "--warmup-iters" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --warmup-iters".to_string())?;
                parsed.warmup_iters = value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid warmup iteration count: {value}"))?;
            }
            "--pause-after-warmup-ms" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --pause-after-warmup-ms".to_string())?;
                parsed.pause_after_warmup_ms = value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid pause duration: {value}"))?;
            }
            "--ready-file" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --ready-file".to_string())?;
                parsed.ready_file = Some(PathBuf::from(value));
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(parsed)
}

fn run_once(preset: BenchmarkPreset, mode: BenchmarkMode) -> Result<BenchmarkPhaseStats, String> {
    let package = BenchmarkPackage::new(preset.load_definition());

    match mode {
        BenchmarkMode::Vanilla => {
            let mut package = package;
            package
                .run_vanilla_phase()
                .map_err(|err| format!("{err:?}"))
        }
        BenchmarkMode::TraceOnly => {
            let mut package = package;
            package.run_trace_only().map_err(|err| format!("{err:?}"))
        }
        BenchmarkMode::StoreReadOnly => {
            let prepared = package
                .prepare_store_read()
                .map_err(|err| format!("{err:?}"))?;
            prepared.run().map_err(|err| format!("{err:?}"))
        }
        BenchmarkMode::AssertionSetupOnly => {
            let prepared = package
                .prepare_assertion_phases()
                .map_err(|err| format!("{err:?}"))?;
            prepared
                .run_assertion_setup_only()
                .map_err(|err| format!("{err:?}"))
        }
        BenchmarkMode::AssertionsOnly => {
            let prepared = package
                .prepare_assertion_phases()
                .map_err(|err| format!("{err:?}"))?;
            prepared
                .run_assertions_only()
                .map_err(|err| format!("{err:?}"))
        }
        BenchmarkMode::Full => {
            let mut package = package;
            package.run_full_phase().map_err(|err| format!("{err:?}"))
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args().map_err(|err| format!("{err}\n\n{}", usage()))?;

    let runtime = Runtime::new()?;
    let _enter_guard = runtime.enter();

    let overall = Instant::now();
    // Warmup iterations intentionally pay the one-time artifact decode and fixture
    // setup costs before the measured iterations begin.
    for iteration in 0..args.warmup_iters {
        let started = Instant::now();
        let stats = run_once(args.preset, args.mode)?;
        println!(
            "warmup_iteration={} preset={} mode={} elapsed_ms={} stats={:?}",
            iteration + 1,
            args.preset.as_str(),
            args.mode.as_str(),
            started.elapsed().as_millis(),
            stats,
        );
    }

    if let Some(path) = args.ready_file.as_ref() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // The ready-file handshake is used by the attach-based profiling path so it
        // can wait for the warmed process to reach a deterministic pause point.
        fs::write(path, std::process::id().to_string())?;
        println!(
            "ready_for_attach pid={} preset={} mode={} warmup_iters={}",
            std::process::id(),
            args.preset.as_str(),
            args.mode.as_str(),
            args.warmup_iters,
        );
    }

    if args.pause_after_warmup_ms > 0 {
        // Holding the process here gives the profiler a clean boundary: samples
        // before the long gap are setup, and samples after it are steady-state.
        println!(
            "pausing_after_warmup pid={} wait_ms={}",
            std::process::id(),
            args.pause_after_warmup_ms,
        );
        thread::sleep(Duration::from_millis(args.pause_after_warmup_ms));
    }

    for iteration in 0..args.iters {
        let started = Instant::now();
        let stats = run_once(args.preset, args.mode)?;
        println!(
            "iteration={} preset={} mode={} elapsed_ms={} stats={:?}",
            iteration + 1,
            args.preset.as_str(),
            args.mode.as_str(),
            started.elapsed().as_millis(),
            stats,
        );
    }

    println!(
        "completed preset={} mode={} warmup_iters={} iters={} total_elapsed_ms={}",
        args.preset.as_str(),
        args.mode.as_str(),
        args.warmup_iters,
        args.iters,
        overall.elapsed().as_millis(),
    );

    Ok(())
}
