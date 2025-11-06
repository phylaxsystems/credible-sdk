use std::{
    fmt,
    str::FromStr,
    time::{
        Duration,
        Instant,
    },
};

use alloy::{
    primitives::{
        Address,
        B256,
        StorageKey,
        utils::KECCAK256_EMPTY,
    },
    providers::{
        Provider,
        ProviderBuilder,
        RpcWithBlock,
    },
    rpc::json_rpc::{
        RpcRecv,
        RpcSend,
    },
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use clap::{
    Args,
    Parser,
    Subcommand,
};
use serde_json::to_string_pretty;

#[derive(Debug, Parser)]
#[command(
    name = "account-proof-poc",
    about = "Query an account's state proof via eth_getProof or benchmark RPC performance"
)]
struct Cli {
    /// HTTP RPC endpoint to query. Defaults to RPC_URL env var or localhost.
    #[arg(long, env = "RPC_URL", default_value = "http://localhost:8545")]
    rpc_url: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Fetch and print the account proof to stdout.
    Proof(ProofArgs),
    /// Benchmark eth_getProof versus eth_getCode performance.
    Bench(BenchArgs),
}

#[derive(Debug, Args)]
struct ProofArgs {
    /// Address to query.
    #[arg(long, value_parser = parse_address)]
    address: Address,
    /// Optional block selector: latest, pending, safe, finalized, earliest, or a block number.
    #[arg(long, value_parser = parse_block_arg)]
    block: Option<BlockArg>,
    /// Storage slots (as 0x-prefixed 32-byte values) to include in the proof.
    #[arg(long = "slot", value_parser = parse_storage_key, value_delimiter = ',', num_args = 0..)]
    slots: Vec<StorageKey>,
}

#[derive(Debug, Args)]
struct BenchArgs {
    /// Address to query.
    #[arg(long, value_parser = parse_address)]
    address: Address,
    /// Number of iterations to benchmark.
    #[arg(long, default_value_t = 100, value_parser = parse_iterations)]
    iterations: u32,
    /// Optional block selector for the RPC calls.
    #[arg(long, value_parser = parse_block_arg)]
    block: Option<BlockArg>,
    /// Storage slots (as 0x-prefixed 32-byte values) to include in the proof.
    #[arg(long = "slot", value_parser = parse_storage_key, value_delimiter = ',', num_args = 0..)]
    slots: Vec<StorageKey>,
}

#[derive(Debug, Clone, Copy)]
enum BlockArg {
    Latest,
    Pending,
    Finalized,
    Safe,
    Earliest,
    Number(u64),
}

#[tokio::main]
async fn main() -> Result<()> {
    let Cli { rpc_url, command } = Cli::parse();

    let url = reqwest::Url::parse(&rpc_url).context("invalid RPC URL")?;
    let provider = ProviderBuilder::new().connect_http(url);

    match command {
        Command::Proof(args) => run_proof(provider, args).await?,
        Command::Bench(args) => run_bench(provider, args).await?,
    }

    Ok(())
}

async fn run_proof<P>(provider: P, args: ProofArgs) -> Result<()>
where
    P: Provider,
{
    let ProofArgs {
        address,
        block,
        slots,
    } = args;

    let request = apply_block(provider.get_proof(address, slots), block);
    let proof = request.await.context("failed to fetch proof")?;

    let pretty = to_string_pretty(&proof).context("failed to encode proof as JSON")?;
    println!("{pretty}");

    Ok(())
}

async fn run_bench<P>(provider: P, args: BenchArgs) -> Result<()>
where
    P: Provider + Clone,
{
    let BenchArgs {
        address,
        iterations,
        block,
        slots,
    } = args;

    let provider_basic = provider.clone();
    let mut basic_durations = Vec::with_capacity(iterations as usize);
    for i in 0..iterations {
        let start = Instant::now();
        let balance_fut = apply_block(provider_basic.get_balance(address), block);
        let nonce_fut = apply_block(provider_basic.get_transaction_count(address), block);
        let code_fut = apply_block(provider_basic.get_code_at(address), block);

        let (_balance, _nonce, _code) = tokio::try_join!(
            async {
                balance_fut
                    .await
                    .map_err(|err| anyhow!("eth_getBalance failed at iteration {i}: {err}"))
            },
            async {
                nonce_fut.await.map_err(|err| {
                    anyhow!("eth_getTransactionCount failed at iteration {i}: {err}")
                })
            },
            async {
                code_fut
                    .await
                    .map_err(|err| anyhow!("eth_getCode failed at iteration {i}: {err}"))
            }
        )?;

        basic_durations.push(start.elapsed());
    }

    let provider_proof_only = provider.clone();
    let mut proof_durations = Vec::with_capacity(iterations as usize);
    for i in 0..iterations {
        let start = Instant::now();
        apply_block(provider_proof_only.get_proof(address, slots.clone()), block)
            .await
            .with_context(|| format!("eth_getProof failed at iteration {i}"))?;
        proof_durations.push(start.elapsed());
    }

    let provider_proof_code = provider;
    let mut proof_with_code_durations = Vec::with_capacity(iterations as usize);
    let mut proof_code_fetches = 0u32;
    for i in 0..iterations {
        let start = Instant::now();
        let proof = apply_block(provider_proof_code.get_proof(address, slots.clone()), block)
            .await
            .with_context(|| format!("eth_getProof failed at iteration {i}"))?;

        if proof.code_hash != KECCAK256_EMPTY {
            apply_block(provider_proof_code.get_code_at(address), block)
                .await
                .with_context(|| format!("eth_getCode (post-proof) failed at iteration {i}"))?;
            proof_code_fetches += 1;
        }

        proof_with_code_durations.push(start.elapsed());
    }

    let basic_stats = Stats::from(&basic_durations);
    let proof_stats = Stats::from(&proof_durations);
    let proof_with_code_stats = Stats::from(&proof_with_code_durations);

    println!(
        "Benchmark over {iterations} iterations (block: {}):",
        block.map_or_else(|| "latest".to_owned(), |b| b.to_string())
    );
    println!("  basic_ref (balance+nonce+code)      -> {basic_stats}");
    println!("  eth_getProof                        -> {proof_stats}");
    println!(
        "  eth_getProof + conditional getCode  -> {proof_with_code_stats} (code fetches: {proof_code_fetches}/{iterations})"
    );

    println!("Ratios vs basic_ref:");
    println!(
        "  eth_getProof                       : {:.3}",
        ratio(&proof_stats, &basic_stats)
    );
    println!(
        "  eth_getProof + conditional getCode : {:.3}",
        ratio(&proof_with_code_stats, &basic_stats)
    );

    Ok(())
}

fn apply_block<Params, Resp, Output, Map>(
    call: RpcWithBlock<Params, Resp, Output, Map>,
    block: Option<BlockArg>,
) -> RpcWithBlock<Params, Resp, Output, Map>
where
    Params: RpcSend,
    Resp: RpcRecv,
    Map: Fn(Resp) -> Output + Clone,
{
    match block.unwrap_or(BlockArg::Latest) {
        BlockArg::Latest => call.latest(),
        BlockArg::Pending => call.pending(),
        BlockArg::Finalized => call.finalized(),
        BlockArg::Safe => call.safe(),
        BlockArg::Earliest => call.earliest(),
        BlockArg::Number(number) => call.number(number),
    }
}

fn parse_address(value: &str) -> Result<Address, String> {
    Address::from_str(value).map_err(|err| format!("invalid address `{value}`: {err}"))
}

fn parse_storage_key(value: &str) -> Result<StorageKey, String> {
    B256::from_str(value)
        .map(Into::into)
        .map_err(|err| format!("invalid storage slot `{value}`: {err}"))
}

fn parse_block_arg(value: &str) -> Result<BlockArg, String> {
    match value {
        "latest" => Ok(BlockArg::Latest),
        "pending" => Ok(BlockArg::Pending),
        "safe" => Ok(BlockArg::Safe),
        "finalized" => Ok(BlockArg::Finalized),
        "earliest" => Ok(BlockArg::Earliest),
        other => parse_block_number(other),
    }
}

fn parse_block_number(value: &str) -> Result<BlockArg, String> {
    if let Some(number) = value.strip_prefix("0x") {
        u64::from_str_radix(number, 16)
            .map(BlockArg::Number)
            .map_err(|err| format!("invalid hex block number `{value}`: {err}"))
    } else {
        value
            .parse::<u64>()
            .map(BlockArg::Number)
            .map_err(|err| format!("invalid block number `{value}`: {err}"))
    }
}

fn parse_iterations(value: &str) -> Result<u32, String> {
    let parsed: u32 = value
        .parse()
        .map_err(|err| format!("invalid iteration count `{value}`: {err}"))?;
    if parsed == 0 {
        return Err("iteration count must be greater than zero".to_owned());
    }
    Ok(parsed)
}

#[derive(Clone)]
struct Stats {
    min_ms: f64,
    max_ms: f64,
    avg_ms: f64,
    median_ms: f64,
}

impl Stats {
    fn from(values: &[Duration]) -> Self {
        let mut sorted = values.to_vec();
        sorted.sort();

        let len = sorted.len();
        let min = sorted.first().cloned().unwrap_or_default();
        let max = sorted.last().cloned().unwrap_or_default();
        let sum: f64 = values.iter().map(|d| d.as_secs_f64()).sum();
        let avg = if len == 0 { 0.0 } else { sum / len as f64 };

        let median = if len == 0 {
            Duration::default()
        } else if len % 2 == 1 {
            sorted[len / 2]
        } else {
            let lower = sorted[(len / 2) - 1].as_secs_f64();
            let upper = sorted[len / 2].as_secs_f64();
            Duration::from_secs_f64((lower + upper) / 2.0)
        };

        Self {
            min_ms: to_ms(min),
            max_ms: to_ms(max),
            avg_ms: avg * 1_000.0,
            median_ms: to_ms(median),
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "avg: {avg:.3} ms, median: {median:.3} ms, min: {min:.3} ms, max: {max:.3} ms",
            avg = self.avg_ms,
            median = self.median_ms,
            min = self.min_ms,
            max = self.max_ms
        )
    }
}

fn to_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn ratio(numerator: &Stats, denominator: &Stats) -> f64 {
    if denominator.avg_ms == 0.0 {
        f64::INFINITY
    } else {
        numerator.avg_ms / denominator.avg_ms
    }
}

impl fmt::Display for BlockArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockArg::Latest => write!(f, "latest"),
            BlockArg::Pending => write!(f, "pending"),
            BlockArg::Finalized => write!(f, "finalized"),
            BlockArg::Safe => write!(f, "safe"),
            BlockArg::Earliest => write!(f, "earliest"),
            BlockArg::Number(number) => write!(f, "{number}"),
        }
    }
}
