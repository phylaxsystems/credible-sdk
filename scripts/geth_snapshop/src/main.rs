use alloy::primitives::{
    B256,
    Bytes,
    U256,
};
use anyhow::{
    Context,
    Result,
    bail,
};
use clap::Parser;
use regex::Regex;
use serde::Deserialize;
use state_store::mdbx::StateWriter;
use std::{
    collections::HashMap,
    io::{
        BufRead,
        BufReader,
        Write,
    },
    process::{
        Command,
        Stdio,
    },
    thread,
};

use state_store::{
    AccountState,
    AddressHash,
    BlockStateUpdate,
    Writer,
    mdbx::common::CircularBufferConfig,
};

#[derive(Parser)]
#[command(name = "geth-dump")]
#[command(about = "Dump Geth state into Redis/MDBX")]
struct Args {
    /// Path to the Geth data directory
    #[arg(long)]
    datadir: String,

    /// Path to geth binary
    #[arg(long, default_value = "geth")]
    geth_bin: String,

    /// State dump backend: auto, snapshot, or trie
    #[arg(long, default_value = "auto")]
    geth_dump_backend: String,

    /// Block number to dump
    #[arg(long)]
    block_number: Option<u64>,

    /// Block hash to dump
    #[arg(long)]
    block_hash: Option<String>,

    /// Starting key for iteration
    #[arg(long)]
    start_key: Option<String>,

    /// Limit number of accounts
    #[arg(long)]
    limit: Option<usize>,

    /// MDBX path
    #[arg(long, default_value = "state")]
    mdbx_path: Option<String>,

    /// State worker buffer size
    #[arg(long, default_value = "3")]
    buffer_size: u8,

    /// JSON output file (use - for stdout)
    #[arg(long)]
    json_output: Option<String>,

    /// Verbose logging
    #[arg(long, short)]
    verbose: bool,
}

/// Storage value can be a simple string or an object with key/value fields
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StorageValue {
    Simple(String),
    Object {
        key: Option<String>,
        hash: Option<String>,
        value: Option<String>,
    },
}

/// Storage can be either a dict or a list
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StorageFormat {
    Dict(HashMap<String, StorageValue>),
    List(Vec<StorageValue>),
}

impl Default for StorageFormat {
    fn default() -> Self {
        StorageFormat::Dict(HashMap::new())
    }
}

/// Raw account from geth's JSON output
#[derive(Debug, Deserialize)]
struct GethAccount {
    key: String,
    balance: Option<String>,
    nonce: Option<u64>,
    #[serde(rename = "codeHash")]
    code_hash: Option<String>,
    code: Option<String>,
    #[serde(default)]
    storage: StorageFormat,
}

/// Root object (contains state root)
#[derive(Debug, Deserialize)]
struct GethRoot {
    root: Option<String>,
}

/// Metadata extracted from geth dump
#[derive(Default)]
struct DumpMetadata {
    block_number: Option<u64>,
    block_hash: Option<B256>,
    state_root: Option<B256>,
    total_accounts: usize,
}

/// Error from geth dump with stderr captured
struct GethDumpError {
    command: String,
    stderr: String,
    source: anyhow::Error,
}

fn parse_int(s: &str) -> Result<U256> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(U256::ZERO);
    }

    // Support both hex and decimal
    if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
        let hex_part = &trimmed[2..];
        if hex_part.is_empty() {
            return Ok(U256::ZERO);
        }
        U256::from_str_radix(hex_part, 16).context("Invalid U256 hex")
    } else {
        U256::from_str_radix(trimmed, 10).context("Invalid U256 decimal")
    }
}

fn parse_hex_b256(s: &str) -> Result<B256> {
    let trimmed = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    let padded = format!("{trimmed:0>64}");
    let bytes = hex::decode(&padded).context("Invalid B256 hex")?;
    Ok(B256::from_slice(&bytes))
}

fn parse_hex_bytes(s: &str) -> Result<Bytes> {
    let trimmed = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if trimmed.is_empty() {
        return Ok(Bytes::new());
    }
    let bytes = hex::decode(trimmed).context("Invalid bytes hex")?;
    Ok(Bytes::from(bytes))
}

fn parse_storage(raw: StorageFormat) -> Result<HashMap<B256, U256>> {
    let mut storage = HashMap::new();

    match raw {
        StorageFormat::Dict(dict) => {
            for (hashed_slot, value) in dict {
                let (slot_key, slot_value) = match value {
                    StorageValue::Simple(v) => (hashed_slot, v),
                    StorageValue::Object {
                        key,
                        hash: _,
                        value,
                    } => {
                        // Use "key" field if present, otherwise use the dict key
                        let k = key.unwrap_or(hashed_slot);
                        let v = value.unwrap_or_else(|| "0x0".to_string());
                        (k, v)
                    }
                };

                let slot = parse_hex_b256(&slot_key)?;
                let val = parse_int(&slot_value)?;
                // Don't filter zero values
                storage.insert(slot, val);
            }
        }
        StorageFormat::List(list) => {
            for entry in list {
                if let StorageValue::Object { key, hash, value } = entry {
                    // Use "key" or "hash" field
                    let slot_key = key.or(hash);
                    if let (Some(k), Some(v)) = (slot_key, value) {
                        let slot = parse_hex_b256(&k)?;
                        let val = parse_int(&v)?;
                        storage.insert(slot, val);
                    }
                }
            }
        }
    }

    Ok(storage)
}

fn parse_account(raw: GethAccount) -> Result<AccountState> {
    let address_hash = AddressHash::from_hash(parse_hex_b256(&raw.key)?);
    let balance = raw
        .balance
        .map(|b| parse_int(&b))
        .transpose()?
        .unwrap_or(U256::ZERO);
    let nonce = raw.nonce.unwrap_or(0);
    let code_hash = raw
        .code_hash
        .map(|h| parse_hex_b256(&h))
        .transpose()?
        .unwrap_or(B256::ZERO);
    let code = raw
        .code
        .filter(|c| c != "0x")
        .map(|c| parse_hex_bytes(&c))
        .transpose()?;
    let storage = parse_storage(raw.storage)?;

    Ok(AccountState {
        address_hash,
        balance,
        nonce,
        code_hash,
        code,
        storage,
        deleted: false,
    })
}

fn build_geth_command(args: &Args, mode: &str) -> Command {
    let mut cmd = Command::new(&args.geth_bin);

    if mode == "snapshot" {
        cmd.args(["snapshot", "dump"]);
    } else {
        cmd.args(["dump", "--iterative", "--incompletes"]);
    }

    cmd.args(["--datadir", &args.datadir]);

    if let Some(limit) = args.limit {
        cmd.args(["--limit", &limit.to_string()]);
    }
    if let Some(ref start_key) = args.start_key {
        cmd.args(["--start", start_key]);
    }
    if let Some(ref block_hash) = args.block_hash {
        cmd.arg(block_hash);
    } else if let Some(block_number) = args.block_number {
        cmd.arg(block_number.to_string());
    }

    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    cmd
}

/// Check if snapshot error is retryable
fn should_retry_snapshot(stderr: &str) -> bool {
    let lowered = stderr.to_lowercase();
    let retry_tokens = [
        "head doesn't match snapshot",
        "snapshot not found",
        "snapshot storage not ready",
        "loaded snapshot journal",
        "failed to load snapshot",
    ];
    retry_tokens.iter().any(|token| lowered.contains(token))
}

/// Check if trie error is retryable
fn should_retry_trie(stderr: &str) -> bool {
    let lowered = stderr.to_lowercase();
    lowered.contains("missing trie node") || lowered.contains("state is not available")
}

/// Extract block info from stderr
fn extract_block_info(stderr_lines: &[String]) -> DumpMetadata {
    let mut block_number: Option<u64> = None;
    let mut block_hash: Option<B256> = None;

    for line in stderr_lines {
        if line.contains("State dump configured") {
            for part in line.split_whitespace() {
                if let Some(val) = part.strip_prefix("block=") {
                    block_number = val.parse().ok();
                } else if let Some(val) = part.strip_prefix("hash=") {
                    block_hash = parse_hex_b256(val).ok();
                }
            }
        }
    }

    DumpMetadata {
        block_number,
        block_hash,
        state_root: None,
        total_accounts: 0,
    }
}

/// Parse snapshot head mismatch error
fn parse_snapshot_head_mismatch(stderr: &str) -> Option<(B256, B256)> {
    let re = Regex::new(r"(?i)head doesn't match snapshot: have (0x[a-f0-9]+), want (0x[a-f0-9]+)")
        .ok()?;

    let caps = re.captures(stderr)?;
    let have = parse_hex_b256(caps.get(1)?.as_str()).ok()?;
    let want = parse_hex_b256(caps.get(2)?.as_str()).ok()?;
    Some((have, want))
}

/// Parse missing trie node error
fn parse_missing_trie_node(stderr: &str) -> Option<B256> {
    // Try "missing trie node" pattern
    let re1 = Regex::new(r"(?i)missing trie node ([0-9a-fx]+)").ok()?;
    if let Some(caps) = re1.captures(stderr)
        && let Ok(hash) = parse_hex_b256(caps.get(1)?.as_str())
    {
        return Some(hash);
    }

    // Try "state X is not available" pattern
    let re2 = Regex::new(r"(?i)state (0x[a-f0-9]+) is not available").ok()?;
    if let Some(caps) = re2.captures(stderr)
        && let Ok(hash) = parse_hex_b256(caps.get(1)?.as_str())
    {
        return Some(hash);
    }

    None
}

/// Check for pruned state error and provide detailed message
fn maybe_pruned_state_error(
    errors: &[GethDumpError],
    block_argument: Option<&str>,
) -> Option<String> {
    let snapshot_error = errors.iter().find(|e| e.command == "snapshot dump")?;
    let trie_error = errors.iter().find(|e| e.command == "dump")?;

    let (head_root, requested_root) = parse_snapshot_head_mismatch(&snapshot_error.stderr)?;
    let missing_node = parse_missing_trie_node(&trie_error.stderr)?;

    if requested_root != missing_node {
        return None;
    }

    let block_label = block_argument.unwrap_or("the requested block");
    Some(format!(
        "Geth pruned the historical state needed for {block_label}: \
         snapshot data only exists for head root {head_root:x}, \
         while the requested block requires state root {requested_root:x} \
         and the trie backend reported missing node {missing_node:x}. \
         Re-sync the datadir with --gcmode=archive or select a block within the \
         snapshot horizon (typically HEAD-127 or newer).",
    ))
}

#[allow(clippy::too_many_lines)]
fn run_geth_dump<F>(
    args: &Args,
    mode: &str,
    on_account: &mut F,
) -> std::result::Result<DumpMetadata, GethDumpError>
where
    F: FnMut(AccountState) -> Result<()>,
{
    let mut cmd = build_geth_command(args, mode);

    if args.verbose {
        eprintln!("Running: {cmd:?}");
    }

    let mut child = cmd.spawn().map_err(|e| {
        GethDumpError {
            command: if mode == "snapshot" {
                "snapshot dump".to_string()
            } else {
                "dump".to_string()
            },
            stderr: String::new(),
            source: anyhow::anyhow!("Failed to spawn geth: {e}"),
        }
    })?;

    let stdout = child.stdout.take().ok_or_else(|| {
        GethDumpError {
            command: if mode == "snapshot" {
                "snapshot dump".to_string()
            } else {
                "dump".to_string()
            },
            stderr: String::new(),
            source: anyhow::anyhow!("No stdout"),
        }
    })?;

    // Spawn thread to read stderr
    let stderr = child.stderr.take();
    let stderr_handle = stderr.map(|s| {
        thread::spawn(move || {
            let reader = BufReader::new(s);
            reader
                .lines()
                .map_while(std::result::Result::ok)
                .collect::<Vec<_>>()
        })
    });

    let reader = BufReader::new(stdout);

    let mut metadata = DumpMetadata::default();

    for line in reader.lines() {
        let Ok(line) = line else {
            continue;
        };
        if line.is_empty() {
            continue;
        }

        // Try parsing as root object first
        if let Ok(root) = serde_json::from_str::<GethRoot>(&line) {
            if let Some(ref r) = root.root
                && let Ok(state_root) = parse_hex_b256(r)
            {
                metadata.state_root = Some(state_root);
            }
            continue;
        }

        // Try parsing as account
        let raw: GethAccount = match serde_json::from_str(&line) {
            Ok(a) => a,
            Err(_) => continue,
        };

        let Ok(account) = parse_account(raw) else {
            continue;
        };

        // Stream account to callback immediately - memory is released after this
        if let Err(e) = on_account(account) {
            return Err(GethDumpError {
                command: if mode == "snapshot" {
                    "snapshot dump".to_string()
                } else {
                    "dump".to_string()
                },
                stderr: String::new(),
                source: e,
            });
        }

        metadata.total_accounts += 1;

        if args.verbose && metadata.total_accounts % 10000 == 0 {
            eprintln!("Processed {} accounts...", metadata.total_accounts);
        }
    }

    // Collect stderr
    let stderr_lines = stderr_handle
        .map(|h| h.join().unwrap_or_default())
        .unwrap_or_default();
    let stderr_text = stderr_lines.join("\n");

    let status = child.wait().map_err(|e| {
        GethDumpError {
            command: if mode == "snapshot" {
                "snapshot dump".to_string()
            } else {
                "dump".to_string()
            },
            stderr: stderr_text.clone(),
            source: anyhow::anyhow!("Failed to wait for geth: {e}"),
        }
    })?;

    if !status.success() {
        return Err(GethDumpError {
            command: if mode == "snapshot" {
                "snapshot dump".to_string()
            } else {
                "dump".to_string()
            },
            stderr: stderr_text,
            source: anyhow::anyhow!("geth {mode} dump failed with status: {status}"),
        });
    }

    // Extract block info from stderr
    let info = extract_block_info(&stderr_lines);
    if metadata.block_number.is_none() {
        metadata.block_number = info.block_number;
    }
    if metadata.block_hash.is_none() {
        metadata.block_hash = info.block_hash;
    }

    Ok(metadata)
}

fn run_with_fallback<F>(args: &Args, on_account: &mut F) -> Result<DumpMetadata>
where
    F: FnMut(AccountState) -> Result<()>,
{
    let backends = match args.geth_dump_backend.as_str() {
        "snapshot" => vec!["snapshot"],
        "trie" => vec!["trie"],
        _ => vec!["snapshot", "trie"],
    };

    let mut errors: Vec<GethDumpError> = Vec::new();
    let block_argument = args
        .block_hash
        .as_deref()
        .or_else(|| args.block_number.map(|_| "block_number"));

    for mode in &backends {
        if args.verbose {
            eprintln!("Trying {mode} backend...");
        }

        match run_geth_dump(args, mode, on_account) {
            Ok(metadata) => return Ok(metadata),
            Err(e) => {
                if args.verbose {
                    eprintln!("{mode} backend failed: {}", e.source);
                }

                // Check if we should retry based on specific errors
                let should_continue = if args.geth_dump_backend == "auto" {
                    if *mode == "snapshot" && should_retry_snapshot(&e.stderr) {
                        if args.verbose {
                            eprintln!("Snapshot backend unavailable, falling back to trie dump.");
                        }
                        true
                    } else if *mode == "trie" && should_retry_trie(&e.stderr) {
                        if args.verbose {
                            eprintln!(
                                "Trie backend missing node data, falling back to snapshot dump."
                            );
                        }
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                errors.push(e);

                if !should_continue && args.geth_dump_backend != "auto" {
                    break;
                }
                if !should_continue && args.geth_dump_backend == "auto" {
                    // Non-retryable error in auto mode, bail out
                    let last_err = errors.pop().unwrap();
                    bail!("{}", last_err.source);
                }
            }
        }
    }

    // Check for pruned state error with detailed message
    if let Some(pruned_msg) = maybe_pruned_state_error(&errors, block_argument) {
        bail!("{pruned_msg}");
    }

    // Generic error with all failures
    let error_messages: Vec<String> = errors
        .iter()
        .map(|e| format!("{}: {}", e.command, e.source))
        .collect();
    bail!(
        "All geth dump attempts failed:\n{}",
        error_messages.join("\n---\n")
    )
}

fn main() -> Result<()> {
    const BATCH_SIZE: usize = 10000;

    let args = Args::parse();

    let block_number = args.block_number.unwrap_or(0);

    // Setup MDBX writer if path provided
    let writer = if let Some(ref path) = args.mdbx_path {
        let w = StateWriter::new(path, CircularBufferConfig::new(args.buffer_size)?)?;
        w.ensure_dump_index_metadata()?;
        Some(w)
    } else {
        None
    };

    // Setup JSON output if path provided
    let mut json_writer: Option<std::io::BufWriter<Box<dyn Write>>> =
        if let Some(ref path) = args.json_output {
            let output: Box<dyn Write> = if path == "-" {
                Box::new(std::io::stdout())
            } else {
                Box::new(std::fs::File::create(path)?)
            };
            Some(std::io::BufWriter::new(output))
        } else {
            None
        };

    // For MDBX, we accumulate into a BlockStateUpdate but commit in batches
    // to avoid OOM while still using the existing API.
    //
    // NOTE: This assumes StateWriter.commit_block() supports being called multiple
    // times and APPENDS/MERGES data rather than replacing. If the current API
    // clears and replaces on each call, you'll need to modify StateWriter to add:
    //   - begin_block(block_number) -> clears namespace
    //   - write_account(account) -> writes single account
    //   - finalize_block(metadata) -> writes block metadata
    let mut update = BlockStateUpdate::new(block_number, B256::ZERO, B256::ZERO);
    let mut accounts_written: usize = 0;
    let mut storage_slots_written: usize = 0;

    // Callback that streams accounts to sinks
    let mut on_account = |account: AccountState| -> Result<()> {
        // Write to JSON immediately
        if let Some(ref mut jw) = json_writer {
            serde_json::to_writer(&mut *jw, &account)?;
            jw.write_all(b"\n")?;
        }

        // Accumulate for MDBX in batches
        if writer.is_some() {
            storage_slots_written += account.storage.len();
            update.merge_account_state(account);
            accounts_written += 1;

            // Commit batch to avoid OOM
            if update.accounts.len() >= BATCH_SIZE {
                if let Some(ref w) = writer {
                    w.commit_block(&update)?;
                }
                // Clear for next batch (keep block info)
                update = BlockStateUpdate::new(block_number, B256::ZERO, B256::ZERO);
            }
        }

        Ok(())
    };

    // Run dump with streaming callback
    let metadata = run_with_fallback(&args, &mut on_account)?;

    // Get final block info
    let final_block_number = args.block_number.or(metadata.block_number).unwrap_or(0);
    let final_block_hash = metadata.block_hash.unwrap_or(B256::ZERO);
    let final_state_root = metadata.state_root.unwrap_or(B256::ZERO);

    // Commit any remaining accounts in the last batch
    if let Some(ref w) = writer {
        if !update.accounts.is_empty() {
            w.commit_block(&update)?;
        }

        // Write final block metadata
        let final_update =
            BlockStateUpdate::new(final_block_number, final_block_hash, final_state_root);
        w.commit_block(&final_update)?;

        if args.verbose {
            eprintln!(
                "MDBX: wrote {accounts_written} accounts, {storage_slots_written} storage slots",
            );
        }
    }

    eprintln!(
        "Synced {} accounts for block {} (state_root={:x})",
        metadata.total_accounts, final_block_number, final_state_root
    );

    Ok(())
}
