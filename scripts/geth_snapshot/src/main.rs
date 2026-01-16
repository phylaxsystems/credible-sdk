use alloy::{
    primitives::{
        B256,
        Bytes,
        KECCAK256_EMPTY,
        U256,
        keccak256,
    },
    rlp::Decodable,
};
use anyhow::{
    Context,
    Result,
    bail,
};
use clap::Parser;
use regex::Regex;
use serde::Deserialize;
use state_store::{
    AccountState,
    AddressHash,
    Reader,
    mdbx::{
        StateWriter,
        common::CircularBufferConfig,
    },
};
use std::{
    collections::HashMap,
    fs::File,
    io::{
        BufRead,
        BufReader,
        Write,
    },
    path::Path,
    process::{
        Command,
        Stdio,
    },
    sync::mpsc::{
        SyncSender,
        sync_channel,
    },
    thread,
};

#[derive(Parser)]
#[command(name = "geth-dump")]
#[command(about = "Dump Geth state into Redis/MDBX")]
struct Args {
    /// Path to the Geth data directory (required if --json not provided)
    #[arg(long)]
    datadir: Option<String>,

    /// Path to geth binary
    #[arg(long, default_value = "geth")]
    geth_bin: String,

    /// State dump backend: auto, snapshot, or trie
    #[arg(long, default_value = "auto")]
    geth_dump_backend: String,

    /// Block number to dump
    #[arg(long)]
    block_number: u64,

    /// Block hash to dump
    #[arg(long)]
    block_hash: String,

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

    /// JSON input file - pre-dumped geth snapshot (use - for stdin)
    /// If provided, --datadir is not required
    #[arg(long)]
    json: Option<String>,

    /// Verbose logging
    #[arg(long, short)]
    verbose: bool,

    /// Fix metadata only - update block number on existing database without re-hydrating
    #[arg(long)]
    fix_metadata: bool,

    /// State root (hex) - used with --fix-metadata
    #[arg(long)]
    state_root: Option<String>,
}

impl Args {
    /// Validate that either --json or --datadir is provided
    fn validate(&self) -> Result<()> {
        match (&self.json, &self.datadir) {
            (None, None) => {
                bail!("Either --json or --datadir must be provided");
            }
            (Some(_), Some(_)) => {
                bail!("Cannot use both --json and --datadir. Choose one input source.");
            }
            _ => Ok(()),
        }
    }

    /// Check if we're reading from a JSON file
    fn is_json_input(&self) -> bool {
        self.json.is_some()
    }
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
#[serde(deny_unknown_fields)]
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

/// Parse an integer value (for balance, nonce, etc.) - NOT RLP encoded
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
        if hex_part.len() > 64 {
            bail!(
                "Hex value too large for U256: {}",
                &trimmed[..trimmed.len().min(80)]
            );
        }
        U256::from_str_radix(hex_part, 16)
            .with_context(|| format!("Invalid U256 hex: {}", &trimmed[..trimmed.len().min(80)]))
    } else {
        U256::from_str_radix(trimmed, 10).with_context(|| {
            format!(
                "Invalid U256 decimal: {}",
                &trimmed[..trimmed.len().min(80)]
            )
        })
    }
}

/// Parse a storage value - these are RLP encoded in geth snapshot dumps
fn parse_storage_value(s: &str) -> Result<U256> {
    let trimmed = s.trim().trim_start_matches("0x").trim_start_matches("0X");

    if trimmed.is_empty() {
        return Ok(U256::ZERO);
    }

    let bytes = hex::decode(trimmed).with_context(|| {
        format!(
            "Invalid hex in storage value: {}",
            &trimmed[..trimmed.len().min(80)]
        )
    })?;

    if bytes.is_empty() {
        return Ok(U256::ZERO);
    }

    // Use alloy's proper RLP decoding
    let decoded: Bytes = Bytes::decode(&mut bytes.as_slice()).with_context(|| {
        format!(
            "Failed to RLP-decode storage value: {}",
            &trimmed[..trimmed.len().min(80)]
        )
    })?;

    if decoded.is_empty() {
        return Ok(U256::ZERO);
    }

    if decoded.len() > 32 {
        bail!(
            "Decoded storage value too large for U256 (got {} bytes, max 32): {}",
            decoded.len(),
            hex::encode(&decoded[..decoded.len().min(40)])
        );
    }

    Ok(U256::from_be_slice(&decoded))
}

fn parse_hex_b256(s: &str) -> Result<B256> {
    let trimmed = s.trim().trim_start_matches("0x").trim_start_matches("0X");

    // Warn if the value is shorter than expected - this could indicate data corruption
    if !trimmed.is_empty() && trimmed.len() < 64 {
        eprintln!(
            "Warning: B256 value shorter than 64 hex chars (got {}), left-padding with zeros: {}",
            trimmed.len(),
            s
        );
    }

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

fn slot_hash_from_preimage(key: &str) -> Result<B256> {
    let preimage = parse_hex_b256(key)
        .with_context(|| format!("Invalid storage slot key: {}", &key[..key.len().min(80)]))?;
    Ok(keccak256(preimage))
}

fn parse_storage(raw: StorageFormat) -> Result<HashMap<B256, U256>> {
    let mut storage = HashMap::new();

    match raw {
        StorageFormat::Dict(dict) => {
            for (hashed_slot, value) in dict {
                let (slot_key, slot_value, slot_is_preimage) = match value {
                    StorageValue::Simple(v) => (hashed_slot, v, false),
                    StorageValue::Object {
                        key,
                        hash,
                        value,
                    } => {
                        // Prefer "hash" if present; otherwise treat "key" as preimage.
                        let has_hash = hash.is_some();
                        let has_key = key.is_some();
                        let k = hash.or(key).unwrap_or(hashed_slot);
                        let v = value.unwrap_or_else(|| "0x0".to_string());
                        let is_preimage = !has_hash && has_key;
                        (k, v, is_preimage)
                    }
                };

                let slot = if slot_is_preimage {
                    slot_hash_from_preimage(&slot_key)?
                } else {
                    parse_hex_b256(&slot_key).with_context(|| {
                        format!(
                            "Invalid storage slot key: {}",
                            &slot_key[..slot_key.len().min(80)]
                        )
                    })?
                };
                // Storage values are RLP-encoded in geth snapshot dumps
                let val = parse_storage_value(&slot_value).with_context(|| {
                    format!(
                        "Invalid storage value for slot {}: {}",
                        &slot_key[..slot_key.len().min(40)],
                        &slot_value[..slot_value.len().min(80)]
                    )
                })?;
                // Don't filter zero values
                storage.insert(slot, val);
            }
        }
        StorageFormat::List(list) => {
            for entry in list {
                if let StorageValue::Object { key, hash, value } = entry {
                    // Prefer "hash" if present; otherwise treat "key" as preimage.
                    let has_hash = hash.is_some();
                    let has_key = key.is_some();
                    let slot_key = hash.or(key);
                    if let (Some(k), Some(v)) = (slot_key, value) {
                        let slot = if !has_hash && has_key {
                            slot_hash_from_preimage(&k)?
                        } else {
                            parse_hex_b256(&k).with_context(|| {
                                format!("Invalid storage slot key: {}", &k[..k.len().min(80)])
                            })?
                        };
                        // Storage values are RLP-encoded in geth snapshot dumps
                        let val = parse_storage_value(&v).with_context(|| {
                            format!(
                                "Invalid storage value for slot {}: {}",
                                &k[..k.len().min(40)],
                                &v[..v.len().min(80)]
                            )
                        })?;
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
        .unwrap_or(KECCAK256_EMPTY);
    let code = raw
        .code
        .filter(|c| c != "0x")
        .map(|c| parse_hex_bytes(&c))
        .transpose()?;
    let storage = parse_storage(raw.storage)?;

    // Validate code hash if code is present
    if let Some(ref code_bytes) = code {
        let computed_hash = keccak256(code_bytes);
        if computed_hash != code_hash {
            bail!(
                "Code hash mismatch for account {address_hash:?}: expected {code_hash:x}, computed {computed_hash:x}",
            );
        }
    } else {
        // If no code, the code_hash should be either zero or the empty code hash
        if code_hash != B256::ZERO && code_hash != KECCAK256_EMPTY {
            bail!("Account {address_hash:?} has no code but non-empty code hash: {code_hash:x}",);
        }
    }

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

    // Safe to unwrap because we validated datadir exists when not using --json
    cmd.args(["--datadir", args.datadir.as_ref().unwrap()]);

    if let Some(limit) = args.limit {
        cmd.args(["--limit", &limit.to_string()]);
    }
    if let Some(ref start_key) = args.start_key {
        cmd.args(["--start", start_key]);
    }

    cmd.arg(args.block_hash.clone());
    cmd.arg(args.block_number.to_string());

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
fn maybe_pruned_state_error(errors: &[GethDumpError], block_hash: &str) -> Option<String> {
    let snapshot_error = errors.iter().find(|e| e.command == "snapshot dump")?;
    let trie_error = errors.iter().find(|e| e.command == "dump")?;

    let (head_root, requested_root) = parse_snapshot_head_mismatch(&snapshot_error.stderr)?;
    let missing_node = parse_missing_trie_node(&trie_error.stderr)?;

    if requested_root != missing_node {
        return None;
    }

    Some(format!(
        "Geth pruned the historical state needed for {block_hash}: \
         snapshot data only exists for head root {head_root:x}, \
         while the requested block requires state root {requested_root:x} \
         and the trie backend reported missing node {missing_node:x}. \
         Re-sync the datadir with --gcmode=archive or select a block within the \
         snapshot horizon (typically HEAD-127 or newer).",
    ))
}

/// Result from the parser thread
struct ParserResult {
    metadata: DumpMetadata,
}

/// Message sent from parser to writer thread
enum ParsedItem {
    Account(AccountState),
    StateRoot(B256),
}

/// Parse a single line and return the parsed item
fn parse_line(line: &str, line_number: usize) -> Result<Option<ParsedItem>> {
    if line.is_empty() {
        return Ok(None);
    }

    // Try parsing as root object first (only matches lines with ONLY "root" field)
    if let Ok(root) = serde_json::from_str::<GethRoot>(line) {
        if let Some(ref r) = root.root
            && let Ok(state_root) = parse_hex_b256(r)
        {
            return Ok(Some(ParsedItem::StateRoot(state_root)));
        }
        return Ok(None);
    }

    // Try parsing as account - FAIL HARD on parse errors, we need 1:1 state
    let raw: GethAccount = serde_json::from_str(line).with_context(|| {
        format!(
            "FATAL: Failed to parse account JSON at line {line_number}. State integrity compromised!\nLine content: {}",
            if line.len() > 200 { &line[..200] } else { line }
        )
    })?;

    let account = parse_account(raw).with_context(|| {
        format!(
            "FATAL: Failed to parse account data at line {line_number}. State integrity compromised!\nLine content: {}",
            if line.len() > 200 { &line[..200] } else { line }
        )
    })?;

    Ok(Some(ParsedItem::Account(account)))
}

/// Run the parser in a separate thread, sending parsed accounts through a channel
fn run_parser_thread(
    json_path: &str,
    tx: &SyncSender<ParsedItem>,
    verbose: bool,
) -> Result<ParserResult> {
    let mut metadata = DumpMetadata::default();
    let mut line_number: usize = 0;

    if json_path == "-" {
        // Read from stdin
        if verbose {
            eprintln!("Reading JSON from stdin...");
        }
        let stdin = std::io::stdin();
        let reader = BufReader::with_capacity(256 * 1024, stdin.lock());

        for line in reader.lines() {
            line_number += 1;
            let line =
                line.with_context(|| format!("Failed to read line {line_number} from JSON input"))?;

            if let Some(item) = parse_line(&line, line_number)? {
                match item {
                    ParsedItem::StateRoot(root) => {
                        metadata.state_root = Some(root);
                        if verbose {
                            eprintln!("Line {line_number}: Parsed state root: {root:x}");
                        }
                    }
                    ParsedItem::Account(_) => {
                        metadata.total_accounts += 1;
                        if tx.send(item).is_err() {
                            // Receiver dropped, stop parsing
                            break;
                        }
                    }
                }
            }
        }
    } else {
        // Read from file
        if verbose {
            eprintln!("Reading JSON from file: {json_path}");
        }

        if !Path::new(&json_path).exists() {
            bail!("JSON input file not found: {json_path}");
        }

        let file = File::open(json_path)
            .with_context(|| format!("Failed to open JSON file: {json_path}"))?;
        let reader = BufReader::with_capacity(256 * 1024, file);

        for line in reader.lines() {
            line_number += 1;
            let line =
                line.with_context(|| format!("Failed to read line {line_number} from JSON input"))?;

            if let Some(item) = parse_line(&line, line_number)? {
                match item {
                    ParsedItem::StateRoot(root) => {
                        metadata.state_root = Some(root);
                        if verbose {
                            eprintln!("Line {line_number}: Parsed state root: {root:x}");
                        }
                    }
                    ParsedItem::Account(_) => {
                        metadata.total_accounts += 1;
                        if tx.send(item).is_err() {
                            // Receiver dropped, stop parsing
                            break;
                        }
                    }
                }
            }

            if verbose && metadata.total_accounts % 10000 == 0 && metadata.total_accounts > 0 {
                eprintln!("Parsed {} accounts...", metadata.total_accounts);
            }
        }
    }

    Ok(ParserResult { metadata })
}

/// Run dump from JSON file input with parallel parsing and writing
fn run_json_dump<F>(args: &Args, on_account: &mut F) -> Result<DumpMetadata>
where
    F: FnMut(AccountState) -> Result<()>,
{
    let json_path = args.json.as_ref().unwrap().clone();
    let verbose = args.verbose;

    // Create a bounded channel to buffer parsed accounts
    // This allows the parser to run ahead of the writer
    let (tx, rx) = sync_channel::<ParsedItem>(1000);

    // Spawn parser thread
    let parser_handle = thread::spawn(move || run_parser_thread(&json_path, &tx, verbose));

    // Process accounts in main thread as they arrive
    let mut state_root: Option<B256> = None;
    let mut accounts_processed: usize = 0;

    for item in rx {
        match item {
            ParsedItem::Account(account) => {
                on_account(account)?;
                accounts_processed += 1;

                if args.verbose && accounts_processed.is_multiple_of(10000) {
                    eprintln!("Written {accounts_processed} accounts...");
                }
            }
            ParsedItem::StateRoot(root) => {
                state_root = Some(root);
            }
        }
    }

    // Wait for parser thread and get metadata
    let parser_result = parser_handle
        .join()
        .map_err(|_| anyhow::anyhow!("Parser thread panicked"))??;

    let mut metadata = parser_result.metadata;
    // Use state root from channel if we received it
    if state_root.is_some() {
        metadata.state_root = state_root;
    }

    Ok(metadata)
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

    let reader = BufReader::with_capacity(256 * 1024, stdout);

    let mut metadata = DumpMetadata::default();
    let mut line_number: usize = 0;

    for line in reader.lines() {
        line_number += 1;
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                return Err(GethDumpError {
                    command: if mode == "snapshot" {
                        "snapshot dump".to_string()
                    } else {
                        "dump".to_string()
                    },
                    stderr: String::new(),
                    source: anyhow::anyhow!("Failed to read line {line_number}: {e}"),
                });
            }
        };

        if line.is_empty() {
            continue;
        }

        // Try parsing as root object first (only matches lines with ONLY "root" field)
        if let Ok(root) = serde_json::from_str::<GethRoot>(&line) {
            if let Some(ref r) = root.root
                && let Ok(state_root) = parse_hex_b256(r)
            {
                metadata.state_root = Some(state_root);
                if args.verbose {
                    eprintln!("Line {line_number}: Parsed state root: {state_root:x}");
                }
            }
            continue;
        }

        // Try parsing as account - FAIL HARD on parse errors, we need 1:1 state
        let raw: GethAccount = match serde_json::from_str(&line) {
            Ok(a) => a,
            Err(e) => {
                return Err(GethDumpError {
                    command: if mode == "snapshot" {
                        "snapshot dump".to_string()
                    } else {
                        "dump".to_string()
                    },
                    stderr: String::new(),
                    source: anyhow::anyhow!(
                        "FATAL: Failed to parse account JSON at line {}. State integrity compromised!\nError: {}\nLine content: {}",
                        line_number,
                        e,
                        if line.len() > 200 {
                            &line[..200]
                        } else {
                            &line
                        }
                    ),
                });
            }
        };

        let account = match parse_account(raw) {
            Ok(a) => a,
            Err(e) => {
                return Err(GethDumpError {
                    command: if mode == "snapshot" {
                        "snapshot dump".to_string()
                    } else {
                        "dump".to_string()
                    },
                    stderr: String::new(),
                    source: anyhow::anyhow!(
                        "FATAL: Failed to parse account data at line {}. State integrity compromised!\nError: {}\nLine content: {}",
                        line_number,
                        e,
                        if line.len() > 200 {
                            &line[..200]
                        } else {
                            &line
                        }
                    ),
                });
            }
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
    if let Some(pruned_msg) = maybe_pruned_state_error(&errors, &args.block_hash) {
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

/// Fix metadata on an existing MDBX database without re-hydrating
fn fix_metadata(args: &Args) -> Result<()> {
    let mdbx_path = args.mdbx_path.as_ref().context("--mdbx-path is required")?;
    let new_block_number = args.block_number;

    let block_hash = parse_hex_b256(&args.block_hash).context("Invalid block hash")?;

    let state_root = args
        .state_root
        .as_ref()
        .map(|r| parse_hex_b256(r))
        .transpose()
        .context("Invalid state root")?;

    eprintln!("Opening database at: {mdbx_path}");

    // Open writer with same buffer size (it will read actual buffer size from db)
    let writer = StateWriter::new(mdbx_path, CircularBufferConfig::new(args.buffer_size)?)
        .context("Failed to open MDBX database")?;

    // Show current state
    let current_block = writer.latest_block_number()?.unwrap_or(0);
    eprintln!("Current latest_block: {current_block}");

    if let Some(meta) = writer.get_block_metadata(current_block)? {
        eprintln!("Current block_hash: {:?}", meta.block_hash);
        eprintln!("Current state_root: {:?}", meta.state_root);
    }

    // Fix metadata
    writer
        .fix_block_metadata(new_block_number, block_hash, state_root)
        .context("Failed to fix metadata")?;

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Validate args
    args.validate()?;

    // Handle --fix-metadata mode
    if args.fix_metadata {
        return fix_metadata(&args);
    }

    // Setup MDBX writer if path provided
    let writer = if let Some(ref path) = args.mdbx_path {
        let w = StateWriter::new(path, CircularBufferConfig::new(args.buffer_size)?)?;
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

    // Start streaming bootstrap session (if using MDBX)
    // This writes accounts progressively without loading all into memory
    let mut bootstrap = writer
        .as_ref()
        .map(|w| w.begin_bootstrap(0, B256::ZERO, B256::ZERO))
        .transpose()?;

    let mut storage_slots_count: usize = 0;

    // Callback that streams accounts to sinks
    let mut on_account = |account: AccountState| -> Result<()> {
        // Write to JSON immediately
        if let Some(ref mut jw) = json_writer {
            serde_json::to_writer(&mut *jw, &account)?;
            jw.write_all(b"\n")?;
        }

        // Write to MDBX immediately (streaming, no accumulation!)
        if let Some(ref mut bs) = bootstrap {
            storage_slots_count += account.storage.len();
            bs.write_account(&account)?;

            if args.verbose {
                let (accts, _, _) = bs.progress();
                if accts % 10000 == 0 {
                    eprintln!("Written {accts} accounts to MDBX...");
                }
            }
        }

        Ok(())
    };

    // Run dump - either from JSON file or from geth
    let metadata = if args.is_json_input() {
        run_json_dump(&args, &mut on_account)?
    } else {
        run_with_fallback(&args, &mut on_account)?
    };

    // Get final block info
    let final_block_number = args.block_number;
    let final_block_hash = metadata.block_hash.unwrap_or(B256::ZERO);
    let final_state_root = metadata.state_root.unwrap_or(B256::ZERO);

    // Finalize bootstrap (commits the transaction)
    if let Some(mut bs) = bootstrap {
        // Update metadata with correct values from geth output
        bs.set_block_number(final_block_number);
        bs.set_metadata(final_block_hash, final_state_root);

        let stats = bs.finalize()?;

        if args.verbose {
            eprintln!(
                "MDBX: wrote {} accounts, {} storage slots to {} namespaces in {:?}",
                stats.accounts_written / usize::from(writer.as_ref().unwrap().buffer_size()),
                storage_slots_count,
                writer.as_ref().unwrap().buffer_size(),
                stats.total_duration,
            );
        }
    }

    eprintln!(
        "Synced {} accounts for block {} (state_root={:x})",
        metadata.total_accounts, final_block_number, final_state_root
    );

    Ok(())
}
