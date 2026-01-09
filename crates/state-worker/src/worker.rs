#![allow(clippy::unreadable_literal)]
//! Core orchestration loop that keeps Redis in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and written into Redis.
use crate::{
    critical,
    genesis::GenesisState,
    state::{
        BlockStateUpdateBuilder,
        TraceProvider,
    },
    system_calls::{
        self,
        SystemCallConfig,
    },
};
use alloy::rpc::types::Header;
use alloy_provider::{
    Provider,
    RootProvider,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use futures::StreamExt;
use state_store::{
    StateReader,
    StateWriter,
    common::{
        BlockStateUpdate,
        error::StateError,
    },
};
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::broadcast,
    time,
};
use tracing::{
    debug,
    info,
    warn,
};

const SUBSCRIPTION_RETRY_DELAY_SECS: u64 = 5;

/// Coordinates block ingestion, tracing, and persistence.
pub struct StateWorker {
    provider: Arc<RootProvider>,
    trace_provider: Box<dyn TraceProvider>,
    writer: StateWriter,
    reader: StateReader,
    genesis_state: Option<GenesisState>,
}

impl StateWorker {
    /// Build a worker that shares the provider/Redis client across async tasks.
    pub fn new(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer: StateWriter,
        reader: StateReader,
        genesis_state: Option<GenesisState>,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer,
            reader,
            genesis_state,
        }
    }

    /// Drive the catch-up + streaming loop. We keep retrying the subscription
    /// because websocket connections can drop in practice.
    pub async fn run(
        &mut self,
        start_override: Option<u64>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        // Recover from any stale locks left by crashed writers
        self.recover_stale_locks_with_healing().await?;

        let mut next_block = self.compute_start_block(start_override)?;

        loop {
            // Check for shutdown before starting catch-up
            if shutdown_rx.try_recv().is_ok() {
                info!("Shutdown signal received");
                return Ok(());
            }

            // Catch up block-by-block, checking shutdown between each
            if self.catch_up(&mut next_block, &mut shutdown_rx).await? {
                info!("Shutdown signal received during catch-up");
                return Ok(());
            }

            match self.stream_blocks(&mut next_block, &mut shutdown_rx).await {
                Ok(()) => {
                    info!("Shutdown signal received during streaming");
                    return Ok(());
                }
                Err(err) => {
                    warn!(error = %err, "block subscription ended, retrying");
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Shutdown signal received during retry sleep");
                            return Ok(());
                        }
                        () = time::sleep(Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS)) => {}
                    }
                }
            }
        }
    }

    /// Enhanced stale lock recovery with self-healing capabilities.
    ///
    /// This method wraps the basic recovery and adds:
    /// 1. Fetching missing state diffs from the trace provider
    /// 2. Rebuilding corrupted namespaces from valid ones
    async fn recover_stale_locks_with_healing(&mut self) -> Result<()> {
        const MAX_RECOVERY_ATTEMPTS: usize = 8;

        for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
            match self.writer.recover_stale_locks() {
                Ok(recoveries) => {
                    if recoveries.is_empty() {
                        debug!("no stale locks detected");
                    } else {
                        for recovery in &recoveries {
                            info!(
                                namespace = %recovery.namespace,
                                target_block = recovery.target_block,
                                previous_block = ?recovery.previous_block,
                                writer_id = %recovery.writer_id,
                                "recovered stale lock and repaired state"
                            );
                        }
                    }

                    // After stale lock recovery succeeds, validate the circular buffer layout.
                    // If any namespace is inconsistent, repair it from a valid snapshot.
                    self.repair_corrupted_namespaces().await?;

                    return Ok(());
                }
                Err(err) => {
                    match err {
                        // Missing diff: fetch from client, store, and retry recovery.
                        StateError::MissingStateDiff {
                            needed_block,
                            target_block,
                        } => {
                            warn!(
                                needed_block,
                                target_block,
                                attempt,
                                "missing state diff detected during stale lock recovery; fetching from trace provider"
                            );

                            self.fetch_and_store_missing_diff(needed_block).await?;
                        }

                        // Stale lock detected: attempt to repair buffer layout first, then retry.
                        StateError::StaleLockDetected {
                            namespace,
                            writer_id,
                            started_at,
                        } => {
                            warn!(
                                namespace = %namespace,
                                writer_id = %writer_id,
                                started_at,
                                attempt,
                                "stale lock detected; attempting namespace repair before retry"
                            );

                            self.repair_corrupted_namespaces().await?;
                        }

                        // Any other error: attempt a repair once, then retry.
                        other => {
                            warn!(
                                error = %other,
                                attempt,
                                "stale lock recovery failed; attempting namespace repair and retry"
                            );

                            if let Err(repair_err) = self.repair_corrupted_namespaces().await {
                                warn!(
                                    error = %repair_err,
                                    attempt,
                                    "namespace repair failed during recovery; will continue retrying"
                                );
                            }

                            if attempt == MAX_RECOVERY_ATTEMPTS {
                                critical!(
                                    error = %other,
                                    "failed to recover stale lock, namespace remains locked, manual intervention required"
                                );
                                return Err(anyhow!(
                                    "stale lock recovery failed after {attempt} attempts: {other}",
                                ));
                            }
                        }
                    }
                }
            }
        }

        Err(anyhow!(
            "stale lock recovery failed after {MAX_RECOVERY_ATTEMPTS} attempts",
        ))
    }

    /// Fetch a missing state diff from the trace provider and store it.
    async fn fetch_and_store_missing_diff(&mut self, block_number: u64) -> Result<()> {
        info!(
            block_number,
            "fetching missing state diff from trace provider"
        );

        // Fetch the block state from the trace provider
        let mut update = self
            .trace_provider
            .fetch_block_state(block_number)
            .await
            .with_context(|| format!("failed to fetch block state for block {block_number}"))?;

        // Apply system call state changes (EIP-2935 & EIP-4788)
        self.apply_system_calls(&mut update, block_number).await?;

        // Store the diff (this doesn't commit the block, just stores the diff for recovery)
        self.writer
            .store_diff_only(&update)
            .with_context(|| format!("failed to store diff for block {block_number}"))?;

        info!(block_number, "successfully stored missing state diff");
        Ok(())
    }

    /// Detect corrupted namespaces (wrong metadata / missing block state keys / etc.)
    /// and rebuild them by fetching the correct state from the trace provider.
    ///
    /// Instead of copying from a valid namespace (which would copy the wrong
    /// block number), we fetch the correct state for each namespace's expected block
    /// directly from the trace provider.
    async fn repair_corrupted_namespaces(&mut self) -> Result<()> {
        let Some((valid_idx, valid_block)) = self
            .writer
            .find_valid_namespace_state()
            .context("failed to find valid namespace state")?
        else {
            // No valid namespace found - this is OK for fresh/empty Redis
            // Nothing to repair, nothing to validate against
            debug!("no valid namespace found; assuming fresh state, skipping repair");
            return Ok(());
        };

        let buffer_size = self.writer.buffer_size();
        let mut repaired_any = false;

        debug!(
            valid_idx,
            valid_block, "found valid namespace snapshot; validating circular buffer layout"
        );

        for namespace_idx in 0..buffer_size {
            if namespace_idx == valid_idx {
                continue; // Skip the valid source namespace
            }

            let expected_block = self
                .writer
                .expected_block_for_namespace(namespace_idx, valid_block);

            let actual_block = self
                .writer
                .get_namespace_block(namespace_idx)
                .context("failed to read namespace block number")?;

            if actual_block != Some(expected_block) {
                warn!(
                    namespace_idx,
                    expected_block,
                    actual_block = ?actual_block,
                    valid_idx,
                    valid_block,
                    "namespace block number mismatch; repairing from valid state"
                );

                // First, ensure all required diffs exist
                // We need diffs from (valid_block + 1) to expected_block
                let start_diff = valid_block + 1;
                for block_num in start_diff..=expected_block {
                    if !self.writer.has_diff(block_num)? {
                        info!(
                            block_num,
                            "fetching missing diff from trace provider for repair"
                        );
                        self.fetch_and_store_missing_diff(block_num).await?;
                    }
                }

                // Now repair: copy from valid namespace, then apply diffs
                self.writer
                    .repair_namespace_from_valid_state(namespace_idx, valid_idx, expected_block)
                    .with_context(|| {
                        format!(
                            "failed to repair namespace {namespace_idx} to block {expected_block}",
                        )
                    })?;

                repaired_any = true;

                info!(
                    namespace_idx,
                    expected_block,
                    source_namespace = valid_idx,
                    source_block = valid_block,
                    "successfully repaired namespace from valid state + diffs"
                );
            }
        }

        if repaired_any {
            info!(
                valid_idx,
                valid_block,
                "repaired one or more corrupted namespaces; redis state is now consistent"
            );
        } else {
            debug!("all namespaces match expected circular buffer layout");
        }

        Ok(())
    }

    /// Determine the next block to ingest. We respect manual overrides so
    /// operators can force a resync of historical ranges when needed.
    fn compute_start_block(&self, override_start: Option<u64>) -> Result<u64> {
        if let Some(block) = override_start {
            return Ok(block);
        }

        let current = self
            .writer
            .latest_block_number()
            .context("failed to read current block from redis")?;

        Ok(current.map_or(0, |b| b + 1))
    }

    /// Sequentially replay blocks until we reach the node's current head.
    async fn catch_up(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<bool> {
        loop {
            let head = self.provider.get_block_number().await?;

            if *next_block > head {
                return Ok(false);
            }

            while *next_block <= head {
                // Process the block fully before checking shutdown
                self.process_block(*next_block).await?;
                *next_block += 1;

                // Check for shutdown after the block is complete
                if shutdown_rx.try_recv().is_ok() {
                    return Ok(true);
                }
            }
        }
    }

    /// Follow the `newHeads` stream and process new blocks in order, tolerating
    /// duplicate/stale headers after reconnects.
    async fn stream_blocks(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<()> {
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received during block streaming");
                    return Ok(());
                }
                maybe_header = stream.next() => {
                    match maybe_header {
                        Some(header) => {
                            let Header { hash: _, inner, .. } = header;
                            let block_number = inner.number;

                            // We may receive a header we already processed if the stream
                            // briefly disconnects; skip without rewinding because we do not
                            // currently handle reorgs.
                            if block_number + 1 < *next_block {
                                debug!(block_number, next_block, "skipping stale header");
                                continue;
                            }

                            // If we are missing a block, trigger a critical error
                            if block_number > *next_block {
                                critical!("Missing block {block_number} (next block: {next_block})");
                                return Err(anyhow!(
                                    "Missing block {block_number} (next block: {next_block})"
                                ));
                            }

                            while *next_block <= block_number {
                                self.process_block(*next_block).await?;
                                *next_block += 1;
                            }
                        }
                        None => {
                            return Err(anyhow!("block subscription completed"));
                        }
                    }
                }
            }
        }
    }

    /// Pull, trace, and persist a single block.
    async fn process_block(&mut self, block_number: u64) -> Result<()> {
        info!(block_number, "processing block");

        if block_number == 0 && self.genesis_state.is_some() {
            return self.process_genesis_block().await;
        }

        let mut update = match self.trace_provider.fetch_block_state(block_number).await {
            Ok(update) => update,
            Err(err) => {
                critical!(error = ?err, block_number, "failed to trace block");
                return Err(anyhow!("failed to trace block {block_number}"));
            }
        };

        // Apply system call state changes (EIP-2935 & EIP-4788)
        self.apply_system_calls(&mut update, block_number).await?;

        match self.writer.commit_block(update) {
            Ok(()) => (),
            Err(err) => {
                critical!(error = ?err, block_number, "failed to persist block");
                return Err(anyhow!("failed to persist block {block_number}"));
            }
        }

        info!(block_number, "block persisted to redis");
        Ok(())
    }

    /// Apply EIP-2935 and EIP-4788 system call state changes
    async fn apply_system_calls(
        &self,
        update: &mut BlockStateUpdate,
        block_number: u64,
    ) -> Result<()> {
        // Fetch block header for system call data
        let block = self
            .provider
            .get_block_by_number(block_number.into())
            .await?
            .context(format!("block {block_number} not found"))?;

        // Get parent block hash (current block's parent_hash field)
        let parent_block_hash = if block_number > 0 {
            Some(block.header.parent_hash)
        } else {
            None
        };

        // Get parent beacon block root from block header (EIP-4788)
        let parent_beacon_block_root = block.header.parent_beacon_block_root;

        let config = SystemCallConfig {
            block_number,
            timestamp: block.header.timestamp,
            parent_block_hash,
            parent_beacon_block_root,
        };

        // Pass the reader to fetch existing account state
        match system_calls::compute_system_call_states(&config, Some(&self.reader)) {
            Ok(states) => {
                for state in states {
                    update.merge_account_state(state);
                }
                debug!(block_number, "applied system call state changes");
                Ok(())
            }
            Err(err) => {
                // Log but don't fail
                warn!(
                    block_number,
                    error = %err,
                    "failed to compute system call states, traces may already include them"
                );
                Ok(())
            }
        }
    }

    /// Process block 0 using the configured genesis state
    async fn process_genesis_block(&mut self) -> Result<()> {
        let Some(genesis) = self.genesis_state.take() else {
            warn!("no genesis state configured; skipping genesis hydration");
            return Ok(());
        };

        let already_exists = self
            .writer
            .latest_block_number()
            .context("failed to check if genesis already exists")?
            .is_some();

        if already_exists {
            info!("block 0 already exists in redis; skipping genesis hydration");
            return Ok(());
        }

        let accounts = genesis.into_accounts();

        if accounts.is_empty() {
            warn!("genesis file contained no accounts; skipping hydration");
            return Ok(());
        }

        // Fetch genesis block header to get hash and state root
        let block = self
            .provider
            .get_block_by_number(0.into())
            .await?
            .context("genesis block not found on chain")?;

        info!("hydrating genesis state from genesis file");

        let update = BlockStateUpdateBuilder::from_accounts(
            0,
            block.header.hash,
            block.header.state_root,
            accounts,
        );

        match self.writer.commit_block(update) {
            Ok(()) => (),
            Err(err) => {
                critical!(error = ?err, block_number = 0, "failed to persist genesis block");
                return Err(anyhow!("failed to persist genesis block"));
            }
        }

        info!(block_number = 0, "genesis block persisted to redis");
        Ok(())
    }
}
