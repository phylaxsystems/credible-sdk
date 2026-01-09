//! Core orchestration loop that keeps the database in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and written into the database.
use crate::{
    critical,
    genesis::GenesisState,
    metrics::WorkerMetrics,
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
    BlockStateUpdate,
    Reader,
    Writer,
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

/// Report of recovery operations performed during startup or health check.
#[derive(Debug, Default)]
pub struct RecoveryReport {
    /// State of each namespace: (namespace_idx, block_number)
    pub namespace_states: Vec<(u8, u64)>,
    /// Blocks for which diffs were successfully recovered
    pub recovered_diffs: Vec<u64>,
    /// Blocks that failed recovery: (block_number, error_message)
    pub failed_recoveries: Vec<(u64, String)>,
    /// Whether the database is in a consistent state after recovery
    pub is_consistent: bool,
    /// Whether the database was bootstrapped (all namespaces at same block)
    pub is_bootstrapped: bool,
    /// The bootstrap block number (if bootstrapped)
    pub bootstrap_block: Option<u64>,
}

impl RecoveryReport {
    /// Returns true if any recovery operations were performed.
    pub fn had_recoveries(&self) -> bool {
        !self.recovered_diffs.is_empty()
    }

    /// Returns true if any recovery operations failed.
    pub fn had_failures(&self) -> bool {
        !self.failed_recoveries.is_empty()
    }
}

/// Coordinates block ingestion, tracing, and persistence.
pub struct StateWorker<WR>
where
    WR: Writer + Reader,
{
    provider: Arc<RootProvider>,
    trace_provider: Box<dyn TraceProvider>,
    writer_reader: WR,
    genesis_state: Option<GenesisState>,
    metrics: WorkerMetrics,
}

impl<WR> StateWorker<WR>
where
    WR: Writer + Reader + Send + Sync,
    <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    pub fn new(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer_reader: WR,
        genesis_state: Option<GenesisState>,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            metrics: WorkerMetrics::new(),
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
        self.recover_stale_locks()?;

        // Check database health and recover missing state diffs
        let recovery_report = self.check_and_recover_state().await?;

        if recovery_report.is_bootstrapped {
            info!(
                bootstrap_block = recovery_report.bootstrap_block,
                "database was bootstrapped, no historical diff recovery needed"
            );
        }

        if recovery_report.had_recoveries() {
            info!(
                recovered_count = recovery_report.recovered_diffs.len(),
                "successfully recovered missing state diffs"
            );
        }

        // If we had failures and it's NOT a bootstrap scenario, this is fatal
        if recovery_report.had_failures() && !recovery_report.is_bootstrapped {
            critical!(
                failed_count = recovery_report.failed_recoveries.len(),
                failed_blocks = ?recovery_report.failed_recoveries,
                "failed to recover state diffs, database is inconsistent"
            );
            return Err(anyhow!(
                "state recovery failed for {} blocks: {:?}",
                recovery_report.failed_recoveries.len(),
                recovery_report.failed_recoveries
            ));
        }

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

    /// Check for and recover from stale write locks.
    ///
    /// If recovery fails (e.g., missing diffs), the lock remains in place, and
    /// the worker fails to start. This prevents readers from accessing corrupt data.
    fn recover_stale_locks(&self) -> Result<()> {
        match self.writer_reader.recover_stale_locks() {
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
                        self.metrics.record_stale_lock_recovery();
                    }
                }
                Ok(())
            }
            Err(err) => {
                // Recovery failed. The lock remains in place to protect readers.
                critical!(
                    error = %err,
                    "failed to recover stale lock, namespace remains locked, manual intervention required"
                );
                Err(anyhow!("stale lock recovery failed: {err}"))
            }
        }
    }

    /// Determine the next block to ingest. We respect manual overrides so
    /// operators can force a resync of historical ranges when needed.
    fn compute_start_block(&self, override_start: Option<u64>) -> Result<u64> {
        if let Some(block) = override_start {
            return Ok(block);
        }

        let current = self
            .writer_reader
            .latest_block_number()
            .context("failed to read current block from the database")?;

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

        // Ensure all intermediate diffs exist before committing
        // This recovers missing diffs by fetching from execution client
        let recovered = self.ensure_intermediate_diffs(block_number).await?;
        if recovered > 0 {
            info!(
                block_number,
                recovered_diffs = recovered,
                "recovered missing intermediate diffs before commit"
            );
        }

        let mut update = match self.trace_provider.fetch_block_state(block_number).await {
            Ok(update) => update,
            Err(err) => {
                critical!(error = ?err, block_number, "failed to trace block");
                self.metrics.record_block_failure();
                return Err(anyhow!("failed to trace block {block_number}"));
            }
        };

        // Apply system call state changes (EIP-2935 & EIP-4788)
        self.apply_system_calls(&mut update, block_number).await?;

        match self.writer_reader.commit_block(&update) {
            Ok(stats) => {
                self.metrics.record_commit(block_number, &stats);
            }
            Err(err) => {
                critical!(error = ?err, block_number, "failed to persist block");
                self.metrics.record_block_failure();
                return Err(anyhow!("failed to persist block {block_number}"));
            }
        }

        info!(block_number, "block persisted to database");
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
        match system_calls::compute_system_call_states(&config, Some(&self.writer_reader)) {
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

        let already_exists = self.writer_reader.latest_block_number()?.is_some();

        if already_exists {
            info!("block 0 already exists in database; skipping genesis hydration");
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

        match self.writer_reader.commit_block(&update) {
            Ok(stats) => {
                self.metrics.record_commit(0, &stats);
            }
            Err(err) => {
                critical!(error = ?err, block_number = 0, "failed to persist genesis block");
                self.metrics.record_block_failure();
                return Err(anyhow!("failed to persist genesis block"));
            }
        }

        info!(block_number = 0, "genesis block persisted to database");
        Ok(())
    }

    /// Ensure all intermediate state diffs exist for committing target_block.
    ///
    /// When rotating the circular buffer (e.g., block 103 replacing block 100
    /// in namespace 1 with buffer_size=3), we need diffs for blocks 101 and 102
    /// to reconstruct the correct state. If any diffs are missing, this method
    /// fetches them from the execution client.
    ///
    /// Returns the number of diffs that were recovered.
    async fn ensure_intermediate_diffs(&mut self, target_block: u64) -> Result<usize> {
        let buffer_size = self.writer_reader.buffer_size();
        let namespace_idx = (target_block % u64::from(buffer_size)) as u8;

        // Get current block in the target namespace
        let current_ns_block = self
            .writer_reader
            .get_namespace_block(namespace_idx)
            .context("failed to get namespace block")?;

        let Some(existing_block) = current_ns_block else {
            // Namespace is empty, no intermediate diffs needed
            return Ok(0);
        };

        // Calculate required diff range
        let start_block = existing_block + 1;
        if target_block <= start_block {
            // No intermediate diffs needed
            return Ok(0);
        }

        // Find missing diffs
        let mut missing_diffs = Vec::new();
        for diff_block in start_block..target_block {
            let has_diff = self
                .writer_reader
                .has_state_diff(diff_block)
                .context(format!("failed to check diff for block {diff_block}"))?;

            if !has_diff {
                missing_diffs.push(diff_block);
            }
        }

        if missing_diffs.is_empty() {
            return Ok(0);
        }

        info!(
            target_block,
            namespace_idx,
            existing_block,
            missing_count = missing_diffs.len(),
            missing_blocks = ?missing_diffs,
            "recovering missing state diffs"
        );

        // Recover each missing diff
        for diff_block in &missing_diffs {
            self.recover_state_diff(*diff_block).await?;
        }

        Ok(missing_diffs.len())
    }

    /// Recover a single state diff by fetching from the execution client.
    async fn recover_state_diff(&mut self, block_number: u64) -> Result<()> {
        info!(block_number, "fetching state diff from execution client");

        // Fetch block state from execution client
        let mut update = match self.trace_provider.fetch_block_state(block_number).await {
            Ok(update) => update,
            Err(err) => {
                critical!(
                    error = ?err,
                    block_number,
                    "failed to fetch block state for diff recovery"
                );
                return Err(anyhow!(
                    "failed to recover state diff for block {block_number}: {err}"
                ));
            }
        };

        // Apply system calls (EIP-2935 & EIP-4788)
        self.apply_system_calls(&mut update, block_number).await?;

        // Store just the diff (not the full block state)
        self.writer_reader
            .store_state_diff(&update)
            .context(format!(
                "failed to store recovered diff for block {block_number}"
            ))?;

        info!(block_number, "successfully recovered state diff");
        Ok(())
    }

    /// Detect if the database is in a bootstrapped state.
    ///
    /// A bootstrapped database has all namespaces populated with the same block number,
    /// which happens when using `bootstrap_from_snapshot`. In this state, there are no
    /// historical diffs stored (since bootstrap populates state directly), but this is
    /// expected and not an error condition.
    fn detect_bootstrap_state(&self) -> Result<Option<u64>> {
        let buffer_size = self.writer_reader.buffer_size();
        let mut namespace_blocks = Vec::new();

        for ns in 0..buffer_size {
            let ns_block = self
                .writer_reader
                .get_namespace_block(ns)
                .context(format!("failed to get namespace {ns} block"))?;

            match ns_block {
                Some(block) => namespace_blocks.push(block),
                None => {
                    // If any namespace is empty, it's not a bootstrap state
                    return Ok(None);
                }
            }
        }

        // Check if all namespaces have the same block
        if namespace_blocks.is_empty() {
            return Ok(None);
        }

        let first_block = namespace_blocks[0];
        let all_same = namespace_blocks.iter().all(|&b| b == first_block);

        if all_same {
            Ok(Some(first_block))
        } else {
            Ok(None)
        }
    }

    /// Check database health and recover from any inconsistencies.
    ///
    /// This performs several checks:
    /// 1. Detects if database was bootstrapped (all namespaces at same block)
    /// 2. Verifies all namespaces are consistent
    /// 3. Checks for missing state diffs in the buffer range
    /// 4. Recovers missing diffs by fetching from execution client
    ///
    /// For bootstrapped databases, missing diffs for blocks <= bootstrap_block
    /// are expected and not considered an error.
    ///
    /// Call this during startup for proactive recovery.
    pub async fn check_and_recover_state(&mut self) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();

        let buffer_size = self.writer_reader.buffer_size();
        let latest = self
            .writer_reader
            .latest_block_number()
            .context("failed to get latest block")?;

        let Some(latest_block) = latest else {
            info!("database is empty, no recovery needed");
            report.is_consistent = true;
            return Ok(report);
        };

        info!(
            latest_block,
            buffer_size, "checking database state for recovery"
        );

        // Check if this is a bootstrapped database
        if let Some(bootstrap_block) = self.detect_bootstrap_state()? {
            info!(
                bootstrap_block,
                "detected bootstrapped database state, all namespaces at same block"
            );
            report.is_bootstrapped = true;
            report.bootstrap_block = Some(bootstrap_block);

            // For bootstrapped state, we only need to verify diffs for blocks
            // AFTER the bootstrap block (which shouldn't exist yet anyway)
            // This is a valid state - no recovery needed
            report.is_consistent = true;

            // Still collect namespace states for the report
            for ns in 0..buffer_size {
                if let Some(block) = self
                    .writer_reader
                    .get_namespace_block(ns)
                    .context(format!("failed to get namespace {ns} block"))?
                {
                    report.namespace_states.push((ns, block));
                }
            }

            return Ok(report);
        }

        // Not a bootstrapped state - check each namespace and collect their states
        for ns in 0..buffer_size {
            let ns_block = self
                .writer_reader
                .get_namespace_block(ns)
                .context(format!("failed to get namespace {ns} block"))?;

            if let Some(block) = ns_block {
                report.namespace_states.push((ns, block));
            }
        }

        // Determine the range of blocks we should have diffs for
        // We need diffs for blocks that might be needed for namespace rotation
        let oldest_needed = latest_block.saturating_sub(u64::from(buffer_size) - 1);

        // Check for missing diffs in the valid range
        let mut missing_diffs = Vec::new();
        for block in oldest_needed..=latest_block {
            // Skip block 0 (genesis doesn't need a diff)
            if block == 0 {
                continue;
            }

            let has_diff = self
                .writer_reader
                .has_state_diff(block)
                .context(format!("failed to check diff for block {block}"))?;

            if !has_diff {
                missing_diffs.push(block);
            }
        }

        if !missing_diffs.is_empty() {
            warn!(
                missing_count = missing_diffs.len(),
                blocks = ?missing_diffs,
                "found missing state diffs, attempting recovery"
            );

            for block in missing_diffs {
                match self.recover_state_diff(block).await {
                    Ok(()) => {
                        report.recovered_diffs.push(block);
                    }
                    Err(err) => {
                        critical!(
                            block,
                            error = %err,
                            "failed to recover state diff"
                        );
                        report.failed_recoveries.push((block, err.to_string()));
                    }
                }
            }
        }

        // Database is consistent if we have no failed recoveries
        report.is_consistent = report.failed_recoveries.is_empty();

        if report.had_recoveries() {
            info!(
                recovered_count = report.recovered_diffs.len(),
                recovered_blocks = ?report.recovered_diffs,
                "state diff recovery complete"
            );
        }

        Ok(report)
    }
}
