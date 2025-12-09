use criterion::{
    BenchmarkId,
    Criterion,
    black_box,
    criterion_group,
    criterion_main,
};
use std::{
    collections::HashMap,
    sync::Arc,
};

type Address = [u8; 20];
type U256 = u128;

#[derive(Debug, Clone, Default)]
pub struct ForkStorageMap {
    pub map: HashMap<U256, U256>,
    pub dont_read_from_inner_db: bool,
}

#[derive(Debug)]
pub struct ForkDbWithArc {
    pub storage: Arc<HashMap<Address, ForkStorageMap>>,
}

impl Clone for ForkDbWithArc {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }
}

impl ForkDbWithArc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: Arc::new(HashMap::default()),
        }
    }
}

impl Default for ForkDbWithArc {
    fn default() -> Self {
        Self::new()
    }

    pub fn mark_accounts_isolated(&mut self, addresses: &[Address]) {
        if addresses.is_empty() {
            return;
        }
        let storage = Arc::make_mut(&mut self.storage);
        for address in addresses {
            let entry = storage.entry(*address).or_default();
            entry.dont_read_from_inner_db = true;
        }
    }
}

#[derive(Debug, Clone)]
pub struct ForkDbWithoutArc {
    pub storage: HashMap<Address, ForkStorageMap>,
}

impl ForkDbWithoutArc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: HashMap::default(),
        }
    }
}

impl Default for ForkDbWithoutArc {
    fn default() -> Self {
        Self::new()
    }

    pub fn mark_accounts_isolated(&mut self, addresses: &[Address]) {
        if addresses.is_empty() {
            return;
        }
        for address in addresses {
            let entry = self.storage.entry(*address).or_default();
            entry.dont_read_from_inner_db = true;
        }
    }
}

// Helper to create test data
fn create_storage_with_data(
    num_addresses: usize,
    slots_per_address: usize,
) -> HashMap<Address, ForkStorageMap> {
    let mut storage = HashMap::new();

    for i in 0..num_addresses {
        let mut addr = [0u8; 20];
        addr[0] = u8::try_from(i % 256).unwrap();

        let mut map = HashMap::new();
        for j in 0..slots_per_address {
            map.insert(j as U256, (i * 1000 + j) as U256);
        }

        storage.insert(
            addr,
            ForkStorageMap {
                map,
                dont_read_from_inner_db: false,
            },
        );
    }

    storage
}

/// Benchmark: Parallel Distribution Pattern
/// Simulates distributing `ForkDb` to multiple assertion executors
fn benchmark_parallel_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_distribution");
    group.sample_size(10);

    for num_clones in &[10, 50, 100] {
        // WITH Arc
        group.bench_with_input(
            BenchmarkId::new("with_arc", num_clones),
            num_clones,
            |b, &num_clones| {
                let storage = create_storage_with_data(500, 10);
                let fork_db = ForkDbWithArc {
                    storage: Arc::new(storage),
                };

                b.iter(|| {
                    let mut clones = vec![fork_db.clone()];
                    for _ in 0..num_clones {
                        clones.push(fork_db.clone());
                    }
                    black_box(clones);
                });
            },
        );

        // WITHOUT Arc
        group.bench_with_input(
            BenchmarkId::new("without_arc", num_clones),
            num_clones,
            |b, &num_clones| {
                let storage = create_storage_with_data(500, 10);
                let fork_db = ForkDbWithoutArc { storage };

                b.iter(|| {
                    let mut clones = vec![fork_db.clone()];
                    for _ in 0..num_clones {
                        clones.push(fork_db.clone());
                    }
                    black_box(clones);
                });
            },
        );
    }

    group.finish();
}

fn benchmark_cow_mutation_single_holder(c: &mut Criterion) {
    let mut group = c.benchmark_group("cow_mutation_single_holder");

    for storage_size in &[100, 500, 1000] {
        // WITH Arc
        group.bench_with_input(
            BenchmarkId::new("with_arc", storage_size),
            storage_size,
            |b, &storage_size| {
                b.iter_batched(
                    || {
                        let storage = create_storage_with_data(storage_size, 10);
                        ForkDbWithArc {
                            storage: Arc::new(storage),
                        }
                    },
                    |mut fork_db| {
                        let addresses: Vec<Address> = (0..10)
                            .map(|i| {
                                let mut addr = [0u8; 20];
                                addr[0] = u8::try_from(i % 256).unwrap();
                                addr
                            })
                            .collect();
                        fork_db.mark_accounts_isolated(&addresses);
                        black_box(fork_db);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // WITHOUT Arc
        group.bench_with_input(
            BenchmarkId::new("without_arc", storage_size),
            storage_size,
            |b, &storage_size| {
                b.iter_batched(
                    || {
                        let storage = create_storage_with_data(storage_size, 10);
                        ForkDbWithoutArc { storage }
                    },
                    |mut fork_db| {
                        let addresses: Vec<Address> = (0..10)
                            .map(|i| {
                                let mut addr = [0u8; 20];
                                addr[0] = u8::try_from(i % 256).unwrap();
                                addr
                            })
                            .collect();
                        fork_db.mark_accounts_isolated(&addresses);
                        black_box(fork_db);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: Copy-on-Write with Shared References
/// Simulates mutations while Arc is shared by other clones
fn benchmark_cow_mutation_shared(c: &mut Criterion) {
    let mut group = c.benchmark_group("cow_mutation_shared");
    group.sample_size(10);

    for storage_size in &[100, 500] {
        // WITH Arc
        group.bench_with_input(
            BenchmarkId::new("with_arc_shared", storage_size),
            storage_size,
            |b, &storage_size| {
                b.iter_batched(
                    || {
                        let storage = create_storage_with_data(storage_size, 10);
                        let fork_db = ForkDbWithArc {
                            storage: Arc::new(storage),
                        };
                        // Create shared clones to simulate parallel execution
                        let clone1 = fork_db.clone();
                        let clone2 = fork_db.clone();
                        (fork_db, clone1, clone2)
                    },
                    |(mut fork_db, _clone1, _clone2)| {
                        let addresses: Vec<Address> = (0..10)
                            .map(|i| {
                                let mut addr = [0u8; 20];
                                addr[0] = u8::try_from(i % 256).unwrap();
                                addr
                            })
                            .collect();
                        // Mutation happens, but other clones still hold references
                        fork_db.mark_accounts_isolated(&addresses);
                        black_box(fork_db);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // WITHOUT Arc (always clones on mutation)
        group.bench_with_input(
            BenchmarkId::new("without_arc_cloned", storage_size),
            storage_size,
            |b, &storage_size| {
                b.iter_batched(
                    || {
                        let storage = create_storage_with_data(storage_size, 10);
                        let fork_db = ForkDbWithoutArc { storage };
                        let clone1 = fork_db.clone();
                        let clone2 = fork_db.clone();
                        (fork_db, clone1, clone2)
                    },
                    |(mut fork_db, _clone1, _clone2)| {
                        let addresses: Vec<Address> = (0..10)
                            .map(|i| {
                                let mut addr = [0u8; 20];
                                addr[0] = u8::try_from(i % 256).unwrap();
                                addr
                            })
                            .collect();
                        fork_db.mark_accounts_isolated(&addresses);
                        black_box(fork_db);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn benchmark_realistic_workflow(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic_workflow");
    group.sample_size(10);

    for num_parallel in &[4, 8] {
        // WITH Arc
        group.bench_with_input(
            BenchmarkId::new("with_arc", num_parallel),
            num_parallel,
            |b, &num_parallel| {
                let storage = create_storage_with_data(500, 10);
                let fork_db = ForkDbWithArc {
                    storage: Arc::new(storage),
                };

                b.iter(|| {
                    // Phase 1: Clone for parallel execution (very cheap with Arc)
                    let mut fork_dbs: Vec<_> = (0..num_parallel).map(|_| fork_db.clone()).collect();

                    // Phase 2: Each executor mutates independently
                    for (idx, fork_db) in fork_dbs.iter_mut().enumerate() {
                        let addr = [u8::try_from(idx).unwrap(); 20];
                        let mut storage = Arc::unwrap_or_clone(Arc::clone(&fork_db.storage));
                        storage.insert(
                            addr,
                            ForkStorageMap {
                                map: HashMap::from([(idx as U256, idx as U256)]),
                                dont_read_from_inner_db: false,
                            },
                        );
                        fork_db.storage = Arc::new(storage);
                    }

                    black_box(fork_dbs);
                });
            },
        );

        // WITHOUT Arc
        group.bench_with_input(
            BenchmarkId::new("without_arc", num_parallel),
            num_parallel,
            |b, &num_parallel| {
                let storage = create_storage_with_data(500, 10);
                let fork_db = ForkDbWithoutArc { storage };

                b.iter(|| {
                    // Phase 1: Clone for parallel execution (expensive)
                    let mut fork_dbs: Vec<_> = (0..num_parallel).map(|_| fork_db.clone()).collect();

                    // Phase 2: Each executor mutates
                    for (idx, fork_db) in fork_dbs.iter_mut().enumerate() {
                        let addr = [u8::try_from(idx).unwrap(); 20];
                        fork_db.storage.insert(
                            addr,
                            ForkStorageMap {
                                map: HashMap::from([(idx as U256, idx as U256)]),
                                dont_read_from_inner_db: false,
                            },
                        );
                    }

                    black_box(fork_dbs);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_parallel_distribution,
    benchmark_cow_mutation_single_holder,
    benchmark_cow_mutation_shared,
    benchmark_realistic_workflow
);
criterion_main!(benches);
