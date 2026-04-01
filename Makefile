build:
	cargo build --verbose --release --locked

build-contracts:
	forge build --root testdata/mock-protocol 

PRESET ?= avg_block_100_aa
MODE ?= full
ITERS ?= 1
FILE ?= artifacts/profiles/$(PRESET)-$(MODE).json.gz
SYMS_FILE ?= $(patsubst %.json.gz,%.json.syms.json,$(FILE))
ATTACH_FILE ?= artifacts/profiles/$(PRESET)-$(MODE)-attach.json.gz
ATTACH_SYMS_FILE ?= $(patsubst %.json.gz,%.json.syms.json,$(ATTACH_FILE))
READY_FILE ?= artifacts/profiles/.perf-ready-$(PRESET)-$(MODE)
PERF_LOG ?= artifacts/profiles/$(PRESET)-$(MODE)-attach.log
WARMUP_ITERS ?= 1
ATTACH_WAIT_MS ?= 5000
# Presymbolicate by default so saved profiles can be reviewed later without needing
# the original local build artifacts in place.
PERF_PROFILE_FLAGS ?= --unstable-presymbolicate
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
# `samply record` needs sudo on macOS, but keeping PATH preserves the user's cargo
# install location for the profiler binary itself.
SAMPLE_RECORD_PREFIX := sudo env PATH="$(PATH)"
else
SAMPLE_RECORD_PREFIX :=
endif

# Clean up test containers
test-cleanup:
	@echo "Cleaning up test containers..."
	@docker ps -a --filter "ancestor=redis:5.0" --format "{{.ID}}" | xargs -r docker rm -f || true
	@docker ps -a --filter "ancestor=solc" --format "{{.ID}}" | xargs -r docker rm -f || true

# Run the rust tests for optimism (excluding state-worker packages which run separately)
test-optimism: build-contracts
	ASSERTION_DA_SOLC_DOCKER_PLATFORM=linux/amd64 cargo nextest run --workspace --exclude mdbx --exclude state-worker --locked  --cargo-profile release --no-tests=warn --no-default-features --features optimism --features test

# Run the rust tests for default evm (excluding state-worker packages which run separately)
test-default: build-contracts
	ASSERTION_DA_SOLC_DOCKER_PLATFORM=linux/amd64 cargo nextest run --workspace --exclude mdbx --exclude state-worker --locked  --cargo-profile release --no-tests=warn --no-default-features --features test

# Run state worker tests (single-threaded to avoid race conditions)
test-state-worker:
	cargo nextest run --package mdbx --package state-worker --locked --cargo-profile release --no-default-features --test-threads=2

test: test-optimism test-default test-state-worker test-cleanup

# Run tests without full tests (skips Docker-dependent tests and integration tests)
test-no-full:
	./scripts/test-no-full.sh

bench-assex: build-contracts
	cargo bench --manifest-path crates/assertion-executor/Cargo.toml --features test --benches

perf-profile: build-contracts
	mkdir -p artifacts/profiles
	cargo build -p benchmark-utils --profile debug-perf --bin benchmark-utils-perf
	# Warm the fixture setup before the recorded iteration so the captured flamegraph
	# reflects steady-state executor work instead of one-time artifact loading noise.
	$(SAMPLE_RECORD_PREFIX) samply record --save-only $(PERF_PROFILE_FLAGS) -o $(FILE) -- ./target/debug-perf/benchmark-utils-perf --preset $(PRESET) --mode $(MODE) --warmup-iters $(WARMUP_ITERS) --pause-after-warmup-ms $(ATTACH_WAIT_MS) --iters $(ITERS)
	@if [ "$(UNAME_S)" = "Darwin" ]; then \
		if [ -f "$(FILE)" ]; then sudo chown $$(id -un):$$(id -gn) "$(FILE)"; fi; \
		if [ -f "$(SYMS_FILE)" ]; then sudo chown $$(id -un):$$(id -gn) "$(SYMS_FILE)"; fi; \
	fi

perf-profile-attach: build-contracts
	mkdir -p artifacts/profiles
	cargo build -p benchmark-utils --profile debug-perf --bin benchmark-utils-perf
	rm -f $(READY_FILE) $(PERF_LOG)
	# Launch the warmed workload first, then attach `samply` once the process has
	# finished its cold setup and is paused at a known synchronization point.
	@perf_pid_file=$$(mktemp); \
	trap 'if [ -f "$$perf_pid_file" ]; then perf_pid=$$(cat "$$perf_pid_file"); kill $$perf_pid 2>/dev/null || true; fi; rm -f "$(READY_FILE)" "$$perf_pid_file"' EXIT; \
	./target/debug-perf/benchmark-utils-perf --preset $(PRESET) --mode $(MODE) --warmup-iters $(WARMUP_ITERS) --pause-after-warmup-ms $(ATTACH_WAIT_MS) --ready-file $(READY_FILE) --iters $(ITERS) > "$(PERF_LOG)" 2>&1 & \
	perf_pid=$$!; \
	echo $$perf_pid > "$$perf_pid_file"; \
	while [ ! -f "$(READY_FILE)" ]; do sleep 0.1; done; \
	$(SAMPLE_RECORD_PREFIX) samply record --save-only $(PERF_PROFILE_FLAGS) -o $(ATTACH_FILE) -p $$perf_pid; \
	wait $$perf_pid; \
	rm -f "$(READY_FILE)" "$$perf_pid_file"
	@if [ "$(UNAME_S)" = "Darwin" ]; then \
		if [ -f "$(ATTACH_FILE)" ]; then sudo chown $$(id -un):$$(id -gn) "$(ATTACH_FILE)"; fi; \
		if [ -f "$(ATTACH_SYMS_FILE)" ]; then sudo chown $$(id -un):$$(id -gn) "$(ATTACH_SYMS_FILE)"; fi; \
	fi

perf-load:
	samply load $(FILE)

# Validate formatting
format:
	cargo fmt --check

# Errors if there is a warning with clippy
lint:
	cargo clippy --all-targets --workspace   --profile dev -- -D warnings -D clippy::pedantic

# Fix linting errors
lint-fix:
	cargo clippy --all-targets --workspace   --profile dev --fix -- -D clippy::pedantic

# Can be used as a manual pre-commit check
pre-commit:
	cargo fmt && make lint

docker-build:
	docker build -f dockerfile/Dockerfile.da -t assertion-da:local .

docker-build-dev:
	docker build -f dockerfile/Dockerfile.da --build-arg BUILD_FLAGS="--features debug_assertions" -t assertion-da:dev-local .

# Run the docker image
compose:
	docker compose -f etc/docker-compose.yaml up

# Run the docker image for development
compose-dev:
	docker compose -f etc/docker-compose-dev.yaml up

# Regenerate dapp-api-client from latest OpenAPI spec
regenerate:
	@echo "Regenerating dapp-api-client from latest OpenAPI spec..."
	cd crates/dapp-api-client && FORCE_SPEC_REGENERATE=true cargo build --features regenerate
	@echo "Client regenerated! Review changes with: git diff crates/dapp-api-client/src/generated/"

# Regenerate dapp-api-client from development environment
regenerate-dev:
	@echo "Regenerating dapp-api-client from development API (localhost:3000)..."
	cd crates/dapp-api-client && DAPP_ENV=development FORCE_SPEC_REGENERATE=true cargo build --features regenerate
	@echo "Client regenerated! Review changes with: git diff crates/dapp-api-client/src/generated/"

.PHONY: run-sidecar-host
run-sidecar-host:
	./scripts/run-sidecar-host.sh

.PHONY: down-sidecar-host
down-sidecar-host:
	docker compose -f docker/maru-besu-sidecar/docker-compose.yml down -v
