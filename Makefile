build:
	cargo build --verbose --release --locked

build-contracts:
	forge build --root testdata/mock-protocol 

# Clean up test containers
test-cleanup:
	@echo "Cleaning up test containers..."
	@docker ps -a --filter "ancestor=redis:5.0" --format "{{.ID}}" | xargs -r docker rm -f || true
	@docker ps -a --filter "ancestor=solc" --format "{{.ID}}" | xargs -r docker rm -f || true

# Run the rust tests for optimism (excluding state-worker packages which run separately)
test-optimism: build-contracts
	ASSERTION_DA_SOLC_DOCKER_PLATFORM=linux/amd64 cargo nextest run --workspace --exclude state-store --exclude state-worker --locked  --cargo-profile release --no-tests=warn --no-default-features --features optimism --features test

# Run the rust tests for default evm (excluding state-worker packages which run separately)
test-default: build-contracts
	ASSERTION_DA_SOLC_DOCKER_PLATFORM=linux/amd64 cargo nextest run --workspace --exclude state-store --exclude state-worker --locked  --cargo-profile release --no-tests=warn --no-default-features --features test

# Run state worker tests (single-threaded to avoid race conditions)
test-state-worker:
	cargo test --package state-store --package state-worker --locked --profile release --no-default-features -- --test-threads=1

make test: test-optimism test-default test-state-worker test-cleanup

# Run tests without full tests (skips Docker-dependent tests and integration tests)
test-no-full:
	./scripts/test-no-full.sh

# Validate formatting
format:
	cargo +nightly fmt --check

# Errors if there is a warning with clippy
lint:
	cargo +nightly clippy --all-targets --workspace   --profile dev -- -D warnings -D clippy::pedantic

# Fix linting errors
lint-fix:
	cargo +nightly clippy --all-targets --workspace   --profile dev --fix -- -D clippy::pedantic

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
