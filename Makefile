build:
	cargo build --verbose --release --locked

build-contracts:
	forge build --root testdata/mock-protocol 

# Run the rust tests for linea
test-linea: build-contracts
	cargo nextest run --workspace --locked  --cargo-profile release --no-tests=warn --no-default-features --features linea --features test

# Run the rust tests for optimism
test-optimism: build-contracts
	cargo nextest run --workspace --locked  --cargo-profile release --no-tests=warn --no-default-features --features optimism --features test

# Run the rust tests for default evm
test-default: build-contracts
	cargo nextest run --workspace --locked  --cargo-profile release --no-tests=warn --no-default-features --features test

make test: test-linea test-optimism test-default

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
