# Repository Guidelines

## Project Structure & Module Organization

Dragonboat is a multi-group Raft library using module path `github.com/neo-f/dragonboat/v4`. Root Go files implement NodeHost, nodes, requests, snapshots, and execution engines. Public packages include `client/`, `config/`, `statemachine/`, `raftio/`, `raftpb/`, and `logger/`. Implementation details live in `internal/`, including Raft consensus, log storage, transport, and state machine management. Storage and transport adapters live in `plugin/`; utilities live in `tools/`.

Tests sit beside source files as `*_test.go`, with integration helpers and tests in `internal/tests/`. Documentation and images live in `docs/`. `examples/README.md` links to the separate examples repository; there is no root application to run.

## Build, Test, and Development Commands

Use Go 1.27 or newer. Run commands from the repository root:

- `go build ./...`: compile all packages.
- `make tools`: build the `checkdisk` utility.
- `make test`: run the full Makefile test suite without cached results.
- `make test-raft TEST_TO_RUN=TestName`: run matching consensus tests.
- `RACE=1 make test`: run tests with race detection.
- `MEMFS_TEST=1 make test`: exercise the in-memory filesystem configuration.
- `make actions-test`: run the CI test selection and generate `coverage.txt`.
- `make install-static-check-tools && make static-check`: install the pinned golangci-lint version and run configured checks. `.golangci.yml` is the source of truth for the lint configuration (linter set, settings and exclusions); do not pass linter flags on the command line.

## Coding Style & Naming Conventions

Format Go changes with `gofmt`; use its standard tab indentation. Follow existing package conventions: lowercase package names, exported `CamelCase` identifiers, and unexported `camelCase` identifiers. Use the fork's `/v4` module path for internal imports. Document exported APIs and preserve applicable license headers.

## Testing Guidelines

Use Go's `testing` package and existing Testify assertions. Name tests `TestXxx` and benchmarks `BenchmarkXxx`. Add automated coverage for behavior changes and regression tests for fixes. Isolate filesystem state with `t.TempDir()` or `b.TempDir()`. Run relevant package tests during development and the full suite before submitting. Codecov reports coverage, but its configuration disables coverage status gates; no numeric minimum is enforced.

## Commit & Pull Request Guidelines

Follow the history's short, prefixed commit subjects, such as `refactor:`, `docs:`, `ci:`, `test:`, or subsystem prefixes such as `raft:`. Keep commits focused.

Read `CONTRIBUTING.md` and `LICENSE`. Discuss substantial proposals in an issue or draft PR. Describe the change, link related issues, and report validation commands and results. For bugs, include reproduction steps and useful execution logs.
