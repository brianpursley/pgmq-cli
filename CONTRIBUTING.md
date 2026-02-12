# Contributing

## Prerequisites

- Go 1.26+
- Docker (for integration tests)

## Build

```sh
make
```

This produces the `bin/pgmq` binary.

## Test

### Unit tests
```sh
make test-unit
```

### Integration tests (requires Docker)
```sh
make test-integration
```

### All tests (Unit + Integration)
```sh
make test
```

## Security Checks

Run vulnerability scanning with:

```sh
make govulncheck
```

This uses `govulncheck` to detect known vulnerabilities in reachable code paths.

## Release Process

1. Ensure `main` is green:
```sh
make test
make govulncheck
```

2. Validate release artifacts locally (snapshot only, no GitHub release publish):
```sh
make release-snapshot
```

3. Inspect generated artifacts in `dist/` and smoke test one binary:
```sh
./dist/<artifact-dir>/pgmq version
./dist/<artifact-dir>/pgmq --help
```

4. Create and push a semantic-version tag (this triggers the release workflow):
```sh
git tag vX.Y.Z
git push origin vX.Y.Z
```

5. Confirm GitHub Actions release job completes and the GitHub Release contains:
- Platform archives
- `checksums.txt`
