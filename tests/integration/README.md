# Integration Tests

These tests spin up a real Postgres + PGMQ instance using Docker and run the `pgmq` CLI against it.

## Prerequisites

- Docker Desktop (or Docker Engine) running
- Go 1.26+

## Run

From the repo root:

```sh
make test-integration
```

## Notes

- Tests build a temporary `pgmq` binary and run it against a containerized database.
- Configuration is written to a temporary `$HOME/.pgmq/config.json` for each test.
- The default test image is pinned to `ghcr.io/pgmq/pg17-pgmq:v1.11.1`.
- Override the image with `PGMQ_TEST_IMAGE` if you need a different version.
- Topic routing tests detect `pgmq.bind_topic(text,text)` at runtime and skip automatically when the feature is unavailable.
- FIFO grouped read tests detect FIFO functions at runtime and skip automatically when the feature is unavailable.
- CI runs integration tests against `v1.7.0`, `v1.8.1`, `v1.9.0`, `v1.10.0`, `v1.11.0`, and `v1.11.1`.
