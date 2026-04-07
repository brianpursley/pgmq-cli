# pgmq-cli

A command-line tool to manage [PGMQ (Postgres Message Queue)](https://github.com/pgmq/pgmq) in PostgreSQL.

## Features

- Initialize PGMQ extension
- Create, list, and drop queues
- Send and read messages
- Topic routing with bind, unbind, list, test, and send commands
- Archive, delete, and purge messages
- Table or JSON output
- Config-based server selection

## Requirements

- Postgres with the PGMQ extension available
- Queue management and queue message commands require `pgmq` 1.7.0 or later
- Topic routing commands require `pgmq` 1.11.0 or later

## Installation

### Prebuilt binaries

Download the latest release for your platform from the [releases page](https://github.com/brianpursley/pgmq-cli/releases).

*(If you encounter problems running the downloaded binary check out the [Frequently Asked Questions section](#frequently-asked-questions) below)*

### Build from source

```sh
make
```

### Install from source to your GOPATH

```sh
make install
```

## Shell Completion

Generate completion scripts with:

```sh
pgmq completion bash
pgmq completion zsh
pgmq completion fish
pgmq completion powershell
```

Examples:

```sh
# Bash (current shell)
source <(pgmq completion bash)

# Zsh (current shell)
source <(pgmq completion zsh)

# Fish
pgmq completion fish > ~/.config/fish/completions/pgmq.fish
```

## Configuration

Create a config file at `~/.pgmq/config.json`:

```json
{
  "defaultServer": "DevServer",
  "servers": {
    "DevServer": {
      "connectionString": "Host=localhost;Username=postgres;Password=postgres;Database=pgmq;"
    }
  }
}
```

- `defaultServer` is optional. If omitted, you must pass `--server` on every command.
- `connectionString` can be a standard Postgres connection string.

## Global Flags

- `--server` / `-s`: Server name (overrides `defaultServer`)
- `-Y` / `--yes`: Skip confirmation prompts for destructive commands
- `--config`: Config file path (overrides default)

## Commands

### `pgmq server`

Manage servers in `~/.pgmq/config.json`.

```sh
pgmq server add DevServer "Host=localhost;Username=postgres;Password=postgres;Database=pgmq;"
pgmq server update DevServer "Host=localhost;Username=postgres;Password=postgres;Database=pgmq;"
pgmq server remove DevServer
pgmq server list
pgmq server get DevServer
pgmq server get-default
pgmq server set-default DevServer
pgmq server unset-default
```

### `pgmq init`

Initialize the PGMQ extension if needed.

```sh
pgmq init
pgmq init --check
```

### `pgmq create`

Create a queue.

```sh
pgmq create MyQueue
pgmq create MyQueue --logged=false
```

### `pgmq list`

List all queues.

```sh
pgmq list
```

### `pgmq metrics`

Get metrics for a queue, or for all queues if no queue is specified.

```sh
pgmq metrics MyQueue
pgmq metrics
```

### `pgmq send`

Send a message to a queue.

```sh
pgmq send MyQueue '{"foo":"bar"}'
pgmq send MyQueue '{"foo":"bar"}' --headers '{"x-pgmq-group":"user123"}'
pgmq send MyQueue '{"foo":"bar"}' --delay 5
pgmq send MyQueue '{"foo":"bar"}' --delay-until 2025-01-01T12:00:00Z
```

### `pgmq topic`

Manage topic routing. These commands require `pgmq` 1.11.0 or later.

Topic patterns use dot-separated segments:

- `*` matches exactly one segment
- `#` matches zero or more segments

Examples:

```sh
pgmq topic bind 'logs.#' all_logs
pgmq topic bind 'logs.*.error' error_logs
pgmq topic bind 'logs.api.error' api_errors

pgmq topic list
pgmq topic list api_errors

pgmq topic test 'logs.api.error'

pgmq topic send 'logs.api.error' '{"message":"API failed"}'
pgmq topic send 'logs.api.error' '{"message":"API failed"}' --headers '{"priority":"high"}'
pgmq topic send 'logs.api.error' '{"message":"API failed"}' --delay 5
pgmq topic send 'logs.api.error' '{"message":"API failed"}' --delay-until 2025-01-01T12:00:00Z

pgmq topic unbind 'logs.api.error' api_errors
```

Notes:

- `pgmq topic list`, `pgmq topic test`, and `pgmq topic send` support `-o json`
- `pgmq topic send -o json` returns an array of `{queue_name, msg_id}` objects
- If a routing key matches no bindings, `pgmq topic send -o json` returns `[]`

### `pgmq read`

Read messages with visibility timeout.

```sh
pgmq read MyQueue --qty 5
```

### `pgmq pop`

Read and delete messages.

```sh
pgmq pop MyQueue --qty 1
```

Use `-o message` to output only the raw message JSON.

### `pgmq delete`

Delete messages by ID.

```sh
pgmq delete MyQueue 42
pgmq delete MyQueue 42 43 44
```

### `pgmq archive`

Archive messages by ID.

```sh
pgmq archive MyQueue 42
pgmq archive MyQueue 42 43 44
```

### `pgmq purge`

Delete all messages in a queue.

```sh
pgmq purge MyQueue --yes
```

If the queue is empty, the command prints `queue is empty`.

### `pgmq drop`

Drop a queue and its archive.

```sh
pgmq drop MyQueue --yes
```

### `pgmq version`

Print the binary version.

```sh
pgmq version
```

## Output Formats

Some commands support different output formats, which can be selected via the `--output` or `-o` flag.

### Table Output (default)

Commands render a simple human-readable table when the output is tabular.

### JSON Output

Use `-o json` to return the output in JSON format.

### Message Output

The `pgmq pop` command supports `-o message` to output only the raw message JSON. If multiple messages are requested, a JSON array is returned.

## Exit Codes

- `0`: Success
- `1`: Server/SQL error
- `2`: Invalid arguments / usage
- `3`: Not found (e.g., queue missing)

## Frequently Asked Questions

### Why do I get an error saying `"pgmq" cannot be opened because the developer cannot be verified` on macOS?

This is a common issue with unsigned binaries on macOS. To bypass this, you can remove the quarantine attribute from the binary:

```sh
xattr -d com.apple.quarantine pgmq
```
