---
title: Observability, storage and Activity
summary: Use logs, metrics, captures and the Activity view to diagnose requests and retain useful history.
category: guides
tags: [operations, logs, metrics, activity, captures]
config_keys: [logLevel, logToStdout, metricsMaxInMemory, captureBuffer, trace]
updated: 2026-09-23
---

# Observability, storage and Activity

Use the Activity view for recent request timing and model events, logs for
process and proxy failures, and metrics for trends. `metricsMaxInMemory` and
`captureBuffer` bound retained in-memory data; increase them only when the
memory cost is acceptable.

```yaml
logLevel: debug
logToStdout: true
metricsMaxInMemory: 1000
captureBuffer: 100
```

Do not put secrets in captures or debug logs. Reduce retention after diagnosing
an issue.

## Recording every request to disk

`captureBuffer` keeps recent captures in memory for the Activity page, so old
requests fall out of the ring and failed responses keep only an error message.
When you need the full history instead, enable `trace`: it writes one
self-contained JSON object per metered request into a directory of
zstd-compressed files.

```yaml
trace:
  enabled: true
  dir: /var/log/llama-swap/trace
  maxFileBytes: 268435456
  level: 3
  includeAborted: false
```

Every line starts with a `type` naming the kind of record and a `v` giving the
format version, so a consumer can skip a kind it does not know.

A `request` line carries the request and response bodies verbatim plus the
request line (`method`, `path` with its query string, `remote_ip`) and the
activity row's timestamp, models, status, duration and token counts, and an
`outcome` of `ok`, `upstream_error`, `client_disconnected` or
`client_disconnected_mid_stream`. A body that is not valid UTF-8 is base64 and
says so in `body_encoding`.

The audio and image endpoints send bodies that are megabytes of binary, and
llama-swap does not store those in the capture ring either. Their lines carry
`"body": null` with `body_omitted: "route_policy"` and `body_bytes`, so a line
never looks like a request that had no body.

`dir` is created if missing. Files are named
`trace-<timestamp>.jsonl.zst`, the name is fixed when the file is created,
and llama-swap never renames or reopens one: a file that is no longer the
newest is finished and safe to copy away. Each file is a complete zstd stream
on its own, so `zstd -d trace-20260923T123000+0900.jsonl.zst` works without
the rest of the directory, and the names sort in the order they were written,
so `zstd -dc trace-*.jsonl.zst` replays the whole history in order.

`maxFileBytes` is measured on compressed bytes, checked after each record, and
is not a hard limit: a record is never split across files, so a file grows to
`maxFileBytes` plus its last record. `level` is the zstd compression level in
`zstd(1)`'s numbering; omit it for the default. llama-swap never deletes old
files — retention is yours to run, by copying them elsewhere or removing them.

Set `includeAborted: true` to also record requests the client abandoned before
a response started (HTTP 499); those lines carry the request only.

## Recording which backend answered

A request line does not say which process served it or how that process was
started, so on its own it cannot be used to reproduce an inference. Three
records under `trace.state` add that, and each is off by default.

```yaml
trace:
  enabled: true
  dir: /var/log/llama-swap/trace
  state:
    backend: true
    config: true
    checkpoint: true
```

**Turning any of these on writes the expanded `cmd`, the `env` and — for
`checkpoint` — the whole effective configuration into the files.** That is
what makes the trace reproducible, and it is also why the switches exist
separately from `enabled`. Use `maskPaths` and `maskEnv` below before enabling
them on a host whose configuration carries credentials.

- `backend` writes a `type: backend` line for every process state transition
  (starting, ready, stopping, stopped, shutdown), with that process's expanded
  command, environment, resolved upstream and start time. The start time is
  observed from the transition into `starting`, so a process that was already
  running reports `started_at: null`.
- `config` writes a `type: config` line at each configuration reload
  boundary. llama-swap reloads its configuration without restarting, so
  without these markers lines written before a reload would be read under
  settings that no longer applied to them.
- `checkpoint` writes a `type: checkpoint` line as the first line of every
  file: the llama-swap build, the active profile, every running process with
  its expanded command, and the effective configuration whole. Rotation cuts
  the stream, so without it a file that is not the first one cannot be read on
  its own.

## Masking fields

The sink records what it has; `maskPaths` and `maskEnv` take parts back out.
Both are empty by default, which masks nothing.

```yaml
trace:
  maskPaths:
    - backend.cmd
    - req.headers.Authorization
    - checkpoint.config.models.qwen3.env
  maskEnv:
    - OPENAI_API_KEY
```

`maskPaths` takes JSON paths into a record and replaces the value at each with
`[REDACTED]`. A path that a record does not have is left alone rather than
created. `maskEnv` names environment variables instead of paths, because an
environment is a list of `NAME=value` strings that no path can select into; a
listed name keeps its name and loses its value, wherever it appears.

Two limits are worth planning around:

- **Bodies cannot be masked.** `req.body` and `resp.body` are the bytes that
  went over the wire, kept verbatim, which is the whole premise of the format.
  A `maskPaths` entry naming a body — or naming `req` or `resp`, which would
  remove one — is refused at startup with a warning in the proxy log. A secret
  in a request body stays in the file.
- **Masking is fail-open.** It is a list of what to hide, not a list of what
  to allow. A header, a command-line flag or a configuration key nobody listed
  is written as it is.

The sink holds full request and response bodies, so it inherits the warning
above with more force: write it somewhere only operators can read, and turn it
off when you are done.
