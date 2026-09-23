---
title: Observability, storage and Activity
summary: Use logs, metrics, captures and the Activity view to diagnose requests and retain useful history.
category: guides
tags: [operations, logs, metrics, activity, captures]
config_keys: [logLevel, logToStdout, metricsMaxInMemory, captureBuffer, captureLog]
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

## Streaming every request to a file

`captureBuffer` keeps recent captures in memory for the Activity page, so old
requests fall out of the ring and failed responses keep only an error message.
When you need the full history instead, enable `captureLog`: it appends one
self-contained JSON object per metered request, independent of
`captureBuffer`.

```yaml
captureLog:
  enabled: true
  path: /var/run/llama-swap/captures.fifo
  includeAborted: false
```

Each line carries the request and response bodies verbatim plus the activity
row's timestamp, models, status, duration and token counts, and an `outcome` of
`ok`, `upstream_error`, `client_disconnected` or
`client_disconnected_mid_stream`. A body that is not valid UTF-8 is base64 and
says so in `body_encoding`.

The audio and image endpoints send bodies that are megabytes of binary, and
llama-swap does not store those in the capture ring either. Their lines carry
`"body": null` with `body_omitted: "route_policy"` and `body_bytes`, so a line
never looks like a request that had no body.

`path` may be a regular file or a FIFO. llama-swap opens it once and never
reopens it, so rotating the file underneath it silently writes to the rotated
inode: point it at a FIFO and let the reader on the other end rotate or ship
the stream. Set `includeAborted: true` to also record requests the client
abandoned before a response started (HTTP 499); those lines carry the request
only.

The sink holds full request and response bodies, so it inherits the warning
above with more force: write it somewhere only operators can read, and turn it
off when you are done.
