package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// The capture log is a write-only JSONL sink: one self-contained line per
// metered request, appended to a regular file or a FIFO. It exists next to the
// in-memory capture ring (captureBuffer, /api/captures/{id}) and never feeds
// it: nothing here reads back, nothing here changes what the ring, the
// activity row or the metrics record.
//
// It deliberately records three things the ring cannot:
//
//   - the response body of a non-200 upstream reply (the ring drops it, see
//     record()'s cf&^captureRespBody),
//   - a client that hung up part-way through a streamed (SSE) response, which
//     the activity row files as a plain 200 because the status had already
//     reached the client,
//   - optionally (includeAborted) the 499 client-closed requests that #1029
//     deliberately keeps out of the ring.

// captureLogOutcome classifies how a metered request ended.
type captureLogOutcome string

const (
	// outcomeOK is a 200 whose client stayed connected to the end.
	outcomeOK captureLogOutcome = "ok"
	// outcomeUpstreamError is any non-200, non-499 status.
	outcomeUpstreamError captureLogOutcome = "upstream_error"
	// outcomeClientDisconnected is the 499 sentinel: the client went away
	// before any response status was written.
	outcomeClientDisconnected captureLogOutcome = "client_disconnected"
	// outcomeClientDisconnectedMidStream is a response that started (so the
	// status is 200 and no 499 sentinel could be recorded) whose client
	// connection was cancelled before the handler returned. This is the case
	// an SSE stream cut short by the client lands in, and the one the
	// activity row cannot distinguish from a normal completion.
	outcomeClientDisconnectedMidStream captureLogOutcome = "client_disconnected_mid_stream"
)

const (
	// bodyEncodingUTF8 marks a body stored verbatim as a JSON string.
	bodyEncodingUTF8 = "utf8"
	// bodyEncodingBase64 marks a body that is not valid UTF-8 and therefore
	// cannot survive a JSON string round trip (Go replaces invalid bytes with
	// U+FFFD), so it is base64-encoded instead.
	bodyEncodingBase64 = "base64"
)

// bodyOmittedRoutePolicy is the reason recorded when captureFieldsByPath
// decides a route's body is not worth storing (the audio and image endpoints,
// whose bodies are megabytes of binary). More reasons are expected — a size
// cap, say — which is why this is a short identifier rather than a bool.
const bodyOmittedRoutePolicy = "route_policy"

// captureLogPayload is one half (request or response) of a log record.
type captureLogPayload struct {
	Headers map[string]string `json:"headers"`
	// Body is the body verbatim: the exact bytes, never re-serialized JSON.
	// Round-tripping through a JSON parser would reorder keys and normalize
	// whitespace, so the line would no longer be what went over the wire.
	//
	// It is null, and BodyOmitted set, when a body existed but was
	// deliberately not written. That is a different fact from an empty body,
	// so the two must not share a representation.
	Body         *string `json:"body"`
	BodyEncoding string  `json:"body_encoding,omitempty"`
	// BodyOmitted names why the body is absent; BodyBytes is how large it
	// was, or null when nothing ever measured it.
	BodyOmitted string `json:"body_omitted,omitempty"`
	BodyBytes   *int   `json:"body_bytes,omitempty"`
}

// setBody records body verbatim, picking the encoding that preserves its bytes.
func (p *captureLogPayload) setBody(body []byte) {
	text, encoding := encodeBody(body)
	p.Body = &text
	p.BodyEncoding = encoding
}

// setWireBody records body as it arrived, without pretending it is text. It is
// used for bytes llama-swap could not decode (a Content-Encoding it failed to
// decompress): base64 is the only honest rendering of them.
func (p *captureLogPayload) setWireBody(body []byte) {
	text := base64.StdEncoding.EncodeToString(body)
	p.Body = &text
	p.BodyEncoding = bodyEncodingBase64
}

// omitBody records that a body existed but was not written, and how big it
// was. size is nil when the size was never measured.
func (p *captureLogPayload) omitBody(reason string, size *int) {
	p.Body = nil
	p.BodyEncoding = ""
	p.BodyOmitted = reason
	p.BodyBytes = size
}

// captureLogTokens is the token count joined in from the activity row.
type captureLogTokens struct {
	Input  int `json:"input"`
	Output int `json:"output"`
}

// captureLogRecord is one JSONL line: the capture envelope joined with the
// activity row, so a consumer needs nothing else to interpret it.
type captureLogRecord struct {
	ID             int               `json:"id"`
	TS             string            `json:"ts"`
	Outcome        captureLogOutcome `json:"outcome"`
	Path           string            `json:"path"`
	RequestedModel string            `json:"requested_model"`
	UsedModel      string            `json:"used_model"`
	Status         int               `json:"status"`
	DurationMs     int               `json:"duration_ms"`
	Tokens         *captureLogTokens `json:"tokens"`
	Error          *string           `json:"error"`
	Req            captureLogPayload `json:"req"`
	Resp           captureLogPayload `json:"resp"`
}

// captureLogQueueDepth is how many encoded lines may wait for the writer
// goroutine. It absorbs a burst while a FIFO reader is slow or has not
// attached yet; beyond it lines are dropped rather than blocking a request.
const captureLogQueueDepth = 1024

// captureLogCloseTimeout bounds how long Shutdown waits for the writer to
// drain. A FIFO with no reader can leave the writer parked in open(2) forever,
// and shutdown must not inherit that wait.
const captureLogCloseTimeout = 3 * time.Second

// captureLogWriter appends encoded records to path. A single goroutine owns
// the file, which is what keeps concurrent requests from interleaving: every
// line is handed over whole and written by one writer in one Write call.
type captureLogWriter struct {
	path           string
	includeAborted bool
	logger         *logmon.Monitor

	lines chan []byte
	// quit is closed by Close. It is never nil. lines is deliberately never
	// closed, so a request racing with shutdown cannot send on a closed
	// channel.
	quit      chan struct{}
	done      chan struct{}
	closeOnce sync.Once

	dropped atomic.Int64
	// warned keeps a broken sink from filling the proxy log with one warning
	// per request.
	warned atomic.Bool
}

// newCaptureLogWriter starts a capture log writer, or returns nil when the
// sink is disabled or misconfigured (an enabled sink with no path is a
// configuration mistake; it is reported and disabled rather than failing
// startup, since the sink is an observability add-on).
func newCaptureLogWriter(cfg config.CaptureLogConfig, logger *logmon.Monitor) *captureLogWriter {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Path == "" {
		if logger != nil {
			logger.Warn("captureLog.enabled is set but captureLog.path is empty; capture log disabled")
		}
		return nil
	}
	w := &captureLogWriter{
		path:           cfg.Path,
		includeAborted: cfg.IncludeAborted,
		logger:         logger,
		lines:          make(chan []byte, captureLogQueueDepth),
		quit:           make(chan struct{}),
		done:           make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *captureLogWriter) warnf(format string, args ...any) {
	if w.logger != nil {
		w.logger.Warnf(format, args...)
	}
}

// write hands an encoded line to the writer goroutine. It never blocks and
// never fails a request: a sink that cannot keep up drops lines and says so.
func (w *captureLogWriter) write(line []byte) {
	if w == nil {
		return
	}
	select {
	case w.lines <- line:
	case <-w.quit:
	default:
		if n := w.dropped.Add(1); n == 1 || n%1000 == 0 {
			w.warnf("capture log %s: queue full, dropped %d record(s)", w.path, n)
		}
	}
}

// Close stops the writer and waits (briefly) for the queue to drain.
func (w *captureLogWriter) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() { close(w.quit) })
	select {
	case <-w.done:
	case <-time.After(captureLogCloseTimeout):
		// Reached when the writer is still parked in open(2) on a FIFO that
		// never got a reader. There is nothing to flush in that case — the
		// file was never opened — so shutdown continues.
		w.warnf("capture log %s: writer did not finish within %s, abandoning it", w.path, captureLogCloseTimeout)
	}
	return nil
}

// open opens the sink for appending.
//
// This runs on the writer goroutine, never on the startup path, and that is
// the whole point: opening a FIFO for writing blocks until a reader attaches
// (open(2), O_WRONLY on a FIFO). Opening it at startup would mean llama-swap
// could not start until someone was reading the log. Here the block is
// harmless — the server is already serving, and records queue in w.lines until
// the reader shows up.
//
// The open is blocking rather than O_NONBLOCK on purpose. A non-blocking FIFO
// fd makes every later write non-blocking too, and a non-blocking write larger
// than PIPE_BUF can write only part of a record, which would splice half a
// JSON line into the stream. Clearing O_NONBLOCK afterwards needs fcntl(2),
// which is not portable to the Windows build. Blocking writes from a single
// goroutine keep each record whole.
//
// There is deliberately no reopen: no SIGHUP handler, no retry after a write
// error. Log rotation is the external reader's job (point the sink at a FIFO
// and let the rotator read from it); a rotator that renames the file underneath
// us would silently write to the rotated inode, which is worse than not
// supporting rotation at all. If the FIFO's reader goes away the writes start
// failing with EPIPE and the sink stops until llama-swap is restarted.
func (w *captureLogWriter) open() (*os.File, error) {
	return os.OpenFile(w.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
}

func (w *captureLogWriter) run() {
	defer close(w.done)

	f, err := w.open()
	if err != nil {
		w.warnf("capture log %s: open failed: %v; capture log disabled for this run", w.path, err)
		// Keep draining so write() stays non-blocking and the queue does not
		// pin dropped records in memory.
		for {
			select {
			case <-w.lines:
			case <-w.quit:
				return
			}
		}
	}
	defer func() {
		if err := f.Close(); err != nil {
			w.warnf("capture log %s: close failed: %v", w.path, err)
		}
	}()

	emit := func(line []byte) {
		if _, err := f.Write(line); err != nil && w.warned.CompareAndSwap(false, true) {
			w.warnf("capture log %s: write failed: %v; further write errors are not reported", w.path, err)
		}
	}

	for {
		select {
		case line := <-w.lines:
			emit(line)
		case <-w.quit:
			// Drain whatever is already queued, then stop.
			for {
				select {
				case line := <-w.lines:
					emit(line)
				default:
					return
				}
			}
		}
	}
}

// captureLogEvent is everything the sink needs to know about one finished
// request. It is a struct rather than a parameter list because the pieces come
// from four different places (the activity row, the request, the recorder and
// record()'s local decoding) and several are easy to swap by accident.
type captureLogEvent struct {
	outcome  captureLogOutcome
	tm       ActivityLogEntry
	r        *http.Request
	recorder *responseBodyCopier
	// cf is the route's capture mask. The sink honors the bodies it drops:
	// the audio and image routes are excluded precisely because their bodies
	// are large binary blobs, and a JSONL line is a worse place for those than
	// the ring is. What the mask drops is still reported as dropped.
	cf         captureFields
	reqBody    []byte
	reqHeaders map[string]string
	// respBody is the response body to record, already decompressed unless
	// respIsWire says otherwise.
	respBody []byte
	// respIsWire marks respBody as the bytes exactly as they arrived, which
	// happens only when decompression failed. Content-Encoding then still
	// describes them, so it stays on the record, and they are recorded as
	// base64 because bytes that would not decompress are not text.
	respIsWire bool
}

// writeCaptureLog joins the capture envelope with the activity row and queues
// one JSONL line. It is called next to storeCapture, where every piece exists
// at once, but is deliberately not called from inside it: storeCapture returns
// early when captureBuffer is 0, does not see the activity row that carries
// the timestamp, model and tokens, and is handed a nil body on the failure
// path whose response the sink is supposed to keep.
//
// It never fails a request: encoding errors are logged and the line dropped.
func (mp *metricsMonitor) writeCaptureLog(ev captureLogEvent) {
	if mp.captureLog == nil {
		return
	}
	if ev.outcome == outcomeClientDisconnected && !mp.captureLog.includeAborted {
		return
	}

	tm := ev.tm
	rec := captureLogRecord{
		ID:         tm.ID,
		TS:         tm.Timestamp.Format(captureLogTimeFormat),
		Outcome:    ev.outcome,
		Path:       tm.ReqPath,
		UsedModel:  tm.Model,
		Status:     tm.RespStatusCode,
		DurationMs: tm.DurationMs,
	}
	if data, ok := swaputil.ReadContext(ev.r.Context()); ok {
		rec.RequestedModel = data.Model
	}
	if tm.ErrorMsg != "" {
		msg := tm.ErrorMsg
		rec.Error = &msg
	}
	// Tokens are only ever parsed on the 200 path, so anywhere else they
	// would be a zero value masquerading as a measurement. null says "not
	// measured"; {"input":0,"output":0} says "measured as zero".
	if ev.outcome == outcomeOK || ev.outcome == outcomeClientDisconnectedMidStream {
		rec.Tokens = &captureLogTokens{
			Input:  tm.Tokens.InputTokens,
			Output: tm.Tokens.OutputTokens,
		}
	}

	rec.Req.Headers = captureLogHeaders(ev.reqHeaders)
	if ev.cf&captureReqBody != 0 {
		rec.Req.setBody(ev.reqBody)
	} else {
		rec.Req.omitBody(bodyOmittedRoutePolicy, declaredRequestLength(ev.r))
	}

	if ev.outcome == outcomeClientDisconnected {
		// Nothing was ever sent, so there is no response half to report.
		rec.Resp.Headers = map[string]string{}
		rec.Resp.setBody(nil)
	} else {
		respHeaders := headerMap(ev.recorder.Header())
		redactHeaders(respHeaders)
		if !ev.respIsWire {
			// The body below is the decompressed form (record() decodes it
			// before parsing), so keeping Content-Encoding would describe the
			// wire bytes and mislead a consumer into inflating a plain
			// string. storeCapture drops it for the same reason.
			delete(respHeaders, "Content-Encoding")
		}
		rec.Resp.Headers = respHeaders
		switch {
		case ev.cf&captureRespBody == 0:
			n := len(ev.respBody)
			rec.Resp.omitBody(bodyOmittedRoutePolicy, &n)
		case ev.respIsWire:
			rec.Resp.setWireBody(ev.respBody)
		default:
			rec.Resp.setBody(ev.respBody)
		}
	}

	line, err := encodeCaptureLogRecord(&rec)
	if err != nil {
		mp.warnf("capture log: encoding record %d failed: %v", tm.ID, err)
		return
	}
	mp.captureLog.write(line)
}

// declaredRequestLength is the size to report for a request body the capture
// mask dropped. Unlike the response, it was never buffered — the middleware
// only reads the body when the mask allows it (metrics_middleware.go), and
// buffering a multi-megabyte upload just to measure it would defeat the point
// of the mask. The client's declared Content-Length is the size that is
// available; a request that declared none reports null.
func declaredRequestLength(r *http.Request) *int {
	if r == nil || r.ContentLength < 0 {
		return nil
	}
	n := int(r.ContentLength)
	return &n
}

// captureLogTimeFormat is RFC 3339 with milliseconds and the local offset.
const captureLogTimeFormat = "2006-01-02T15:04:05.000Z07:00"

// captureLogHeaders normalizes a possibly nil header map so the record always
// carries an object rather than null.
func captureLogHeaders(h map[string]string) map[string]string {
	if h == nil {
		return map[string]string{}
	}
	return h
}

// encodeBody renders body for a JSON string field, choosing the encoding that
// preserves the bytes exactly.
func encodeBody(body []byte) (string, string) {
	if len(body) == 0 {
		return "", bodyEncodingUTF8
	}
	if utf8.Valid(body) {
		return string(body), bodyEncodingUTF8
	}
	return base64.StdEncoding.EncodeToString(body), bodyEncodingBase64
}

// encodeCaptureLogRecord renders rec as one JSONL line (trailing newline
// included). HTML escaping is off so bodies read as they were sent; the bytes
// still round-trip exactly either way.
func encodeCaptureLogRecord(rec *captureLogRecord) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(rec); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
