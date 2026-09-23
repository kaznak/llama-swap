package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// The capture log is a write-only JSONL sink: one self-contained line per
// metered request, zstd-compressed into size-rotated files in a directory. It
// exists next to the in-memory capture ring (captureBuffer,
// /api/captures/{id}) and never feeds it: nothing here reads back, nothing
// here changes what the ring, the activity row or the metrics record.
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
// goroutine. It absorbs a burst while the writer is compressing or rotating;
// beyond it lines are dropped rather than blocking a request.
const captureLogQueueDepth = 1024

// captureLogDefaultMaxFileBytes is the rotation threshold used when the
// configuration gives none: 256 MiB of compressed output.
const captureLogDefaultMaxFileBytes int64 = 256 << 20

// captureLogFileTimeFormat stamps a file name with the local time the file was
// opened. Basic ISO 8601, so the name needs no quoting in a shell.
const captureLogFileTimeFormat = "20060102T150405Z0700"

const (
	captureLogFilePrefix = "captures-"
	captureLogFileSuffix = ".jsonl.zst"
)

// captureLogMaxNameSeq bounds the search for a free name within one second.
// It is also the point at which the zero-padded suffix would grow a digit and
// stop sorting in open order, and reaching it means more than a thousand files
// were rotated inside one second, which is a misconfiguration rather than
// something to spin on.
const captureLogMaxNameSeq = 1000

// countingWriter counts the bytes that reach the file underneath the encoder.
// The rotation threshold is measured on compressed output, and only the bytes
// leaving the encoder say how large the file actually is, so the count is
// taken here rather than with stat(2).
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// captureLogFile is one output file: a zstd stream over a regular file, plus
// the number of compressed bytes already in it.
//
// Each file is a complete, independent zstd stream. It gets its own encoder
// and is finished with close(), which writes the frame epilogue. Nothing is
// shared across files — no trained dictionary, no delta against the previous
// file — so any one file decompresses on its own (`zstd -d <file>`) without
// the rest of the directory being present.
type captureLogFile struct {
	name    string
	f       *os.File
	counter *countingWriter
	enc     *zstd.Encoder
}

// openCaptureLogFile creates the next file in dir. The name is fixed when the
// file is created and never changes: there is no "current" symlink and no
// rename on rotation, so a copier that picks up a finished file is never
// looking at a path whose meaning changed underneath it.
//
// now is normally time.Now. Two files opened within the same second would
// collide, so the later one takes a numeric suffix; O_EXCL makes that check
// atomic instead of a stat-then-create race. The suffix is "_" plus a
// zero-padded count, chosen so that a plain name still sorts before its own
// collision suffixes ('_' > '.') and the suffixes sort among themselves: the
// names in the directory are therefore in the order they were opened, which is
// the order their records were written.
//
// level is the zstd compression level in zstd(1)'s numbering; 0 means "not
// configured" and leaves the klauspost default in place.
func openCaptureLogFile(dir string, level int, now time.Time) (*captureLogFile, error) {
	stamp := now.Format(captureLogFileTimeFormat)
	var f *os.File
	var name string
	for seq := 0; ; seq++ {
		if seq >= captureLogMaxNameSeq {
			return nil, fmt.Errorf("no free name for %s%s* in %s", captureLogFilePrefix, stamp, dir)
		}
		if seq == 0 {
			name = captureLogFilePrefix + stamp + captureLogFileSuffix
		} else {
			name = fmt.Sprintf("%s%s_%03d%s", captureLogFilePrefix, stamp, seq, captureLogFileSuffix)
		}
		var err error
		f, err = os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			break
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
	}

	// One writer goroutine owns this encoder and the measured throughput of
	// the sink is orders of magnitude below what a single zstd thread
	// sustains, so the encoder is given exactly one. The library's default is
	// GOMAXPROCS, which would start a pool of block workers per open file for
	// no gain.
	opts := []zstd.EOption{zstd.WithEncoderConcurrency(1)}
	if level != 0 {
		opts = append(opts, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	}
	counter := &countingWriter{w: f}
	enc, err := zstd.NewWriter(counter, opts...)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &captureLogFile{name: name, f: f, counter: counter, enc: enc}, nil
}

// write appends one encoded line and flushes it. The flush is per record on
// purpose, for two reasons. A process that dies without closing the frame
// still leaves every already-written record decompressible. And the byte count
// stays exact: an unflushed record would sit inside the encoder and make the
// file look smaller than it is when the rotation threshold is tested. A record
// is hundreds of kilobytes, so what a flush boundary costs in ratio is noise.
func (c *captureLogFile) write(line []byte) error {
	if _, err := c.enc.Write(line); err != nil {
		return err
	}
	return c.enc.Flush()
}

// bytes is the compressed size on disk, counting everything flushed so far.
func (c *captureLogFile) bytes() int64 {
	return c.counter.n
}

// close finishes the zstd frame and closes the file. Until this has run the
// file is a truncated stream — readable up to the last flush, but without the
// epilogue — which is why graceful shutdown has to reach it.
func (c *captureLogFile) close() error {
	encErr := c.enc.Close()
	fErr := c.f.Close()
	if encErr != nil {
		return encErr
	}
	return fErr
}

// captureLogWriter appends encoded records to rotating files in dir. A single
// goroutine owns the open file and its encoder, which is what keeps concurrent
// requests from interleaving: every line is handed over whole and written by
// that one writer.
type captureLogWriter struct {
	dir string
	// maxFileBytes is the rotation threshold, tested on compressed bytes after
	// a record is flushed. It is not a hard cap: records are never split, so a
	// file grows to the threshold plus one last record.
	maxFileBytes   int64
	level          int
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
// sink is disabled or misconfigured (an enabled sink with no directory, or a
// directory that cannot be created, is a configuration mistake; it is reported
// and disabled rather than failing startup, since the sink is an
// observability add-on).
//
// The directory is created here, on the startup path, so a bad setting is
// reported immediately. The files inside it are opened lazily by the writer
// goroutine, so an enabled sink that never sees a request leaves no empty
// stream behind.
func newCaptureLogWriter(cfg config.CaptureLogConfig, logger *logmon.Monitor) *captureLogWriter {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Dir == "" {
		if logger != nil {
			logger.Warn("captureLog.enabled is set but captureLog.dir is empty; capture log disabled")
		}
		return nil
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		if logger != nil {
			logger.Warnf("capture log %s: creating the directory failed: %v; capture log disabled", cfg.Dir, err)
		}
		return nil
	}
	maxFileBytes := cfg.MaxFileBytes
	if maxFileBytes <= 0 {
		maxFileBytes = captureLogDefaultMaxFileBytes
	}
	w := &captureLogWriter{
		dir:            cfg.Dir,
		maxFileBytes:   maxFileBytes,
		level:          cfg.Level,
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
			w.warnf("capture log %s: queue full, dropped %d record(s)", w.dir, n)
		}
	}
}

// Close stops the writer and waits for it to drain the queue and finish the
// zstd frame of whatever file it has open. The wait is deliberately unbounded:
// closing the frame is the only thing that turns the newest file into a
// complete stream, and the writer never blocks on anything but writes to a
// regular file.
func (w *captureLogWriter) Close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() { close(w.quit) })
	<-w.done
	return nil
}

// run owns every file and encoder the sink opens; nothing else touches them.
// That sole ownership is what keeps concurrent requests from interleaving — a
// record is encoded by the caller, handed over whole, and written here.
//
// Rotation is by size, measured on compressed bytes after each record is
// flushed, and it closes the frame before starting the next file. A record is
// never split across files, so a file ends up at the threshold plus its last
// record.
//
// There is no reopen of a file once it is closed and no reuse of a name: a
// finished file is finished, which is what makes it safe for an external
// copier to pick up. Deletion of old files is deliberately not implemented —
// retention belongs to whatever copies them away.
func (w *captureLogWriter) run() {
	defer close(w.done)

	var cur *captureLogFile
	// broken latches when a file cannot be opened at all. The loop keeps
	// draining afterwards so write() stays non-blocking and the queue does not
	// pin dropped records in memory, but nothing is retried: a directory that
	// cannot be written to will not start working mid-run, and retrying per
	// record would fill the proxy log.
	broken := false
	defer func() {
		if cur != nil {
			if err := cur.close(); err != nil {
				w.warnf("capture log %s: closing %s failed: %v", w.dir, cur.name, err)
			}
		}
	}()

	emit := func(line []byte) {
		if broken {
			return
		}
		if cur == nil {
			f, err := openCaptureLogFile(w.dir, w.level, time.Now())
			if err != nil {
				w.warnf("capture log %s: open failed: %v; capture log disabled for this run", w.dir, err)
				broken = true
				return
			}
			cur = f
		}
		if err := cur.write(line); err != nil && w.warned.CompareAndSwap(false, true) {
			w.warnf("capture log %s: writing %s failed: %v; further write errors are not reported", w.dir, cur.name, err)
		}
		if cur.bytes() >= w.maxFileBytes {
			if err := cur.close(); err != nil {
				w.warnf("capture log %s: closing %s failed: %v", w.dir, cur.name, err)
			}
			cur = nil
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
