package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// captureLogSink builds a metrics monitor whose capture ring is off
// (captureBuffer 0) and whose JSONL sink writes into a temp directory, proving
// the sink does not depend on the ring. Returns the monitor and the sink
// directory.
func captureLogSink(t *testing.T, cfg config.CaptureLogConfig) (*metricsMonitor, string) {
	t.Helper()
	return captureLogSinkWithState(t, cfg, nil)
}

// captureLogSinkWithState is captureLogSink with the process-management seam
// supplied, which is what turns the trace and checkpoint records on. A nil
// state leaves them off however captureLog.trace is set.
func captureLogSinkWithState(t *testing.T, cfg config.CaptureLogConfig, state captureLogStateFunc) (*metricsMonitor, string) {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = filepath.Join(t.TempDir(), "captures")
	}
	mm := newTestMetricsMonitor(t, logmon.NewWriter(io.Discard), 10, 0)
	mm.attachCaptureLog(cfg, state)
	t.Cleanup(func() {
		if err := mm.Close(); err != nil {
			t.Errorf("metricsMonitor.Close: %v", err)
		}
	})
	return mm, cfg.Dir
}

// captureLogFiles lists the sink's files in the order they were opened. The
// names sort that way by construction (see openCaptureLogFile), which is what
// lets a consumer concatenate them.
func captureLogFiles(t *testing.T, dir string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, captureLogFilePrefix+"*"+captureLogFileSuffix))
	if err != nil {
		t.Fatalf("listing capture log files: %v", err)
	}
	sort.Strings(names)
	return names
}

// decodeCaptureLogFile decompresses one file on its own, with a decoder that
// has never seen any other file. That independence is the point: no shared
// dictionary and no delta chain, so a single file is enough to read the
// records in it.
func decodeCaptureLogFile(t *testing.T, name string) []byte {
	t.Helper()
	f, err := os.Open(name)
	if err != nil {
		t.Fatalf("opening %s: %v", name, err)
	}
	defer f.Close()
	dec, err := zstd.NewReader(f)
	if err != nil {
		t.Fatalf("zstd reader for %s: %v", name, err)
	}
	defer dec.Close()
	data, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("decompressing %s: %v", name, err)
	}
	return data
}

// readCaptureLogBytes closes the sink (finishing the last frame) and returns
// the concatenation of every file's decompressed contents.
func readCaptureLogBytes(t *testing.T, mm *metricsMonitor, dir string) []byte {
	t.Helper()
	if err := mm.Close(); err != nil {
		t.Fatalf("metricsMonitor.Close: %v", err)
	}
	var out []byte
	for _, name := range captureLogFiles(t, dir) {
		out = append(out, decodeCaptureLogFile(t, name)...)
	}
	return out
}

// parseCaptureLogLines decodes JSONL text into records.
func parseCaptureLogLines(t *testing.T, data []byte) []captureLogRecord {
	t.Helper()
	var out []captureLogRecord
	for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		if line == "" {
			continue
		}
		var rec captureLogRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("capture log line is not valid JSON: %v\nline: %q", err, line)
		}
		out = append(out, rec)
	}
	return out
}

// readCaptureLog closes the sink (flushing it) and returns the decoded lines.
func readCaptureLog(t *testing.T, mm *metricsMonitor, dir string) []captureLogRecord {
	t.Helper()
	return parseCaptureLogLines(t, readCaptureLogBytes(t, mm, dir))
}

// body is the payload's body as text. A payload whose body was omitted has
// none at all; the tests that care about that check Body and BodyOmitted
// directly rather than going through here.
func (p captureLogPayload) body() string {
	if p.Body == nil {
		return ""
	}
	return *p.Body
}

// postRequest is a metered POST carrying reqBody, with the model context the
// middleware would have installed.
func postRequest(path, model string, reqBody []byte) *http.Request {
	r := httptest.NewRequest(http.MethodPost, path, nil)
	ctx := swaputil.SetContext(r.Context(), swaputil.ReqContextData{Model: model, ModelID: model})
	return r.WithContext(ctx)
}

// respond writes a complete response through a body copier, the way an
// upstream handler would.
func respond(t *testing.T, status int, contentType string, body []byte) *responseBodyCopier {
	t.Helper()
	copier := newBodyCopier(httptest.NewRecorder())
	copier.Header().Set("Content-Type", contentType)
	copier.WriteHeader(status)
	if _, err := copier.Write(body); err != nil {
		t.Fatalf("writing response: %v", err)
	}
	return copier
}

// TestCaptureLog_SuccessIsByteExact is the sink's core promise: the bodies on
// the line are the bytes that went over the wire, not a re-serialization. The
// request body here has key order and whitespace a JSON round trip would
// destroy.
func TestCaptureLog_SuccessIsByteExact(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	reqBody := []byte("{\"model\":\"m\",\n  \"z_first\":1,   \"a_second\":2,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}")
	respBody := []byte(`{"usage":{"prompt_tokens":12,"completion_tokens":7},"choices":[{"text":"ok"}]}`)

	r := postRequest("/v1/chat/completions", "alias-m", reqBody)
	copier := respond(t, http.StatusOK, "application/json", respBody)
	mm.record("real-m", r, copier, captureAll, reqBody, map[string]string{"Content-Type": "application/json"})

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	rec := recs[0]
	if rec.Outcome != outcomeOK {
		t.Errorf("outcome = %q, want %q", rec.Outcome, outcomeOK)
	}
	if rec.Req.BodyEncoding != bodyEncodingUTF8 || rec.Req.body() != string(reqBody) {
		t.Errorf("req body = %q (%s), want byte-identical %q", rec.Req.body(), rec.Req.BodyEncoding, reqBody)
	}
	if rec.Resp.BodyEncoding != bodyEncodingUTF8 || rec.Resp.body() != string(respBody) {
		t.Errorf("resp body = %q (%s), want byte-identical %q", rec.Resp.body(), rec.Resp.BodyEncoding, respBody)
	}
	if rec.Status != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Status)
	}
	if rec.Path != "/v1/chat/completions" {
		t.Errorf("path = %q", rec.Path)
	}
	// The join with the activity row: neither model nor the token counts are
	// in the capture envelope.
	if rec.RequestedModel != "alias-m" || rec.UsedModel != "real-m" {
		t.Errorf("models = (%q, %q), want (alias-m, real-m)", rec.RequestedModel, rec.UsedModel)
	}
	if rec.Tokens == nil || rec.Tokens.Input != 12 || rec.Tokens.Output != 7 {
		t.Errorf("tokens = %+v, want input 12 output 7", rec.Tokens)
	}
	if rec.ID == 0 || rec.TS == "" {
		t.Errorf("id = %d, ts = %q: both come from the activity row", rec.ID, rec.TS)
	}
	if rec.Error != nil {
		t.Errorf("error = %q, want null", *rec.Error)
	}
	if enc := rec.Resp.Headers["Content-Type"]; enc != "application/json" {
		t.Errorf("resp Content-Type = %q", enc)
	}
}

// TestCaptureLog_StreamingKeepsWholeEventStream checks an SSE response lands
// on the line in full — every data: line, including the ones with no usage
// block, and the terminating [DONE].
func TestCaptureLog_StreamingKeepsWholeEventStream(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	respBody := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"He\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n" +
		"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n" +
		"data: [DONE]\n\n")

	r := postRequest("/v1/chat/completions", "m", nil)
	copier := respond(t, http.StatusOK, "text/event-stream", respBody)
	mm.record("m", r, copier, captureAll, []byte(`{"stream":true}`), nil)

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	rec := recs[0]
	if rec.Resp.body() != string(respBody) {
		t.Errorf("resp body = %q, want the whole stream %q", rec.Resp.body(), respBody)
	}
	for _, want := range []string{`"content":"He"`, `"content":"llo"`, "data: [DONE]"} {
		if !strings.Contains(rec.Resp.body(), want) {
			t.Errorf("resp body is missing %q", want)
		}
	}
	if rec.Outcome != outcomeOK {
		t.Errorf("outcome = %q, want %q", rec.Outcome, outcomeOK)
	}
	if rec.Tokens == nil || rec.Tokens.Input != 5 || rec.Tokens.Output != 2 {
		t.Errorf("tokens = %+v, want the stream's usage block", rec.Tokens)
	}
}

// TestCaptureLog_UpstreamErrorKeepsResponseBody covers the first hole the sink
// exists to plug: record() strips the response body from a failed request's
// capture (cf&^captureRespBody), so the ring can only show ErrorMsg. The JSONL
// line keeps the body.
func TestCaptureLog_UpstreamErrorKeepsResponseBody(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	respBody := []byte(`{"error":{"message":"context length exceeded","code":"ctx"}}`)
	r := postRequest("/v1/chat/completions", "m", nil)
	copier := respond(t, http.StatusBadGateway, "application/json", respBody)
	mm.record("m", r, copier, captureAll, []byte(`{"model":"m"}`), nil)

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	rec := recs[0]
	if rec.Outcome != outcomeUpstreamError {
		t.Errorf("outcome = %q, want %q", rec.Outcome, outcomeUpstreamError)
	}
	if rec.Status != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Status)
	}
	if rec.Resp.body() != string(respBody) {
		t.Errorf("resp body = %q, want the failed upstream reply %q", rec.Resp.body(), respBody)
	}
	if rec.Error == nil || *rec.Error != "context length exceeded" {
		t.Errorf("error = %v, want the activity row's message", rec.Error)
	}
	if rec.Tokens != nil {
		t.Errorf("tokens = %+v, want null: nothing was parsed on the failure path", rec.Tokens)
	}
}

// TestCaptureLog_DisabledWritesNothing checks the default: no directory, no
// goroutine, nothing on the request path.
func TestCaptureLog_DisabledWritesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "captures")
	mm := newTestMetricsMonitor(t, logmon.NewWriter(io.Discard), 10, 5)
	mm.attachCaptureLog(config.CaptureLogConfig{Enabled: false, Dir: dir}, nil)

	if mm.captureLog != nil {
		t.Fatal("a disabled sink must not start a writer")
	}

	r := postRequest("/v1/chat/completions", "m", nil)
	copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1}}`))
	mm.record("m", r, copier, captureAll, []byte(`{"model":"m"}`), nil)

	if err := mm.Close(); err != nil {
		t.Fatalf("metricsMonitor.Close: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("capture log %s exists (stat err = %v); a disabled sink must not create it", dir, err)
	}
	// The ring is untouched by the sink being off.
	if entries := metricsEntries(t, mm); len(entries) != 1 {
		t.Fatalf("want 1 activity entry, got %d", len(entries))
	}
}

// TestCaptureLog_InvalidUTF8IsBase64 checks bodies that are not valid UTF-8
// survive: Go's JSON encoder replaces invalid bytes with U+FFFD, so such a
// body must be base64 instead of a lossy string.
func TestCaptureLog_InvalidUTF8IsBase64(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	reqBody := []byte{0x7b, 0xff, 0xfe, 0x00, 0x80, 0x7d} // "{" + invalid + "}"
	respBody := []byte{0x00, 0x01, 0xc3, 0x28, 0xff}      // 0xc3 0x28 is an invalid sequence

	r := postRequest("/v1/audio/transcriptions", "m", reqBody)
	copier := respond(t, http.StatusOK, "application/octet-stream", respBody)
	mm.record("m", r, copier, captureAll, reqBody, nil)

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	rec := recs[0]
	for _, tc := range []struct {
		name    string
		payload captureLogPayload
		want    []byte
	}{
		{"req", rec.Req, reqBody},
		{"resp", rec.Resp, respBody},
	} {
		if tc.payload.BodyEncoding != bodyEncodingBase64 {
			t.Errorf("%s body_encoding = %q, want %q", tc.name, tc.payload.BodyEncoding, bodyEncodingBase64)
			continue
		}
		got, err := base64.StdEncoding.DecodeString(tc.payload.body())
		if err != nil {
			t.Errorf("%s body is not base64: %v", tc.name, err)
			continue
		}
		if string(got) != string(tc.want) {
			t.Errorf("%s body = % x, want % x", tc.name, got, tc.want)
		}
	}
}

// TestCaptureLog_ConcurrentRequestsDoNotInterleave is the property a
// per-request writer would break. Every body is far larger than PIPE_BUF and
// than any buffer the writer might use, so a second writer racing inside one
// record would splice the lines together and the JSON would not parse.
func TestCaptureLog_ConcurrentRequestsDoNotInterleave(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	const requests = 24
	const filler = 32 * 1024

	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			marker := fmt.Sprintf("req-%02d", i)
			reqBody := []byte(fmt.Sprintf(`{"marker":%q,"pad":%q}`, marker, strings.Repeat(string(rune('a'+i%26)), filler)))
			respBody := []byte(fmt.Sprintf(`{"marker":%q,"pad":%q,"usage":{"prompt_tokens":%d,"completion_tokens":1}}`,
				marker, strings.Repeat(string(rune('A'+i%26)), filler), i))
			r := postRequest("/v1/chat/completions", marker, reqBody)
			copier := respond(t, http.StatusOK, "application/json", respBody)
			mm.record("m", r, copier, captureAll, reqBody, nil)
		}(i)
	}
	wg.Wait()

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != requests {
		t.Fatalf("want %d lines, got %d", requests, len(recs))
	}
	seen := make(map[string]bool, requests)
	for _, rec := range recs {
		var req struct {
			Marker string `json:"marker"`
			Pad    string `json:"pad"`
		}
		if err := json.Unmarshal([]byte(rec.Req.body()), &req); err != nil {
			t.Fatalf("req body is spliced: %v", err)
		}
		var resp struct {
			Marker string `json:"marker"`
			Pad    string `json:"pad"`
		}
		if err := json.Unmarshal([]byte(rec.Resp.body()), &resp); err != nil {
			t.Fatalf("resp body is spliced: %v", err)
		}
		// A spliced line could still parse if two records happened to nest,
		// so check both halves belong to the same request and are whole.
		if req.Marker != resp.Marker || req.Marker != rec.RequestedModel {
			t.Fatalf("line mixes requests: req %q, resp %q, model %q", req.Marker, resp.Marker, rec.RequestedModel)
		}
		if len(req.Pad) != filler || len(resp.Pad) != filler {
			t.Fatalf("%s: padding truncated (req %d, resp %d, want %d)", req.Marker, len(req.Pad), len(resp.Pad), filler)
		}
		if seen[req.Marker] {
			t.Fatalf("%s logged twice", req.Marker)
		}
		seen[req.Marker] = true
	}
	if len(seen) != requests {
		t.Fatalf("want %d distinct requests, got %d", requests, len(seen))
	}
}

// TestCaptureLog_MidStreamDisconnect covers the second hole: once a status has
// reached the client, MarkClientClosed cannot record the 499 sentinel, so an
// SSE stream the client cuts short is filed as a plain 200 and is
// indistinguishable from a completed one in the activity log. The JSONL line
// carries the distinction.
func TestCaptureLog_MidStreamDisconnect(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	// A stream that stops after two deltas, with no usage block and no
	// [DONE]: exactly what a client hanging up produces.
	partial := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"He\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n")

	clientCtx, clientGone := context.WithCancel(context.Background())
	r := swaputil.WithClientContext(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(clientCtx))
	r = r.WithContext(swaputil.SetContext(r.Context(), swaputil.ReqContextData{Model: "m", ModelID: "m"}))
	copier := respond(t, http.StatusOK, "text/event-stream", partial)
	clientGone()

	mm.record("m", r, copier, captureAll, []byte(`{"stream":true}`), nil)

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	rec := recs[0]
	if rec.Outcome != outcomeClientDisconnectedMidStream {
		t.Errorf("outcome = %q, want %q", rec.Outcome, outcomeClientDisconnectedMidStream)
	}
	if rec.Status != http.StatusOK {
		t.Errorf("status = %d: the wire status really was 200, only the outcome differs", rec.Status)
	}
	if rec.Resp.body() != string(partial) {
		t.Errorf("resp body = %q, want the truncated stream", rec.Resp.body())
	}
}

// TestCaptureLog_ServerSideCancelIsNotMidStream guards the same distinction
// MarkClientClosed makes: a request cancelled server-side (an operator
// cancelling from the UI, a peer router shutting down) still had a live
// client, and must not be reported as a disconnect.
func TestCaptureLog_ServerSideCancelIsNotMidStream(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	r := swaputil.WithClientContext(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	// A cancelled child of the request context, as the inflight tracker makes.
	derived, cancel := context.WithCancel(r.Context())
	cancel()
	r = r.WithContext(swaputil.SetContext(derived, swaputil.ReqContextData{Model: "m", ModelID: "m"}))

	copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	mm.record("m", r, copier, captureAll, []byte(`{"model":"m"}`), nil)

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	if recs[0].Outcome != outcomeOK {
		t.Errorf("outcome = %q, want %q: the client was still connected", recs[0].Outcome, outcomeOK)
	}
}

// TestCaptureLog_AbortedRequest covers the 499 path #1029 keeps out of the
// capture ring: off by default, and request-only when includeAborted is set.
func TestCaptureLog_AbortedRequest(t *testing.T) {
	abort := func(t *testing.T, mm *metricsMonitor) {
		t.Helper()
		r := postRequest("/v1/chat/completions", "m", nil)
		copier := newBodyCopier(httptest.NewRecorder())
		copier.Header().Set("Content-Type", "application/json")
		copier.MarkStatus(swaputil.StatusClientClosedRequest)
		mm.record("m", r, copier, captureAll, []byte(`{"model":"m"}`), map[string]string{"X-Test": "1"})
	}

	t.Run("excluded by default", func(t *testing.T) {
		mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})
		abort(t, mm)
		if err := mm.Close(); err != nil {
			t.Fatalf("metricsMonitor.Close: %v", err)
		}
		// A sink that never got a record never opens a file at all, so the
		// directory is expected to be empty rather than to hold an empty
		// stream.
		if names := captureLogFiles(t, dir); len(names) != 0 {
			t.Fatalf("want no capture log files, got %v", names)
		}
	})

	t.Run("included when asked", func(t *testing.T) {
		mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true, IncludeAborted: true})
		abort(t, mm)
		recs := readCaptureLog(t, mm, dir)
		if len(recs) != 1 {
			t.Fatalf("want 1 line, got %d", len(recs))
		}
		rec := recs[0]
		if rec.Outcome != outcomeClientDisconnected {
			t.Errorf("outcome = %q, want %q", rec.Outcome, outcomeClientDisconnected)
		}
		if rec.Status != swaputil.StatusClientClosedRequest {
			t.Errorf("status = %d, want 499", rec.Status)
		}
		if rec.Req.body() != `{"model":"m"}` || rec.Req.Headers["X-Test"] != "1" {
			t.Errorf("req = %+v, want the buffered request", rec.Req)
		}
		if rec.Resp.body() != "" || len(rec.Resp.Headers) != 0 {
			t.Errorf("resp = %+v, want empty: nothing was ever sent", rec.Resp)
		}
		if rec.Tokens != nil {
			t.Errorf("tokens = %+v, want null", rec.Tokens)
		}
	})
}

// TestCaptureLog_RedactsSensitiveHeaders checks the sink reuses the existing
// redaction rather than logging credentials in the clear.
func TestCaptureLog_RedactsSensitiveHeaders(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	reqHeaders := map[string]string{"Authorization": "Bearer secret", "Content-Type": "application/json"}
	redactHeaders(reqHeaders)

	r := postRequest("/v1/chat/completions", "m", nil)
	copier := newBodyCopier(httptest.NewRecorder())
	copier.Header().Set("Content-Type", "application/json")
	copier.Header().Set("Set-Cookie", "session=secret")
	copier.WriteHeader(http.StatusOK)
	if _, err := copier.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`)); err != nil {
		t.Fatalf("writing response: %v", err)
	}
	mm.record("m", r, copier, captureAll, []byte(`{"model":"m"}`), reqHeaders)

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	if got := recs[0].Req.Headers["Authorization"]; got != "[REDACTED]" {
		t.Errorf("req Authorization = %q, want [REDACTED]", got)
	}
	if got := recs[0].Resp.Headers["Set-Cookie"]; got != "[REDACTED]" {
		t.Errorf("resp Set-Cookie = %q, want [REDACTED]", got)
	}
}

// TestCaptureLog_RoutePolicyOmitsBody checks the sink honors
// captureFieldsByPath: the audio and image routes exist in that table because
// their bodies are large binary blobs, and a JSONL line is a worse place for
// those than the ring. What the mask drops must still be reported as dropped,
// so the line never quietly looks like a request with no body.
func TestCaptureLog_RoutePolicyOmitsBody(t *testing.T) {
	t.Run("response body", func(t *testing.T) {
		mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

		// /v1/audio/speech keeps the request but not the response body.
		const route = "/v1/audio/speech"
		respBody := bytes.Repeat([]byte{0xff, 0x00, 0x42}, 400) // 1200 bytes of "audio"
		reqBody := []byte(`{"model":"tts","input":"hello"}`)

		r := postRequest(route, "tts", reqBody)
		copier := respond(t, http.StatusOK, "audio/mpeg", respBody)
		mm.record("tts", r, copier, captureFieldsFor(route), reqBody, nil)

		recs := readCaptureLog(t, mm, dir)
		if len(recs) != 1 {
			t.Fatalf("want 1 line, got %d", len(recs))
		}
		rec := recs[0]
		if rec.Resp.Body != nil {
			t.Errorf("resp body = %q, want null: the route's mask drops it", *rec.Resp.Body)
		}
		if rec.Resp.BodyOmitted != bodyOmittedRoutePolicy {
			t.Errorf("resp body_omitted = %q, want %q", rec.Resp.BodyOmitted, bodyOmittedRoutePolicy)
		}
		if rec.Resp.BodyBytes == nil || *rec.Resp.BodyBytes != len(respBody) {
			t.Errorf("resp body_bytes = %v, want %d", rec.Resp.BodyBytes, len(respBody))
		}
		if rec.Resp.BodyEncoding != "" {
			t.Errorf("resp body_encoding = %q, want none: there is no body to encode", rec.Resp.BodyEncoding)
		}
		// The half the mask keeps is unaffected.
		if rec.Req.body() != string(reqBody) || rec.Req.BodyOmitted != "" {
			t.Errorf("req = %+v, want the request body verbatim", rec.Req)
		}
	})

	t.Run("request body", func(t *testing.T) {
		mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

		// /v1/audio/transcriptions keeps the response but not the request,
		// which the middleware therefore never buffers: the only size the
		// sink can report is the declared Content-Length.
		const route = "/v1/audio/transcriptions"
		upload := strings.Repeat("x", 4096)
		r := httptest.NewRequest(http.MethodPost, route, strings.NewReader(upload))
		r = r.WithContext(swaputil.SetContext(r.Context(), swaputil.ReqContextData{Model: "whisper", ModelID: "whisper"}))

		respBody := []byte(`{"text":"hello"}`)
		copier := respond(t, http.StatusOK, "application/json", respBody)
		// nil reqBody is what the middleware passes when the mask drops it.
		mm.record("whisper", r, copier, captureFieldsFor(route), nil, nil)

		recs := readCaptureLog(t, mm, dir)
		if len(recs) != 1 {
			t.Fatalf("want 1 line, got %d", len(recs))
		}
		rec := recs[0]
		if rec.Req.Body != nil {
			t.Errorf("req body = %q, want null: the route's mask drops it", *rec.Req.Body)
		}
		if rec.Req.BodyOmitted != bodyOmittedRoutePolicy {
			t.Errorf("req body_omitted = %q, want %q", rec.Req.BodyOmitted, bodyOmittedRoutePolicy)
		}
		if rec.Req.BodyBytes == nil || *rec.Req.BodyBytes != len(upload) {
			t.Errorf("req body_bytes = %v, want the declared %d", rec.Req.BodyBytes, len(upload))
		}
		if rec.Resp.body() != string(respBody) {
			t.Errorf("resp body = %q, want the transcription kept", rec.Resp.body())
		}
	})
}

// TestCaptureLog_EmptyResponseBodyStillEmitsLine covers a 200 whose body is
// empty. record() files the activity row and returns before storeCapture, so
// this used to leave no line at all — a sink where a missing line can mean
// "the request succeeded" cannot be used to reconstruct traffic.
func TestCaptureLog_EmptyResponseBodyStillEmitsLine(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	reqBody := []byte(`{"model":"m"}`)
	r := postRequest("/v1/chat/completions", "m", reqBody)
	copier := respond(t, http.StatusOK, "application/json", nil)
	mm.record("m", r, copier, captureAll, reqBody, nil)

	// The activity row is recorded as before; the sink is additive.
	if entries := metricsEntries(t, mm); len(entries) != 1 {
		t.Fatalf("want 1 activity entry, got %d", len(entries))
	}

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	rec := recs[0]
	if rec.Outcome != outcomeOK {
		t.Errorf("outcome = %q, want %q: an empty body is still a success", rec.Outcome, outcomeOK)
	}
	if rec.Status != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Status)
	}
	// An empty body is not an omitted one, and the line must say which.
	if rec.Resp.Body == nil || *rec.Resp.Body != "" || rec.Resp.BodyEncoding != bodyEncodingUTF8 {
		t.Errorf("resp = %+v, want an empty utf8 body", rec.Resp)
	}
	if rec.Resp.BodyOmitted != "" {
		t.Errorf("resp body_omitted = %q, want none: nothing was dropped", rec.Resp.BodyOmitted)
	}
	if rec.Req.body() != string(reqBody) {
		t.Errorf("req body = %q, want %q", rec.Req.body(), reqBody)
	}
}

// TestCaptureLog_DecompressionFailureKeepsWireBytes covers the other 200 that
// used to leave no line: a response whose Content-Encoding would not
// decompress. The bytes that failed are the only evidence of what the upstream
// sent and nothing else keeps them, so the line carries them as they arrived.
func TestCaptureLog_DecompressionFailureKeepsWireBytes(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true})

	// Claims gzip, is not gzip.
	respBody := []byte{0x1f, 0x8b, 0x08, 0x00, 'n', 'o', 't', 'g', 'z', 0xff}

	r := postRequest("/v1/chat/completions", "m", nil)
	copier := newBodyCopier(httptest.NewRecorder())
	copier.Header().Set("Content-Type", "application/json")
	copier.Header().Set("Content-Encoding", "gzip")
	copier.WriteHeader(http.StatusOK)
	if _, err := copier.Write(respBody); err != nil {
		t.Fatalf("writing response: %v", err)
	}
	mm.record("m", r, copier, captureAll, []byte(`{"model":"m"}`), nil)

	recs := readCaptureLog(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	rec := recs[0]
	if rec.Outcome != outcomeOK {
		t.Errorf("outcome = %q, want %q: the request itself succeeded", rec.Outcome, outcomeOK)
	}
	if rec.Error == nil || !strings.HasPrefix(*rec.Error, "response decompression failed") {
		t.Errorf("error = %v, want the activity row's decompression message", rec.Error)
	}
	if rec.Resp.BodyEncoding != bodyEncodingBase64 {
		t.Fatalf("resp body_encoding = %q, want %q: undecodable bytes are not text", rec.Resp.BodyEncoding, bodyEncodingBase64)
	}
	got, err := base64.StdEncoding.DecodeString(rec.Resp.body())
	if err != nil {
		t.Fatalf("resp body is not base64: %v", err)
	}
	if !bytes.Equal(got, respBody) {
		t.Errorf("resp body = % x, want the wire bytes % x", got, respBody)
	}
	// Unlike the decoded path, Content-Encoding still describes these bytes,
	// so it stays: a consumer needs it to know what they were supposed to be.
	if enc := rec.Resp.Headers["Content-Encoding"]; enc != "gzip" {
		t.Errorf("resp Content-Encoding = %q, want gzip alongside the wire bytes", enc)
	}
}

// captureLogTestLines builds n JSONL-shaped lines, each big enough and random
// enough that the compressed form of a single one exceeds the small rotation
// thresholds these tests use.
func captureLogTestLines(t *testing.T, n int) [][]byte {
	t.Helper()
	lines := make([][]byte, 0, n)
	for i := range n {
		filler := make([]byte, 4096)
		if _, err := rand.Read(filler); err != nil {
			t.Fatalf("rand: %v", err)
		}
		rec := captureLogRecord{
			ID:      i + 1,
			TS:      "2026-09-23T12:30:00.000+09:00",
			Outcome: outcomeOK,
			Path:    "/v1/chat/completions",
		}
		rec.Req.setBody([]byte(hex.EncodeToString(filler)))
		line, err := encodeCaptureLogRecord(&rec)
		if err != nil {
			t.Fatalf("encoding record %d: %v", i, err)
		}
		lines = append(lines, line)
	}
	return lines
}

// TestCaptureLog_RotationKeepsEveryByte drives the writer directly so the
// bytes handed in are known exactly, then checks the three properties the
// rotated form has to have: every file is a complete zstd stream on its own,
// the files concatenate back to precisely what was written, and no file ends
// mid-record.
func TestCaptureLog_RotationKeepsEveryByte(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "captures")
	// One record compresses to well over 1 KiB of random hex, so the threshold
	// is crossed by nearly every record and several files come out. The
	// threshold is on compressed bytes and a record is never split, so a file
	// is allowed to overshoot it by its last record — which is exactly what
	// happens here.
	w := newCaptureLogWriter(config.CaptureLogConfig{Enabled: true, Dir: dir, MaxFileBytes: 1024}, logmon.NewWriter(io.Discard), nil, nil)
	if w == nil {
		t.Fatal("newCaptureLogWriter returned nil for an enabled sink")
	}

	lines := captureLogTestLines(t, 24)
	// Well under captureLogQueueDepth, so nothing can be dropped.
	for _, line := range lines {
		w.write(line)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("captureLogWriter.Close: %v", err)
	}
	if n := w.dropped.Load(); n != 0 {
		t.Fatalf("dropped %d record(s); the queue should have absorbed them all", n)
	}

	names := captureLogFiles(t, dir)
	if len(names) < 2 {
		t.Fatalf("want more than one file at a 1 KiB threshold, got %v", names)
	}

	var joined []byte
	for _, name := range names {
		// Each file is opened with a decoder of its own: no dictionary and no
		// previous file is in scope, so this fails unless the file is a
		// self-contained stream.
		data := decodeCaptureLogFile(t, name)
		if len(data) == 0 {
			t.Fatalf("%s decompressed to nothing", name)
		}
		if data[len(data)-1] != '\n' {
			t.Errorf("%s does not end on a record boundary", name)
		}
		for i, line := range bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n")) {
			if !json.Valid(line) {
				t.Fatalf("%s line %d is not valid JSON (a record was split): %.120q", name, i, line)
			}
		}
		joined = append(joined, data...)
	}

	if want := bytes.Join(lines, nil); !bytes.Equal(joined, want) {
		t.Fatalf("concatenated files differ from what was written: got %d bytes, want %d", len(joined), len(want))
	}
}

// TestCaptureLog_RotatesAcrossRequests is the same property from the request
// side: with a small threshold a run of metered requests spreads over several
// files, and reading them all back gives the records in the order they were
// made.
func TestCaptureLog_RotatesAcrossRequests(t *testing.T) {
	mm, dir := captureLogSink(t, config.CaptureLogConfig{Enabled: true, MaxFileBytes: 1024})

	const requests = 8
	bodies := make([]string, 0, requests)
	for i := range requests {
		filler := make([]byte, 4096)
		if _, err := rand.Read(filler); err != nil {
			t.Fatalf("rand: %v", err)
		}
		reqBody := []byte(fmt.Sprintf(`{"model":"m","n":%d,"filler":%q}`, i, hex.EncodeToString(filler)))
		bodies = append(bodies, string(reqBody))
		r := postRequest("/v1/chat/completions", "m", reqBody)
		copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
		mm.record("m", r, copier, captureAll, reqBody, nil)
	}

	data := readCaptureLogBytes(t, mm, dir)
	if names := captureLogFiles(t, dir); len(names) < 2 {
		t.Fatalf("want more than one file at a 1 KiB threshold, got %v", names)
	}
	recs := parseCaptureLogLines(t, data)
	if len(recs) != requests {
		t.Fatalf("want %d records across the rotated files, got %d", requests, len(recs))
	}
	for i, rec := range recs {
		if rec.Req.body() != bodies[i] {
			t.Fatalf("record %d is out of order or truncated", i)
		}
		if i > 0 && rec.ID <= recs[i-1].ID {
			t.Errorf("record %d has id %d, not after %d", i, rec.ID, recs[i-1].ID)
		}
	}
}

// TestCaptureLog_UnclosedFileReadsToLastFlush is the crash case. Every record
// is flushed as it is written, so a file whose frame was never closed — the
// process was killed, Close never ran — still decompresses up to the last
// record that got through. The stream has no epilogue, so the decoder reports
// an unexpected EOF at the end; the records before it are intact.
func TestCaptureLog_UnclosedFileReadsToLastFlush(t *testing.T) {
	dir := t.TempDir()
	f, err := openCaptureLogFile(dir, 3, time.Now())
	if err != nil {
		t.Fatalf("openCaptureLogFile: %v", err)
	}
	lines := captureLogTestLines(t, 3)
	for _, line := range lines {
		if err := f.write(line); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	// Deliberately no f.close(): that is what a killed process leaves behind.

	raw, err := os.Open(filepath.Join(dir, f.name))
	if err != nil {
		t.Fatalf("opening %s: %v", f.name, err)
	}
	defer raw.Close()
	dec, err := zstd.NewReader(raw)
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	data, err := io.ReadAll(dec)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("decompressing a truncated stream: %v", err)
	}
	if want := bytes.Join(lines, nil); !bytes.Equal(data, want) {
		t.Fatalf("recovered %d bytes from the unclosed file, want the %d flushed", len(data), len(want))
	}
}

// TestCaptureLog_NamesAreUniqueAndSorted pins the naming rule: the timestamp
// alone is only second-resolution, so two files opened in the same second must
// still get different names, and the names must sort in the order they were
// opened — that order is what makes concatenating the directory meaningful.
func TestCaptureLog_NamesAreUniqueAndSorted(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 9, 23, 12, 30, 0, 0, time.FixedZone("JST", 9*3600))

	var opened []string
	for range 3 {
		f, err := openCaptureLogFile(dir, 0, now)
		if err != nil {
			t.Fatalf("openCaptureLogFile: %v", err)
		}
		if err := f.close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		opened = append(opened, f.name)
	}

	if opened[0] != "captures-20260923T123000+0900.jsonl.zst" {
		t.Errorf("first name = %q", opened[0])
	}
	if !sort.StringsAreSorted(opened) {
		t.Errorf("names %v do not sort in the order they were opened", opened)
	}
	for i := 1; i < len(opened); i++ {
		if opened[i] == opened[i-1] {
			t.Fatalf("name %q was reused", opened[i])
		}
	}
	if names := captureLogFiles(t, dir); len(names) != len(opened) {
		t.Fatalf("want %d files on disk, got %v", len(opened), names)
	}
}
