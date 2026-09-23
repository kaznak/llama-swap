package server

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/event"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
)

// traceTestConfig is a configuration whose command is a macro template,
// so a test that reads back the recorded cmd is reading the expansion rather
// than the text in the file. ${PORT} is allocated from startPort and
// ${MODEL_ID} from the model key, and both also reach the default proxy.
const traceTestConfig = `
startPort: 5800
macros:
  server-bin: /opt/llama-server --flash-attn
models:
  m:
    cmd: |
      ${server-bin}
      --port ${PORT}
      --model ${MODEL_ID}.gguf
    env:
      - OPENAI_API_KEY=sekret
      - PLAIN=1
`

// traceStateStub stands in for the Server as the trace's window
// onto process management. The configuration goes through the real loader and
// the detail through the real traceModelDetails, so what the tests read
// back is what a running llama-swap would record.
type traceStateStub struct {
	mu      sync.Mutex
	cfg     config.Config
	running map[string]string
	profile string
}

func newTraceStateStub(t *testing.T, source string) *traceStateStub {
	t.Helper()
	cfg, err := config.LoadConfigFromReader(strings.NewReader(source))
	if err != nil {
		t.Fatalf("loading the test configuration: %v", err)
	}
	return &traceStateStub{cfg: cfg, running: map[string]string{}}
}

func (s *traceStateStub) state() traceServerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	running := make(map[string]string, len(s.running))
	for name, state := range s.running {
		running[name] = state
	}
	return traceServerState{
		Build:   traceBuild{Version: "v1.2.3", Commit: "deadbeef", Date: "2026-09-23"},
		Profile: s.profile,
		Running: running,
		Models:  traceModelDetails(s.cfg),
		// The same method Server.traceState hands over, not a reimplementation
		// of it: a stub that marshals the configuration its own way would have
		// let the redaction go missing without a test noticing.
		ConfigYAML: s.cfg.RedactedFullYAML,
	}
}

func (s *traceStateStub) setRunning(name, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state == "" {
		delete(s.running, name)
		return
	}
	s.running[name] = state
}

// traceRawLines closes the sink and returns each line as a generic JSON
// object, so records of every kind can be inspected in one pass.
func traceRawLines(t *testing.T, mm *metricsMonitor, dir string) []map[string]any {
	t.Helper()
	return traceDecodeLines(t, readTraceBytes(t, mm, dir))
}

func traceDecodeLines(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("trace line is not valid JSON: %v\nline: %q", err, line)
		}
		out = append(out, rec)
	}
	return out
}

// traceTypes is the sequence of record kinds, which is what the ordering
// assertions are about.
func traceTypes(lines []map[string]any) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		kind, _ := line["type"].(string)
		out = append(out, kind)
	}
	return out
}

// tracePeekLines reads the sink without closing it, tolerating the
// unterminated frame of the file still being written. It is how the tests that
// go through the event bus wait for an asynchronously delivered record.
func tracePeekLines(t *testing.T, dir string) []map[string]any {
	t.Helper()
	var data []byte
	for _, name := range traceFiles(t, dir) {
		f, err := os.Open(name)
		if err != nil {
			t.Fatalf("opening %s: %v", name, err)
		}
		dec, err := zstd.NewReader(f)
		if err != nil {
			f.Close()
			t.Fatalf("zstd reader for %s: %v", name, err)
		}
		chunk, err := io.ReadAll(dec)
		dec.Close()
		f.Close()
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("decompressing %s: %v", name, err)
		}
		data = append(data, chunk...)
	}
	return traceDecodeLines(t, data)
}

// waitForTraceTypes polls until the sink holds at least one record of
// kind. Event delivery is asynchronous (each event type has its own consumer
// goroutine), so a test that emits on the bus has to wait for the record
// rather than assume it is already there.
func waitForTraceTypes(t *testing.T, dir, kind string) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		lines := tracePeekLines(t, dir)
		for _, got := range traceTypes(lines) {
			if got == kind {
				return lines
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %q record reached the sink within the deadline; got %v", kind, traceTypes(lines))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestTrace_RequestRecordCarriesRequestLine checks the three things a
// request record needs to be reproducible beyond its bodies: the kind and
// format version every consumer dispatches on, and the request line —
// method, the target with its query string, and the client address.
func TestTrace_RequestRecordCarriesRequestLine(t *testing.T) {
	mm, dir := traceSink(t, config.TraceConfig{Enabled: true})

	r := postRequest("/v1/chat/completions?stream=true&n=2", "m", nil)
	r.RemoteAddr = "192.0.2.7:51234"
	copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1}}`))
	mm.record("m", r, copier, captureAll, []byte(`{"model":"m"}`), nil)

	lines := traceRawLines(t, mm, dir)
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d", len(lines))
	}
	rec := lines[0]
	if rec["type"] != traceTypeRequest {
		t.Errorf("type = %v, want %q", rec["type"], traceTypeRequest)
	}
	if v, _ := rec["v"].(float64); int(v) != traceFormatVersion {
		t.Errorf("v = %v, want %d", rec["v"], traceFormatVersion)
	}
	if rec["method"] != http.MethodPost {
		t.Errorf("method = %v, want POST", rec["method"])
	}
	// The query is part of the request line: without it a GET cannot be
	// replayed, and the activity row's path drops it.
	if want := "/v1/chat/completions?stream=true&n=2"; rec["path"] != want {
		t.Errorf("path = %v, want %q", rec["path"], want)
	}
	if rec["remote_ip"] != "192.0.2.7" {
		t.Errorf("remote_ip = %v, want 192.0.2.7", rec["remote_ip"])
	}
}

// TestTrace_RequestRecordUsesForwardedIP pins remote_ip to the same
// resolution the in-flight view uses, so the same request reads the same in
// both places.
func TestTrace_RequestRecordUsesForwardedIP(t *testing.T) {
	mm, dir := traceSink(t, config.TraceConfig{Enabled: true})

	r := postRequest("/v1/chat/completions", "m", nil)
	r.RemoteAddr = "10.0.0.1:4000"
	r.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")
	copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1}}`))
	mm.record("m", r, copier, captureAll, nil, nil)

	lines := traceRawLines(t, mm, dir)
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d", len(lines))
	}
	if lines[0]["remote_ip"] != "203.0.113.9" {
		t.Errorf("remote_ip = %v, want the forwarded client 203.0.113.9", lines[0]["remote_ip"])
	}
}

// TestTrace_StateIsOffByDefault is the switch: an enabled sink with no
// trace settings records requests and nothing else. The state records carry
// expanded command lines, environments and the effective configuration, so
// turning the sink on must not start recording them.
func TestTrace_StateIsOffByDefault(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	mm, dir := traceSinkWithState(t, config.TraceConfig{Enabled: true}, stub.state)

	if mm.traceState != nil {
		t.Fatal("no trace switch is set, so no tracer should exist")
	}
	event.Emit(swaputil.ProcessStateChangeEvent{ProcessName: "m", OldState: "stopped", NewState: "starting"})
	event.Emit(swaputil.ConfigFileChangedEvent{State: swaputil.ReloadingStateEnd})

	r := postRequest("/v1/chat/completions", "m", nil)
	copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1}}`))
	mm.record("m", r, copier, captureAll, nil, nil)

	types := traceTypes(traceRawLines(t, mm, dir))
	if len(types) != 1 || types[0] != traceTypeRequest {
		t.Fatalf("record kinds = %v, want only a request record", types)
	}
}

// TestTrace_BackendStateRecordsTransitions checks the backend record: one
// line per process state transition, carrying the detail a request record
// cannot — which command actually ran, with which environment, against which
// upstream, since when.
//
// It goes through the real event bus, which is also the assertion that the
// sink is subscribed to it rather than being called directly.
func TestTrace_BackendStateRecordsTransitions(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	cfg := config.TraceConfig{Enabled: true}
	cfg.State.Backend = true
	mm, dir := traceSinkWithState(t, cfg, stub.state)
	if mm.traceState == nil {
		t.Fatal("trace.state.backend is set but no tracer was started")
	}

	event.Emit(swaputil.ProcessStateChangeEvent{
		ProcessName: "m",
		OldState:    string(process.StateStopped),
		NewState:    string(process.StateStarting),
	})
	lines := waitForTraceTypes(t, dir, traceTypeBackend)

	var rec map[string]any
	for _, line := range lines {
		if line["type"] == traceTypeBackend {
			rec = line
		}
	}
	backend, ok := rec["backend"].(map[string]any)
	if !ok {
		t.Fatalf("backend record has no backend object: %v", rec)
	}
	if backend["process_name"] != "m" {
		t.Errorf("process_name = %v, want m", backend["process_name"])
	}
	if backend["old_state"] != "stopped" || backend["new_state"] != "starting" {
		t.Errorf("states = (%v, %v), want (stopped, starting)", backend["old_state"], backend["new_state"])
	}
	// The recorded command is the expansion, not the template: ${server-bin},
	// ${PORT} and ${MODEL_ID} are all resolved.
	wantCmd := []any{"/opt/llama-server", "--flash-attn", "--port", "5800", "--model", "m.gguf"}
	gotCmd, _ := backend["cmd"].([]any)
	if len(gotCmd) != len(wantCmd) {
		t.Fatalf("cmd = %v, want the expanded %v", backend["cmd"], wantCmd)
	}
	for i := range wantCmd {
		if gotCmd[i] != wantCmd[i] {
			t.Fatalf("cmd = %v, want the expanded %v", backend["cmd"], wantCmd)
		}
	}
	if backend["upstream"] != "http://localhost:5800" {
		t.Errorf("upstream = %v, want the resolved http://localhost:5800", backend["upstream"])
	}
	env, _ := backend["env"].([]any)
	if len(env) != 2 || env[0] != "OPENAI_API_KEY=sekret" {
		t.Errorf("env = %v, want the configured environment", backend["env"])
	}
	// Entering "starting" is what the start time is observed from, so this
	// transition is the one that has it.
	if started, _ := backend["started_at"].(string); started == "" {
		t.Errorf("started_at = %v, want the time the process entered starting", backend["started_at"])
	}
}

// TestTrace_BackendStateHasNoStartTimeBeforeStart is the other half of
// the derived start time: it is observed from the event stream, so a process
// whose start the sink did not see reports null rather than a guess.
func TestTrace_BackendStateHasNoStartTimeBeforeStart(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	cfg := config.TraceConfig{Enabled: true}
	cfg.State.Backend = true
	mm, dir := traceSinkWithState(t, cfg, stub.state)

	mm.traceState.onProcessStateChange(swaputil.ProcessStateChangeEvent{
		ProcessName: "m",
		OldState:    string(process.StateReady),
		NewState:    string(process.StateStopping),
	})

	lines := traceRawLines(t, mm, dir)
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d", len(lines))
	}
	backend, _ := lines[0]["backend"].(map[string]any)
	if backend["started_at"] != nil {
		t.Errorf("started_at = %v, want null for a process whose start was not observed", backend["started_at"])
	}
}

// TestTrace_ConfigStateRecordsReloadBoundaries checks the reload markers.
// The configuration is hot-reloaded, so without them a reader would interpret
// records under settings that had already been replaced. Both boundaries are
// recorded: together they bracket the window in which either configuration
// could have served a request.
func TestTrace_ConfigStateRecordsReloadBoundaries(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	cfg := config.TraceConfig{Enabled: true}
	cfg.State.Config = true
	mm, dir := traceSinkWithState(t, cfg, stub.state)

	mm.traceState.onConfigFileChanged(swaputil.ConfigFileChangedEvent{State: swaputil.ReloadingStateStart})
	mm.traceState.onConfigFileChanged(swaputil.ConfigFileChangedEvent{State: swaputil.ReloadingStateEnd})

	lines := traceRawLines(t, mm, dir)
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d", len(lines))
	}
	for i, want := range []string{traceReloadStart, traceReloadEnd} {
		if lines[i]["type"] != traceTypeConfig {
			t.Fatalf("line %d type = %v, want %q", i, lines[i]["type"], traceTypeConfig)
		}
		reload, _ := lines[i]["config"].(map[string]any)
		if reload["state"] != want {
			t.Errorf("line %d state = %v, want %q", i, reload["state"], want)
		}
	}
}

// TestTrace_CheckpointIsFirstLineOfEveryFile is the redundancy the
// checkpoint exists for. The trace is only readable from the beginning of the
// stream, and rotation cuts the stream, so every file has to open with the
// full state — not just the first one.
func TestTrace_CheckpointIsFirstLineOfEveryFile(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	stub.setRunning("m", string(process.StateReady))
	cfg := config.TraceConfig{Enabled: true, MaxFileBytes: 1024}
	cfg.State.Checkpoint = true
	mm, dir := traceSinkWithState(t, cfg, stub.state)

	// Incompressible bodies, so the 1 KiB threshold is crossed by the
	// records themselves and several files come out.
	for range 4 {
		filler := make([]byte, 4096)
		if _, err := rand.Read(filler); err != nil {
			t.Fatalf("rand: %v", err)
		}
		reqBody := []byte(`{"model":"m","filler":"` + hex.EncodeToString(filler) + `"}`)
		r := postRequest("/v1/chat/completions", "m", reqBody)
		copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1}}`))
		mm.record("m", r, copier, captureAll, reqBody, nil)
	}

	if err := mm.Close(); err != nil {
		t.Fatalf("metricsMonitor.Close: %v", err)
	}
	names := traceFiles(t, dir)
	if len(names) < 2 {
		t.Fatalf("want more than one file at a 1 KiB threshold, got %v", names)
	}
	for _, name := range names {
		lines := traceDecodeLines(t, decodeTraceFile(t, name))
		if len(lines) == 0 {
			t.Fatalf("%s is empty", name)
		}
		if lines[0]["type"] != traceTypeCheckpoint {
			t.Fatalf("%s starts with a %v record, want a checkpoint", name, lines[0]["type"])
		}
		for i, line := range lines[1:] {
			if line["type"] == traceTypeCheckpoint {
				t.Fatalf("%s has a second checkpoint at line %d", name, i+2)
			}
		}
	}
}

// TestTrace_CheckpointCarriesRunningStateAndConfig checks the contents of
// the dump: the build that wrote the file, the active profile, every running
// process with its expanded command, and the whole effective configuration.
func TestTrace_CheckpointCarriesRunningStateAndConfig(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	stub.setRunning("m", string(process.StateReady))
	stub.profile = "evening"
	cfg := config.TraceConfig{Enabled: true}
	cfg.State.Checkpoint = true
	mm, dir := traceSinkWithState(t, cfg, stub.state)

	r := postRequest("/v1/chat/completions", "m", nil)
	copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1}}`))
	mm.record("m", r, copier, captureAll, nil, nil)

	lines := traceRawLines(t, mm, dir)
	if len(lines) != 2 {
		t.Fatalf("want a checkpoint and a request, got %d lines", len(lines))
	}
	cp, _ := lines[0]["checkpoint"].(map[string]any)
	if cp == nil {
		t.Fatalf("checkpoint record has no checkpoint object: %v", lines[0])
	}
	build, _ := cp["llama_swap"].(map[string]any)
	if build["version"] != "v1.2.3" || build["commit"] != "deadbeef" {
		t.Errorf("llama_swap = %v, want the build info", cp["llama_swap"])
	}
	if cp["profile"] != "evening" {
		t.Errorf("profile = %v, want evening", cp["profile"])
	}
	procs, _ := cp["processes"].([]any)
	if len(procs) != 1 {
		t.Fatalf("processes = %v, want the one running model", cp["processes"])
	}
	proc, _ := procs[0].(map[string]any)
	if proc["process_name"] != "m" || proc["state"] != "ready" {
		t.Errorf("process = %v, want m in state ready", proc)
	}
	if cmd, _ := proc["cmd"].([]any); len(cmd) == 0 || cmd[0] != "/opt/llama-server" {
		t.Errorf("process cmd = %v, want the expanded command", proc["cmd"])
	}
	// The effective configuration, whole, with the configured key names and
	// the same expansion.
	effective, _ := cp["config"].(map[string]any)
	if effective == nil {
		t.Fatalf("checkpoint carries no config: %v", cp["config_error"])
	}
	models, _ := effective["models"].(map[string]any)
	model, _ := models["m"].(map[string]any)
	if model == nil {
		t.Fatalf("effective config has no model m: %v", effective["models"])
	}
	if cmd, _ := model["cmd"].(string); !strings.Contains(cmd, "--port 5800") {
		t.Errorf("effective config cmd = %q, want the expanded port", cmd)
	}
}

// TestTrace_StateAndRequestsKeepOrder is the ordering contract. State
// records arrive asynchronously but go through the same queue as the requests,
// and a single writer goroutine owns the file, so the file is in the order the
// records were produced. A backend record that landed after the requests it
// describes would say the wrong thing about them.
func TestTrace_StateAndRequestsKeepOrder(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	cfg := config.TraceConfig{Enabled: true}
	cfg.State.Backend = true
	cfg.State.Config = true
	mm, dir := traceSinkWithState(t, cfg, stub.state)

	request := func() {
		r := postRequest("/v1/chat/completions", "m", nil)
		copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1}}`))
		mm.record("m", r, copier, captureAll, nil, nil)
	}

	mm.traceState.onProcessStateChange(swaputil.ProcessStateChangeEvent{
		ProcessName: "m", OldState: "stopped", NewState: "starting",
	})
	mm.traceState.onProcessStateChange(swaputil.ProcessStateChangeEvent{
		ProcessName: "m", OldState: "starting", NewState: "ready",
	})
	request()
	request()
	mm.traceState.onConfigFileChanged(swaputil.ConfigFileChangedEvent{State: swaputil.ReloadingStateEnd})
	request()
	mm.traceState.onProcessStateChange(swaputil.ProcessStateChangeEvent{
		ProcessName: "m", OldState: "ready", NewState: "stopping",
	})

	want := []string{
		traceTypeBackend,
		traceTypeBackend,
		traceTypeRequest,
		traceTypeRequest,
		traceTypeConfig,
		traceTypeRequest,
		traceTypeBackend,
	}
	got := traceTypes(traceRawLines(t, mm, dir))
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("record order = %v, want %v", got, want)
	}
}

// TestTrace_MaskPathsRedactRecordFields checks maskPaths on both a
// request and a state record, and that everything it does not name is
// untouched.
func TestTrace_MaskPathsRedactRecordFields(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	cfg := config.TraceConfig{
		Enabled:   true,
		MaskPaths: []string{"backend.cmd", "req.headers.Authorization"},
	}
	cfg.State.Backend = true
	mm, dir := traceSinkWithState(t, cfg, stub.state)

	mm.traceState.onProcessStateChange(swaputil.ProcessStateChangeEvent{
		ProcessName: "m", OldState: "stopped", NewState: "starting",
	})
	r := postRequest("/v1/chat/completions", "m", nil)
	copier := respond(t, http.StatusOK, "application/json", []byte(`{"usage":{"prompt_tokens":1}}`))
	mm.record("m", r, copier, captureAll, []byte(`{"model":"m"}`), map[string]string{
		"Authorization": "Bearer sk-live-1234",
		"Content-Type":  "application/json",
	})

	lines := traceRawLines(t, mm, dir)
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d", len(lines))
	}
	backend, _ := lines[0]["backend"].(map[string]any)
	if backend["cmd"] != config.RedactedPlaceholder {
		t.Errorf("backend.cmd = %v, want it masked", backend["cmd"])
	}
	// Named fields only: the upstream next to the masked command is still there.
	if backend["upstream"] != "http://localhost:5800" {
		t.Errorf("backend.upstream = %v, want it untouched", backend["upstream"])
	}
	req, _ := lines[1]["req"].(map[string]any)
	headers, _ := req["headers"].(map[string]any)
	if headers["Authorization"] != config.RedactedPlaceholder {
		t.Errorf("req.headers.Authorization = %v, want it masked", headers["Authorization"])
	}
	if headers["Content-Type"] != "application/json" {
		t.Errorf("req.headers.Content-Type = %v, want it untouched", headers["Content-Type"])
	}
	// A path that names nothing in this record must not create it: masking
	// hides values, it does not invent fields.
	if _, invented := lines[1]["backend"]; invented {
		t.Errorf("a backend.cmd mask invented a backend object on a request record: %v", lines[1])
	}
}

// TestTrace_MaskEnvRedactsByName checks the environment mask. An
// environment is a list of "NAME=value" strings, so no JSON path can select
// one entry; the name does it instead, and it reaches both the backend record
// and the models inside the checkpoint's effective configuration.
func TestTrace_MaskEnvRedactsByName(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	stub.setRunning("m", string(process.StateReady))
	cfg := config.TraceConfig{Enabled: true, MaskEnv: []string{"OPENAI_API_KEY"}}
	cfg.State.Backend = true
	cfg.State.Checkpoint = true
	mm, dir := traceSinkWithState(t, cfg, stub.state)

	mm.traceState.onProcessStateChange(swaputil.ProcessStateChangeEvent{
		ProcessName: "m", OldState: "stopped", NewState: "starting",
	})

	lines := traceRawLines(t, mm, dir)
	if len(lines) != 2 {
		t.Fatalf("want a checkpoint and a backend record, got %d lines", len(lines))
	}
	wantEnv := []any{"OPENAI_API_KEY=" + config.RedactedPlaceholder, "PLAIN=1"}
	assertEnv := func(where string, got any) {
		t.Helper()
		list, _ := got.([]any)
		if len(list) != len(wantEnv) {
			t.Fatalf("%s env = %v, want %v", where, got, wantEnv)
		}
		for i := range wantEnv {
			if list[i] != wantEnv[i] {
				t.Fatalf("%s env = %v, want %v", where, got, wantEnv)
			}
		}
	}

	cp, _ := lines[0]["checkpoint"].(map[string]any)
	procs, _ := cp["processes"].([]any)
	if len(procs) != 1 {
		t.Fatalf("checkpoint processes = %v", cp["processes"])
	}
	proc, _ := procs[0].(map[string]any)
	assertEnv("checkpoint process", proc["env"])

	effective, _ := cp["config"].(map[string]any)
	models, _ := effective["models"].(map[string]any)
	model, _ := models["m"].(map[string]any)
	assertEnv("checkpoint config model", model["env"])

	backend, _ := lines[1]["backend"].(map[string]any)
	assertEnv("backend record", backend["env"])
}

// TestTrace_MaskCannotReachBodies is the boundary. req.body and resp.body
// are the bytes that went over the wire, kept verbatim, and that is the whole
// point of the format — so a mask path naming one is refused rather than
// applied, and the bodies come out byte-identical next to a field that was
// masked.
func TestTrace_MaskCannotReachBodies(t *testing.T) {
	cfg := config.TraceConfig{
		Enabled: true,
		MaskPaths: []string{
			"req.body",
			"resp.body",
			"req",  // would take the body with it
			"resp", // same
			"req.headers.Authorization",
		},
	}
	mm, dir := traceSink(t, cfg)

	// Key order and whitespace a JSON round trip would destroy, so a body
	// that had been rewritten would not compare equal.
	reqBody := []byte("{\"model\":\"m\",\n  \"z_first\":1,   \"a_second\":2}")
	respBody := []byte(`{"usage":{"prompt_tokens":1},"choices":[{"text":"<b>hi</b>"}]}`)
	r := postRequest("/v1/chat/completions", "m", reqBody)
	copier := respond(t, http.StatusOK, "application/json", respBody)
	mm.record("m", r, copier, captureAll, reqBody, map[string]string{"Authorization": "Bearer sk-live-1234"})

	recs := readTrace(t, mm, dir)
	if len(recs) != 1 {
		t.Fatalf("want 1 line, got %d", len(recs))
	}
	rec := recs[0]
	if rec.Req.body() != string(reqBody) {
		t.Errorf("req body = %q, want the verbatim %q", rec.Req.body(), reqBody)
	}
	if rec.Resp.body() != string(respBody) {
		t.Errorf("resp body = %q, want the verbatim %q", rec.Resp.body(), respBody)
	}
	// The refusals must not disable the rest of the mask.
	if rec.Req.Headers["Authorization"] != config.RedactedPlaceholder {
		t.Errorf("req.headers.Authorization = %q, want it masked", rec.Req.Headers["Authorization"])
	}
}

// TestTrace_MaskRefusesBodyPaths pins which paths are refused, at the
// level where the decision is made.
func TestTrace_MaskRefusesBodyPaths(t *testing.T) {
	refused := []string{"req.body", "resp.body", "req.body.messages", "req", "resp"}
	for _, path := range refused {
		if _, bad := traceMaskRefusal(path); !bad {
			t.Errorf("%q should be refused: it would rewrite a verbatim body", path)
		}
	}
	allowed := []string{"req.headers.Authorization", "resp.headers.Set-Cookie", "backend.cmd", "checkpoint.config", "req.body_bytes"}
	for _, path := range allowed {
		if reason, bad := traceMaskRefusal(path); bad {
			t.Errorf("%q should be allowed, refused with %q", path, reason)
		}
	}
}

// TestTrace_MaskIsEmptyByDefault is the default: everything is recorded
// and nothing is masked, so an operator who configures nothing gets the full
// record rather than a silently filtered one.
func TestTrace_MaskIsEmptyByDefault(t *testing.T) {
	if mask := newTraceMask(config.TraceConfig{Enabled: true}, nil); mask != nil {
		t.Fatalf("an unconfigured mask should be nil, got %+v", mask)
	}
	var nilMask *traceMask
	line := []byte(`{"req":{"headers":{"Authorization":"secret"}}}` + "\n")
	if got := nilMask.applyPaths(line); !bytes.Equal(got, line) {
		t.Errorf("a nil mask changed the line: %s", got)
	}
	if got := nilMask.maskEnv([]string{"K=v"}); len(got) != 1 || got[0] != "K=v" {
		t.Errorf("a nil mask changed the environment: %v", got)
	}
}

// TestTrace_CheckpointConfigIsRedactedAndWhole covers the two halves of
// RedactedFullYAML at once: llama-swap's own redaction reaches secrets that
// maskPaths cannot (this one is inside an env entry, and nothing was
// configured to be masked here), while the pruning RedactedYAML does for
// readability is not applied, so a key whose resolved value is empty is still
// on the record.
func TestTrace_CheckpointConfigIsRedactedAndWhole(t *testing.T) {
	stub := newTraceStateStub(t, traceTestConfig)
	cfg := config.TraceConfig{Enabled: true}
	cfg.State.Checkpoint = true
	mm, dir := traceSinkWithState(t, cfg, stub.state)

	r := postRequest("/v1/chat/completions", "m", nil)
	copier := respond(t, http.StatusOK, "application/json", []byte(`{}`))
	mm.record("m", r, copier, captureAll, nil, nil)

	lines := traceRawLines(t, mm, dir)
	raw, err := json.Marshal(lines[0])
	if err != nil {
		t.Fatalf("re-encoding the checkpoint: %v", err)
	}
	if bytes.Contains(raw, []byte("sekret")) {
		t.Errorf("the checkpoint carries the env secret verbatim: %s", raw)
	}
	if !bytes.Contains(raw, []byte("PLAIN=1")) {
		t.Errorf("the checkpoint dropped an env entry that is not a secret: %s", raw)
	}

	cp, _ := lines[0]["checkpoint"].(map[string]any)
	effective, _ := cp["config"].(map[string]any)
	if effective == nil {
		t.Fatalf("checkpoint has no effective configuration: %v", lines[0])
	}
	if !traceHasEmptyValue(effective) {
		t.Errorf("no empty value survived in the effective configuration; it looks pruned")
	}
}

// traceHasEmptyValue reports whether any leaf in the tree is nil, an
// empty string, an empty map or an empty slice — the shapes pruneEmpty drops.
func traceHasEmptyValue(node any) bool {
	switch t := node.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case map[string]any:
		if len(t) == 0 {
			return true
		}
		for _, v := range t {
			if traceHasEmptyValue(v) {
				return true
			}
		}
	case []any:
		if len(t) == 0 {
			return true
		}
		for _, v := range t {
			if traceHasEmptyValue(v) {
				return true
			}
		}
	}
	return false
}
