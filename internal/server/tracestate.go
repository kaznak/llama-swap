package server

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/event"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/mostlygeek/llama-swap/internal/process"
	"github.com/mostlygeek/llama-swap/internal/swaputil"
	"gopkg.in/yaml.v3"
)

// A request record says what was asked and what came back. It does not say
// which backend answered or how that backend was configured, so on its own it
// cannot be used to reproduce the inference. The state records fill that in,
// and they come in two shapes for two different failure modes:
//
//   - The event record is the primary form. The process-wide event bus
//     (internal/swaputil/events.go) already carries every process state
//     transition and every configuration reload, so subscribing to it puts a
//     record in the stream at the moment the fact changed, interleaved with
//     the requests it applies to. Hot reload is the reason the config record
//     exists: without a marker, records written before a reload would be read
//     under a configuration that no longer applied to them.
//
//   - The checkpoint is the redundancy. An event record only works if the
//     reader has every line since the process started, and rotation breaks
//     exactly that: a file opened an hour in begins in the middle. Each file
//     therefore opens with a full state dump, so it can be interpreted
//     without the files before it.
//
// Both are opt-in (trace.state.*) and off by default, because they carry
// the expanded command lines, environments and effective configuration that
// make the log reproducible — and that is also what makes it sensitive.

// traceProcessDetail is what a record has to carry for a backend to be
// reproducible: the command as it will actually be executed, its environment,
// the upstream address llama-swap proxies to, and when it started.
//
// Cmd is post-macro: the configured cmd is a template ("${latest-llama}
// --port ${PORT} --model ${MODEL_ID}"), macros are expanded while the
// configuration is loaded, and this is the argv that reaches exec.Command —
// the same value process.doStart builds, via the same ModelConfig method.
type traceProcessDetail struct {
	Cmd []string `json:"cmd"`
	// CmdError is set instead of Cmd when the command cannot be split into an
	// argv (an unbalanced quote, say). The process would fail to start with
	// the same error, so the record says so rather than showing nothing.
	CmdError string   `json:"cmd_error,omitempty"`
	Env      []string `json:"env"`
	Upstream string   `json:"upstream"`
	// StartedAt is when this process was last seen entering the starting
	// state, or null when it started before the sink was watching. It is
	// derived from the event stream because process management keeps no start
	// time of its own.
	StartedAt *string `json:"started_at"`
}

// traceBackendRecord is one process state transition.
type traceBackendRecord struct {
	Type    string       `json:"type"`
	V       int          `json:"v"`
	TS      string       `json:"ts"`
	Backend traceBackend `json:"backend"`
}

type traceBackend struct {
	ProcessName string `json:"process_name"`
	OldState    string `json:"old_state"`
	NewState    string `json:"new_state"`
	traceProcessDetail
}

// traceConfigRecord marks a configuration reload boundary.
type traceConfigRecord struct {
	Type   string      `json:"type"`
	V      int         `json:"v"`
	TS     string      `json:"ts"`
	Config traceReload `json:"config"`
}

type traceReload struct {
	State string `json:"state"`
}

const (
	traceReloadStart = "reloading_start"
	traceReloadEnd   = "reloading_end"
)

// traceCheckpointRecord is the state dump at the head of a file.
type traceCheckpointRecord struct {
	Type       string          `json:"type"`
	V          int             `json:"v"`
	TS         string          `json:"ts"`
	Checkpoint traceCheckpoint `json:"checkpoint"`
}

type traceCheckpoint struct {
	Build     traceBuild               `json:"llama_swap"`
	Profile   string                   `json:"profile"`
	Processes []traceCheckpointProcess `json:"processes"`
	// Config is the effective configuration, whole. It is written once per
	// file rather than once per record, so its size does not matter, and
	// without it a reader has to be told out of band what the settings were.
	Config json.RawMessage `json:"config"`
	// ConfigError says why Config is null, when it is.
	ConfigError string `json:"config_error,omitempty"`
}

// traceBuild identifies the llama-swap that wrote the file. The values
// are the ones GET /api/version reports: they come from ldflags, so a build
// that set none says version "0".
type traceBuild struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

type traceCheckpointProcess struct {
	ProcessName string `json:"process_name"`
	State       string `json:"state"`
	traceProcessDetail
}

// traceServerState is the seam between the sink and process management.
// attachTrace is handed one of these functions at construction; the
// tracer calls it on its own goroutines (at construction and on each event)
// and publishes the result as a snapshot, so the writer goroutine only ever
// reads already-materialized data. Nothing the writer does can block on a
// router lock or on a process that is busy starting.
type traceStateFunc func() traceServerState

type traceServerState struct {
	Build   traceBuild
	Profile string
	// Running is every process that is not stopped, keyed by process name.
	Running map[string]string
	// Models is the configured detail for every model, whether running or
	// not, keyed by the same name.
	Models map[string]traceProcessDetail
	// ConfigYAML renders the effective configuration in its configured form
	// (YAML key names, macros expanded). It is a function because it is only
	// needed when checkpoints are on, and the result is cached by the tracer:
	// the configuration cannot change under a running Server, a reload builds
	// a new one.
	ConfigYAML func() ([]byte, error)
}

// traceSnapshot is the materialized state a checkpoint is rendered from.
// It is published as an immutable value and swapped atomically.
type traceSnapshot struct {
	build       traceBuild
	profile     string
	processes   []traceCheckpointProcess
	config      json.RawMessage
	configError string
}

// traceStateTracer subscribes the sink to the process-wide event bus and keeps
// the checkpoint snapshot current.
//
// Its records go into the sink through the same queue as the request records,
// so the single writer goroutine keeps them in the order they were produced.
// There is deliberately no second path to the file: two writers would
// interleave lines, and a "state as of" record that can land after the
// requests it describes is worse than none.
type traceStateTracer struct {
	cfg    config.TraceStateConfig
	mask   *traceMask
	state  traceStateFunc
	logger *logmon.Monitor

	snapshot atomic.Pointer[traceSnapshot]

	// sink is nil until start; checkpointLine never reads it, so the writer
	// can be handed that method before the sink exists.
	sink *traceWriter

	// mu guards startedAt and serializes refreshes. Each event type is
	// delivered on its own goroutine, so the handlers run concurrently with
	// each other.
	mu sync.Mutex
	// startedAt is when each process last entered the starting state.
	// Process management keeps no such timestamp, so it is observed here.
	startedAt map[string]time.Time

	// configOnce renders and caches the effective configuration. The
	// configuration is fixed for the lifetime of a Server, so this runs once
	// however many files are rotated.
	configOnce func() (json.RawMessage, string)

	cancels []context.CancelFunc
}

// newTraceStateTracer builds the tracer for cfg, or returns nil when no state
// record is enabled. It takes an initial snapshot but subscribes to nothing;
// start does that, once the sink it emits into exists.
func newTraceStateTracer(cfg config.TraceConfig, mask *traceMask, state traceStateFunc, logger *logmon.Monitor) *traceStateTracer {
	if !cfg.Enabled || state == nil {
		return nil
	}
	if !cfg.State.Backend && !cfg.State.Config && !cfg.State.Checkpoint {
		return nil
	}
	t := &traceStateTracer{
		cfg:       cfg.State,
		mask:      mask,
		state:     state,
		logger:    logger,
		startedAt: make(map[string]time.Time),
	}
	t.configOnce = sync.OnceValues(t.renderConfig)
	t.refresh()
	return t
}

// start attaches the sink and subscribes to the event bus. The subscriptions
// are process-wide (the dispatcher is a package-level default), which is why
// Close has to cancel them: a hot reload builds a new Server, and a retired
// tracer that kept listening would keep writing into a sink that is closing.
func (t *traceStateTracer) start(sink *traceWriter) {
	if t == nil {
		return
	}
	t.sink = sink
	if t.cfg.Backend || t.cfg.Checkpoint {
		t.cancels = append(t.cancels, event.On(t.onProcessStateChange))
	}
	if t.cfg.Config || t.cfg.Checkpoint {
		t.cancels = append(t.cancels, event.On(t.onConfigFileChanged))
	}
}

// Close unsubscribes. It does not touch the sink: the sink's own Close drains
// and finishes the file, and it must run after this so a record produced by a
// last in-flight event is still written.
func (t *traceStateTracer) Close() {
	if t == nil {
		return
	}
	for _, cancel := range t.cancels {
		cancel()
	}
	t.cancels = nil
}

// onProcessStateChange writes one backend record and refreshes the snapshot.
// Both happen for every transition even when only one of the two switches is
// on, because a checkpoint written from a stale snapshot is a wrong record
// rather than a missing one.
func (t *traceStateTracer) onProcessStateChange(e swaputil.ProcessStateChangeEvent) {
	now := time.Now()
	t.mu.Lock()
	// The start time is observed here because process management does not
	// keep one. Entering "starting" is the transition that begins a process;
	// reaching a terminal state ends it, and keeping a stale timestamp would
	// attribute the next run's requests to the previous start.
	switch e.NewState {
	case string(process.StateStarting):
		t.startedAt[e.ProcessName] = now
	case string(process.StateStopped), string(process.StateShutdown):
		delete(t.startedAt, e.ProcessName)
	}
	started := t.startedAtLocked(e.ProcessName)
	t.mu.Unlock()

	if t.cfg.Backend {
		detail := t.detailFor(e.ProcessName)
		detail.StartedAt = started
		t.emit(&traceBackendRecord{
			Type: traceTypeBackend,
			V:    traceFormatVersion,
			TS:   now.Format(traceTimeFormat),
			Backend: traceBackend{
				ProcessName:        e.ProcessName,
				OldState:           e.OldState,
				NewState:           e.NewState,
				traceProcessDetail: detail,
			},
		})
	}
	t.refresh()
}

// onConfigFileChanged writes one record per reload boundary. Both boundaries
// are recorded: the pair brackets the window in which requests may have been
// served by either configuration.
func (t *traceStateTracer) onConfigFileChanged(e swaputil.ConfigFileChangedEvent) {
	state := traceReloadEnd
	if e.State == swaputil.ReloadingStateStart {
		state = traceReloadStart
	}
	if t.cfg.Config {
		t.emit(&traceConfigRecord{
			Type:   traceTypeConfig,
			V:      traceFormatVersion,
			TS:     time.Now().Format(traceTimeFormat),
			Config: traceReload{State: state},
		})
	}
	t.refresh()
}

// emit encodes, masks and queues one record. It goes through the sink's
// ordinary non-blocking write, so a tracer that cannot keep up drops records
// and is counted with the rest rather than stalling the event bus.
func (t *traceStateTracer) emit(rec any) {
	if t.sink == nil {
		return
	}
	line, err := encodeTraceRecord(rec)
	if err != nil {
		if t.logger != nil {
			t.logger.Warnf("trace: encoding a state record failed: %v", err)
		}
		return
	}
	t.sink.write(t.mask.applyPaths(line))
}

// checkpointLine renders the state dump for a file opened at now, or nil when
// checkpoints are off. This is the function the writer goroutine calls, and it
// only reads the published snapshot: no lock of process management is taken
// here, and nothing it does can block on a process that is busy starting.
func (t *traceStateTracer) checkpointLine(now time.Time) []byte {
	if t == nil || !t.cfg.Checkpoint {
		return nil
	}
	snap := t.snapshot.Load()
	if snap == nil {
		return nil
	}
	line, err := encodeTraceRecord(&traceCheckpointRecord{
		Type: traceTypeCheckpoint,
		V:    traceFormatVersion,
		TS:   now.Format(traceTimeFormat),
		Checkpoint: traceCheckpoint{
			Build:       snap.build,
			Profile:     snap.profile,
			Processes:   snap.processes,
			Config:      snap.config,
			ConfigError: snap.configError,
		},
	})
	if err != nil {
		if t.logger != nil {
			t.logger.Warnf("trace: encoding the checkpoint failed: %v", err)
		}
		return nil
	}
	return t.mask.applyPaths(line)
}

// refresh re-reads process management and publishes a new snapshot. It runs on
// the tracer's goroutines only.
func (t *traceStateTracer) refresh() {
	if !t.cfg.Checkpoint {
		return
	}
	state := t.state()
	configJSON, configErr := t.configOnce()

	names := make([]string, 0, len(state.Running))
	for name := range state.Running {
		names = append(names, name)
	}
	sort.Strings(names)

	t.mu.Lock()
	processes := make([]traceCheckpointProcess, 0, len(names))
	for _, name := range names {
		detail := t.maskedDetail(state.Models[name])
		detail.StartedAt = t.startedAtLocked(name)
		processes = append(processes, traceCheckpointProcess{
			ProcessName:        name,
			State:              state.Running[name],
			traceProcessDetail: detail,
		})
	}
	t.mu.Unlock()

	t.snapshot.Store(&traceSnapshot{
		build:       state.Build,
		profile:     state.Profile,
		processes:   processes,
		config:      configJSON,
		configError: configErr,
	})
}

// detailFor looks up one model's configured detail, masked. A name with no
// model (a process the configuration no longer has) yields an empty detail
// rather than nothing, so the record still says which process changed.
func (t *traceStateTracer) detailFor(name string) traceProcessDetail {
	return t.maskedDetail(t.state().Models[name])
}

func (t *traceStateTracer) maskedDetail(detail traceProcessDetail) traceProcessDetail {
	detail.Env = t.mask.maskEnv(detail.Env)
	return detail
}

// startedAtLocked formats the observed start time, or nil when the process
// started before this sink was watching. t.mu must be held.
func (t *traceStateTracer) startedAtLocked(name string) *string {
	started, ok := t.startedAt[name]
	if !ok {
		return nil
	}
	text := started.Format(traceTimeFormat)
	return &text
}

// renderConfig converts the effective configuration into the JSON a
// checkpoint embeds. The route is YAML first: the configuration structs carry
// yaml tags and only some of them carry json tags, so marshaling them to JSON
// directly would name half the fields after Go identifiers. Going through YAML
// gives every key the name the configuration file uses.
//
// The intermediate generic value is also where maskEnv reaches the models'
// environments, which no JSON path could select (the name is inside the
// "NAME=value" string).
func (t *traceStateTracer) renderConfig() (json.RawMessage, string) {
	state := t.state()
	if state.ConfigYAML == nil {
		return nil, "no configuration source"
	}
	encoded, err := state.ConfigYAML()
	if err != nil {
		return nil, "marshaling the configuration failed: " + err.Error()
	}
	var value any
	if err := yaml.Unmarshal(encoded, &value); err != nil {
		return nil, "decoding the configuration failed: " + err.Error()
	}
	t.mask.maskEnvInValue(value)
	out, err := json.Marshal(value)
	if err != nil {
		return nil, "encoding the configuration failed: " + err.Error()
	}
	return out, ""
}

// traceState is the Server's side of the seam above: everything the
// state records need that lives outside internal/server's sink.
//
// It is deliberately a plain read of already-resolved values. cfg is the
// effective configuration — macros (${latest-llama}, ${PORT}, ${MODEL_ID},
// ${env.*}) are expanded while it is loaded, and a hot reload builds a new
// Server rather than mutating this one — so the command and upstream here are
// the ones the process actually runs with.
func (s *Server) traceState() traceServerState {
	state := traceServerState{
		Build: traceBuild{
			Version: s.build.Version,
			Commit:  s.build.Commit,
			Date:    s.build.Date,
		},
		Profile: s.ActiveProfile(),
		Running: make(map[string]string),
		Models:  traceModelDetails(s.cfg),
		// Redacted rather than marshaled raw: the checkpoint is the one record
		// that carries the whole configuration, and llama-swap already knows
		// which of its own fields are credentials — including the secret
		// arguments inside a cmd and the secret-named entries of an env, which
		// maskPaths cannot reach into. maskPaths and maskEnv still apply on
		// top, for whatever this does not consider a secret.
		ConfigYAML: s.cfg.RedactedFullYAML,
	}
	if s.local != nil {
		for id, processState := range s.local.RunningModels() {
			state.Running[id] = string(processState)
		}
	}
	return state
}

// traceModelDetails renders every configured model's reproducible
// detail. SanitizedCommand is the same call process.doStart makes, so the argv
// recorded is the argv executed rather than a second parse of the same string.
func traceModelDetails(cfg config.Config) map[string]traceProcessDetail {
	details := make(map[string]traceProcessDetail, len(cfg.Models))
	for id, model := range cfg.Models {
		detail := traceProcessDetail{
			Env:      model.Env,
			Upstream: model.Proxy,
		}
		args, err := model.SanitizedCommand()
		if err != nil {
			detail.CmdError = err.Error()
		} else {
			detail.Cmd = args
		}
		details[id] = detail
	}
	return details
}
