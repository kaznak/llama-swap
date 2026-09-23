package server

import (
	"strings"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// The trace records what it has and then takes things back out here.
// That order is deliberate: a sink that decides field by field what is worth
// keeping cannot be used to reproduce a request, so the default (an empty
// maskPaths and maskEnv) masks nothing and the configuration names the
// exceptions.
//
// Two consequences follow from it, and both are stated in the configuration
// documentation because an operator has to plan around them:
//
//   - Masking is fail-open. A field nobody listed is written as it is. This
//     is a list of what to hide, not a list of what to allow, so a secret in
//     a header or a command-line flag nobody thought of is in the file.
//   - Bodies are out of reach. req.body and resp.body hold the bytes that
//     went over the wire, verbatim, and rewriting a value inside one would
//     make the record a paraphrase of the traffic rather than a copy of it.
//     A mask path naming a body is refused at startup rather than quietly
//     ignored.

// traceMask is the redaction configured for the sink. A nil *traceMask
// is the "nothing configured" case and every method tolerates it, so callers
// never branch on it.
type traceMask struct {
	// paths are gjson/sjson paths, already filtered of anything that would
	// reach a body.
	paths []string
	// env holds the environment variable names to blank, as a set.
	env map[string]struct{}
}

// traceBodyPaths are the record fields a mask may never touch.
var traceBodyPaths = []string{"req.body", "resp.body"}

// newTraceMask compiles cfg's mask settings, reporting (once, at startup)
// every path it refuses. It returns nil when nothing is configured so the
// common case costs nothing per record.
func newTraceMask(cfg config.TraceConfig, logger *logmon.Monitor) *traceMask {
	m := &traceMask{}
	for _, path := range cfg.MaskPaths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		if reason, bad := traceMaskRefusal(path); bad {
			if logger != nil {
				logger.Warnf("trace.maskPaths: ignoring %q: %s", path, reason)
			}
			continue
		}
		m.paths = append(m.paths, path)
	}
	for _, name := range cfg.MaskEnv {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if m.env == nil {
			m.env = make(map[string]struct{})
		}
		m.env[name] = struct{}{}
	}
	if len(m.paths) == 0 && len(m.env) == 0 {
		return nil
	}
	return m
}

// traceMaskRefusal reports why a configured path cannot be applied. The
// two refusals are a body (verbatim by contract) and the object containing one,
// since replacing that whole object takes the body with it.
func traceMaskRefusal(path string) (string, bool) {
	for _, body := range traceBodyPaths {
		if path == body || strings.HasPrefix(path, body+".") {
			return "request and response bodies are kept verbatim and cannot be masked", true
		}
		if container, _, _ := strings.Cut(body, "."); path == container {
			return "masking " + container + " would remove " + body + ", which is kept verbatim", true
		}
	}
	return "", false
}

// applyPaths replaces the value at each configured path with the redaction
// placeholder, and returns line unchanged when nothing matches.
//
// A path that is not present is skipped rather than set: sjson creates what it
// cannot find, so setting blindly would invent fields (a "backend.cmd" on
// every request record) and change what the absence of a field means.
// Everything sjson does not touch is copied through byte for byte, which is
// what keeps the bodies next to a masked field verbatim.
func (m *traceMask) applyPaths(line []byte) []byte {
	if m == nil || len(m.paths) == 0 {
		return line
	}
	for _, path := range m.paths {
		if !gjson.GetBytes(line, path).Exists() {
			continue
		}
		masked, err := sjson.SetBytes(line, path, config.RedactedPlaceholder)
		if err != nil {
			continue
		}
		line = masked
	}
	return line
}

// maskEnv returns env with the value of every configured name replaced. It is
// applied while the record is built rather than by path, because an
// environment is a list of "NAME=value" strings: the name is inside the value,
// so no path can select one.
//
// An entry without "=" is a name with no value and is left alone. The input
// slice is never modified: it belongs to the configuration and is shared with
// everything else that reads it.
func (m *traceMask) maskEnv(env []string) []string {
	if len(env) == 0 {
		return env
	}
	if m == nil || len(m.env) == 0 {
		return env
	}
	out := make([]string, len(env))
	for i, entry := range env {
		name, _, found := strings.Cut(entry, "=")
		if !found {
			out[i] = entry
			continue
		}
		if _, masked := m.env[name]; masked {
			out[i] = name + "=" + config.RedactedPlaceholder
			continue
		}
		out[i] = entry
	}
	return out
}

// maskEnvInValue walks a decoded configuration and applies maskEnv to every
// "env" list it finds. The effective configuration in a checkpoint carries the
// same environments as the backend records, and it arrives as a generic value
// (config structs are marshaled through YAML to get their configured key
// names), so the walk is over map[string]any rather than over fields.
//
// It rewrites in place: the value is a private copy, decoded for this record.
func (m *traceMask) maskEnvInValue(value any) {
	if m == nil || len(m.env) == 0 {
		return
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "env" {
				if list, ok := child.([]any); ok {
					typed[key] = m.maskEnvList(list)
					continue
				}
			}
			m.maskEnvInValue(child)
		}
	case []any:
		for _, child := range typed {
			m.maskEnvInValue(child)
		}
	}
}

// maskEnvList applies maskEnv to a decoded YAML list, leaving non-string
// elements alone.
func (m *traceMask) maskEnvList(list []any) []any {
	out := make([]any, len(list))
	for i, entry := range list {
		text, ok := entry.(string)
		if !ok {
			out[i] = entry
			continue
		}
		out[i] = m.maskEnv([]string{text})[0]
	}
	return out
}
