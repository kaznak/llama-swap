//go:build unix

package server

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/mostlygeek/llama-swap/internal/config"
	"github.com/mostlygeek/llama-swap/internal/logmon"
)

// TestCaptureLog_FIFOTargetDoesNotBlock is the reason the sink opens its file
// on a background goroutine. open(2) on a FIFO for writing blocks until a
// reader attaches, so an eager open would mean llama-swap could not start
// until somebody was reading the log. Here nothing on the request path waits:
// records queue, and the stream starts flowing the moment a reader shows up.
func TestCaptureLog_FIFOTargetDoesNotBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "captures.fifo")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	mm := newTestMetricsMonitor(t, logmon.NewWriter(io.Discard), 10, 0)
	t.Cleanup(func() {
		if err := mm.Close(); err != nil {
			t.Errorf("metricsMonitor.Close: %v", err)
		}
	})

	attached := make(chan struct{})
	go func() {
		defer close(attached)
		mm.attachCaptureLog(config.CaptureLogConfig{Enabled: true, Path: path})
	}()
	select {
	case <-attached:
	case <-time.After(5 * time.Second):
		t.Fatal("attaching the sink blocked: a FIFO with no reader must not stall startup")
	}

	respBody := []byte(`{"usage":{"prompt_tokens":3,"completion_tokens":4}}`)
	recorded := make(chan struct{})
	go func() {
		defer close(recorded)
		r := postRequest("/v1/chat/completions", "m", nil)
		copier := respond(t, http.StatusOK, "application/json", respBody)
		mm.record("m", r, copier, captureAll, []byte(`{"model":"m"}`), nil)
	}()
	select {
	case <-recorded:
	case <-time.After(5 * time.Second):
		t.Fatal("record blocked: a request must never wait on the capture log")
	}

	// Attach the reader only now, after the record was already queued.
	lines := make(chan string, 1)
	failures := make(chan error, 1)
	go func() {
		f, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			failures <- err
			return
		}
		defer f.Close()
		line, err := bufio.NewReader(f).ReadString('\n')
		if err != nil {
			failures <- err
			return
		}
		lines <- line
	}()

	select {
	case err := <-failures:
		t.Fatalf("reading the FIFO: %v", err)
	case line := <-lines:
		var rec captureLogRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("FIFO line is not valid JSON: %v\nline: %q", err, line)
		}
		if rec.Resp.body() != string(respBody) {
			t.Errorf("resp body = %q, want %q", rec.Resp.body(), respBody)
		}
		if rec.Outcome != outcomeOK {
			t.Errorf("outcome = %q, want %q", rec.Outcome, outcomeOK)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no line arrived after a reader attached to the FIFO")
	}
}
