package zerologqueue

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/nanostack-dev/pgkit/queue"
	"github.com/rs/zerolog"
)

func decodeLogLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("decode log line %q: %v", buf.String(), err)
	}
	return entry
}

func TestWorkerCallbacksOnJobFailed(t *testing.T) {
	var buf bytes.Buffer
	onJobFailed, _ := WorkerCallbacks(zerolog.New(&buf), "resource search sync", nil)

	onJobFailed(context.Background(), queue.Job{
		ID:        42,
		QueueName: "resource-search-sync",
		Attempts:  3,
		LastError: sql.NullString{String: "boom", Valid: true},
	})

	entry := decodeLogLine(t, &buf)
	cases := map[string]any{
		"level":      "error",
		"job_id":     float64(42),
		"queue":      "resource-search-sync",
		"attempts":   float64(3),
		"last_error": "boom",
		"message":    "resource search sync job permanently failed",
	}
	for field, want := range cases {
		if got := entry[field]; got != want {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}
}

func TestWorkerCallbacksOnJobStuckDefaultSeverityAlwaysWarns(t *testing.T) {
	tests := []struct {
		name   string
		result queue.ReapResult
	}{
		{"only requeued", queue.ReapResult{Requeued: 2}},
		{"only failed", queue.ReapResult{Failed: 1}},
		{"both", queue.ReapResult{Requeued: 2, Failed: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			_, onJobStuck := WorkerCallbacks(zerolog.New(&buf), "queue", nil)

			onJobStuck(context.Background(), tt.result)

			entry := decodeLogLine(t, &buf)
			if entry["level"] != "warn" {
				t.Errorf("level = %v, want warn (nil severity always warns)", entry["level"])
			}
			if entry["requeued"] != float64(tt.result.Requeued) {
				t.Errorf("requeued = %v, want %v", entry["requeued"], tt.result.Requeued)
			}
			if entry["failed"] != float64(tt.result.Failed) {
				t.Errorf("failed = %v, want %v", entry["failed"], tt.result.Failed)
			}
			if entry["message"] != "queue jobs stuck in processing were reaped" {
				t.Errorf("message = %v", entry["message"])
			}
		})
	}
}

func TestWorkerCallbacksOnJobStuckCustomSeverity(t *testing.T) {
	var buf bytes.Buffer
	_, onJobStuck := WorkerCallbacks(zerolog.New(&buf), "queue", EscalateOnFailure)

	onJobStuck(context.Background(), queue.ReapResult{Requeued: 5})
	if entry := decodeLogLine(t, &buf); entry["level"] != "warn" {
		t.Errorf("requeued-only level = %v, want warn", entry["level"])
	}

	buf.Reset()
	onJobStuck(context.Background(), queue.ReapResult{Failed: 1})
	if entry := decodeLogLine(t, &buf); entry["level"] != "error" {
		t.Errorf("failed level = %v, want error", entry["level"])
	}
}

func TestEscalateOnFailure(t *testing.T) {
	tests := []struct {
		name   string
		result queue.ReapResult
		want   zerolog.Level
	}{
		{"only requeued", queue.ReapResult{Requeued: 3}, zerolog.WarnLevel},
		{"only failed", queue.ReapResult{Failed: 1}, zerolog.ErrorLevel},
		{"both", queue.ReapResult{Requeued: 3, Failed: 1}, zerolog.ErrorLevel},
		{"neither", queue.ReapResult{}, zerolog.WarnLevel},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EscalateOnFailure(tt.result); got != tt.want {
				t.Errorf("EscalateOnFailure(%+v) = %v, want %v", tt.result, got, tt.want)
			}
		})
	}
}
