package adminui

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	qpkg "github.com/nanostack-dev/pgkit/queue"
)

func newOfflineQueue(t *testing.T) *qpkg.Client {
	t.Helper()
	db, err := sql.Open("pgx", "postgres://pgkit:pgkit@127.0.0.1:1/pgkit_test?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	queue, err := qpkg.New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	return queue
}

func rawRequest(t *testing.T, method, target, body string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("admin", "secret")
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	defer func() { _ = resp.Body.Close() }()
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, target, err)
	}
	return resp, content
}

func getMap(t *testing.T, target string, wantStatus int) map[string]any {
	t.Helper()
	resp, content := rawRequest(t, http.MethodGet, target, "", nil)
	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s: status %d, want %d: %s", target, resp.StatusCode, wantStatus, content)
	}
	var decoded map[string]any
	if err := json.Unmarshal(content, &decoded); err != nil {
		t.Fatalf("decode %s: %v: %s", target, err, content)
	}
	return decoded
}

func requireErrorBody(t *testing.T, target string, wantStatus int, wantMessage string) {
	t.Helper()
	body := getMap(t, target, wantStatus)
	if body["error"] != wantMessage {
		t.Fatalf("GET %s: error = %v, want %q", target, body["error"], wantMessage)
	}
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func requireKeys(t *testing.T, label string, values map[string]any, want ...string) {
	t.Helper()
	slices.Sort(want)
	if got := sortedKeys(values); !slices.Equal(got, want) {
		t.Fatalf("%s keys = %v, want %v", label, got, want)
	}
}

func mustParseTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}

func requireBucketLayout(t *testing.T, starts []string, now string, seconds, count int) {
	t.Helper()
	if len(starts) != count {
		t.Fatalf("buckets = %d, want %d", len(starts), count)
	}
	step := time.Duration(seconds) * time.Second
	for i, raw := range starts {
		start := mustParseTime(t, raw)
		if start.Unix()%int64(seconds) != 0 {
			t.Fatalf("bucket %d starts at %s, not aligned to %ds", i, raw, seconds)
		}
		if i > 0 && !start.Equal(mustParseTime(t, starts[i-1]).Add(step)) {
			t.Fatalf("bucket %d starts at %s, previous %s", i, raw, starts[i-1])
		}
	}
	last := mustParseTime(t, starts[len(starts)-1])
	current := mustParseTime(t, now)
	if current.Before(last) || !current.Before(last.Add(step)) {
		t.Fatalf("now %s outside last bucket starting %s", now, starts[len(starts)-1])
	}
}

func bucketContaining(t *testing.T, starts []string, seconds int, at time.Time) int {
	t.Helper()
	step := time.Duration(seconds) * time.Second
	for i, raw := range starts {
		start := mustParseTime(t, raw)
		if !at.Before(start) && at.Before(start.Add(step)) {
			return i
		}
	}
	t.Fatalf("%s is outside every bucket", at)
	return -1
}
