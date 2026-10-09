package adminui

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/nanostack-dev/pgkit/workflow"
)

func TestConfigReflectsMutationSwitchAndWorkflows(t *testing.T) {
	queue := newOfflineQueue(t)
	workflows, err := workflow.New(queue)
	if err != nil {
		t.Fatal(err)
	}
	enabled, disabled := true, false
	cases := []struct {
		name    string
		options Options
		want    dashboardConfig
	}{
		{"mutations and workflows", Options{Token: "secret", Workflow: workflows, EnableMutations: &enabled, Limit: 35}, dashboardConfig{MutationsEnabled: true, WorkflowsEnabled: true, PageSize: 35}},
		{"read only without workflows", Options{Token: "secret", EnableMutations: &disabled}, dashboardConfig{PageSize: defaultListLimit}},
		{"page size is capped", Options{Token: "secret", EnableMutations: &enabled, Limit: 5000}, dashboardConfig{MutationsEnabled: true, PageSize: maxListLimit}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newServer(t, queue, tc.options)
			var got dashboardConfig
			getJSON(t, server.URL+"/api/dashboard/config", http.StatusOK, &got)
			if got != tc.want {
				t.Fatalf("config = %+v, want %+v", got, tc.want)
			}
			raw := getMap(t, server.URL+"/api/dashboard/config", http.StatusOK)
			requireKeys(t, "config", raw, "mutations_enabled", "workflows_enabled", "page_size")
		})
	}
}

func TestListParametersAreClampedAndValidated(t *testing.T) {
	queue := newOfflineQueue(t)
	server := newServer(t, queue, Options{Token: "secret"})

	limits := map[string]int{"": defaultListLimit, "?limit=1000": maxListLimit, "?limit=0": 1, "?limit=-3": 1, "?limit=7": 7, "?limit=abc": defaultListLimit}
	for query, want := range limits {
		var runs listResponse[runSummary]
		getJSON(t, server.URL+"/api/dashboard/workflow/runs"+query, http.StatusOK, &runs)
		if runs.Limit != want || runs.Offset != 0 || runs.Total != 0 || runs.Items == nil || len(runs.Items) != 0 {
			t.Fatalf("runs%s = %+v, want limit %d and an empty list", query, runs, want)
		}
	}
	var offset listResponse[runSummary]
	getJSON(t, server.URL+"/api/dashboard/workflow/runs?offset=40", http.StatusOK, &offset)
	if offset.Offset != 40 {
		t.Fatalf("offset = %+v", offset)
	}

	requireErrorBody(t, server.URL+"/api/dashboard/workflow/runs?offset=-1", http.StatusBadRequest, "offset must not be negative")
	requireErrorBody(t, server.URL+"/api/dashboard/queue/jobs?offset=-1", http.StatusBadRequest, "offset must not be negative")
	requireErrorBody(t, server.URL+"/api/dashboard/queue/jobs?search="+strings.Repeat("x", 300), http.StatusBadRequest, "search is too long")
	requireErrorBody(t, server.URL+"/api/dashboard/workflow/runs?search="+strings.Repeat("x", 300), http.StatusBadRequest, "search is too long")
	requireErrorBody(t, server.URL+"/api/dashboard/overview?range=7d", http.StatusBadRequest, "range must be 1h or 24h")
	requireErrorBody(t, server.URL+"/api/dashboard/overview?range=1H", http.StatusBadRequest, "range must be 1h or 24h")
}

func TestWorkflowEndpointsAreEmptyWithoutWorkflows(t *testing.T) {
	queue := newOfflineQueue(t)
	server := newServer(t, queue, Options{Token: "secret"})

	workflows := getMap(t, server.URL+"/api/dashboard/workflow/workflows", http.StatusOK)
	items, ok := workflows["items"].([]any)
	if !ok || len(items) != 0 {
		t.Fatalf("workflows = %v", workflows)
	}
	requireErrorBody(t, server.URL+"/api/dashboard/workflow/runs/any", http.StatusNotFound, "workflows are not enabled")
	requireErrorBody(t, server.URL+"/api/dashboard/workflow/runs/any/tree", http.StatusNotFound, "workflows are not enabled")
}

func TestSPARoutesAndResponseHeaders(t *testing.T) {
	queue := newOfflineQueue(t)
	assets := fstest.MapFS{
		"index.html": {Data: []byte("<html>pgkit admin</html>")},
		"_app/x.js":  {Data: []byte("export {}")},
	}
	server := newServer(t, queue, Options{Token: "secret", Assets: assets})

	for _, route := range []string{"/", "/queues", "/locks", "/workflows", "/workflows/abc"} {
		resp, body := rawRequest(t, http.MethodGet, server.URL+route, "", nil)
		if resp.StatusCode != http.StatusOK || string(body) != "<html>pgkit admin</html>" {
			t.Fatalf("GET %s: status %d, body %q", route, resp.StatusCode, body)
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
			t.Fatalf("GET %s: Cache-Control = %q", route, got)
		}
		if got := resp.Header.Get("Content-Type"); got != "text/html; charset=utf-8" {
			t.Fatalf("GET %s: Content-Type = %q", route, got)
		}
		if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("GET %s: X-Content-Type-Options = %q", route, got)
		}
	}

	resp, body := rawRequest(t, http.MethodGet, server.URL+"/_app/x.js", "", nil)
	if resp.StatusCode != http.StatusOK || string(body) != "export {}" {
		t.Fatalf("asset: status %d, body %q", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("asset Cache-Control = %q", got)
	}
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("asset X-Content-Type-Options = %q", got)
	}

	resp, _ = rawRequest(t, http.MethodGet, server.URL+"/_app/missing.js", "", nil)
	if resp.StatusCode != http.StatusNotFound || strings.Contains(resp.Header.Get("Cache-Control"), "immutable") {
		t.Fatalf("missing asset: status %d, Cache-Control %q", resp.StatusCode, resp.Header.Get("Cache-Control"))
	}

	resp, _ = rawRequest(t, http.MethodGet, server.URL+"/api/dashboard/config", "", nil)
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("api X-Content-Type-Options = %q", got)
	}
}

func TestUnauthenticatedRequestsAreRejected(t *testing.T) {
	queue := newOfflineQueue(t)
	server := newServer(t, queue, Options{Token: "secret", Assets: fstest.MapFS{"index.html": {Data: []byte("x")}}})

	for _, route := range []string{"/locks", "/_app/x.js", "/api/dashboard/config", "/api/dashboard/overview", "/api/dashboard/locks", "/api/dashboard/queue/queues", "/api/dashboard/queue/jobs/1", "/api/dashboard/workflow/workflows", "/api/dashboard/workflow/runs/a/tree"} {
		resp, err := http.Get(server.URL + route)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET %s: status %d, want 401", route, resp.StatusCode)
		}
		if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("GET %s: X-Content-Type-Options = %q", route, got)
		}
	}
}

func TestStaticAssetsAreGzippedWhenAccepted(t *testing.T) {
	queue := newOfflineQueue(t)
	script := strings.Repeat("export const value = 'pgkit admin';\n", 200)
	server := newServer(t, queue, Options{Token: "secret", Assets: fstest.MapFS{
		"index.html":   {Data: []byte("<html>pgkit admin</html>")},
		"_app/big.js":  {Data: []byte(script)},
		"_app/tiny.js": {Data: []byte("export {}")},
	}})

	resp, body := rawRequest(t, http.MethodGet, server.URL+"/_app/big.js", "", map[string]string{"Accept-Encoding": "br, gzip;q=0.8"})
	if resp.Header.Get("Content-Encoding") != "gzip" || resp.Header.Get("Vary") != "Accept-Encoding" {
		t.Fatalf("headers = %v", resp.Header)
	}
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil || string(decoded) != script {
		t.Fatalf("decoded %d bytes, err %v", len(decoded), err)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/javascript; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}

	resp, body = rawRequest(t, http.MethodGet, server.URL+"/_app/big.js", "", map[string]string{"Accept-Encoding": "gzip;q=0"})
	if resp.Header.Get("Content-Encoding") != "" || string(body) != script {
		t.Fatalf("refused gzip: encoding %q, %d bytes", resp.Header.Get("Content-Encoding"), len(body))
	}

	resp, body = rawRequest(t, http.MethodGet, server.URL+"/_app/tiny.js", "", map[string]string{"Accept-Encoding": "gzip"})
	if resp.Header.Get("Content-Encoding") != "" || string(body) != "export {}" {
		t.Fatalf("tiny asset: encoding %q, body %q", resp.Header.Get("Content-Encoding"), body)
	}

	resp, _ = rawRequest(t, http.MethodGet, server.URL+"/_app/", "", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("directory listing: status %d", resp.StatusCode)
	}
}
