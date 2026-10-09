package adminui

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestAdminUISnapshotAndWorkflowRun(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, workflows := newClients(t, ctx, db)
	child := workflow.Define("adminui-child", func(wf *workflow.Context, n int) (int, error) {
		return wf.Step("double", func(context.Context) (int, error) { return n * 2, nil })
	})
	parent := workflow.Define("adminui-demo", func(wf *workflow.Context, n int) (int, error) {
		doubled, err := wf.Call("child", child, n)
		if err != nil {
			return 0, err
		}
		return wf.Step("finalize", func(context.Context) (int, error) { return doubled + 1, nil })
	})
	runWorker(t, ctx, workflows, parent, child)
	if _, err := queue.Enqueue(ctx, qpkg.EnqueueParams{QueueName: "adminui.audit", Payload: []byte(`{"event":"boot"}`), MaxAttempts: 3}); err != nil {
		t.Fatalf("enqueue queue job: %v", err)
	}
	run, err := workflows.Start(ctx, parent, 20, workflow.Key("adminui-run"))
	if err != nil {
		t.Fatalf("start workflow run: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if _, err := run.Result(waitCtx); err != nil {
		t.Fatalf("run: %v", err)
	}
	server := newServer(t, queue, Options{Token: "secret", Workflow: workflows})

	var snapshot snapshotResponse
	getJSON(t, server.URL+"/api/dashboard/snapshot", http.StatusOK, &snapshot)
	if snapshot.Queue.Summary.TotalJobs == 0 || snapshot.Workflow.Runs.Total != 2 {
		t.Fatalf("snapshot = %+v", snapshot)
	}

	var detail workflowRunDetail
	getJSON(t, server.URL+"/api/dashboard/workflow/runs/"+run.ID, http.StatusOK, &detail)
	if detail.Run.ID != run.ID || detail.Run.Status != "succeeded" || *detail.Run.Key != "adminui-run" || *detail.Run.Output != "41" {
		t.Fatalf("run = %+v", detail.Run)
	}
	if len(detail.Steps) != 2 || detail.Steps[0].Name != "child" || detail.Steps[0].Kind != "child" || detail.Steps[1].Name != "finalize" {
		t.Fatalf("steps = %+v", detail.Steps)
	}
	if len(detail.Children) != 1 || detail.Children[0].Workflow != "adminui-child" || *detail.Children[0].ParentRunID != run.ID {
		t.Fatalf("children = %+v", detail.Children)
	}

	var children listResponse[workflowRun]
	getJSON(t, server.URL+"/api/dashboard/workflow/runs?parent_run_id="+run.ID, http.StatusOK, &children)
	if children.Total != 1 {
		t.Fatalf("children = %+v", children)
	}
	getJSON(t, server.URL+"/api/dashboard/workflow/runs/missing", http.StatusNotFound, nil)
}

func TestAdminUICancelsAndRetriesWorkflowRuns(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, workflows := newClients(t, ctx, db)
	approved := workflow.NewSignal[string]("approved")
	flow := workflow.Define("adminui-approval", func(wf *workflow.Context, _ struct{}) (string, error) {
		return wf.Receive(approved, workflow.Forever)
	})
	runWorker(t, ctx, workflows, flow)
	run, err := workflows.Start(ctx, flow, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	requireEventually(t, 10*time.Second, 20*time.Millisecond, func() bool {
		info, err := workflows.GetRun(ctx, run.ID)
		return err == nil && info.Status == workflow.RunWaiting
	})
	server := newServer(t, queue, Options{Token: "secret", Workflow: workflows})

	var cancelled workflowRun
	postJSON(t, server.URL+"/api/dashboard/workflow/runs/"+run.ID+"/cancel", http.StatusOK, &cancelled)
	if cancelled.Status != "cancelled" {
		t.Fatalf("run = %+v", cancelled)
	}
	postJSON(t, server.URL+"/api/dashboard/workflow/runs/"+run.ID+"/cancel", http.StatusOK, nil)

	var retried workflowRun
	postJSON(t, server.URL+"/api/dashboard/workflow/runs/"+run.ID+"/retry", http.StatusOK, &retried)
	if workflow.RunStatus(retried.Status).Finished() {
		t.Fatalf("a retried run is still finished: %+v", retried)
	}
	postJSON(t, server.URL+"/api/dashboard/workflow/runs/"+run.ID+"/retry", http.StatusConflict, nil)
	postJSON(t, server.URL+"/api/dashboard/workflow/runs/missing/cancel", http.StatusNotFound, nil)

	if err := workflows.Signal(ctx, run.ID, approved, "yes"); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if got, err := run.Result(waitCtx); err != nil || got != "yes" {
		t.Fatalf("result = %q, err = %v", got, err)
	}
}

func TestAdminUIWithoutMutationsHasNoWorkflowActions(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, workflows := newClients(t, ctx, db)
	disabled := false
	server := newServer(t, queue, Options{Token: "secret", Workflow: workflows, EnableMutations: &disabled})

	for _, action := range []string{"cancel", "retry"} {
		postJSON(t, server.URL+"/api/dashboard/workflow/runs/any/"+action, http.StatusMethodNotAllowed, nil)
	}
}

func newClients(t *testing.T, ctx context.Context, db *sql.DB) (*qpkg.Client, *workflow.Client) {
	t.Helper()
	queue, err := qpkg.New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := queue.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure queue schema: %v", err)
	}
	workflows, err := workflow.New(queue)
	if err != nil {
		t.Fatalf("new workflow client: %v", err)
	}
	if err := workflows.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure workflow schema: %v", err)
	}
	return queue, workflows
}

func runWorker(t *testing.T, ctx context.Context, workflows *workflow.Client, definitions ...workflow.Definition) {
	t.Helper()
	worker, err := workflows.Worker("adminui-test").Workflows(definitions...).Pickup(qpkg.OnEnqueue().RescanEvery(100 * time.Millisecond)).Build()
	if err != nil {
		t.Fatalf("build worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = worker.Run(workerCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	<-worker.Ready()
}

func newServer(t *testing.T, queue *qpkg.Client, options Options) *httptest.Server {
	t.Helper()
	ui, err := New(queue, options)
	if err != nil {
		t.Fatalf("new admin ui: %v", err)
	}
	server := httptest.NewServer(ui.Handler())
	t.Cleanup(server.Close)
	return server
}

func getJSON(t *testing.T, url string, wantStatus int, target any) {
	t.Helper()
	request(t, http.MethodGet, url, wantStatus, target)
}

func postJSON(t *testing.T, url string, wantStatus int, target any) {
	t.Helper()
	request(t, http.MethodPost, url, wantStatus, target)
}

func request(t *testing.T, method, url string, wantStatus int, target any) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("admin", "secret")
	req.Header.Set("X-Requested-With", "pgkit-admin-ui")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status %d, want %d", method, url, resp.StatusCode, wantStatus)
	}
	if target != nil {
		if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
			t.Fatalf("decode %s: %v", url, err)
		}
	}
}

func TestAdminUIRequiresAuthAndSupportsMutations(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, err := qpkg.New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := queue.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	ui, err := New(queue, Options{Token: "secret"})
	if err != nil {
		t.Fatalf("new admin ui: %v", err)
	}
	server := httptest.NewServer(ui.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/dashboard/queue/summary")
	if err != nil {
		t.Fatalf("get without auth: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/dashboard/queue/jobs", strings.NewReader(`{"queue_name":"mutations","payload":{"hello":"world"},"max_attempts":2}`))
	if err != nil {
		t.Fatalf("new mutation request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-With", "pgkit-admin-ui")
	req.SetBasicAuth("admin", "secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post mutation: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 mutation, got %d", resp.StatusCode)
	}
	jobs, err := queue.ListJobs(ctx, qpkg.ListJobsParams{Limit: 10, QueueName: "mutations"})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 enqueued job, got %d", len(jobs))
	}
}

func createTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	pg, connString := startPostgres(t, ctx)
	t.Cleanup(func() {
		_ = pg.Terminate(ctx)
	})
	db, err := sql.Open("pgx", connString)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	if err := waitForPing(ctx, db, 20*time.Second); err != nil {
		t.Fatalf("db ping: %v", err)
	}
	return db
}

func startPostgres(t *testing.T, ctx context.Context) (testcontainers.Container, string) {
	t.Helper()
	pg, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("pgkit_test"),
		postgres.WithUsername("pgkit"),
		postgres.WithPassword("pgkit"),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	connString, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pg.Terminate(ctx)
		t.Fatalf("postgres connection string: %v", err)
	}
	return pg, connString
}

func waitForPing(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func requireEventually(t *testing.T, timeout, interval time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatal("condition not met in time")
}

func TestAdminUIProtectsMutations(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, _ := newClients(t, ctx, db)
	server := newServer(t, queue, Options{Token: "secret"})
	host := strings.TrimPrefix(server.URL, "http://")
	enqueue := func(user, password string, headers map[string]string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, server.URL+"/api/dashboard/queue/jobs", strings.NewReader(`{"queue_name":"guarded","payload":{}}`))
		if err != nil {
			t.Fatal(err)
		}
		if user != "" {
			req.SetBasicAuth(user, password)
		}
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	cases := []struct {
		name     string
		user     string
		password string
		headers  map[string]string
		want     int
	}{
		{"no credentials", "", "", map[string]string{"X-Requested-With": "pgkit-admin-ui"}, http.StatusUnauthorized},
		{"wrong token", "admin", "wrong", map[string]string{"X-Requested-With": "pgkit-admin-ui"}, http.StatusUnauthorized},
		{"no CSRF proof", "admin", "secret", nil, http.StatusForbidden},
		{"origin containing the host", "admin", "secret", map[string]string{"Origin": "http://" + host + ".evil.example"}, http.StatusForbidden},
		{"other origin", "admin", "secret", map[string]string{"Origin": "https://evil.example"}, http.StatusForbidden},
		{"same origin", "admin", "secret", map[string]string{"Origin": server.URL}, http.StatusCreated},
		{"admin UI header", "admin", "secret", map[string]string{"X-Requested-With": "pgkit-admin-ui"}, http.StatusCreated},
	}
	for _, tc := range cases {
		if got := enqueue(tc.user, tc.password, tc.headers); got != tc.want {
			t.Fatalf("%s: status %d, want %d", tc.name, got, tc.want)
		}
	}
	jobs, err := queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1, QueueName: "guarded"})
	if err != nil || jobs != 2 {
		t.Fatalf("guarded jobs = %d (err %v), want only the two accepted mutations", jobs, err)
	}
}

func TestAdminUIHidesServerErrorDetails(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, workflows := newClients(t, ctx, db)
	server := newServer(t, queue, Options{Token: "secret", Workflow: workflows})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"/api/dashboard/snapshot", "/api/dashboard/workflow/runs", "/api/dashboard/workflow/runs/any"} {
		var body map[string]string
		getJSON(t, server.URL+path, http.StatusInternalServerError, &body)
		if body["error"] != "Internal Server Error" {
			t.Fatalf("%s leaked %q", path, body["error"])
		}
	}
}
