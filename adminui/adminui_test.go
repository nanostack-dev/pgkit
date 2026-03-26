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
	queue, err := qpkg.New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := queue.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure queue schema: %v", err)
	}
	def, err := workflow.Define("adminui-demo", func(b *workflow.Builder) {
		b.ForEach("fanout", func(_ context.Context, _ workflow.StepContext) ([]any, error) {
			return []any{
				map[string]any{"value": 1},
				map[string]any{"value": 2},
				map[string]any{"value": 3},
			}, nil
		}, func(_ context.Context, step workflow.StepContext) (any, error) {
			var input struct {
				Value int `json:"value"`
			}
			if err := step.DecodeInput(&input); err != nil {
				return nil, err
			}
			return map[string]any{"processed": input.Value * 2}, nil
		}, workflow.StepOptions{})
		b.Step("finalize", func(_ context.Context, step workflow.StepContext) (any, error) {
			return map[string]any{"count": len(step.ItemOutputs("fanout"))}, nil
		}, workflow.StepOptions{DependsOn: []string{"fanout"}})
	})
	if err != nil {
		t.Fatalf("define workflow: %v", err)
	}
	module, err := workflow.New(db, queue, def)
	if err != nil {
		t.Fatalf("new workflow module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure workflow schema: %v", err)
	}
	if _, err := module.Publish(ctx, "adminui-demo"); err != nil {
		t.Fatalf("publish workflow: %v", err)
	}
	if err := module.Activate(ctx, "adminui-demo", 1); err != nil {
		t.Fatalf("activate workflow: %v", err)
	}
	worker, err := workflow.NewWorker(module, workflow.WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second})
	if err != nil {
		t.Fatalf("new workflow worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()
	if _, err := queue.Enqueue(ctx, qpkg.EnqueueParams{QueueName: "adminui.audit", Payload: []byte(`{"event":"boot"}`), MaxAttempts: 3}); err != nil {
		t.Fatalf("enqueue queue job: %v", err)
	}
	run, err := module.Start(ctx, "adminui-demo", nil, &workflow.StartRunOptions{CreatedBy: "tester", CorrelationKey: "adminui-run"})
	if err != nil {
		t.Fatalf("start workflow run: %v", err)
	}
	requireEventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRunGraphView(ctx, run.ID)
		return err == nil && view.Run.Status == workflow.RunStatusSucceeded
	})
	ui, err := New(queue, Options{Token: "secret", Workflow: module})
	if err != nil {
		t.Fatalf("new admin ui: %v", err)
	}
	server := httptest.NewServer(ui.Handler())
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/dashboard/snapshot", nil)
	if err != nil {
		t.Fatalf("new snapshot request: %v", err)
	}
	req.SetBasicAuth("admin", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 snapshot, got %d", resp.StatusCode)
	}
	var snapshot snapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot.Queue.Summary.TotalJobs == 0 {
		t.Fatal("expected queue jobs in snapshot")
	}
	if snapshot.Workflow.Runs.Total == 0 {
		t.Fatal("expected workflow runs in snapshot")
	}

	req, err = http.NewRequest(http.MethodGet, server.URL+"/api/dashboard/workflow/runs/"+run.ID, nil)
	if err != nil {
		t.Fatalf("new workflow detail request: %v", err)
	}
	req.SetBasicAuth("admin", "secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get workflow detail: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 workflow detail, got %d", resp.StatusCode)
	}
	var graphView workflow.RunGraphView
	if err := json.NewDecoder(resp.Body).Decode(&graphView); err != nil {
		t.Fatalf("decode workflow detail: %v", err)
	}
	if graphView.Run.ID != run.ID {
		t.Fatalf("expected run id %s, got %s", run.ID, graphView.Run.ID)
	}
	if len(graphView.Nodes) == 0 {
		t.Fatal("expected workflow graph nodes")
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
	defer resp.Body.Close()
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
