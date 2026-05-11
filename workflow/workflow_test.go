package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPublishActivateAndStartVersionedWorkflow(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	v1, err := Define("provision", func(b *Builder) {
		b.Title("Provision")
		b.Step("first", func(_ context.Context, _ StepContext) (any, error) {
			return map[string]any{"step": 1}, nil
		}, StepOptions{})
	})
	if err != nil {
		t.Fatalf("define v1: %v", err)
	}

	module, err := New(db, queue, v1)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	pub1, err := module.Publish(ctx, "provision")
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	if !pub1.Published || pub1.Definition.Version != 1 {
		t.Fatalf("unexpected publish v1 result: %+v", pub1)
	}
	if err := module.Activate(ctx, "provision", 1); err != nil {
		t.Fatalf("activate v1: %v", err)
	}

	v2, err := Define("provision", func(b *Builder) {
		b.Title("Provision")
		b.Step("first", func(_ context.Context, _ StepContext) (any, error) {
			return map[string]any{"step": 1}, nil
		}, StepOptions{})
		b.Step("second", func(_ context.Context, _ StepContext) (any, error) {
			return map[string]any{"step": 2}, nil
		}, StepOptions{DependsOn: []string{"first"}})
	})
	if err != nil {
		t.Fatalf("define v2: %v", err)
	}
	if err := module.Register(v2); err != nil {
		t.Fatalf("register v2: %v", err)
	}
	pub2, err := module.Publish(ctx, "provision")
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	if !pub2.Published || pub2.Definition.Version != 2 {
		t.Fatalf("unexpected publish v2 result: %+v", pub2)
	}

	defs, err := module.ListDefinitions(ctx, "provision")
	if err != nil {
		t.Fatalf("list definitions: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("expected 2 definitions, got %d", len(defs))
	}

	run, err := module.StartVersion(ctx, "provision", 1, map[string]any{"tenant": "a"}, nil)
	if err != nil {
		t.Fatalf("start v1: %v", err)
	}
	if run.WorkflowVersion != 1 {
		t.Fatalf("expected run version 1, got %d", run.WorkflowVersion)
	}

	view, err := module.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if len(view.Steps) != 1 || view.Steps[0].StepName != "first" {
		t.Fatalf("unexpected v1 steps: %+v", view.Steps)
	}

	if err := module.Activate(ctx, "provision", 2); err != nil {
		t.Fatalf("activate v2: %v", err)
	}
	active, err := module.GetActiveDefinition(ctx, "provision")
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if active.Version != 2 || active.Status != DefinitionStatusActive {
		t.Fatalf("unexpected active definition: %+v", active)
	}
	graph := Graph{}
	if err := json.Unmarshal(active.GraphJSON, &graph); err != nil {
		t.Fatalf("unmarshal graph: %v", err)
	}
	if len(graph.Nodes) != 2 {
		t.Fatalf("expected 2 graph nodes, got %d", len(graph.Nodes))
	}
}

func TestWorkflowVersionResolutionAcrossFreshModule(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	v1, err := Define("restartable", func(b *Builder) {
		b.Step("only", func(_ context.Context, _ StepContext) (any, error) {
			return map[string]any{"version": 1}, nil
		}, StepOptions{})
	})
	if err != nil {
		t.Fatalf("define v1: %v", err)
	}
	v2, err := Define("restartable", func(b *Builder) {
		b.Step("only", func(_ context.Context, _ StepContext) (any, error) {
			return map[string]any{"version": 2}, nil
		}, StepOptions{})
		b.Step("after", func(_ context.Context, step StepContext) (any, error) {
			var prior struct {
				Version int `json:"version"`
			}
			if err := step.Output("only", &prior); err != nil {
				return nil, err
			}
			return map[string]any{"after": prior.Version}, nil
		}, StepOptions{DependsOn: []string{"only"}})
	})
	if err != nil {
		t.Fatalf("define v2: %v", err)
	}

	module1, err := New(db, queue, v1, v2)
	if err != nil {
		t.Fatalf("new module1: %v", err)
	}
	if err := module1.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module1.Publish(ctx, "restartable"); err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	if _, err := module1.Publish(ctx, "restartable"); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	if err := module1.Activate(ctx, "restartable", 2); err != nil {
		t.Fatalf("activate v2: %v", err)
	}

	module2, err := New(db, queue, v1, v2)
	if err != nil {
		t.Fatalf("new module2: %v", err)
	}
	worker, err := NewWorker(module2, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module2.Start(ctx, "restartable", map[string]any{"id": 7}, nil)
	if err != nil {
		t.Fatalf("start with fresh module: %v", err)
	}
	requireEventually(t, 6*time.Second, 50*time.Millisecond, func() bool {
		view, err := module2.GetRun(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusSucceeded
	})
	view, err := module2.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if len(view.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(view.Steps))
	}
}

func TestWorkflowWorkerExecutesDependentSteps(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	var firstCalled atomic.Int32
	var secondCalled atomic.Int32

	def, err := Define("dependent", func(b *Builder) {
		b.Step("first", func(_ context.Context, step StepContext) (any, error) {
			firstCalled.Add(1)
			var input struct {
				Name string `json:"name"`
			}
			if err := step.DecodeInput(&input); err != nil {
				return nil, err
			}
			return map[string]any{"greeting": "hello " + input.Name}, nil
		}, StepOptions{})
		b.Step("second", func(_ context.Context, step StepContext) (any, error) {
			secondCalled.Add(1)
			var first struct {
				Greeting string `json:"greeting"`
			}
			if err := step.Output("first", &first); err != nil {
				return nil, err
			}
			return map[string]any{"message": first.Greeting + "!"}, nil
		}, StepOptions{DependsOn: []string{"first"}})
	})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "dependent"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "dependent", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}

	worker, err := NewWorker(module, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second, BatchSizePerQueue: 10})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module.Start(ctx, "dependent", map[string]any{"name": "rowan"}, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	requireEventually(t, 6*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusSucceeded
	})

	if firstCalled.Load() != 1 || secondCalled.Load() != 1 {
		t.Fatalf("unexpected step calls: first=%d second=%d", firstCalled.Load(), secondCalled.Load())
	}
	view, err := module.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run final: %v", err)
	}
	if len(view.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(view.Steps))
	}
	statuses := map[string]StepStatus{}
	for _, step := range view.Steps {
		statuses[step.StepName] = step.Status
	}
	if statuses["first"] != StepStatusSucceeded || statuses["second"] != StepStatusSucceeded {
		t.Fatalf("unexpected step statuses: %+v", statuses)
	}
}

func TestWorkflowTxStepPersistsAtomicData(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	if _, err := db.ExecContext(ctx, `CREATE TABLE workflow_test_records (id SERIAL PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create records table: %v", err)
	}

	def, err := Define("tx-step", func(b *Builder) {
		b.TxStep("insert", func(ctx context.Context, tx *sql.Tx, step StepContext) (any, error) {
			var input struct {
				Value string `json:"value"`
			}
			if err := step.DecodeInput(&input); err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO workflow_test_records (value) VALUES ($1)`, input.Value); err != nil {
				return nil, err
			}
			return map[string]any{"inserted": input.Value}, nil
		}, StepOptions{})
	})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "tx-step"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "tx-step", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}
	worker, err := NewWorker(module, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module.Start(ctx, "tx-step", map[string]any{"value": "persist-me"}, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	requireEventually(t, 6*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusSucceeded
	})
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_test_records WHERE value = 'persist-me'`).Scan(&count); err != nil {
		t.Fatalf("count record: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 persisted record, got %d", count)
	}
}

func TestWorkflowRetryAndTerminalFailure(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	var attempts atomic.Int32
	var failedRuns atomic.Int32

	def, err := Define("retry-flow", func(b *Builder) {
		b.Step("unstable", func(_ context.Context, _ StepContext) (any, error) {
			n := attempts.Add(1)
			if n < 3 {
				return nil, fmt.Errorf("attempt %d failed", n)
			}
			return map[string]any{"ok": true}, nil
		}, StepOptions{RetryPolicy: RetryPolicy{MaxAttempts: 3, BackoffBase: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond}})
	})
	if err != nil {
		t.Fatalf("define retry flow: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "retry-flow"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "retry-flow", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}
	worker, err := NewWorker(module, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second, OnRunFailed: func(_ context.Context, _ RunRecord) { failedRuns.Add(1) }})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module.Start(ctx, "retry-flow", map[string]any{"id": 1}, nil)
	if err != nil {
		t.Fatalf("start retry run: %v", err)
	}
	requireEventually(t, 6*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusSucceeded
	})
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
	if failedRuns.Load() != 0 {
		t.Fatalf("expected no failed run callbacks, got %d", failedRuns.Load())
	}

	var nonRetryAttempts atomic.Int32
	def2, err := Define("fail-flow", func(b *Builder) {
		b.Step("always-bad", func(_ context.Context, _ StepContext) (any, error) {
			nonRetryAttempts.Add(1)
			return nil, NonRetryable(errors.New("fatal"))
		}, StepOptions{RetryPolicy: RetryPolicy{MaxAttempts: 5}})
	})
	if err != nil {
		t.Fatalf("define fail flow: %v", err)
	}
	if err := module.Register(def2); err != nil {
		t.Fatalf("register fail flow: %v", err)
	}
	if _, err := module.Publish(ctx, "fail-flow"); err != nil {
		t.Fatalf("publish fail flow: %v", err)
	}
	if err := module.Activate(ctx, "fail-flow", 1); err != nil {
		t.Fatalf("activate fail flow: %v", err)
	}
	run2, err := module.Start(ctx, "fail-flow", nil, nil)
	if err != nil {
		t.Fatalf("start fail flow: %v", err)
	}
	requireEventually(t, 6*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run2.ID)
		return err == nil && view.Run.Status == RunStatusFailed
	})
	if nonRetryAttempts.Load() != 1 {
		t.Fatalf("expected 1 non-retryable attempt, got %d", nonRetryAttempts.Load())
	}
}

func TestDefinitionValidationEdgeCases(t *testing.T) {
	if _, err := Define("", func(_ *Builder) {}); !errors.Is(err, ErrEmptyWorkflowName) {
		t.Fatalf("expected empty workflow name error, got %v", err)
	}
	if _, err := Define("dup", func(b *Builder) {
		b.Step("a", func(_ context.Context, _ StepContext) (any, error) { return nil, nil }, StepOptions{})
		b.Step("a", func(_ context.Context, _ StepContext) (any, error) { return nil, nil }, StepOptions{})
	}); !errors.Is(err, ErrDuplicateStepName) {
		t.Fatalf("expected duplicate step name error, got %v", err)
	}
	if _, err := Define("missing-dep", func(b *Builder) {
		b.Step("a", func(_ context.Context, _ StepContext) (any, error) { return nil, nil }, StepOptions{DependsOn: []string{"missing"}})
	}); !errors.Is(err, ErrUnknownDependency) {
		t.Fatalf("expected unknown dependency error, got %v", err)
	}
	if _, err := Define("cycle", func(b *Builder) {
		b.Step("a", func(_ context.Context, _ StepContext) (any, error) { return nil, nil }, StepOptions{DependsOn: []string{"b"}})
		b.Step("b", func(_ context.Context, _ StepContext) (any, error) { return nil, nil }, StepOptions{DependsOn: []string{"a"}})
	}); !errors.Is(err, ErrDefinitionCycle) {
		t.Fatalf("expected cycle error, got %v", err)
	}
}

func TestDefinitionDiffVersions(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	v1, err := Define("diffable", func(b *Builder) {
		b.Step("a", func(_ context.Context, _ StepContext) (any, error) { return nil, nil }, StepOptions{})
	})
	if err != nil {
		t.Fatalf("define v1: %v", err)
	}
	v2, err := Define("diffable", func(b *Builder) {
		b.Step("a", func(_ context.Context, _ StepContext) (any, error) { return nil, nil }, StepOptions{RetryPolicy: RetryPolicy{MaxAttempts: 5}})
		b.Step("b", func(_ context.Context, _ StepContext) (any, error) { return nil, nil }, StepOptions{DependsOn: []string{"a"}})
	})
	if err != nil {
		t.Fatalf("define v2: %v", err)
	}
	module, err := New(db, queue, v1, v2)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "diffable"); err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	if _, err := module.Publish(ctx, "diffable"); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	diff, err := module.DiffDefinitionVersions(ctx, "diffable", 1, 2)
	if err != nil {
		t.Fatalf("diff versions: %v", err)
	}
	if len(diff.AddedNodes) != 1 || diff.AddedNodes[0] != "b" {
		t.Fatalf("unexpected added nodes: %+v", diff.AddedNodes)
	}
	if len(diff.ChangedNodes) != 1 || diff.ChangedNodes[0] != "a" {
		t.Fatalf("unexpected changed nodes: %+v", diff.ChangedNodes)
	}
	if len(diff.AddedEdges) != 1 || diff.AddedEdges[0].From != "a" || diff.AddedEdges[0].To != "b" {
		t.Fatalf("unexpected added edges: %+v", diff.AddedEdges)
	}
}

func TestWorkflowForEachStep(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	var resolverCalls atomic.Int32
	var childCalls atomic.Int32
	def, err := Define("foreach-flow", func(b *Builder) {
		b.ForEach("fanout", func(_ context.Context, step StepContext) ([]any, error) {
			resolverCalls.Add(1)
			var input struct {
				Values []int `json:"values"`
			}
			if err := step.DecodeRunInput(&input); err != nil {
				return nil, err
			}
			items := make([]any, 0, len(input.Values))
			for _, value := range input.Values {
				items = append(items, map[string]any{"value": value})
			}
			return items, nil
		}, func(_ context.Context, step StepContext) (any, error) {
			childCalls.Add(1)
			var input struct {
				Value int `json:"value"`
			}
			if err := step.DecodeInput(&input); err != nil {
				return nil, err
			}
			return map[string]any{"processed": input.Value * 2}, nil
		}, StepOptions{})
		b.Step("final", func(_ context.Context, step StepContext) (any, error) {
			var first struct {
				Processed int `json:"processed"`
			}
			if err := step.ItemOutput("fanout", "000000", &first); err != nil {
				return nil, err
			}
			return map[string]any{"first": first.Processed}, nil
		}, StepOptions{DependsOn: []string{"fanout"}})
	})
	if err != nil {
		t.Fatalf("define foreach flow: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "foreach-flow"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "foreach-flow", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}
	worker, err := NewWorker(module, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module.Start(ctx, "foreach-flow", map[string]any{"values": []int{1, 2, 3}}, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	requireEventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusSucceeded
	})
	if resolverCalls.Load() != 1 {
		t.Fatalf("expected 1 foreach resolver call, got %d", resolverCalls.Load())
	}
	if childCalls.Load() != 3 {
		t.Fatalf("expected 3 foreach child handler calls, got %d", childCalls.Load())
	}
	view, err := module.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if len(view.Steps) != 5 {
		t.Fatalf("expected 5 step records, got %d", len(view.Steps))
	}
	foreachChildren := 0
	for _, step := range view.Steps {
		if step.StepName == "fanout" && step.ItemKey != "" {
			foreachChildren++
			if step.Status != StepStatusSucceeded {
				t.Fatalf("expected foreach child succeeded, got %+v", step)
			}
		}
	}
	if foreachChildren != 3 {
		t.Fatalf("expected 3 foreach children, got %d", foreachChildren)
	}
}

func TestWorkflowForEachEmptyCollection(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	var resolverCalls atomic.Int32
	var childCalls atomic.Int32
	def, err := Define("foreach-empty", func(b *Builder) {
		b.ForEach("fanout", func(_ context.Context, _ StepContext) ([]any, error) {
			resolverCalls.Add(1)
			return []any{}, nil
		}, func(_ context.Context, _ StepContext) (any, error) {
			childCalls.Add(1)
			return map[string]any{"unexpected": true}, nil
		}, StepOptions{})
		b.Step("final", func(_ context.Context, step StepContext) (any, error) {
			return map[string]any{"run_id": step.RunID}, nil
		}, StepOptions{DependsOn: []string{"fanout"}})
	})
	if err != nil {
		t.Fatalf("define foreach empty flow: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "foreach-empty"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "foreach-empty", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}
	worker, err := NewWorker(module, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module.Start(ctx, "foreach-empty", map[string]any{"values": []int{}}, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	requireEventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusSucceeded
	})
	if resolverCalls.Load() != 1 {
		t.Fatalf("expected 1 foreach resolver call, got %d", resolverCalls.Load())
	}
	if childCalls.Load() != 0 {
		t.Fatalf("expected 0 foreach child handler calls, got %d", childCalls.Load())
	}
	view, err := module.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if len(view.Steps) != 2 {
		t.Fatalf("expected 2 step records, got %d", len(view.Steps))
	}
	statuses := map[string]StepStatus{}
	for _, step := range view.Steps {
		if step.ItemKey != "" {
			t.Fatalf("expected no foreach child steps, got %+v", step)
		}
		statuses[step.StepName] = step.Status
	}
	if statuses["fanout"] != StepStatusSucceeded || statuses["final"] != StepStatusSucceeded {
		t.Fatalf("unexpected step statuses: %+v", statuses)
	}
}

func TestWorkflowForEachChildRetry(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	var resolverCalls atomic.Int32
	var childCalls atomic.Int32
	var flakyCalls atomic.Int32
	def, err := Define("foreach-retry", func(b *Builder) {
		b.ForEach("fanout", func(_ context.Context, step StepContext) ([]any, error) {
			resolverCalls.Add(1)
			var input struct {
				Values []int `json:"values"`
			}
			if err := step.DecodeRunInput(&input); err != nil {
				return nil, err
			}
			items := make([]any, 0, len(input.Values))
			for _, value := range input.Values {
				items = append(items, map[string]any{"value": value})
			}
			return items, nil
		}, func(_ context.Context, step StepContext) (any, error) {
			childCalls.Add(1)
			var input struct {
				Value int `json:"value"`
			}
			if err := step.DecodeInput(&input); err != nil {
				return nil, err
			}
			if step.ItemKey == "000001" && flakyCalls.Add(1) == 1 {
				return nil, fmt.Errorf("transient foreach failure")
			}
			return map[string]any{"processed": input.Value * 10}, nil
		}, StepOptions{RetryPolicy: RetryPolicy{MaxAttempts: 3, BackoffBase: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond}})
		b.Step("final", func(_ context.Context, step StepContext) (any, error) {
			var second struct {
				Processed int `json:"processed"`
			}
			if err := step.ItemOutput("fanout", "000001", &second); err != nil {
				return nil, err
			}
			return map[string]any{"second": second.Processed}, nil
		}, StepOptions{DependsOn: []string{"fanout"}})
	})
	if err != nil {
		t.Fatalf("define foreach retry flow: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "foreach-retry"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "foreach-retry", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}
	worker, err := NewWorker(module, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module.Start(ctx, "foreach-retry", map[string]any{"values": []int{1, 2, 3}}, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	requireEventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusSucceeded
	})
	if resolverCalls.Load() != 1 {
		t.Fatalf("expected 1 foreach resolver call, got %d", resolverCalls.Load())
	}
	if flakyCalls.Load() != 2 {
		t.Fatalf("expected flaky child to run twice, got %d", flakyCalls.Load())
	}
	if childCalls.Load() != 4 {
		t.Fatalf("expected 4 foreach child handler calls, got %d", childCalls.Load())
	}
	view, err := module.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	var final StepRecord
	foreachChildren := 0
	for _, step := range view.Steps {
		if step.StepName == "fanout" && step.ItemKey != "" {
			foreachChildren++
			if step.Status != StepStatusSucceeded {
				t.Fatalf("expected foreach child succeeded, got %+v", step)
			}
		}
		if step.StepName == "final" {
			final = step
		}
	}
	if foreachChildren != 3 {
		t.Fatalf("expected 3 foreach children, got %d", foreachChildren)
	}
	if final.Status != StepStatusSucceeded {
		t.Fatalf("expected final step succeeded, got %+v", final)
	}
}

func TestWorkflowForEachChildTerminalFailure(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	var failedRuns atomic.Int32
	var failedSteps atomic.Int32
	def, err := Define("foreach-terminal", func(b *Builder) {
		b.ForEach("fanout", func(_ context.Context, _ StepContext) ([]any, error) {
			return []any{
				map[string]any{"value": 1},
				map[string]any{"value": 2},
				map[string]any{"value": 3},
			}, nil
		}, func(_ context.Context, step StepContext) (any, error) {
			var input struct {
				Value int `json:"value"`
			}
			if err := step.DecodeInput(&input); err != nil {
				return nil, err
			}
			if input.Value == 2 {
				return nil, NonRetryable(errors.New("bad item"))
			}
			return map[string]any{"processed": input.Value}, nil
		}, StepOptions{RetryPolicy: RetryPolicy{MaxAttempts: 3, BackoffBase: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond}})
		b.Step("final", func(_ context.Context, step StepContext) (any, error) {
			return map[string]any{"run_id": step.RunID}, nil
		}, StepOptions{DependsOn: []string{"fanout"}})
	})
	if err != nil {
		t.Fatalf("define foreach terminal flow: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "foreach-terminal"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "foreach-terminal", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}
	worker, err := NewWorker(module, WorkerConfig{
		PollInterval: 20 * time.Millisecond,
		ReapInterval: time.Second,
		OnRunFailed:  func(_ context.Context, _ RunRecord) { failedRuns.Add(1) },
		OnStepFailed: func(_ context.Context, step StepRecord) {
			if step.StepName == "fanout" && step.ItemKey != "" {
				failedSteps.Add(1)
			}
		},
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module.Start(ctx, "foreach-terminal", nil, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	requireEventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusFailed
	})
	if failedRuns.Load() != 1 {
		t.Fatalf("expected 1 run failure callback, got %d", failedRuns.Load())
	}
	if failedSteps.Load() != 1 {
		t.Fatalf("expected 1 failed foreach child callback, got %d", failedSteps.Load())
	}
	view, err := module.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	statuses := map[string]StepStatus{}
	failedChildren := 0
	for _, step := range view.Steps {
		if step.StepName == "fanout" && step.ItemKey != "" && step.Status == StepStatusFailed {
			failedChildren++
		}
		if step.ItemKey == "" {
			statuses[step.StepName] = step.Status
		}
	}
	if failedChildren != 1 {
		t.Fatalf("expected 1 failed foreach child, got %d", failedChildren)
	}
	if statuses["final"] != StepStatusCancelled {
		t.Fatalf("expected final step to be cancelled after run failure, got %s", statuses["final"])
	}
}

func TestWorkflowIgnoresQueuedStepsAfterRunFailure(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	var retryJobID atomic.Int64
	var retryCalls atomic.Int32
	def, err := Define("inactive-run", func(b *Builder) {
		b.Step("aaa-retry", func(_ context.Context, step StepContext) (any, error) {
			if retryCalls.Add(1) == 1 {
				return nil, fmt.Errorf("retry me once")
			}
			return map[string]any{"step": step.StepName}, nil
		}, StepOptions{RetryPolicy: RetryPolicy{MaxAttempts: 3, BackoffBase: 200 * time.Millisecond, BackoffMax: 200 * time.Millisecond}})
		b.Step("zzz-fail", func(_ context.Context, _ StepContext) (any, error) {
			return nil, NonRetryable(errors.New("boom"))
		}, StepOptions{})
	})
	if err != nil {
		t.Fatalf("define inactive-run flow: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "inactive-run"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "inactive-run", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}
	workerA, err := NewWorker(module, WorkerConfig{PollInterval: 10 * time.Millisecond, ReapInterval: time.Second, BatchSizePerQueue: 4})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = workerA.Run(workerCtx) }()

	run, err := module.Start(ctx, "inactive-run", nil, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	view, err := module.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	for _, step := range view.Steps {
		if !step.QueueJobID.Valid {
			continue
		}
		if step.StepName == "aaa-retry" {
			retryJobID.Store(step.QueueJobID.Int64)
		}
	}
	if retryJobID.Load() == 0 {
		t.Fatalf("expected retry queue job id, got %d", retryJobID.Load())
	}
	requireEventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusFailed
	})
	requireEventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		job, err := queue.GetJob(ctx, retryJobID.Load())
		return err == nil && job.Status == qpkg.StatusDone
	})
	requireEventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRun(ctx, run.ID)
		if err != nil {
			return false
		}
		statuses := map[string]StepStatus{}
		for _, step := range view.Steps {
			statuses[step.StepName] = step.Status
		}
		return statuses["aaa-retry"] == StepStatusCancelled && statuses["zzz-fail"] == StepStatusFailed
	})
	graphView, err := module.GetRunGraphView(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run graph view: %v", err)
	}
	if graphView.Summary.FailedNodes != 1 || graphView.Summary.CancelledNodes != 1 {
		t.Fatalf("unexpected graph summary after failure: %+v", graphView.Summary)
	}
	if retryCalls.Load() > 1 {
		t.Fatalf("expected retry step to stop after run failure, got %d calls", retryCalls.Load())
	}
}

func TestWorkflowRunGraphView(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	var flakyCalls atomic.Int32
	def, err := Define("graph-view", func(b *Builder) {
		b.ForEach("fanout", func(_ context.Context, _ StepContext) ([]any, error) {
			return []any{
				map[string]any{"value": 1},
				map[string]any{"value": 2},
				map[string]any{"value": 3},
			}, nil
		}, func(_ context.Context, step StepContext) (any, error) {
			var input struct {
				Value int `json:"value"`
			}
			if err := step.DecodeInput(&input); err != nil {
				return nil, err
			}
			if step.ItemKey == "000001" && flakyCalls.Add(1) == 1 {
				return nil, fmt.Errorf("retry once")
			}
			return map[string]any{"processed": input.Value * 3}, nil
		}, StepOptions{RetryPolicy: RetryPolicy{MaxAttempts: 3, BackoffBase: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond}})
		b.Step("final", func(_ context.Context, step StepContext) (any, error) {
			outputs := step.ItemOutputs("fanout")
			if len(outputs) != 3 {
				return nil, fmt.Errorf("expected 3 outputs, got %d", len(outputs))
			}
			return map[string]any{"count": len(outputs)}, nil
		}, StepOptions{DependsOn: []string{"fanout"}})
	})
	if err != nil {
		t.Fatalf("define graph view flow: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "graph-view"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "graph-view", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}
	worker, err := NewWorker(module, WorkerConfig{PollInterval: 20 * time.Millisecond, ReapInterval: time.Second})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module.Start(ctx, "graph-view", nil, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	requireEventually(t, 8*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRunGraphView(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusSucceeded && view.Summary.TotalItems == 3
	})
	graphView, err := module.GetRunGraphView(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run graph view: %v", err)
	}
	if graphView.Summary.TotalNodes != 2 || graphView.Summary.SucceededNodes != 2 {
		t.Fatalf("unexpected graph summary: %+v", graphView.Summary)
	}
	if len(graphView.Nodes) != 2 {
		t.Fatalf("expected 2 graph nodes, got %d", len(graphView.Nodes))
	}
	var fanoutNode RunGraphNodeView
	var finalNode RunGraphNodeView
	for _, node := range graphView.Nodes {
		if node.Node.ID == "fanout" {
			fanoutNode = node
		}
		if node.Node.ID == "final" {
			finalNode = node
		}
	}
	if fanoutNode.Status != StepStatusSucceeded {
		t.Fatalf("expected fanout node succeeded, got %+v", fanoutNode)
	}
	if fanoutNode.Step == nil || fanoutNode.Step.Status != StepStatusSucceeded {
		t.Fatalf("expected fanout root step succeeded, got %+v", fanoutNode.Step)
	}
	if len(fanoutNode.Items) != 3 || fanoutNode.ItemCounts.Succeeded != 3 {
		t.Fatalf("unexpected fanout item view: %+v", fanoutNode)
	}
	if finalNode.Status != StepStatusSucceeded || finalNode.Step == nil || finalNode.Step.Status != StepStatusSucceeded {
		t.Fatalf("unexpected final node view: %+v", finalNode)
	}
}

func TestWorkflowForEachHighFanout(t *testing.T) {
	ctx := context.Background()
	db := createWorkflowTestDB(t, ctx)
	queue := createWorkflowQueue(t, ctx, db)

	const itemCount = 64
	var childCalls atomic.Int32
	def, err := Define("foreach-high-fanout", func(b *Builder) {
		b.ForEach("fanout", func(_ context.Context, _ StepContext) ([]any, error) {
			items := make([]any, 0, itemCount)
			for i := 0; i < itemCount; i++ {
				items = append(items, map[string]any{"value": i})
			}
			return items, nil
		}, func(_ context.Context, step StepContext) (any, error) {
			childCalls.Add(1)
			var input struct {
				Value int `json:"value"`
			}
			if err := step.DecodeInput(&input); err != nil {
				return nil, err
			}
			return map[string]any{"processed": input.Value + 1}, nil
		}, StepOptions{})
		b.Step("final", func(_ context.Context, step StepContext) (any, error) {
			outputs := step.ItemOutputs("fanout")
			return map[string]any{"count": len(outputs)}, nil
		}, StepOptions{DependsOn: []string{"fanout"}})
	})
	if err != nil {
		t.Fatalf("define high fanout flow: %v", err)
	}

	module, err := New(db, queue, def)
	if err != nil {
		t.Fatalf("new module: %v", err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := module.Publish(ctx, "foreach-high-fanout"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := module.Activate(ctx, "foreach-high-fanout", 1); err != nil {
		t.Fatalf("activate: %v", err)
	}
	worker, err := NewWorker(module, WorkerConfig{PollInterval: 10 * time.Millisecond, ReapInterval: time.Second, BatchSizePerQueue: 64})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = worker.Run(workerCtx) }()

	run, err := module.Start(ctx, "foreach-high-fanout", nil, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	requireEventually(t, 12*time.Second, 50*time.Millisecond, func() bool {
		view, err := module.GetRunGraphView(ctx, run.ID)
		return err == nil && view.Run.Status == RunStatusSucceeded && view.Summary.TotalItems == itemCount
	})
	if childCalls.Load() != itemCount {
		t.Fatalf("expected %d child calls, got %d", itemCount, childCalls.Load())
	}
	graphView, err := module.GetRunGraphView(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run graph view: %v", err)
	}
	if graphView.Summary.TotalItems != itemCount || graphView.Summary.FailedItems != 0 {
		t.Fatalf("unexpected graph summary: %+v", graphView.Summary)
	}
}

func createWorkflowQueue(t *testing.T, ctx context.Context, db *sql.DB) *qpkg.Client {
	t.Helper()
	queue, err := qpkg.New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if err := queue.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure queue schema: %v", err)
	}
	return queue
}

func createWorkflowTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	pg, connString := startWorkflowPostgres(t, ctx)
	t.Cleanup(func() { _ = pg.Terminate(ctx) })
	db, err := sql.Open("pgx", connString)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := waitWorkflowPing(ctx, db, 20*time.Second); err != nil {
		t.Fatalf("db ping: %v", err)
	}
	return db
}

func startWorkflowPostgres(t *testing.T, ctx context.Context) (testcontainers.Container, string) {
	t.Helper()
	pg, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("pgkit_workflow_test"),
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

func waitWorkflowPing(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}
	return lastErr
}

func requireEventually(t *testing.T, timeout, tick time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(tick)
	}
	t.Fatal("condition not met in time")
}
