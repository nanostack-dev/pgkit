package workflow_test

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
)

func Example() {
	ctx := context.Background()
	db, err := sql.Open("pgx", "postgres://pgkit:pgkit@localhost:5432/pgkit_test?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	queue, err := qpkg.New(db)
	if err != nil {
		log.Fatal(err)
	}
	if err := queue.EnsureSchema(ctx); err != nil {
		log.Fatal(err)
	}

	def, err := workflow.Define("hello-world", func(b *workflow.Builder) {
		b.Title("Hello World")
		b.Step("prepare", func(_ context.Context, step workflow.StepContext) (any, error) {
			var input struct {
				Name string `json:"name"`
			}
			if err := step.DecodeInput(&input); err != nil {
				return nil, err
			}
			return map[string]any{"message": "hello " + input.Name}, nil
		}, workflow.StepOptions{})
		b.TxStep("persist", func(ctx context.Context, tx *sql.Tx, step workflow.StepContext) (any, error) {
			var payload struct {
				Message string `json:"message"`
			}
			if err := step.Output("prepare", &payload); err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO example_messages (message) VALUES ($1)`, payload.Message); err != nil {
				return nil, err
			}
			return map[string]any{"stored": true}, nil
		}, workflow.StepOptions{DependsOn: []string{"prepare"}})
	})
	if err != nil {
		log.Fatal(err)
	}

	module, err := workflow.New(db, queue, def)
	if err != nil {
		log.Fatal(err)
	}
	if err := module.EnsureSchema(ctx); err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS example_messages (id SERIAL PRIMARY KEY, message TEXT NOT NULL)`); err != nil {
		log.Fatal(err)
	}
	if _, err := module.Publish(ctx, "hello-world"); err != nil {
		log.Fatal(err)
	}
	if err := module.Activate(ctx, "hello-world", 1); err != nil {
		log.Fatal(err)
	}

	worker, err := workflow.NewWorker(module, workflow.WorkerConfig{
		PollInterval: 50 * time.Millisecond,
		ReapInterval: 2 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_ = worker.Run(workerCtx)
	}()

	run, err := module.Start(ctx, "hello-world", map[string]any{"name": "nanostack"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	for start := time.Now(); time.Since(start) < 5*time.Second; {
		view, err := module.GetRun(ctx, run.ID)
		if err == nil && view.Run.Status == workflow.RunStatusSucceeded {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}
