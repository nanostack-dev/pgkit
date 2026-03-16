package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nanostack-dev/pgkit/adminui"
	pgkitfx "github.com/nanostack-dev/pgkit/fx"
	adminuifx "github.com/nanostack-dev/pgkit/fx/adminui"
	queuefx "github.com/nanostack-dev/pgkit/fx/queue"
	workflowfx "github.com/nanostack-dev/pgkit/fx/workflow"
	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/fx"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	token := strings.TrimSpace(os.Getenv("PGKIT_DASHBOARD_TOKEN"))
	if token == "" {
		token = "change-me"
	}
	addr := strings.TrimSpace(os.Getenv("PGKIT_PLAYGROUND_ADDR"))
	if addr == "" {
		addr = "127.0.0.1:18081"
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	app := fx.New(
		fx.Provide(func() (testcontainers.Container, string, error) {
			pg, err := postgres.Run(
				context.Background(),
				"postgres:16-alpine",
				postgres.WithDatabase("pgkit_test"),
				postgres.WithUsername("pgkit"),
				postgres.WithPassword("pgkit"),
			)
			if err != nil {
				return nil, "", err
			}
			connString, err := pg.ConnectionString(context.Background(), "sslmode=disable")
			if err != nil {
				_ = pg.Terminate(context.Background())
				return nil, "", err
			}
			return pg, connString, nil
		}),
		fx.Provide(func(container testcontainers.Container, dsn string) (*sql.DB, error) {
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				return nil, err
			}
			if err := waitForPing(context.Background(), db, 20*time.Second); err != nil {
				_ = db.Close()
				return nil, err
			}
			_ = container
			return db, nil
		}),
		fx.Provide(fx.Annotate(func() *workflow.Definition {
			def, err := workflow.Define("playground-orders", func(b *workflow.Builder) {
				b.ForEach("fanout", func(_ context.Context, _ workflow.StepContext) ([]any, error) {
					return []any{
						map[string]any{"value": 1},
						map[string]any{"value": 2},
						map[string]any{"value": 3},
						map[string]any{"value": 4},
					}, nil
				}, func(_ context.Context, step workflow.StepContext) (any, error) {
					var input struct {
						Value int `json:"value"`
					}
					if err := step.DecodeInput(&input); err != nil {
						return nil, err
					}
					return map[string]any{"processed": input.Value * 10}, nil
				}, workflow.StepOptions{})
				b.Step("finalize", func(_ context.Context, step workflow.StepContext) (any, error) {
					return map[string]any{"count": len(step.ItemOutputs("fanout"))}, nil
				}, workflow.StepOptions{DependsOn: []string{"fanout"}})
			})
			if err != nil {
				panic(err)
			}
			return def
		}, fx.ResultTags(`group:"pgkit.workflow.definitions"`))),
		pgkitfx.All(pgkitfx.Options{
			Queue: queuefx.Options{EnsureSchema: true},
			Workflow: workflowfx.Options{
				EnsureSchema: true,
				StartWorker:  true,
				WorkerConfig: workflow.WorkerConfig{PollInterval: 100 * time.Millisecond, ReapInterval: 5 * time.Second},
			},
			AdminUI: adminuifx.Options{
				StartServer: true,
				Addr:        addr,
				UIOptions:   adminui.Options{Token: token},
			},
		}),
		fx.Invoke(func(lc fx.Lifecycle, container testcontainers.Container, db *sql.DB, queue *qpkg.Client, module *workflow.Module) error {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					if _, err := module.Publish(ctx, "playground-orders"); err != nil {
						return err
					}
					if err := module.Activate(ctx, "playground-orders", 1); err != nil {
						return err
					}
					_, _ = queue.Enqueue(ctx, qpkg.EnqueueParams{QueueName: "playground.audit", Payload: []byte(`{"event":"boot"}`), MaxAttempts: 3})
					_, _ = module.Start(ctx, "playground-orders", nil, &workflow.StartRunOptions{CreatedBy: "playground", CorrelationKey: "demo-seed"})
					logger.Info("pgkit playground ready", "addr", addr, "token", token)
					return nil
				},
				OnStop: func(context.Context) error {
					_ = db.Close()
					return container.Terminate(context.Background())
				},
			})
			return nil
		}),
	)

	startCtx, startCancel := context.WithTimeout(ctx, 60*time.Second)
	defer startCancel()
	if err := app.Start(startCtx); err != nil {
		panic(fmt.Errorf("start app: %w", err))
	}
	<-ctx.Done()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopCancel()
	_ = app.Stop(stopCtx)
}

func waitForPing(ctx context.Context, db *sql.DB, timeout time.Duration) error {
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
