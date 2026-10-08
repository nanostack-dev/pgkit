// Command pgkit-playground starts PostgreSQL in a container, runs order workflows
// on a worker and serves the admin UI, to explore pgkit locally.
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

	token := envOr("PGKIT_DASHBOARD_TOKEN", "change-me")
	addr := envOr("PGKIT_PLAYGROUND_ADDR", "127.0.0.1:18081")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	app := fx.New(
		fx.NopLogger,
		fx.Provide(startPostgres, openDatabase),
		fx.Provide(
			fulfilOrder,
			asWorkflowDefinition(func(orders *workflow.Workflow[order, fulfilment]) workflow.Definition { return orders }),
			asWorkflowDefinition(func() workflow.Definition { return shipItem }),
		),
		pgkitfx.All(pgkitfx.Options{
			Queue: queuefx.Options{EnsureSchema: true},
			Workflow: workflowfx.Options{
				EnsureSchema: true,
				StartWorker:  true,
				WorkerID:     "playground",
				Pickup:       qpkg.OnEnqueue().RescanEvery(time.Second),
			},
			AdminUI: adminuifx.Options{StartServer: true, Addr: addr, UIOptions: adminui.Options{Token: token}},
		}),
		fx.Invoke(func(lc fx.Lifecycle, db *sql.DB, workflows *workflow.Client, orders *workflow.Workflow[order, fulfilment]) {
			lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
				if err := createPlaygroundTables(ctx, db); err != nil {
					return err
				}
				go simulateTraffic(context.WithoutCancel(ctx), logger, workflows, orders)
				logger.Info("pgkit playground ready", "url", "http://"+addr, "user", "any", "password", token)
				return nil
			}})
		}),
	)

	startCtx, startCancel := context.WithTimeout(ctx, 90*time.Second)
	defer startCancel()
	if err := app.Start(startCtx); err != nil {
		panic(fmt.Errorf("start playground: %w", err))
	}
	<-ctx.Done()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopCancel()
	_ = app.Stop(stopCtx)
}

// simulateTraffic starts orders that exercise each path: express, cooling-off,
// manager approval (approved, rejected, left waiting) and a cancelled order.
func simulateTraffic(ctx context.Context, logger *slog.Logger, workflows *workflow.Client, orders *workflow.Workflow[order, fulfilment]) {
	start := func(in order) workflow.Run[fulfilment] {
		run, err := workflows.Start(ctx, orders, in, workflow.Key(in.ID))
		if err != nil {
			logger.Error("start order", "order", in.ID, "error", err)
		}
		return run
	}
	start(sampleOrder("ORD-1001", true, 2, 2_500))
	start(sampleOrder("ORD-1002", false, 3, 1_200))
	approved := start(sampleOrder("ORD-1003", true, 2, 40_000))
	rejected := start(sampleOrder("ORD-1004", true, 1, 90_000))
	start(sampleOrder("ORD-1005", true, 1, 75_000))
	cancelled := start(sampleOrder("ORD-1006", false, 2, 60_000))

	time.Sleep(2 * time.Second)
	_ = workflows.Signal(ctx, approved.ID, managerApproval, approval{By: "lea", Approved: true})
	_ = workflows.Signal(ctx, rejected.ID, managerApproval, approval{By: "sam", Approved: false})
	_ = workflows.Cancel(ctx, cancelled.ID)

	for i := 0; ctx.Err() == nil; i++ {
		time.Sleep(15 * time.Second)
		start(sampleOrder(fmt.Sprintf("ORD-%d", 2000+i), i%2 == 0, 1+i%4, 1_500))
	}
}

func startPostgres(lc fx.Lifecycle) (testcontainers.Container, error) {
	container, err := postgres.Run(context.Background(), "postgres:16-alpine",
		postgres.WithDatabase("pgkit"), postgres.WithUsername("pgkit"), postgres.WithPassword("pgkit"),
		postgres.BasicWaitStrategies())
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(ctx context.Context) error { return container.Terminate(ctx) }})
	return container, nil
}

func openDatabase(lc fx.Lifecycle, container testcontainers.Container) (*sql.DB, error) {
	dsn, err := container.(*postgres.PostgresContainer).ConnectionString(context.Background(), "sslmode=disable")
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return db.Close() }})
	return db, nil
}

// asWorkflowDefinition adds a workflow to those the workflowfx worker executes.
func asWorkflowDefinition(provide any) any {
	return fx.Annotate(provide, fx.ResultTags(`group:"`+workflowfx.DefinitionsGroup+`"`))
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
