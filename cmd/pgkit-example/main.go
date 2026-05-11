package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nanostack-dev/pgkit/adminui"
	"github.com/nanostack-dev/pgkit/pglock"
	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	dsn := strings.TrimSpace(os.Getenv("PGKIT_DATABASE_URL"))
	if dsn == "" {
		dsn = "postgres://pgkit:pgkit@localhost:5432/pgkit_test?sslmode=disable"
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		panic(fmt.Errorf("open db: %w", err))
	}
	defer func() { _ = db.Close() }()

	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		pingCancel()
		panic(fmt.Errorf("ping db: %w", err))
	}
	pingCancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	queue, err := qpkg.New(db)
	if err != nil {
		panic(fmt.Errorf("new queue client: %w", err))
	}
	queue.SetLogger(slogAdapter{logger: logger.With("component", "pgqueue")})

	if err := queue.EnsureSchema(ctx); err != nil {
		panic(fmt.Errorf("ensure queue schema: %w", err))
	}

	workflowModule, err := workflow.New(db, queue)
	if err != nil {
		panic(fmt.Errorf("new workflow module: %w", err))
	}
	if err := workflowModule.EnsureSchema(ctx); err != nil {
		panic(fmt.Errorf("ensure workflow schema: %w", err))
	}

	locker, err := pglock.New(db)
	if err != nil {
		panic(fmt.Errorf("new lock client: %w", err))
	}
	locker.SetLogger(slogAdapter{logger: logger.With("component", "pglock")})

	// Optional heartbeat monitor example: one instance logs queue health every 30m.
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = locker.TryWithSessionLock(ctx, "pgkit-example-monitor", func(runCtx context.Context) error {
					jobs, err := queue.ListJobs(runCtx, qpkg.ListJobsParams{Limit: 1, Status: qpkg.StatusFailed})
					if err != nil {
						logger.Error("monitor failed", "error", err)
						return nil
					}
					if len(jobs) > 0 {
						logger.Warn("queue has failed jobs", "count_at_least", len(jobs))
					}
					return nil
				})
			}
		}
	}()

	dashboard, err := adminui.NewFromEnv(queue, workflowModule)
	if err != nil {
		panic(fmt.Errorf("create dashboard: %w (set PGKIT_DASHBOARD_TOKEN)", err))
	}

	addr := strings.TrimSpace(os.Getenv("PGKIT_EXAMPLE_ADDR"))
	if addr == "" {
		addr = ":8080"
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           dashboard.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("pgkit example started", "addr", addr)
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(fmt.Errorf("http server: %w", err))
	}
}

type slogAdapter struct {
	logger *slog.Logger
}

func (s slogAdapter) Debug(ctx context.Context, msg string, fields map[string]any) {
	s.logger.DebugContext(ctx, msg, slog.Any("fields", fields))
}

func (s slogAdapter) Info(ctx context.Context, msg string, fields map[string]any) {
	s.logger.InfoContext(ctx, msg, slog.Any("fields", fields))
}

func (s slogAdapter) Warn(ctx context.Context, msg string, fields map[string]any) {
	s.logger.WarnContext(ctx, msg, slog.Any("fields", fields))
}

func (s slogAdapter) Error(ctx context.Context, msg string, fields map[string]any) {
	s.logger.ErrorContext(ctx, msg, slog.Any("fields", fields))
}
