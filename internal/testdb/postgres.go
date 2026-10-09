package testdb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func Open(t *testing.T, ctx context.Context, database string) *sql.DB {
	t.Helper()
	container, dsn, err := Start(ctx, database)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
		defer cancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate postgres: %v", err)
		}
	})
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})
	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		t.Fatalf("postgres ping: %v", err)
	}
	return db
}

func Start(ctx context.Context, database string) (*postgres.PostgresContainer, string, error) {
	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase(database),
		postgres.WithUsername("pgkit"),
		postgres.WithPassword("pgkit"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, "", fmt.Errorf("start postgres: %w", err)
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
		defer cancel()
		_ = container.Terminate(cleanupCtx)
		return nil, "", fmt.Errorf("postgres connection string: %w", err)
	}
	return container, dsn, nil
}
