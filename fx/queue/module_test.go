package queuefx

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	qpkg "github.com/nanostack-dev/pgkit/queue"
	"go.uber.org/fx"
)

func TestAnInvalidRetentionPolicyStopsTheAppFromStarting(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://localhost:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	app := fx.New(
		fx.NopLogger,
		fx.Supply(db),
		Module(Options{Retention: &qpkg.RetentionPolicy{}}),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("build: %v", err)
	}

	err = app.Start(context.Background())
	if !errors.Is(err, qpkg.ErrInvalidRetentionPolicy) {
		t.Fatalf("Start() = %v, want ErrInvalidRetentionPolicy", err)
	}
}
