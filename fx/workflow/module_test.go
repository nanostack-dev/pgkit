package workflowfx

import (
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
	"go.uber.org/fx"
)

type starter struct {
	client   *workflow.Client
	workflow *workflow.Workflow[int, int]
}

func TestADefinitionMayDependOnTheClient(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://localhost:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	queue, err := qpkg.New(db)
	if err != nil {
		t.Fatal(err)
	}

	app := fx.New(
		fx.NopLogger,
		fx.Supply(queue),
		Module(Options{StartWorker: true}),
		fx.Provide(func(client *workflow.Client) *starter {
			return &starter{client: client, workflow: workflow.Define("self-starting", func(*workflow.Context, int) (int, error) { return 0, nil })}
		}),
		fx.Provide(fx.Annotate(func(s *starter) workflow.Definition { return s.workflow }, fx.ResultTags(`group:"`+DefinitionsGroup+`"`))),
	)

	if err := app.Err(); err != nil {
		t.Fatalf("a definition depending on the client does not build: %v", err)
	}
}
