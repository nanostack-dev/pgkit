package workflowfx

import (
	"context"
	"database/sql"
	"fmt"

	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
	"go.uber.org/fx"
)

type Params struct {
	fx.In

	DB    *sql.DB
	Queue *qpkg.Client
	Defs  []*workflow.Definition `group:"pgkit.workflow.definitions"`
}

type Options struct {
	EnsureSchema bool
	StartWorker  bool
	WorkerConfig workflow.WorkerConfig
	Definitions  []*workflow.Definition
}

func Module(opts Options) fx.Option {
	provideDefs := make([]any, 0, len(opts.Definitions))
	for _, def := range opts.Definitions {
		definition := def
		provideDefs = append(provideDefs, fx.Annotate(func() *workflow.Definition { return definition }, fx.ResultTags(`group:"pgkit.workflow.definitions"`)))
	}
	items := make([]fx.Option, 0, 3+len(provideDefs))
	for _, provider := range provideDefs {
		items = append(items, fx.Provide(provider))
	}
	items = append(items,
		fx.Provide(func(p Params) (*workflow.Module, error) {
			defs := append([]*workflow.Definition(nil), p.Defs...)
			return workflow.New(p.DB, p.Queue, defs...)
		}),
		fx.Invoke(func(lc fx.Lifecycle, module *workflow.Module) error {
			if opts.EnsureSchema {
				lc.Append(fx.Hook{OnStart: func(ctx context.Context) error { return module.EnsureSchema(ctx) }})
			}
			if opts.StartWorker {
				worker, err := workflow.NewWorker(module, opts.WorkerConfig)
				if err != nil {
					return fmt.Errorf("create workflow worker: %w", err)
				}
				workerCtx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				lc.Append(fx.Hook{
					OnStart: func(context.Context) error {
						go func() {
							defer close(done)
							_ = worker.Run(workerCtx)
						}()
						return nil
					},
					OnStop: func(stopCtx context.Context) error {
						cancel()
						select {
						case <-done:
							return nil
						case <-stopCtx.Done():
							return stopCtx.Err()
						}
					},
				})
			}
			return nil
		}),
	)
	return fx.Module("pgkit.workflow", items...)
}
