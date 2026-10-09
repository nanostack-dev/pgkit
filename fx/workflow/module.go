package workflowfx

import (
	"context"
	"fmt"
	"slices"

	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
	"go.uber.org/fx"
)

// DefinitionsGroup collects workflows provided by other modules, which the worker
// started by Module executes along with Options.Workflows:
//
//	fx.Provide(fx.Annotate(newCheckout, fx.ResultTags(`group:"pgkit.workflow.definitions"`)))
const DefinitionsGroup = "pgkit.workflow.definitions"

type Params struct {
	fx.In

	Queue     *qpkg.Client
	Workflows []workflow.Definition `group:"pgkit.workflow.definitions"`
}

type Options struct {
	// EnsureSchema creates the workflow tables on start.
	EnsureSchema bool
	// StartWorker runs a worker for every provided workflow between start and stop.
	StartWorker bool
	// WorkerID names the worker; it defaults to "pgkit-workflow".
	WorkerID string
	// Pickup and WorkerConfig tune the worker; see workflow.WorkerBuilder.
	Pickup       qpkg.Pickup
	WorkerConfig workflow.WorkerConfig
	// Workflows are executed by the worker in addition to the DefinitionsGroup.
	Workflows []workflow.Definition
}

// Module provides a *workflow.Client and, with StartWorker, runs a worker. Workflow
// definitions may depend on the client, as a service that starts its own runs does.
func Module(opts Options) fx.Option {
	return fx.Module("pgkit.workflow",
		fx.Provide(func(q *qpkg.Client) (*workflow.Client, error) {
			return workflow.New(q)
		}),
		fx.Invoke(func(lc fx.Lifecycle, client *workflow.Client, p Params) error {
			if opts.EnsureSchema {
				lc.Append(fx.Hook{OnStart: client.EnsureSchema})
			}
			if !opts.StartWorker {
				return nil
			}
			return appendWorker(lc, client, opts, slices.Concat(opts.Workflows, p.Workflows))
		}),
	)
}

func appendWorker(lc fx.Lifecycle, client *workflow.Client, opts Options, workflows []workflow.Definition) error {
	id := opts.WorkerID
	if id == "" {
		id = "pgkit-workflow"
	}
	worker, err := client.Worker(id).Workflows(workflows...).Pickup(opts.Pickup).Tune(opts.WorkerConfig).Build()
	if err != nil {
		return fmt.Errorf("build workflow worker: %w", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				_ = worker.Run(runCtx)
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
	return nil
}
