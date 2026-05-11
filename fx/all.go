package fx

import (
	adminuifx "github.com/nanostack-dev/pgkit/fx/adminui"
	queuefx "github.com/nanostack-dev/pgkit/fx/queue"
	workflowfx "github.com/nanostack-dev/pgkit/fx/workflow"
	"go.uber.org/fx"
)

type Options struct {
	Queue    queuefx.Options
	Workflow workflowfx.Options
	AdminUI  adminuifx.Options
}

func All(opts Options) fx.Option {
	return fx.Options(
		queuefx.Module(opts.Queue),
		workflowfx.Module(opts.Workflow),
		adminuifx.Module(opts.AdminUI),
	)
}
