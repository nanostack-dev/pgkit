package adminuifx

import (
	"context"

	"github.com/nanostack-dev/pgkit/adminui"
	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
	"go.uber.org/fx"
)

type Params struct {
	fx.In

	Queue    *qpkg.Client
	Workflow *workflow.Module `optional:"true"`
}

type Options struct {
	StartServer bool
	Addr        string
	UIOptions   adminui.Options
}

func Module(opts Options) fx.Option {
	return fx.Module(
		"pgkit.adminui",
		fx.Provide(func(p Params) (*adminui.UI, error) {
			uiOptions := opts.UIOptions
			if uiOptions.Workflow == nil {
				uiOptions.Workflow = p.Workflow
			}
			return adminui.New(p.Queue, uiOptions)
		}),
		fx.Invoke(func(lc fx.Lifecycle, ui *adminui.UI) {
			if !opts.StartServer {
				return
			}
			lc.Append(fx.Hook{
				OnStart: func(context.Context) error {
					go func() { _ = ui.ListenAndServe(opts.Addr) }()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return ui.Shutdown(ctx)
				},
			})
		}),
	)
}
