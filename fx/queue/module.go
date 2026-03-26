package queuefx

import (
	"context"
	"database/sql"

	qpkg "github.com/nanostack-dev/pgkit/queue"
	"go.uber.org/fx"
)

type Params struct {
	fx.In

	DB *sql.DB
}

type Options struct {
	EnsureSchema bool
	ClientHooks  []qpkg.Hook
}

func Module(opts Options) fx.Option {
	return fx.Module(
		"pgkit.queue",
		fx.Provide(func(p Params) (*qpkg.Client, error) {
			return qpkg.New(p.DB, opts.ClientHooks...)
		}),
		fx.Invoke(func(lc fx.Lifecycle, client *qpkg.Client) {
			if !opts.EnsureSchema {
				return
			}
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return client.EnsureSchema(ctx)
				},
			})
		}),
	)
}
