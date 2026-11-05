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
	// Retention, when set, purges finished jobs from start to stop.
	Retention *qpkg.RetentionPolicy
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
		fx.Invoke(func(lc fx.Lifecycle, client *qpkg.Client) {
			if opts.Retention == nil {
				return
			}
			policy := *opts.Retention
			retentionCtx, stopRetention := context.WithCancel(context.Background())
			stopped := make(chan struct{})
			lc.Append(fx.Hook{
				OnStart: func(context.Context) error {
					if err := policy.Validate(); err != nil {
						return err
					}
					go func() {
						defer close(stopped)
						_ = client.RunRetention(retentionCtx, policy)
					}()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					stopRetention()
					select {
					case <-stopped:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			})
		}),
	)
}
