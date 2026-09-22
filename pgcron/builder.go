package pgcron

import (
	"context"
	"database/sql"
	"time"
)

// Builder is a job configuration. Methods return independent copies; DoTx
// validates and builds the job without starting it or accessing the database.
type Builder struct {
	db     *sql.DB
	params Params
}

func Named(db *sql.DB, name string) Builder {
	return Builder{db: db, params: Params{Name: name}}
}

func (b Builder) Every(interval time.Duration) Builder {
	b.params.Interval = interval
	return b
}

func (b Builder) RetryEvery(interval time.Duration) Builder {
	b.params.RetryInterval = interval
	return b
}

// AfterRun sets a best-effort hook outside the transaction, after commit and
// lock release. Essential work belongs in DoTx, not in this hook.
func (b Builder) AfterRun(run func(context.Context) error) Builder {
	b.params.AfterRun = run
	return b
}

func (b Builder) DoTx(run func(context.Context, *sql.Tx) error) (*Job, error) {
	b.params.Run = run
	return New(b.db, b.params)
}
