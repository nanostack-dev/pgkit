package workflow

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// TestATxStepWhoseCommitOutcomeIsUnknownRunsOnce makes PostgreSQL commit a
// transactional step while the client sees the commit fail, as when the
// connection drops after COMMIT. The run must resume from the committed checkpoint
// instead of applying the step's writes a second time.
func TestATxStepWhoseCommitOutcomeIsUnknownRunsOnce(t *testing.T) {
	t.Parallel()
	url := newDatabase(t)
	failNextCommit := &atomic.Bool{}
	db := openWithCommitFault(t, url, failNextCommit)
	h := newHarnessOn(t, db, url)
	if err := h.queue.ListenOn(url); err != nil {
		t.Fatal(err)
	}
	createLedger(h)
	calls := newCounter()
	flow := Define("ambiguous-commit", func(wf *Context, _ struct{}) (int, error) {
		return wf.TxStep("record", func(ctx context.Context, tx *sql.Tx) (int, error) {
			if _, err := tx.ExecContext(ctx, `INSERT INTO ledger (entry) VALUES ('once')`); err != nil {
				return 0, err
			}
			failNextCommit.Store(calls.add("record") == 1)
			return 42, nil
		})
	})
	h.startWorkerWith(workerOptions{config: WorkerConfig{Concurrency: 1}}, flow)

	if got := mustResult(h, mustStart(h, flow, struct{}{})); got != 42 {
		t.Fatalf("result = %d", got)
	}
	if calls.get("record") != 1 {
		t.Fatalf("the step ran %d times after a commit it could not confirm", calls.get("record"))
	}
	if h.queryInt(`SELECT count(*) FROM ledger`) != 1 {
		t.Fatal("the step's write was applied twice")
	}
}

func openWithCommitFault(t *testing.T, url string, failNextCommit *atomic.Bool) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(commitFaultConnector{Connector: stdlib.GetConnector(*config), failNextCommit: failNextCommit})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type commitFaultConnector struct {
	driver.Connector
	failNextCommit *atomic.Bool
}

func (c commitFaultConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &commitFaultConn{Conn: conn, failNextCommit: c.failNextCommit}, nil
}

type commitFaultConn struct {
	driver.Conn
	failNextCommit *atomic.Bool
}

func (c *commitFaultConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return commitFaultTx{Tx: tx, failNextCommit: c.failNextCommit}, nil
}

func (c *commitFaultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *commitFaultConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *commitFaultConn) CheckNamedValue(value *driver.NamedValue) error {
	return c.Conn.(driver.NamedValueChecker).CheckNamedValue(value)
}

type commitFaultTx struct {
	driver.Tx
	failNextCommit *atomic.Bool
}

func (t commitFaultTx) Commit() error {
	if err := t.Tx.Commit(); err != nil {
		return err
	}
	if t.failNextCommit.CompareAndSwap(true, false) {
		return errors.New("connection lost after COMMIT")
	}
	return nil
}
