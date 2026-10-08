package workflow

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// The workflow SQL avoids driver-specific parameter types (arrays, []byte for
// jsonb), so it runs on lib/pq as on pgx. This scenario covers every operation.
func TestEveryOperationWorksOnLibPQ(t *testing.T) {
	t.Parallel()
	url := newDatabase(t)
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	h := newHarnessOn(t, db, url)
	if err := h.queue.ListenOn(url); err != nil {
		t.Fatal(err)
	}
	createLedger(h)
	reviewed := NewSignal[approval]("reviewed")
	child := Define("pq-child", func(wf *Context, n int) (int, error) {
		return wf.Step("square", func(context.Context) (int, error) { return n * n, nil })
	})
	flow := Define("pq-everything", func(wf *Context, in order) (receipt, error) {
		charged, err := wf.Step("charge", func(context.Context) (int, error) { return in.Total, nil })
		if err != nil {
			return receipt{}, err
		}
		if _, err := wf.TxStep("ledger", func(ctx context.Context, tx *sql.Tx) (bool, error) {
			_, err := tx.ExecContext(ctx, `INSERT INTO ledger (entry) VALUES ($1)`, in.ID)
			return true, err
		}); err != nil {
			return receipt{}, err
		}
		parts := []*Future[int]{wf.Async("a", func(context.Context) (int, error) { return 1, nil }), wf.Start("child", child, 3)}
		for _, part := range parts {
			value, err := part.Wait()
			if err != nil {
				return receipt{}, err
			}
			charged += value
		}
		if err := wf.Sleep("settle", 10*time.Millisecond); err != nil {
			return receipt{}, err
		}
		review, err := wf.Receive(reviewed, Forever)
		if err != nil {
			return receipt{}, err
		}
		if _, err := wf.Receive(reviewed, 0); !errors.Is(err, ErrTimeout) {
			return receipt{}, errors.New("expected a timeout")
		}
		return receipt{OrderID: in.ID, Charged: charged, Note: review.By}, nil
	})
	h.startWorker(flow, child)

	run := mustStart(h, flow, order{ID: "pq-1", Total: 5}, Key("pq-key"), Timeout(time.Minute))
	h.waitForStatus(run.ID, RunWaiting)
	h.waitForStep(run.ID, "reviewed", StepWaiting)
	if err := h.client.Signal(h.ctx, run.ID, reviewed, approval{By: "pq"}); err != nil {
		t.Fatal(err)
	}

	got := mustResult(h, run)
	if got != (receipt{OrderID: "pq-1", Charged: 15, Note: "pq"}) {
		t.Fatalf("receipt = %+v", got)
	}
	if h.queryInt(`SELECT count(*) FROM ledger WHERE entry = 'pq-1'`) != 1 {
		t.Fatal("ledger entry missing")
	}
	if again := mustStart(h, flow, order{ID: "other"}, Key("pq-key")); again.ID != run.ID {
		t.Fatal("keyed start failed on lib/pq")
	}
	if found, err := h.client.RunByKey(h.ctx, flow, "pq-key"); err != nil || found.ID != run.ID {
		t.Fatalf("RunByKey: %v", err)
	}
	if runs, err := h.client.ListRuns(h.ctx, ListRunsParams{Search: "pq", Status: RunSucceeded}); err != nil || len(runs) != 2 {
		t.Fatalf("ListRuns = %d runs, err = %v", len(runs), err)
	}

	waiting := mustStart(h, flow, order{ID: "pq-2", Total: 1})
	h.waitForStep(waiting.ID, "reviewed", StepWaiting)
	if err := h.client.Cancel(h.ctx, waiting.ID); err != nil {
		t.Fatal(err)
	}
	_, err = result(h, waiting)
	requireErrorIs(t, err, ErrCancelled)
	if err := h.client.Retry(h.ctx, waiting.ID); err != nil {
		t.Fatal(err)
	}
	h.waitForStep(waiting.ID, "reviewed", StepWaiting)
	h.waitForStatus(waiting.ID, RunWaiting)
	tx, err := db.BeginTx(h.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.client.SignalTx(h.ctx, tx, waiting.ID, reviewed, approval{By: "tx"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := mustResult(h, waiting); got.Note != "tx" {
		t.Fatalf("receipt = %+v", got)
	}
	if deleted, err := h.client.Purge(h.ctx, PurgeParams{}); err != nil || deleted != 4 {
		t.Fatalf("Purge deleted %d, err = %v", deleted, err)
	}
}
