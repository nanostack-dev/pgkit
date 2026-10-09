package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// TestManyRunsUnderContention starts runs that use every operation on three
// competing workers, signals them concurrently and stops a worker midway. Every run
// must finish with the right result and every step and transaction must have taken
// effect exactly once.
func TestManyRunsUnderContention(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}
	h := newHarness(t)
	createLedger(h)
	const runs = 60
	bonus := NewSignal[int]("bonus")
	calls := newCounter()
	square := Define("stress-square", func(wf *Context, n int) (int, error) {
		return wf.Step("square", func(ctx context.Context) (int, error) {
			calls.add(IdempotencyKey(ctx))
			return n * n, nil
		})
	})
	flow := Define("stress", func(wf *Context, n int) (int, error) {
		first, err := wf.Step("first", func(ctx context.Context) (int, error) {
			if calls.add(IdempotencyKey(ctx)+"/attempt") == 1 && n%5 == 0 {
				return 0, fmt.Errorf("transient failure for %d", n)
			}
			calls.add(IdempotencyKey(ctx))
			return n, nil
		}, Retry{MaxAttempts: 3, Backoff: 5 * time.Millisecond})
		if err != nil {
			return 0, err
		}
		if _, err := wf.TxStep("ledger", func(ctx context.Context, tx *sql.Tx) (bool, error) {
			_, err := tx.ExecContext(ctx, `INSERT INTO ledger (entry) VALUES ($1)`, wf.RunID())
			return true, err
		}); err != nil {
			return 0, err
		}
		parts := []*Future[int]{
			wf.Async("async", func(ctx context.Context) (int, error) {
				calls.add(IdempotencyKey(ctx))
				time.Sleep(time.Duration(rand.IntN(5)) * time.Millisecond)
				return 1, nil
			}),
			wf.Start("child", square, n),
		}
		total := first
		for _, part := range parts {
			value, err := part.Wait()
			if err != nil {
				return 0, err
			}
			total += value
		}
		if err := wf.Sleep("breathe", time.Duration(rand.IntN(20))*time.Millisecond); err != nil {
			return 0, err
		}
		extra, err := wf.Receive(bonus, time.Minute)
		if err != nil {
			return 0, err
		}
		return total + extra, nil
	})

	stopFirst := h.startWorkerWith(workerOptions{id: "a", config: WorkerConfig{Concurrency: 4}}, flow, square)
	h.startWorkerWith(workerOptions{id: "b", config: WorkerConfig{Concurrency: 4}}, flow, square)
	h.startWorkerWith(workerOptions{id: "c", config: WorkerConfig{Concurrency: 2}}, flow, square)

	started := make([]Run[int], runs)
	for n := range runs {
		started[n] = mustStart(h, flow, n)
	}
	var senders sync.WaitGroup
	for n := range runs {
		senders.Go(func() {
			time.Sleep(time.Duration(rand.IntN(200)) * time.Millisecond)
			if err := h.client.Signal(h.ctx, started[n].ID, bonus, 1000); err != nil {
				t.Errorf("signal run %d: %v", n, err)
			}
		})
	}
	time.Sleep(100 * time.Millisecond)
	stopFirst()
	senders.Wait()

	for n, run := range started {
		if got, want := mustResult(h, run), n+1+n*n+1000; got != want {
			t.Fatalf("run %d = %d, want %d", n, got, want)
		}
		for _, step := range []string{"first", "async"} {
			if got := calls.get(run.ID + "/" + step); got != 1 {
				t.Fatalf("run %d step %s took effect %d times", n, step, got)
			}
		}
	}
	if got := h.queryInt(`SELECT count(*) FROM ledger`); got != runs {
		t.Fatalf("ledger entries = %d, want %d", got, runs)
	}
	if got := h.queryInt(`SELECT count(DISTINCT entry) FROM ledger`); got != runs {
		t.Fatalf("distinct ledger entries = %d, want %d", got, runs)
	}
	if got := h.queryInt(`SELECT count(*) FROM pgqueue_jobs WHERE status <> 'done'`); got != 0 {
		t.Fatalf("%d jobs are not settled", got)
	}
}
