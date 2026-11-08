package queue

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestCancelStopsAJobThatIsNotRunningAtOnce(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		later := time.Now().Add(time.Hour)
		available := mustEnqueue(t, q, EnqueueParams{QueueName: "cancel"})
		scheduled := mustEnqueue(t, q, EnqueueParams{QueueName: "cancel-later", AvailableAt: &later})
		retryable := mustEnqueue(t, q, EnqueueParams{QueueName: "cancel-retry"})
		mustClaim(t, q, "cancel-retry", 0)
		if err := q.Retry(ctx, retryable, time.Hour, errors.New("later")); err != nil {
			t.Fatalf("retry: %v", err)
		}

		for _, id := range []int64{available, scheduled, retryable} {
			state, err := q.Cancel(ctx, id)
			if err != nil || state != StateCancelled {
				t.Fatalf("cancel %d = %s, %v", id, state, err)
			}
			d.requireState(t, id, StateCancelled)
			job, _ := q.GetJob(ctx, id)
			if !job.DoneAt.Valid || !job.CancelRequestedAt.Valid {
				t.Fatalf("cancelled job %d: done %v cancel requested %v", id, job.DoneAt, job.CancelRequestedAt)
			}
		}
		if _, found, err := q.Claim(ctx, "cancel", "w"); err != nil || found {
			t.Fatalf("claimed a cancelled job: found=%v err=%v", found, err)
		}
	})
}

func TestCancelRefusesMissingAndFinalizedJobs(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		done := mustEnqueue(t, q, EnqueueParams{QueueName: "final"})
		mustClaim(t, q, "final", 0)
		if err := q.Ack(ctx, done); err != nil {
			t.Fatalf("ack: %v", err)
		}
		cancelled := mustEnqueue(t, q, EnqueueParams{QueueName: "final"})
		if _, err := q.Cancel(ctx, cancelled); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		for _, id := range []int64{done, cancelled, 1 << 40} {
			if _, err := q.Cancel(ctx, id); !errors.Is(err, ErrJobNotFound) {
				t.Fatalf("cancel %d: err %v, want ErrJobNotFound", id, err)
			}
		}
		d.requireState(t, done, StateCompleted)
	})
}

func TestCancelTxRolledBackChangesNothing(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "rollback"})
		tx, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if _, err := q.CancelTx(ctx, tx, id); err != nil {
			t.Fatalf("cancel tx: %v", err)
		}
		_ = tx.Rollback()
		d.requireState(t, id, StateAvailable)
	})
}

func TestCancellingARunningJobInterruptsAnOnEnqueueHandler(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		started := make(chan int64, 1)
		cause := make(chan error, 1)
		var failed atomic.Int32
		worker, err := q.Worker("interrupt").
			Pickup(OnEnqueue()).
			Tune(WorkerConfig{OnJobFailed: func(context.Context, Job) { failed.Add(1) }}).
			HandleRaw("interrupt", func(handlerCtx context.Context, job Job) error {
				started <- job.ID
				<-handlerCtx.Done()
				cause <- context.Cause(handlerCtx)
				return handlerCtx.Err()
			}).
			Build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		runWorker(t, worker)
		<-worker.Ready()

		id := mustEnqueue(t, q, EnqueueParams{QueueName: "interrupt"})
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not start")
		}
		state, err := q.Cancel(ctx, id)
		if err != nil || state != StateRunning {
			t.Fatalf("cancel running job = %s, %v; want running with a cancel request", state, err)
		}
		select {
		case got := <-cause:
			if !errors.Is(got, ErrJobCancelled) {
				t.Fatalf("handler context cause = %v, want ErrJobCancelled", got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the handler's context was not cancelled")
		}
		d.requireStateEventually(t, id, StateCancelled, 5*time.Second)
		if failed.Load() != 0 {
			t.Fatal("OnJobFailed ran for a cancelled job")
		}
	})
}

func TestACancelRequestDecidesTheOutcomeOfAPollingWorkersJob(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		for _, tc := range []struct {
			name    string
			outcome error
			want    JobState
		}{
			{"failure", errors.New("gave up"), StateCancelled},
			{"non-retryable", NonRetryable(errors.New("refused")), StateCancelled},
			{"success", nil, StateCompleted},
		} {
			t.Run(tc.name, func(t *testing.T) {
				queueName := "poll-cancel-" + tc.name
				release := make(chan struct{})
				started := make(chan struct{}, 1)
				worker, err := q.Worker(queueName).
					Pickup(PollEvery(20*time.Millisecond)).
					HandleRaw(queueName, func(context.Context, Job) error {
						started <- struct{}{}
						<-release
						return tc.outcome
					}).
					Build()
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				id := mustEnqueue(t, q, EnqueueParams{QueueName: queueName})
				runWorker(t, worker)
				<-started
				if _, err := q.Cancel(ctx, id); err != nil {
					t.Fatalf("cancel: %v", err)
				}
				close(release)
				d.requireStateEventually(t, id, tc.want, 5*time.Second)
			})
		}
	})
}

func TestAHandlerCanCancelOrSnoozeItsJob(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		var runs atomic.Int32
		worker, err := q.Worker("outcomes").
			Pickup(PollEvery(20*time.Millisecond)).
			HandleRaw("cancel-me", func(context.Context, Job) error {
				return Cancel(errors.New("no longer wanted"))
			}).
			HandleRaw("snooze-me", func(context.Context, Job) error {
				runs.Add(1)
				return Snooze(time.Hour)
			}).
			HandleRaw("snooze-briefly", func(_ context.Context, job Job) error {
				if job.Claims == 1 {
					return Snooze(0)
				}
				return nil
			}).
			Build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		cancelled := mustEnqueue(t, q, EnqueueParams{QueueName: "cancel-me"})
		snoozed := mustEnqueue(t, q, EnqueueParams{QueueName: "snooze-me", MaxAttempts: 1})
		brief := mustEnqueue(t, q, EnqueueParams{QueueName: "snooze-briefly", MaxAttempts: 1})
		runWorker(t, worker)

		d.requireStateEventually(t, cancelled, StateCancelled, 5*time.Second)
		if job, _ := q.GetJob(ctx, cancelled); job.LastError.String != "no longer wanted" {
			t.Fatalf("cancelled job last error = %q", job.LastError.String)
		}
		d.requireStateEventually(t, snoozed, StateScheduled, 5*time.Second)
		job, _ := q.GetJob(ctx, snoozed)
		if job.Attempts != 0 || time.Until(job.AvailableAt) < 50*time.Minute {
			t.Fatalf("snoozed job: attempts %d available at %s, want the attempt refunded and an hour's wait", job.Attempts, job.AvailableAt)
		}
		d.requireStateEventually(t, brief, StateCompleted, 5*time.Second)
		if runs.Load() != 1 {
			t.Fatalf("snoozed handler ran %d times", runs.Load())
		}
	})
}

func TestAReplayedCancelledJobForgetsItsCancelRequest(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "replay-cancel"})
		if _, err := q.Cancel(ctx, id); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		if err := q.ReplayJob(ctx, id); err != nil {
			t.Fatalf("replay: %v", err)
		}
		job := mustClaim(t, q, "replay-cancel", 0)
		if job.CancelRequestedAt.Valid {
			t.Fatal("the replayed job still carries its cancel request")
		}
		if err := q.Retry(ctx, id, 0, errors.New("transient")); err != nil {
			t.Fatalf("retry: %v", err)
		}
		d.requireState(t, id, StateAvailable)
		var requested sql.NullTime
		_ = d.db.QueryRowContext(ctx, `SELECT cancel_requested_at FROM pgqueue_jobs WHERE id = $1`, id).Scan(&requested)
		if requested.Valid {
			t.Fatal("cancel request reappeared")
		}
	})
}

func TestSnoozeTxCancelsAJobThatWasAskedToStop(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "snooze-cancel"})
		job := mustClaim(t, q, "snooze-cancel", time.Minute)
		if state, err := q.Cancel(ctx, id); err != nil || state != StateRunning {
			t.Fatalf("cancel = %s, %v; want a cancel request on the running job", state, err)
		}

		if err := inTxOn(d.db, func(tx *sql.Tx) error { return q.SnoozeTx(ctx, tx, *job, time.Unix(0, 0)) }); err != nil {
			t.Fatalf("snooze: %v", err)
		}
		d.requireState(t, id, StateCancelled)
		after, err := q.GetJob(ctx, id)
		if err != nil || !after.DoneAt.Valid || after.Attempts != job.Attempts {
			t.Fatalf("cancelled job = %+v, err %v; want done_at set and the attempt kept", after, err)
		}
		if _, found, err := q.Claim(ctx, "snooze-cancel", "other"); err != nil || found {
			t.Fatalf("claim after the cancelled snooze: found=%v err=%v; the cancel was lost", found, err)
		}
	})
}

func TestAWorkerHandsBackAJobThatWasAskedToStopAsCancelled(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		w, err := NewWorker(q, NewHandlerRegistry(), WorkerConfig{})
		if err != nil {
			t.Fatalf("new worker: %v", err)
		}
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "hand-back-cancel"})
		job := mustClaim(t, q, "hand-back-cancel", time.Minute)
		if _, err := q.Cancel(ctx, id); err != nil {
			t.Fatalf("cancel: %v", err)
		}

		w.handBack(ctx, job)

		d.requireState(t, id, StateCancelled)
		if _, found, err := q.Claim(ctx, "hand-back-cancel", "other"); err != nil || found {
			t.Fatalf("claim after hand back: found=%v err=%v; the cancel was lost", found, err)
		}
	})
}
