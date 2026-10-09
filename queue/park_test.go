package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func enqueueAndClaim(t *testing.T, q *Client, queueName string, maxAttempts int) *Job {
	t.Helper()
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, EnqueueParams{QueueName: queueName, Payload: []byte(`{}`), MaxAttempts: maxAttempts}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, found, err := q.Claim(ctx, queueName, "test-worker")
	if err != nil || !found {
		t.Fatalf("claim: found=%v err=%v", found, err)
	}
	return job
}

func inTx(t *testing.T, q *Client, run func(tx *sql.Tx) error) error {
	t.Helper()
	tx, err := q.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := run(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func databaseNow(t *testing.T, q *Client) time.Time {
	t.Helper()
	var now time.Time
	if err := q.DB().QueryRowContext(context.Background(), `SELECT NOW()`).Scan(&now); err != nil {
		t.Fatalf("now: %v", err)
	}
	return now
}

func requireClaimable(t *testing.T, q *Client, queueName string, want bool) *Job {
	t.Helper()
	job, found, err := q.Claim(context.Background(), queueName, "test-worker")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if found != want {
		t.Fatalf("claimable = %v, want %v", found, want)
	}
	return job
}

func requireWake(t *testing.T, sub *Subscription, within time.Duration) {
	t.Helper()
	select {
	case <-sub.Wake():
	case <-time.After(within):
		t.Fatal("subscription was not woken")
	}
}

func requireNoWake(t *testing.T, sub *Subscription, during time.Duration) {
	t.Helper()
	select {
	case <-sub.Wake():
		t.Fatal("subscription was woken")
	case <-time.After(during):
	}
}

func subscribeReady(t *testing.T, q *Client, keys ...string) *Subscription {
	t.Helper()
	sub, err := q.Subscribe(keys...)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(sub.Close)
	requireWake(t, sub, 10*time.Second)
	return sub
}

func TestSnoozeTxDelaysTheJobWithoutCountingItsAttempt(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	job := enqueueAndClaim(t, q, queueName, 1)

	until := databaseNow(t, q).Add(time.Hour)
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *job, until) }); err != nil {
		t.Fatalf("snooze: %v", err)
	}

	snoozed, err := q.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if snoozed.Status != StatusPending || snoozed.Attempts != 0 || snoozed.MaxAttempts != 1 || snoozed.Claims != 1 || snoozed.ClaimedBy.Valid {
		t.Fatalf("snoozed job = %+v, want its attempt refunded", snoozed)
	}
	if !snoozed.AvailableAt.Equal(until) {
		t.Fatalf("available at %v, want %v", snoozed.AvailableAt, until)
	}
	requireClaimable(t, q, queueName, false)
}

func TestSnoozeTxWithAZeroTimeParksTheJobUntilWakeTx(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	job := enqueueAndClaim(t, q, queueName, 1)

	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *job, time.Time{}) }); err != nil {
		t.Fatalf("snooze: %v", err)
	}
	requireClaimable(t, q, queueName, false)
	parked, err := q.GetJob(context.Background(), job.ID)
	if err != nil || !parked.AvailableAt.Equal(ParkedUntil) {
		t.Fatalf("parked job = %+v, err = %v", parked, err)
	}
	if _, err := q.ListJobs(context.Background(), ListJobsParams{Limit: 100}); err != nil {
		t.Fatalf("a parked job breaks ListJobs: %v", err)
	}

	if err := inTx(t, q, func(tx *sql.Tx) error { return q.WakeTx(context.Background(), tx, job.ID) }); err != nil {
		t.Fatalf("wake: %v", err)
	}
	woken := requireClaimable(t, q, queueName, true)
	if woken.ID != job.ID || woken.Attempts != 1 || woken.MaxAttempts != 1 || woken.Claims != 2 {
		t.Fatalf("woken job = %+v", woken)
	}
}

func TestSnoozeTxUntilAPastTimeNotifiesWorkers(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	sub := subscribeReady(t, q, queueName)
	job := enqueueAndClaim(t, q, queueName, 3)
	requireWake(t, sub, 5*time.Second)

	past := databaseNow(t, q).Add(-time.Minute)
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *job, past) }); err != nil {
		t.Fatalf("snooze: %v", err)
	}
	requireWake(t, sub, 5*time.Second)
	requireClaimable(t, q, queueName, true)
}

func TestSnoozeTxUntilALaterTimeDoesNotNotify(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	sub := subscribeReady(t, q, queueName)
	job := enqueueAndClaim(t, q, queueName, 3)
	requireWake(t, sub, 5*time.Second)

	later := databaseNow(t, q).Add(time.Hour)
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *job, later) }); err != nil {
		t.Fatalf("snooze: %v", err)
	}
	requireNoWake(t, sub, 300*time.Millisecond)
}

func TestSnoozeTxRefusesAJobThatIsNotProcessing(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	id, err := q.Enqueue(context.Background(), EnqueueParams{QueueName: queueName, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	pending, err := q.GetJob(context.Background(), id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	err = inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *pending, time.Time{}) })
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestSnoozeTxRollsBackWithItsTransaction(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	job := enqueueAndClaim(t, q, queueName, 3)

	rollback := errors.New("rollback")
	err := inTx(t, q, func(tx *sql.Tx) error {
		if err := q.SnoozeTx(context.Background(), tx, *job, time.Time{}); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("expected rollback, got %v", err)
	}
	still, err := q.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if still.Status != StatusProcessing {
		t.Fatalf("status = %s, want processing", still.Status)
	}
}

func TestWakeTxNotifiesWorkersAtCommit(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	sub := subscribeReady(t, q, queueName)
	later := databaseNow(t, q).Add(time.Hour)
	id, err := q.Enqueue(context.Background(), EnqueueParams{QueueName: queueName, Payload: []byte(`{}`), AvailableAt: &later})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	requireWake(t, sub, 5*time.Second)

	tx, err := q.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := q.WakeTx(context.Background(), tx, id); err != nil {
		t.Fatalf("wake: %v", err)
	}
	requireNoWake(t, sub, 200*time.Millisecond)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	requireWake(t, sub, 5*time.Second)
	requireClaimable(t, q, queueName, true)
}

func TestWakeTxLeavesAProcessingJobAlone(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	job := enqueueAndClaim(t, q, queueName, 3)

	if err := inTx(t, q, func(tx *sql.Tx) error { return q.WakeTx(context.Background(), tx, job.ID) }); err != nil {
		t.Fatalf("wake: %v", err)
	}
	still, err := q.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if still.Status != StatusProcessing || !still.AvailableAt.Equal(job.AvailableAt) {
		t.Fatalf("job changed: %+v", still)
	}
}

func TestHeartbeatKeepsTheReaperAway(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	job := enqueueAndClaim(t, q, queueName, 3)
	if _, err := q.DB().Exec(`UPDATE pgqueue_jobs SET claimed_at = NOW() - interval '1 hour' WHERE id = $1`, job.ID); err != nil {
		t.Fatalf("age claim: %v", err)
	}

	if err := q.Heartbeat(context.Background(), *job); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if _, err := q.ReapStuckJobs(context.Background(), 30*time.Minute); err != nil {
		t.Fatalf("reap: %v", err)
	}
	still, err := q.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if still.Status != StatusProcessing {
		t.Fatalf("status = %s, want processing", still.Status)
	}
}

func TestHeartbeatReportsAClaimTheReaperTookBack(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	first := enqueueAndClaim(t, q, queueName, 3)
	if _, err := q.DB().Exec(`UPDATE pgqueue_jobs SET claimed_at = NOW() - interval '1 hour' WHERE id = $1`, first.ID); err != nil {
		t.Fatalf("age claim: %v", err)
	}
	if _, err := q.ReapStuckJobs(context.Background(), time.Minute); err != nil {
		t.Fatalf("reap: %v", err)
	}
	second := requireClaimable(t, q, queueName, true)

	if err := q.Heartbeat(context.Background(), *first); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("stale claim heartbeat: expected ErrJobNotFound, got %v", err)
	}
	if err := q.Heartbeat(context.Background(), *second); err != nil {
		t.Fatalf("current claim heartbeat: %v", err)
	}
}

func TestHeartbeatReportsAFinishedJob(t *testing.T) {
	q := sharedQueue(t)
	job := enqueueAndClaim(t, q, uniqueQueueName(t), 3)
	if err := q.Ack(context.Background(), job.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := q.Heartbeat(context.Background(), *job); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestNotifyTxWakesSubscribersOfItsKeyAtCommit(t *testing.T) {
	q := sharedQueue(t)
	key := uniqueQueueName(t)
	sub := subscribeReady(t, q, key)
	other := subscribeReady(t, q, key+"-other")

	tx, err := q.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := q.NotifyTx(context.Background(), tx, key); err != nil {
		t.Fatalf("notify: %v", err)
	}
	requireNoWake(t, sub, 200*time.Millisecond)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	requireWake(t, sub, 5*time.Second)
	requireNoWake(t, other, 200*time.Millisecond)
}

func TestNotifyTxIsDroppedWithARolledBackTransaction(t *testing.T) {
	q := sharedQueue(t)
	key := uniqueQueueName(t)
	sub := subscribeReady(t, q, key)

	rollback := errors.New("rollback")
	err := inTx(t, q, func(tx *sql.Tx) error {
		if err := q.NotifyTx(context.Background(), tx, key); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("expected rollback, got %v", err)
	}
	requireNoWake(t, sub, 300*time.Millisecond)
}

func TestSubscriptionCloseIsIdempotent(t *testing.T) {
	q := sharedQueue(t)
	sub, err := q.Subscribe(uniqueQueueName(t))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	sub.Close()
	sub.Close()
}

func TestSubscribeRefusesDriversWithoutNotifications(t *testing.T) {
	db, err := sql.Open("pgkit-without-notifications", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	q, err := New(db)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	if _, err := q.Subscribe("key"); !errors.Is(err, ErrNotificationsUnsupported) {
		t.Fatalf("expected ErrNotificationsUnsupported, got %v", err)
	}
}

func TestReapHonorsASubSecondVisibilityTimeout(t *testing.T) {
	q := sharedQueue(t)
	const visibility = 900 * time.Millisecond
	reap := func(job *Job) JobStatus {
		t.Helper()
		if _, err := q.Reap(context.Background(), ReapParams{VisibilityTimeout: visibility, QueueNames: []string{job.QueueName}}); err != nil {
			t.Fatalf("reap: %v", err)
		}
		reaped, err := q.GetJob(context.Background(), job.ID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		return reaped.Status
	}

	// A claim younger than the timeout survives, which only shows while the reap runs
	// within the timeout of the aging: a slower round proves nothing and is retried.
	for round := 1; ; round++ {
		job := enqueueAndClaim(t, q, fmt.Sprintf("%s-%d", uniqueQueueName(t), round), 3)
		started := time.Now()
		ageClaim(t, q, job.ID, "10 milliseconds")
		status := reap(job)
		if time.Since(started) < visibility-100*time.Millisecond {
			if status != StatusProcessing {
				t.Fatalf("a claim 10ms old was reaped with a 900ms visibility timeout: %s", status)
			}
			break
		}
		if round == 3 {
			t.Fatal("no round reaped within the visibility timeout")
		}
	}
	job := enqueueAndClaim(t, q, uniqueQueueName(t), 3)
	ageClaim(t, q, job.ID, "950 milliseconds")
	if status := reap(job); status != StatusPending {
		t.Fatalf("a claim 950ms old survived a 900ms visibility timeout: %s", status)
	}
}

func ageClaim(t *testing.T, q *Client, id int64, age string) {
	t.Helper()
	if _, err := q.DB().Exec(`UPDATE pgqueue_jobs SET claimed_at = NOW() - $2::interval WHERE id = $1`, id, age); err != nil {
		t.Fatalf("age claim: %v", err)
	}
}

func TestRetryHonorsASubSecondDelay(t *testing.T) {
	q := sharedQueue(t)
	job := enqueueAndClaim(t, q, uniqueQueueName(t), 3)

	tx, err := q.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := q.RetryTx(context.Background(), tx, job.ID, 400*time.Millisecond, errors.New("blip")); err != nil {
		t.Fatalf("retry: %v", err)
	}
	var delay time.Duration
	var micros int64
	if err := tx.QueryRow(`SELECT (EXTRACT(EPOCH FROM available_at - NOW()) * 1000000)::bigint FROM pgqueue_jobs WHERE id = $1`, job.ID).Scan(&micros); err != nil {
		t.Fatalf("read delay: %v", err)
	}
	delay = time.Duration(micros) * time.Microsecond
	if delay != 400*time.Millisecond {
		t.Fatalf("delay = %v, want 400ms", delay)
	}
}

func TestReapRefusesANonPositiveVisibilityTimeout(t *testing.T) {
	q := sharedQueue(t)
	for _, timeout := range []time.Duration{0, -time.Second} {
		if _, err := q.Reap(context.Background(), ReapParams{VisibilityTimeout: timeout}); !errors.Is(err, ErrInvalidVisibilityTimeout) {
			t.Fatalf("timeout %v: expected ErrInvalidVisibilityTimeout, got %v", timeout, err)
		}
	}
}

func TestAWorkerWithoutHandlersReapsNothing(t *testing.T) {
	q := sharedQueue(t)
	other := enqueueAndClaim(t, q, uniqueQueueName(t), 3)
	ageClaim(t, q, other.ID, "1 hour")
	worker, err := NewWorker(q, NewHandlerRegistry(), WorkerConfig{PollInterval: time.Hour, ReapInterval: time.Hour, VisibilityTimeout: time.Millisecond})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.reap(context.Background()); err != nil {
		t.Fatalf("reap: %v", err)
	}

	if job, _ := q.GetJob(context.Background(), other.ID); job.Status != StatusProcessing {
		t.Fatalf("a worker without handlers reaped another queue's job: %s", job.Status)
	}
}

func TestASnoozedClaimCannotActOnTheNextOne(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	first := enqueueAndClaim(t, q, queueName, 3)
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *first, time.Unix(0, 0)) }); err != nil {
		t.Fatalf("snooze: %v", err)
	}
	second := requireClaimable(t, q, queueName, true)

	if err := q.Heartbeat(context.Background(), *first); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("the snoozed claim still extends the new one: %v", err)
	}
	err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *first, time.Time{}) })
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("the snoozed claim parked the new one: %v", err)
	}
	if err := q.Heartbeat(context.Background(), *second); err != nil {
		t.Fatalf("current claim: %v", err)
	}
}

func TestAReplayedJobFencesTheClaimBeforeIt(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	stale := enqueueAndClaim(t, q, queueName, 1)
	ageClaim(t, q, stale.ID, "1 hour")
	if result, err := q.Reap(context.Background(), ReapParams{VisibilityTimeout: time.Minute, QueueNames: []string{queueName}}); err != nil || result.Failed != 1 {
		t.Fatalf("reap = %+v, err = %v, want the exhausted job failed", result, err)
	}
	if err := q.ReplayJob(context.Background(), stale.ID); err != nil {
		t.Fatalf("replay: %v", err)
	}
	current := requireClaimable(t, q, queueName, true)
	if current.Attempts != stale.Attempts {
		t.Fatalf("the replay did not reset attempts: %d", current.Attempts)
	}

	if err := q.Heartbeat(context.Background(), *stale); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("the claim before the replay extended the current one: %v", err)
	}
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.LockClaimTx(context.Background(), tx, *stale) }); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("the claim before the replay locked the current one: %v", err)
	}
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *stale, time.Time{}) }); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("the claim before the replay parked the current one: %v", err)
	}
	if err := q.Heartbeat(context.Background(), *current); err != nil {
		t.Fatalf("current claim: %v", err)
	}
}

func TestSnoozesDoNotGrowTheBudgetAReplayRestores(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	job := enqueueAndClaim(t, q, queueName, 1)
	for range 2 {
		if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *job, time.Unix(0, 0)) }); err != nil {
			t.Fatalf("snooze: %v", err)
		}
		job = requireClaimable(t, q, queueName, true)
	}
	failOnce := func() {
		t.Helper()
		if err := q.Retry(context.Background(), job.ID, 0, errors.New("boom")); err != nil {
			t.Fatalf("retry: %v", err)
		}
		failed, err := q.GetJob(context.Background(), job.ID)
		if err != nil || failed.Status != StatusFailed || failed.MaxAttempts != 1 {
			t.Fatalf("job = %+v, err = %v, want failed after its one attempt", failed, err)
		}
	}
	failOnce()

	if err := q.ReplayJob(context.Background(), job.ID); err != nil {
		t.Fatalf("replay: %v", err)
	}
	job = requireClaimable(t, q, queueName, true)
	failOnce()
}

func TestLockClaimTxHoldsOnlyACurrentClaim(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	stale := enqueueAndClaim(t, q, queueName, 3)
	ageClaim(t, q, stale.ID, "1 hour")
	if _, err := q.Reap(context.Background(), ReapParams{VisibilityTimeout: time.Minute, QueueNames: []string{queueName}}); err != nil {
		t.Fatalf("reap: %v", err)
	}
	current := requireClaimable(t, q, queueName, true)

	if err := inTx(t, q, func(tx *sql.Tx) error { return q.LockClaimTx(context.Background(), tx, *stale) }); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("stale claim: expected ErrJobNotFound, got %v", err)
	}
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.LockClaimTx(context.Background(), tx, *current) }); err != nil {
		t.Fatalf("current claim: %v", err)
	}
}

func TestWakeTxRolledBackChangesNothing(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	sub := subscribeReady(t, q, queueName)
	later := databaseNow(t, q).Add(time.Hour)
	id, err := q.Enqueue(context.Background(), EnqueueParams{QueueName: queueName, Payload: []byte(`{}`), AvailableAt: &later})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	requireWake(t, sub, 5*time.Second)

	rollback := errors.New("rollback")
	err = inTx(t, q, func(tx *sql.Tx) error {
		if err := q.WakeTx(context.Background(), tx, id); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("expected rollback, got %v", err)
	}

	requireNoWake(t, sub, 300*time.Millisecond)
	job, err := q.GetJob(context.Background(), id)
	if err != nil || !job.AvailableAt.Equal(later) {
		t.Fatalf("job = %+v, err = %v", job, err)
	}
}

func TestAHandlerFinishingDuringShutdownStillSettlesItsJob(t *testing.T) {
	cases := []struct {
		name       string
		outcome    error
		wantStatus JobStatus
		wantError  string
	}{
		{"success", nil, StatusDone, ""},
		{"retryable failure", errors.New("try later"), StatusPending, "try later"},
		{"final failure", NonRetryable(errors.New("give up")), StatusFailed, "give up"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := sharedQueue(t)
			queueName := uniqueQueueName(t)
			reached := make(chan struct{})
			release := make(chan struct{})
			worker, err := q.Worker("stopping").Pickup(PollEvery(10*time.Millisecond)).HandleRaw(queueName, func(context.Context, Job) error {
				close(reached)
				<-release
				return tc.outcome
			}).Build()
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			id, err := q.Enqueue(context.Background(), EnqueueParams{QueueName: queueName, Payload: []byte(`{}`)})
			if err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			<-reached

			cancel()
			close(release)
			if err := <-done; err != nil {
				t.Fatalf("run: %v", err)
			}

			job, err := q.GetJob(context.Background(), id)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if job.Status != tc.wantStatus || job.LastError.String != tc.wantError {
				t.Fatalf("job = %s %q, want %s %q", job.Status, job.LastError.String, tc.wantStatus, tc.wantError)
			}
		})
	}
}

func TestSnoozeTxRefusesAClaimTheReaperTookBack(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	stale := enqueueAndClaim(t, q, queueName, 3)
	if _, err := q.DB().Exec(`UPDATE pgqueue_jobs SET claimed_at = NOW() - interval '1 hour' WHERE id = $1`, stale.ID); err != nil {
		t.Fatalf("age claim: %v", err)
	}
	if _, err := q.Reap(context.Background(), ReapParams{VisibilityTimeout: time.Minute, QueueNames: []string{queueName}}); err != nil {
		t.Fatalf("reap: %v", err)
	}
	current := requireClaimable(t, q, queueName, true)

	err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, *stale, time.Time{}) })
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("a stale claim snoozed the job: %v", err)
	}
	still, err := q.GetJob(context.Background(), current.ID)
	if err != nil || still.Status != StatusProcessing {
		t.Fatalf("job = %+v, err = %v", still, err)
	}
}

func TestReapOnlyTouchesTheChosenQueues(t *testing.T) {
	q := sharedQueue(t)
	mine, other := uniqueQueueName(t), uniqueQueueName(t)+"-other"
	mineRetryable := enqueueAndClaim(t, q, mine, 3)
	mineExhausted := enqueueAndClaim(t, q, mine, 1)
	otherRetryable := enqueueAndClaim(t, q, other, 3)
	otherExhausted := enqueueAndClaim(t, q, other, 1)
	for _, job := range []*Job{mineRetryable, mineExhausted, otherRetryable, otherExhausted} {
		ageClaim(t, q, job.ID, "1 hour")
	}

	result, err := q.Reap(context.Background(), ReapParams{VisibilityTimeout: time.Minute, QueueNames: []string{mine}})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}

	if result != (ReapResult{Requeued: 1, Failed: 1}) {
		t.Fatalf("result = %+v, want one requeued and one failed", result)
	}
	want := map[int64]JobStatus{
		mineRetryable.ID: StatusPending, mineExhausted.ID: StatusFailed,
		otherRetryable.ID: StatusProcessing, otherExhausted.ID: StatusProcessing,
	}
	for id, status := range want {
		if job, _ := q.GetJob(context.Background(), id); job.Status != status {
			t.Fatalf("job %d = %s, want %s", id, job.Status, status)
		}
	}
}

func TestAJobClaimedAsTheWorkerStopsIsHandedBackUnhandled(t *testing.T) {
	sharedQueue(t)
	queueName := uniqueQueueName(t)
	claimed := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	q, err := New(shared.db, HookFunc(func(_ context.Context, kind EventKind, meta map[string]any) {
		if kind == EventClaim && meta["queue"] == queueName {
			once.Do(func() { close(claimed) })
			<-release
		}
	}))
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	var handled atomic.Int32
	worker, err := q.Worker("stopping").Pickup(PollEvery(10*time.Millisecond)).HandleRaw(queueName, func(context.Context, Job) error {
		handled.Add(1)
		return nil
	}).Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	id, err := q.Enqueue(context.Background(), EnqueueParams{QueueName: queueName, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-claimed

	cancel()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	if handled.Load() != 0 {
		t.Fatal("the worker handled a job it claimed while stopping")
	}
	job, err := q.GetJob(context.Background(), id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != StatusPending || job.Attempts != 0 || job.Claims != 1 || job.AvailableAt.After(databaseNow(t, q)) {
		t.Fatalf("job = %+v, want it pending, due now and its attempt refunded", job)
	}
}
