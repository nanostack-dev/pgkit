package queue

import (
	"context"
	"database/sql"
	"errors"
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

func TestSnoozeTxDelaysTheJobWithoutSpendingItsAttempt(t *testing.T) {
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
	if snoozed.Status != StatusPending || snoozed.Attempts != 0 || snoozed.ClaimedBy.Valid {
		t.Fatalf("snoozed job = %+v", snoozed)
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
	if woken.ID != job.ID || woken.Attempts != 1 {
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

func TestReapStuckJobsHonorsASubSecondVisibilityTimeout(t *testing.T) {
	q := sharedQueue(t)
	job := enqueueAndClaim(t, q, uniqueQueueName(t), 3)

	if _, err := q.ReapStuckJobs(context.Background(), 500*time.Millisecond); err != nil {
		t.Fatalf("reap: %v", err)
	}
	if fresh, err := q.GetJob(context.Background(), job.ID); err != nil || fresh.Status != StatusProcessing {
		t.Fatalf("a job claimed just now was reaped with a 500ms visibility timeout: %+v %v", fresh, err)
	}
	time.Sleep(600 * time.Millisecond)
	if _, err := q.ReapStuckJobs(context.Background(), 500*time.Millisecond); err != nil {
		t.Fatalf("reap: %v", err)
	}
	reaped, err := q.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if reaped.Status != StatusPending {
		t.Fatalf("status = %s, want pending after the timeout", reaped.Status)
	}
}

func TestRetryHonorsASubSecondDelay(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	job := enqueueAndClaim(t, q, queueName, 3)

	if err := q.Retry(context.Background(), job.ID, 400*time.Millisecond, errors.New("blip")); err != nil {
		t.Fatalf("retry: %v", err)
	}
	requireClaimable(t, q, queueName, false)
	time.Sleep(500 * time.Millisecond)
	requireClaimable(t, q, queueName, true)
}

func TestAHandlerFinishingDuringShutdownStillSettlesItsJob(t *testing.T) {
	q := sharedQueue(t)
	queueName := uniqueQueueName(t)
	reached := make(chan struct{})
	release := make(chan struct{})
	worker, err := q.Worker("stopping").Pickup(PollEvery(10*time.Millisecond)).HandleRaw(queueName, func(context.Context, Job) error {
		close(reached)
		<-release
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
	if job.Status != StatusDone {
		t.Fatalf("status = %s, want done", job.Status)
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
	mineJob := enqueueAndClaim(t, q, mine, 3)
	otherJob := enqueueAndClaim(t, q, other, 3)
	if _, err := q.DB().Exec(`UPDATE pgqueue_jobs SET claimed_at = NOW() - interval '1 hour' WHERE id IN ($1, $2)`, mineJob.ID, otherJob.ID); err != nil {
		t.Fatalf("age claims: %v", err)
	}

	result, err := q.Reap(context.Background(), ReapParams{VisibilityTimeout: time.Minute, QueueNames: []string{mine}})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}

	if result.Requeued != 1 {
		t.Fatalf("requeued %d jobs, want 1", result.Requeued)
	}
	if job, _ := q.GetJob(context.Background(), otherJob.ID); job.Status != StatusProcessing {
		t.Fatalf("another queue's job was reaped: %s", job.Status)
	}
}
