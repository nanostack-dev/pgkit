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
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, job.ID, until) }); err != nil {
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

	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, job.ID, time.Time{}) }); err != nil {
		t.Fatalf("snooze: %v", err)
	}
	requireClaimable(t, q, queueName, false)

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
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, job.ID, past) }); err != nil {
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
	if err := inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, job.ID, later) }); err != nil {
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
	err = inTx(t, q, func(tx *sql.Tx) error { return q.SnoozeTx(context.Background(), tx, id, time.Time{}) })
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
		if err := q.SnoozeTx(context.Background(), tx, job.ID, time.Time{}); err != nil {
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
