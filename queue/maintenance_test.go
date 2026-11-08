package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestTheSchedulerMakesDueScheduledAndRetryableJobsAvailable(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		past := time.Now().Add(-time.Minute)
		later := time.Now().Add(time.Hour)
		dueScheduled := mustEnqueue(t, q, EnqueueParams{QueueName: "sched", AvailableAt: &later})
		futureScheduled := mustEnqueue(t, q, EnqueueParams{QueueName: "sched", AvailableAt: &later})
		dueRetryable := mustEnqueue(t, q, EnqueueParams{QueueName: "sched"})
		mustClaim(t, q, "sched", 0)
		if err := q.Retry(ctx, dueRetryable, time.Hour, errors.New("later")); err != nil {
			t.Fatalf("retry: %v", err)
		}
		futureRetryable := mustEnqueue(t, q, EnqueueParams{QueueName: "sched"})
		mustClaim(t, q, "sched", 0)
		if err := q.Retry(ctx, futureRetryable, time.Hour, errors.New("later")); err != nil {
			t.Fatalf("retry: %v", err)
		}
		running := mustEnqueue(t, q, EnqueueParams{QueueName: "sched"})
		mustClaim(t, q, "sched", 0)
		d.exec(t, `UPDATE pgqueue_jobs SET available_at = $2 WHERE id IN ($1, $3)`, dueScheduled, past, dueRetryable)

		promoted, err := q.scheduleDue(ctx, 100)
		if err != nil || promoted != 2 {
			t.Fatalf("scheduleDue = %d, %v; want 2", promoted, err)
		}
		d.requireState(t, dueScheduled, StateAvailable)
		d.requireState(t, dueRetryable, StateAvailable)
		d.requireState(t, futureScheduled, StateScheduled)
		d.requireState(t, futureRetryable, StateRetryable)
		d.requireState(t, running, StateRunning)
	})
}

func TestTheSchedulerWorksInBatches(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		q := d.client(t)
		past := time.Now().Add(-time.Second)
		for range 7 {
			id := mustEnqueue(t, q, EnqueueParams{QueueName: "batch", AvailableAt: &past})
			d.exec(t, `UPDATE pgqueue_jobs SET state = 'scheduled' WHERE id = $1`, id)
		}
		promoted, err := q.scheduleDue(context.Background(), 3)
		if err != nil || promoted != 7 {
			t.Fatalf("scheduleDue in batches of 3 = %d, %v; want 7", promoted, err)
		}
	})
}

func TestTheSchedulerNotifiesEachQueueWithNewlyAvailableJobs(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		q := d.client(t)
		later := time.Now().Add(time.Hour)
		for _, name := range []string{"notify-a", "notify-b"} {
			id := mustEnqueue(t, q, EnqueueParams{QueueName: name, AvailableAt: &later})
			d.exec(t, `UPDATE pgqueue_jobs SET available_at = NOW() WHERE id = $1`, id)
		}
		subA := subscribeReady(t, q, "notify-a")
		subB := subscribeReady(t, q, "notify-b")
		subC := subscribeReady(t, q, "notify-c")
		if _, err := q.scheduleDue(context.Background(), 100); err != nil {
			t.Fatalf("scheduleDue: %v", err)
		}
		requireWake(t, subA, 2*time.Second)
		requireWake(t, subB, 2*time.Second)
		requireNoWake(t, subC, 300*time.Millisecond)
	})
}

func TestTheRescuerTakesBackRunningJobsWhoseLeaseExpired(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		claimAs := func(maxAttempts int, lease time.Duration) int64 {
			id := mustEnqueue(t, q, EnqueueParams{QueueName: "rescue", MaxAttempts: maxAttempts})
			if job := mustClaim(t, q, "rescue", lease); job.ID != id {
				t.Fatalf("claimed %d, want %d", job.ID, id)
			}
			return id
		}
		retried := claimAs(3, time.Minute)
		exhausted := claimAs(1, time.Minute)
		cancelled := claimAs(3, time.Minute)
		alive := claimAs(3, time.Minute)
		noLeaseOld := claimAs(3, 0)
		noLeaseYoung := claimAs(3, 0)
		d.exec(t, `UPDATE pgqueue_jobs SET lease_expires_at = NOW() - interval '1 second' WHERE id IN ($1, $2, $3)`,
			retried, exhausted, cancelled)
		if _, err := q.Cancel(ctx, cancelled); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		d.exec(t, `UPDATE pgqueue_jobs SET claimed_at = NOW() - interval '2 hours' WHERE id = $1`, noLeaseOld)

		jobs, err := q.rescueExpired(ctx, time.Hour, 100)
		if err != nil {
			t.Fatalf("rescue: %v", err)
		}
		if result := countRescued(jobs); result != (ReapResult{Requeued: 2, Failed: 1, Cancelled: 1}) {
			t.Fatalf("rescue result = %+v", result)
		}
		d.requireState(t, retried, StateAvailable)
		d.requireState(t, noLeaseOld, StateAvailable)
		d.requireState(t, exhausted, StateDiscarded)
		d.requireState(t, cancelled, StateCancelled)
		d.requireState(t, alive, StateRunning)
		d.requireState(t, noLeaseYoung, StateRunning)
		if job, _ := q.GetJob(ctx, exhausted); job.LastError.String != rescueReason || !job.DoneAt.Valid {
			t.Fatalf("discarded job = last error %q done %v", job.LastError.String, job.DoneAt)
		}
	})
}

func TestTheRescuerWorksInBatches(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		q := d.client(t)
		for range 5 {
			mustEnqueue(t, q, EnqueueParams{QueueName: "rescue-batch"})
			mustClaim(t, q, "rescue-batch", time.Minute)
		}
		d.exec(t, `UPDATE pgqueue_jobs SET lease_expires_at = NOW() - interval '1 second'`)
		jobs, err := q.rescueExpired(context.Background(), time.Hour, 2)
		if err != nil || len(jobs) != 5 {
			t.Fatalf("rescue in batches of 2 = %d jobs, %v; want 5", len(jobs), err)
		}
	})
}

func TestHeartbeatExtendsTheLeaseByItsOwnLength(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		mustEnqueue(t, q, EnqueueParams{QueueName: "beat"})
		job := mustClaim(t, q, "beat", 10*time.Minute)
		d.exec(t, `UPDATE pgqueue_jobs SET claimed_at = claimed_at - interval '9 minutes',
			lease_expires_at = lease_expires_at - interval '9 minutes' WHERE id = $1`, job.ID)
		if err := q.Heartbeat(ctx, *job); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
		var remaining time.Duration
		var seconds float64
		if err := d.db.QueryRowContext(ctx,
			`SELECT EXTRACT(EPOCH FROM lease_expires_at - NOW())::float8 FROM pgqueue_jobs WHERE id = $1`, job.ID,
		).Scan(&seconds); err != nil {
			t.Fatalf("read lease: %v", err)
		}
		remaining = time.Duration(seconds * float64(time.Second))
		if remaining < 9*time.Minute || remaining > 10*time.Minute {
			t.Fatalf("lease after heartbeat ends in %s, want about 10m", remaining)
		}
	})
}

func TestOnlyTheLeaderRunsMaintenanceAndAFollowerTakesOver(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		first := d.client(t)
		second := d.bareClient(t)
		for _, q := range []*Client{first, second} {
			if err := q.SetMaintenance(MaintenanceConfig{ScheduleInterval: 50 * time.Millisecond}); err != nil {
				t.Fatalf("set maintenance: %v", err)
			}
		}
		leaveFirst := first.maintainer.join(nil)
		requireEventually(t, 5*time.Second, 10*time.Millisecond, first.IsLeader)
		leaveSecond := second.maintainer.join(nil)
		defer leaveSecond()

		later := time.Now().Add(time.Hour)
		id := mustEnqueue(t, first, EnqueueParams{QueueName: "lead", AvailableAt: &later})
		d.exec(t, `UPDATE pgqueue_jobs SET available_at = NOW() WHERE id = $1`, id)
		d.requireStateEventually(t, id, StateAvailable, 3*time.Second)
		if second.IsLeader() {
			t.Fatal("both clients lead")
		}

		leaveFirst()
		requireEventually(t, 5*time.Second, 10*time.Millisecond, second.IsLeader)
		other := mustEnqueue(t, second, EnqueueParams{QueueName: "lead", AvailableAt: &later})
		d.exec(t, `UPDATE pgqueue_jobs SET available_at = NOW() WHERE id = $1`, other)
		d.requireStateEventually(t, other, StateAvailable, 3*time.Second)
	})
}

func TestALeaderWithRetentionPurgesEachFinalState(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		q := d.client(t)
		finalize := func(state JobState) int64 {
			id := mustEnqueue(t, q, EnqueueParams{QueueName: "clean"})
			switch state {
			case StateCompleted:
				mustClaim(t, q, "clean", 0)
				_ = q.Ack(ctx, id)
			case StateDiscarded:
				mustClaim(t, q, "clean", 0)
				_ = q.Fail(ctx, id, errors.New("boom"))
			case StateCancelled:
				_, _ = q.Cancel(ctx, id)
			}
			d.requireState(t, id, state)
			d.exec(t, `UPDATE pgqueue_jobs SET updated_at = NOW() - interval '2 days' WHERE id = $1`, id)
			return id
		}
		completed := finalize(StateCompleted)
		discarded := finalize(StateDiscarded)
		cancelled := finalize(StateCancelled)
		keptCancelled := finalize(StateCancelled)
		d.exec(t, `UPDATE pgqueue_jobs SET updated_at = NOW() WHERE id = $1`, keptCancelled)
		pending := mustEnqueue(t, q, EnqueueParams{QueueName: "clean"})
		d.exec(t, `UPDATE pgqueue_jobs SET updated_at = NOW() - interval '30 days' WHERE id = $1`, pending)

		if err := q.SetMaintenance(MaintenanceConfig{Retention: &RetentionPolicy{
			KeepDone: time.Hour, KeepCancelled: time.Hour, BatchSize: 1,
		}}); err != nil {
			t.Fatalf("set maintenance: %v", err)
		}
		leave := q.maintainer.join(nil)
		defer leave()

		requireEventually(t, 5*time.Second, 20*time.Millisecond, func() bool {
			_, err := q.GetJob(ctx, cancelled)
			return errors.Is(err, ErrJobNotFound)
		})
		if _, err := q.GetJob(ctx, completed); !errors.Is(err, ErrJobNotFound) {
			t.Fatalf("completed job past retention survived: %v", err)
		}
		for _, id := range []int64{discarded, keptCancelled, pending} {
			if _, err := q.GetJob(ctx, id); err != nil {
				t.Fatalf("job %d was purged: %v", id, err)
			}
		}
	})
}

func TestARescueReportsStuckJobsToTheWorkerOfTheirQueue(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		q := d.client(t)
		if err := q.SetMaintenance(MaintenanceConfig{RescueInterval: 50 * time.Millisecond}); err != nil {
			t.Fatalf("set maintenance: %v", err)
		}
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "stuck-report"})
		mustClaim(t, q, "stuck-report", time.Minute)
		d.exec(t, `UPDATE pgqueue_jobs SET lease_expires_at = NOW() - interval '1 second', available_at = NOW() + interval '1 hour' WHERE id = $1`, id)

		var reported atomic.Int64
		var handled atomic.Int32
		worker, err := q.Worker("reporter").
			Pickup(PollEvery(time.Hour)).
			Tune(WorkerConfig{ReapInterval: time.Hour, OnJobStuck: func(_ context.Context, result ReapResult) {
				reported.Add(result.Requeued)
			}}).
			HandleRaw("stuck-report", func(context.Context, Job) error {
				handled.Add(1)
				return nil
			}).
			Build()
		if err != nil {
			t.Fatalf("build worker: %v", err)
		}
		runWorker(t, worker)
		requireEventually(t, 5*time.Second, 20*time.Millisecond, func() bool { return reported.Load() == 1 })
		d.requireState(t, id, StateAvailable)
	})
}

func TestAnOnEnqueueWorkerTakesADelayedJobWhenTheSchedulerMakesItDue(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		q := d.client(t)
		if err := q.SetMaintenance(MaintenanceConfig{ScheduleInterval: 50 * time.Millisecond}); err != nil {
			t.Fatalf("set maintenance: %v", err)
		}
		handled := make(chan int64, 1)
		worker, err := q.Worker("delayed").
			Pickup(OnEnqueue().RescanEvery(noRescan)).
			HandleRaw("delayed", func(_ context.Context, job Job) error {
				handled <- job.ID
				return nil
			}).
			Build()
		if err != nil {
			t.Fatalf("build worker: %v", err)
		}
		runWorker(t, worker)
		<-worker.Ready()

		soon := time.Now().Add(500 * time.Millisecond)
		id := mustEnqueue(t, q, EnqueueParams{QueueName: "delayed", AvailableAt: &soon})
		select {
		case got := <-handled:
			if got != id {
				t.Fatalf("handled %d, want %d", got, id)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the delayed job was not taken once due, without a rescan")
		}
	})
}

func TestMaintenanceConfigRejectsNegativeSettings(t *testing.T) {
	q := sharedQueue(t)
	for _, config := range []MaintenanceConfig{
		{ScheduleInterval: -time.Second},
		{RescueInterval: -time.Second},
		{RescueAfter: -time.Second},
		{BatchSize: -1},
		{Retention: &RetentionPolicy{}},
	} {
		if err := q.SetMaintenance(config); err == nil {
			t.Fatalf("SetMaintenance(%+v) accepted an invalid config", config)
		}
	}
}

// A rescue that read a running job from its snapshot must not revive it when the
// handler settles it before the rescue locks the row. pg_sleep in the stuck
// condition holds the rescue in that window while the job is acked. The
// production condition sits in the locking scan, which PostgreSQL rechecks on the
// newest row version; a condition behind a subquery is not rechecked, so that
// case proves the second guard, on the rescuing UPDATE itself, holds on its own.
func TestARescueNeverRevivesAJobSettledWhileItRuns(t *testing.T) {
	sleepy := expiredClaimSQL + ` AND pg_sleep(1) IS NOT NULL`
	for name, stuck := range map[string]string{
		"locking scan": sleepy,
		"subquery":     `id IN (SELECT id FROM pgqueue_jobs WHERE ` + sleepy + `)`,
	} {
		t.Run(name, func(t *testing.T) {
			eachDriver(t, func(t *testing.T, d driverDB) {
				ctx := context.Background()
				q := d.client(t)
				id := mustEnqueue(t, q, EnqueueParams{QueueName: "rescue-race"})
				mustClaim(t, q, "rescue-race", time.Minute)
				d.exec(t, `UPDATE pgqueue_jobs SET lease_expires_at = NOW() - interval '1 second' WHERE id = $1`, id)

				type outcome struct {
					jobs []rescuedJob
					err  error
				}
				done := make(chan outcome, 1)
				go func() {
					jobs, err := q.rescueJobs(ctx, stuck, 10, rescueReason, time.Hour.Seconds())
					done <- outcome{jobs, err}
				}()
				waitForSleepingQuery(t, d)
				if err := q.Ack(ctx, id); err != nil {
					t.Fatalf("ack during the rescue: %v", err)
				}

				got := <-done
				if got.err != nil || len(got.jobs) != 0 {
					t.Fatalf("rescue = %+v, err %v; want nothing rescued", got.jobs, got.err)
				}
				d.requireState(t, id, StateCompleted)
			})
		})
	}
}

// waitForSleepingQuery waits until a session of d's database is in pg_sleep.
func waitForSleepingQuery(t *testing.T, d driverDB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var sleeping int
		if err := d.db.QueryRowContext(context.Background(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event = 'PgSleep'`).Scan(&sleeping); err != nil {
			t.Fatalf("read pg_stat_activity: %v", err)
		}
		if sleeping > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no session reached pg_sleep")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
