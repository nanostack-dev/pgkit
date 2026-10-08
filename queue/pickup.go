package queue

import "time"

const defaultRescanInterval = 5 * time.Second

// Pickup decides when a worker looks for claimable jobs. Build one with
// PollEvery or OnEnqueue; the zero value falls back to WorkerConfig.PollInterval.
type Pickup struct {
	onEnqueue bool
	scanEvery time.Duration
}

// PollEvery scans the worker's queues every interval.
func PollEvery(interval time.Duration) Pickup {
	return Pickup{scanEvery: interval}
}

// OnEnqueue claims as soon as a job for one of the worker's queues may have become
// claimable: enqueued, replayed or requeued by the reaper. Notifications are hints
// sent at commit, so the worker still rescans every 5 seconds (see RescanEvery) to
// pick up delayed jobs and retries as they fall due, and anything sent while its
// listener was reconnecting.
//
// All workers of a Client share one dedicated LISTEN connection, opened outside
// the *sql.DB pool. It is configured like the pool's connections with the pgx
// stdlib driver; with any other driver, give it one with Client.ListenOn.
func OnEnqueue() Pickup {
	return Pickup{onEnqueue: true, scanEvery: defaultRescanInterval}
}

// RescanEvery sets how often the worker scans without being notified.
func (p Pickup) RescanEvery(interval time.Duration) Pickup {
	p.scanEvery = interval
	return p
}

func (p Pickup) isZero() bool {
	return p == Pickup{}
}
