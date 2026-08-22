// Package zerologqueue builds queue.WorkerConfig's OnJobFailed/OnJobStuck
// callbacks from a zerolog.Logger. queue itself stays logging-library
// agnostic (see queue.Logger), so this lives in its own package: importing
// it is opt-in for callers who already use zerolog, and queue never takes a
// zerolog dependency.
package zerologqueue

import (
	"context"

	"github.com/nanostack-dev/pgkit/queue"
	"github.com/rs/zerolog"
)

// WorkerCallbacks builds the OnJobFailed and OnJobStuck pair most
// queue.WorkerConfig values in this codebase reimplement by hand: a
// permanently-failed job logged at error with its id, queue, attempts and
// last error, and a stuck-job reap logged at the level severity picks for the
// result. A nil severity always logs a reap at warn, matching queue's own
// reap logging.
func WorkerCallbacks(
	logger zerolog.Logger,
	label string,
	severity func(queue.ReapResult) zerolog.Level,
) (
	onJobFailed func(context.Context, queue.Job),
	onJobStuck func(context.Context, queue.ReapResult),
) {
	if severity == nil {
		severity = func(queue.ReapResult) zerolog.Level { return zerolog.WarnLevel }
	}

	onJobFailed = func(_ context.Context, job queue.Job) {
		logger.Error().
			Int64("job_id", job.ID).
			Str("queue", job.QueueName).
			Int("attempts", job.Attempts).
			Str("last_error", job.LastError.String).
			Msg(label + " job permanently failed")
	}

	onJobStuck = func(_ context.Context, result queue.ReapResult) {
		logger.WithLevel(severity(result)).
			Int64("requeued", result.Requeued).
			Int64("failed", result.Failed).
			Msg(label + " jobs stuck in processing were reaped")
	}

	return onJobFailed, onJobStuck
}

// EscalateOnFailure is a ready-made severity policy: a reap that dead-letters
// at least one job (Failed > 0) logs at error, since that job's failure never
// reaches OnJobFailed; a reap that only requeued jobs logs at warn.
func EscalateOnFailure(result queue.ReapResult) zerolog.Level {
	if result.Failed > 0 {
		return zerolog.ErrorLevel
	}
	return zerolog.WarnLevel
}
