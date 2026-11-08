package queue

import "slices"

// JobState is the lifecycle state of a job, stored in pgqueue_jobs.state.
//
//	available ──claim──► running ──► completed | retryable | discarded | cancelled
//	scheduled, retryable ──due (scheduler or claim)──► available
//	completed, cancelled, discarded ──replay──► available
type JobState string

const (
	// StateAvailable jobs are due and wait for a worker.
	StateAvailable JobState = "available"
	// StateScheduled jobs wait for their availability time: enqueued for later,
	// snoozed, or parked until WakeTx.
	StateScheduled JobState = "scheduled"
	// StateRunning jobs are claimed by a worker.
	StateRunning JobState = "running"
	// StateRetryable jobs failed an attempt and wait for their backoff to end.
	StateRetryable JobState = "retryable"
	// StateCompleted jobs succeeded.
	StateCompleted JobState = "completed"
	// StateCancelled jobs were asked to stop: Client.Cancel or a handler returning
	// Cancel.
	StateCancelled JobState = "cancelled"
	// StateDiscarded jobs gave up: attempts exhausted, NonRetryable or Fail.
	StateDiscarded JobState = "discarded"
)

// JobStates lists every state in lifecycle order.
func JobStates() []JobState {
	return []JobState{
		StateAvailable, StateScheduled, StateRunning, StateRetryable,
		StateCompleted, StateCancelled, StateDiscarded,
	}
}

// Finalized reports whether a job in state s stays there until a replay or a
// retention purge.
func (s JobState) Finalized() bool {
	return s == StateCompleted || s == StateCancelled || s == StateDiscarded
}

// Valid reports whether s is one of the JobStates.
func (s JobState) Valid() bool {
	return slices.Contains(JobStates(), s)
}

// LegacyStatus is the coarse status that the pgqueue_jobs.status column keeps in
// sync with s for binaries and queries written before job states existed.
func (s JobState) LegacyStatus() JobStatus {
	switch s {
	case StateAvailable, StateScheduled, StateRetryable:
		return StatusPending
	case StateRunning:
		return StatusProcessing
	case StateCompleted:
		return StatusDone
	default:
		return StatusFailed
	}
}
