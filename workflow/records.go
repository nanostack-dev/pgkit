package workflow

import (
	"encoding/json"
	"time"
)

// RunStatus is where a run is in its life.
type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunWaiting   RunStatus = "waiting"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

// Finished reports whether the run reached a final status.
func (s RunStatus) Finished() bool {
	return s == RunSucceeded || s == RunFailed || s == RunCancelled
}

// StepKind is the durable operation a checkpoint records.
type StepKind string

const (
	KindStep   StepKind = "step"
	KindSleep  StepKind = "sleep"
	KindSignal StepKind = "signal"
	KindChild  StepKind = "child"
)

// StepStatus is where a checkpoint is in its life.
type StepStatus string

const (
	StepRunning   StepStatus = "running"
	StepWaiting   StepStatus = "waiting"
	StepRetrying  StepStatus = "retrying"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepTimedOut  StepStatus = "timed_out"
)

// RunInfo is a run as stored. Unset times are zero.
type RunInfo struct {
	ID          string          `json:"id"`
	Workflow    string          `json:"workflow"`
	Version     int             `json:"version"`
	Key         string          `json:"key,omitempty"`
	Status      RunStatus       `json:"status"`
	Input       json.RawMessage `json:"input"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
	TimedOut    bool            `json:"timed_out,omitempty"`
	ParentRunID string          `json:"parent_run_id,omitempty"`
	WakeAt      time.Time       `json:"wake_at"`
	DeadlineAt  time.Time       `json:"deadline_at"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   time.Time       `json:"started_at"`
	CompletedAt time.Time       `json:"completed_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// StepInfo is a checkpoint as stored. Name includes the #n suffix of a repeated
// step name. Unset times are zero.
type StepInfo struct {
	RunID       string          `json:"run_id"`
	Name        string          `json:"name"`
	Kind        StepKind        `json:"kind"`
	Status      StepStatus      `json:"status"`
	Attempts    int             `json:"attempts"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
	WakeAt      time.Time       `json:"wake_at"`
	ChildRunID  string          `json:"child_run_id,omitempty"`
	Signal      string          `json:"signal,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt time.Time       `json:"completed_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// ListRunsParams filters ListRuns and CountRuns; zero fields do not filter. Search
// matches run IDs, keys and workflow names.
type ListRunsParams struct {
	Workflow    string
	Status      RunStatus
	ParentRunID string
	Search      string
	Limit       int
	Offset      int
}

// PurgeParams selects the finished run trees Purge deletes: those whose top-level
// run completed more than OlderThan ago, at most Limit trees when Limit is positive.
type PurgeParams struct {
	OlderThan time.Duration
	Limit     int
}
