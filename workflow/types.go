package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type DefinitionStatus string

const (
	DefinitionStatusDraft      DefinitionStatus = "draft"
	DefinitionStatusActive     DefinitionStatus = "active"
	DefinitionStatusDeprecated DefinitionStatus = "deprecated"
	DefinitionStatusRetired    DefinitionStatus = "retired"
)

type RunStatus string

const (
	RunStatusRunning    RunStatus = "running"
	RunStatusSucceeded  RunStatus = "succeeded"
	RunStatusFailed     RunStatus = "failed"
	RunStatusCancelling RunStatus = "cancelling"
	RunStatusCancelled  RunStatus = "cancelled"
)

type StepStatus string

const (
	StepStatusPending      StepStatus = "pending"
	StepStatusQueued       StepStatus = "queued"
	StepStatusRunning      StepStatus = "running"
	StepStatusWaitingRetry StepStatus = "waiting_retry"
	StepStatusSucceeded    StepStatus = "succeeded"
	StepStatusFailed       StepStatus = "failed"
	StepStatusCancelled    StepStatus = "cancelled"
	StepStatusSkipped      StepStatus = "skipped"
)

type StepKind string

const (
	StepKindStep    StepKind = "step"
	StepKindForEach StepKind = "foreach"
)

type RetryPolicy struct {
	MaxAttempts int
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.BackoffBase <= 0 {
		p.BackoffBase = time.Second
	}
	if p.BackoffMax <= 0 {
		p.BackoffMax = 30 * time.Second
	}
	return p
}

type DefinitionRecord struct {
	ID              int64
	WorkflowName    string
	Version         int
	Status          DefinitionStatus
	Title           string
	Description     sql.NullString
	GraphJSON       []byte
	InputSchemaJSON []byte
	MetadataJSON    []byte
	ContentHash     string
	CreatedAt       time.Time
	ActivatedAt     sql.NullTime
	DeprecatedAt    sql.NullTime
	RetiredAt       sql.NullTime
}

type RunRecord struct {
	ID                   string
	WorkflowDefinitionID int64
	WorkflowName         string
	WorkflowVersion      int
	Status               RunStatus
	InputJSON            []byte
	ContextJSON          []byte
	StartedAt            time.Time
	CompletedAt          sql.NullTime
	CreatedBy            sql.NullString
	CorrelationKey       sql.NullString
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type StepRecord struct {
	ID             int64
	RunID          string
	StepName       string
	ItemKey        string
	StepKind       StepKind
	Status         StepStatus
	QueueJobID     sql.NullInt64
	Attempt        int
	MaxAttempts    int
	InputJSON      []byte
	OutputJSON     []byte
	ErrorJSON      []byte
	AvailableAt    sql.NullTime
	StartedAt      sql.NullTime
	CompletedAt    sql.NullTime
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DependencyJSON []string
}

type Graph struct {
	Name        string         `json:"name"`
	Version     int            `json:"version"`
	Title       string         `json:"title"`
	Description string         `json:"description,omitempty"`
	Nodes       []GraphNode    `json:"nodes"`
	Edges       []GraphEdge    `json:"edges"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type GraphNode struct {
	ID          string         `json:"id"`
	Kind        string         `json:"kind"`
	Label       string         `json:"label"`
	Queue       string         `json:"queue,omitempty"`
	MaxAttempts int            `json:"max_attempts,omitempty"`
	DependsOn   []string       `json:"depends_on,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type GraphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type GraphDiff struct {
	AddedNodes   []string    `json:"added_nodes"`
	RemovedNodes []string    `json:"removed_nodes"`
	ChangedNodes []string    `json:"changed_nodes"`
	AddedEdges   []GraphEdge `json:"added_edges"`
	RemovedEdges []GraphEdge `json:"removed_edges"`
	FromVersion  int         `json:"from_version"`
	ToVersion    int         `json:"to_version"`
	WorkflowName string      `json:"workflow_name"`
}

type StartRunOptions struct {
	CreatedBy      string
	CorrelationKey string
	ContextJSON    []byte
}

type ListRunsParams struct {
	WorkflowName string
	Status       RunStatus
	Search       string
	Limit        int
	Offset       int
}

type StepContext struct {
	RunID        string
	WorkflowName string
	Version      int
	StepName     string
	ItemKey      string
	Attempt      int
	RunInput     json.RawMessage
	Input        json.RawMessage
	Logger       Logger
	state        map[string]json.RawMessage
}

type StepItemOutput struct {
	ItemKey string          `json:"item_key"`
	Payload json.RawMessage `json:"payload"`
}

type RunGraphView struct {
	Run        RunRecord           `json:"run"`
	Definition DefinitionRecord    `json:"definition"`
	Graph      Graph               `json:"graph"`
	Nodes      []RunGraphNodeView  `json:"nodes"`
	Edges      []GraphEdge         `json:"edges"`
	Summary    RunGraphViewSummary `json:"summary"`
}

type RunGraphViewSummary struct {
	TotalNodes     int `json:"total_nodes"`
	PendingNodes   int `json:"pending_nodes"`
	QueuedNodes    int `json:"queued_nodes"`
	RunningNodes   int `json:"running_nodes"`
	RetryingNodes  int `json:"retrying_nodes"`
	SucceededNodes int `json:"succeeded_nodes"`
	FailedNodes    int `json:"failed_nodes"`
	CancelledNodes int `json:"cancelled_nodes"`
	SkippedNodes   int `json:"skipped_nodes"`
	TotalItems     int `json:"total_items"`
	FailedItems    int `json:"failed_items"`
}

type RunGraphNodeView struct {
	Node       GraphNode              `json:"node"`
	Status     StepStatus             `json:"status"`
	Step       *StepRecord            `json:"step,omitempty"`
	Items      []StepRecord           `json:"items,omitempty"`
	ItemCounts RunGraphNodeItemCounts `json:"item_counts"`
}

type RunGraphNodeItemCounts struct {
	Total        int `json:"total"`
	Pending      int `json:"pending"`
	Queued       int `json:"queued"`
	Running      int `json:"running"`
	WaitingRetry int `json:"waiting_retry"`
	Succeeded    int `json:"succeeded"`
	Failed       int `json:"failed"`
	Cancelled    int `json:"cancelled"`
	Skipped      int `json:"skipped"`
}

func (c StepContext) DecodeRunInput(target any) error {
	if target == nil {
		return fmt.Errorf("workflow: decode run input target is nil")
	}
	if len(c.RunInput) == 0 {
		return nil
	}
	return json.Unmarshal(c.RunInput, target)
}

func (c StepContext) DecodeInput(target any) error {
	if target == nil {
		return fmt.Errorf("workflow: decode input target is nil")
	}
	if len(c.Input) == 0 {
		return nil
	}
	return json.Unmarshal(c.Input, target)
}

func (c StepContext) Output(stepName string, target any) error {
	payload, ok := c.state[stepOutputStateKey(stepName, rootStepItemKey)]
	if !ok || len(payload) == 0 {
		return fmt.Errorf("%w: %s", ErrStepOutputNotFound, stepName)
	}
	if target == nil {
		return fmt.Errorf("workflow: output target is nil")
	}
	return json.Unmarshal(payload, target)
}

func (c StepContext) ItemOutput(stepName, itemKey string, target any) error {
	key := stepOutputStateKey(stepName, itemKey)
	payload, ok := c.state[key]
	if !ok || len(payload) == 0 {
		return fmt.Errorf("%w: %s", ErrStepOutputNotFound, key)
	}
	if target == nil {
		return fmt.Errorf("workflow: item output target is nil")
	}
	return json.Unmarshal(payload, target)
}

func (c StepContext) ItemOutputs(stepName string) []StepItemOutput {
	return collectStepItemOutputs(c.state, stepName)
}

type StepHandler func(ctx context.Context, step StepContext) (any, error)

type TxStepHandler func(ctx context.Context, tx *sql.Tx, step StepContext) (any, error)

type ForEachResolver func(ctx context.Context, step StepContext) ([]any, error)

type StepOptions struct {
	Label       string
	QueueName   string
	DependsOn   []string
	RetryPolicy RetryPolicy
	Metadata    map[string]any
}

type StepSpec struct {
	Name        string
	Kind        StepKind
	Label       string
	QueueName   string
	DependsOn   []string
	RetryPolicy RetryPolicy
	Handler     StepHandler
	TxHandler   TxStepHandler
	Resolver    ForEachResolver
	Metadata    map[string]any
}

type Definition struct {
	Name        string
	Title       string
	Description string
	Metadata    map[string]any
	steps       []StepSpec
}

type PublishResult struct {
	Definition DefinitionRecord
	Published  bool
}

type WorkerConfig struct {
	WorkerID          string
	PollInterval      time.Duration
	ReapInterval      time.Duration
	VisibilityTimeout time.Duration
	BatchSizePerQueue int
	BackoffBase       time.Duration
	BackoffMax        time.Duration
	OnRunFailed       func(ctx context.Context, run RunRecord)
	OnStepFailed      func(ctx context.Context, step StepRecord)
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.WorkerID == "" {
		c.WorkerID = "workflow-worker"
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 100 * time.Millisecond
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = 5 * time.Second
	}
	if c.VisibilityTimeout <= 0 {
		c.VisibilityTimeout = 30 * time.Second
	}
	if c.BatchSizePerQueue <= 0 {
		c.BatchSizePerQueue = 25
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 30 * time.Second
	}
	return c
}

func (c WorkerConfig) validate() error {
	if c.PollInterval <= 0 || c.ReapInterval <= 0 || c.VisibilityTimeout <= 0 {
		return ErrInvalidWorkerConfig
	}
	if c.BatchSizePerQueue <= 0 || c.BackoffBase <= 0 || c.BackoffMax <= 0 {
		return ErrInvalidWorkerConfig
	}
	return nil
}
