package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	qpkg "github.com/nanostack-dev/pgkit/queue"
)

const internalQueueName = "workflow.internal.step"

type jobPayload struct {
	RunID    string `json:"run_id"`
	StepName string `json:"step_name"`
	ItemKey  string `json:"item_key,omitempty"`
}

type RunView struct {
	Run   RunRecord
	Steps []StepRecord
}

func (m *Module) GetRunGraphView(ctx context.Context, runID string) (*RunGraphView, error) {
	run, err := getRunByID(ctx, m.db, runID)
	if err != nil {
		return nil, err
	}
	defRecord, err := getDefinitionByVersionDB(ctx, m.db, run.WorkflowName, run.WorkflowVersion)
	if err != nil {
		return nil, err
	}
	var graph Graph
	if err := json.Unmarshal(defRecord.GraphJSON, &graph); err != nil {
		return nil, fmt.Errorf("workflow: unmarshal run graph: %w", err)
	}
	steps, err := listStepsByRun(ctx, m.db, runID)
	if err != nil {
		return nil, err
	}
	view, err := buildRunGraphView(*run, *defRecord, graph, steps)
	if err != nil {
		return nil, err
	}
	return view, nil
}

func (m *Module) Start(ctx context.Context, workflowName string, input any, opts *StartRunOptions) (*RunRecord, error) {
	defRecord, err := m.GetActiveDefinition(ctx, workflowName)
	if err != nil {
		return nil, err
	}
	return m.StartVersion(ctx, workflowName, defRecord.Version, input, opts)
}

func (m *Module) StartVersion(
	ctx context.Context,
	workflowName string,
	version int,
	input any,
	opts *StartRunOptions,
) (*RunRecord, error) {
	inputJSON, err := encodeJSON(input)
	if err != nil {
		return nil, err
	}
	contextJSON := []byte(`{}`)
	if opts != nil && len(opts.ContextJSON) > 0 {
		contextJSON = opts.ContextJSON
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("workflow: begin start tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	defRecord, err := getDefinitionByVersion(ctx, tx, workflowName, version)
	if err != nil {
		return nil, err
	}
	def, err := m.resolveDefinitionByRecord(defRecord)
	if err != nil {
		return nil, err
	}
	if err := def.validate(); err != nil {
		return nil, err
	}
	runID := uuid.NewString()
	row := tx.QueryRowContext(ctx, `
INSERT INTO workflow_runs (
    id, workflow_definition_id, workflow_name, workflow_version, status, input_json, context_json, created_by, correlation_key
) VALUES ($1, $2, $3, $4, 'running', $5::jsonb, $6::jsonb, $7, $8)
RETURNING id, workflow_definition_id, workflow_name, workflow_version, status, input_json, context_json,
          started_at, completed_at, created_by, correlation_key, created_at, updated_at`,
		runID,
		defRecord.ID,
		workflowName,
		version,
		string(inputJSON),
		string(contextJSON),
		nullStringFromOpts(opts, func(o *StartRunOptions) string { return o.CreatedBy }),
		nullStringFromOpts(opts, func(o *StartRunOptions) string { return o.CorrelationKey }),
	)
	run, err := scanRun(row)
	if err != nil {
		return nil, fmt.Errorf("workflow: insert run: %w", err)
	}
	steps := def.Steps()
	if err := createRunSteps(ctx, tx, m.queue, runID, inputJSON, steps); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("workflow: commit start tx: %w", err)
	}
	return &run, nil
}

func (m *Module) GetRun(ctx context.Context, runID string) (*RunView, error) {
	run, err := getRunByID(ctx, m.db, runID)
	if err != nil {
		return nil, err
	}
	steps, err := listStepsByRun(ctx, m.db, runID)
	if err != nil {
		return nil, err
	}
	return &RunView{Run: *run, Steps: steps}, nil
}

func buildRunGraphView(run RunRecord, definition DefinitionRecord, graph Graph, steps []StepRecord) (*RunGraphView, error) {
	byName := make(map[string][]StepRecord, len(steps))
	for _, step := range steps {
		byName[step.StepName] = append(byName[step.StepName], step)
	}
	for _, node := range graph.Nodes {
		if _, ok := byName[node.ID]; !ok {
			byName[node.ID] = nil
		}
	}
	nodes := make([]RunGraphNodeView, 0, len(graph.Nodes))
	summary := RunGraphViewSummary{TotalNodes: len(graph.Nodes)}
	for _, node := range graph.Nodes {
		records := append([]StepRecord(nil), byName[node.ID]...)
		sort.Slice(records, func(i, j int) bool {
			if records[i].ItemKey == records[j].ItemKey {
				return records[i].ID < records[j].ID
			}
			return records[i].ItemKey < records[j].ItemKey
		})
		nodeView := RunGraphNodeView{Node: node}
		root, items := splitRootAndItemRecords(records)
		if root != nil {
			record := *root
			nodeView.Step = &record
		}
		if len(items) == 0 {
			if root == nil {
				nodeView.Status = StepStatusPending
			} else {
				nodeView.Status = root.Status
			}
		} else {
			nodeView.Items = items
			nodeView.Status = aggregateNodeStatus(root, items)
			for _, record := range items {
				incrementNodeItemCounts(&nodeView.ItemCounts, record.Status)
			}
		}
		accumulateGraphSummary(&summary, nodeView)
		nodes = append(nodes, nodeView)
	}
	return &RunGraphView{
		Run:        run,
		Definition: definition,
		Graph:      graph,
		Nodes:      nodes,
		Edges:      append([]GraphEdge(nil), graph.Edges...),
		Summary:    summary,
	}, nil
}

func aggregateNodeStatus(root *StepRecord, records []StepRecord) StepStatus {
	if root != nil && root.Status == StepStatusFailed {
		return StepStatusFailed
	}
	if root != nil && root.Status != StepStatusSucceeded {
		return root.Status
	}
	hasRunning := false
	hasQueued := false
	hasPending := false
	hasWaitingRetry := false
	hasCancelled := false
	hasSkipped := false
	allSucceeded := len(records) > 0
	for _, record := range records {
		switch record.Status {
		case StepStatusFailed:
			return StepStatusFailed
		case StepStatusRunning:
			hasRunning = true
			allSucceeded = false
		case StepStatusWaitingRetry:
			hasWaitingRetry = true
			allSucceeded = false
		case StepStatusQueued:
			hasQueued = true
			allSucceeded = false
		case StepStatusPending:
			hasPending = true
			allSucceeded = false
		case StepStatusCancelled:
			hasCancelled = true
			allSucceeded = false
		case StepStatusSkipped:
			hasSkipped = true
			allSucceeded = false
		case StepStatusSucceeded:
		default:
			allSucceeded = false
		}
	}
	if allSucceeded {
		return StepStatusSucceeded
	}
	if hasRunning {
		return StepStatusRunning
	}
	if hasWaitingRetry {
		return StepStatusWaitingRetry
	}
	if hasQueued {
		return StepStatusQueued
	}
	if hasPending {
		return StepStatusPending
	}
	if hasCancelled {
		return StepStatusCancelled
	}
	if hasSkipped {
		return StepStatusSkipped
	}
	return StepStatusPending
}

func incrementNodeItemCounts(counts *RunGraphNodeItemCounts, status StepStatus) {
	counts.Total++
	switch status {
	case StepStatusPending:
		counts.Pending++
	case StepStatusQueued:
		counts.Queued++
	case StepStatusRunning:
		counts.Running++
	case StepStatusWaitingRetry:
		counts.WaitingRetry++
	case StepStatusSucceeded:
		counts.Succeeded++
	case StepStatusFailed:
		counts.Failed++
	case StepStatusCancelled:
		counts.Cancelled++
	case StepStatusSkipped:
		counts.Skipped++
	}
}

func accumulateGraphSummary(summary *RunGraphViewSummary, node RunGraphNodeView) {
	if node.Items != nil {
		summary.TotalItems += node.ItemCounts.Total
		summary.FailedItems += node.ItemCounts.Failed
	}
	switch node.Status {
	case StepStatusPending:
		summary.PendingNodes++
	case StepStatusQueued:
		summary.QueuedNodes++
	case StepStatusRunning:
		summary.RunningNodes++
	case StepStatusWaitingRetry:
		summary.RetryingNodes++
	case StepStatusSucceeded:
		summary.SucceededNodes++
	case StepStatusFailed:
		summary.FailedNodes++
	case StepStatusCancelled:
		summary.CancelledNodes++
	case StepStatusSkipped:
		summary.SkippedNodes++
	}
}

type Worker struct {
	module   *Module
	registry *qpkg.HandlerRegistry
	worker   *qpkg.Worker
	cfg      WorkerConfig
}

func NewWorker(module *Module, cfg WorkerConfig) (*Worker, error) {
	if module == nil || module.db == nil {
		return nil, ErrNilDB
	}
	if module.queue == nil {
		return nil, ErrNilQueue
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	registry := qpkg.NewHandlerRegistry()
	if err := qpkg.RegisterJSONTyped[jobPayload](registry, internalQueueName, func(ctx context.Context, payload jobPayload, job qpkg.Job) error {
		return module.executeJob(ctx, payload, job, cfg)
	}); err != nil {
		return nil, err
	}
	pqWorker, err := qpkg.NewWorker(module.queue, registry, qpkg.WorkerConfig{
		WorkerID:          cfg.WorkerID,
		PollInterval:      cfg.PollInterval,
		ReapInterval:      cfg.ReapInterval,
		VisibilityTimeout: cfg.VisibilityTimeout,
		BatchSizePerQueue: cfg.BatchSizePerQueue,
		BackoffBase:       cfg.BackoffBase,
		BackoffMax:        cfg.BackoffMax,
	})
	if err != nil {
		return nil, err
	}
	return &Worker{module: module, registry: registry, worker: pqWorker, cfg: cfg}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	return w.worker.Run(ctx)
}

func (m *Module) executeJob(ctx context.Context, payload jobPayload, job qpkg.Job, cfg WorkerConfig) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("workflow: begin execute tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	step, err := getStepByQueueJobID(ctx, tx, job.ID)
	if err != nil {
		return NonRetryable(err)
	}
	if step.RunID != payload.RunID || step.StepName != payload.StepName || step.ItemKey != payload.ItemKey {
		return NonRetryable(ErrInvalidJobPayload)
	}
	run, err := getRunByID(ctx, tx, step.RunID)
	if err != nil {
		return NonRetryable(err)
	}
	if run.Status != RunStatusRunning {
		if err := cancelStepForInactiveRun(ctx, tx, step.ID); err != nil {
			return NonRetryable(err)
		}
		if err := m.queue.AckTx(ctx, tx, job.ID); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("workflow: commit inactive run tx: %w", err)
		}
		m.log.Info(ctx, "workflow step skipped for inactive run", map[string]any{"run_id": run.ID, "step_name": step.StepName, "item_key": step.ItemKey, "run_status": run.Status})
		return qpkg.Handled()
	}
	if step.Status != StepStatusQueued && step.Status != StepStatusWaitingRetry {
		return NonRetryable(fmt.Errorf("%w: %s status=%s", ErrStepNotRunnable, step.StepName, step.Status))
	}
	defRecord, err := getDefinitionByID(ctx, tx, run.WorkflowDefinitionID)
	if err != nil {
		return NonRetryable(err)
	}
	def, err := m.resolveDefinitionByRecord(defRecord)
	if err != nil {
		return NonRetryable(err)
	}
	if defRecord.Version != run.WorkflowVersion {
		return NonRetryable(fmt.Errorf("workflow: run definition version mismatch: run=%d definition=%d", run.WorkflowVersion, defRecord.Version))
	}
	spec, ok := def.Step(step.StepName)
	if !ok {
		return NonRetryable(fmt.Errorf("%w: %s", ErrStepNotFound, step.StepName))
	}
	if err := markStepRunning(ctx, tx, step.ID); err != nil {
		return err
	}
	state, err := loadSucceededOutputs(ctx, tx, run.ID)
	if err != nil {
		return err
	}
	stepCtx := buildStepExecutionContext(run, step, state, m.log)
	output, err := executeStepHandler(ctx, tx, spec, stepCtx)
	if err != nil {
		if handleErr := m.handleStepFailure(ctx, tx, run, step, spec, job.ID, err, cfg); handleErr != nil {
			return NonRetryable(handleErr)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("workflow: commit failure tx: %w", err)
		}
		return qpkg.Handled()
	}
	outputJSON, err := encodeJSON(output)
	if err != nil {
		return NonRetryable(err)
	}
	completed, err := m.completeStepSuccess(ctx, tx, run, step, spec, job.ID, outputJSON, def, state)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workflow: commit success tx: %w", err)
	}
	if completed {
		m.log.Info(ctx, "workflow run completed", map[string]any{"run_id": run.ID})
	}
	return qpkg.Handled()
}

func buildStepExecutionContext(run *RunRecord, step *StepRecord, state map[string]json.RawMessage, logger Logger) StepContext {
	return StepContext{
		RunID:        run.ID,
		WorkflowName: run.WorkflowName,
		Version:      run.WorkflowVersion,
		StepName:     step.StepName,
		ItemKey:      step.ItemKey,
		Attempt:      step.Attempt + 1,
		RunInput:     json.RawMessage(run.InputJSON),
		Input:        json.RawMessage(step.InputJSON),
		Logger:       logger,
		state:        state,
	}
}

func executeStepHandler(ctx context.Context, tx *sql.Tx, spec StepSpec, stepCtx StepContext) (any, error) {
	if spec.Kind == StepKindForEach && stepCtx.ItemKey == rootStepItemKey {
		return spec.Resolver(ctx, stepCtx)
	}
	if spec.TxHandler != nil {
		return spec.TxHandler(ctx, tx, stepCtx)
	}
	return spec.Handler(ctx, stepCtx)
}

func (m *Module) completeStepSuccess(
	ctx context.Context,
	tx *sql.Tx,
	run *RunRecord,
	step *StepRecord,
	spec StepSpec,
	queueJobID int64,
	outputJSON []byte,
	def *Definition,
	state map[string]json.RawMessage,
) (bool, error) {
	if err := markStepSucceeded(ctx, tx, step.ID, outputJSON); err != nil {
		return false, NonRetryable(err)
	}
	if spec.Kind == StepKindForEach && step.ItemKey == rootStepItemKey {
		if err := materializeForEachChildren(ctx, tx, m.queue, run.ID, spec, outputJSON); err != nil {
			return false, err
		}
	}
	rememberStepOutput(state, step.StepName, step.ItemKey, outputJSON)
	if err := scheduleUnlockedDependents(ctx, tx, m.queue, run.ID, def); err != nil {
		return false, err
	}
	completed, err := finalizeRunIfDone(ctx, tx, run.ID)
	if err != nil {
		return false, err
	}
	if err := m.queue.AckTx(ctx, tx, queueJobID); err != nil {
		return false, err
	}
	return completed, nil
}

func createRunSteps(
	ctx context.Context,
	tx *sql.Tx,
	queue *qpkg.Client,
	runID string,
	inputJSON []byte,
	steps []StepSpec,
) error {
	sorted := append([]StepSpec(nil), steps...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for _, step := range sorted {
		depsJSON, err := json.Marshal(step.DependsOn)
		if err != nil {
			return fmt.Errorf("workflow: marshal step dependencies: %w", err)
		}
		row := tx.QueryRowContext(ctx, `
INSERT INTO workflow_steps (
    run_id, step_name, item_key, step_kind, status, max_attempts, input_json, dependency_json
) VALUES ($1, $2, '', $3, 'pending', $4, $5::jsonb, $6::jsonb)
RETURNING id, run_id, step_name, item_key, step_kind, status, queue_job_id, attempt, max_attempts, input_json, output_json,
          error_json, dependency_json, available_at, started_at, completed_at, created_at, updated_at`,
			runID,
			step.Name,
			step.Kind,
			step.RetryPolicy.MaxAttempts,
			string(inputJSON),
			string(depsJSON),
		)
		record, err := scanStep(row)
		if err != nil {
			return fmt.Errorf("workflow: insert step: %w", err)
		}
		if len(step.DependsOn) == 0 {
			if err := enqueueStep(ctx, tx, queue, record.ID, runID, step.Name, step.RetryPolicy.MaxAttempts); err != nil {
				return err
			}
		}
	}
	return nil
}

func enqueueStep(ctx context.Context, tx *sql.Tx, queue *qpkg.Client, stepID int64, runID, stepName string, maxAttempts int) error {
	return enqueueStepWithItem(ctx, tx, queue, stepID, runID, stepName, "", maxAttempts)
}

func enqueueStepWithItem(ctx context.Context, tx *sql.Tx, queue *qpkg.Client, stepID int64, runID, stepName, itemKey string, maxAttempts int) error {
	payloadJSON, err := json.Marshal(jobPayload{RunID: runID, StepName: stepName, ItemKey: itemKey})
	if err != nil {
		return fmt.Errorf("workflow: marshal queue payload: %w", err)
	}
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	jobID, err := queue.EnqueueTx(ctx, tx, qpkg.EnqueueParams{
		QueueName:   internalQueueName,
		Payload:     payloadJSON,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		return fmt.Errorf("workflow: enqueue step job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE workflow_steps
SET status = 'queued', queue_job_id = $2, available_at = NOW(), updated_at = NOW()
WHERE id = $1 AND status IN ('pending', 'waiting_retry')`, stepID, jobID); err != nil {
		return fmt.Errorf("workflow: mark step queued: %w", err)
	}
	return nil
}

func markStepRunning(ctx context.Context, tx *sql.Tx, stepID int64) error {
	res, err := tx.ExecContext(ctx, `
UPDATE workflow_steps
SET status = 'running', attempt = attempt + 1, started_at = NOW(), updated_at = NOW()
WHERE id = $1 AND status IN ('queued', 'waiting_retry')`, stepID)
	if err != nil {
		return fmt.Errorf("workflow: mark step running: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return ErrStepNotRunnable
	}
	return nil
}

func markStepSucceeded(ctx context.Context, tx *sql.Tx, stepID int64, outputJSON []byte) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE workflow_steps
SET status = 'succeeded', output_json = $2::jsonb, error_json = NULL, completed_at = NOW(), updated_at = NOW()
WHERE id = $1`, stepID, string(outputJSON)); err != nil {
		return fmt.Errorf("workflow: mark step succeeded: %w", err)
	}
	return nil
}

func (m *Module) handleStepFailure(
	ctx context.Context,
	tx *sql.Tx,
	run *RunRecord,
	step *StepRecord,
	spec StepSpec,
	queueJobID int64,
	handlerErr error,
	cfg WorkerConfig,
) error {
	retryable := !isNonRetryable(handlerErr)
	errJSON, err := encodeErrorJSON(handlerErr)
	if err != nil {
		return err
	}
	if retryable && step.Attempt+1 < spec.RetryPolicy.MaxAttempts {
		delay := qpkg.ExponentialBackoff(spec.RetryPolicy.BackoffBase, step.Attempt+1, spec.RetryPolicy.BackoffMax)
		if _, err := tx.ExecContext(ctx, `
UPDATE workflow_steps
SET status = 'waiting_retry', error_json = $2::jsonb, updated_at = NOW()
WHERE id = $1`, step.ID, string(errJSON)); err != nil {
			return fmt.Errorf("workflow: mark step waiting retry: %w", err)
		}
		if err := m.queue.RetryTx(ctx, tx, queueJobID, delay, handlerErr); err != nil {
			return err
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE workflow_steps
SET status = 'failed', error_json = $2::jsonb, completed_at = NOW(), updated_at = NOW()
WHERE id = $1`, step.ID, string(errJSON)); err != nil {
		return fmt.Errorf("workflow: mark step failed: %w", err)
	}
	now := time.Now().UTC()
	if err := updateRunStatus(ctx, tx, run.ID, RunStatusFailed, &now); err != nil {
		return err
	}
	if err := cancelPendingStepsForRun(ctx, tx, run.ID, step.ID); err != nil {
		return err
	}
	if err := m.queue.FailTx(ctx, tx, queueJobID, handlerErr); err != nil {
		return err
	}
	if cfg.OnStepFailed != nil {
		cfg.OnStepFailed(ctx, StepRecord{ID: step.ID, RunID: step.RunID, StepName: step.StepName, ItemKey: step.ItemKey, Status: StepStatusFailed, Attempt: step.Attempt + 1})
	}
	if cfg.OnRunFailed != nil {
		cfg.OnRunFailed(ctx, RunRecord{ID: run.ID, WorkflowName: run.WorkflowName, WorkflowVersion: run.WorkflowVersion, Status: RunStatusFailed})
	}
	return nil
}

func cancelStepForInactiveRun(ctx context.Context, tx *sql.Tx, stepID int64) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE workflow_steps
SET status = CASE
        WHEN status IN ('succeeded', 'failed', 'cancelled', 'skipped') THEN status
        ELSE 'cancelled'
    END,
    completed_at = CASE
        WHEN status IN ('succeeded', 'failed', 'cancelled', 'skipped') THEN completed_at
        ELSE NOW()
    END,
    updated_at = NOW()
WHERE id = $1`, stepID); err != nil {
		return fmt.Errorf("workflow: cancel step for inactive run: %w", err)
	}
	return nil
}

func cancelPendingStepsForRun(ctx context.Context, tx *sql.Tx, runID string, failedStepID int64) error {
	// Only steps that have not reached a terminal state are cancelled here. This
	// keeps completed history intact while preventing still-actionable work from
	// being picked up after the run is already marked failed.
	if _, err := tx.ExecContext(ctx, `
UPDATE workflow_steps
SET status = 'cancelled', completed_at = NOW(), updated_at = NOW()
WHERE run_id = $1
  AND id <> $2
  AND status IN ('pending', 'queued', 'waiting_retry')`, runID, failedStepID); err != nil {
		return fmt.Errorf("workflow: cancel pending run steps: %w", err)
	}
	return nil
}

func loadSucceededOutputs(ctx context.Context, tx *sql.Tx, runID string) (map[string]json.RawMessage, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT step_name, item_key, output_json
FROM workflow_steps
WHERE run_id = $1 AND status = 'succeeded'`, runID)
	if err != nil {
		return nil, fmt.Errorf("workflow: load step outputs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	state := make(map[string]json.RawMessage)
	for rows.Next() {
		var stepName string
		var itemKey string
		var output []byte
		if err := rows.Scan(&stepName, &itemKey, &output); err != nil {
			return nil, fmt.Errorf("workflow: scan step output: %w", err)
		}
		rememberStepOutput(state, stepName, itemKey, output)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workflow: iterate step outputs: %w", err)
	}
	return state, nil
}

func materializeForEachChildren(
	ctx context.Context,
	tx *sql.Tx,
	queue *qpkg.Client,
	runID string,
	spec StepSpec,
	outputJSON []byte,
) error {
	var items []json.RawMessage
	if len(outputJSON) > 0 {
		if err := json.Unmarshal(outputJSON, &items); err != nil {
			return NonRetryable(fmt.Errorf("workflow: foreach step %s must return a JSON array: %w", spec.Name, err))
		}
	}
	for idx, item := range items {
		depsJSON, err := json.Marshal(spec.DependsOn)
		if err != nil {
			return fmt.Errorf("workflow: marshal foreach dependencies: %w", err)
		}
		itemKey := fmt.Sprintf("%06d", idx)
		// Foreach child rows are unique per (run, step, item). The upsert keeps
		// materialization idempotent when the parent step is replayed while still
		// refreshing the child input payload derived from the resolver output.
		row := tx.QueryRowContext(ctx, `
INSERT INTO workflow_steps (
    run_id, step_name, item_key, step_kind, status, max_attempts, input_json, dependency_json
) VALUES ($1, $2, $3, $4, 'pending', $5, $6::jsonb, $7::jsonb)
ON CONFLICT (run_id, step_name, item_key) DO UPDATE SET input_json = EXCLUDED.input_json
RETURNING id, run_id, step_name, item_key, step_kind, status, queue_job_id, attempt, max_attempts, input_json, output_json,
          error_json, dependency_json, available_at, started_at, completed_at, created_at, updated_at`,
			runID,
			spec.Name,
			itemKey,
			spec.Kind,
			spec.RetryPolicy.MaxAttempts,
			string(item),
			string(depsJSON),
		)
		record, err := scanStep(row)
		if err != nil {
			return fmt.Errorf("workflow: insert foreach child step: %w", err)
		}
		if record.Status == StepStatusPending {
			if err := enqueueStepWithItem(ctx, tx, queue, record.ID, runID, record.StepName, record.ItemKey, spec.RetryPolicy.MaxAttempts); err != nil {
				return err
			}
		}
	}
	return nil
}

func scheduleUnlockedDependents(
	ctx context.Context,
	tx *sql.Tx,
	queue *qpkg.Client,
	runID string,
	def *Definition,
) error {
	steps, err := listStepsByRun(ctx, tx, runID)
	if err != nil {
		return err
	}
	byName := make(map[string][]StepRecord, len(steps))
	for _, step := range steps {
		byName[step.StepName] = append(byName[step.StepName], step)
	}
	for _, spec := range def.Steps() {
		records, ok := byName[spec.Name]
		if !ok || len(records) == 0 {
			continue
		}
		record, ok := findRootStepRecord(records)
		if !ok {
			continue
		}
		if record.Status != StepStatusPending {
			continue
		}
		ready := true
		for _, dep := range spec.DependsOn {
			depRecords, ok := byName[dep]
			if !ok || len(depRecords) == 0 {
				ready = false
				break
			}
			if !allStepRecordsSucceeded(depRecords) {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		if err := enqueueStep(ctx, tx, queue, record.ID, runID, record.StepName, spec.RetryPolicy.MaxAttempts); err != nil {
			return err
		}
	}
	return nil
}

func finalizeRunIfDone(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT status FROM workflow_steps WHERE run_id = $1`, runID)
	if err != nil {
		return false, fmt.Errorf("workflow: list run statuses: %w", err)
	}
	defer func() { _ = rows.Close() }()
	allDone := true
	for rows.Next() {
		var status StepStatus
		if err := rows.Scan(&status); err != nil {
			return false, fmt.Errorf("workflow: scan step status: %w", err)
		}
		switch status {
		case StepStatusSucceeded, StepStatusSkipped, StepStatusCancelled:
		default:
			allDone = false
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("workflow: iterate run statuses: %w", err)
	}
	if !allDone {
		return false, nil
	}
	now := time.Now().UTC()
	if err := updateRunStatus(ctx, tx, runID, RunStatusSucceeded, &now); err != nil {
		return false, err
	}
	return true, nil
}

func encodeJSON(v any) ([]byte, error) {
	if v == nil {
		return []byte(`{}`), nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("workflow: marshal json: %w", err)
	}
	if len(data) == 0 {
		return []byte(`{}`), nil
	}
	return data, nil
}

func encodeErrorJSON(err error) ([]byte, error) {
	if err == nil {
		return []byte(`{}`), nil
	}
	payload := map[string]any{"message": err.Error()}
	return encodeJSON(payload)
}

func nullStringFromOpts(opts *StartRunOptions, getter func(*StartRunOptions) string) any {
	if opts == nil {
		return nil
	}
	value := getter(opts)
	if value == "" {
		return nil
	}
	return value
}
