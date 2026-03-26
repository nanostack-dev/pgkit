package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type scanner interface {
	Scan(dest ...any) error
}

const (
	definitionSelectColumns = `id, workflow_name, version, status, title, description, graph_json, input_schema_json,
       metadata_json, content_hash, created_at, activated_at, deprecated_at, retired_at`
	runSelectColumns = `id, workflow_definition_id, workflow_name, workflow_version, status, input_json, context_json,
       started_at, completed_at, created_by, correlation_key, created_at, updated_at`
	stepSelectColumns = `id, run_id, step_name, item_key, step_kind, status, queue_job_id, attempt, max_attempts, input_json, output_json,
       error_json, dependency_json, available_at, started_at, completed_at, created_at, updated_at`
)

func scanDefinition(row scanner) (DefinitionRecord, error) {
	var record DefinitionRecord
	if err := row.Scan(
		&record.ID,
		&record.WorkflowName,
		&record.Version,
		&record.Status,
		&record.Title,
		&record.Description,
		&record.GraphJSON,
		&record.InputSchemaJSON,
		&record.MetadataJSON,
		&record.ContentHash,
		&record.CreatedAt,
		&record.ActivatedAt,
		&record.DeprecatedAt,
		&record.RetiredAt,
	); err != nil {
		return DefinitionRecord{}, err
	}
	return record, nil
}

func scanRun(row scanner) (RunRecord, error) {
	var record RunRecord
	if err := row.Scan(
		&record.ID,
		&record.WorkflowDefinitionID,
		&record.WorkflowName,
		&record.WorkflowVersion,
		&record.Status,
		&record.InputJSON,
		&record.ContextJSON,
		&record.StartedAt,
		&record.CompletedAt,
		&record.CreatedBy,
		&record.CorrelationKey,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return RunRecord{}, err
	}
	return record, nil
}

func scanStep(row scanner) (StepRecord, error) {
	var record StepRecord
	var dependencyJSON []byte
	if err := row.Scan(
		&record.ID,
		&record.RunID,
		&record.StepName,
		&record.ItemKey,
		&record.StepKind,
		&record.Status,
		&record.QueueJobID,
		&record.Attempt,
		&record.MaxAttempts,
		&record.InputJSON,
		&record.OutputJSON,
		&record.ErrorJSON,
		&dependencyJSON,
		&record.AvailableAt,
		&record.StartedAt,
		&record.CompletedAt,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return StepRecord{}, err
	}
	if len(dependencyJSON) > 0 {
		if err := json.Unmarshal(dependencyJSON, &record.DependencyJSON); err != nil {
			return StepRecord{}, fmt.Errorf("workflow: unmarshal step dependencies: %w", err)
		}
	}
	return record, nil
}

func getDefinitionByHash(ctx context.Context, tx *sql.Tx, workflowName, hash string) (*DefinitionRecord, error) {
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM workflow_definitions
WHERE workflow_name = $1 AND content_hash = $2
LIMIT 1`, definitionSelectColumns), workflowName, hash)
	record, err := scanDefinition(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("workflow: get definition by hash: %w", err)
	}
	return &record, nil
}

func nextDefinitionVersion(ctx context.Context, tx *sql.Tx, workflowName string) (int, error) {
	var next int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) + 1 FROM workflow_definitions WHERE workflow_name = $1`, workflowName,
	).Scan(&next); err != nil {
		return 0, fmt.Errorf("workflow: next definition version: %w", err)
	}
	return next, nil
}

func getDefinitionByVersion(ctx context.Context, tx *sql.Tx, workflowName string, version int) (*DefinitionRecord, error) {
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM workflow_definitions
WHERE workflow_name = $1 AND version = $2`, definitionSelectColumns), workflowName, version)
	record, err := scanDefinition(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s@v%d", ErrDefinitionNotFound, workflowName, version)
		}
		return nil, fmt.Errorf("workflow: get definition by version: %w", err)
	}
	return &record, nil
}

func getDefinitionByVersionDB(ctx context.Context, db *sql.DB, workflowName string, version int) (*DefinitionRecord, error) {
	row := db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM workflow_definitions
WHERE workflow_name = $1 AND version = $2`, definitionSelectColumns), workflowName, version)
	record, err := scanDefinition(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s@v%d", ErrDefinitionNotFound, workflowName, version)
		}
		return nil, fmt.Errorf("workflow: get definition by version: %w", err)
	}
	return &record, nil
}

func listStepsByRun(ctx context.Context, db queryer, runID string) ([]StepRecord, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT %s
FROM workflow_steps
WHERE run_id = $1
ORDER BY id ASC`, stepSelectColumns), runID)
	if err != nil {
		return nil, fmt.Errorf("workflow: list steps by run: %w", err)
	}
	defer func() { _ = rows.Close() }()
	steps := make([]StepRecord, 0)
	for rows.Next() {
		record, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		steps = append(steps, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workflow: iterate steps by run: %w", err)
	}
	return steps, nil
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func getRunByID(ctx context.Context, db queryer, runID string) (*RunRecord, error) {
	row := db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM workflow_runs
WHERE id = $1`, runSelectColumns), runID)
	record, err := scanRun(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
		}
		return nil, fmt.Errorf("workflow: get run: %w", err)
	}
	return &record, nil
}

func listRuns(ctx context.Context, db *sql.DB, params ListRunsParams) ([]RunRecord, error) {
	limit, offset := normalizeRunListPage(params)
	whereClause, args := buildRunFilterClause(params)
	query := fmt.Sprintf(`
SELECT %s
FROM workflow_runs%s
ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, runSelectColumns, whereClause, len(args)+1, len(args)+2)
	args = append(args, limit, offset)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("workflow: list runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	runs := make([]RunRecord, 0)
	for rows.Next() {
		record, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workflow: iterate runs: %w", err)
	}
	return runs, nil
}

func countRuns(ctx context.Context, db *sql.DB, params ListRunsParams) (int64, error) {
	whereClause, args := buildRunFilterClause(params)
	query := `SELECT COUNT(*) FROM workflow_runs` + whereClause
	var total int64
	if err := db.QueryRowContext(ctx, query, args...).Scan(&total); err != nil {
		return 0, fmt.Errorf("workflow: count runs: %w", err)
	}
	return total, nil
}

func normalizeRunListPage(params ListRunsParams) (int, int) {
	limit := params.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	offset := params.Offset
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func buildRunFilterClause(params ListRunsParams) (string, []any) {
	clauses := make([]string, 0, 3)
	args := make([]any, 0, 4)
	argIndex := 1
	if workflowName := strings.TrimSpace(params.WorkflowName); workflowName != "" {
		clauses = append(clauses, fmt.Sprintf("workflow_name = $%d", argIndex))
		args = append(args, workflowName)
		argIndex++
	}
	if params.Status != "" {
		clauses = append(clauses, fmt.Sprintf("status = $%d", argIndex))
		args = append(args, params.Status)
		argIndex++
	}
	if search := strings.TrimSpace(params.Search); search != "" {
		clauses = append(clauses, fmt.Sprintf("(workflow_name ILIKE $%d OR id ILIKE $%d OR correlation_key ILIKE $%d OR created_by ILIKE $%d)", argIndex, argIndex, argIndex, argIndex))
		args = append(args, "%"+search+"%")
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func getStepByQueueJobID(ctx context.Context, tx *sql.Tx, queueJobID int64) (*StepRecord, error) {
	// Lock the step row while the worker handles the claimed queue job so step
	// state transitions, retries, and the final ack all observe one consistent
	// record for this job execution.
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM workflow_steps
WHERE queue_job_id = $1
FOR UPDATE`, stepSelectColumns), queueJobID)
	record, err := scanStep(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: queue_job_id=%d", ErrStepNotFound, queueJobID)
		}
		return nil, fmt.Errorf("workflow: get step by queue job id: %w", err)
	}
	return &record, nil
}

func getDefinitionByID(ctx context.Context, tx *sql.Tx, definitionID int64) (*DefinitionRecord, error) {
	row := tx.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM workflow_definitions
WHERE id = $1`, definitionSelectColumns), definitionID)
	record, err := scanDefinition(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: id=%d", ErrDefinitionNotFound, definitionID)
		}
		return nil, fmt.Errorf("workflow: get definition by id: %w", err)
	}
	return &record, nil
}

func updateRunStatus(ctx context.Context, tx *sql.Tx, runID string, status RunStatus, completedAt *time.Time) error {
	var completed any
	if completedAt != nil {
		completed = completedAt.UTC()
	}
	// Some callers change run status before the run is terminal. Preserve the
	// existing completion timestamp unless the caller explicitly provides a new
	// terminal time.
	if _, err := tx.ExecContext(ctx, `
UPDATE workflow_runs
SET status = $2,
    completed_at = CASE WHEN $3::timestamptz IS NULL THEN completed_at ELSE $3 END,
    updated_at = NOW()
WHERE id = $1`, runID, status, completed); err != nil {
		return fmt.Errorf("workflow: update run status: %w", err)
	}
	return nil
}
