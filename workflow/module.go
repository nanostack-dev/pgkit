package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	qpkg "github.com/nanostack-dev/pgkit/queue"
)

const schemaVersion = 1

type Module struct {
	db          *sql.DB
	queue       *qpkg.Client
	definitions map[string][]*Definition
	log         Logger
}

func New(db *sql.DB, queue *qpkg.Client, definitions ...*Definition) (*Module, error) {
	if db == nil {
		return nil, ErrNilDB
	}
	if queue == nil {
		return nil, ErrNilQueue
	}
	m := &Module{
		db:          db,
		queue:       queue,
		definitions: make(map[string][]*Definition, len(definitions)),
		log:         noopLogger{},
	}
	for _, def := range definitions {
		if def == nil {
			continue
		}
		if err := m.Register(def); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Module) SetLogger(logger Logger) {
	if m == nil {
		return
	}
	if logger == nil {
		m.log = noopLogger{}
		return
	}
	m.log = logger
}

func (m *Module) Register(def *Definition) error {
	if def == nil {
		return ErrDefinitionNotFound
	}
	if err := def.validate(); err != nil {
		return err
	}
	if m.definitions == nil {
		m.definitions = map[string][]*Definition{}
	}
	hash, _, err := def.ContentHash()
	if err != nil {
		return err
	}
	for _, existing := range m.definitions[def.Name] {
		existingHash, _, err := existing.ContentHash()
		if err != nil {
			return err
		}
		if existingHash == hash {
			return nil
		}
	}
	m.definitions[def.Name] = append(m.definitions[def.Name], def)
	return nil
}

func (m *Module) currentDefinition(name string) (*Definition, bool) {
	defs, ok := m.definitions[name]
	if !ok || len(defs) == 0 {
		return nil, false
	}
	return defs[len(defs)-1], true
}

func (m *Module) definitionsForName(name string) ([]*Definition, bool) {
	defs, ok := m.definitions[name]
	if !ok || len(defs) == 0 {
		return nil, false
	}
	out := make([]*Definition, len(defs))
	copy(out, defs)
	return out, true
}

func (m *Module) resolveDefinitionByRecord(record *DefinitionRecord) (*Definition, error) {
	if record == nil {
		return nil, ErrDefinitionNotFound
	}
	defs, ok := m.definitions[record.WorkflowName]
	if !ok {
		return nil, fmt.Errorf("%w: %s@v%d", ErrDefinitionNotRegistered, record.WorkflowName, record.Version)
	}
	for _, def := range defs {
		hash, _, err := def.ContentHash()
		if err != nil {
			return nil, err
		}
		if hash == record.ContentHash {
			return def, nil
		}
	}
	return nil, fmt.Errorf("%w: %s@v%d", ErrDefinitionNotRegistered, record.WorkflowName, record.Version)
}

func (m *Module) RegisteredDefinitions() []string {
	names := make([]string, 0, len(m.definitions))
	for name := range m.definitions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (m *Module) EnsureSchema(ctx context.Context) error {
	if m == nil || m.db == nil {
		return ErrNilDB
	}
	if _, err := m.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS workflow_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)`); err != nil {
		return fmt.Errorf("workflow: create meta table: %w", err)
	}
	var currentVersion int
	err := m.db.QueryRowContext(ctx, `SELECT value::int FROM workflow_meta WHERE key = 'schema_version'`).Scan(&currentVersion)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("workflow: read schema version: %w", err)
	}
	if currentVersion < 1 {
		if err := m.migrateV1(ctx); err != nil {
			return err
		}
	}
	if _, err := m.db.ExecContext(ctx, `
INSERT INTO workflow_meta (key, value) VALUES ('schema_version', $1)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, fmt.Sprintf("%d", schemaVersion)); err != nil {
		return fmt.Errorf("workflow: upsert schema version: %w", err)
	}
	return nil
}

func (m *Module) migrateV1(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS workflow_definitions (
    id BIGSERIAL PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('draft', 'active', 'deprecated', 'retired')),
    title TEXT NOT NULL,
    description TEXT,
    graph_json JSONB NOT NULL,
    input_schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at TIMESTAMPTZ,
    deprecated_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ,
    UNIQUE (workflow_name, version)
);

CREATE INDEX IF NOT EXISTS idx_workflow_definitions_name_status
ON workflow_definitions(workflow_name, status, version DESC);

CREATE TABLE IF NOT EXISTS workflow_definition_aliases (
    workflow_name TEXT PRIMARY KEY,
    active_definition_id BIGINT NOT NULL REFERENCES workflow_definitions(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id TEXT PRIMARY KEY,
    workflow_definition_id BIGINT NOT NULL REFERENCES workflow_definitions(id),
    workflow_name TEXT NOT NULL,
    workflow_version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelling', 'cancelled')),
    input_json JSONB NOT NULL,
    context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_by TEXT,
    correlation_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_definition
ON workflow_runs(workflow_definition_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_name_version
ON workflow_runs(workflow_name, workflow_version, created_at DESC);

CREATE TABLE IF NOT EXISTS workflow_steps (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    step_name TEXT NOT NULL,
    item_key TEXT NOT NULL DEFAULT '',
    step_kind TEXT NOT NULL CHECK (step_kind IN ('step', 'foreach')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'queued', 'running', 'waiting_retry', 'succeeded', 'failed', 'cancelled', 'skipped')),
    queue_job_id BIGINT,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    input_json JSONB NOT NULL,
    output_json JSONB,
    error_json JSONB,
    dependency_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    available_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, step_name, item_key)
);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_run_status
ON workflow_steps(run_id, status, step_name);

CREATE INDEX IF NOT EXISTS idx_workflow_steps_queue_job
ON workflow_steps(queue_job_id)
WHERE queue_job_id IS NOT NULL;
`)
	if err != nil {
		return fmt.Errorf("workflow: migrate v1: %w", err)
	}
	return nil
}

func (m *Module) PublishAll(ctx context.Context) ([]PublishResult, error) {
	if len(m.definitions) == 0 {
		return nil, ErrNoRegisteredDefinitions
	}
	results := make([]PublishResult, 0, len(m.definitions))
	for _, name := range m.RegisteredDefinitions() {
		result, err := m.Publish(ctx, name)
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	}
	return results, nil
}

func (m *Module) Publish(ctx context.Context, workflowName string) (*PublishResult, error) {
	defs, ok := m.definitionsForName(workflowName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDefinitionNotRegistered, workflowName)
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("workflow: begin publish tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var def *Definition
	for _, candidate := range defs {
		hash, _, err := candidate.ContentHash()
		if err != nil {
			return nil, fmt.Errorf("workflow: compute definition hash: %w", err)
		}
		existing, err := getDefinitionByHash(ctx, tx, workflowName, hash)
		if err != nil {
			return nil, err
		}
		if existing == nil {
			def = candidate
			break
		}
	}
	if def == nil {
		latestVersion, err := nextDefinitionVersion(ctx, tx, workflowName)
		if err != nil {
			return nil, err
		}
		// nextDefinitionVersion returns the next unused version slot. When every
		// registered definition is already published, the current latest record is
		// therefore one version behind that value.
		latest, err := getDefinitionByVersion(ctx, tx, workflowName, latestVersion-1)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("workflow: commit publish existing tx: %w", err)
		}
		return &PublishResult{Definition: *latest, Published: false}, nil
	}
	hash, _, err := def.ContentHash()
	if err != nil {
		return nil, fmt.Errorf("workflow: compute definition hash: %w", err)
	}
	nextVersion, err := nextDefinitionVersion(ctx, tx, workflowName)
	if err != nil {
		return nil, err
	}
	graph := def.Graph(nextVersion)
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		return nil, fmt.Errorf("workflow: marshal graph json: %w", err)
	}
	metadataJSON, err := encodeJSONOrEmpty(def.Metadata)
	if err != nil {
		return nil, err
	}
	inputSchema := []byte(`{}`)
	row := tx.QueryRowContext(ctx, `
INSERT INTO workflow_definitions (
    workflow_name, version, status, title, description, graph_json, input_schema_json, metadata_json, content_hash
) VALUES ($1, $2, 'draft', $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8)
RETURNING id, workflow_name, version, status, title, description, graph_json, input_schema_json, metadata_json,
          content_hash, created_at, activated_at, deprecated_at, retired_at`,
		def.Name,
		nextVersion,
		coalesceString(def.Title, def.Name),
		nullableString(def.Description),
		string(graphJSON),
		string(inputSchema),
		string(metadataJSON),
		hash,
	)
	record, err := scanDefinition(row)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("workflow: commit publish tx: %w", err)
	}
	return &PublishResult{Definition: record, Published: true}, nil
}

func (m *Module) Activate(ctx context.Context, workflowName string, version int) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("workflow: begin activate tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	target, err := getDefinitionByVersion(ctx, tx, workflowName, version)
	if err != nil {
		return err
	}
	nowStatus := DefinitionStatusActive
	// Promote the requested version and deprecate any previously active or draft
	// versions in a single statement so readers never observe two active
	// definitions for the same workflow name.
	if _, err := tx.ExecContext(ctx, `
UPDATE workflow_definitions
SET status = CASE WHEN id = $1 THEN 'active' ELSE 'deprecated' END,
    activated_at = CASE WHEN id = $1 THEN NOW() ELSE activated_at END,
    deprecated_at = CASE WHEN id <> $1 AND status = 'active' THEN NOW() ELSE deprecated_at END
WHERE workflow_name = $2 AND status IN ('active', 'draft', 'deprecated')`, target.ID, workflowName); err != nil {
		return fmt.Errorf("workflow: update definition statuses: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO workflow_definition_aliases (workflow_name, active_definition_id, updated_at)
VALUES ($1, $2, NOW())
ON CONFLICT (workflow_name) DO UPDATE SET active_definition_id = EXCLUDED.active_definition_id, updated_at = NOW()`,
		workflowName, target.ID,
	); err != nil {
		return fmt.Errorf("workflow: upsert definition alias: %w", err)
	}
	target.Status = nowStatus
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("workflow: commit activate tx: %w", err)
	}
	return nil
}

func (m *Module) GetActiveDefinition(ctx context.Context, workflowName string) (*DefinitionRecord, error) {
	row := m.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT d.%s
FROM workflow_definition_aliases a
JOIN workflow_definitions d ON d.id = a.active_definition_id
WHERE a.workflow_name = $1`, strings.ReplaceAll(definitionSelectColumns, ", ", ", d.")), workflowName)
	record, err := scanDefinition(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", ErrActiveDefinitionNotFound, workflowName)
		}
		return nil, fmt.Errorf("workflow: get active definition: %w", err)
	}
	return &record, nil
}

func (m *Module) ListDefinitions(ctx context.Context, workflowName string) ([]DefinitionRecord, error) {
	query := fmt.Sprintf(`
SELECT %s
FROM workflow_definitions`, definitionSelectColumns)
	args := []any{}
	if strings.TrimSpace(workflowName) != "" {
		query += ` WHERE workflow_name = $1`
		args = append(args, workflowName)
	}
	query += ` ORDER BY workflow_name ASC, version DESC`
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("workflow: list definitions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var defs []DefinitionRecord
	for rows.Next() {
		record, err := scanDefinition(rows)
		if err != nil {
			return nil, err
		}
		defs = append(defs, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workflow: iterate definitions: %w", err)
	}
	return defs, nil
}

func (m *Module) GetDefinitionVersion(ctx context.Context, workflowName string, version int) (*DefinitionRecord, error) {
	return getDefinitionByVersionDB(ctx, m.db, workflowName, version)
}

func (m *Module) ListRuns(ctx context.Context, params ListRunsParams) ([]RunRecord, error) {
	if m == nil || m.db == nil {
		return nil, ErrNilDB
	}
	return listRuns(ctx, m.db, params)
}

func (m *Module) CountRuns(ctx context.Context, params ListRunsParams) (int64, error) {
	if m == nil || m.db == nil {
		return 0, ErrNilDB
	}
	return countRuns(ctx, m.db, params)
}

// RetryRun starts a new run using the original run input and metadata.
// Only failed or cancelled runs are retryable.
func (m *Module) RetryRun(ctx context.Context, runID string) (*RunRecord, error) {
	if m == nil || m.db == nil {
		return nil, ErrNilDB
	}

	run, err := getRunByID(ctx, m.db, strings.TrimSpace(runID))
	if err != nil {
		return nil, err
	}
	if run.Status != RunStatusFailed && run.Status != RunStatusCancelled {
		return nil, fmt.Errorf("%w: %s status=%s", ErrRunNotRetryable, run.ID, run.Status)
	}

	var opts StartRunOptions
	if run.CreatedBy.Valid {
		opts.CreatedBy = run.CreatedBy.String
	}
	if run.CorrelationKey.Valid {
		opts.CorrelationKey = run.CorrelationKey.String
	}
	if len(run.ContextJSON) > 0 {
		opts.ContextJSON = append([]byte(nil), run.ContextJSON...)
	}

	return m.StartVersion(ctx, run.WorkflowName, run.WorkflowVersion, json.RawMessage(run.InputJSON), &opts)
}

// RetryStep replays the queue work for a failed or cancelled step inside an existing run.
func (m *Module) RetryStep(ctx context.Context, stepID int64) (*StepRecord, error) {
	if m == nil || m.db == nil {
		return nil, ErrNilDB
	}
	if m.queue == nil {
		return nil, ErrNilQueue
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("workflow: begin retry step tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	step, err := getStepByID(ctx, tx, stepID)
	if err != nil {
		return nil, err
	}
	run, err := getRunByID(ctx, tx, step.RunID)
	if err != nil {
		return nil, err
	}
	if step.Status != StepStatusFailed && step.Status != StepStatusCancelled {
		return nil, fmt.Errorf("%w: %d status=%s", ErrStepNotRetryable, step.ID, step.Status)
	}

	defRecord, err := getDefinitionByID(ctx, tx, run.WorkflowDefinitionID)
	if err != nil {
		return nil, err
	}
	def, err := m.resolveDefinitionByRecord(defRecord)
	if err != nil {
		return nil, err
	}
	spec, ok := def.Step(step.StepName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrStepNotFound, step.StepName)
	}

	if err := updateRunStatus(ctx, tx, run.ID, RunStatusRunning, nil); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE workflow_steps
SET status = 'pending',
    error_json = NULL,
    output_json = NULL,
    started_at = NULL,
    completed_at = NULL,
    available_at = NULL,
    updated_at = NOW()
WHERE id = $1`, step.ID); err != nil {
		return nil, fmt.Errorf("workflow: reset step for retry: %w", err)
	}

	if step.QueueJobID.Valid {
		if err := m.queue.ReplayJobTx(ctx, tx, step.QueueJobID.Int64); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE workflow_steps
SET status = 'queued', available_at = NOW(), updated_at = NOW()
WHERE id = $1`, step.ID); err != nil {
			return nil, fmt.Errorf("workflow: mark step queued on replay: %w", err)
		}
	} else {
		if err := enqueueStepWithItem(ctx, tx, m.queue, step.ID, step.RunID, step.StepName, step.ItemKey, spec.RetryPolicy.MaxAttempts); err != nil {
			return nil, err
		}
	}

	updated, err := getStepByID(ctx, tx, step.ID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("workflow: commit retry step tx: %w", err)
	}
	return updated, nil
}

func (m *Module) DiffDefinitionVersions(ctx context.Context, workflowName string, fromVersion, toVersion int) (*GraphDiff, error) {
	from, err := m.GetDefinitionVersion(ctx, workflowName, fromVersion)
	if err != nil {
		return nil, err
	}
	to, err := m.GetDefinitionVersion(ctx, workflowName, toVersion)
	if err != nil {
		return nil, err
	}
	var fromGraph Graph
	if err := json.Unmarshal(from.GraphJSON, &fromGraph); err != nil {
		return nil, fmt.Errorf("workflow: unmarshal from graph: %w", err)
	}
	var toGraph Graph
	if err := json.Unmarshal(to.GraphJSON, &toGraph); err != nil {
		return nil, fmt.Errorf("workflow: unmarshal to graph: %w", err)
	}
	return diffGraphs(fromGraph, toGraph), nil
}

func (m *Module) Definition(name string) (*Definition, bool) {
	return m.currentDefinition(name)
}

func encodeJSONOrEmpty(v any) ([]byte, error) {
	if v == nil {
		return []byte(`{}`), nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("workflow: marshal json payload: %w", err)
	}
	if len(data) == 0 {
		return []byte(`{}`), nil
	}
	return data, nil
}

func nullableString(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func coalesceString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
