package adminui

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nanostack-dev/pgkit/workflow"
)

const (
	maxDetailChildren   = 100
	maxDetailAncestors  = 32
	maxDetailSignals    = 50
	signalPreviewBytes  = 200
	signalPreviewFetch  = signalPreviewBytes + 1
	maxTreeDepth        = 8
	maxTreeNodes        = 300
	overviewWaitingRuns = 8
	overviewFailedRuns  = 5
	overviewRecentRuns  = 8
)

type stepRef struct {
	Name       string  `json:"name"`
	Kind       string  `json:"kind"`
	Status     string  `json:"status"`
	Attempts   int     `json:"attempts"`
	Signal     *string `json:"signal"`
	WakeAt     *string `json:"wake_at"`
	ChildRunID *string `json:"child_run_id"`
	Error      *string `json:"error"`
}

type runSummary struct {
	ID          string   `json:"id"`
	Workflow    string   `json:"workflow"`
	Version     int      `json:"version"`
	Key         *string  `json:"key"`
	Status      string   `json:"status"`
	Error       *string  `json:"error"`
	TimedOut    bool     `json:"timed_out"`
	ParentRunID *string  `json:"parent_run_id"`
	WakeAt      *string  `json:"wake_at"`
	DeadlineAt  *string  `json:"deadline_at"`
	CreatedAt   string   `json:"created_at"`
	StartedAt   *string  `json:"started_at"`
	CompletedAt *string  `json:"completed_at"`
	UpdatedAt   string   `json:"updated_at"`
	ChildCount  int      `json:"child_count"`
	StepCount   int      `json:"step_count"`
	CurrentStep *stepRef `json:"current_step"`
}

type workflowSummary struct {
	Workflow  string `json:"workflow"`
	Versions  []int  `json:"versions"`
	Total     int64  `json:"total"`
	Pending   int64  `json:"pending"`
	Running   int64  `json:"running"`
	Waiting   int64  `json:"waiting"`
	Succeeded int64  `json:"succeeded"`
	Failed    int64  `json:"failed"`
	Cancelled int64  `json:"cancelled"`
	LastRunAt string `json:"last_run_at"`
}

type runAncestor struct {
	ID       string  `json:"id"`
	Workflow string  `json:"workflow"`
	Status   string  `json:"status"`
	Key      *string `json:"key"`
}

type runSignal struct {
	ID             int64  `json:"id"`
	Name           string `json:"name"`
	Received       bool   `json:"received"`
	PayloadPreview string `json:"payload_preview"`
	CreatedAt      string `json:"created_at"`
}

type workflowRunDetail struct {
	Now        string         `json:"now"`
	Run        workflowRun    `json:"run"`
	JobID      *int64         `json:"job_id"`
	Steps      []workflowStep `json:"steps"`
	Children   []runSummary   `json:"children"`
	ChildTotal int64          `json:"child_total"`
	Ancestors  []runAncestor  `json:"ancestors"`
	Signals    []runSignal    `json:"signals"`
}

type treeNode struct {
	ID          string  `json:"id"`
	ParentRunID *string `json:"parent_run_id"`
	Workflow    string  `json:"workflow"`
	Version     int     `json:"version"`
	Key         *string `json:"key"`
	Status      string  `json:"status"`
	Depth       int     `json:"depth"`
	CreatedAt   string  `json:"created_at"`
	CompletedAt *string `json:"completed_at"`
	ChildCount  int     `json:"child_count"`
}

type runTree struct {
	RootID    string     `json:"root_id"`
	Nodes     []treeNode `json:"nodes"`
	Truncated bool       `json:"truncated"`
}

type workflowCounts struct {
	Pending   int64 `json:"pending"`
	Running   int64 `json:"running"`
	Waiting   int64 `json:"waiting"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Cancelled int64 `json:"cancelled"`
	Total     int64 `json:"total"`
}

type workflowBucket struct {
	Start     string `json:"start"`
	Succeeded int64  `json:"succeeded"`
	Failed    int64  `json:"failed"`
	Cancelled int64  `json:"cancelled"`
}

type workflowThroughput struct {
	BucketSeconds int              `json:"bucket_seconds"`
	Buckets       []workflowBucket `json:"buckets"`
}

type workflowOverview struct {
	Counts     workflowCounts     `json:"counts"`
	Throughput workflowThroughput `json:"throughput"`
	Waiting    []runSummary       `json:"waiting"`
	Failed     []runSummary       `json:"failed"`
	Recent     []runSummary       `json:"recent"`
}

type runFilter struct {
	workflow    string
	status      string
	parentRunID string
	search      string
	topLevel    bool
}

type runSelection struct {
	where  string
	args   []any
	order  string
	limit  int
	offset int
}

func (f runFilter) clause() (string, []any) {
	var conditions []string
	var args []any
	add := func(condition string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf(condition, len(args)))
	}
	if f.workflow != "" {
		add("workflow = $%d", f.workflow)
	}
	if f.status != "" {
		add("status = $%d", f.status)
	}
	if f.parentRunID != "" {
		add("parent_run_id = $%d", f.parentRunID)
	}
	if f.topLevel {
		conditions = append(conditions, "parent_run_id IS NULL")
	}
	if f.search != "" {
		add("(id ILIKE $%[1]d OR key ILIKE $%[1]d OR workflow ILIKE $%[1]d)", likePattern(f.search))
	}
	if len(conditions) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(conditions, " AND "), args
}

func (u *UI) handleWorkflowWorkflows(w http.ResponseWriter, r *http.Request) {
	items := []workflowSummary{}
	if u.workflow != nil {
		var err error
		items, err = queryWorkflowSummaries(r.Context(), u.queue.DB())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (u *UI) handleWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	page, err := u.pageRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	search, err := searchTerm(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if u.workflow == nil {
		writeJSON(w, http.StatusOK, listResponse[runSummary]{Items: []runSummary{}, Limit: page.limit, Offset: page.offset})
		return
	}
	filter := runFilter{
		workflow:    queryText(r, "workflow"),
		status:      queryText(r, "status"),
		parentRunID: queryText(r, "parent_run_id"),
		search:      search,
		topLevel:    parseBoolDefault(queryText(r, "top_level"), false),
	}
	response, err := queryRunList(r.Context(), u.queue.DB(), filter, page)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (u *UI) handleWorkflowRun(w http.ResponseWriter, r *http.Request) {
	if u.workflow == nil {
		writeError(w, http.StatusNotFound, errNoWorkflows)
		return
	}
	detail, err := u.buildWorkflowRunDetail(r.Context(), strings.TrimSpace(r.PathValue("id")))
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (u *UI) handleWorkflowRunTree(w http.ResponseWriter, r *http.Request) {
	if u.workflow == nil {
		writeError(w, http.StatusNotFound, errNoWorkflows)
		return
	}
	tree, err := queryRunTree(r.Context(), u.queue.DB(), strings.TrimSpace(r.PathValue("id")))
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tree)
}

func (u *UI) buildWorkflowRunDetail(ctx context.Context, runID string) (workflowRunDetail, error) {
	run, err := u.workflow.GetRun(ctx, runID)
	if err != nil {
		return workflowRunDetail{}, err
	}
	steps, err := u.workflow.ListSteps(ctx, runID)
	if err != nil {
		return workflowRunDetail{}, err
	}
	detail := workflowRunDetail{Run: toWorkflowRun(run), Steps: make([]workflowStep, 0, len(steps))}
	for _, step := range steps {
		detail.Steps = append(detail.Steps, toWorkflowStep(step))
	}
	err = u.withSnapshot(ctx, func(q querier) error {
		return fillRunDetail(ctx, q, runID, &detail)
	})
	if err != nil {
		return workflowRunDetail{}, err
	}
	return detail, nil
}

func fillRunDetail(ctx context.Context, q querier, runID string, detail *workflowRunDetail) error {
	var err error
	if detail.Now, err = queryNow(ctx, q); err != nil {
		return err
	}
	if detail.JobID, err = queryRunJobID(ctx, q, runID); err != nil {
		return err
	}
	if detail.ChildTotal, err = queryChildTotal(ctx, q, runID); err != nil {
		return err
	}
	detail.Children, err = queryRunSummaries(ctx, q, runSelection{
		where:  " WHERE parent_run_id = $1",
		args:   []any{runID},
		order:  "created_at ASC, id ASC",
		limit:  maxDetailChildren,
		offset: 0,
	})
	if err != nil {
		return err
	}
	if detail.Ancestors, err = queryRunAncestors(ctx, q, runID); err != nil {
		return err
	}
	detail.Signals, err = queryRunSignals(ctx, q, runID)
	return err
}

func queryRunJobID(ctx context.Context, q querier, runID string) (*int64, error) {
	var jobID sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT job_id FROM pgworkflow_runs WHERE id = $1`, runID).Scan(&jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", workflow.ErrRunNotFound, runID)
	}
	if err != nil {
		return nil, fmt.Errorf("adminui: run job id: %w", err)
	}
	if !jobID.Valid {
		return nil, nil
	}
	return &jobID.Int64, nil
}

func queryChildTotal(ctx context.Context, q querier, runID string) (int64, error) {
	var total int64
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM pgworkflow_runs WHERE parent_run_id = $1`, runID).Scan(&total); err != nil {
		return 0, fmt.Errorf("adminui: child total: %w", err)
	}
	return total, nil
}

func queryRunAncestors(ctx context.Context, q querier, runID string) ([]runAncestor, error) {
	rows, err := q.QueryContext(ctx, `
WITH RECURSIVE chain AS (
    SELECT parent_run_id AS id, 1 AS distance
    FROM pgworkflow_runs
    WHERE id = $1 AND parent_run_id IS NOT NULL
    UNION ALL
    SELECT r.parent_run_id, chain.distance + 1
    FROM chain
    JOIN pgworkflow_runs r ON r.id = chain.id
    WHERE r.parent_run_id IS NOT NULL AND chain.distance < $2::int
)
SELECT a.id, a.workflow, a.status, a.key
FROM chain
JOIN pgworkflow_runs a ON a.id = chain.id
ORDER BY chain.distance DESC`, runID, maxDetailAncestors)
	if err != nil {
		return nil, fmt.Errorf("adminui: run ancestors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	ancestors := []runAncestor{}
	for rows.Next() {
		var ancestor runAncestor
		var key sql.NullString
		if err := rows.Scan(&ancestor.ID, &ancestor.Workflow, &ancestor.Status, &key); err != nil {
			return nil, fmt.Errorf("adminui: scan run ancestor: %w", err)
		}
		ancestor.Key = nullableText(key)
		ancestors = append(ancestors, ancestor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("adminui: iterate run ancestors: %w", err)
	}
	return ancestors, nil
}

func queryRunSignals(ctx context.Context, q querier, runID string) ([]runSignal, error) {
	rows, err := q.QueryContext(ctx, `
SELECT id, name, received_by IS NOT NULL, left(payload::text, $2::int), created_at
FROM pgworkflow_signals
WHERE run_id = $1
ORDER BY id DESC
LIMIT $3`, runID, signalPreviewFetch, maxDetailSignals)
	if err != nil {
		return nil, fmt.Errorf("adminui: run signals: %w", err)
	}
	defer func() { _ = rows.Close() }()
	signals := []runSignal{}
	for rows.Next() {
		var signal runSignal
		var head string
		var createdAt time.Time
		if err := rows.Scan(&signal.ID, &signal.Name, &signal.Received, &head, &createdAt); err != nil {
			return nil, fmt.Errorf("adminui: scan run signal: %w", err)
		}
		signal.PayloadPreview = truncateText(head, signalPreviewBytes)
		signal.CreatedAt = formatTime(createdAt)
		signals = append(signals, signal)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("adminui: iterate run signals: %w", err)
	}
	return signals, nil
}

func queryRunList(ctx context.Context, q querier, filter runFilter, page pageRequest) (listResponse[runSummary], error) {
	where, args := filter.clause()
	var total int64
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM pgworkflow_runs`+where, args...).Scan(&total); err != nil {
		return listResponse[runSummary]{}, fmt.Errorf("adminui: count runs: %w", err)
	}
	items, err := queryRunSummaries(ctx, q, runSelection{where: where, args: args, order: "created_at DESC, id DESC", limit: page.limit, offset: page.offset})
	if err != nil {
		return listResponse[runSummary]{}, err
	}
	return listResponse[runSummary]{Items: items, Total: total, Limit: page.limit, Offset: page.offset}, nil
}

func queryRunSummaries(ctx context.Context, q querier, selection runSelection) ([]runSummary, error) {
	args := append(slices.Clone(selection.args), selection.limit, selection.offset)
	query := fmt.Sprintf(`
SELECT r.id, r.workflow, r.version, r.key, r.status, r.error, r.timed_out, r.parent_run_id, r.wake_at, r.deadline_at,
       r.created_at, r.started_at, r.completed_at, r.updated_at,
       (SELECT count(*) FROM pgworkflow_runs c WHERE c.parent_run_id = r.id),
       (SELECT count(*) FROM pgworkflow_steps s WHERE s.run_id = r.id),
       cur.name AS step_name, cur.kind AS step_kind, cur.status AS step_status, cur.attempts AS step_attempts,
       cur.signal AS step_signal, cur.wake_at AS step_wake_at, cur.child_run_id AS step_child_run_id,
       cur.error AS step_error
FROM (
    SELECT id, workflow, version, key, status, error, timed_out, parent_run_id, wake_at, deadline_at,
           created_at, started_at, completed_at, updated_at
    FROM pgworkflow_runs%s
    ORDER BY %s
    LIMIT $%d OFFSET $%d
) r
LEFT JOIN LATERAL (
    SELECT s.name, s.kind, s.status, s.attempts, s.signal, s.wake_at, s.child_run_id, s.error
    FROM pgworkflow_steps s
    WHERE s.run_id = r.id AND s.status <> 'succeeded'
    ORDER BY s.created_at DESC, s.name DESC
    LIMIT 1
) cur ON TRUE
ORDER BY %s`, selection.where, selection.order, len(args)-1, len(args), selection.order)
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("adminui: run summaries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	runs := []runSummary{}
	for rows.Next() {
		run, err := scanRunSummary(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("adminui: iterate run summaries: %w", err)
	}
	return runs, nil
}

func scanRunSummary(rows *sql.Rows) (runSummary, error) {
	var (
		run                                        runSummary
		key, runError, parentRunID                 sql.NullString
		wakeAt, deadlineAt, startedAt, completedAt sql.NullTime
		createdAt, updatedAt                       time.Time
		stepName, stepKind, stepStatus             sql.NullString
		stepAttempts                               sql.NullInt64
		stepSignal, stepChildRunID, stepError      sql.NullString
		stepWakeAt                                 sql.NullTime
	)
	err := rows.Scan(&run.ID, &run.Workflow, &run.Version, &key, &run.Status, &runError, &run.TimedOut, &parentRunID,
		&wakeAt, &deadlineAt, &createdAt, &startedAt, &completedAt, &updatedAt, &run.ChildCount, &run.StepCount,
		&stepName, &stepKind, &stepStatus, &stepAttempts, &stepSignal, &stepWakeAt, &stepChildRunID, &stepError)
	if err != nil {
		return runSummary{}, fmt.Errorf("adminui: scan run summary: %w", err)
	}
	run.Key = nullableText(key)
	run.Error = nullableText(runError)
	run.ParentRunID = nullableText(parentRunID)
	run.WakeAt = formatNullTime(wakeAt)
	run.DeadlineAt = formatNullTime(deadlineAt)
	run.CreatedAt = formatTime(createdAt)
	run.StartedAt = formatNullTime(startedAt)
	run.CompletedAt = formatNullTime(completedAt)
	run.UpdatedAt = formatTime(updatedAt)
	if stepName.Valid {
		run.CurrentStep = &stepRef{
			Name:       stepName.String,
			Kind:       stepKind.String,
			Status:     stepStatus.String,
			Attempts:   int(stepAttempts.Int64),
			Signal:     nullableText(stepSignal),
			WakeAt:     formatNullTime(stepWakeAt),
			ChildRunID: nullableText(stepChildRunID),
			Error:      nullableText(stepError),
		}
	}
	return run, nil
}

func queryWorkflowSummaries(ctx context.Context, q querier) ([]workflowSummary, error) {
	rows, err := q.QueryContext(ctx, `
SELECT workflow,
       jsonb_agg(DISTINCT version ORDER BY version),
       count(*),
       count(*) FILTER (WHERE status = 'pending'),
       count(*) FILTER (WHERE status = 'running'),
       count(*) FILTER (WHERE status = 'waiting'),
       count(*) FILTER (WHERE status = 'succeeded'),
       count(*) FILTER (WHERE status = 'failed'),
       count(*) FILTER (WHERE status = 'cancelled'),
       max(created_at)
FROM pgworkflow_runs
GROUP BY workflow
ORDER BY workflow`)
	if err != nil {
		return nil, fmt.Errorf("adminui: workflow summaries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	items := []workflowSummary{}
	for rows.Next() {
		var item workflowSummary
		var versions []byte
		var lastRunAt time.Time
		if err := rows.Scan(&item.Workflow, &versions, &item.Total, &item.Pending, &item.Running, &item.Waiting,
			&item.Succeeded, &item.Failed, &item.Cancelled, &lastRunAt); err != nil {
			return nil, fmt.Errorf("adminui: scan workflow summary: %w", err)
		}
		if err := json.Unmarshal(versions, &item.Versions); err != nil {
			return nil, fmt.Errorf("adminui: decode workflow versions: %w", err)
		}
		item.LastRunAt = formatTime(lastRunAt)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("adminui: iterate workflow summaries: %w", err)
	}
	return items, nil
}

func queryRunTree(ctx context.Context, q querier, runID string) (runTree, error) {
	// Each path element is a fixed-width creation timestamp followed by the run id, so
	// ordering by the path array yields depth-first order with siblings by creation time.
	rows, err := q.QueryContext(ctx, `
WITH RECURSIVE tree AS (
    SELECT r.id, r.parent_run_id, r.workflow, r.version, r.key, r.status, r.created_at, r.completed_at,
           0 AS depth,
           ARRAY[to_char(r.created_at AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISSUS') || r.id] AS path
    FROM pgworkflow_runs r
    WHERE r.id = $1
    UNION ALL
    SELECT c.id, c.parent_run_id, c.workflow, c.version, c.key, c.status, c.created_at, c.completed_at,
           tree.depth + 1,
           tree.path || (to_char(c.created_at AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISSUS') || c.id)
    FROM tree
    JOIN pgworkflow_runs c ON c.parent_run_id = tree.id
    WHERE tree.depth < $2::int
)
SELECT page.id, page.parent_run_id, page.workflow, page.version, page.key, page.status, page.depth,
       page.created_at, page.completed_at,
       (SELECT count(*) FROM pgworkflow_runs k WHERE k.parent_run_id = page.id)
FROM (SELECT * FROM tree ORDER BY path LIMIT $3) page
ORDER BY page.path`, runID, maxTreeDepth, maxTreeNodes+1)
	if err != nil {
		return runTree{}, fmt.Errorf("adminui: run tree: %w", err)
	}
	defer func() { _ = rows.Close() }()
	nodes := []treeNode{}
	for rows.Next() {
		var (
			node             treeNode
			parentRunID, key sql.NullString
			createdAt        time.Time
			completedAt      sql.NullTime
		)
		if err := rows.Scan(&node.ID, &parentRunID, &node.Workflow, &node.Version, &key, &node.Status, &node.Depth,
			&createdAt, &completedAt, &node.ChildCount); err != nil {
			return runTree{}, fmt.Errorf("adminui: scan run tree: %w", err)
		}
		node.ParentRunID = nullableText(parentRunID)
		node.Key = nullableText(key)
		node.CreatedAt = formatTime(createdAt)
		node.CompletedAt = formatNullTime(completedAt)
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return runTree{}, fmt.Errorf("adminui: iterate run tree: %w", err)
	}
	if len(nodes) == 0 {
		return runTree{}, fmt.Errorf("%w: %s", workflow.ErrRunNotFound, runID)
	}
	truncated := len(nodes) > maxTreeNodes
	nodes = nodes[:min(len(nodes), maxTreeNodes)]
	for _, node := range nodes {
		if node.Depth == maxTreeDepth && node.ChildCount > 0 {
			truncated = true
		}
	}
	return runTree{RootID: nodes[0].ID, Nodes: nodes, Truncated: truncated}, nil
}

const workflowThroughputSQL = throughputSeriesSQL + `,
finished AS (
    SELECT floor(extract(epoch FROM r.completed_at) / settings.size)::bigint AS idx,
           count(*) FILTER (WHERE r.status = 'succeeded') AS succeeded,
           count(*) FILTER (WHERE r.status = 'failed') AS failed,
           count(*) FILTER (WHERE r.status = 'cancelled') AS cancelled
    FROM pgworkflow_runs r, settings, window_start
    WHERE r.status IN ('succeeded', 'failed', 'cancelled') AND r.completed_at >= window_start.starts_at
    GROUP BY 1
)
SELECT to_timestamp(series.idx * settings.size),
       COALESCE(finished.succeeded, 0), COALESCE(finished.failed, 0), COALESCE(finished.cancelled, 0)
FROM series
CROSS JOIN settings
LEFT JOIN finished ON finished.idx = series.idx
ORDER BY series.idx`

func queryWorkflowOverview(ctx context.Context, q querier, window throughputWindow) (*workflowOverview, error) {
	overview := &workflowOverview{}
	var err error
	if overview.Counts, err = queryWorkflowCounts(ctx, q); err != nil {
		return nil, err
	}
	if overview.Throughput, err = queryWorkflowThroughput(ctx, q, window); err != nil {
		return nil, err
	}
	overview.Waiting, err = queryRunSummaries(ctx, q, runSelection{
		where: " WHERE status = 'waiting'", order: "wake_at ASC NULLS LAST, created_at ASC", limit: overviewWaitingRuns,
	})
	if err != nil {
		return nil, err
	}
	overview.Failed, err = queryRunSummaries(ctx, q, runSelection{
		where: " WHERE status = 'failed'", order: "completed_at DESC NULLS LAST, id DESC", limit: overviewFailedRuns,
	})
	if err != nil {
		return nil, err
	}
	overview.Recent, err = queryRunSummaries(ctx, q, runSelection{order: "updated_at DESC, id DESC", limit: overviewRecentRuns})
	if err != nil {
		return nil, err
	}
	return overview, nil
}

func queryWorkflowCounts(ctx context.Context, q querier) (workflowCounts, error) {
	var counts workflowCounts
	err := q.QueryRowContext(ctx, `
SELECT count(*) FILTER (WHERE status = 'pending'),
       count(*) FILTER (WHERE status = 'running'),
       count(*) FILTER (WHERE status = 'waiting'),
       count(*) FILTER (WHERE status = 'succeeded'),
       count(*) FILTER (WHERE status = 'failed'),
       count(*) FILTER (WHERE status = 'cancelled'),
       count(*)
FROM pgworkflow_runs`).Scan(&counts.Pending, &counts.Running, &counts.Waiting, &counts.Succeeded, &counts.Failed,
		&counts.Cancelled, &counts.Total)
	if err != nil {
		return workflowCounts{}, fmt.Errorf("adminui: workflow counts: %w", err)
	}
	return counts, nil
}

func queryWorkflowThroughput(ctx context.Context, q querier, window throughputWindow) (workflowThroughput, error) {
	rows, err := q.QueryContext(ctx, workflowThroughputSQL, window.bucketSeconds, window.bucketCount)
	if err != nil {
		return workflowThroughput{}, fmt.Errorf("adminui: workflow throughput: %w", err)
	}
	defer func() { _ = rows.Close() }()
	throughput := workflowThroughput{BucketSeconds: window.bucketSeconds, Buckets: make([]workflowBucket, 0, window.bucketCount)}
	for rows.Next() {
		var start time.Time
		var bucket workflowBucket
		if err := rows.Scan(&start, &bucket.Succeeded, &bucket.Failed, &bucket.Cancelled); err != nil {
			return workflowThroughput{}, fmt.Errorf("adminui: scan workflow throughput: %w", err)
		}
		bucket.Start = formatTime(start)
		throughput.Buckets = append(throughput.Buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return workflowThroughput{}, fmt.Errorf("adminui: iterate workflow throughput: %w", err)
	}
	return throughput, nil
}
