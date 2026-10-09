package adminui

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	qpkg "github.com/nanostack-dev/pgkit/queue"
)

const (
	overviewFailedJobs = 5
	overviewRecentJobs = 8
)

type queueCounts struct {
	summary     queueSummary
	dueJobs     int64
	oldestDueAt sql.NullTime
}

type queueBucket struct {
	Start  string `json:"start"`
	Done   int64  `json:"done"`
	Failed int64  `json:"failed"`
}

type queueThroughput struct {
	BucketSeconds int           `json:"bucket_seconds"`
	Buckets       []queueBucket `json:"buckets"`
}

type queueOverview struct {
	Summary     queueSummary    `json:"summary"`
	DueJobs     int64           `json:"due_jobs"`
	OldestDueAt *string         `json:"oldest_due_at"`
	Throughput  queueThroughput `json:"throughput"`
	FailedJobs  []queueJob      `json:"failed_jobs"`
	RecentJobs  []queueJob      `json:"recent_jobs"`
}

type queueBreakdown struct {
	QueueName      string  `json:"queue_name"`
	Total          int64   `json:"total"`
	Pending        int64   `json:"pending"`
	Due            int64   `json:"due"`
	Processing     int64   `json:"processing"`
	Done           int64   `json:"done"`
	Failed         int64   `json:"failed"`
	OldestDueAt    *string `json:"oldest_due_at"`
	LastActivityAt string  `json:"last_activity_at"`
}

type queueBreakdownResponse struct {
	Now   string           `json:"now"`
	Items []queueBreakdown `json:"items"`
}

type queueJobDetail struct {
	queueJob
	Payload          string  `json:"payload"`
	PayloadEncoding  string  `json:"payload_encoding"`
	PayloadBytes     int     `json:"payload_bytes"`
	PayloadTruncated bool    `json:"payload_truncated"`
	RunID            *string `json:"run_id"`
}

func (u *UI) handleQueueQueues(w http.ResponseWriter, r *http.Request) {
	var response queueBreakdownResponse
	err := u.withSnapshot(r.Context(), func(q querier) error {
		now, err := queryNow(r.Context(), q)
		if err != nil {
			return err
		}
		items, err := queryQueueBreakdown(r.Context(), q)
		if err != nil {
			return err
		}
		response = queueBreakdownResponse{Now: now, Items: items}
		return nil
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (u *UI) handleQueueJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathID(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusBadRequest, errInvalidJobID)
		return
	}
	row, err := queryQueueJob(r.Context(), u.queue.DB(), id)
	if errors.Is(err, qpkg.ErrJobNotFound) {
		writeError(w, http.StatusNotFound, errJobNotFound)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, row.detail())
}

type jobRow struct {
	job  qpkg.Job
	head []byte
	size int
}

func (row jobRow) summary() queueJob {
	return newQueueJob(row.job, previewPayload(row.head[:min(len(row.head), previewFetchBytes)], row.size))
}

func (row jobRow) detail() queueJobDetail {
	encoded := encodePayload(row.head, row.size)
	return queueJobDetail{
		queueJob:         row.summary(),
		Payload:          encoded.text,
		PayloadEncoding:  encoded.encoding,
		PayloadBytes:     encoded.size,
		PayloadTruncated: encoded.truncated,
		RunID:            workflowRunIDOfJob(row.job.QueueName, row.head),
	}
}

const queueJobSelect = `
SELECT id, queue_name, status, attempts, max_attempts, available_at, claimed_by, claimed_at, done_at, last_error,
       substring(payload FROM 1 FOR $1), octet_length(payload), created_at, updated_at
FROM pgqueue_jobs`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJobRow(row rowScanner) (jobRow, error) {
	var result jobRow
	job := &result.job
	err := row.Scan(&job.ID, &job.QueueName, &job.Status, &job.Attempts, &job.MaxAttempts, &job.AvailableAt,
		&job.ClaimedBy, &job.ClaimedAt, &job.DoneAt, &job.LastError, &result.head, &result.size, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		return jobRow{}, err
	}
	return result, nil
}

func queryQueueJob(ctx context.Context, q querier, id int64) (jobRow, error) {
	row, err := scanJobRow(q.QueryRowContext(ctx, queueJobSelect+` WHERE id = $2`, maxPayloadBytes, id))
	if errors.Is(err, sql.ErrNoRows) {
		return jobRow{}, qpkg.ErrJobNotFound
	}
	if err != nil {
		return jobRow{}, fmt.Errorf("adminui: queue job: %w", err)
	}
	return row, nil
}

func (u *UI) buildQueueSummary(ctx context.Context) (queueSummary, error) {
	counts, err := queryQueueCounts(ctx, u.queue.DB())
	if err != nil {
		return queueSummary{}, err
	}
	return counts.summary, nil
}

func queryQueueCounts(ctx context.Context, q querier) (queueCounts, error) {
	var counts queueCounts
	summary := &counts.summary
	err := q.QueryRowContext(ctx, `
SELECT count(*),
       count(*) FILTER (WHERE status = 'pending'),
       count(*) FILTER (WHERE status = 'processing'),
       count(*) FILTER (WHERE status = 'done'),
       count(*) FILTER (WHERE status = 'failed'),
       count(DISTINCT queue_name),
       count(*) FILTER (WHERE status = 'pending' AND available_at <= NOW()),
       min(available_at) FILTER (WHERE status = 'pending' AND available_at <= NOW())
FROM pgqueue_jobs`).Scan(&summary.TotalJobs, &summary.PendingJobs, &summary.ProcessingJobs, &summary.DoneJobs,
		&summary.FailedJobs, &summary.Queues, &counts.dueJobs, &counts.oldestDueAt)
	if err != nil {
		return queueCounts{}, fmt.Errorf("adminui: queue counts: %w", err)
	}
	locks, err := queryAdvisoryLockCount(ctx, q)
	if err != nil {
		return queueCounts{}, err
	}
	summary.AdvisoryLocks = locks
	return counts, nil
}

const throughputSeriesSQL = `
WITH settings AS (
    SELECT $1::bigint AS size,
           $2::int AS buckets,
           floor(extract(epoch FROM NOW()) / $1::bigint)::bigint AS last_index
), window_start AS (
    SELECT to_timestamp((last_index - buckets + 1) * size) AS starts_at FROM settings
), series AS (
    SELECT settings.last_index - steps.n AS idx
    FROM settings, generate_series(settings.buckets - 1, 0, -1) AS steps(n)
)`

const queueThroughputSQL = throughputSeriesSQL + `,
done AS (
    SELECT floor(extract(epoch FROM j.done_at) / settings.size)::bigint AS idx, count(*) AS n
    FROM pgqueue_jobs j, settings, window_start
    WHERE j.status = 'done' AND j.done_at >= window_start.starts_at
    GROUP BY 1
), failed AS (
    SELECT floor(extract(epoch FROM j.updated_at) / settings.size)::bigint AS idx, count(*) AS n
    FROM pgqueue_jobs j, settings, window_start
    WHERE j.status = 'failed' AND j.updated_at >= window_start.starts_at
    GROUP BY 1
)
SELECT to_timestamp(series.idx * settings.size), COALESCE(done.n, 0), COALESCE(failed.n, 0)
FROM series
CROSS JOIN settings
LEFT JOIN done ON done.idx = series.idx
LEFT JOIN failed ON failed.idx = series.idx
ORDER BY series.idx`

func queryQueueThroughput(ctx context.Context, q querier, window throughputWindow) (queueThroughput, error) {
	rows, err := q.QueryContext(ctx, queueThroughputSQL, window.bucketSeconds, window.bucketCount)
	if err != nil {
		return queueThroughput{}, fmt.Errorf("adminui: queue throughput: %w", err)
	}
	defer func() { _ = rows.Close() }()
	throughput := queueThroughput{BucketSeconds: window.bucketSeconds, Buckets: make([]queueBucket, 0, window.bucketCount)}
	for rows.Next() {
		var start time.Time
		var bucket queueBucket
		if err := rows.Scan(&start, &bucket.Done, &bucket.Failed); err != nil {
			return queueThroughput{}, fmt.Errorf("adminui: scan queue throughput: %w", err)
		}
		bucket.Start = formatTime(start)
		throughput.Buckets = append(throughput.Buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return queueThroughput{}, fmt.Errorf("adminui: iterate queue throughput: %w", err)
	}
	return throughput, nil
}

func queryQueueJobPreviews(ctx context.Context, q querier, status qpkg.JobStatus, limit int) ([]queueJob, error) {
	rows, err := q.QueryContext(ctx, queueJobSelect+`
WHERE ($2::text = '' OR status = $2::text)
ORDER BY updated_at DESC, id DESC
LIMIT $3`, previewFetchBytes, string(status), limit)
	if err != nil {
		return nil, fmt.Errorf("adminui: queue job previews: %w", err)
	}
	defer func() { _ = rows.Close() }()
	jobs := make([]queueJob, 0, limit)
	for rows.Next() {
		row, err := scanJobRow(rows)
		if err != nil {
			return nil, fmt.Errorf("adminui: scan queue job preview: %w", err)
		}
		jobs = append(jobs, row.summary())
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("adminui: iterate queue job previews: %w", err)
	}
	return jobs, nil
}

func queryQueueBreakdown(ctx context.Context, q querier) ([]queueBreakdown, error) {
	rows, err := q.QueryContext(ctx, `
SELECT queue_name,
       count(*),
       count(*) FILTER (WHERE status = 'pending'),
       count(*) FILTER (WHERE status = 'pending' AND available_at <= NOW()),
       count(*) FILTER (WHERE status = 'processing'),
       count(*) FILTER (WHERE status = 'done'),
       count(*) FILTER (WHERE status = 'failed'),
       min(available_at) FILTER (WHERE status = 'pending' AND available_at <= NOW()),
       max(updated_at)
FROM pgqueue_jobs
GROUP BY queue_name
ORDER BY queue_name`)
	if err != nil {
		return nil, fmt.Errorf("adminui: queue breakdown: %w", err)
	}
	defer func() { _ = rows.Close() }()
	items := []queueBreakdown{}
	for rows.Next() {
		var (
			item         queueBreakdown
			oldestDueAt  sql.NullTime
			lastActivity time.Time
		)
		if err := rows.Scan(&item.QueueName, &item.Total, &item.Pending, &item.Due, &item.Processing, &item.Done,
			&item.Failed, &oldestDueAt, &lastActivity); err != nil {
			return nil, fmt.Errorf("adminui: scan queue breakdown: %w", err)
		}
		item.OldestDueAt = formatNullTime(oldestDueAt)
		item.LastActivityAt = formatTime(lastActivity)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("adminui: iterate queue breakdown: %w", err)
	}
	return items, nil
}
