package adminui

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
)

func enqueueJob(t *testing.T, ctx context.Context, queue *qpkg.Client, name string, payload []byte, availableAt *time.Time) int64 {
	t.Helper()
	id, err := queue.Enqueue(ctx, qpkg.EnqueueParams{QueueName: name, Payload: payload, MaxAttempts: 3, AvailableAt: availableAt})
	if err != nil {
		t.Fatalf("enqueue %s: %v", name, err)
	}
	return id
}

func claimJob(t *testing.T, ctx context.Context, queue *qpkg.Client, name string) *qpkg.Job {
	t.Helper()
	job, found, err := queue.Claim(ctx, name, "adminui-test")
	if err != nil || !found {
		t.Fatalf("claim %s: found=%v err=%v", name, found, err)
	}
	return job
}

func TestQueueOverviewAndBreakdown(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, _ := newClients(t, ctx, db)

	doneID := enqueueJob(t, ctx, queue, "overview.done", []byte(`{"n":1}`), nil)
	claimed := claimJob(t, ctx, queue, "overview.done")
	if claimed.ID != doneID {
		t.Fatalf("claimed %d, want %d", claimed.ID, doneID)
	}
	if err := queue.Ack(ctx, doneID); err != nil {
		t.Fatal(err)
	}
	failedID := enqueueJob(t, ctx, queue, "overview.failed", []byte(`{"n":2}`), nil)
	claimJob(t, ctx, queue, "overview.failed")
	if err := queue.Fail(ctx, failedID, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	dueID := enqueueJob(t, ctx, queue, "overview.pending", []byte(`{"n":3}`), nil)
	later := time.Now().Add(time.Hour)
	enqueueJob(t, ctx, queue, "overview.later", []byte(`{"n":4}`), &later)
	enqueueJob(t, ctx, queue, "overview.busy", []byte(`{"n":5}`), nil)
	claimJob(t, ctx, queue, "overview.busy")
	server := newServer(t, queue, Options{Token: "secret"})

	t.Run("overview counts and lists", func(t *testing.T) {
		var overview overviewResponse
		getJSON(t, server.URL+"/api/dashboard/overview", http.StatusOK, &overview)
		summary := overview.Queue.Summary
		if summary.TotalJobs != 5 || summary.PendingJobs != 2 || summary.ProcessingJobs != 1 || summary.DoneJobs != 1 || summary.FailedJobs != 1 || summary.Queues != 5 {
			t.Fatalf("summary = %+v", summary)
		}
		if overview.Queue.DueJobs != 1 || overview.Queue.OldestDueAt == nil {
			t.Fatalf("due = %d, oldest = %v", overview.Queue.DueJobs, overview.Queue.OldestDueAt)
		}
		due, err := queue.GetJob(ctx, dueID)
		if err != nil {
			t.Fatal(err)
		}
		if *overview.Queue.OldestDueAt != formatTime(due.AvailableAt) {
			t.Fatalf("oldest_due_at = %s, want %s", *overview.Queue.OldestDueAt, formatTime(due.AvailableAt))
		}
		if len(overview.Queue.FailedJobs) != 1 || overview.Queue.FailedJobs[0].ID != failedID || overview.Queue.FailedJobs[0].LastError == nil || !strings.Contains(*overview.Queue.FailedJobs[0].LastError, "boom") {
			t.Fatalf("failed_jobs = %+v", overview.Queue.FailedJobs)
		}
		if overview.Queue.FailedJobs[0].PayloadPreview != `{"n":2}` {
			t.Fatalf("failed job preview = %q", overview.Queue.FailedJobs[0].PayloadPreview)
		}
		if len(overview.Queue.RecentJobs) != 5 {
			t.Fatalf("recent_jobs = %d, want 5", len(overview.Queue.RecentJobs))
		}
		if overview.Workflow != nil {
			t.Fatalf("workflow = %+v, want null without workflows", overview.Workflow)
		}
		requireBucketLayout(t, queueBucketStarts(overview.Queue.Throughput), overview.Now, 60, 60)
		if overview.Queue.Throughput.BucketSeconds != 60 {
			t.Fatalf("bucket_seconds = %d", overview.Queue.Throughput.BucketSeconds)
		}
	})

	t.Run("overview json shape", func(t *testing.T) {
		raw := getMap(t, server.URL+"/api/dashboard/overview", http.StatusOK)
		requireKeys(t, "overview", raw, "now", "queue", "workflow")
		if value, present := raw["workflow"]; !present || value != nil {
			t.Fatalf("workflow = %v (present=%v), want explicit null", value, present)
		}
		queueSection := raw["queue"].(map[string]any)
		requireKeys(t, "queue", queueSection, "summary", "due_jobs", "oldest_due_at", "throughput", "failed_jobs", "recent_jobs")
		requireKeys(t, "summary", queueSection["summary"].(map[string]any), "total_jobs", "pending_jobs", "processing_jobs", "done_jobs", "failed_jobs", "advisory_locks", "queues")
		throughput := queueSection["throughput"].(map[string]any)
		requireKeys(t, "throughput", throughput, "bucket_seconds", "buckets")
		requireKeys(t, "bucket", throughput["buckets"].([]any)[0].(map[string]any), "start", "done", "failed")
	})

	t.Run("overview 24h range", func(t *testing.T) {
		var overview overviewResponse
		getJSON(t, server.URL+"/api/dashboard/overview?range=24h", http.StatusOK, &overview)
		if overview.Queue.Throughput.BucketSeconds != 1800 {
			t.Fatalf("bucket_seconds = %d", overview.Queue.Throughput.BucketSeconds)
		}
		requireBucketLayout(t, queueBucketStarts(overview.Queue.Throughput), overview.Now, 1800, 48)
	})

	t.Run("queue breakdown", func(t *testing.T) {
		var response queueBreakdownResponse
		getJSON(t, server.URL+"/api/dashboard/queue/queues", http.StatusOK, &response)
		if _, err := time.Parse(time.RFC3339Nano, response.Now); err != nil {
			t.Fatalf("now = %q: %v", response.Now, err)
		}
		names := make([]string, 0, len(response.Items))
		byName := map[string]queueBreakdown{}
		for _, item := range response.Items {
			names = append(names, item.QueueName)
			byName[item.QueueName] = item
		}
		wantNames := []string{"overview.busy", "overview.done", "overview.failed", "overview.later", "overview.pending"}
		if strings.Join(names, ",") != strings.Join(wantNames, ",") {
			t.Fatalf("queues = %v, want %v", names, wantNames)
		}
		pending := byName["overview.pending"]
		if pending.Total != 1 || pending.Pending != 1 || pending.Due != 1 || pending.OldestDueAt == nil {
			t.Fatalf("pending queue = %+v", pending)
		}
		deferred := byName["overview.later"]
		if deferred.Total != 1 || deferred.Pending != 1 || deferred.Due != 0 || deferred.OldestDueAt != nil {
			t.Fatalf("later queue = %+v", deferred)
		}
		if busy := byName["overview.busy"]; busy.Processing != 1 || busy.Total != 1 || busy.Pending != 0 {
			t.Fatalf("busy queue = %+v", busy)
		}
		if done := byName["overview.done"]; done.Done != 1 || done.Total != 1 {
			t.Fatalf("done queue = %+v", done)
		}
		if failed := byName["overview.failed"]; failed.Failed != 1 || failed.Total != 1 {
			t.Fatalf("failed queue = %+v", failed)
		}
		for _, item := range response.Items {
			if _, err := time.Parse(time.RFC3339, item.LastActivityAt); err != nil {
				t.Fatalf("last_activity_at = %q: %v", item.LastActivityAt, err)
			}
		}
		raw := getMap(t, server.URL+"/api/dashboard/queue/queues", http.StatusOK)
		requireKeys(t, "queues response", raw, "now", "items")
		requireKeys(t, "queue item", raw["items"].([]any)[0].(map[string]any), "queue_name", "total", "pending", "due", "processing", "done", "failed", "oldest_due_at", "last_activity_at")
	})

	t.Run("throughput places finished jobs in the current bucket", func(t *testing.T) {
		for range 5 {
			ackedID := enqueueJob(t, ctx, queue, "overview.done", []byte(`{"n":"again"}`), nil)
			claimJob(t, ctx, queue, "overview.done")
			if err := queue.Ack(ctx, ackedID); err != nil {
				t.Fatal(err)
			}
			failedAgainID := enqueueJob(t, ctx, queue, "overview.failed", []byte(`{"n":"again"}`), nil)
			claimJob(t, ctx, queue, "overview.failed")
			if err := queue.Fail(ctx, failedAgainID, errors.New("again")); err != nil {
				t.Fatal(err)
			}
			acked, err := queue.GetJob(ctx, ackedID)
			if err != nil {
				t.Fatal(err)
			}
			failedAgain, err := queue.GetJob(ctx, failedAgainID)
			if err != nil {
				t.Fatal(err)
			}

			var overview overviewResponse
			getJSON(t, server.URL+"/api/dashboard/overview", http.StatusOK, &overview)
			buckets := overview.Queue.Throughput.Buckets
			var doneTotal, failedTotal int64
			for _, bucket := range buckets {
				doneTotal += bucket.Done
				failedTotal += bucket.Failed
			}
			wantDone, err := queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1, Status: qpkg.StatusDone})
			if err != nil {
				t.Fatal(err)
			}
			wantFailed, err := queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1, Status: qpkg.StatusFailed})
			if err != nil {
				t.Fatal(err)
			}
			if doneTotal != wantDone || failedTotal != wantFailed {
				t.Fatalf("throughput totals = %d done, %d failed; want %d, %d", doneTotal, failedTotal, wantDone, wantFailed)
			}
			starts := queueBucketStarts(overview.Queue.Throughput)
			doneIndex := bucketContaining(t, starts, 60, acked.DoneAt.Time)
			failedIndex := bucketContaining(t, starts, 60, failedAgain.UpdatedAt)
			if buckets[doneIndex].Done < 1 || buckets[failedIndex].Failed < 1 {
				t.Fatalf("buckets = %+v", buckets)
			}
			last := len(buckets) - 1
			if doneIndex == last && failedIndex == last {
				return
			}
		}
		t.Fatal("finished jobs never landed in the last bucket")
	})
}

func queueBucketStarts(throughput queueThroughput) []string {
	starts := make([]string, 0, len(throughput.Buckets))
	for _, bucket := range throughput.Buckets {
		starts = append(starts, bucket.Start)
	}
	return starts
}

func TestQueueJobDetail(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, workflows := newClients(t, ctx, db)
	server := newServer(t, queue, Options{Token: "secret", Workflow: workflows})
	detailURL := func(id int64) string {
		return server.URL + "/api/dashboard/queue/jobs/" + strconv.FormatInt(id, 10)
	}

	t.Run("json payload", func(t *testing.T) {
		payload := []byte(`{"hello":"world"}`)
		id := enqueueJob(t, ctx, queue, "detail.json", payload, nil)
		var job queueJobDetail
		getJSON(t, detailURL(id), http.StatusOK, &job)
		if job.ID != id || job.QueueName != "detail.json" || job.Status != "pending" || job.MaxAttempts != 3 {
			t.Fatalf("job = %+v", job.queueJob)
		}
		if job.Payload != string(payload) || job.PayloadEncoding != "json" || job.PayloadBytes != len(payload) || job.PayloadTruncated || job.RunID != nil {
			t.Fatalf("payload = %q encoding=%s bytes=%d truncated=%v run_id=%v", job.Payload, job.PayloadEncoding, job.PayloadBytes, job.PayloadTruncated, job.RunID)
		}
		raw := getMap(t, detailURL(id), http.StatusOK)
		requireKeys(t, "job detail", raw, "id", "queue_name", "status", "attempts", "max_attempts", "claims", "available_at", "claimed_by", "claimed_at", "done_at", "last_error", "payload_preview", "created_at", "updated_at", "payload", "payload_encoding", "payload_bytes", "payload_truncated", "run_id")
		if value, present := raw["run_id"]; !present || value != nil {
			t.Fatalf("run_id = %v (present=%v), want explicit null", value, present)
		}
	})

	t.Run("text payload", func(t *testing.T) {
		id := enqueueJob(t, ctx, queue, "detail.text", []byte("plain\ttext payload"), nil)
		var job queueJobDetail
		getJSON(t, detailURL(id), http.StatusOK, &job)
		if job.Payload != "plain\ttext payload" || job.PayloadEncoding != "text" {
			t.Fatalf("job = %q %s", job.Payload, job.PayloadEncoding)
		}
	})

	t.Run("binary payload is base64", func(t *testing.T) {
		payload := []byte{0x00, 0x01, 0xff, 0xfe, 0x10}
		id := enqueueJob(t, ctx, queue, "detail.binary", payload, nil)
		var job queueJobDetail
		getJSON(t, detailURL(id), http.StatusOK, &job)
		if job.PayloadEncoding != "base64" || job.Payload != base64.StdEncoding.EncodeToString(payload) || job.PayloadBytes != len(payload) || job.PayloadTruncated {
			t.Fatalf("job = %q %s bytes=%d truncated=%v", job.Payload, job.PayloadEncoding, job.PayloadBytes, job.PayloadTruncated)
		}
		if !strings.HasPrefix(job.PayloadPreview, "base64:") {
			t.Fatalf("preview = %q", job.PayloadPreview)
		}
	})

	t.Run("oversized json is cut and becomes text", func(t *testing.T) {
		payload := []byte(`{"k":"` + strings.Repeat("a", 1<<20) + `"}`)
		id := enqueueJob(t, ctx, queue, "detail.big", payload, nil)
		var job queueJobDetail
		getJSON(t, detailURL(id), http.StatusOK, &job)
		if job.PayloadEncoding != "text" || !job.PayloadTruncated || len(job.Payload) != 1<<20 || job.PayloadBytes != len(payload) {
			t.Fatalf("encoding=%s truncated=%v shown=%d bytes=%d", job.PayloadEncoding, job.PayloadTruncated, len(job.Payload), job.PayloadBytes)
		}
		if len(job.PayloadPreview) > previewBytes+len("...") {
			t.Fatalf("preview has %d bytes", len(job.PayloadPreview))
		}
	})

	t.Run("cut inside a multibyte character stays text", func(t *testing.T) {
		payload := []byte("a" + strings.Repeat("é", 600000))
		id := enqueueJob(t, ctx, queue, "detail.unicode", payload, nil)
		var job queueJobDetail
		getJSON(t, detailURL(id), http.StatusOK, &job)
		if job.PayloadEncoding != "text" || !job.PayloadTruncated || !utf8.ValidString(job.Payload) || job.PayloadBytes != len(payload) {
			t.Fatalf("encoding=%s truncated=%v valid=%v bytes=%d", job.PayloadEncoding, job.PayloadTruncated, utf8.ValidString(job.Payload), job.PayloadBytes)
		}
		if !bytes.HasPrefix(payload, []byte(job.Payload)) {
			t.Fatal("shown payload is not a prefix of the stored payload")
		}
	})

	t.Run("claimed job reports its claims", func(t *testing.T) {
		id := enqueueJob(t, ctx, queue, "detail.claimed", []byte(`{"claim":true}`), nil)
		claimed, found, err := queue.Claim(ctx, "detail.claimed", "worker-a")
		if err != nil || !found || claimed.ID != id {
			t.Fatalf("claim = %+v found=%v err=%v", claimed, found, err)
		}
		var job queueJobDetail
		getJSON(t, detailURL(id), http.StatusOK, &job)
		if job.Status != "processing" || job.Claims != 1 || job.Attempts != 1 || job.ClaimedBy == nil || *job.ClaimedBy != "worker-a" {
			t.Fatalf("job = %+v", job.queueJob)
		}
		var page listResponse[queueJob]
		getJSON(t, server.URL+"/api/dashboard/queue/jobs?queue=detail.claimed", http.StatusOK, &page)
		if len(page.Items) != 1 || page.Items[0].Claims != 1 {
			t.Fatalf("listed = %+v", page.Items)
		}
	})

	t.Run("workflow run job exposes the run id", func(t *testing.T) {
		flow := workflow.Define("detail-flow", func(_ *workflow.Context, n int) (int, error) { return n, nil })
		run, err := workflows.Start(ctx, flow, 1)
		if err != nil {
			t.Fatal(err)
		}
		var runDetail workflowRunDetail
		getJSON(t, server.URL+"/api/dashboard/workflow/runs/"+run.ID, http.StatusOK, &runDetail)
		if runDetail.JobID == nil {
			t.Fatal("run has no job_id")
		}
		var job queueJobDetail
		getJSON(t, detailURL(*runDetail.JobID), http.StatusOK, &job)
		if job.QueueName != "pgworkflow:detail-flow" || job.RunID == nil || *job.RunID != run.ID || job.PayloadEncoding != "json" {
			t.Fatalf("job = %+v run_id=%v", job.queueJob, job.RunID)
		}
	})

	t.Run("run id is only read from workflow queues", func(t *testing.T) {
		id := enqueueJob(t, ctx, queue, "detail.lookalike", []byte(`{"run_id":"abc"}`), nil)
		var job queueJobDetail
		getJSON(t, detailURL(id), http.StatusOK, &job)
		if job.RunID != nil {
			t.Fatalf("run_id = %v", *job.RunID)
		}
		other := enqueueJob(t, ctx, queue, "pgworkflow:not-a-run", []byte(`{"other":1}`), nil)
		getJSON(t, detailURL(other), http.StatusOK, &job)
		if job.RunID != nil {
			t.Fatalf("run_id = %v", *job.RunID)
		}
	})

	t.Run("missing and invalid ids", func(t *testing.T) {
		requireErrorBody(t, server.URL+"/api/dashboard/queue/jobs/999999", http.StatusNotFound, "job not found")
		for _, id := range []string{"abc", "0", "-4", "1.5"} {
			requireErrorBody(t, server.URL+"/api/dashboard/queue/jobs/"+id, http.StatusBadRequest, "invalid job id")
		}
	})
}

func TestMutationGuardsAndErrorMapping(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t, ctx)
	queue, _ := newClients(t, ctx, db)
	server := newServer(t, queue, Options{Token: "secret"})
	host := strings.TrimPrefix(server.URL, "http://")
	enqueueURL := server.URL + "/api/dashboard/queue/jobs"
	body := `{"queue_name":"csrf","payload":{"a":1}}`
	jsonType := map[string]string{"Content-Type": "application/json"}
	withHeaders := func(extra map[string]string) map[string]string {
		headers := map[string]string{"Content-Type": "application/json"}
		maps.Copy(headers, extra)
		return headers
	}

	rejected := []map[string]string{
		jsonType,
		{"Origin": "http://" + host + ".evil.example"},
		{"Origin": "http://evil.example/" + host},
		{"Origin": "https://evil.example"},
		{"Origin": "null"},
		{"Origin": "http://" + host + "@evil.example"},
		{"X-Requested-With": "XMLHttpRequest"},
	}
	for _, extra := range rejected {
		resp, content := rawRequest(t, http.MethodPost, enqueueURL, body, withHeaders(extra))
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("headers %v: status %d, want 403: %s", extra, resp.StatusCode, content)
		}
	}
	count := func() int64 {
		total, err := queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1, QueueName: "csrf"})
		if err != nil {
			t.Fatal(err)
		}
		return total
	}
	if got := count(); got != 0 {
		t.Fatalf("%d jobs enqueued by rejected requests", got)
	}

	accepted := []map[string]string{
		{"Origin": "http://" + host},
		{"X-Requested-With": "pgkit-admin-ui"},
	}
	for _, extra := range accepted {
		resp, content := rawRequest(t, http.MethodPost, enqueueURL, body, withHeaders(extra))
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("headers %v: status %d, want 201: %s", extra, resp.StatusCode, content)
		}
	}
	if got := count(); got != 2 {
		t.Fatalf("%d jobs enqueued, want 2", got)
	}

	ui := map[string]string{"X-Requested-With": "pgkit-admin-ui", "Content-Type": "application/json"}
	post := func(payload string) (int, string) {
		resp, content := rawRequest(t, http.MethodPost, enqueueURL, payload, ui)
		return resp.StatusCode, strings.TrimSpace(string(content))
	}
	if status, content := post(`{"queue_name":"  ","payload":{}}`); status != http.StatusBadRequest || content != `{"error":"queue name is required"}` {
		t.Fatalf("empty queue: %d %s", status, content)
	}
	if status, content := post(`{"queue_name":`); status != http.StatusBadRequest || content != `{"error":"invalid json body"}` {
		t.Fatalf("bad json: %d %s", status, content)
	}
	for _, missing := range []string{`{"queue_name":"no-payload"}`, `{"queue_name":"no-payload","payload":null}`, `{"queue_name":"no-payload","payload":""}`} {
		if status, content := post(missing); status != http.StatusBadRequest || content != `{"error":"payload is required"}` {
			t.Fatalf("missing payload %s: %d %s", missing, status, content)
		}
	}
	if status, content := post(`{"queue_name":"","payload":null}`); status != http.StatusBadRequest || content != `{"error":"queue name is required"}` {
		t.Fatalf("missing queue name and payload: %d %s", status, content)
	}
	if total, err := queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1, QueueName: "no-payload"}); err != nil || total != 0 {
		t.Fatalf("jobs enqueued without a payload: %d, err = %v", total, err)
	}
	if status, content := post(`{"queue_name":"with-payload","payload":0}`); status != http.StatusCreated {
		t.Fatalf("scalar payload: %d %s", status, content)
	}

	do := func(method, path string) (int, string) {
		resp, content := rawRequest(t, method, server.URL+path, "", ui)
		return resp.StatusCode, strings.TrimSpace(string(content))
	}
	if status, content := do(http.MethodPost, "/api/dashboard/queue/jobs/abc/replay"); status != http.StatusBadRequest || content != `{"error":"invalid job id"}` {
		t.Fatalf("replay invalid id: %d %s", status, content)
	}
	if status, content := do(http.MethodPost, "/api/dashboard/queue/jobs/424242/replay"); status != http.StatusNotFound || content != `{"error":"job not found or not replayable"}` {
		t.Fatalf("replay missing: %d %s", status, content)
	}
	pendingID := enqueueJob(t, ctx, queue, "mapping.pending", []byte(`{}`), nil)
	if status, content := do(http.MethodPost, "/api/dashboard/queue/jobs/"+strconv.FormatInt(pendingID, 10)+"/replay"); status != http.StatusNotFound || content != `{"error":"job not found or not replayable"}` {
		t.Fatalf("replay pending: %d %s", status, content)
	}
	if status, content := do(http.MethodDelete, "/api/dashboard/queue/jobs/424242"); status != http.StatusNotFound || content != `{"error":"job not found"}` {
		t.Fatalf("delete missing: %d %s", status, content)
	}
	if status, content := do(http.MethodDelete, "/api/dashboard/queue/jobs/0"); status != http.StatusBadRequest || content != `{"error":"invalid job id"}` {
		t.Fatalf("delete invalid id: %d %s", status, content)
	}

	busyID := enqueueJob(t, ctx, queue, "mapping.busy", []byte(`{}`), nil)
	claimJob(t, ctx, queue, "mapping.busy")
	if status, content := do(http.MethodDelete, "/api/dashboard/queue/jobs/"+strconv.FormatInt(busyID, 10)); status != http.StatusConflict || content != `{"error":"job is currently processing"}` {
		t.Fatalf("delete busy: %d %s", status, content)
	}
	if err := queue.Fail(ctx, busyID, errors.New("stop")); err != nil {
		t.Fatal(err)
	}
	var replayed queueJob
	postJSON(t, server.URL+"/api/dashboard/queue/jobs/"+strconv.FormatInt(busyID, 10)+"/replay", http.StatusOK, &replayed)
	if replayed.Status != "pending" || replayed.ID != busyID {
		t.Fatalf("replayed = %+v", replayed)
	}
	if status, _ := do(http.MethodDelete, "/api/dashboard/queue/jobs/"+strconv.FormatInt(busyID, 10)); status != http.StatusNoContent {
		t.Fatalf("delete pending: %d", status)
	}
}
