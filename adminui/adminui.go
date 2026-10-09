package adminui

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	qpkg "github.com/nanostack-dev/pgkit/queue"
	"github.com/nanostack-dev/pgkit/workflow"
)

const (
	defaultTokenEnv              = "PGKIT_DASHBOARD_TOKEN"
	defaultEnableMutationsEnv    = "PGKIT_DASHBOARD_ENABLE_API"
	defaultAdminServerAddr       = "127.0.0.1:18081"
	defaultListLimit             = 20
	maxListLimit                 = 100
	requestHeaderRequestedWith   = "X-Requested-With"
	requestHeaderRequestedWithUI = "pgkit-admin-ui"
)

var ErrMissingToken = errors.New("pgkit adminui: missing dashboard token")

type UI struct {
	queue           *qpkg.Client
	workflow        *workflow.Client
	token           string
	enableMutations bool
	listLimit       int
	assets          fs.FS
	server          *http.Server
}

type Options struct {
	Token           string
	TokenEnv        string
	EnableMutations *bool
	EnableAPIEnv    string
	Limit           int
	Workflow        *workflow.Client
	Assets          fs.FS
}

type queueSummary struct {
	TotalJobs      int64 `json:"total_jobs"`
	PendingJobs    int64 `json:"pending_jobs"`
	ProcessingJobs int64 `json:"processing_jobs"`
	DoneJobs       int64 `json:"done_jobs"`
	FailedJobs     int64 `json:"failed_jobs"`
	AdvisoryLocks  int   `json:"advisory_locks"`
	Queues         int64 `json:"queues"`
}

type queueJob struct {
	ID             int64   `json:"id"`
	QueueName      string  `json:"queue_name"`
	Status         string  `json:"status"`
	Attempts       int     `json:"attempts"`
	MaxAttempts    int     `json:"max_attempts"`
	AvailableAt    string  `json:"available_at"`
	ClaimedBy      *string `json:"claimed_by"`
	ClaimedAt      *string `json:"claimed_at"`
	DoneAt         *string `json:"done_at"`
	LastError      *string `json:"last_error"`
	PayloadPreview string  `json:"payload_preview"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
}

type workflowRun struct {
	ID          string  `json:"id"`
	Workflow    string  `json:"workflow"`
	Version     int     `json:"version"`
	Key         *string `json:"key"`
	Status      string  `json:"status"`
	Error       *string `json:"error"`
	TimedOut    bool    `json:"timed_out"`
	ParentRunID *string `json:"parent_run_id"`
	Input       string  `json:"input"`
	Output      *string `json:"output"`
	WakeAt      *string `json:"wake_at"`
	DeadlineAt  *string `json:"deadline_at"`
	CreatedAt   string  `json:"created_at"`
	StartedAt   *string `json:"started_at"`
	CompletedAt *string `json:"completed_at"`
	UpdatedAt   string  `json:"updated_at"`
}

type workflowStep struct {
	Name        string  `json:"name"`
	Kind        string  `json:"kind"`
	Status      string  `json:"status"`
	Attempts    int     `json:"attempts"`
	Output      *string `json:"output"`
	Error       *string `json:"error"`
	WakeAt      *string `json:"wake_at"`
	ChildRunID  *string `json:"child_run_id"`
	Signal      *string `json:"signal"`
	CreatedAt   string  `json:"created_at"`
	CompletedAt *string `json:"completed_at"`
	UpdatedAt   string  `json:"updated_at"`
}

type workflowRunDetail struct {
	Run      workflowRun    `json:"run"`
	Steps    []workflowStep `json:"steps"`
	Children []workflowRun  `json:"children"`
}

type listResponse[T any] struct {
	Items  []T   `json:"items"`
	Total  int64 `json:"total"`
	Limit  int   `json:"limit"`
	Offset int   `json:"offset"`
}

type snapshotResponse struct {
	Queue struct {
		Summary queueSummary           `json:"summary"`
		Jobs    listResponse[queueJob] `json:"jobs"`
		Locks   []qpkg.AdvisoryLock    `json:"locks"`
	} `json:"queue"`
	Workflow struct {
		Runs listResponse[workflowRun] `json:"runs"`
	} `json:"workflow"`
}

type enqueueRequest struct {
	QueueName    string          `json:"queue_name"`
	Payload      json.RawMessage `json:"payload"`
	MaxAttempts  int             `json:"max_attempts"`
	DelaySeconds int             `json:"delay_seconds"`
}

func New(queue *qpkg.Client, opts Options) (*UI, error) {
	if queue == nil {
		return nil, qpkg.ErrNilDB
	}
	token := strings.TrimSpace(opts.Token)
	if token == "" {
		envName := opts.TokenEnv
		if envName == "" {
			envName = defaultTokenEnv
		}
		token = strings.TrimSpace(os.Getenv(envName))
	}
	if token == "" {
		return nil, ErrMissingToken
	}
	enableMutations := true
	if opts.EnableMutations != nil {
		enableMutations = *opts.EnableMutations
	} else {
		envName := opts.EnableAPIEnv
		if envName == "" {
			envName = defaultEnableMutationsEnv
		}
		if raw := strings.TrimSpace(os.Getenv(envName)); raw != "" {
			enableMutations = parseBoolDefault(raw, true)
		}
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	assets := opts.Assets
	if assets == nil {
		sub, err := fs.Sub(embeddedDist, "dist")
		if err != nil {
			return nil, fmt.Errorf("pgkit adminui: sub embedded dist: %w", err)
		}
		assets = sub
	}
	return &UI{
		queue:           queue,
		workflow:        opts.Workflow,
		token:           token,
		enableMutations: enableMutations,
		listLimit:       limit,
		assets:          assets,
	}, nil
}

func NewFromEnv(queue *qpkg.Client, workflows *workflow.Client) (*UI, error) {
	return New(queue, Options{Workflow: workflows})
}

func (u *UI) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/dashboard/snapshot", u.requireToken(u.handleSnapshot))
	mux.HandleFunc("GET /api/dashboard/queue/summary", u.requireToken(u.handleQueueSummary))
	mux.HandleFunc("GET /api/dashboard/queue/jobs", u.requireToken(u.handleQueueJobs))
	mux.HandleFunc("GET /api/dashboard/queue/locks", u.requireToken(u.handleQueueLocks))
	mux.HandleFunc("GET /api/dashboard/workflow/runs", u.requireToken(u.handleWorkflowRuns))
	mux.HandleFunc("GET /api/dashboard/workflow/runs/{id}", u.requireToken(u.handleWorkflowRun))
	if u.enableMutations {
		mux.HandleFunc("POST /api/dashboard/queue/jobs", u.requireToken(u.requireCSRF(u.handleEnqueueJob)))
		mux.HandleFunc("POST /api/dashboard/queue/jobs/{id}/replay", u.requireToken(u.requireCSRF(u.handleReplayJob)))
		mux.HandleFunc("DELETE /api/dashboard/queue/jobs/{id}", u.requireToken(u.requireCSRF(u.handleDeleteJob)))
		mux.HandleFunc("POST /api/dashboard/workflow/runs/{id}/retry", u.requireToken(u.requireCSRF(u.handleRetryWorkflowRun)))
		mux.HandleFunc("POST /api/dashboard/workflow/runs/{id}/cancel", u.requireToken(u.requireCSRF(u.handleCancelWorkflowRun)))
	}
	fileServer := http.FileServer(http.FS(u.assets))
	mux.Handle("GET /_app/", u.requireToken(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileServer.ServeHTTP(w, r)
	})))
	mux.HandleFunc("GET /", u.requireToken(u.handleSPA))
	mux.HandleFunc("GET /queues", u.requireToken(u.handleSPA))
	mux.HandleFunc("GET /workflows", u.requireToken(u.handleSPA))
	mux.HandleFunc("GET /workflows/{id}", u.requireToken(u.handleSPA))
	mux.HandleFunc("GET /favicon.ico", u.requireToken(u.handleAsset))
	return mux
}

func (u *UI) ListenAndServe(addr string) error {
	if addr == "" {
		addr = defaultAdminServerAddr
	}
	u.server = &http.Server{Addr: addr, Handler: u.Handler(), ReadHeaderTimeout: 5 * time.Second}
	if err := u.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (u *UI) Shutdown(ctx context.Context) error {
	if u.server == nil {
		return nil
	}
	return u.server.Shutdown(ctx)
}

func (u *UI) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	summary, err := u.buildQueueSummary(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	jobs, err := u.buildQueueJobs(r.Context(), qpkg.ListJobsParams{Limit: 8})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	locks, err := qpkg.ListAdvisoryLocks(r.Context(), u.queue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	runs := listResponse[workflowRun]{Items: []workflowRun{}, Total: 0, Limit: 8, Offset: 0}
	if u.workflow != nil {
		runs, err = u.buildWorkflowRuns(r.Context(), workflow.ListRunsParams{Limit: 8})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}
	var response snapshotResponse
	response.Queue.Summary = summary
	response.Queue.Jobs = jobs
	response.Queue.Locks = locks
	response.Workflow.Runs = runs
	writeJSON(w, http.StatusOK, response)
}

func (u *UI) handleQueueSummary(w http.ResponseWriter, r *http.Request) {
	summary, err := u.buildQueueSummary(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (u *UI) handleQueueJobs(w http.ResponseWriter, r *http.Request) {
	params := qpkg.ListJobsParams{
		Limit:     queryInt(r, "limit", u.listLimit),
		Offset:    queryInt(r, "offset", 0),
		QueueName: strings.TrimSpace(r.URL.Query().Get("queue")),
		Search:    strings.TrimSpace(r.URL.Query().Get("search")),
	}
	if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
		params.Status = qpkg.JobStatus(status)
	}
	jobs, err := u.buildQueueJobs(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (u *UI) handleQueueLocks(w http.ResponseWriter, r *http.Request) {
	locks, err := qpkg.ListAdvisoryLocks(r.Context(), u.queue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, locks)
}

func (u *UI) handleWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	if u.workflow == nil {
		writeJSON(w, http.StatusOK, listResponse[workflowRun]{Items: []workflowRun{}, Total: 0, Limit: queryInt(r, "limit", u.listLimit), Offset: queryInt(r, "offset", 0)})
		return
	}
	params := workflow.ListRunsParams{
		Workflow:    strings.TrimSpace(r.URL.Query().Get("workflow")),
		ParentRunID: strings.TrimSpace(r.URL.Query().Get("parent_run_id")),
		Search:      strings.TrimSpace(r.URL.Query().Get("search")),
		Limit:       queryInt(r, "limit", u.listLimit),
		Offset:      queryInt(r, "offset", 0),
	}
	if status := strings.TrimSpace(r.URL.Query().Get("status")); status != "" {
		params.Status = workflow.RunStatus(status)
	}
	response, err := u.buildWorkflowRuns(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (u *UI) handleWorkflowRun(w http.ResponseWriter, r *http.Request) {
	if u.workflow == nil {
		http.NotFound(w, r)
		return
	}
	detail, err := u.buildWorkflowRunDetail(r.Context(), strings.TrimSpace(r.PathValue("id")))
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (u *UI) handleEnqueueJob(w http.ResponseWriter, r *http.Request) {
	var request enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid json body: %w", err))
		return
	}
	params := qpkg.EnqueueParams{QueueName: strings.TrimSpace(request.QueueName), Payload: request.Payload, MaxAttempts: request.MaxAttempts}
	if request.DelaySeconds > 0 {
		at := time.Now().UTC().Add(time.Duration(request.DelaySeconds) * time.Second)
		params.AvailableAt = &at
	}
	id, err := u.queue.Enqueue(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (u *UI) handleReplayJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathID(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid job id"))
		return
	}
	if err := u.queue.ReplayJob(r.Context(), id); err != nil {
		status := http.StatusConflict
		if errors.Is(err, qpkg.ErrJobNotFound) {
			status = http.StatusNotFound
		}
		writeError(w, status, err)
		return
	}
	job, err := u.queue.GetJob(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, toQueueJob(*job))
}

func (u *UI) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathID(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid job id"))
		return
	}
	if err := u.queue.DeleteJob(r.Context(), id); err != nil {
		status := http.StatusConflict
		if errors.Is(err, qpkg.ErrJobNotFound) {
			status = http.StatusNotFound
		}
		writeError(w, status, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (u *UI) handleRetryWorkflowRun(w http.ResponseWriter, r *http.Request) {
	u.mutateWorkflowRun(w, r, u.workflow.Retry)
}

func (u *UI) handleCancelWorkflowRun(w http.ResponseWriter, r *http.Request) {
	u.mutateWorkflowRun(w, r, u.workflow.Cancel)
}

func (u *UI) mutateWorkflowRun(w http.ResponseWriter, r *http.Request, mutate func(context.Context, string) error) {
	if u.workflow == nil {
		http.NotFound(w, r)
		return
	}
	runID := strings.TrimSpace(r.PathValue("id"))
	if err := mutate(r.Context(), runID); err != nil {
		writeWorkflowError(w, err)
		return
	}
	run, err := u.workflow.GetRun(r.Context(), runID)
	if err != nil {
		writeWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorkflowRun(run))
}

func writeWorkflowError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, workflow.ErrRunNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, workflow.ErrRunNotRetryable), errors.Is(err, workflow.ErrRunFinished):
		writeError(w, http.StatusConflict, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func (u *UI) handleSPA(w http.ResponseWriter, _ *http.Request) {
	u.serveAsset(w, "index.html", "text/html; charset=utf-8")
}

func (u *UI) handleAsset(w http.ResponseWriter, r *http.Request) {
	u.serveAsset(w, strings.TrimPrefix(path.Clean(r.URL.Path), "/"), "")
}

func (u *UI) serveAsset(w http.ResponseWriter, name, contentType string) {
	content, err := fs.ReadFile(u.assets, name)
	if err != nil {
		http.NotFound(w, &http.Request{})
		return
	}
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	_, _ = w.Write(content)
}

func (u *UI) buildQueueSummary(ctx context.Context) (queueSummary, error) {
	total, err := u.queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1})
	if err != nil {
		return queueSummary{}, err
	}
	pending, err := u.queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1, Status: qpkg.StatusPending})
	if err != nil {
		return queueSummary{}, err
	}
	processing, err := u.queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1, Status: qpkg.StatusProcessing})
	if err != nil {
		return queueSummary{}, err
	}
	done, err := u.queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1, Status: qpkg.StatusDone})
	if err != nil {
		return queueSummary{}, err
	}
	failed, err := u.queue.CountJobs(ctx, qpkg.ListJobsParams{Limit: 1, Status: qpkg.StatusFailed})
	if err != nil {
		return queueSummary{}, err
	}
	locks, err := qpkg.ListAdvisoryLocks(ctx, u.queue)
	if err != nil {
		return queueSummary{}, err
	}
	queues, err := qpkg.CountDistinctQueues(ctx, u.queue)
	if err != nil {
		return queueSummary{}, err
	}
	return queueSummary{TotalJobs: total, PendingJobs: pending, ProcessingJobs: processing, DoneJobs: done, FailedJobs: failed, AdvisoryLocks: len(locks), Queues: queues}, nil
}

func (u *UI) buildQueueJobs(ctx context.Context, params qpkg.ListJobsParams) (listResponse[queueJob], error) {
	jobs, err := u.queue.ListJobs(ctx, params)
	if err != nil {
		return listResponse[queueJob]{}, err
	}
	total, err := u.queue.CountJobs(ctx, params)
	if err != nil {
		return listResponse[queueJob]{}, err
	}
	items := make([]queueJob, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, toQueueJob(job))
	}
	return listResponse[queueJob]{Items: items, Total: total, Limit: params.Limit, Offset: params.Offset}, nil
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
	children, err := u.workflow.ListRuns(ctx, workflow.ListRunsParams{ParentRunID: runID, Limit: maxListLimit})
	if err != nil {
		return workflowRunDetail{}, err
	}
	detail := workflowRunDetail{Run: toWorkflowRun(run), Steps: make([]workflowStep, 0, len(steps)), Children: make([]workflowRun, 0, len(children))}
	for _, step := range steps {
		detail.Steps = append(detail.Steps, toWorkflowStep(step))
	}
	for _, child := range children {
		detail.Children = append(detail.Children, toWorkflowRun(child))
	}
	return detail, nil
}

func (u *UI) buildWorkflowRuns(ctx context.Context, params workflow.ListRunsParams) (listResponse[workflowRun], error) {
	runs, err := u.workflow.ListRuns(ctx, params)
	if err != nil {
		return listResponse[workflowRun]{}, err
	}
	total, err := u.workflow.CountRuns(ctx, params)
	if err != nil {
		return listResponse[workflowRun]{}, err
	}
	items := make([]workflowRun, 0, len(runs))
	for _, run := range runs {
		items = append(items, toWorkflowRun(run))
	}
	return listResponse[workflowRun]{Items: items, Total: total, Limit: params.Limit, Offset: params.Offset}, nil
}

func toQueueJob(job qpkg.Job) queueJob {
	return queueJob{
		ID:             job.ID,
		QueueName:      job.QueueName,
		Status:         string(job.Status),
		Attempts:       job.Attempts,
		MaxAttempts:    job.MaxAttempts,
		AvailableAt:    job.AvailableAt.Format(time.RFC3339),
		ClaimedBy:      nullString(job.ClaimedBy),
		ClaimedAt:      nullTime(job.ClaimedAt),
		DoneAt:         nullTime(job.DoneAt),
		LastError:      nullString(job.LastError),
		PayloadPreview: qpkg.PayloadPreview(job.Payload),
		CreatedAt:      job.CreatedAt.Format(time.RFC3339),
		UpdatedAt:      job.UpdatedAt.Format(time.RFC3339),
	}
}

func toWorkflowRun(run workflow.RunInfo) workflowRun {
	return workflowRun{
		ID:          run.ID,
		Workflow:    run.Workflow,
		Version:     run.Version,
		Key:         optionalString(run.Key),
		Status:      string(run.Status),
		Error:       optionalString(run.Error),
		TimedOut:    run.TimedOut,
		ParentRunID: optionalString(run.ParentRunID),
		Input:       string(run.Input),
		Output:      optionalString(string(run.Output)),
		WakeAt:      optionalTime(run.WakeAt),
		DeadlineAt:  optionalTime(run.DeadlineAt),
		CreatedAt:   run.CreatedAt.Format(time.RFC3339),
		StartedAt:   optionalTime(run.StartedAt),
		CompletedAt: optionalTime(run.CompletedAt),
		UpdatedAt:   run.UpdatedAt.Format(time.RFC3339),
	}
}

func toWorkflowStep(step workflow.StepInfo) workflowStep {
	return workflowStep{
		Name:        step.Name,
		Kind:        string(step.Kind),
		Status:      string(step.Status),
		Attempts:    step.Attempts,
		Output:      optionalString(string(step.Output)),
		Error:       optionalString(step.Error),
		WakeAt:      optionalTime(step.WakeAt),
		ChildRunID:  optionalString(step.ChildRunID),
		Signal:      optionalString(step.Signal),
		CreatedAt:   step.CreatedAt.Format(time.RFC3339),
		CompletedAt: optionalTime(step.CompletedAt),
		UpdatedAt:   step.UpdatedAt.Format(time.RFC3339),
	}
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func optionalTime(value time.Time) *string {
	if value.IsZero() {
		return nil
	}
	formatted := value.Format(time.RFC3339)
	return &formatted
}

func (u *UI) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user == "" || subtle.ConstantTimeCompare([]byte(pass), []byte(u.token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="pgkit-admin"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (u *UI) requireCSRF(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(requestHeaderRequestedWith) == requestHeaderRequestedWithUI {
			next(w, r)
			return
		}
		if sameOrigin(r) {
			next(w, r)
			return
		}
		http.Error(w, "forbidden: missing CSRF token", http.StatusForbidden)
	}
}

// sameOrigin reports whether the request's Origin header names this server's host
// exactly; a host that merely contains it, like admin.example.com.evil.example,
// does not count.
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	parsed, err := url.Parse(origin)
	return err == nil && parsed.Host != "" && strings.EqualFold(parsed.Host, r.Host)
}

func queryInt(r *http.Request, key string, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func parsePathID(raw string) (int64, bool) {
	id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func parseBoolDefault(raw string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func nullString(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	value := v.String
	return &value
}

func nullTime(v sql.NullTime) *string {
	if !v.Valid {
		return nil
	}
	value := v.Time.Format(time.RFC3339)
	return &value
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// writeError reports err to the client, except for server errors, whose details
// (database errors included) stay out of responses.
func writeError(w http.ResponseWriter, status int, err error) {
	message := err.Error()
	if status >= http.StatusInternalServerError {
		message = http.StatusText(status)
	}
	writeJSON(w, status, map[string]any{"error": message})
}
