package queue

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultDashboardTokenEnv = "PGKIT_DASHBOARD_TOKEN"

const defaultDashboardEnableAPIEnv = "PGKIT_DASHBOARD_ENABLE_API"

// DefaultAdminServerAddr is the recommended listen address for embedded admin usage.
// It is loopback-only by default for safety.
const DefaultAdminServerAddr = "127.0.0.1:18081"

const (
	defaultDashboardListLimit = 50
	maxDashboardListLimit     = 100
)

var ErrMissingDashboardToken = errors.New("pgqueue: missing dashboard token")

//go:embed templates/*.html
var dashboardTemplates embed.FS

// Dashboard serves the pgqueue admin UI.
type Dashboard struct {
	client        *Client
	token         string
	tmpl          *template.Template
	enableEnqueue bool
	listLimit     int
	server        *http.Server
}

// DashboardOptions configures the dashboard.
type DashboardOptions struct {
	Token            string
	Limit            int
	TokenEnv         string
	EnableEnqueueAPI *bool
	EnableAPIEnv     string
}

type dashboardJobsQuery struct {
	QueueName string
	Status    string
	Search    string
	Offset    int
	Limit     int
	Page      int
}

type dashboardJobRow struct {
	ID          int64
	Queue       string
	Status      string
	Attempts    string
	AvailableAt string
	ClaimedBy   string
	Payload     string
	LastError   string
	CanReplay   bool
	CanDelete   bool
}

// NewDashboardFromEnv creates a dashboard using PGKIT_DASHBOARD_TOKEN env var.
func NewDashboardFromEnv(client *Client) (*Dashboard, error) {
	return NewDashboardFromEnvWithOptions(client, DashboardOptions{})
}

// NewDashboardFromEnvWithOptions creates a dashboard with custom options.
func NewDashboardFromEnvWithOptions(client *Client, opts DashboardOptions) (*Dashboard, error) {
	if client == nil || client.db == nil {
		return nil, ErrNilDB
	}

	token := strings.TrimSpace(opts.Token)
	if token == "" {
		envName := opts.TokenEnv
		if envName == "" {
			envName = defaultDashboardTokenEnv
		}
		token = strings.TrimSpace(os.Getenv(envName))
	}
	if token == "" {
		return nil, ErrMissingDashboardToken
	}

	tmpl, err := template.ParseFS(dashboardTemplates, "templates/dashboard.html")
	if err != nil {
		return nil, fmt.Errorf("pgqueue: parse dashboard template: %w", err)
	}

	enableEnqueue := true
	if opts.EnableEnqueueAPI != nil {
		enableEnqueue = *opts.EnableEnqueueAPI
	} else {
		envName := opts.EnableAPIEnv
		if envName == "" {
			envName = defaultDashboardEnableAPIEnv
		}
		envVal := strings.TrimSpace(os.Getenv(envName))
		if envVal != "" {
			enableEnqueue = parseBoolDefault(envVal, true)
		}
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultDashboardListLimit
	}
	if limit > maxDashboardListLimit {
		limit = maxDashboardListLimit
	}

	return &Dashboard{
		client:        client,
		token:         token,
		tmpl:          tmpl,
		enableEnqueue: enableEnqueue,
		listLimit:     limit,
	}, nil
}

// Handler returns the HTTP handler for the dashboard.
func (d *Dashboard) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", d.requireToken(d.handleIndex))
	mux.HandleFunc("GET /fragment/jobs", d.requireToken(d.handleJobsFragment))
	mux.HandleFunc("GET /fragment/jobs/{id}", d.requireToken(d.handleJobRowFragment))
	mux.HandleFunc("GET /fragment/locks", d.requireToken(d.handleLocksFragment))
	if d.enableEnqueue {
		mux.HandleFunc("POST /enqueue", d.requireToken(d.requireCSRF(d.handleEnqueue)))
		mux.HandleFunc("POST /api/jobs", d.requireToken(d.requireCSRF(d.handleEnqueueAPI)))
	}
	mux.HandleFunc("POST /jobs/{id}/replay", d.requireToken(d.requireCSRF(d.handleReplayJob)))
	mux.HandleFunc("DELETE /jobs/{id}", d.requireToken(d.requireCSRF(d.handleDeleteJob)))
	return mux
}

// ListenAndServe starts the dashboard HTTP server on the given address.
// If addr is empty, DefaultAdminServerAddr is used.
// The call blocks until the server is stopped or an error occurs.
// Use Shutdown to gracefully stop the server.
func (d *Dashboard) ListenAndServe(addr string) error {
	if addr == "" {
		addr = DefaultAdminServerAddr
	}
	d.server = &http.Server{
		Addr:              addr,
		Handler:           d.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := d.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the embedded dashboard server.
// If no server was started via ListenAndServe, this is a no-op.
func (d *Dashboard) Shutdown(ctx context.Context) error {
	if d.server == nil {
		return nil
	}
	return d.server.Shutdown(ctx)
}

// Addr returns the listen address of the running server, or an empty string
// if the server has not been started.
func (d *Dashboard) Addr() string {
	if d.server == nil {
		return ""
	}
	return d.server.Addr
}

func (d *Dashboard) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := d.tmpl.ExecuteTemplate(w, "dashboard.html", map[string]any{
		"can_enqueue": d.enableEnqueue,
		"limit":       d.listLimit,
	}); err != nil {
		d.client.logError(r.Context(), "render dashboard index failed", map[string]any{"error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (d *Dashboard) handleJobsFragment(w http.ResponseWriter, r *http.Request) {
	query := d.readJobsQuery(r)

	params := ListJobsParams{
		Limit:     query.Limit,
		Offset:    query.Offset,
		QueueName: query.QueueName,
		Search:    query.Search,
	}
	if query.Status != "" {
		params.Status = JobStatus(query.Status)
	}

	jobs, err := d.client.ListJobs(r.Context(), params)
	if err != nil {
		d.client.logError(r.Context(), "list jobs failed", map[string]any{"error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	total, err := d.client.CountJobs(r.Context(), params)
	if err != nil {
		d.client.logError(r.Context(), "count jobs failed", map[string]any{"error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	rows := make([]dashboardJobRow, 0, len(jobs))
	for _, j := range jobs {
		rows = append(rows, buildDashboardRow(j))
	}

	hasPrev := query.Offset > 0
	nextOffset := query.Offset + query.Limit
	hasNext := int64(nextOffset) < total
	prevOffset := query.Offset - query.Limit
	if prevOffset < 0 {
		prevOffset = 0
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := d.tmpl.ExecuteTemplate(w, "jobs_fragment", map[string]any{
		"rows":        rows,
		"total":       total,
		"offset":      query.Offset,
		"limit":       query.Limit,
		"has_prev":    hasPrev,
		"has_next":    hasNext,
		"prev_offset": prevOffset,
		"next_offset": nextOffset,
		"filters": map[string]any{
			"queue":  query.QueueName,
			"status": query.Status,
			"search": query.Search,
		},
	}); err != nil {
		d.client.logError(r.Context(), "render jobs fragment failed", map[string]any{"error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (d *Dashboard) handleJobRowFragment(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathID(r.PathValue("id"))
	if !ok {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	job, err := d.client.GetJob(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		d.client.logError(r.Context(), "get job failed", map[string]any{"id": id, "error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := d.tmpl.ExecuteTemplate(w, "job_row", buildDashboardRow(*job)); err != nil {
		d.client.logError(r.Context(), "render job row failed", map[string]any{"id": id, "error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (d *Dashboard) handleReplayJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathID(r.PathValue("id"))
	if !ok {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	if err := d.client.ReplayJob(r.Context(), id); err != nil {
		if errors.Is(err, ErrJobNotFound) {
			http.Error(w, "job not replayable", http.StatusConflict)
			return
		}
		d.client.logError(r.Context(), "replay job failed", map[string]any{"id": id, "error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	job, err := d.client.GetJob(r.Context(), id)
	if err != nil {
		d.client.logError(r.Context(), "get replayed job failed", map[string]any{"id": id, "error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := d.tmpl.ExecuteTemplate(w, "job_row", buildDashboardRow(*job)); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (d *Dashboard) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathID(r.PathValue("id"))
	if !ok {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	if err := d.client.DeleteJob(r.Context(), id); err != nil {
		switch {
		case errors.Is(err, ErrJobNotFound):
			http.Error(w, "not found", http.StatusNotFound)
		case errors.Is(err, ErrJobBusy):
			http.Error(w, "job is processing", http.StatusConflict)
		default:
			d.client.logError(r.Context(), "delete job failed", map[string]any{"id": id, "error": err.Error()})
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (d *Dashboard) handleLocksFragment(w http.ResponseWriter, r *http.Request) {
	locks, err := listAdvisoryLocks(r.Context(), d.client.db)
	if err != nil {
		d.client.logError(r.Context(), "list advisory locks failed", map[string]any{"error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := d.tmpl.ExecuteTemplate(w, "locks_fragment", map[string]any{"rows": locks}); err != nil {
		d.client.logError(r.Context(), "render locks fragment failed", map[string]any{"error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (d *Dashboard) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if !d.enableEnqueue {
		http.Error(w, "enqueue API disabled", http.StatusNotFound)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	queueName := strings.TrimSpace(r.FormValue("queue"))
	payload := []byte(r.FormValue("payload"))
	maxAttempts := defaultMaxAttempts
	if rawAttempts := strings.TrimSpace(r.FormValue("max_attempts")); rawAttempts != "" {
		parsed, err := strconv.Atoi(rawAttempts)
		if err != nil {
			http.Error(w, "max_attempts must be an integer", http.StatusBadRequest)
			return
		}
		maxAttempts = parsed
	}

	p := EnqueueParams{QueueName: queueName, Payload: payload, MaxAttempts: maxAttempts}

	if rawDelay := strings.TrimSpace(r.FormValue("delay_seconds")); rawDelay != "" {
		seconds, err := strconv.Atoi(rawDelay)
		if err != nil {
			http.Error(w, "delay_seconds must be an integer", http.StatusBadRequest)
			return
		}
		if seconds > 0 {
			t := time.Now().UTC().Add(time.Duration(seconds) * time.Second)
			p.AvailableAt = &t
		}
	}

	id, err := d.client.Enqueue(r.Context(), p)
	if err != nil {
		d.client.logWarn(r.Context(), "enqueue from dashboard form failed", map[string]any{"error": err.Error(), "queue": queueName})
		http.Error(w, "enqueue failed", http.StatusBadRequest)
		return
	}
	d.client.logInfo(r.Context(), "job enqueued from dashboard form", map[string]any{"id": id, "queue": queueName})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, "<div class=\"text-emerald-700\">Enqueued job id %d</div>", id)
}

type enqueueAPIRequest struct {
	QueueName    string          `json:"queue_name"`
	Payload      json.RawMessage `json:"payload"`
	MaxAttempts  int             `json:"max_attempts"`
	DelaySeconds int             `json:"delay_seconds"`
}

func (d *Dashboard) handleEnqueueAPI(w http.ResponseWriter, r *http.Request) {
	if !d.enableEnqueue {
		http.Error(w, "enqueue API disabled", http.StatusNotFound)
		return
	}

	var reqBody enqueueAPIRequest
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	p := EnqueueParams{
		QueueName:   strings.TrimSpace(reqBody.QueueName),
		Payload:     reqBody.Payload,
		MaxAttempts: reqBody.MaxAttempts,
	}
	if reqBody.DelaySeconds > 0 {
		t := time.Now().UTC().Add(time.Duration(reqBody.DelaySeconds) * time.Second)
		p.AvailableAt = &t
	}

	id, err := d.client.Enqueue(r.Context(), p)
	if err != nil {
		d.client.logWarn(r.Context(), "enqueue from dashboard api failed", map[string]any{"error": err.Error(), "queue": p.QueueName})
		http.Error(w, "enqueue failed", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
}

func (d *Dashboard) readJobsQuery(r *http.Request) dashboardJobsQuery {
	query := dashboardJobsQuery{
		QueueName: strings.TrimSpace(r.URL.Query().Get("queue")),
		Status:    strings.TrimSpace(r.URL.Query().Get("status")),
		Search:    strings.TrimSpace(r.URL.Query().Get("search")),
		Limit:     d.listLimit,
	}

	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			query.Limit = parsed
		}
	}
	if query.Limit <= 0 {
		query.Limit = d.listLimit
	}
	if query.Limit > maxDashboardListLimit {
		query.Limit = maxDashboardListLimit
	}

	if v := strings.TrimSpace(r.URL.Query().Get("offset")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			query.Offset = parsed
		}
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	query.Page = (query.Offset / query.Limit) + 1

	if len(query.Search) > maxSearchLength {
		query.Search = query.Search[:maxSearchLength]
	}

	return query
}

func buildDashboardRow(j Job) dashboardJobRow {
	return dashboardJobRow{
		ID:          j.ID,
		Queue:       j.QueueName,
		Status:      string(j.Status),
		Attempts:    fmt.Sprintf("%d/%d", j.Attempts, j.MaxAttempts),
		AvailableAt: j.AvailableAt.Format(time.RFC3339),
		ClaimedBy:   nullStringOrDash(j.ClaimedBy),
		Payload:     payloadPreview(j.Payload),
		LastError:   nullStringOrDash(j.LastError),
		CanReplay:   j.Status == StatusDone || j.Status == StatusFailed,
		CanDelete:   j.Status != StatusProcessing,
	}
}

func parsePathID(raw string) (int64, bool) {
	id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// requireToken enforces Basic Auth with constant-time token comparison.
func (d *Dashboard) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(pass), []byte(d.token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="pgqueue-dashboard"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if user == "" {
			w.Header().Set("WWW-Authenticate", `Basic realm="pgqueue-dashboard"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

// requireCSRF provides simple CSRF mitigation for mutating requests.
func (d *Dashboard) requireCSRF(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("HX-Request") == "true" {
			next(w, r)
			return
		}

		origin := r.Header.Get("Origin")
		if origin != "" {
			host := r.Host
			if host != "" && strings.Contains(origin, host) {
				next(w, r)
				return
			}
		}

		http.Error(w, "forbidden: missing CSRF token", http.StatusForbidden)
	}
}

func payloadPreview(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	s := string(payload)
	if isPrintableUTF8(s) {
		if len(s) > 120 {
			return s[:120] + "..."
		}
		return s
	}
	return "base64:" + base64.StdEncoding.EncodeToString(payload)
}

func isPrintableUTF8(s string) bool {
	for _, r := range s {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			return false
		}
	}
	return true
}

func nullStringOrDash(v sql.NullString) string {
	if !v.Valid || v.String == "" {
		return "-"
	}
	return v.String
}

func listAdvisoryLocks(ctx context.Context, db *sql.DB) ([]AdvisoryLock, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	rows, err := db.QueryContext(
		ctx,
		`SELECT pid, mode, granted, classid, objid, objsubid
         FROM pg_locks
         WHERE locktype = 'advisory'
         ORDER BY pid ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("query pg_locks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]AdvisoryLock, 0)
	for rows.Next() {
		var pid int64
		var mode string
		var granted bool
		var classID int64
		var objID int64
		var objSubID int64
		if err := rows.Scan(&pid, &mode, &granted, &classID, &objID, &objSubID); err != nil {
			return nil, fmt.Errorf("scan pg_locks: %w", err)
		}
		out = append(out, AdvisoryLock{
			PID:         pid,
			Mode:        mode,
			Granted:     granted,
			ClassID:     classID,
			ObjectID:    objID,
			ObjectSubID: objSubID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pg_locks: %w", err)
	}

	return out, nil
}

func parseBoolDefault(raw string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(raw))
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return def
	}
}
