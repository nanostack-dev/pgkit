package pgqueue

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

const defaultDashboardListLimit = 100

var ErrMissingDashboardToken = errors.New("pgqueue: missing dashboard token")

//go:embed templates/*.html
var dashboardTemplates embed.FS

// Dashboard serves the pgqueue admin UI.
type Dashboard struct {
	client        *Client
	token         string
	tmpl          *template.Template
	enableEnqueue bool
}

// DashboardOptions configures the dashboard.
type DashboardOptions struct {
	Token            string
	Limit            int
	TokenEnv         string
	EnableEnqueueAPI *bool
	EnableAPIEnv     string
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

	return &Dashboard{
		client:        client,
		token:         token,
		tmpl:          tmpl,
		enableEnqueue: enableEnqueue,
	}, nil
}

// Handler returns the HTTP handler for the dashboard.
func (d *Dashboard) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", d.requireToken(d.handleIndex))
	mux.HandleFunc("GET /fragment/jobs", d.requireToken(d.handleJobsFragment))
	mux.HandleFunc("GET /fragment/locks", d.requireToken(d.handleLocksFragment))
	if d.enableEnqueue {
		mux.HandleFunc("POST /enqueue", d.requireToken(d.requireCSRF(d.handleEnqueue)))
		mux.HandleFunc("POST /api/jobs", d.requireToken(d.handleEnqueueAPI))
	}
	return mux
}

func (d *Dashboard) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := d.tmpl.ExecuteTemplate(w, "dashboard.html", map[string]any{
		"can_enqueue": d.enableEnqueue,
	}); err != nil {
		d.client.logError(r.Context(), "render dashboard index failed", map[string]any{"error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func (d *Dashboard) handleJobsFragment(w http.ResponseWriter, r *http.Request) {
	jobs, err := d.client.ListJobs(r.Context(), ListJobsParams{Limit: defaultDashboardListLimit})
	if err != nil {
		d.client.logError(r.Context(), "list jobs failed", map[string]any{"error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	rows := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		rows = append(rows, map[string]any{
			"id":           j.ID,
			"queue":        j.QueueName,
			"status":       j.Status,
			"attempts":     fmt.Sprintf("%d/%d", j.Attempts, j.MaxAttempts),
			"available_at": j.AvailableAt.Format(time.RFC3339),
			"claimed_by":   nullStringOrDash(j.ClaimedBy),
			"payload":      payloadPreview(j.Payload),
			"last_error":   nullStringOrDash(j.LastError),
		})
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := d.tmpl.ExecuteTemplate(w, "jobs_fragment", map[string]any{"rows": rows}); err != nil {
		d.client.logError(r.Context(), "render jobs fragment failed", map[string]any{"error": err.Error()})
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
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
		http.Error(w, fmt.Sprintf("invalid form: %v", err), http.StatusBadRequest)
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

	p := EnqueueParams{
		QueueName:   queueName,
		Payload:     payload,
		MaxAttempts: maxAttempts,
	}

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
		http.Error(w, fmt.Sprintf("enqueue failed: %v", err), http.StatusBadRequest)
		return
	}
	d.client.logInfo(r.Context(), "job enqueued from dashboard form", map[string]any{"id": id, "queue": queueName})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, "<div class=\"text-green-700\">enqueued job id %d</div>", id)
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

	d.client.logInfo(r.Context(), "job enqueued from dashboard api", map[string]any{"id": id, "queue": p.QueueName})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
}

// requireToken enforces Basic Auth with constant-time token comparison (P1 #9).
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

// requireCSRF provides simple CSRF mitigation for HTMX POST requests (P1 #9).
// It requires either the HX-Request header (set automatically by HTMX) or
// a matching Origin header. This prevents simple cross-origin form submissions.
func (d *Dashboard) requireCSRF(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// HTMX always sends HX-Request: true
		if r.Header.Get("HX-Request") == "true" {
			next(w, r)
			return
		}

		// Fallback: check Origin header matches Host
		origin := r.Header.Get("Origin")
		if origin != "" {
			// Origin is present; for same-origin requests it should match.
			// We accept if Origin contains the Host.
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

func listAdvisoryLocks(ctx context.Context, db *sql.DB) ([]map[string]any, error) {
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
	defer rows.Close()

	out := make([]map[string]any, 0)
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
		out = append(out, map[string]any{
			"pid":      pid,
			"mode":     mode,
			"granted":  granted,
			"classid":  classID,
			"objid":    objID,
			"objsubid": objSubID,
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
