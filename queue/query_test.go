package queue

import (
	"strings"
	"testing"
)

// The job column list is the contract between every SELECT/RETURNING clause and
// scanJob. Order matters: scanJob assigns positionally.
func TestJobColumnLists(t *testing.T) {
	const want = "id, queue_name, payload, status, attempts, max_attempts, available_at, " +
		"claimed_by, claimed_at, done_at, last_error, created_at, updated_at"

	if jobSelectColumns != want {
		t.Errorf("jobSelectColumns:\n got %q\nwant %q", jobSelectColumns, want)
	}

	// Claim's RETURNING list joins a CTE that also exposes an id column, so every
	// name must stay alias-qualified.
	for _, column := range jobColumns {
		if !strings.Contains(jobReturningColumns, "j."+column) {
			t.Errorf("jobReturningColumns missing qualified column %q: %s", column, jobReturningColumns)
		}
	}
	if strings.Count(jobReturningColumns, "j.") != len(jobColumns) {
		t.Errorf("jobReturningColumns should qualify all %d columns: %s", len(jobColumns), jobReturningColumns)
	}
}

func TestBuildJobFilterClause(t *testing.T) {
	tests := []struct {
		name       string
		params     ListJobsParams
		wantClause string
		wantArgs   []any
	}{
		{
			name:       "no filters",
			params:     ListJobsParams{Limit: 10},
			wantClause: "",
			wantArgs:   []any{},
		},
		{
			name:       "queue only",
			params:     ListJobsParams{QueueName: "emails"},
			wantClause: " WHERE queue_name = $1",
			wantArgs:   []any{"emails"},
		},
		{
			name:       "status only",
			params:     ListJobsParams{Status: StatusFailed},
			wantClause: " WHERE status = $1",
			wantArgs:   []any{"failed"},
		},
		{
			name:   "search only reuses one placeholder",
			params: ListJobsParams{Search: "boom"},
			wantClause: " WHERE (queue_name ILIKE $1 OR encode(payload, 'escape') ILIKE $1 " +
				"OR COALESCE(last_error, '') ILIKE $1)",
			wantArgs: []any{"%boom%"},
		},
		{
			name:       "queue and status number sequentially",
			params:     ListJobsParams{QueueName: "emails", Status: StatusPending},
			wantClause: " WHERE queue_name = $1 AND status = $2",
			wantArgs:   []any{"emails", "pending"},
		},
		{
			name:   "all three filters",
			params: ListJobsParams{QueueName: "emails", Status: StatusFailed, Search: "boom"},
			wantClause: " WHERE queue_name = $1 AND status = $2 AND " +
				"(queue_name ILIKE $3 OR encode(payload, 'escape') ILIKE $3 OR COALESCE(last_error, '') ILIKE $3)",
			wantArgs: []any{"emails", "failed", "%boom%"},
		},
		{
			name:       "search is trimmed and wrapped",
			params:     ListJobsParams{Search: "  boom  "},
			wantClause: " WHERE (queue_name ILIKE $1 OR encode(payload, 'escape') ILIKE $1 OR COALESCE(last_error, '') ILIKE $1)",
			wantArgs:   []any{"%boom%"},
		},
		{
			name:       "whitespace-only search is not a filter",
			params:     ListJobsParams{Search: "   "},
			wantClause: "",
			wantArgs:   []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, args := buildJobFilterClause(tt.params)
			if clause != tt.wantClause {
				t.Errorf("clause:\n got %q\nwant %q", clause, tt.wantClause)
			}
			if len(args) != len(tt.wantArgs) {
				t.Fatalf("args: got %v, want %v", args, tt.wantArgs)
			}
			for i := range args {
				if args[i] != tt.wantArgs[i] {
					t.Errorf("args[%d]: got %v, want %v", i, args[i], tt.wantArgs[i])
				}
			}
		})
	}
}

// ListJobs appends LIMIT (and optionally OFFSET) after the filter args, so the
// placeholder it picks must continue the filter's numbering rather than restart it.
func TestJobFilterArgCountDrivesPagingPlaceholders(t *testing.T) {
	tests := []struct {
		name          string
		params        ListJobsParams
		wantLimitArg  int
		wantOffsetArg int
	}{
		{name: "no filters", params: ListJobsParams{}, wantLimitArg: 1, wantOffsetArg: 2},
		{name: "one filter", params: ListJobsParams{QueueName: "emails"}, wantLimitArg: 2, wantOffsetArg: 3},
		{
			name:          "three filters",
			params:        ListJobsParams{QueueName: "emails", Status: StatusFailed, Search: "boom"},
			wantLimitArg:  4,
			wantOffsetArg: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, args := buildJobFilterClause(tt.params)
			if got := len(args) + 1; got != tt.wantLimitArg {
				t.Errorf("LIMIT placeholder: got $%d, want $%d", got, tt.wantLimitArg)
			}
			args = append(args, 10) // ListJobs appends the limit before computing OFFSET
			if got := len(args) + 1; got != tt.wantOffsetArg {
				t.Errorf("OFFSET placeholder: got $%d, want $%d", got, tt.wantOffsetArg)
			}
		})
	}
}
