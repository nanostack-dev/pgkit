package queue

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"
)

// TestIsConnectivityError pins the classifier that decides whether a worker
// poll-loop failure logs at warn (transient DB connectivity/lifecycle) or stays
// at error (a genuine fault). The critical negatives are the schema/logic faults
// that must NOT be downgraded.
func TestIsConnectivityError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},

		// Standard sentinels, including wrapped, must downgrade.
		{"context canceled", context.Canceled, true},
		{"context deadline", context.DeadlineExceeded, true},
		{"wrapped context canceled", fmt.Errorf("pgqueue: begin claim tx: %w", context.Canceled), true},
		{"bad conn", driver.ErrBadConn, true},
		{"conn done", sql.ErrConnDone, true},
		{"eof", io.EOF, true},

		// Wrapped driver errors we can only see as text — the shapes observed in
		// production preview churn.
		{
			"preview database torn down (3D000)",
			errors.New(`pgqueue: begin claim tx: pq: database "pr_update_go_deps_nanostack" does not exist (3D000)`),
			true,
		},
		{
			"connection reset by peer",
			errors.New("pgqueue: begin claim tx: read tcp 172.18.0.15:40200->192.168.2.61:5432: read: connection reset by peer"),
			true,
		},
		{"connection refused", errors.New("pgqueue: begin claim tx: dial tcp: connection refused"), true},
		{"broken pipe", errors.New("pgqueue: commit claim tx: write: broken pipe"), true},
		{"admin shutdown 57P01", errors.New("pq: terminating connection due to administrator command (57P01)"), true},

		// Critical negatives: real faults must keep logging at error.
		{
			"missing relation is schema drift, not connectivity (42P01)",
			errors.New(`pgqueue: claim: pq: relation "pgqueue_jobs" does not exist (42P01)`),
			false,
		},
		{"sql syntax fault", errors.New(`pq: syntax error at or near "SELCT" (42601)`), false},
		{"plain business error", errors.New("pgqueue: job not found or not in expected state"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isConnectivityError(tc.err); got != tc.want {
				t.Fatalf("isConnectivityError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
