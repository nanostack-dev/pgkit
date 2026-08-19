package queue

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
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

		// Server-sent errors carry a SQLSTATE, matched structurally on
		// pgconn.PgError.Code — no substring guessing.
		{
			"preview database torn down (3D000)",
			&pgconn.PgError{Code: "3D000", Message: `database "pr_update_go_deps_nanostack" does not exist`},
			true,
		},
		{"admin shutdown (57P01)", &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}, true},
		{"cannot connect now (57P03)", &pgconn.PgError{Code: "57P03", Message: "the database system is shutting down"}, true},
		{"connection failure (08006)", &pgconn.PgError{Code: "08006"}, true},
		{"connection does not exist (08003)", &pgconn.PgError{Code: "08003"}, true},
		{
			"wrapped pg error is still found through errors.As",
			fmt.Errorf("pgqueue: begin claim tx: %w", &pgconn.PgError{Code: "3D000", Message: `database "preview_42" does not exist`}),
			true,
		},

		// lib/pq is the other mainstream driver (anchor wires it up). Its *pq.Error
		// exposes the SQLSTATE structurally via SQLState() just like pgx, so a
		// torn-down preview database is downgraded regardless of driver, and a
		// wrapped one is still found through errors.As.
		{
			"lib/pq preview database torn down (3D000)",
			&pq.Error{Code: "3D000", Message: `database "pr_organization_create_with_licen_nanostack" does not exist`},
			true,
		},
		{
			"wrapped lib/pq pg error is still found through errors.As",
			fmt.Errorf("pgqueue: begin claim tx: %w", &pq.Error{Code: "57P01", Message: "terminating connection due to administrator command"}),
			true,
		},
		{
			"lib/pq missing relation is schema drift, not connectivity (42P01)",
			&pq.Error{Code: "42P01", Message: `relation "pgqueue_jobs" does not exist`},
			false,
		},

		// Network and driver failures that carry no SQLSTATE, and the fallback for
		// callers that wired up a non-pgx driver. Text is all there is.
		{
			"connection reset by peer",
			errors.New("pgqueue: begin claim tx: read tcp 172.18.0.15:40200->192.168.2.61:5432: read: connection reset by peer"),
			true,
		},
		{"connection refused", errors.New("pgqueue: begin claim tx: dial tcp: connection refused"), true},
		{"broken pipe", errors.New("pgqueue: commit claim tx: write: broken pipe"), true},

		// Critical negatives: real faults must keep logging at error.
		{
			"missing relation is schema drift, not connectivity (42P01)",
			errors.New(`pgqueue: claim: pq: relation "pgqueue_jobs" does not exist (42P01)`),
			false,
		},
		{
			"missing relation as a pg error is still schema drift (42P01)",
			&pgconn.PgError{Code: "42P01", Message: `relation "pgqueue_jobs" does not exist`},
			false,
		},
		{"sql syntax fault", errors.New(`pq: syntax error at or near "SELCT" (42601)`), false},
		{"sql syntax fault as a pg error", &pgconn.PgError{Code: "42601", Message: `syntax error at or near "SELCT"`}, false},
		{"plain business error", errors.New("pgqueue: job not found or not in expected state"), false},

		// A SQLSTATE reachable only as message text — a bare string with no driver
		// error type behind it — is not downgraded. That is the deliberate trade of
		// matching codes structurally: real driver errors (pgx, lib/pq) expose
		// SQLState() and are covered by the branch above, while the fragment list
		// keeps no raw codes to collide with unrelated text.
		{"bare sqlstate in text is not matched", errors.New("pq: terminating connection due to administrator command (57P01)"), false},
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
