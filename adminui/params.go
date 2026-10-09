package adminui

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"
)

const maxSearchLength = 256

var (
	errInvalidRange    = errors.New("range must be 1h or 24h")
	errNegativeOffset  = errors.New("offset must not be negative")
	errSearchTooLong   = errors.New("search is too long")
	errInvalidJobID    = errors.New("invalid job id")
	errJobNotFound     = errors.New("job not found")
	errRunNotFound     = errors.New("run not found")
	errNoWorkflows     = errors.New("workflows are not enabled")
	errInvalidBody     = errors.New("invalid json body")
	errQueueRequired   = errors.New("queue name is required")
	errPayloadRequired = errors.New("payload is required")
	errJobBusy         = errors.New("job is currently processing")
	errNotReplayable   = errors.New("job not found or not replayable")
	errForbiddenCSRF   = errors.New("forbidden: missing CSRF token")
)

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type pageRequest struct {
	limit  int
	offset int
}

func (u *UI) pageRequest(r *http.Request) (pageRequest, error) {
	offset := queryInt(r, "offset", 0)
	if offset < 0 {
		return pageRequest{}, errNegativeOffset
	}
	limit := min(max(queryInt(r, "limit", u.listLimit), 1), maxListLimit)
	return pageRequest{limit: limit, offset: offset}, nil
}

func queryText(r *http.Request, key string) string {
	return strings.TrimSpace(r.URL.Query().Get(key))
}

func searchTerm(r *http.Request) (string, error) {
	term := queryText(r, "search")
	if len(term) > maxSearchLength {
		return "", errSearchTooLong
	}
	return term, nil
}

func likePattern(term string) string {
	escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(term)
	return "%" + escaped + "%"
}

func (u *UI) withSnapshot(ctx context.Context, fn func(q querier) error) error {
	tx, err := u.queue.DB().BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return fn(tx)
}

func queryNow(ctx context.Context, q querier) (string, error) {
	var now time.Time
	if err := q.QueryRowContext(ctx, `SELECT NOW()`).Scan(&now); err != nil {
		return "", err
	}
	return formatNow(now), nil
}

func formatNow(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

const timeLayout = "2006-01-02T15:04:05.000Z07:00"

func formatTime(value time.Time) string {
	return value.UTC().Format(timeLayout)
}

func formatNullTime(value sql.NullTime) *string {
	if !value.Valid {
		return nil
	}
	formatted := formatTime(value.Time)
	return &formatted
}

func nullableText(value sql.NullString) *string {
	if !value.Valid || value.String == "" {
		return nil
	}
	text := value.String
	return &text
}
