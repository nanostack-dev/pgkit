package pglock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"time"
)

var ErrNilDB = errors.New("pglock: db is nil")

const keyNamespace = "nanostack.dev/pgkit/pglock:"

const unlockTimeout = 3 * time.Second

type Client struct {
	db  *sql.DB
	log Logger
}

// Logger is a minimal logging adapter for custom logging tools.
type Logger interface {
	Debug(ctx context.Context, msg string, fields map[string]any)
	Info(ctx context.Context, msg string, fields map[string]any)
	Warn(ctx context.Context, msg string, fields map[string]any)
	Error(ctx context.Context, msg string, fields map[string]any)
}

type noopLogger struct{}

func (noopLogger) Debug(context.Context, string, map[string]any) {}
func (noopLogger) Info(context.Context, string, map[string]any)  {}
func (noopLogger) Warn(context.Context, string, map[string]any)  {}
func (noopLogger) Error(context.Context, string, map[string]any) {}

func New(db *sql.DB) (*Client, error) {
	if db == nil {
		return nil, ErrNilDB
	}
	return &Client{db: db, log: noopLogger{}}, nil
}

// SetLogger sets a custom logger adapter.
func (c *Client) SetLogger(logger Logger) {
	if c == nil {
		return
	}
	if logger == nil {
		c.log = noopLogger{}
		return
	}
	c.log = logger
}

func (c *Client) TryWithLock(
	ctx context.Context,
	key string,
	fn func(context.Context, *sql.Tx) error,
) (acquired bool, err error) {
	if c == nil || c.db == nil {
		return false, ErrNilDB
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("pglock: begin tx: %w", err)
	}

	locked, err := tryAdvisoryXactLock(ctx, tx, KeyHash(key))
	if err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("pglock: acquire xact lock: %w", err)
	}

	if !locked {
		_ = tx.Rollback()
		c.log.Debug(ctx, "transaction lock busy", map[string]any{"key": key})
		return false, nil
	}
	c.log.Debug(ctx, "transaction lock acquired", map[string]any{"key": key})

	if fn != nil {
		if err := fn(ctx, tx); err != nil {
			_ = tx.Rollback()
			return true, err
		}
	}

	if err := tx.Commit(); err != nil {
		return true, fmt.Errorf("pglock: commit tx: %w", err)
	}
	c.log.Debug(ctx, "transaction lock released", map[string]any{"key": key})

	return true, nil
}

func (c *Client) TryWithSessionLock(
	ctx context.Context,
	key string,
	fn func(context.Context) error,
) (acquired bool, err error) {
	if c == nil || c.db == nil {
		return false, ErrNilDB
	}

	conn, err := c.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("pglock: get conn: %w", err)
	}
	defer conn.Close()

	lockID := KeyHash(key)
	var locked bool
	err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&locked)
	if err != nil {
		return false, fmt.Errorf("pglock: acquire session lock: %w", err)
	}
	if !locked {
		c.log.Debug(ctx, "session lock busy", map[string]any{"key": key})
		return false, nil
	}
	c.log.Debug(ctx, "session lock acquired", map[string]any{"key": key})

	var fnErr error
	if fn != nil {
		fnErr = fn(ctx)
	}

	unlockErr := unlockSessionLock(conn, lockID)
	if fnErr != nil || unlockErr != nil {
		return true, errors.Join(fnErr, unlockErr)
	}

	c.log.Debug(ctx, "session lock released", map[string]any{"key": key})

	return true, nil
}

func tryAdvisoryXactLock(ctx context.Context, tx *sql.Tx, key int64) (bool, error) {
	var locked bool
	err := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", key).Scan(&locked)
	if err != nil {
		return false, err
	}
	return locked, nil
}

func unlockSessionLock(conn *sql.Conn, key int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), unlockTimeout)
	defer cancel()

	var unlocked bool
	err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", key).Scan(&unlocked)
	if err != nil {
		return fmt.Errorf("pglock: unlock session lock: %w", err)
	}
	if !unlocked {
		return errors.New("pglock: session lock was not held")
	}

	return nil
}

func KeyHash(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(keyNamespace))
	_, _ = h.Write([]byte(key))
	return int64(h.Sum64())
}
