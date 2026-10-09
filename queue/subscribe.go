package queue

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

// Subscription wakes when NotifyTx commits a notification for one of its keys, and
// when the shared listener (re)connects, since notifications sent while it was down
// are lost. A wake is a hint: re-read the state it announces.
type Subscription struct {
	notifier  *notifier
	sub       *subscription
	closeOnce sync.Once
}

// Subscribe listens for keys on the Client's shared LISTEN connection, the one
// OnEnqueue workers use. It needs the pgx stdlib driver, ListenOn or ListenWith.
func (c *Client) Subscribe(keys ...string) (*Subscription, error) {
	if c == nil || c.db == nil {
		return nil, ErrNilDB
	}
	if !c.supportsNotifications() {
		return nil, ErrNotificationsUnsupported
	}
	return &Subscription{notifier: c.notifier, sub: c.notifier.subscribe(keys)}, nil
}

// Wake receives a value after one or more notifications; several collapse into one.
// The channel is never closed, not even by Close: select on it with your own
// cancellation instead of ranging over it.
func (s *Subscription) Wake() <-chan struct{} {
	return s.sub.wake
}

// Close stops the subscription. The listener connection closes with the last one.
func (s *Subscription) Close() {
	s.closeOnce.Do(func() { s.notifier.unsubscribe(s.sub) })
}

// NotifyTx wakes the subscriptions for key when tx commits. Keys are cut to 200
// characters, like queue names.
func (c *Client) NotifyTx(ctx context.Context, tx *sql.Tx, key string) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	if tx == nil {
		return fmt.Errorf("pgqueue: tx is nil")
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, notifyKey(key)); err != nil {
		return fmt.Errorf("pgqueue: notify: %w", err)
	}
	return nil
}
