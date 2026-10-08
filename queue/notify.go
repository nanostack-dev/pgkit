package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// NotifyChannel receives a notification each time a job may have become claimable:
// enqueued, replayed or requeued by the reaper. The payload is the queue name cut
// to notifyKeyLength characters, which keeps it under the 8000-byte NOTIFY limit.
const NotifyChannel = "pgqueue_jobs"

const (
	notifyKeyLength     = 200
	listenRetryDelay    = time.Second
	listenerCloseBudget = time.Second
)

// notifySQL notifies for every row of the named CTE; it must agree with notifyKey.
func notifySQL(cte string) string {
	return fmt.Sprintf("pg_notify('%s', left(%s.queue_name, %d))", NotifyChannel, cte, notifyKeyLength)
}

func notifyKey(queueName string) string {
	runes := []rune(queueName)
	if len(runes) <= notifyKeyLength {
		return queueName
	}
	return string(runes[:notifyKeyLength])
}

var ErrNotificationsUnsupported = errors.New("pgqueue: OnEnqueue pickup needs the pgx stdlib driver")

func supportsNotifications(db *sql.DB) bool {
	_, ok := db.Driver().(*stdlib.Driver)
	return ok
}

// notifier shares one LISTEN connection between all subscribed workers of a
// Client. It runs while at least one worker is subscribed.
type notifier struct {
	client *Client

	mu        sync.Mutex
	subs      map[*subscription]struct{}
	listening bool
	stop      context.CancelFunc
	stopped   chan struct{}

	beforeConnect func()
}

type subscription struct {
	keys      map[string]struct{}
	wake      chan struct{}
	listening atomic.Bool
}

func newNotifier(client *Client) *notifier {
	return &notifier{client: client, subs: map[*subscription]struct{}{}}
}

func (n *notifier) subscribe(queueNames []string) *subscription {
	sub := &subscription{keys: map[string]struct{}{}, wake: make(chan struct{}, 1)}
	for _, name := range queueNames {
		sub.keys[notifyKey(name)] = struct{}{}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.subs[sub] = struct{}{}
	if n.stop == nil {
		ctx, stop := context.WithCancel(context.Background())
		n.stop, n.stopped = stop, make(chan struct{})
		go n.run(ctx, n.stopped)
	} else if n.listening {
		sub.listening.Store(true)
		signal(sub.wake)
	}
	return sub
}

func (n *notifier) unsubscribe(sub *subscription) {
	n.mu.Lock()
	delete(n.subs, sub)
	if len(n.subs) > 0 {
		n.mu.Unlock()
		return
	}
	stop, stopped := n.stop, n.stopped
	n.stop, n.stopped, n.listening = nil, nil, false
	n.mu.Unlock()

	stop()
	<-stopped
}

func (n *notifier) run(ctx context.Context, stopped chan<- struct{}) {
	defer close(stopped)
	for {
		err := n.listen(ctx)
		n.setListening(false)
		if ctx.Err() != nil {
			return
		}
		n.client.logFailure(ctx, "queue listener failed", map[string]any{"error": err.Error()}, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(listenRetryDelay):
		}
	}
}

func (n *notifier) listen(ctx context.Context) error {
	if n.beforeConnect != nil {
		n.beforeConnect()
	}
	conn, err := n.connect(ctx)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), listenerCloseBudget)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return fmt.Errorf("pgqueue: listen: %w", err)
	}
	n.setListening(true)
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("pgqueue: wait for notification: %w", err)
		}
		n.dispatch(notification)
	}
}

// connect opens a connection outside the *sql.DB pool, configured like the pool's
// own connections, so listening never takes a slot from the application.
func (n *notifier) connect(ctx context.Context) (*pgx.Conn, error) {
	pooled, err := n.client.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: read connection config: %w", err)
	}
	defer func() { _ = pooled.Close() }()

	var config *pgx.ConnConfig
	if err := pooled.Raw(func(driverConn any) error {
		stdConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return ErrNotificationsUnsupported
		}
		config = stdConn.Conn().Config()
		return nil
	}); err != nil {
		return nil, err
	}
	config.OnNotification = nil

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("pgqueue: open listener connection: %w", err)
	}
	return conn, nil
}

// setListening wakes every subscriber when listening starts: anything enqueued
// before LISTEN took effect produced no notification this connection will see.
func (n *notifier) setListening(listening bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.listening = listening
	for sub := range n.subs {
		sub.listening.Store(listening)
		if listening {
			signal(sub.wake)
		}
	}
}

func (n *notifier) dispatch(notification *pgconn.Notification) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for sub := range n.subs {
		if notification == nil {
			signal(sub.wake)
			continue
		}
		if _, ok := sub.keys[notification.Payload]; ok {
			signal(sub.wake)
		}
	}
}

func signal(wake chan<- struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}
