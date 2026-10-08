package queue

import (
	"context"
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
// to 200 characters, which keeps it under the 8000-byte NOTIFY limit.
const NotifyChannel = "pgqueue_jobs"

const (
	notifyKeyLength     = 200
	listenRetryDelay    = time.Second
	listenerCloseBudget = time.Second
)

var ErrNotificationsUnsupported = errors.New(
	"pgqueue: OnEnqueue pickup needs the pgx stdlib driver, Client.ListenOn or Client.ListenWith")

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

// ListenOn makes OnEnqueue workers listen on connections opened from connString
// (URL or key=value DSN). Call it before building workers when db does not use the
// pgx stdlib driver, lib/pq included.
func (c *Client) ListenOn(connString string) error {
	config, err := pgx.ParseConfig(connString)
	if err != nil {
		return fmt.Errorf("pgqueue: listener connection string: %w", err)
	}
	c.ListenWith(func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.ConnectConfig(ctx, config.Copy())
	})
	return nil
}

// ListenWith makes OnEnqueue workers listen on connections from connect, called
// on every (re)connect. Use it when credentials rotate or a connection needs setup
// that a connection string cannot express.
func (c *Client) ListenWith(connect func(context.Context) (*pgx.Conn, error)) {
	c.notifier.mu.Lock()
	defer c.notifier.mu.Unlock()
	c.notifier.connect = connect
}

func (c *Client) supportsNotifications() bool {
	c.notifier.mu.Lock()
	configured := c.notifier.connect != nil
	c.notifier.mu.Unlock()
	_, pgxPool := c.db.Driver().(*stdlib.Driver)
	return configured || pgxPool
}

// notifier shares one LISTEN connection between the subscribed workers of a
// Client, while at least one is subscribed. Each listener run has a generation,
// so a run that is shutting down cannot overwrite the state of its successor.
type notifier struct {
	client *Client

	mu         sync.Mutex
	connect    func(context.Context) (*pgx.Conn, error)
	subs       map[*subscription]struct{}
	generation uint64
	listening  bool
	stop       context.CancelFunc
	stopped    chan struct{}

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
		n.generation++
		ctx, stop := context.WithCancel(context.Background())
		n.stop, n.stopped = stop, make(chan struct{})
		go n.run(ctx, n.generation, n.stopped)
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
	n.generation++
	n.mu.Unlock()

	stop()
	<-stopped
}

func (n *notifier) run(ctx context.Context, generation uint64, stopped chan<- struct{}) {
	defer close(stopped)
	for {
		err := n.listen(ctx, generation)
		n.setListening(generation, false)
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

func (n *notifier) listen(ctx context.Context, generation uint64) error {
	if n.beforeConnect != nil {
		n.beforeConnect()
	}
	conn, err := n.open(ctx)
	if err != nil {
		return fmt.Errorf("pgqueue: open listener connection: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), listenerCloseBudget)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return fmt.Errorf("pgqueue: listen: %w", err)
	}
	n.setListening(generation, true)
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("pgqueue: wait for notification: %w", err)
		}
		n.dispatch(generation, notification)
	}
}

// open dials outside the *sql.DB pool, so listening never holds a slot the
// application needs.
func (n *notifier) open(ctx context.Context) (*pgx.Conn, error) {
	n.mu.Lock()
	connect := n.connect
	n.mu.Unlock()
	if connect != nil {
		return connect(ctx)
	}

	config, err := n.poolConfig(ctx)
	if err != nil {
		return nil, err
	}
	config.OnNotification = nil
	return pgx.ConnectConfig(ctx, config)
}

func (n *notifier) poolConfig(ctx context.Context) (*pgx.ConnConfig, error) {
	pooled, err := n.client.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = pooled.Close() }()

	var config *pgx.ConnConfig
	err = pooled.Raw(func(driverConn any) error {
		stdConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return ErrNotificationsUnsupported
		}
		config = stdConn.Conn().Config()
		return nil
	})
	return config, err
}

// setListening wakes every subscriber when listening starts: a job enqueued before
// LISTEN took effect sent a notification this connection never sees.
func (n *notifier) setListening(generation uint64, listening bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if generation != n.generation {
		return
	}
	n.listening = listening
	for sub := range n.subs {
		sub.listening.Store(listening)
		if listening {
			signal(sub.wake)
		}
	}
}

// dispatch wakes every subscriber on a nil notification, which a connection with
// its own OnNotification handler returns.
func (n *notifier) dispatch(generation uint64, notification *pgconn.Notification) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if generation != n.generation {
		return
	}
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
