package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"
)

var ErrInvalidMaintenanceConfig = errors.New("pgqueue: invalid maintenance config")

const (
	defaultScheduleInterval = time.Second
	defaultRescueInterval   = 30 * time.Second
	defaultRescueAfter      = time.Hour
	defaultMaintenanceBatch = 1000

	rescueReason = "rescued: lease expired"

	// schemaCompletionRetry is how often a leader retries CompleteSchema after a
	// failure, or while another client is completing the schema.
	schemaCompletionRetry = time.Minute
)

// MaintenanceConfig tunes the services that the elected leader among a database's
// workers runs. Zero values take the defaults.
type MaintenanceConfig struct {
	// ScheduleInterval is how often due scheduled and retryable jobs become
	// available and their queues are notified. Default one second.
	ScheduleInterval time.Duration
	// RescueInterval is how often running jobs whose lease expired are taken
	// back. Default 30 seconds.
	RescueInterval time.Duration
	// RescueAfter is how long a running job claimed without a lease (Client.Claim,
	// or a binary older than job states) may run before it is taken back.
	// Default one hour.
	RescueAfter time.Duration
	// BatchSize is the number of jobs each service changes per statement.
	// Default 1000.
	BatchSize int
	// Retention, when set, purges finalized jobs on the leader only. Nil keeps
	// them; RunRetention remains available.
	Retention *RetentionPolicy
}

func (c MaintenanceConfig) withDefaults() MaintenanceConfig {
	if c.ScheduleInterval == 0 {
		c.ScheduleInterval = defaultScheduleInterval
	}
	if c.RescueInterval == 0 {
		c.RescueInterval = defaultRescueInterval
	}
	if c.RescueAfter == 0 {
		c.RescueAfter = defaultRescueAfter
	}
	if c.BatchSize == 0 {
		c.BatchSize = defaultMaintenanceBatch
	}
	if c.Retention != nil {
		retention := c.Retention.withDefaults()
		c.Retention = &retention
	}
	return c
}

func (c MaintenanceConfig) validate() error {
	if c.ScheduleInterval <= 0 || c.RescueInterval <= 0 || c.RescueAfter <= 0 || c.BatchSize <= 0 {
		return ErrInvalidMaintenanceConfig
	}
	if c.Retention != nil {
		return c.Retention.validate()
	}
	return nil
}

// SetMaintenance configures the maintenance services this client runs while it
// is the leader. It applies from the next leadership term.
func (c *Client) SetMaintenance(config MaintenanceConfig) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return err
	}
	c.maintainer.mu.Lock()
	defer c.maintainer.mu.Unlock()
	c.maintainer.config = config
	return nil
}

// RunMaintenance takes part in the leader election until ctx ends, without
// running a worker. Workers take part on their own; use it in a process that
// claims with Client.Claim or only enqueues.
func (c *Client) RunMaintenance(ctx context.Context) error {
	if c == nil || c.db == nil {
		return ErrNilDB
	}
	leave := c.maintainer.join(nil)
	defer leave()
	<-ctx.Done()
	return nil
}

// IsLeader reports whether this client currently runs the maintenance services.
func (c *Client) IsLeader() bool {
	return c != nil && c.maintainer.leading()
}

// maintainer runs one elector per Client while at least one worker or
// RunMaintenance call has joined, and the maintenance services while it leads.
type maintainer struct {
	client *Client

	mu         sync.Mutex
	config     MaintenanceConfig
	workers    map[*Worker]int
	members    int
	elector    *elector
	stop       context.CancelFunc
	stopped    chan struct{}
	electEvery time.Duration
	ttl        time.Duration
}

func newMaintainer(client *Client) *maintainer {
	return &maintainer{
		client:     client,
		config:     MaintenanceConfig{}.withDefaults(),
		workers:    map[*Worker]int{},
		electEvery: defaultElectInterval,
		ttl:        defaultLeaderTTL,
	}
}

// join adds a member, starting the elector for the first one. The returned leave
// removes it; the last leave stops the elector and resigns its term.
func (m *maintainer) join(worker *Worker) (leave func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.members++
	if worker != nil {
		m.workers[worker]++
	}
	if m.stop == nil {
		e := newElector(sqlLeaderStore{db: m.client.db, name: maintenanceLeadership}, newLeaderID())
		e.electEvery, e.ttl = m.electEvery, m.ttl
		e.onError = func(ctx context.Context, msg string, err error) {
			m.client.logFailure(ctx, msg, map[string]any{"error": err.Error()}, err)
		}
		ctx, stop := context.WithCancel(context.Background())
		stopped := make(chan struct{})
		m.elector, m.stop, m.stopped = e, stop, stopped
		go func() {
			defer close(stopped)
			e.run(ctx, m.lead)
		}()
	}
	var once sync.Once
	return func() { once.Do(func() { m.leave(worker) }) }
}

func (m *maintainer) leave(worker *Worker) {
	m.mu.Lock()
	m.members--
	if worker != nil {
		if m.workers[worker]--; m.workers[worker] <= 0 {
			delete(m.workers, worker)
		}
	}
	if m.members > 0 {
		m.mu.Unlock()
		return
	}
	stop, stopped := m.stop, m.stopped
	m.elector, m.stop, m.stopped = nil, nil, nil
	m.mu.Unlock()

	stop()
	<-stopped
}

func (m *maintainer) leading() bool {
	m.mu.Lock()
	e := m.elector
	m.mu.Unlock()
	return e != nil && e.leading()
}

func (m *maintainer) snapshot() (MaintenanceConfig, []*Worker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	workers := make([]*Worker, 0, len(m.workers))
	for w := range m.workers {
		workers = append(workers, w)
	}
	return m.config, workers
}

// lead runs every maintenance service until ctx ends with the term.
func (m *maintainer) lead(ctx context.Context) {
	config, _ := m.snapshot()
	m.client.logInfo(ctx, "queue maintenance leader elected", nil)
	var wg sync.WaitGroup
	every := func(interval time.Duration, pass func(context.Context) error, failure string) {
		wg.Go(func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				if err := pass(ctx); err != nil && ctx.Err() == nil {
					m.client.logFailure(ctx, failure, map[string]any{"error": err.Error()}, err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		})
	}
	every(config.ScheduleInterval, func(ctx context.Context) error {
		_, err := m.client.scheduleDue(ctx, config.BatchSize)
		return err
	}, "queue scheduler pass failed")
	every(config.RescueInterval, func(ctx context.Context) error {
		return m.rescue(ctx, config)
	}, "queue rescuer pass failed")
	wg.Go(func() { m.completeSchema(ctx) })
	if config.Retention != nil {
		policy := *config.Retention
		every(policy.Interval, func(ctx context.Context) error {
			_, err := m.client.PurgeFinished(ctx, policy)
			return err
		}, "queue retention purge failed")
	}
	wg.Wait()
}

// completeSchema runs CompleteSchema on the leader until the schema is complete
// or the term ends. An index build that the end of a term interrupts starts again
// under the next leader; backfilled rows and finished steps are kept.
func (m *maintainer) completeSchema(ctx context.Context) {
	ticker := time.NewTicker(schemaCompletionRetry)
	defer ticker.Stop()
	for {
		done, err := m.client.completeSchemaOnLeader(ctx)
		if err != nil && ctx.Err() == nil {
			m.client.logFailure(ctx, "queue schema completion failed", map[string]any{"error": err.Error()}, err)
		}
		if done {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *maintainer) rescue(ctx context.Context, config MaintenanceConfig) error {
	jobs, err := m.client.rescueExpired(ctx, config.RescueAfter, config.BatchSize)
	if len(jobs) > 0 {
		result := countRescued(jobs)
		m.client.logWarn(ctx, "queue rescued stuck jobs", map[string]any{
			"requeued": result.Requeued, "failed": result.Failed, "cancelled": result.Cancelled,
		})
		_, workers := m.snapshot()
		for _, w := range workers {
			w.reportStuck(ctx, jobs)
		}
	}
	return err
}

// scheduleDue makes due scheduled and retryable jobs available in batches and
// notifies their queues, so OnEnqueue workers take them without a rescan. It
// returns how many jobs it made available. Like the claim, it repeats the state
// filter on status so a legacy index serves it until CompleteSchema finishes.
func (c *Client) scheduleDue(ctx context.Context, batchSize int) (int64, error) {
	var total int64
	for ctx.Err() == nil {
		var promoted int64
		var notified int
		err := c.db.QueryRowContext(ctx, `
WITH due AS (
    SELECT id FROM pgqueue_jobs
    WHERE state IN ('scheduled', 'retryable') AND status = 'pending' AND available_at <= NOW()
    ORDER BY available_at, id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
), promoted AS (
    UPDATE pgqueue_jobs j
    SET state = 'available', updated_at = NOW()
    FROM due
    WHERE j.id = due.id
    RETURNING j.queue_name
), notified AS (
    SELECT `+notifySQL("queues")+` FROM (SELECT DISTINCT queue_name FROM promoted) queues
)
SELECT (SELECT count(*) FROM promoted), (SELECT count(*) FROM notified)`, batchSize).Scan(&promoted, &notified)
		if err != nil {
			return total, fmt.Errorf("pgqueue: schedule due jobs: %w", err)
		}
		total += promoted
		if promoted < int64(batchSize) {
			return total, nil
		}
	}
	return total, ctx.Err()
}

// expiredClaimSQL matches running jobs whose lease expired, and running jobs
// without a lease claimed more than $1 seconds ago. The redundant status
// predicate lets the legacy partial index serve the scan until CompleteSchema has
// built idx_pgqueue_running.
const expiredClaimSQL = `state = 'running' AND status = 'processing'
	AND (lease_expires_at < NOW()
	     OR (lease_expires_at IS NULL AND claimed_at < NOW() - make_interval(secs => $1::float8)))`

// rescueExpired takes back running jobs whose lease expired, and running jobs
// without a lease claimed more than rescueAfter ago, in batches.
func (c *Client) rescueExpired(ctx context.Context, rescueAfter time.Duration, batchSize int) ([]rescuedJob, error) {
	var all []rescuedJob
	for ctx.Err() == nil {
		jobs, err := c.rescueJobs(ctx, expiredClaimSQL, batchSize, rescueReason, rescueAfter.Seconds())
		all = append(all, jobs...)
		if err != nil {
			return all, fmt.Errorf("pgqueue: rescue stuck jobs: %w", err)
		}
		if len(jobs) < batchSize {
			break
		}
	}
	if len(all) > 0 {
		result := countRescued(all)
		c.emit(ctx, EventReap, map[string]any{
			"requeued": result.Requeued, "failed": result.Failed, "cancelled": result.Cancelled, "rescue": true,
		})
	}
	return all, ctx.Err()
}

// reportStuck calls OnJobStuck with the rescued jobs of the worker's queues.
func (w *Worker) reportStuck(ctx context.Context, jobs []rescuedJob) {
	if w.cfg.OnJobStuck == nil {
		return
	}
	queues := w.registry.queueNames()
	var mine []rescuedJob
	for _, job := range jobs {
		if slices.Contains(queues, job.queueName) {
			mine = append(mine, job)
		}
	}
	if len(mine) > 0 {
		w.cfg.OnJobStuck(ctx, countRescued(mine))
	}
}

func newLeaderID() string {
	host, _ := os.Hostname()
	suffix := make([]byte, 6)
	_, _ = rand.Read(suffix)
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(suffix))
}
