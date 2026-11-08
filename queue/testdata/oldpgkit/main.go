// Command oldpgkit runs the released pgkit queue (the version go.mod pins) against
// a database, one command per line, so queue/compat_test.go can prove that the
// binaries consumers run today keep working next to the current code.
//
// Usage: oldpgkit <dsn>. Each stdin line is a JSON command; each stdout line is
// its JSON reply. Workers run in the background until worker_stop or EOF.
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nanostack-dev/pgkit/queue"
)

type command struct {
	Op           string   `json:"op"`
	Queue        string   `json:"queue"`
	Queues       []string `json:"queues"`
	Status       string   `json:"status"`
	ID           int64    `json:"id"`
	Claims       int      `json:"claims"`
	DelayMS      int64    `json:"delay_ms"`
	MaxAttempts  int      `json:"max_attempts"`
	VisibilityMS int64    `json:"visibility_ms"`
	OlderThanMS  int64    `json:"older_than_ms"`
	KeepDoneMS   int64    `json:"keep_done_ms"`
	KeepFailedMS int64    `json:"keep_failed_ms"`
	TimeoutMS    int64    `json:"timeout_ms"`
	Name         string   `json:"name"`
	FailFirst    bool     `json:"fail_first"`
}

type jobReply struct {
	ID          int64     `json:"id"`
	Queue       string    `json:"queue"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	Claims      int       `json:"claims"`
	AvailableAt time.Time `json:"available_at"`
}

type reply struct {
	OK       bool       `json:"ok"`
	Error    string     `json:"error,omitempty"`
	NotFound bool       `json:"not_found,omitempty"`
	Busy     bool       `json:"busy,omitempty"`
	ID       int64      `json:"id,omitempty"`
	Found    bool       `json:"found,omitempty"`
	Job      *jobReply  `json:"job,omitempty"`
	Jobs     []jobReply `json:"jobs,omitempty"`
	Count    int64      `json:"count,omitempty"`
	Requeued int64      `json:"requeued,omitempty"`
	Failed   int64      `json:"failed,omitempty"`
	Done     int64      `json:"done,omitempty"`
	Deleted  int64      `json:"deleted,omitempty"`
}

type runningWorker struct {
	cancel context.CancelFunc
	done   chan error
}

type driver struct {
	db      *sql.DB
	client  *queue.Client
	workers map[string]*runningWorker
	mu      sync.Mutex
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: oldpgkit <dsn>")
		os.Exit(2)
	}
	db, err := sql.Open("pgx", os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	client, err := queue.New(db)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	d := &driver{db: db, client: client, workers: map[string]*runningWorker{}}
	out := json.NewEncoder(os.Stdout)
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<20)
	for in.Scan() {
		var cmd command
		if err := json.Unmarshal(in.Bytes(), &cmd); err != nil {
			_ = out.Encode(reply{Error: err.Error()})
			continue
		}
		r, err := d.run(cmd)
		if err != nil {
			r = reply{
				Error:    err.Error(),
				NotFound: errors.Is(err, queue.ErrJobNotFound),
				Busy:     errors.Is(err, queue.ErrJobBusy),
			}
		} else {
			r.OK = true
		}
		_ = out.Encode(r)
	}
	d.mu.Lock()
	for _, w := range d.workers {
		w.cancel()
		<-w.done
	}
	d.mu.Unlock()
	_ = db.Close()
}

func (d *driver) run(cmd command) (reply, error) {
	timeout := 30 * time.Second
	if cmd.TimeoutMS > 0 {
		timeout = time.Duration(cmd.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c := d.client
	switch cmd.Op {
	case "ensure_schema":
		return reply{}, c.EnsureSchema(ctx)
	case "enqueue":
		p := queue.EnqueueParams{QueueName: cmd.Queue, Payload: []byte(`{}`), MaxAttempts: cmd.MaxAttempts}
		if cmd.DelayMS != 0 {
			var now time.Time
			if err := d.db.QueryRowContext(ctx, `SELECT NOW()`).Scan(&now); err != nil {
				return reply{}, err
			}
			at := now.Add(time.Duration(cmd.DelayMS) * time.Millisecond)
			p.AvailableAt = &at
		}
		id, err := c.Enqueue(ctx, p)
		return reply{ID: id}, err
	case "claim":
		job, found, err := c.Claim(ctx, cmd.Queue, "old-binary")
		if err != nil || !found {
			return reply{}, err
		}
		return reply{Found: true, Job: toReply(*job)}, nil
	case "ack":
		return reply{}, c.Ack(ctx, cmd.ID)
	case "retry":
		return reply{}, c.Retry(ctx, cmd.ID, time.Duration(cmd.DelayMS)*time.Millisecond, errors.New("old retry"))
	case "fail":
		return reply{}, c.Fail(ctx, cmd.ID, errors.New("old fail"))
	case "reap":
		res, err := c.Reap(ctx, queue.ReapParams{
			VisibilityTimeout: time.Duration(cmd.VisibilityMS) * time.Millisecond,
			QueueNames:        cmd.Queues,
		})
		return reply{Requeued: res.Requeued, Failed: res.Failed}, err
	case "snooze":
		var until time.Time
		if cmd.DelayMS != 0 {
			var now time.Time
			if err := d.db.QueryRowContext(ctx, `SELECT NOW()`).Scan(&now); err != nil {
				return reply{}, err
			}
			until = now.Add(time.Duration(cmd.DelayMS) * time.Millisecond)
		}
		return reply{}, d.inTx(ctx, func(tx *sql.Tx) error {
			return c.SnoozeTx(ctx, tx, queue.Job{ID: cmd.ID, Claims: cmd.Claims}, until)
		})
	case "wake":
		return reply{}, d.inTx(ctx, func(tx *sql.Tx) error { return c.WakeTx(ctx, tx, cmd.ID) })
	case "lock_claim":
		return reply{}, d.inTx(ctx, func(tx *sql.Tx) error {
			return c.LockClaimTx(ctx, tx, queue.Job{ID: cmd.ID, Claims: cmd.Claims})
		})
	case "heartbeat":
		return reply{}, c.Heartbeat(ctx, queue.Job{ID: cmd.ID, Claims: cmd.Claims})
	case "replay":
		return reply{}, c.ReplayJob(ctx, cmd.ID)
	case "delete":
		return reply{}, c.DeleteJob(ctx, cmd.ID)
	case "purge":
		n, err := c.Purge(ctx, queue.PurgeParams{
			Status:    queue.JobStatus(cmd.Status),
			OlderThan: time.Duration(cmd.OlderThanMS) * time.Millisecond,
		})
		return reply{Deleted: n}, err
	case "purge_finished":
		res, err := c.PurgeFinished(ctx, queue.RetentionPolicy{
			KeepDone:   time.Duration(cmd.KeepDoneMS) * time.Millisecond,
			KeepFailed: time.Duration(cmd.KeepFailedMS) * time.Millisecond,
			BatchSize:  2,
		})
		return reply{Done: res.Done, Failed: res.Failed}, err
	case "get":
		job, err := c.GetJob(ctx, cmd.ID)
		if err != nil {
			return reply{}, err
		}
		return reply{Job: toReply(*job)}, nil
	case "list":
		jobs, err := c.ListJobs(ctx, queue.ListJobsParams{Limit: 1000, QueueName: cmd.Queue, Status: queue.JobStatus(cmd.Status)})
		r := reply{}
		for _, job := range jobs {
			r.Jobs = append(r.Jobs, *toReply(job))
		}
		return r, err
	case "count":
		n, err := c.CountJobs(ctx, queue.ListJobsParams{Limit: 1, QueueName: cmd.Queue, Status: queue.JobStatus(cmd.Status)})
		return reply{Count: n}, err
	case "worker_start":
		return reply{}, d.startWorker(cmd)
	case "worker_stop":
		return reply{}, d.stopWorker(cmd.Name)
	default:
		return reply{}, fmt.Errorf("unknown op %q", cmd.Op)
	}
}

func (d *driver) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// startWorker runs a released Worker on cmd.Queues with OnEnqueue pickup and a
// one-minute rescan, so it only finds new work through notifications. Each handled
// attempt is recorded in compat_processed; with FailFirst, first attempts fail.
func (d *driver) startWorker(cmd command) error {
	registry := queue.NewHandlerRegistry()
	for _, name := range cmd.Queues {
		if err := registry.Register(name, func(ctx context.Context, job queue.Job) error {
			failed := cmd.FailFirst && job.Attempts == 1
			if _, err := d.db.ExecContext(ctx,
				`INSERT INTO compat_processed (job_id, binary_name, attempt, succeeded) VALUES ($1, 'old', $2, $3)`,
				job.ID, job.Attempts, !failed,
			); err != nil {
				return err
			}
			if failed {
				return errors.New("first attempt fails")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	w, err := queue.NewWorker(d.client, registry, queue.WorkerConfig{
		WorkerID:          "old-" + cmd.Name,
		Pickup:            queue.OnEnqueue().RescanEvery(time.Minute),
		VisibilityTimeout: time.Minute,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-w.Ready():
	case err := <-done:
		cancel()
		return fmt.Errorf("worker stopped before ready: %w", err)
	case <-time.After(30 * time.Second):
		cancel()
		return errors.New("worker not ready after 30s")
	}
	d.mu.Lock()
	d.workers[cmd.Name] = &runningWorker{cancel: cancel, done: done}
	d.mu.Unlock()
	return nil
}

func (d *driver) stopWorker(name string) error {
	d.mu.Lock()
	w, ok := d.workers[name]
	delete(d.workers, name)
	d.mu.Unlock()
	if !ok {
		return fmt.Errorf("no worker %q", name)
	}
	w.cancel()
	return <-w.done
}

func toReply(j queue.Job) *jobReply {
	return &jobReply{
		ID: j.ID, Queue: j.QueueName, Status: string(j.Status), Attempts: j.Attempts,
		MaxAttempts: j.MaxAttempts, Claims: j.Claims, AvailableAt: j.AvailableAt,
	}
}
