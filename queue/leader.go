package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

const (
	maintenanceLeadership = "maintenance"

	defaultElectInterval = 5 * time.Second
	defaultLeaderTTL     = 15 * time.Second
	leaderTrustMargin    = 2 * time.Second
	renewRetryInterval   = time.Second
	resignBudget         = 5 * time.Second
)

// leaderTerm identifies one leadership term: the same client elected twice holds
// two terms, and only the current one can renew or resign.
type leaderTerm struct {
	leaderID  string
	electedAt time.Time
}

// leaderStore is the database side of leader election.
type leaderStore interface {
	// elect makes leaderID the leader if nobody holds an unexpired term.
	elect(ctx context.Context, leaderID string, ttl time.Duration) (leaderTerm, bool, error)
	// renew extends term by ttl and reports false once term was replaced.
	renew(ctx context.Context, term leaderTerm, ttl time.Duration) (bool, error)
	// resign ends term if it is still current.
	resign(ctx context.Context, term leaderTerm) error
}

type sqlLeaderStore struct {
	db   *sql.DB
	name string
}

func (s sqlLeaderStore) elect(ctx context.Context, leaderID string, ttl time.Duration) (leaderTerm, bool, error) {
	var electedAt time.Time
	err := s.db.QueryRowContext(ctx, `
INSERT INTO pgqueue_leader (name, leader_id, elected_at, expires_at)
VALUES ($1, $2, NOW(), NOW() + make_interval(secs => $3::float8))
ON CONFLICT (name) DO UPDATE
SET leader_id = EXCLUDED.leader_id, elected_at = EXCLUDED.elected_at, expires_at = EXCLUDED.expires_at
WHERE pgqueue_leader.expires_at < NOW()
RETURNING elected_at`, s.name, leaderID, ttl.Seconds()).Scan(&electedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return leaderTerm{}, false, nil
	}
	if err != nil {
		return leaderTerm{}, false, fmt.Errorf("pgqueue: elect leader: %w", err)
	}
	return leaderTerm{leaderID: leaderID, electedAt: electedAt}, true, nil
}

func (s sqlLeaderStore) renew(ctx context.Context, term leaderTerm, ttl time.Duration) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE pgqueue_leader SET expires_at = NOW() + make_interval(secs => $4::float8)
WHERE name = $1 AND leader_id = $2 AND elected_at = $3`, s.name, term.leaderID, term.electedAt, ttl.Seconds())
	if err != nil {
		return false, fmt.Errorf("pgqueue: renew leadership: %w", err)
	}
	renewed, err := res.RowsAffected()
	return renewed == 1, err
}

func (s sqlLeaderStore) resign(ctx context.Context, term leaderTerm) error {
	if _, err := s.db.ExecContext(ctx, `
DELETE FROM pgqueue_leader WHERE name = $1 AND leader_id = $2 AND elected_at = $3`,
		s.name, term.leaderID, term.electedAt); err != nil {
		return fmt.Errorf("pgqueue: resign leadership: %w", err)
	}
	return nil
}

// elector alternates between follower and leader. A follower tries to win every
// electEvery; a leader renews every electEvery and trusts its term until
// attempt start + ttl - margin by the local clock. It steps down when its term is
// replaced, when renewals keep failing until that trust deadline, or when it
// stops, so it never acts on a term the database may already have given away.
type elector struct {
	store      leaderStore
	id         string
	electEvery time.Duration
	ttl        time.Duration
	margin     time.Duration
	onError    func(ctx context.Context, msg string, err error)

	leader atomic.Bool
}

func newElector(store leaderStore, id string) *elector {
	return &elector{
		store:      store,
		id:         id,
		electEvery: defaultElectInterval,
		ttl:        defaultLeaderTTL,
		margin:     leaderTrustMargin,
		onError:    func(context.Context, string, error) {},
	}
}

func (e *elector) leading() bool {
	return e.leader.Load()
}

// run elects until ctx ends and runs lead for every term it wins. lead's context
// ends when the term does; run waits for lead to return before it resigns or
// tries again.
func (e *elector) run(ctx context.Context, lead func(context.Context)) {
	for ctx.Err() == nil {
		attempt := time.Now()
		term, won, err := e.store.elect(ctx, e.id, e.ttl)
		if err != nil && ctx.Err() == nil {
			e.onError(ctx, "queue leader election failed", err)
		}
		if won {
			e.hold(ctx, term, attempt, lead)
		}
		if !sleepCtx(ctx, e.followerWait()) {
			return
		}
	}
}

func (e *elector) followerWait() time.Duration {
	jitter := e.electEvery / 5
	if jitter <= 0 {
		return e.electEvery
	}
	return e.electEvery + rand.N(jitter)
}

func (e *elector) hold(ctx context.Context, term leaderTerm, attempt time.Time, lead func(context.Context)) {
	trustedUntil := attempt.Add(e.trustWindow())
	leaderCtx, cancel := context.WithCancel(ctx)
	led := make(chan struct{})
	e.leader.Store(true)
	go func() {
		defer close(led)
		lead(leaderCtx)
	}()

	stepDown := func(resign bool) {
		e.leader.Store(false)
		cancel()
		<-led
		if resign {
			resignCtx, cancelResign := context.WithTimeout(context.WithoutCancel(ctx), resignBudget)
			defer cancelResign()
			if err := e.store.resign(resignCtx, term); err != nil {
				e.onError(ctx, "queue leader resignation failed", err)
			}
		}
	}

	timer := time.NewTimer(e.electEvery)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			stepDown(true)
			return
		case <-timer.C:
		}

		attempt := time.Now()
		if !attempt.Before(trustedUntil) {
			stepDown(true)
			return
		}
		renewCtx, cancelRenew := context.WithDeadline(ctx, trustedUntil)
		renewed, err := e.store.renew(renewCtx, term, e.ttl)
		cancelRenew()
		switch {
		case ctx.Err() != nil:
			stepDown(true)
			return
		case err == nil && renewed:
			trustedUntil = attempt.Add(e.trustWindow())
			timer.Reset(e.electEvery)
		case err == nil:
			stepDown(false)
			return
		default:
			e.onError(ctx, "queue leader renewal failed", err)
			if !time.Now().Before(trustedUntil) {
				stepDown(true)
				return
			}
			timer.Reset(min(renewRetryInterval, time.Until(trustedUntil)))
		}
	}
}

// trustWindow is how long after an attempt starts the leader trusts its term. The
// margin never exceeds a quarter of the TTL, so a short TTL still leaves a window.
func (e *elector) trustWindow() time.Duration {
	return e.ttl - min(e.margin, e.ttl/4)
}

// sleepCtx waits for d and reports false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
