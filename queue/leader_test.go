package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestOneOfManyCompetingClientsIsElected(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		d.client(t)
		var won atomic.Int32
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() {
				store := sqlLeaderStore{db: d.db, name: "competition"}
				_, elected, err := store.elect(context.Background(), fmt.Sprintf("client-%d", i), time.Minute)
				if err != nil {
					t.Errorf("elect: %v", err)
				}
				if elected {
					won.Add(1)
				}
			})
		}
		wg.Wait()
		if won.Load() != 1 {
			t.Fatalf("%d clients elected, want 1", won.Load())
		}
	})
}

func TestALeaderTermIsRenewedOnlyByItsHolderAndUntilReplaced(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		d.client(t)
		store := sqlLeaderStore{db: d.db, name: "terms"}
		first, won, err := store.elect(ctx, "first", time.Minute)
		if err != nil || !won {
			t.Fatalf("first elect: won=%v err=%v", won, err)
		}
		if _, won, err := store.elect(ctx, "second", time.Minute); err != nil || won {
			t.Fatalf("second elect during a live term: won=%v err=%v", won, err)
		}
		if renewed, err := store.renew(ctx, leaderTerm{leaderID: "second", electedAt: first.electedAt}, time.Minute); err != nil || renewed {
			t.Fatalf("renew by another client: renewed=%v err=%v", renewed, err)
		}
		if renewed, err := store.renew(ctx, first, time.Minute); err != nil || !renewed {
			t.Fatalf("renew by holder: renewed=%v err=%v", renewed, err)
		}

		d.exec(t, `UPDATE pgqueue_leader SET expires_at = NOW() - interval '1 second' WHERE name = 'terms'`)
		second, won, err := store.elect(ctx, "second", time.Minute)
		if err != nil || !won {
			t.Fatalf("elect after expiry: won=%v err=%v", won, err)
		}
		if renewed, err := store.renew(ctx, first, time.Minute); err != nil || renewed {
			t.Fatalf("renew of a replaced term: renewed=%v err=%v", renewed, err)
		}
		if err := store.resign(ctx, first); err != nil {
			t.Fatalf("resign of a replaced term: %v", err)
		}
		if renewed, err := store.renew(ctx, second, time.Minute); err != nil || !renewed {
			t.Fatalf("a stale resignation ended the current term: renewed=%v err=%v", renewed, err)
		}
	})
}

func TestTheSameClientElectedTwiceHoldsTwoTerms(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		ctx := context.Background()
		d.client(t)
		store := sqlLeaderStore{db: d.db, name: "same-id"}
		old, _, _ := store.elect(ctx, "client", time.Minute)
		d.exec(t, `UPDATE pgqueue_leader SET expires_at = NOW() - interval '1 second' WHERE name = 'same-id'`)
		if _, won, err := store.elect(ctx, "client", time.Minute); err != nil || !won {
			t.Fatalf("re-elect: won=%v err=%v", won, err)
		}
		if renewed, err := store.renew(ctx, old, time.Minute); err != nil || renewed {
			t.Fatalf("the old term of the same client renewed: renewed=%v err=%v", renewed, err)
		}
	})
}

func TestResigningLetsAnotherClientLeadWithoutWaitingForExpiry(t *testing.T) {
	eachDriver(t, func(t *testing.T, d driverDB) {
		first := d.client(t)
		second := d.bareClient(t)
		first.maintainer.ttl = time.Hour
		second.maintainer.ttl = time.Hour

		leaveFirst := first.maintainer.join(nil)
		requireEventually(t, 5*time.Second, 10*time.Millisecond, first.IsLeader)
		leaveSecond := second.maintainer.join(nil)
		defer leaveSecond()
		time.Sleep(300 * time.Millisecond)
		if second.IsLeader() {
			t.Fatal("two clients lead at once")
		}

		leaveFirst()
		if first.IsLeader() {
			t.Fatal("a client that left still leads")
		}
		requireEventually(t, 5*time.Second, 10*time.Millisecond, second.IsLeader)
	})
}

// fakeLeaderStore is an in-memory leaderStore for synctest bubbles, where the
// clock is fake and a database round trip would not advance it.
type fakeLeaderStore struct {
	mu        sync.Mutex
	holder    string
	electedAt time.Time
	expiresAt time.Time
	renewErr  error
	renews    int
	resigns   int
}

func (s *fakeLeaderStore) elect(_ context.Context, id string, ttl time.Duration) (leaderTerm, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.holder != "" && now.Before(s.expiresAt) {
		return leaderTerm{}, false, nil
	}
	s.holder, s.electedAt, s.expiresAt = id, now, now.Add(ttl)
	return leaderTerm{leaderID: id, electedAt: now}, true, nil
}

func (s *fakeLeaderStore) renew(_ context.Context, term leaderTerm, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renews++
	if s.renewErr != nil {
		return false, s.renewErr
	}
	if s.holder != term.leaderID || !s.electedAt.Equal(term.electedAt) {
		return false, nil
	}
	s.expiresAt = time.Now().Add(ttl)
	return true, nil
}

func (s *fakeLeaderStore) resign(_ context.Context, term leaderTerm) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resigns++
	if s.holder == term.leaderID && s.electedAt.Equal(term.electedAt) {
		s.holder = ""
	}
	return nil
}

func (s *fakeLeaderStore) set(update func(*fakeLeaderStore)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	update(s)
}

func (s *fakeLeaderStore) get(read func(*fakeLeaderStore)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	read(s)
}

// startElector runs an elector with the default timings and records how long
// each term's lead function ran.
func startElector(t *testing.T, store leaderStore, id string) (e *elector, terms *atomic.Int32, stop func()) {
	t.Helper()
	e = newElector(store, id)
	terms = &atomic.Int32{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.run(ctx, func(leaderCtx context.Context) {
			terms.Add(1)
			<-leaderCtx.Done()
		})
	}()
	return e, terms, func() {
		cancel()
		<-done
	}
}

func TestALeaderSustainsItsTermByRenewing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeLeaderStore{}
		e, terms, stop := startElector(t, store, "leader")
		defer stop()
		synctest.Wait()
		if !e.leading() {
			t.Fatal("not elected on start")
		}

		time.Sleep(10 * defaultLeaderTTL)
		synctest.Wait()
		if !e.leading() || terms.Load() != 1 {
			t.Fatalf("leading=%v terms=%d after ten TTLs, want one sustained term", e.leading(), terms.Load())
		}
		store.get(func(s *fakeLeaderStore) {
			if s.renews < 10 {
				t.Fatalf("%d renewals in ten TTLs", s.renews)
			}
		})
	})
}

func TestALeaderStepsDownBeforeItsTermCanExpireWhenRenewalsFail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeLeaderStore{}
		e, _, stop := startElector(t, store, "leader")
		defer stop()
		synctest.Wait()
		var lastRenewal time.Time
		store.set(func(s *fakeLeaderStore) { s.renewErr = errors.New("database unreachable") })
		store.get(func(s *fakeLeaderStore) { lastRenewal = s.expiresAt.Add(-defaultLeaderTTL) })

		for e.leading() {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
		}
		steppedDown := time.Now()
		if deadline := lastRenewal.Add(defaultLeaderTTL); !steppedDown.Before(deadline) {
			t.Fatalf("stepped down at %s, after the database term expired at %s", steppedDown, deadline)
		}
		if held := steppedDown.Sub(lastRenewal); held < defaultLeaderTTL-leaderTrustMargin-defaultElectInterval {
			t.Fatalf("stepped down after %s, before the trust window ended", held)
		}
	})
}

func TestALeaderStepsDownAtOnceWhenItsTermIsReplaced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeLeaderStore{}
		e, _, stop := startElector(t, store, "leader")
		defer stop()
		synctest.Wait()
		store.set(func(s *fakeLeaderStore) { s.holder, s.electedAt = "usurper", time.Now() })

		time.Sleep(defaultElectInterval)
		synctest.Wait()
		if e.leading() {
			t.Fatal("still leading one renewal after the term was replaced")
		}
		store.get(func(s *fakeLeaderStore) {
			if s.resigns != 0 || s.holder != "usurper" {
				t.Fatalf("a replaced leader resigned the new term: resigns=%d holder=%s", s.resigns, s.holder)
			}
		})
	})
}

func TestAStoppingLeaderResignsSoAFollowerTakesOver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeLeaderStore{}
		first, _, stopFirst := startElector(t, store, "first")
		synctest.Wait()
		second, _, stopSecond := startElector(t, store, "second")
		defer stopSecond()
		synctest.Wait()
		if !first.leading() || second.leading() {
			t.Fatalf("first=%v second=%v, want only first leading", first.leading(), second.leading())
		}

		stopFirst()
		if first.leading() {
			t.Fatal("a stopped elector still leads")
		}
		time.Sleep(2 * defaultElectInterval)
		synctest.Wait()
		if !second.leading() {
			t.Fatal("the follower did not take over within two election intervals of a resignation")
		}
	})
}

func TestALeaderStopsItsServicesBeforeItResigns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeLeaderStore{}
		e := newElector(store, "leader")
		ctx, cancel := context.WithCancel(t.Context())
		var servicesRunning atomic.Bool
		done := make(chan struct{})
		go func() {
			defer close(done)
			e.run(ctx, func(leaderCtx context.Context) {
				servicesRunning.Store(true)
				<-leaderCtx.Done()
				time.Sleep(time.Second)
				servicesRunning.Store(false)
			})
		}()
		synctest.Wait()
		cancel()
		<-done
		if servicesRunning.Load() {
			t.Fatal("run returned while services were still running")
		}
		store.get(func(s *fakeLeaderStore) {
			if s.resigns != 1 || s.holder != "" {
				t.Fatalf("resigns=%d holder=%q, want one resignation", s.resigns, s.holder)
			}
		})
	})
}
