package lease

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testClock() (*time.Time, func() time.Time) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &base, func() time.Time { return base }
}

func TestAssignNeverOverwritesExistingLease(t *testing.T) {
	_, now := testClock()
	registry := NewRegistry()
	vessel := uuid.New()

	if _, err := registry.Assign(vessel, "A", time.Minute, now()); err != nil {
		t.Fatalf("first assign: %v", err)
	}
	_, err := registry.Assign(vessel, "B", time.Minute, now())
	if !errors.Is(err, ErrLeaseExists) {
		t.Fatalf("expected ErrLeaseExists, got %v", err)
	}
	lease, ok := registry.Get(vessel)
	if !ok || lease.Owner != "A" {
		t.Fatalf("lease was overwritten by the rejected assignment: %+v ok=%v", lease, ok)
	}
}

func TestTakeoverRejectedWhileLeaseActive(t *testing.T) {
	_, now := testClock()
	registry := NewRegistry()
	vessel := uuid.New()
	initial, err := registry.Assign(vessel, "A", time.Minute, now())
	if err != nil {
		t.Fatalf("assign: %v", err)
	}

	for _, previous := range []string{"A", "someone-else"} {
		_, _, err := registry.Takeover(vessel, previous, "B", time.Minute, now())
		if !errors.Is(err, ErrLeaseStillActive) {
			t.Fatalf("previous=%q: expected ErrLeaseStillActive, got %v", previous, err)
		}
	}
	current, ok := registry.Get(vessel)
	if !ok || current.Owner != "A" || current.FencingToken != initial.FencingToken {
		t.Fatalf("active lease changed after rejected takeover: %+v", current)
	}
}

func TestTakeoverRequiresMatchingPreviousOwner(t *testing.T) {
	clock, now := testClock()
	registry := NewRegistry()
	vessel := uuid.New()
	initial, err := registry.Assign(vessel, "A", time.Minute, now())
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	*clock = initial.ExpiresAt.Add(time.Second) // lease is now expired

	_, current, err := registry.Takeover(vessel, "impostor", "B", time.Minute, now())
	if !errors.Is(err, ErrLeaseOwnerMismatch) {
		t.Fatalf("expected ErrLeaseOwnerMismatch, got %v", err)
	}
	after, ok := registry.Get(vessel)
	if !ok || after.Owner != "A" || after.FencingToken != initial.FencingToken {
		t.Fatalf("lease changed on rejected takeover: %+v was %+v", after, current)
	}
}

func TestTakeoverOfExpiredLeaseIssuesNewFencingToken(t *testing.T) {
	clock, now := testClock()
	registry := NewRegistry()
	vessel := uuid.New()
	initial, err := registry.Assign(vessel, "A", time.Minute, now())
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	*clock = initial.ExpiresAt.Add(time.Second)

	newLease, previous, err := registry.Takeover(vessel, "A", "B", 2*time.Minute, now())
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if previous.FencingToken != initial.FencingToken || previous.Owner != "A" {
		t.Fatalf("previous lease not returned: %+v", previous)
	}
	if newLease.Owner != "B" || newLease.FencingToken != initial.FencingToken+1 {
		t.Fatalf("unexpected new lease: %+v", newLease)
	}
	if !newLease.Active(now()) {
		t.Fatalf("new lease should be active: %+v", newLease)
	}

	// The old token must be dead: neither renewal nor any record of owner A remains.
	if _, _, err := registry.Renew(vessel, "A", initial.FencingToken, time.Minute, now()); err == nil {
		t.Fatal("renewal with the old fencing token succeeded after takeover")
	}
	current, _ := registry.Get(vessel)
	if current.Owner != "B" {
		t.Fatalf("registry still serves owner A: %+v", current)
	}
}

func TestTakeoverWithoutLease(t *testing.T) {
	_, now := testClock()
	registry := NewRegistry()
	_, _, err := registry.Takeover(uuid.New(), "A", "B", time.Minute, now())
	if !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("expected ErrLeaseNotFound, got %v", err)
	}
}

func TestTakeoverValidation(t *testing.T) {
	_, now := testClock()
	registry := NewRegistry()
	vessel := uuid.New()
	if _, err := registry.Assign(vessel, "A", time.Minute, now()); err != nil {
		t.Fatalf("assign: %v", err)
	}
	cases := []struct {
		name             string
		vessel           uuid.UUID
		previous, newOwn string
		ttl              time.Duration
	}{
		{"nil vessel", uuid.Nil, "A", "B", time.Minute},
		{"empty previous", vessel, "", "B", time.Minute},
		{"empty new", vessel, "A", "", time.Minute},
		{"zero ttl", vessel, "A", "B", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := registry.Takeover(tc.vessel, tc.previous, tc.newOwn, tc.ttl, now()); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestRollbackTakeoverRestoresPreviousLease(t *testing.T) {
	clock, now := testClock()
	registry := NewRegistry()
	vessel := uuid.New()
	initial, err := registry.Assign(vessel, "A", time.Minute, now())
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	*clock = initial.ExpiresAt.Add(time.Second)
	newLease, previous, err := registry.Takeover(vessel, "A", "B", time.Minute, now())
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	registry.RollbackTakeover(vessel, previous)
	restored, ok := registry.Get(vessel)
	if !ok || restored != initial {
		t.Fatalf("rollback did not restore initial lease: %+v want %+v", restored, initial)
	}
	// The aborted token stays consumed and can never authorize anything.
	if restored.FencingToken == newLease.FencingToken {
		t.Fatal("rolled-back lease must not carry the aborted fencing token")
	}
}

// While A keeps its lease alive with heartbeats, no concurrent takeover may
// succeed: the active-expiry decision and the token bump are atomic.
func TestTakeoverCannotPreemptConcurrentRenewals(t *testing.T) {
	_, now := testClock()
	registry := NewRegistry()
	vessel := uuid.New()
	initial, err := registry.Assign(vessel, "A", time.Minute, now())
	if err != nil {
		t.Fatalf("assign: %v", err)
	}

	const rounds, perRound = 20, 25
	var renewOK atomic.Int64
	for range rounds {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range perRound * 2 {
			wg.Add(1)
			if i%2 == 0 {
				go func() {
					defer wg.Done()
					<-start
					if lease, _, err := registry.Renew(vessel, "A", initial.FencingToken, time.Minute, now()); err == nil {
						if lease.Owner == "A" {
							renewOK.Add(1)
						}
					}
				}()
			} else {
				go func() {
					defer wg.Done()
					<-start
					_, _, _ = registry.Takeover(vessel, "A", "B", time.Minute, now())
				}()
			}
		}
		close(start)
		wg.Wait()
	}

	final, ok := registry.Get(vessel)
	if !ok || final.Owner != "A" || final.FencingToken != initial.FencingToken {
		t.Fatalf("lease changed during takeover storm: %+v", final)
	}
	if renewOK.Load() == 0 {
		t.Fatal("renewer never succeeded")
	}
}

// At the expiry boundary an old owner must never be able to renew while a
// succession lands: either the lease is gone (takeover won) or it is expired
// (renewal rejected) — the old token can never come back authorized.
func TestRenewAndTakeoverRaceAtExpiry(t *testing.T) {
	clock, now := testClock()
	registry := NewRegistry()
	vessel := uuid.New()
	initial, err := registry.Assign(vessel, "A", time.Minute, now())
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	*clock = initial.ExpiresAt.Add(time.Millisecond)

	var renewSuccess atomic.Int64
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, _, err := registry.Renew(vessel, "A", initial.FencingToken, time.Minute, now()); err == nil {
				renewSuccess.Add(1)
			}
		}()
		go func() {
			defer wg.Done()
			_, _, _ = registry.Takeover(vessel, "A", "B", time.Minute, now())
		}()
	}
	wg.Wait()

	if renewSuccess.Load() != 0 {
		t.Fatalf("%d stale renewals landed on an expired lease during takeover race", renewSuccess.Load())
	}
	final, ok := registry.Get(vessel)
	if !ok || final.Owner != "B" || final.FencingToken != initial.FencingToken+1 || !final.Active(now()) {
		t.Fatalf("unexpected post-race lease: %+v ok=%v", final, ok)
	}
}
