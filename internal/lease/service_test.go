package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

const chamberID = "chamber-1"

func newTestService(now func() time.Time, store journal.Store) (*Service, *Registry) {
	registry := NewRegistry()
	gates := gate.NewState(chamberID)
	controller := admission.NewController(admission.NewQueue(), gates, signal.NewStore(), now)
	if store == nil {
		store = journal.NewMemoryStore()
	}
	recorder := journal.NewRecorder(store, now)
	return NewService(registry, controller, recorder, now), registry
}

func latestLeaseEvent(t *testing.T, store journal.Store) model.Event {
	t.Helper()
	events, err := store.Events(context.Background(), chamberID, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == model.EventLeaseChanged {
			return events[i]
		}
	}
	t.Fatal("no lease event recorded")
	return model.Event{}
}

// Reproduces the reported incident: A holds an unexpired controlling lease; B
// submits a takeover naming a mismatched previous_owner. The lease must not
// move and A's fencing token must stay valid.
func TestTakeoverMismatchedOwnerWhileActiveIsRejected(t *testing.T) {
	_, now := testClock()
	svc, registry := newTestService(now, nil)
	vessel := uuid.New()
	ctx := context.Background()

	aLease, err := svc.Assign(ctx, chamberID, vessel, "A", time.Minute)
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	_, err = svc.Takeover(ctx, chamberID, vessel, "not-A", "B", time.Minute)
	if !errors.Is(err, ErrLeaseStillActive) {
		t.Fatalf("expected active rejection, got %v", err)
	}
	if !svc.Authorized(vessel, "A", aLease.FencingToken) {
		t.Fatal("A lost admission authorization after a rejected takeover")
	}
	current, _ := registry.Get(vessel)
	if current.Owner != "A" || current.FencingToken != aLease.FencingToken {
		t.Fatalf("lease changed: %+v", current)
	}
}

// Even after expiry, a request that cannot name the current owner is refused:
// the succession path requires explicit identification of the previous owner.
func TestTakeoverMismatchedOwnerAfterExpiryIsRejected(t *testing.T) {
	clock, now := testClock()
	svc, registry := newTestService(now, nil)
	vessel := uuid.New()
	ctx := context.Background()

	aLease, err := svc.Assign(ctx, chamberID, vessel, "A", time.Minute)
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	*clock = aLease.ExpiresAt.Add(time.Second)

	_, err = svc.Takeover(ctx, chamberID, vessel, "impostor", "B", time.Minute)
	if !errors.Is(err, ErrLeaseOwnerMismatch) {
		t.Fatalf("expected owner mismatch, got %v", err)
	}
	current, _ := registry.Get(vessel)
	if current.Owner != "A" || current.FencingToken != aLease.FencingToken {
		t.Fatalf("expired lease was reassigned to B on mismatched previous_owner: %+v", current)
	}
}

func TestAuthorizedSuccessionAfterExpiry(t *testing.T) {
	clock, now := testClock()
	store := journal.NewMemoryStore()
	svc, _ := newTestService(now, store)
	vessel := uuid.New()
	ctx := context.Background()

	aLease, err := svc.Assign(ctx, chamberID, vessel, "A", time.Minute)
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	*clock = aLease.ExpiresAt.Add(time.Second)

	bLease, err := svc.Takeover(ctx, chamberID, vessel, "A", "B", 2*time.Minute)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if bLease.FencingToken != aLease.FencingToken+1 || bLease.Owner != "B" {
		t.Fatalf("unexpected succession lease: %+v", bLease)
	}
	if !svc.Authorized(vessel, "B", bLease.FencingToken) {
		t.Fatal("new owner B is not authorized after takeover")
	}
	// Every pre-succession credential must now be dead for admission.
	if svc.Authorized(vessel, "A", aLease.FencingToken) {
		t.Fatal("old owner A remains authorized after takeover")
	}
	if svc.Authorized(vessel, "B", aLease.FencingToken) {
		t.Fatal("old fencing token authorizes the new owner")
	}
	// A cannot resurrect its control via a heartbeat with the stale token.
	*clock = bLease.ExpiresAt.Add(-time.Second) // still within B's lease
	if _, err := svc.Heartbeat(ctx, chamberID, vessel, "A", aLease.FencingToken, time.Minute); err == nil {
		t.Fatal("A renewed the lease with a stale token after takeover")
	}

	event := latestLeaseEvent(t, store)
	var payload map[string]any
	if err := event.Decode(&payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["change"] != "taken_over" || payload["previous_owner"] != "A" ||
		payload["owner"] != "B" ||
		uint64(payload["previous_fencing_token"].(float64)) != aLease.FencingToken ||
		uint64(payload["fencing_token"].(float64)) != bLease.FencingToken {
		t.Fatalf("takeover journal payload does not match the new lease: %v", payload)
	}
}

// failingStore fails Append for exactly one lease event (the failAfter-th),
// simulating a transient journal outage during a state transition.
type failingStore struct {
	journal.Store
	mu        sync.Mutex
	leaseHits int
	failAfter int
}

func (f *failingStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	if event.Type == model.EventLeaseChanged {
		f.mu.Lock()
		f.leaseHits++
		fail := f.leaseHits == f.failAfter
		f.mu.Unlock()
		if fail {
			return model.Event{}, fmt.Errorf("journal unavailable")
		}
	}
	return f.Store.Append(ctx, event)
}

func TestTakeoverRollsBackWhenJournalFails(t *testing.T) {
	clock, now := testClock()
	store := &failingStore{Store: journal.NewMemoryStore(), failAfter: 2} // assign ok, takeover fails
	svc, registry := newTestService(now, store)
	vessel := uuid.New()
	ctx := context.Background()

	aLease, err := svc.Assign(ctx, chamberID, vessel, "A", time.Minute)
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	*clock = aLease.ExpiresAt.Add(time.Second)

	if _, err := svc.Takeover(ctx, chamberID, vessel, "A", "B", time.Minute); err == nil {
		t.Fatal("expected journal failure")
	}
	current, ok := registry.Get(vessel)
	if !ok || current != aLease {
		t.Fatalf("takeover was not rolled back after journal failure: %+v", current)
	}
	// B's aborted token must remain invalid even though the state was restored;
	// a later successful succession gets a fresh token distinct from the abort.
	*clock = current.ExpiresAt.Add(2 * time.Second)
	retry, err := svc.Takeover(ctx, chamberID, vessel, "A", "B", time.Minute)
	if err != nil {
		t.Fatalf("retry takeover: %v", err)
	}
	if retry.FencingToken != aLease.FencingToken+2 {
		t.Fatalf("aborted token was reused: got %d want %d", retry.FencingToken, aLease.FencingToken+2)
	}
}

func TestAuthorizedLeaseJudgesReturnedValue(t *testing.T) {
	clock, now := testClock()
	svc, _ := newTestService(now, nil)
	vessel := uuid.New()
	ctx := context.Background()

	aLease, err := svc.Assign(ctx, chamberID, vessel, "A", time.Minute)
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	renewed, err := svc.Heartbeat(ctx, chamberID, vessel, "A", aLease.FencingToken, 2*time.Minute)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	// The post-renewal admission check evaluates exactly the returned lease,
	// independent of any later registry state.
	if !svc.AuthorizedLease(vessel, "A", renewed) {
		t.Fatal("returned renewal fails its own admission check")
	}
	*clock = renewed.ExpiresAt.Add(time.Second)
	bLease, err := svc.Takeover(ctx, chamberID, vessel, "A", "B", time.Minute)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if svc.AuthorizedLease(vessel, "A", renewed) {
		t.Fatal("stale renewal value still authorizes A after succession")
	}
	if !svc.AuthorizedLease(vessel, "B", bLease) {
		t.Fatal("new lease fails its own admission check")
	}
}

func TestHeartbeatAndTakeoverStormIsSafe(t *testing.T) {
	// While the lease is continuously alive, interleaved renewal and takeover
	// batches must never move ownership. After it expires, exactly one of a
	// storm of succession requests wins.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mu := sync.Mutex{}
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return base
	}
	advance := func(d time.Duration) {
		mu.Lock()
		base = base.Add(d)
		mu.Unlock()
	}
	svc, registry := newTestService(now, journal.NewMemoryStore())
	vessel := uuid.New()
	ctx := context.Background()

	aLease, err := svc.Assign(ctx, chamberID, vessel, "A", time.Minute)
	if err != nil {
		t.Fatalf("assign: %v", err)
	}

	const rounds, perRound = 20, 25
	for range rounds {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range perRound * 2 {
			wg.Add(1)
			if i%2 == 0 {
				go func() {
					defer wg.Done()
					<-start
					_, _ = svc.Heartbeat(ctx, chamberID, vessel, "A", aLease.FencingToken, time.Minute)
				}()
			} else {
				go func() {
					defer wg.Done()
					<-start
					_, _ = svc.Takeover(ctx, chamberID, vessel, "A", "B", time.Minute)
				}()
			}
		}
		close(start)
		wg.Wait()
	}

	current, _ := registry.Get(vessel)
	if current.Owner != "A" || current.FencingToken != aLease.FencingToken {
		t.Fatalf("takeover preempted active renewals: %+v", current)
	}

	// Expire it: one and only one succession must succeed.
	advance(2 * time.Minute)
	var winners atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if lease, err := svc.Takeover(ctx, chamberID, vessel, "A", "B", time.Minute); err == nil && lease.Owner == "B" {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("expected exactly one successful takeover, got %d", winners.Load())
	}
	final, _ := registry.Get(vessel)
	if final.Owner != "B" || final.FencingToken != aLease.FencingToken+1 {
		t.Fatalf("unexpected final lease: %+v", final)
	}
}
