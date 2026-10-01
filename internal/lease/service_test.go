package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

type harness struct {
	svc   *Service
	reg   *Registry
	store *journal.MemoryStore
	clk   *clock
}

func newHarness() *harness {
	clk := &clock{t: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}
	reg := NewRegistry()
	gates := gate.NewState("chamber-1")
	signals := signal.NewStore()
	queue := admission.NewQueue()
	ctrl := admission.NewController(queue, gates, signals, clk.now)
	store := journal.NewMemoryStore()
	rec := journal.NewRecorder(store, clk.now)
	svc := NewService(reg, ctrl, rec, clk.now)
	return &harness{svc: svc, reg: reg, store: store, clk: clk}
}

func (h *harness) leaseEvents(t *testing.T) []model.Event {
	t.Helper()
	events, err := h.store.Events(context.Background(), "chamber-1", 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	return events
}

// An active lease must not move when previous_owner does not name the holder.
func TestTakeoverRejectsMismatchedPreviousOwner(t *testing.T) {
	h := newHarness()
	vessel := uuid.New()
	const ttl = 30 * time.Second

	a, err := h.svc.Assign(context.Background(), "chamber-1", vessel, "A", ttl)
	if err != nil {
		t.Fatalf("assign A: %v", err)
	}

	_, err = h.svc.Takeover(context.Background(), "chamber-1", vessel, "impostor", a.FencingToken, "B", ttl)
	if !errors.Is(err, ErrOwnershipMismatch) {
		t.Fatalf("want ErrOwnershipMismatch, got %v", err)
	}

	got, ok := h.reg.Get(vessel)
	if !ok {
		t.Fatal("lease vanished after rejected takeover")
	}
	if got.Owner != "A" || got.FencingToken != a.FencingToken {
		t.Fatalf("lease altered: %+v", got)
	}
	if !h.svc.Authorized(vessel, "A", a.FencingToken) {
		t.Fatal("holder A lost authorization despite rejected takeover")
	}
	if h.svc.Authorized(vessel, "B", a.FencingToken) {
		t.Fatal("B became authorized without proof of ownership")
	}
}

// Naming the right owner with a stale/wrong token must also be refused.
func TestTakeoverRejectsWrongFencingToken(t *testing.T) {
	h := newHarness()
	vessel := uuid.New()
	const ttl = 30 * time.Second

	a, err := h.svc.Assign(context.Background(), "chamber-1", vessel, "A", ttl)
	if err != nil {
		t.Fatalf("assign A: %v", err)
	}

	_, err = h.svc.Takeover(context.Background(), "chamber-1", vessel, "A", a.FencingToken+99, "B", ttl)
	if !errors.Is(err, ErrFencingMismatch) {
		t.Fatalf("want ErrFencingMismatch, got %v", err)
	}
	if h.svc.Authorized(vessel, "A", a.FencingToken+99) {
		t.Fatal("stale token unexpectedly authorized")
	}
	if !h.svc.Authorized(vessel, "A", a.FencingToken) {
		t.Fatal("current holder A lost authorization")
	}
}

// The explicit authorization path: correct owner + current token transfers the
// lease, bumps the fence, journals the new token, and invalidates the old one.
func TestTakeoverAuthorizedPath(t *testing.T) {
	h := newHarness()
	vessel := uuid.New()
	const ttl = 30 * time.Second

	a, err := h.svc.Assign(context.Background(), "chamber-1", vessel, "A", ttl)
	if err != nil {
		t.Fatalf("assign A: %v", err)
	}
	b, err := h.svc.Takeover(context.Background(), "chamber-1", vessel, "A", a.FencingToken, "B", ttl)
	if err != nil {
		t.Fatalf("authorized takeover: %v", err)
	}
	if b.Owner != "B" {
		t.Fatalf("new owner = %q, want B", b.Owner)
	}
	if b.FencingToken <= a.FencingToken {
		t.Fatalf("fencing token did not advance: old=%d new=%d", a.FencingToken, b.FencingToken)
	}
	if h.svc.Authorized(vessel, "A", a.FencingToken) {
		t.Fatal("old fence of A is still authorized")
	}
	if !h.svc.Authorized(vessel, "B", b.FencingToken) {
		t.Fatal("new owner B is not authorized with the new fence")
	}

	events := h.leaseEvents(t)
	var found bool
	for _, ev := range events {
		if ev.Type != model.EventLeaseChanged {
			continue
		}
		var lease model.Lease
		if err := ev.Decode(&lease); err != nil {
			t.Fatalf("decode lease event: %v", err)
		}
		if lease.Owner == "B" {
			found = true
			if lease.FencingToken != b.FencingToken {
				t.Fatalf("journaled token=%d does not match new token=%d", lease.FencingToken, b.FencingToken)
			}
			if ev.Generation != b.FencingToken {
				t.Fatalf("event generation=%d does not match new token=%d", ev.Generation, b.FencingToken)
			}
		}
	}
	if !found {
		t.Fatal("successful takeover was not journaled as a lease.changed event")
	}
}

// A rejected takeover must not leave an event behind (no phantom new token).
func TestTakeoverRejectedWritesNoJournalEvent(t *testing.T) {
	h := newHarness()
	vessel := uuid.New()
	const ttl = 30 * time.Second

	a, err := h.svc.Assign(context.Background(), "chamber-1", vessel, "A", ttl)
	if err != nil {
		t.Fatalf("assign A: %v", err)
	}
	eventsBefore := len(h.leaseEvents(t))

	if _, err := h.svc.Takeover(context.Background(), "chamber-1", vessel, "A", a.FencingToken+1, "B", ttl); err == nil {
		t.Fatal("expected takeover to fail with a wrong token")
	}
	if got := len(h.leaseEvents(t)); got != eventsBefore {
		t.Fatalf("journal mutated on rejected takeover: before=%d after=%d", eventsBefore, got)
	}
}

// Once the lease has expired it may be reclaimed without credentials (sweep).
func TestTakeoverAfterExpiry(t *testing.T) {
	h := newHarness()
	vessel := uuid.New()
	const ttl = 30 * time.Second

	a, err := h.svc.Assign(context.Background(), "chamber-1", vessel, "A", ttl)
	if err != nil {
		t.Fatalf("assign A: %v", err)
	}
	h.clk.advance(ttl + time.Second)

	if h.svc.Authorized(vessel, "A", a.FencingToken) {
		t.Fatal("expired lease still reports authorized")
	}
	b, err := h.svc.Takeover(context.Background(), "chamber-1", vessel, "wrong-owner", 0, "B", ttl)
	if err != nil {
		t.Fatalf("reclaim after expiry: %v", err)
	}
	if b.Owner != "B" || b.FencingToken <= a.FencingToken {
		t.Fatalf("unexpected reclaimed lease: %+v", b)
	}
	if !h.svc.Authorized(vessel, "B", b.FencingToken) {
		t.Fatal("reclaiming owner B is not authorized")
	}
}

// A lease that never existed may be claimed without credentials.
func TestTakeoverAbsentLease(t *testing.T) {
	h := newHarness()
	vessel := uuid.New()
	b, err := h.svc.Takeover(context.Background(), "chamber-1", vessel, "", 0, "B", time.Minute)
	if err != nil {
		t.Fatalf("claim absent lease: %v", err)
	}
	if !h.svc.Authorized(vessel, "B", b.FencingToken) {
		t.Fatal("claimant B is not authorized")
	}
}

// Plain Assign must never overwrite an active lease owned by someone else.
func TestAssignCannotClobberActiveForeignOwner(t *testing.T) {
	h := newHarness()
	vessel := uuid.New()
	const ttl = 30 * time.Second

	a, err := h.svc.Assign(context.Background(), "chamber-1", vessel, "A", ttl)
	if err != nil {
		t.Fatalf("assign A: %v", err)
	}
	if _, err := h.svc.Assign(context.Background(), "chamber-1", vessel, "B", ttl); !errors.Is(err, ErrLeaseStillActive) {
		t.Fatalf("want ErrLeaseStillActive, got %v", err)
	}
	if !h.svc.Authorized(vessel, "A", a.FencingToken) {
		t.Fatal("holder A was preempted by a bare Assign")
	}
}

// The original report end-to-end: an impostor cannot seize A's unexpired lease,
// including while A is concurrently renewing it.
func TestConcurrentRenewalCannotBePreempted(t *testing.T) {
	h := newHarness()
	vessel := uuid.New()
	const ttl = time.Hour // never expires during the test, isolating the ownership check

	a, err := h.svc.Assign(context.Background(), "chamber-1", vessel, "A", ttl)
	if err != nil {
		t.Fatalf("assign A: %v", err)
	}

	const iterations = 200
	var wg sync.WaitGroup
	wg.Add(2)

	// Holder A keeps renewing with its stable fencing token.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			renewed, err := h.svc.Heartbeat(context.Background(), "chamber-1", vessel, "A", a.FencingToken, ttl)
			if err == nil {
				if renewed.Owner != "A" || renewed.FencingToken != a.FencingToken {
					t.Errorf("renewal returned unexpected lease: %+v", renewed)
					return
				}
			}
		}
	}()

	// Attacker repeatedly tries to seize the lease using mismatched credentials.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			prevToken := a.FencingToken
			if i%2 == 0 {
				prevToken++ // wrong token
			}
			if _, err := h.svc.Takeover(context.Background(), "chamber-1", vessel, "impostor", prevToken, "B", ttl); err == nil {
				t.Errorf("unauthorized takeover succeeded on iteration %d", i)
				return
			}
		}
	}()

	wg.Wait()

	got, ok := h.reg.Get(vessel)
	if !ok {
		t.Fatal("lease disappeared under concurrency")
	}
	if got.Owner != "A" || got.FencingToken != a.FencingToken {
		t.Fatalf("lease was preempted: %+v", got)
	}
	if !h.svc.Authorized(vessel, "A", a.FencingToken) {
		t.Fatal("holder A is no longer authorized after the attack")
	}
}

// Fencing tokens are strictly monotonic across every (re)assignment.
func TestFencingTokensMonotonic(t *testing.T) {
	h := newHarness()
	vessel := uuid.New()
	const ttl = 10 * time.Second

	var last uint64
	for i, owner := range []string{"A", "B", "C", "D"} {
		var token uint64
		if i == 0 {
			l, err := h.svc.Assign(context.Background(), "chamber-1", vessel, owner, ttl)
			if err != nil {
				t.Fatalf("assign %s: %v", owner, err)
			}
			token = l.FencingToken
		} else {
			h.clk.advance(ttl + time.Second) // let prior lease expire
			l, err := h.svc.Takeover(context.Background(), "chamber-1", vessel, "nobody", 0, owner, ttl)
			if err != nil {
				t.Fatalf("takeover %s: %v", owner, err)
			}
			token = l.FencingToken
		}
		if token <= last {
			t.Fatalf("token not monotonic at step %d: %d <= %d", i, token, last)
		}
		last = token
	}
}
