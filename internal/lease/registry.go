package lease

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

var (
	ErrLeaseNotFound      = errors.New("no lease for vessel")
	ErrLeaseStillActive   = errors.New("lease is still active")
	ErrLeaseOwnerMismatch = errors.New("previous_owner does not match the current lease owner")
	ErrLeaseTokenStale    = errors.New("lease owner or fencing token is stale")
	ErrLeaseExists        = errors.New("vessel already holds a lease")
)

// Registry owns monotonically increasing fencing tokens per vessel.
type Registry struct {
	mu        sync.RWMutex
	leases    map[uuid.UUID]model.Lease
	nextToken map[uuid.UUID]uint64
}

func NewRegistry() *Registry {
	return &Registry{
		leases:    make(map[uuid.UUID]model.Lease),
		nextToken: make(map[uuid.UUID]uint64),
	}
}

// Assign creates the first lease for a vessel. It refuses to overwrite an
// existing lease so that a fresh assignment can never silently invalidate an
// owner that still holds control. Succession after expiry must go through
// Takeover, which enforces the authorization path.
func (r *Registry) Assign(vesselID uuid.UUID, owner string, ttl time.Duration, now time.Time) (model.Lease, error) {
	if vesselID == uuid.Nil || owner == "" || ttl <= 0 {
		return model.Lease{}, errors.New("assignment requires vessel, owner and positive ttl")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.leases[vesselID]; exists {
		return model.Lease{}, ErrLeaseExists
	}
	return r.assignLocked(vesselID, owner, ttl, now), nil
}

// assignLocked issues the next fencing token and stores the lease. Callers
// must hold r.mu and are responsible for having validated any preconditions.
func (r *Registry) assignLocked(vesselID uuid.UUID, owner string, ttl time.Duration, now time.Time) model.Lease {
	r.nextToken[vesselID]++
	lease := model.Lease{
		VesselID: vesselID, Owner: owner, FencingToken: r.nextToken[vesselID],
		ExpiresAt: now.UTC().Add(ttl),
	}
	r.leases[vesselID] = lease
	return lease
}

// Takeover succeeds a lease to a new owner. It is the only authorized path to
// replace a lease and it is deliberately strict:
//
//   - the vessel must currently have a lease on record;
//   - previousOwner must name that lease's owner, so control cannot be stolen
//     by a request that cannot identify who currently holds it;
//   - the current lease must be expired — a live lease can only be renewed by
//     its owner or released deliberately.
//
// The new lease always gets the next monotonically increasing fencing token,
// which invalidates every token issued before it. The check and the token bump
// happen under one lock, so a concurrent heartbeat cannot extend the lease or
// observe an intermediate state.
func (r *Registry) Takeover(vesselID uuid.UUID, previousOwner, newOwner string, ttl time.Duration, now time.Time) (model.Lease, model.Lease, error) {
	if vesselID == uuid.Nil {
		return model.Lease{}, model.Lease{}, errors.New("takeover requires a vessel")
	}
	if previousOwner == "" || newOwner == "" {
		return model.Lease{}, model.Lease{}, errors.New("takeover requires previous and new owner")
	}
	if ttl <= 0 {
		return model.Lease{}, model.Lease{}, errors.New("takeover ttl must be positive")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.leases[vesselID]
	if !ok {
		return model.Lease{}, model.Lease{}, ErrLeaseNotFound
	}
	// Re-check the clock under the lock after reading the lease.
	if current.Active(now.UTC()) {
		return model.Lease{}, current, ErrLeaseStillActive
	}
	if current.Owner != previousOwner {
		return model.Lease{}, current, ErrLeaseOwnerMismatch
	}
	newLease := r.assignLocked(vesselID, newOwner, ttl, now)
	return newLease, current, nil
}

// RollbackTakeover restores the previous lease when a takeover could not be
// journaled. The fencing sequence is intentionally not unwound: the token that
// was issued for the aborted transfer stays consumed so it can never authorize
// admission after the rollback.
func (r *Registry) RollbackTakeover(vesselID uuid.UUID, previous model.Lease) {
	if vesselID == uuid.Nil || previous.Owner == "" || previous.FencingToken == 0 {
		return
	}
	r.mu.Lock()
	r.leases[vesselID] = previous
	r.mu.Unlock()
}

// RollbackAssign removes a lease only when it still carries the token that was
// just issued, so a concurrent operation cannot have its newer state undone.
func (r *Registry) RollbackAssign(vesselID uuid.UUID, token uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lease, ok := r.leases[vesselID]; ok && lease.FencingToken == token {
		delete(r.leases, vesselID)
	}
}

// Renew extends the lease for its current owner when owner and fencing token
// match and the lease is still active. It also returns the pre-renewal lease
// so the caller can restore the previous expiry if journaling fails.
func (r *Registry) Renew(vesselID uuid.UUID, owner string, token uint64, ttl time.Duration, now time.Time) (model.Lease, model.Lease, error) {
	if ttl <= 0 {
		return model.Lease{}, model.Lease{}, errors.New("renewal ttl must be positive")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[vesselID]
	if !ok {
		return model.Lease{}, model.Lease{}, fmt.Errorf("no lease for vessel %s", vesselID)
	}
	if lease.Owner != owner || lease.FencingToken != token {
		return model.Lease{}, model.Lease{}, ErrLeaseTokenStale
	}
	now = now.UTC()
	// The expiry decision and the extension are one atomic step: a lease that
	// has expired cannot be renewed even if another request races to take it
	// over at the same instant.
	if !lease.Active(now) {
		return model.Lease{}, model.Lease{}, errors.New("lease has expired")
	}
	previous := lease
	lease.ExpiresAt = now.Add(ttl)
	r.leases[vesselID] = lease
	return lease, previous, nil
}

// RollbackRenew restores the previous expiry when a renewal could not be
// journaled, but only if the lease is still the one that was renewed.
func (r *Registry) RollbackRenew(vesselID uuid.UUID, token uint64, expiresAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lease, ok := r.leases[vesselID]; ok && lease.FencingToken == token {
		lease.ExpiresAt = expiresAt
		r.leases[vesselID] = lease
	}
}

func (r *Registry) Get(vesselID uuid.UUID) (model.Lease, bool) {
	r.mu.RLock()
	lease, ok := r.leases[vesselID]
	r.mu.RUnlock()
	return lease, ok
}

// Release removes the lease when owner and fencing token match, returning the
// removed lease so the caller can restore it if the journal write fails.
func (r *Registry) Release(vesselID uuid.UUID, owner string, token uint64) (model.Lease, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[vesselID]
	if !ok || lease.Owner != owner || lease.FencingToken != token {
		return model.Lease{}, false
	}
	delete(r.leases, vesselID)
	return lease, true
}

// RollbackRelease restores a release when the event could not be journaled.
// If the vessel already carries a newer lease (e.g. a takeover won the race)
// the stale release is not restored.
func (r *Registry) RollbackRelease(previous model.Lease) {
	if previous.Owner == "" || previous.FencingToken == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.leases[previous.VesselID]; exists {
		return
	}
	r.leases[previous.VesselID] = previous
}

func (r *Registry) Snapshot() []model.Lease {
	r.mu.RLock()
	result := make([]model.Lease, 0, len(r.leases))
	for _, lease := range r.leases {
		result = append(result, lease)
	}
	r.mu.RUnlock()
	return result
}
