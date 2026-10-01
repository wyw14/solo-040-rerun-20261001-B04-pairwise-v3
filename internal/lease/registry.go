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
	ErrOwnershipMismatch = errors.New("previous owner does not match the active lease")
	ErrFencingMismatch   = errors.New("previous fencing token does not match the active lease")
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

func (r *Registry) Assign(vesselID uuid.UUID, owner string, ttl time.Duration, now time.Time) (model.Lease, error) {
	if vesselID == uuid.Nil || owner == "" || ttl <= 0 {
		return model.Lease{}, errors.New("assignment requires vessel, owner and positive ttl")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// An active lease held by a different owner cannot be overwritten; it must
	// expire or be handed over through Takeover with the holder's credential.
	if current, ok := r.leases[vesselID]; ok && current.Owner != owner && current.Active(now) {
		return model.Lease{}, ErrLeaseStillActive
	}
	return r.assignLocked(vesselID, owner, ttl, now)
}

// assignLocked issues the next fencing token and stores the lease. The caller
// must hold r.mu and have already validated that any existing lease may be
// replaced.
func (r *Registry) assignLocked(vesselID uuid.UUID, owner string, ttl time.Duration, now time.Time) (model.Lease, error) {
	r.nextToken[vesselID]++
	lease := model.Lease{
		VesselID: vesselID, Owner: owner, FencingToken: r.nextToken[vesselID],
		ExpiresAt: now.UTC().Add(ttl),
	}
	r.leases[vesselID] = lease
	return lease, nil
}

// Takeover transfers a lease to a new owner atomically. An unexpired lease can
// only move when the requester proves it holds the current lease by naming both
// the current owner and the current fencing token (an explicit authorization
// path). An expired or absent lease may be claimed by anyone without
// credentials. The read-modify-write happens under a single write lock so a
// concurrent renewal cannot be clobbered.
func (r *Registry) Takeover(vesselID uuid.UUID, previousOwner string, previousToken uint64, newOwner string, ttl time.Duration, now time.Time) (model.Lease, error) {
	if vesselID == uuid.Nil || newOwner == "" || ttl <= 0 {
		return model.Lease{}, errors.New("takeover requires vessel, new owner and positive ttl")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.leases[vesselID]
	if ok && current.Active(now) {
		if current.Owner != previousOwner {
			return model.Lease{}, ErrOwnershipMismatch
		}
		if current.FencingToken != previousToken {
			return model.Lease{}, ErrFencingMismatch
		}
	}
	return r.assignLocked(vesselID, newOwner, ttl, now)
}

func (r *Registry) Renew(vesselID uuid.UUID, owner string, token uint64, ttl time.Duration, now time.Time) (model.Lease, error) {
	if ttl <= 0 {
		return model.Lease{}, errors.New("renewal ttl must be positive")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[vesselID]
	if !ok {
		return model.Lease{}, fmt.Errorf("no lease for vessel %s", vesselID)
	}
	if lease.Owner != owner || lease.FencingToken != token {
		return model.Lease{}, errors.New("lease owner or fencing token is stale")
	}
	if !lease.Active(now) {
		return model.Lease{}, errors.New("lease has expired")
	}
	lease.ExpiresAt = now.UTC().Add(ttl)
	r.leases[vesselID] = lease
	return lease, nil
}

func (r *Registry) Get(vesselID uuid.UUID) (model.Lease, bool) {
	r.mu.RLock()
	lease, ok := r.leases[vesselID]
	r.mu.RUnlock()
	return lease, ok
}

func (r *Registry) Release(vesselID uuid.UUID, owner string, token uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	lease, ok := r.leases[vesselID]
	if !ok || lease.Owner != owner || lease.FencingToken != token {
		return false
	}
	delete(r.leases, vesselID)
	return true
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
