package lease

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

var ErrLeaseStillActive = errors.New("lease is still active")

// Service journals ownership changes and exposes admission authorization.
type Service struct {
	registry  *Registry
	admission *admission.Controller
	recorder  *journal.Recorder
	now       func() time.Time
}

func NewService(registry *Registry, admissionController *admission.Controller, recorder *journal.Recorder, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{registry: registry, admission: admissionController, recorder: recorder, now: now}
}

func (s *Service) Assign(ctx context.Context, chamberID string, vesselID uuid.UUID, owner string, ttl time.Duration) (model.Lease, error) {
	lease, err := s.registry.Assign(vesselID, owner, ttl, s.now())
	if err != nil {
		return model.Lease{}, err
	}
	if !s.admission.OwnerAllowed(vesselID, owner, lease.FencingToken, lease, s.now()) {
		return model.Lease{}, errors.New("new lease is not authorized for admission")
	}
	_, err = s.recorder.Record(ctx, chamberID, model.EventLeaseChanged, uuid.Nil, lease.FencingToken, lease)
	return lease, err
}

// Takeover hands an active lease to a new owner only through an explicit
// authorization path: the caller must present the holder's current owner and
// fencing token. An expired or absent lease can be claimed without
// credentials. The registry transfer is atomic, so a concurrent renewal cannot
// be preempted by a request that did not prove ownership. On success the new
// token is journaled and the old credential is confirmed fenced, keeping the
// token, journal and admission authorization consistent.
func (s *Service) Takeover(ctx context.Context, chamberID string, vesselID uuid.UUID, previousOwner string, previousToken uint64, newOwner string, ttl time.Duration) (model.Lease, error) {
	lease, err := s.registry.Takeover(vesselID, previousOwner, previousToken, newOwner, ttl, s.now())
	if err != nil {
		return model.Lease{}, err
	}
	if !s.admission.OwnerAllowed(vesselID, newOwner, lease.FencingToken, lease, s.now()) {
		return model.Lease{}, errors.New("new lease is not authorized for admission")
	}
	if s.Authorized(vesselID, previousOwner, previousToken) {
		return model.Lease{}, errors.New("previous owner credential is still authorized after takeover")
	}
	if _, err := s.recorder.Record(ctx, chamberID, model.EventLeaseChanged, uuid.Nil, lease.FencingToken, lease); err != nil {
		return model.Lease{}, err
	}
	return lease, nil
}

func (s *Service) Heartbeat(ctx context.Context, chamberID string, vesselID uuid.UUID, owner string, token uint64, ttl time.Duration) (model.Lease, error) {
	lease, err := s.registry.Renew(vesselID, owner, token, ttl, s.now())
	if err != nil {
		return model.Lease{}, err
	}
	_, err = s.recorder.Record(ctx, chamberID, model.EventLeaseChanged, uuid.Nil, token, lease)
	return lease, err
}

func (s *Service) Authorized(vesselID uuid.UUID, owner string, token uint64) bool {
	lease, ok := s.registry.Get(vesselID)
	return ok && s.admission.OwnerAllowed(vesselID, owner, token, lease, s.now())
}

func (s *Service) Release(ctx context.Context, chamberID string, vesselID uuid.UUID, owner string, token uint64) error {
	if !s.registry.Release(vesselID, owner, token) {
		return errors.New("lease owner or fencing token is stale")
	}
	_, err := s.recorder.Record(ctx, chamberID, model.EventLeaseChanged, uuid.Nil, token, map[string]any{
		"vessel_id": vesselID, "owner": owner, "released": true,
	})
	return err
}
