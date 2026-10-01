package lease

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Lease-change discriminators carried in journal payloads. They make every
// ownership transition explicit so a replay or audit can tell a renewal apart
// from an authorized succession.
const (
	changeAssigned  = "assigned"
	changeRenewed   = "renewed"
	changeTakenOver = "taken_over"
	changeReleased  = "released"
)

// changePayload is the single journal shape for lease events. The previous_*
// fields are populated only on succession, where they record the authorization
// path (which owner and fencing token were replaced).
type changePayload struct {
	VesselID             uuid.UUID  `json:"vessel_id"`
	Change               string     `json:"change"`
	Owner                string     `json:"owner"`
	PreviousOwner        string     `json:"previous_owner,omitempty"`
	FencingToken         uint64     `json:"fencing_token"`
	PreviousFencingToken uint64     `json:"previous_fencing_token,omitempty"`
	ExpiresAt            *time.Time `json:"expires_at,omitempty"`
	Released             bool       `json:"released,omitempty"`
}

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
	expiresAt := lease.ExpiresAt
	if _, err := s.recorder.Record(ctx, chamberID, model.EventLeaseChanged, uuid.Nil, 0, changePayload{
		VesselID: vesselID, Change: changeAssigned, Owner: owner,
		FencingToken: lease.FencingToken, ExpiresAt: &expiresAt,
	}); err != nil {
		// Never leave an unjournaled lease able to authorize admission.
		s.registry.RollbackAssign(vesselID, lease.FencingToken)
		return model.Lease{}, err
	}
	return lease, nil
}

func (s *Service) Heartbeat(ctx context.Context, chamberID string, vesselID uuid.UUID, owner string, token uint64, ttl time.Duration) (model.Lease, error) {
	lease, previous, err := s.registry.Renew(vesselID, owner, token, ttl, s.now())
	if err != nil {
		return model.Lease{}, err
	}
	expiresAt := lease.ExpiresAt
	if _, err := s.recorder.Record(ctx, chamberID, model.EventLeaseChanged, uuid.Nil, token, changePayload{
		VesselID: vesselID, Change: changeRenewed, Owner: owner,
		FencingToken: lease.FencingToken, ExpiresAt: &expiresAt,
	}); err != nil {
		s.registry.RollbackRenew(vesselID, lease.FencingToken, previous.ExpiresAt)
		return model.Lease{}, err
	}
	return lease, nil
}

// Takeover hands an expired lease to a new owner. Succession requires the
// caller to name the current owner (previousOwner) and only succeeds once the
// current lease has expired. The new lease is journaled with both the old and
// the new fencing tokens; if the journal write fails the registry change is
// rolled back so the durable log and admission authorization never disagree.
func (s *Service) Takeover(ctx context.Context, chamberID string, vesselID uuid.UUID, previousOwner, newOwner string, ttl time.Duration) (model.Lease, error) {
	lease, previous, err := s.registry.Takeover(vesselID, previousOwner, newOwner, ttl, s.now())
	if err != nil {
		return model.Lease{}, err
	}
	expiresAt := lease.ExpiresAt
	if _, err := s.recorder.Record(ctx, chamberID, model.EventLeaseChanged, uuid.Nil, lease.FencingToken, changePayload{
		VesselID: vesselID, Change: changeTakenOver, Owner: newOwner, PreviousOwner: previousOwner,
		FencingToken: lease.FencingToken, PreviousFencingToken: previous.FencingToken, ExpiresAt: &expiresAt,
	}); err != nil {
		s.registry.RollbackTakeover(vesselID, previous)
		return model.Lease{}, err
	}
	return lease, nil
}

// Authorized reports whether owner may currently act for the vessel, reading
// the live lease. Prefer AuthorizedLease for a lease value a mutation just
// returned, since re-reading the registry can race a concurrent takeover.
func (s *Service) Authorized(vesselID uuid.UUID, owner string, token uint64) bool {
	lease, ok := s.registry.Get(vesselID)
	return ok && s.admission.OwnerAllowed(vesselID, owner, token, lease, s.now())
}

// AuthorizedLease validates a specific lease value without re-reading the
// registry, so a freshly renewed lease is judged on its own fencing token and
// expiry rather than on whatever a racing takeover installed afterwards.
func (s *Service) AuthorizedLease(vesselID uuid.UUID, owner string, lease model.Lease) bool {
	return s.admission.OwnerAllowed(vesselID, owner, lease.FencingToken, lease, s.now())
}

func (s *Service) Release(ctx context.Context, chamberID string, vesselID uuid.UUID, owner string, token uint64) error {
	previous, ok := s.registry.Release(vesselID, owner, token)
	if !ok {
		return ErrLeaseTokenStale
	}
	if _, err := s.recorder.Record(ctx, chamberID, model.EventLeaseChanged, uuid.Nil, token, changePayload{
		VesselID: vesselID, Change: changeReleased, Owner: owner, FencingToken: token, Released: true,
	}); err != nil {
		s.registry.RollbackRelease(previous)
		return err
	}
	return nil
}
