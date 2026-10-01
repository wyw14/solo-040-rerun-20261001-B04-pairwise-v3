package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/wyw14/cry-96/internal/lease"
)

func expiredLeasesHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, runtime.LeaseSweeper.Expired())
	}
}

type reassignInput struct {
	ChamberID     string `json:"chamber_id"`
	PreviousOwner string `json:"previous_owner"`
	NewOwner      string `json:"new_owner"`
	TTLSeconds    int    `json:"ttl_seconds"`
}

func reassignLeaseHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input reassignInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if input.ChamberID == "" || input.PreviousOwner == "" || input.NewOwner == "" {
			writeError(w, http.StatusBadRequest, errors.New("chamber_id, previous_owner and new_owner are required"))
			return
		}
		if input.PreviousOwner == input.NewOwner {
			writeError(w, http.StatusBadRequest, errors.New("new_owner must differ from previous_owner"))
			return
		}
		vesselID, err := parseUUID(chi.URLParam(r, "vesselID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		value, err := runtime.Leases.Takeover(
			r.Context(), input.ChamberID, vesselID, input.PreviousOwner, input.NewOwner,
			durationSeconds(input.TTLSeconds),
		)
		if err != nil {
			writeError(w, leaseErrorStatus(err), err)
			return
		}
		writeJSON(w, http.StatusOK, value)
	}
}

// leaseErrorStatus maps succession failures to 409: a mismatched previous
// owner, an active lease or an absent lease all describe a state in which the
// requested ownership transition cannot be performed.
func leaseErrorStatus(err error) int {
	switch {
	case errors.Is(err, lease.ErrLeaseStillActive),
		errors.Is(err, lease.ErrLeaseOwnerMismatch),
		errors.Is(err, lease.ErrLeaseNotFound),
		errors.Is(err, lease.ErrLeaseExists),
		errors.Is(err, lease.ErrLeaseTokenStale):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

type releaseLeaseInput struct {
	ChamberID string `json:"chamber_id"`
	Owner     string `json:"owner"`
	Token     uint64 `json:"token"`
}

func releaseLeaseHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input releaseLeaseInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		vesselID, err := parseUUID(chi.URLParam(r, "vesselID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := runtime.Leases.Release(r.Context(), input.ChamberID, vesselID, input.Owner, input.Token); err != nil {
			writeError(w, leaseErrorStatus(err), err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"released": true})
	}
}
