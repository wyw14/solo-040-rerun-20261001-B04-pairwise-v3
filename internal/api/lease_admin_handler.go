package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func expiredLeasesHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, runtime.LeaseSweeper.Expired())
	}
}

type reassignInput struct {
	ChamberID     string `json:"chamber_id"`
	PreviousOwner string `json:"previous_owner"`
	PreviousToken uint64 `json:"previous_token"`
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
		vesselID, err := parseUUID(chi.URLParam(r, "vesselID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		value, err := runtime.Leases.Takeover(
			r.Context(), input.ChamberID, vesselID,
			input.PreviousOwner, input.PreviousToken,
			input.NewOwner, durationSeconds(input.TTLSeconds),
		)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, value)
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
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"released": true})
	}
}
