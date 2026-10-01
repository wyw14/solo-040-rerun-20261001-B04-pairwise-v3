package api

import (
	"errors"
	"net/http"

	"github.com/wyw14/cry-96/internal/model"
)

func chambersHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, runtime.Snapshot.Chambers(runtime.Chambers))
	}
}

func interlocksHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := make(map[string]any, len(runtime.Chambers))
		for _, chamberID := range runtime.Chambers {
			reservation, reserved := runtime.Interlocks.Reservation(chamberID)
			var reservationValue any
			if reserved {
				reservationValue = reservation
			}
			result[chamberID] = map[string]any{
				"equipment":         runtime.Interlocks.Snapshot(chamberID),
				"emergency":         runtime.Safety.Status(chamberID),
				"pending_commands":  runtime.GateCommands.Pending(chamberID),
				"pending_equipment": runtime.Safety.Barrier().Pending(chamberID),
				"reservation":       reservationValue,
			}
		}
		result["signal_handoffs"] = runtime.Signals.History()
		writeJSON(w, http.StatusOK, result)
	}
}

func incidentsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		incidents, err := runtime.Incidents.List(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, incidents)
	}
}

type leaseInput struct {
	ChamberID  string `json:"chamber_id"`
	VesselID   string `json:"vessel_id"`
	Owner      string `json:"owner"`
	Token      uint64 `json:"token,omitempty"`
	TTLSeconds int    `json:"ttl_seconds"`
}

func assignLeaseHandler(runtime *Runtime) http.HandlerFunc {
	return leaseMutationHandler(runtime, false)
}

func heartbeatHandler(runtime *Runtime) http.HandlerFunc {
	return leaseMutationHandler(runtime, true)
}

func leaseMutationHandler(runtime *Runtime, heartbeat bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input leaseInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if heartbeat {
			input.VesselID = chiURLParam(r, "vesselID")
		}
		vesselID, err := parseUUID(input.VesselID)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		ttl := durationSeconds(input.TTLSeconds)
		var result model.Lease
		if heartbeat {
			result, err = runtime.Leases.Heartbeat(r.Context(), input.ChamberID, vesselID, input.Owner, input.Token, ttl)
			// Validate the lease the renewal actually returned. Re-reading the
			// registry here would race a concurrent takeover that can install
			// a newer fencing token between the renewal and the check.
			if err == nil && !runtime.Leases.AuthorizedLease(vesselID, input.Owner, result) {
				err = errors.New("renewed lease is not authorized for admission")
			}
		} else {
			result, err = runtime.Leases.Assign(r.Context(), input.ChamberID, vesselID, input.Owner, ttl)
		}
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}
