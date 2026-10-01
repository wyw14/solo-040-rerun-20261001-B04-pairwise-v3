package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/lease"
	"github.com/wyw14/cry-96/internal/signal"
)

// leaseOnlyRuntime wires just the lease graph with a controllable clock so
// tests can deterministically move leases past expiry.
func leaseOnlyRuntime(now func() time.Time) *Runtime {
	registry := lease.NewRegistry()
	controller := admission.NewController(admission.NewQueue(), gate.NewState("chamber-1"), signal.NewStore(), now)
	recorder := journal.NewRecorder(journal.NewMemoryStore(), now)
	return &Runtime{
		Recorder:     recorder,
		Leases:       lease.NewService(registry, controller, recorder, now),
		LeaseSweeper: lease.NewSweeper(registry, now),
		Now:          now,
	}
}

func leaseRouter(t *testing.T) (*Runtime, http.Handler, func(time.Duration)) {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var mu sync.Mutex
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
	runtime := leaseOnlyRuntime(now)
	return runtime, Router(runtime, ""), advance
}

func doJSON(t *testing.T, handler http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var decoded map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("decode %q: %v body=%s", path, err, rec.Body.String())
		}
	}
	return rec.Code, decoded
}

// End-to-end reproduction of the reported takeover flaw through the HTTP API.
func TestReassignLeaseAuthorizationPath(t *testing.T) {
	runtime, handler, advance := leaseRouter(t)
	vessel := uuid.New().String()

	// A acquires a controlling lease.
	status, body := doJSON(t, handler, http.MethodPost, "/api/leases", map[string]any{
		"chamber_id": "chamber-1", "vessel_id": vessel, "owner": "A", "ttl_seconds": 30,
	})
	if status != http.StatusOK {
		t.Fatalf("assign status=%d body=%v", status, body)
	}
	oldToken := uint64(body["fencing_token"].(float64))

	// B attempts a takeover while A's lease is active with a mismatched
	// previous_owner: must be rejected, lease stays with A, old token valid.
	status, body = doJSON(t, handler, http.MethodPost, "/api/leases/"+vessel+"/reassign", map[string]any{
		"chamber_id": "chamber-1", "previous_owner": "not-A", "new_owner": "B", "ttl_seconds": 30,
	})
	if status != http.StatusConflict {
		t.Fatalf("mismatched active takeover status=%d body=%v", status, body)
	}
	if !runtime.Leases.Authorized(mustUUID(t, vessel), "A", oldToken) {
		t.Fatal("A's authorization was lost after rejected takeover")
	}

	// A takeover naming the correct owner is still refused while active.
	status, _ = doJSON(t, handler, http.MethodPost, "/api/leases/"+vessel+"/reassign", map[string]any{
		"chamber_id": "chamber-1", "previous_owner": "A", "new_owner": "B", "ttl_seconds": 30,
	})
	if status != http.StatusConflict {
		t.Fatalf("active takeover status=%d want 409", status)
	}

	// Expire A's lease, then the authorized succession (correct previous owner)
	// succeeds and yields a fresh fencing token.
	advance(31 * time.Second)
	status, body = doJSON(t, handler, http.MethodPost, "/api/leases/"+vessel+"/reassign", map[string]any{
		"chamber_id": "chamber-1", "previous_owner": "impostor", "new_owner": "B", "ttl_seconds": 30,
	})
	if status != http.StatusConflict {
		t.Fatalf("mismatched expired takeover status=%d want 409", status)
	}
	status, body = doJSON(t, handler, http.MethodPost, "/api/leases/"+vessel+"/reassign", map[string]any{
		"chamber_id": "chamber-1", "previous_owner": "A", "new_owner": "B", "ttl_seconds": 30,
	})
	if status != http.StatusOK {
		t.Fatalf("authorized expired takeover status=%d body=%v", status, body)
	}
	newToken := uint64(body["fencing_token"].(float64))
	if body["owner"] != "B" || newToken != oldToken+1 {
		t.Fatalf("unexpected succession body=%v", body)
	}

	// Old fencing token is dead for admission; new token authorizes B.
	if runtime.Leases.Authorized(mustUUID(t, vessel), "A", oldToken) {
		t.Fatal("old owner/token still authorized after succession")
	}
	if !runtime.Leases.Authorized(mustUUID(t, vessel), "B", newToken) {
		t.Fatal("new owner is not authorized after succession")
	}

	// A cannot revive the old lease via a heartbeat with the stale token.
	status, _ = doJSON(t, handler, http.MethodPost, "/api/leases/"+vessel+"/heartbeat", map[string]any{
		"chamber_id": "chamber-1", "owner": "A", "token": oldToken, "ttl_seconds": 30,
	})
	if status != http.StatusConflict {
		t.Fatalf("stale heartbeat status=%d want 409", status)
	}
}

func TestReassignLeaseInputValidation(t *testing.T) {
	_, handler, _ := leaseRouter(t)
	vessel := uuid.New().String()
	cases := []map[string]any{
		{"previous_owner": "A", "new_owner": "B", "ttl_seconds": 30}, // missing chamber
		{"chamber_id": "chamber-1", "new_owner": "B", "ttl_seconds": 30},
		{"chamber_id": "chamber-1", "previous_owner": "A", "ttl_seconds": 30},
		{"chamber_id": "chamber-1", "previous_owner": "A", "new_owner": "A", "ttl_seconds": 30},
	}
	for i, body := range cases {
		status, resp := doJSON(t, handler, http.MethodPost, "/api/leases/"+vessel+"/reassign", body)
		if status != http.StatusBadRequest {
			t.Fatalf("case %d status=%d body=%v want 400", i, status, resp)
		}
	}
}

func mustUUID(t *testing.T, value string) uuid.UUID {
	t.Helper()
	parsed, err := uuid.Parse(value)
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}
	return parsed
}
