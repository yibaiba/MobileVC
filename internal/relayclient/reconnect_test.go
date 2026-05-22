package relayclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"mobilevc/internal/gateway"
	"mobilevc/internal/relay"
	"mobilevc/internal/relay/e2ee"
)

func TestNormalizedBackoffDefaults(t *testing.T) {
	backoff := normalizedBackoff(ReconnectBackoff{})
	if backoff.Initial != 500*time.Millisecond {
		t.Fatalf("initial backoff: got %s", backoff.Initial)
	}
	if backoff.Max != 5*time.Second {
		t.Fatalf("max backoff: got %s", backoff.Max)
	}
}

func TestNextBackoffCapsAtMax(t *testing.T) {
	got := nextBackoff(ReconnectBackoff{
		Initial: 4 * time.Second,
		Max:     5 * time.Second,
	})
	if got != 5*time.Second {
		t.Fatalf("next backoff: got %s", got)
	}
}

func TestReconnectDeadlineStartsFromCurrentDisconnect(t *testing.T) {
	grace := 30 * time.Second
	first := reconnectDeadline(grace)
	time.Sleep(time.Millisecond)
	second := reconnectDeadline(grace)
	if !second.After(first) {
		t.Fatalf("deadline was not refreshed: first=%s second=%s", first, second)
	}
}

func TestRunRegistersNewSessionAfterRotate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registrations := make(chan relay.AgentRegisterFrame, 2)
	relayURL := startRelayRegisterStub(t, ctx, registrations)
	nodeStore, err := e2ee.LoadOrCreateNodeIdentityStore(filepath.Join(t.TempDir(), e2ee.NodeIdentityFileName))
	if err != nil {
		t.Fatal(err)
	}
	downloadRoot := t.TempDir()
	events := make(chan PairingReadyEvent, 2)
	handler := &rotateThenStopHandler{cancel: cancel}
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, Config{
			RelayURL:          relayURL,
			PairingTTL:        time.Minute,
			AgentGracePeriod:  time.Minute,
			PairingEventPath:  filepath.Join(t.TempDir(), "pairing.json"),
			DownloadRoots:     []string{downloadRoot},
			Capabilities:      e2ee.ProductionCapabilities(),
			NodeIdentityStore: nodeStore,
		}, handler, func(_ string, event PairingReadyEvent) error {
			events <- event
			return nil
		})
	}()

	firstEvent := readPairingEvent(t, events)
	secondEvent := readPairingEvent(t, events)
	if firstEvent.SessionID == secondEvent.SessionID {
		t.Fatalf("rotate reused relay session id %q", firstEvent.SessionID)
	}
	if firstEvent.PairingSecret == secondEvent.PairingSecret {
		t.Fatal("rotate reused pairing secret")
	}
	firstRegister := readAgentRegister(t, registrations)
	secondRegister := readAgentRegister(t, registrations)
	if firstRegister.SessionID != firstEvent.SessionID || secondRegister.SessionID != secondEvent.SessionID {
		t.Fatalf("registrations did not match pairing events: %#v %#v", firstRegister, secondRegister)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after test cancellation")
	}
}

type rotateThenStopHandler struct {
	calls  atomic.Int32
	cancel context.CancelFunc
}

func (h *rotateThenStopHandler) ServeClientConn(_ context.Context, client gateway.ClientConn) {
	if h.calls.Add(1) == 1 {
		rotator, ok := client.(interface{ RotateRelaySession() error })
		if !ok {
			panic("relay client does not support session rotation")
		}
		_ = rotator.RotateRelaySession()
		return
	}
	h.cancel()
	_ = client.Close()
}

func startRelayRegisterStub(t *testing.T, ctx context.Context, registrations chan<- relay.AgentRegisterFrame) string {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/relay/agent" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade relay register stub: %v", err)
			return
		}
		go func() {
			defer conn.Close()
			var frame relay.AgentRegisterFrame
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			registrations <- frame
			_ = conn.WriteJSON(relay.AgentRegisteredFrame{
				Type: relay.TypeAgentRegistered, Version: relay.Version, SessionID: frame.SessionID,
			})
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				var raw map[string]any
				if err := conn.ReadJSON(&raw); err != nil {
					return
				}
			}
		}()
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func readPairingEvent(t *testing.T, events <-chan PairingReadyEvent) PairingReadyEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pairing event")
	}
	return PairingReadyEvent{}
}

func readAgentRegister(t *testing.T, registrations <-chan relay.AgentRegisterFrame) relay.AgentRegisterFrame {
	t.Helper()
	select {
	case frame := <-registrations:
		return frame
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent registration")
	}
	return relay.AgentRegisterFrame{}
}
