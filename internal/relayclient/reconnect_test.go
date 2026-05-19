package relayclient

import (
	"testing"
	"time"
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
