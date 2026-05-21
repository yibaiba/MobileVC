package e2ee

import (
	"strings"
	"testing"
)

func TestTunnelFrameRoundTripValidatesRequiredFields(t *testing.T) {
	frame := TunnelFrame{
		Type: TunnelFrameStreamOpen, Version: TunnelVersion,
		StreamID: 7, StreamType: TunnelStreamMobileVCWS, Window: 32,
		Metadata: map[string]string{"route": "/ws"},
	}

	raw, err := MarshalTunnelFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalTunnelFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != frame.Type || decoded.StreamID != frame.StreamID || decoded.Metadata["route"] != "/ws" {
		t.Fatalf("decoded frame mismatch: %#v", decoded)
	}

	invalid := frame
	invalid.StreamType = ""
	if err := ValidateTunnelFrame(invalid); err == nil {
		t.Fatal("accepted stream.open without streamType")
	}
}

func TestTunnelFrameDataUsesBase64Payload(t *testing.T) {
	frame := TunnelFrame{
		Type: TunnelFrameStreamData, Version: TunnelVersion,
		StreamID: 7, Seq: 1, Payload: []byte("secret bytes"),
	}
	raw, err := MarshalTunnelFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret") {
		t.Fatalf("tunnel frame leaked plaintext payload in JSON: %s", raw)
	}
	decoded, err := UnmarshalTunnelFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded.Payload) != "secret bytes" {
		t.Fatalf("payload mismatch: %q", decoded.Payload)
	}
}

func TestTunnelCounterStateRejectsReplayPerStreamAndTracksWindows(t *testing.T) {
	state := NewTunnelCounterState()
	open := TunnelFrame{
		Type: TunnelFrameStreamOpen, Version: TunnelVersion,
		StreamID: 7, StreamType: TunnelStreamFileDownload, Window: 8,
	}
	if err := state.Observe(open); err != nil {
		t.Fatal(err)
	}
	seq := state.NextSeq()
	if seq != 1 {
		t.Fatalf("first seq: got %d want 1", seq)
	}
	data := TunnelFrame{
		Type: TunnelFrameStreamData, Version: TunnelVersion,
		StreamID: 7, Seq: seq, Payload: []byte("chunk"),
	}
	if err := state.Observe(data); err != nil {
		t.Fatal(err)
	}
	if err := state.Observe(data); err == nil || !strings.Contains(err.Error(), "replay") {
		t.Fatalf("expected replay failure, got %v", err)
	}
	otherStream := data
	otherStream.StreamID = 8
	if err := state.Observe(otherStream); err != nil {
		t.Fatalf("same seq on different stream should be independent: %v", err)
	}
}

func TestTunnelCounterStateRejectsZeroWindow(t *testing.T) {
	state := NewTunnelCounterState()
	err := state.Observe(TunnelFrame{
		Type: TunnelFrameStreamOpen, Version: TunnelVersion,
		StreamID: 7, StreamType: TunnelStreamFileDownload,
	})
	if err == nil || !strings.Contains(err.Error(), "window") {
		t.Fatalf("expected zero window failure, got %v", err)
	}
}
