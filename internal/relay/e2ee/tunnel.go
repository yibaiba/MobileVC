package e2ee

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	TunnelVersion = 1

	TunnelFrameStreamOpen  = "stream.open"
	TunnelFrameStreamData  = "stream.data"
	TunnelFrameStreamAck   = "stream.ack"
	TunnelFrameStreamClose = "stream.close"
	TunnelFrameStreamReset = "stream.reset"
	TunnelFrameStreamError = "stream.error"
	TunnelFramePing        = "ping"
	TunnelFramePong        = "pong"

	TunnelStreamMobileVCWS   = "mobilevc.ws"
	TunnelStreamFileDownload = "file.download"
)

type TunnelFrame struct {
	Type       string            `json:"type"`
	Version    int               `json:"version"`
	StreamID   uint64            `json:"streamId,omitempty"`
	StreamType string            `json:"streamType,omitempty"`
	Seq        uint64            `json:"seq,omitempty"`
	Ack        uint64            `json:"ack,omitempty"`
	Window     uint32            `json:"window,omitempty"`
	Payload    []byte            `json:"-"`
	ErrorCode  string            `json:"errorCode,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type tunnelFrameJSON struct {
	Type       string            `json:"type"`
	Version    int               `json:"version"`
	StreamID   uint64            `json:"streamId,omitempty"`
	StreamType string            `json:"streamType,omitempty"`
	Seq        uint64            `json:"seq,omitempty"`
	Ack        uint64            `json:"ack,omitempty"`
	Window     uint32            `json:"window,omitempty"`
	Payload    string            `json:"payload,omitempty"`
	ErrorCode  string            `json:"errorCode,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

func MarshalTunnelFrame(frame TunnelFrame) ([]byte, error) {
	if err := ValidateTunnelFrame(frame); err != nil {
		return nil, err
	}
	wire := tunnelFrameJSON{
		Type: frame.Type, Version: frame.Version, StreamID: frame.StreamID,
		StreamType: frame.StreamType, Seq: frame.Seq, Ack: frame.Ack, Window: frame.Window,
		ErrorCode: frame.ErrorCode, Metadata: sortedMetadata(frame.Metadata),
	}
	if len(frame.Payload) > 0 {
		wire.Payload = base64.RawURLEncoding.EncodeToString(frame.Payload)
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal tunnel frame: %w", err)
	}
	return raw, nil
}

func UnmarshalTunnelFrame(raw []byte) (TunnelFrame, error) {
	var wire tunnelFrameJSON
	if err := json.Unmarshal(raw, &wire); err != nil {
		return TunnelFrame{}, fmt.Errorf("parse tunnel frame: %w", err)
	}
	var payload []byte
	if strings.TrimSpace(wire.Payload) != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(wire.Payload)
		if err != nil {
			return TunnelFrame{}, fmt.Errorf("decode tunnel payload: %w", err)
		}
		payload = decoded
	}
	frame := TunnelFrame{
		Type: wire.Type, Version: wire.Version, StreamID: wire.StreamID,
		StreamType: wire.StreamType, Seq: wire.Seq, Ack: wire.Ack, Window: wire.Window,
		Payload: payload, ErrorCode: wire.ErrorCode, Metadata: wire.Metadata,
	}
	if err := ValidateTunnelFrame(frame); err != nil {
		return TunnelFrame{}, err
	}
	return frame, nil
}

func ValidateTunnelFrame(frame TunnelFrame) error {
	if frame.Version != TunnelVersion {
		return errors.New("invalid tunnel frame version")
	}
	switch frame.Type {
	case TunnelFrameStreamOpen:
		return requireTunnelFields(frame, true, true, false, false, true, false, false)
	case TunnelFrameStreamData:
		return requireTunnelFields(frame, true, false, true, false, false, true, false)
	case TunnelFrameStreamAck:
		return requireTunnelFields(frame, true, false, false, true, true, false, false)
	case TunnelFrameStreamClose:
		return requireTunnelFields(frame, true, false, true, false, false, false, false)
	case TunnelFrameStreamReset:
		return requireTunnelFields(frame, true, false, false, false, false, false, false)
	case TunnelFrameStreamError:
		return requireTunnelFields(frame, true, false, false, false, false, false, true)
	case TunnelFramePing, TunnelFramePong:
		return requireTunnelFields(frame, false, false, false, false, false, false, false)
	default:
		return fmt.Errorf("unknown tunnel frame type: %s", frame.Type)
	}
}

type TunnelCounterState struct {
	nextSeq uint64
	seen    map[uint64]map[uint64]struct{}
	windows map[uint64]uint32
}

func NewTunnelCounterState() *TunnelCounterState {
	return &TunnelCounterState{
		nextSeq: 1,
		seen:    map[uint64]map[uint64]struct{}{},
		windows: map[uint64]uint32{},
	}
}

func (s *TunnelCounterState) NextSeq() uint64 {
	seq := s.nextSeq
	s.nextSeq++
	return seq
}

func (s *TunnelCounterState) Observe(frame TunnelFrame) error {
	if err := ValidateTunnelFrame(frame); err != nil {
		return err
	}
	if frame.Type == TunnelFrameStreamOpen || frame.Type == TunnelFrameStreamAck {
		if frame.Window == 0 {
			return errors.New("stream window exceeded")
		}
		s.windows[frame.StreamID] = frame.Window
	}
	if frame.Type != TunnelFrameStreamData && frame.Type != TunnelFrameStreamClose {
		return nil
	}
	seenByStream := s.seen[frame.StreamID]
	if seenByStream == nil {
		seenByStream = map[uint64]struct{}{}
		s.seen[frame.StreamID] = seenByStream
	}
	if _, ok := seenByStream[frame.Seq]; ok {
		return errors.New("e2ee replay detected")
	}
	seenByStream[frame.Seq] = struct{}{}
	return nil
}

func requireTunnelFields(
	frame TunnelFrame,
	streamID bool,
	streamType bool,
	seq bool,
	ack bool,
	window bool,
	payload bool,
	errorCode bool,
) error {
	if streamID && frame.StreamID == 0 {
		return errors.New("tunnel frame missing streamId")
	}
	if streamType && strings.TrimSpace(frame.StreamType) == "" {
		return errors.New("tunnel frame missing streamType")
	}
	if seq && frame.Seq == 0 {
		return errors.New("tunnel frame missing seq")
	}
	if ack && frame.Ack == 0 {
		return errors.New("tunnel frame missing ack")
	}
	if window && frame.Window == 0 {
		return errors.New("tunnel frame missing window")
	}
	if payload && len(frame.Payload) == 0 {
		return errors.New("tunnel frame missing payload")
	}
	if errorCode && strings.TrimSpace(frame.ErrorCode) == "" {
		return errors.New("tunnel frame missing errorCode")
	}
	return nil
}

func sortedMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(metadata))
	for _, key := range keys {
		out[key] = metadata[key]
	}
	return out
}
