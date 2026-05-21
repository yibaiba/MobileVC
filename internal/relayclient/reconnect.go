package relayclient

import (
	"context"
	"fmt"
	"time"

	"github.com/gorilla/websocket"

	"mobilevc/internal/gateway"
)

type Handler interface {
	ServeClientConn(context.Context, gateway.ClientConn)
}

func serveWithReconnect(ctx context.Context, cfg Config, handler Handler, conn *websocket.Conn, sessionID string, pairingSecret string, reconnectSecret string) error {
	for {
		e2eeHandler := newAgentE2EEHandshakeHandlerWithDeviceTrust(
			sessionID,
			pairingSecret,
			relayClientCapabilities(cfg),
			cfg.NodeIdentity,
			cfg.DeviceTrust,
		)
		handler.ServeClientConn(ctx, newGatewayConnWithE2EE(conn, sessionID, e2eeHandler))
		_ = conn.Close()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		deadline := reconnectDeadline(cfg.AgentGracePeriod)
		nextConn, err := reconnectWithinGrace(ctx, cfg, sessionID, reconnectSecret, deadline)
		if err != nil {
			return err
		}
		conn = nextConn
	}
}

func reconnectWithinGrace(ctx context.Context, cfg Config, sessionID string, reconnectSecret string, deadline time.Time) (*websocket.Conn, error) {
	backoff := normalizedBackoff(cfg.ReconnectBackoff)
	for time.Now().Before(deadline) {
		conn, err := dialAgent(ctx, cfg.RelayURL)
		if err == nil {
			req := agentReconnectRequest{SessionID: sessionID, ReconnectSecret: reconnectSecret}
			if err = reconnectAgent(conn, req); err == nil {
				return conn, nil
			}
			_ = conn.Close()
		}
		if err := sleepBackoff(ctx, backoff.Initial); err != nil {
			return nil, err
		}
		backoff.Initial = nextBackoff(backoff)
	}
	return nil, fmt.Errorf("relay agent reconnect grace period expired")
}

func reconnectDeadline(grace time.Duration) time.Time {
	return time.Now().Add(grace)
}

func normalizedBackoff(backoff ReconnectBackoff) ReconnectBackoff {
	if backoff.Initial <= 0 {
		backoff.Initial = 500 * time.Millisecond
	}
	if backoff.Max <= 0 {
		backoff.Max = 5 * time.Second
	}
	return backoff
}

func sleepBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(backoff ReconnectBackoff) time.Duration {
	next := backoff.Initial * 2
	if next > backoff.Max {
		return backoff.Max
	}
	return next
}
