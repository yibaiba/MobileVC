package main

import (
	"fmt"
	"net"
	"net/http"

	"mobilevc/internal/logx"
	"mobilevc/internal/relay"
)

var version = "dev"

func main() {
	logx.Info("relay", "starting MobileVC relay")
	cfg, err := relay.LoadConfigFromEnv()
	if err != nil {
		logx.Error("relay", "load relay config failed: %v", err)
		panic(err)
	}
	server, err := relay.NewServer(cfg)
	if err != nil {
		logx.Error("relay", "initialize relay failed: %v", err)
		panic(err)
	}
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		logx.Error("relay", "listen failed: %v", err)
		panic(fmt.Errorf("listen tcp %s: %w", cfg.Addr, err))
	}
	logx.Info("relay", "ready: addr=%s publicURL=%s", cfg.Addr, cfg.PublicURL)
	if err := http.Serve(listener, server.Handler(version)); err != nil {
		logx.Error("relay", "server stopped unexpectedly: %v", err)
		panic(err)
	}
}
