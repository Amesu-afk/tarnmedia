package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tarnveil/tarnmedia/internal/config"
	"github.com/tarnveil/tarnmedia/internal/sfu"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	mediaServer, err := sfu.New(cfg)
	if err != nil {
		slog.Error("initialize TarnMedia", "error", err)
		os.Exit(1)
	}

	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mediaServer.Handler(),
		ReadHeaderTimeout: 8 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	stopMaintenance := make(chan struct{})
	go mediaServer.RunMaintenance(stopMaintenance)
	go func() {
		slog.Info("TarnMedia listening", "address", cfg.Addr, "udpMin", cfg.UDPMin, "udpMax", cfg.UDPMax)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("TarnMedia stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	shutdownSignal, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	<-shutdownSignal.Done()
	close(stopMaintenance)

	ctx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(ctx)
	mediaServer.Close()
}
