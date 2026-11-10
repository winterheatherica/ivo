// Command gateway runs the REST API for the app and the admin web app.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	gamev1 "ivo/gen/ivo/game/v1"
	netizenv1 "ivo/gen/ivo/netizen/v1"
	"ivo/pkg/grpcx"
	"ivo/pkg/logging"
	"ivo/services/gateway/internal/gateway"
)

func main() {
	log := logging.New("gateway")
	if err := run(log); err != nil {
		log.Error("gateway stopped with an error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := gateway.LoadConfig()

	gameConn, err := grpcx.Dial(cfg.GameAddr)
	if err != nil {
		return err
	}
	defer gameConn.Close()
	netizenConn, err := grpcx.Dial(cfg.NetizenAddr)
	if err != nil {
		return err
	}
	defer netizenConn.Close()

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: gateway.New(gamev1.NewGameServiceClient(gameConn),
			netizenv1.NewNetizenServiceClient(netizenConn), log, cfg).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Info("gateway is running", "http", cfg.HTTPAddr, "auth_mode", cfg.AuthMode)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
