// Command netizen runs the netizen service: the reaction consumer, the comment
// worker, and the gRPC NetizenService.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	gamev1 "ivo/gen/ivo/game/v1"
	netizenv1 "ivo/gen/ivo/netizen/v1"
	"ivo/pkg/clock"
	"ivo/pkg/grpcx"
	"ivo/pkg/kafka"
	"ivo/pkg/logging"
	"ivo/services/netizen/internal/engine"
	"ivo/services/netizen/internal/netizen"
)

func main() {
	log := logging.New("netizen")
	if err := run(log); err != nil {
		log.Error("netizen stopped with an error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := netizen.LoadConfig()

	client, store, err := netizen.OpenStore(ctx, cfg.MongoURI, cfg.MongoDB, log)
	if err != nil {
		return err
	}
	defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()
	if err := store.SeedPersonas(ctx, engine.Generate(cfg.PersonaCount)); err != nil {
		return err
	}
	personas, err := store.LoadPersonas(ctx)
	if err != nil {
		return err
	}

	gameConn, err := grpcx.Dial(cfg.GameAddr)
	if err != nil {
		return err
	}
	defer gameConn.Close()
	producer, err := kafka.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	svc := netizen.New(store, clock.Real{}, log, cfg, producer, gamev1.NewGameServiceClient(gameConn), personas)
	srv := grpcx.NewServer(log)
	netizenv1.RegisterNetizenServiceServer(srv, svc)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return grpcx.Serve(gctx, srv, cfg.GRPCAddr) })
	if cfg.RunReactions {
		reactions := &kafka.Consumer{Brokers: cfg.Brokers, Group: "netizen", Name: "netizen",
			Handlers: svc.ReactionHandlers(), Log: log}
		g.Go(func() error { return reactions.Run(gctx) })
	}
	if cfg.RunComments {
		comments := &kafka.Consumer{Brokers: cfg.Brokers, Group: "netizen-comment", Name: "netizen",
			Handlers: svc.CommentHandlers(), Log: log}
		g.Go(func() error { return comments.Run(gctx) })
	}
	log.Info("netizen is running", "grpc", cfg.GRPCAddr, "personas", len(personas),
		"delay_scale_bp", cfg.DelayScaleBP)
	return g.Wait()
}
