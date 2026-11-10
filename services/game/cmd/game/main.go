// Command game runs the game service: the gRPC GameService, the outbox relay,
// the event consumers, and the leaderboard fold.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	_ "time/tzdata" // destination time zones without depending on the image

	"golang.org/x/sync/errgroup"

	gamev1 "ivo/gen/ivo/game/v1"
	"ivo/pkg/clock"
	"ivo/pkg/grpcx"
	"ivo/pkg/kafka"
	"ivo/pkg/logging"
	"ivo/pkg/outbox"
	"ivo/pkg/pgdb"
	"ivo/services/game/internal/game"
	"ivo/services/game/migrations"
)

func main() {
	log := logging.New("game")
	if err := run(log); err != nil {
		log.Error("game stopped with an error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := game.LoadConfig()

	pool, err := pgdb.Open(ctx, cfg.DatabaseURL, log)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pgdb.Migrate(ctx, pool, "game", migrations.Files); err != nil {
		return err
	}

	clk := clock.Real{}
	svc := game.New(pool, clk, log, cfg, game.Free{})
	if cfg.DevSeason {
		if err := svc.EnsureDevSeason(ctx); err != nil {
			return err
		}
	}

	producer, err := kafka.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	relay := &outbox.Relay{
		Pool:       pool,
		Schema:     "game",
		Publishers: map[string]outbox.Publisher{outbox.DestinationKafka: producer},
		Clock:      clk,
		Interval:   cfg.RelayInterval,
		BatchSize:  100,
		Log:        log,
	}
	consumer := &kafka.Consumer{
		Brokers:  cfg.Brokers,
		Group:    "game",
		Name:     "game",
		Handlers: svc.Handlers(),
		Log:      log,
	}
	srv := grpcx.NewServer(log)
	gamev1.RegisterGameServiceServer(srv, svc)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return grpcx.Serve(gctx, srv, cfg.GRPCAddr) })
	g.Go(func() error { return relay.Run(gctx) })
	g.Go(func() error { return consumer.Run(gctx) })
	g.Go(func() error { return svc.RunFold(gctx) })
	log.Info("game is running", "grpc", cfg.GRPCAddr)
	return g.Wait()
}
