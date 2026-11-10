// Command imaging runs the imaging service: the post.submitted consumer, the
// batch dispatcher, and the outbox relay.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"ivo/pkg/clock"
	"ivo/pkg/kafka"
	"ivo/pkg/logging"
	"ivo/pkg/outbox"
	"ivo/pkg/pgdb"
	"ivo/services/imaging/internal/imaging"
	"ivo/services/imaging/migrations"
)

func main() {
	log := logging.New("imaging")
	if err := run(log); err != nil {
		log.Error("imaging stopped with an error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := imaging.LoadConfig()

	pool, err := pgdb.Open(ctx, cfg.DatabaseURL, log)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pgdb.Migrate(ctx, pool, "imaging", migrations.Files); err != nil {
		return err
	}

	clk := clock.Real{}
	svc := imaging.New(pool, clk, log, cfg, imaging.Placeholder{})
	producer, err := kafka.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	relay := &outbox.Relay{
		Pool:       pool,
		Schema:     "imaging",
		Publishers: map[string]outbox.Publisher{outbox.DestinationKafka: producer},
		Clock:      clk,
		Interval:   cfg.RelayInterval,
		BatchSize:  100,
		Log:        log,
	}
	consumer := &kafka.Consumer{
		Brokers:  cfg.Brokers,
		Group:    "imaging",
		Name:     "imaging",
		Handlers: svc.Handlers(),
		Log:      log,
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return relay.Run(gctx) })
	g.Go(func() error { return consumer.Run(gctx) })
	g.Go(func() error { return svc.RunDispatcher(gctx) })
	log.Info("imaging is running", "renderer", "placeholder", "batch_interval", cfg.BatchInterval)
	return g.Wait()
}
