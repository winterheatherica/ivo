package game

import (
	"time"

	"ivo/pkg/config"
)

// Config is the game service's configuration from the environment.
type Config struct {
	GRPCAddr       string
	DatabaseURL    string
	Brokers        []string
	AutoProvision  bool          // dev: create the player and idol automatically on the first post
	DevSeason      bool          // dev: make sure an active season exists at startup
	RenderEstimate time.Duration // estimated time from posting until the photo is ready
	DailyPostLimit int
	BaseCost       int64
	ExpressCost    int64
	FoldInterval   time.Duration
	RelayInterval  time.Duration
}

// LoadConfig reads Config from the environment.
func LoadConfig() Config {
	return Config{
		GRPCAddr:       config.String("GRPC_ADDR", ":9090"),
		DatabaseURL:    config.String("DATABASE_URL", "postgres://ivo:ivo@localhost:5432/ivo?sslmode=disable"),
		Brokers:        config.List("KAFKA_BROKERS", []string{"localhost:19092"}),
		AutoProvision:  config.Bool("AUTO_PROVISION", false),
		DevSeason:      config.Bool("DEV_SEASON", false),
		RenderEstimate: config.Duration("RENDER_ESTIMATE", 70*time.Minute),
		DailyPostLimit: config.Int("DAILY_POST_LIMIT", 3),
		BaseCost:       int64(config.Int("POST_BASE_COST", 10)),
		ExpressCost:    int64(config.Int("POST_EXPRESS_COST", 10)),
		FoldInterval:   config.Duration("FOLD_INTERVAL", 5*time.Minute),
		RelayInterval:  config.Duration("RELAY_INTERVAL", time.Second),
	}
}
