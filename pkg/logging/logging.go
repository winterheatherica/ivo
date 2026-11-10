// Package logging sets up a JSON logger. Captions and comment text must never
// be written to the logs (design §5).
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New creates a JSON logger to stdout with a service field.
func New(service string) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}
