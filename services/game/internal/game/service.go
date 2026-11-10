// Package game implements the game service: posts, popularity, seasons, and
// the leaderboard (design §8, docs/detail/flow-1-create-post.md).
package game

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	gamev1 "ivo/gen/ivo/game/v1"
	"ivo/pkg/clock"
	"ivo/pkg/ids"
)

const (
	schema = "game"
	source = "ivo/game"
)

// Service implements GameService and game's event consumers.
type Service struct {
	gamev1.UnimplementedGameServiceServer

	pool    *pgxpool.Pool
	clock   clock.Clock
	log     *slog.Logger
	cfg     Config
	credits Credits
}

// New creates a Service.
func New(pool *pgxpool.Pool, clk clock.Clock, log *slog.Logger, cfg Config, credits Credits) *Service {
	return &Service{pool: pool, clock: clk, log: log, cfg: cfg, credits: credits}
}

// querier is satisfied by pgxpool.Pool and pgx.Tx.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// EnsureDevSeason makes sure there is an active season covering the current time.
// Dev environments only; in production, seasons are created through the admin tools.
func (s *Service) EnsureDevSeason(ctx context.Context) error {
	now := s.clock.Now()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var endsAt time.Time
	err = tx.QueryRow(ctx, `SELECT ends_at FROM game.seasons WHERE status = 'active' FOR UPDATE`).Scan(&endsAt)
	switch {
	case err == nil && endsAt.After(now):
		return nil
	case err == nil:
		if _, err := tx.Exec(ctx, `UPDATE game.seasons SET status = 'closed' WHERE status = 'active'`); err != nil {
			return err
		}
	case !errors.Is(err, pgx.ErrNoRows):
		return err
	}

	start := now.Truncate(24 * time.Hour)
	var lastEnd *time.Time
	if err := tx.QueryRow(ctx, `SELECT max(ends_at) FROM game.seasons`).Scan(&lastEnd); err != nil {
		return err
	}
	if lastEnd != nil && lastEnd.After(start) {
		start = *lastEnd
	}
	if _, err := tx.Exec(ctx, `INSERT INTO game.seasons (season_id, name, starts_at, ends_at, rules_version, status)
		VALUES ($1, $2, $3, $4, $5, 'active')`,
		ids.New(), "Season "+start.Format("2006-01"), start, start.Add(90*24*time.Hour), RulesVersion); err != nil {
		return err
	}
	s.log.Info("dev season created", "starts_at", start)
	return tx.Commit(ctx)
}
