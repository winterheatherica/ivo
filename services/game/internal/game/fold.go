package game

import (
	"context"
	"time"
)

// foldSQL folds events that are already effective into the follower snapshot and
// the season score. Events are selected by folded_at IS NULL, so events recorded
// late still get folded (docs/detail/flow-3-publish-and-reactions.md#leaderboard-fold).
const foldSQL = `
WITH due AS (
    SELECT id FROM game.popularity_events
     WHERE folded_at IS NULL AND effective_at <= $1
     ORDER BY id
     LIMIT 5000
     FOR UPDATE SKIP LOCKED
), folded AS (
    UPDATE game.popularity_events e
       SET folded_at = $1
      FROM due
     WHERE e.id = due.id
    RETURNING e.player_id, e.season_id, e.delta_followers, e.delta_score
), followers AS (
    UPDATE game.follower_snapshots f
       SET followers = GREATEST(f.followers + d.delta, 0), as_of = $1
      FROM (SELECT player_id, sum(delta_followers) AS delta FROM folded GROUP BY player_id) d
     WHERE f.player_id = d.player_id
)
INSERT INTO game.season_scores (season_id, player_id, score, as_of)
SELECT season_id, player_id, sum(delta_score), $1
  FROM folded
 WHERE season_id IS NOT NULL
 GROUP BY season_id, player_id
ON CONFLICT (season_id, player_id)
DO UPDATE SET score = game.season_scores.score + EXCLUDED.score, as_of = $1`

// RunFold runs a fold every FoldInterval until ctx is cancelled.
func (s *Service) RunFold(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.FoldInterval)
	defer ticker.Stop()
	for {
		if err := s.Fold(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("leaderboard fold failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Fold runs one fold. An advisory lock makes sure only one replica folds at
// a time.
func (s *Service) Fold(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var leader bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('game.fold'))`).Scan(&leader); err != nil {
		return err
	}
	if !leader {
		return nil
	}
	tag, err := tx.Exec(ctx, foldSQL, s.clock.Now())
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if n := tag.RowsAffected(); n > 0 {
		s.log.Debug("fold done", "season_scores", n)
	}
	return nil
}
