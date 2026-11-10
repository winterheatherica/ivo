package game

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	gamev1 "ivo/gen/ivo/game/v1"
	imagingv1 "ivo/gen/ivo/imaging/v1"
	netizenv1 "ivo/gen/ivo/netizen/v1"
	"ivo/pkg/clock"
	"ivo/pkg/events"
	"ivo/pkg/kafka"
	"ivo/pkg/outbox"
)

// frozenJSON is the shape of FrozenInputs stored in the posts.frozen_inputs column.
type frozenJSON struct {
	RulesVersion       int32 `json:"rules_version"`
	Followers          int64 `json:"followers"`
	LocationNoveltyBP  int32 `json:"location_novelty_bp"`
	FrequencyPenaltyBP int32 `json:"frequency_penalty_bp"`
	LocalPublishHour   int32 `json:"local_publish_hour"`
}

// Handlers returns the game consumer group's handlers per topic.
func (s *Service) Handlers() map[string]kafka.Handler {
	return map[string]kafka.Handler{
		events.TopicImageCompleted:       s.onImageCompleted,
		events.TopicImageFailed:          s.onImageFailed,
		events.TopicImageReviewRequested: s.onImageReviewRequested,
		events.TopicEngagementRecorded:   s.onEngagementRecorded,
	}
}

// inTx runs fn in one transaction together with recording processed_events.
// An event that was already processed is skipped without effects.
func (s *Service) inTx(ctx context.Context, rec kafka.Record, fn func(tx pgx.Tx, now time.Time) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	now := s.clock.Now()
	fresh, err := outbox.MarkProcessed(ctx, tx, schema, "game."+rec.Topic, rec.Meta.ID, now)
	if err != nil {
		return err
	}
	if !fresh {
		return nil
	}
	if err := fn(tx, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// onImageCompleted makes the post ready and schedules post.published through the
// delayed outbox (docs/detail/flow-2-photo-session.md#7-game-and-billing-react).
func (s *Service) onImageCompleted(ctx context.Context, rec kafka.Record) error {
	var ev imagingv1.ImageCompleted
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		return fmt.Errorf("corrupt image.completed payload: %w", err)
	}
	postID, err := uuid.Parse(ev.GetPostId())
	if err != nil {
		return fmt.Errorf("invalid post_id: %w", err)
	}
	return s.inTx(ctx, rec, func(tx pgx.Tx, now time.Time) error {
		if ev.GetVariant() != 0 {
			return nil // regeneration results are detailed in M2
		}
		var (
			playerID                               uuid.UUID
			st, destinationID, region, country, tz string
			requested                              time.Time
			outfitTags, poseTags                   []string
		)
		err := tx.QueryRow(ctx, `SELECT p.player_id, p.status, p.requested_publish_at, p.destination_id,
			d.region_code, d.country_code, d.timezone, o.style_tags, po.style_tags
			FROM game.posts p
			JOIN game.destinations d ON d.destination_id = p.destination_id
			JOIN game.catalog_items o ON o.item_id = p.outfit_id
			JOIN game.catalog_items po ON po.item_id = p.pose_id
			WHERE p.post_id = $1
			FOR UPDATE OF p`, postID).Scan(&playerID, &st, &requested, &destinationID, &region, &country,
			&tz, &outfitTags, &poseTags)
		if errors.Is(err, pgx.ErrNoRows) {
			s.log.Warn("image.completed for an unknown post", "post_id", postID)
			return nil
		}
		if err != nil {
			return err
		}
		if st != "queued" && st != "review" {
			s.log.Warn("anomaly: image.completed for a post that is already final", "post_id", postID, "status", st)
			return nil
		}

		publishedAt := requested
		if now.After(publishedAt) {
			publishedAt = now
		}
		inputs, err := s.freezeInputs(ctx, tx, playerID, postID, destinationID, tz, publishedAt)
		if err != nil {
			return err
		}
		frozen, err := json.Marshal(frozenJSON{
			RulesVersion:       inputs.GetRulesVersion(),
			Followers:          inputs.GetFollowers(),
			LocationNoveltyBP:  inputs.GetLocationNoveltyBp(),
			FrequencyPenaltyBP: inputs.GetFrequencyPenaltyBp(),
			LocalPublishHour:   inputs.GetLocalPublishHour(),
		})
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE game.posts SET status = 'ready', ready_at = $2, image_s3_key = $3,
			image_tags = $4, published_at = $5, rules_version = $6, frozen_inputs = $7
			WHERE post_id = $1`, postID, now, ev.GetS3Key(), ev.GetImageTags(), publishedAt,
			inputs.GetRulesVersion(), frozen); err != nil {
			return err
		}

		published := &gamev1.PostPublished{
			PostId:        postID.String(),
			PlayerId:      playerID.String(),
			PublishedAt:   timestamppb.New(publishedAt),
			DestinationId: destinationID,
			RegionCode:    region,
			CountryCode:   strings.TrimSpace(country),
			StyleTags:     mergeTags(outfitTags, poseTags),
			ImageTags:     ev.GetImageTags(),
			Inputs:        inputs,
		}
		msg, err := outbox.NewEvent(source, events.TopicPostPublished, postID.String(), published, now)
		if err != nil {
			return err
		}
		msg.AvailableAt = publishedAt // the publish time is the available_at column (ADR-0012)
		msg.PlayerID = &playerID
		return outbox.Insert(ctx, tx, schema, msg)
	})
}

// freezeInputs computes the numbers that are frozen when the photo is ready (design §8).
func (s *Service) freezeInputs(ctx context.Context, tx pgx.Tx, playerID, postID uuid.UUID,
	destinationID, tz string, publishedAt time.Time) (*gamev1.FrozenInputs, error) {
	followers, err := currentFollowers(ctx, tx, playerID, publishedAt)
	if err != nil {
		return nil, err
	}
	var lastVisit *time.Time
	if err := tx.QueryRow(ctx, `SELECT max(published_at) FROM game.posts
		WHERE player_id = $1 AND destination_id = $2 AND status = 'ready'
		  AND published_at < $3 AND post_id <> $4`,
		playerID, destinationID, publishedAt, postID).Scan(&lastVisit); err != nil {
		return nil, err
	}
	var recent int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM game.posts
		WHERE player_id = $1 AND status = 'ready'
		  AND published_at >= $2 AND published_at < $3 AND post_id <> $4`,
		playerID, publishedAt.Add(-24*time.Hour), publishedAt, postID).Scan(&recent); err != nil {
		return nil, err
	}
	rulesVersion := int32(RulesVersion)
	var seasonRules int32
	err = tx.QueryRow(ctx, `SELECT rules_version FROM game.seasons WHERE starts_at <= $1 AND ends_at > $1`,
		publishedAt).Scan(&seasonRules)
	switch {
	case err == nil:
		rulesVersion = seasonRules
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, err
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = clock.WIB
	}
	return &gamev1.FrozenInputs{
		RulesVersion:       rulesVersion,
		Followers:          followers,
		LocationNoveltyBp:  NoveltyBP(lastVisit, publishedAt),
		FrequencyPenaltyBp: FrequencyPenaltyBP(recent),
		LocalPublishHour:   int32(publishedAt.In(loc).Hour()),
	}, nil
}

func (s *Service) onImageFailed(ctx context.Context, rec kafka.Record) error {
	var ev imagingv1.ImageFailed
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		return fmt.Errorf("corrupt image.failed payload: %w", err)
	}
	postID, err := uuid.Parse(ev.GetPostId())
	if err != nil {
		return fmt.Errorf("invalid post_id: %w", err)
	}
	newStatus := "failed"
	switch ev.GetReason() {
	case imagingv1.ImageFailureReason_IMAGE_FAILURE_REASON_NSFW_REJECTED,
		imagingv1.ImageFailureReason_IMAGE_FAILURE_REASON_REVIEW_REJECTED,
		imagingv1.ImageFailureReason_IMAGE_FAILURE_REASON_REVIEW_EXPIRED:
		newStatus = "rejected"
	}
	reason := strings.ToLower(strings.TrimPrefix(ev.GetReason().String(), "IMAGE_FAILURE_REASON_"))
	return s.inTx(ctx, rec, func(tx pgx.Tx, _ time.Time) error {
		_, err := tx.Exec(ctx, `UPDATE game.posts SET status = $2, failure_reason = $3
			WHERE post_id = $1 AND status IN ('queued', 'review')`, postID, newStatus, reason)
		return err
	})
}

func (s *Service) onImageReviewRequested(ctx context.Context, rec kafka.Record) error {
	var ev imagingv1.ImageReviewRequested
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		return fmt.Errorf("corrupt image.review.requested payload: %w", err)
	}
	postID, err := uuid.Parse(ev.GetPostId())
	if err != nil {
		return fmt.Errorf("invalid post_id: %w", err)
	}
	return s.inTx(ctx, rec, func(tx pgx.Tx, _ time.Time) error {
		_, err := tx.Exec(ctx, `UPDATE game.posts SET status = 'review' WHERE post_id = $1 AND status = 'queued'`, postID)
		return err
	})
}

// onEngagementRecorded writes one PopularityEvent per bucket
// (docs/detail/flow-3-publish-and-reactions.md#3-game-records-the-engagement).
func (s *Service) onEngagementRecorded(ctx context.Context, rec kafka.Record) error {
	var ev netizenv1.EngagementRecorded
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		return fmt.Errorf("corrupt engagement.recorded payload: %w", err)
	}
	playerID, err1 := uuid.Parse(ev.GetPlayerId())
	postID, err2 := uuid.Parse(ev.GetPostId())
	if err := errors.Join(err1, err2); err != nil {
		return fmt.Errorf("invalid ID: %w", err)
	}
	n := len(ev.GetBuckets())
	effective := make([]time.Time, n)
	likes := make([]int32, n)
	comments := make([]int32, n)
	deltaFollowers := make([]int64, n)
	deltaScore := make([]int64, n)
	for i, b := range ev.GetBuckets() {
		// A bucket applies when its last reaction happens. Older producers without
		// effective_at use the end of the bucket, so the numbers are never too early.
		effective[i] = b.GetStartsAt().AsTime().Add(clock.BucketSize)
		if b.GetEffectiveAt() != nil {
			effective[i] = b.GetEffectiveAt().AsTime()
		}
		likes[i], comments[i] = b.GetLikes(), b.GetComments()
		deltaFollowers[i], deltaScore[i] = Convert(b.GetLikes(), b.GetComments())
	}
	return s.inTx(ctx, rec, func(tx pgx.Tx, now time.Time) error {
		_, err := tx.Exec(ctx, `INSERT INTO game.popularity_events (player_id, season_id, post_id, kind,
			effective_at, likes, comments, delta_followers, delta_score, rules_version,
			source_event_id, recorded_at)
			SELECT $1, s.season_id, $2, 'engagement', b.effective_at, b.likes, b.comments,
			       b.delta_followers, b.delta_score, $10, $3, $4
			FROM unnest($5::timestamptz[], $6::int[], $7::int[], $8::bigint[], $9::bigint[])
			     AS b(effective_at, likes, comments, delta_followers, delta_score)
			LEFT JOIN game.seasons s ON b.effective_at >= s.starts_at AND b.effective_at < s.ends_at
			ON CONFLICT (source_event_id, effective_at) DO NOTHING`,
			playerID, postID, rec.Meta.ID, now, effective, likes, comments, deltaFollowers, deltaScore,
			ev.GetRulesVersion())
		return err
	})
}

func mergeTags(lists ...[]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, list := range lists {
		for _, t := range list {
			if !seen[t] {
				seen[t] = true
				out = append(out, t)
			}
		}
	}
	return out
}
