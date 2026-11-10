package game

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gamev1 "ivo/gen/ivo/game/v1"
	"ivo/pkg/clock"
	"ivo/pkg/detrand"
	"ivo/pkg/events"
	"ivo/pkg/grpcx"
	"ivo/pkg/ids"
	"ivo/pkg/outbox"
	"ivo/pkg/pgdb"
)

const maxCaptionRunes = 280

// captionBlocklist is only a first safeguard; a text classifier comes later (design §15).
var captionBlocklist = []string{"porn", "nsfw", "naked", "nude"}

var idolStarterTags = []string{"1girl", "solo", "long_hair", "brown_hair", "brown_eyes"}

type itemChoice struct{ id, kind string }

type photoRow struct {
	id           uuid.UUID
	s3Key        string
	attribution  string
	footX, footY float32
	scale        float32
	lightAzimuth int16
}

type postInput struct {
	destinationID string
	timezone      string
	photo         photoRow
	idolID        uuid.UUID
	idolTags      []string
	publishAt     time.Time
	localHour     int32
}

// CreatePost follows docs/detail/flow-1-create-post.md.
func (s *Service) CreatePost(ctx context.Context, req *gamev1.CreatePostRequest) (*gamev1.CreatePostResponse, error) {
	playerID, err := uuid.Parse(req.GetPlayerId())
	if err != nil {
		return nil, grpcx.Fail(codes.InvalidArgument, "INVALID_PLAYER", "invalid player_id")
	}
	key, err := uuid.Parse(req.GetIdempotencyKey())
	if err != nil {
		return nil, grpcx.Fail(codes.InvalidArgument, "IDEMPOTENCY_KEY_INVALID", "Idempotency-Key must be a UUID")
	}
	postID := ids.PostID(playerID, key)
	hash := requestHash(req)

	if resp, err := s.replay(ctx, playerID, postID, hash); resp != nil || err != nil {
		return resp, err
	}

	now := s.clock.Now()
	if err := s.ensurePlayer(ctx, playerID, now); err != nil {
		return nil, err
	}
	in, err := s.validate(ctx, playerID, postID, req, now)
	if err != nil {
		return nil, err
	}
	cost := s.cfg.BaseCost
	if req.GetExpress() {
		cost += s.cfg.ExpressCost
	}
	if err := s.credits.Hold(ctx, postID, playerID, cost); err != nil {
		return nil, err
	}

	if err := s.insertPost(ctx, playerID, postID, key, hash, req, in, cost, now); err != nil {
		if pgdb.IsUniqueViolation(err) {
			// A twin request arrived at the same time and the other one won. Its hold is
			// the same one (hold_id = post_id), so it must not be voided here.
			if resp, rerr := s.replay(ctx, playerID, postID, hash); resp != nil || rerr != nil {
				return resp, rerr
			}
		}
		if voidErr := s.credits.Void(context.WithoutCancel(ctx), postID, "create_post_failed"); voidErr != nil {
			s.log.Error("voiding the hold failed", "post_id", postID, "err", voidErr)
		}
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, grpcx.Internal(s.log, "CreatePost", err)
	}

	post, err := s.loadPost(ctx, playerID, postID)
	if err != nil {
		return nil, grpcx.Internal(s.log, "CreatePost", err)
	}
	return &gamev1.CreatePostResponse{Post: post}, nil
}

// replay returns the existing post for the same Idempotency-Key, or (nil, nil)
// if there is none yet.
func (s *Service) replay(ctx context.Context, playerID, postID uuid.UUID, hash []byte) (*gamev1.CreatePostResponse, error) {
	var stored []byte
	err := s.pool.QueryRow(ctx, `SELECT request_hash FROM game.posts WHERE post_id = $1`, postID).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, grpcx.Internal(s.log, "CreatePost", err)
	}
	if !bytes.Equal(stored, hash) {
		return nil, grpcx.Fail(codes.InvalidArgument, "IDEMPOTENCY_KEY_REUSED",
			"Idempotency-Key was already used for a different request")
	}
	post, err := s.loadPost(ctx, playerID, postID)
	if err != nil {
		return nil, grpcx.Internal(s.log, "CreatePost", err)
	}
	return &gamev1.CreatePostResponse{Post: post, Replayed: true}, nil
}

func requestHash(req *gamev1.CreatePostRequest) []byte {
	publish := ""
	if ts := req.GetRequestedPublishAt(); ts != nil {
		publish = ts.AsTime().UTC().Format(time.RFC3339Nano)
	}
	canonical := strings.Join([]string{
		req.GetDestinationId(), req.GetOutfitId(), req.GetPoseId(), req.GetExpressionId(),
		strings.TrimSpace(req.GetCaption()), publish, fmt.Sprint(req.GetExpress()),
	}, "\x1f")
	sum := sha256.Sum256([]byte(canonical))
	return sum[:]
}

// ensurePlayer creates the player, the idol, and the follower snapshot in dev
// environments. In production, players are created at registration through auth (M3).
func (s *Service) ensurePlayer(ctx context.Context, playerID uuid.UUID, now time.Time) error {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM game.players WHERE player_id = $1)`,
		playerID).Scan(&exists); err != nil {
		return grpcx.Internal(s.log, "CreatePost", err)
	}
	if exists {
		return nil
	}
	if !s.cfg.AutoProvision {
		return grpcx.Fail(codes.NotFound, "PLAYER_NOT_FOUND", "player is not registered")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return grpcx.Internal(s.log, "CreatePost", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	stmts := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO game.players (player_id, display_name, created_at) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`, []any{playerID, "Player " + playerID.String()[:8], now}},
		{`INSERT INTO game.idols (idol_id, player_id, name, age, appearance_tags, personality, created_at)
			VALUES ($1, $2, 'Rara', 23, $3, $4, $5) ON CONFLICT (player_id) DO NOTHING`,
			[]any{ids.New(), playerID, idolStarterTags, "Cheerful, loves to travel, and makes friends easily.", now}},
		{`INSERT INTO game.follower_snapshots (player_id, followers, as_of) VALUES ($1, 0, $2)
			ON CONFLICT DO NOTHING`, []any{playerID, now}},
	}
	for _, st := range stmts {
		if _, err := tx.Exec(ctx, st.sql, st.args...); err != nil {
			return grpcx.Internal(s.log, "CreatePost", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return grpcx.Internal(s.log, "CreatePost", err)
	}
	s.log.Info("dev player created", "player_id", playerID)
	return nil
}

func (s *Service) validate(ctx context.Context, playerID, postID uuid.UUID, req *gamev1.CreatePostRequest, now time.Time) (postInput, error) {
	var in postInput
	caption := strings.TrimSpace(req.GetCaption())
	if utf8.RuneCountInString(caption) > maxCaptionRunes {
		return in, grpcx.Fail(codes.InvalidArgument, "CAPTION_TOO_LONG", "the caption is limited to 280 characters")
	}
	lower := strings.ToLower(caption)
	for _, word := range captionBlocklist {
		if strings.Contains(lower, word) {
			return in, grpcx.Fail(codes.InvalidArgument, "CAPTION_REJECTED", "the caption violates the content policy")
		}
	}

	var threshold int64
	err := s.pool.QueryRow(ctx, `SELECT timezone, follower_threshold FROM game.destinations
		WHERE destination_id = $1 AND active`, req.GetDestinationId()).Scan(&in.timezone, &threshold)
	if errors.Is(err, pgx.ErrNoRows) {
		return in, grpcx.Fail(codes.InvalidArgument, "INVALID_CHOICE", "unknown destination")
	}
	if err != nil {
		return in, grpcx.Internal(s.log, "CreatePost", err)
	}
	in.destinationID = req.GetDestinationId()
	followers, err := currentFollowers(ctx, s.pool, playerID, now)
	if err != nil {
		return in, grpcx.Internal(s.log, "CreatePost", err)
	}
	if followers < threshold {
		return in, grpcx.Fail(codes.FailedPrecondition, "DESTINATION_LOCKED",
			fmt.Sprintf("the destination unlocks at %d followers", threshold))
	}

	if err := s.checkItems(ctx, playerID, now, []itemChoice{
		{req.GetOutfitId(), "outfit"}, {req.GetPoseId(), "pose"}, {req.GetExpressionId(), "expression"},
	}); err != nil {
		return in, err
	}

	eta := now.Add(s.cfg.RenderEstimate)
	in.publishAt = eta
	if ts := req.GetRequestedPublishAt(); ts != nil {
		want := ts.AsTime().UTC()
		if want.Before(eta.Add(-time.Minute)) || want.After(now.Add(24*time.Hour)) {
			return in, grpcx.Fail(codes.InvalidArgument, "PUBLISH_TIME_INVALID",
				"the publish time must be between the estimated photo completion and 24 hours from now")
		}
		if want.After(eta) {
			in.publishAt = want
		}
	}
	loc, err := time.LoadLocation(in.timezone)
	if err != nil {
		loc = clock.WIB
	}
	in.localHour = int32(in.publishAt.In(loc).Hour())

	if in.photo, err = s.pickPhoto(ctx, postID, in.destinationID); err != nil {
		return in, err
	}
	err = s.pool.QueryRow(ctx, `SELECT idol_id, appearance_tags FROM game.idols WHERE player_id = $1`,
		playerID).Scan(&in.idolID, &in.idolTags)
	if errors.Is(err, pgx.ErrNoRows) {
		return in, grpcx.Fail(codes.FailedPrecondition, "IDOL_MISSING", "the player has no idol yet")
	}
	if err != nil {
		return in, grpcx.Internal(s.log, "CreatePost", err)
	}
	return in, nil
}

func (s *Service) checkItems(ctx context.Context, playerID uuid.UUID, now time.Time, choices []itemChoice) error {
	for _, c := range choices {
		var kind string
		var needsEntitlement bool
		err := s.pool.QueryRow(ctx, `SELECT kind, requires_entitlement FROM game.catalog_items
			WHERE item_id = $1 AND active`, c.id).Scan(&kind, &needsEntitlement)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && kind != c.kind) {
			return grpcx.Fail(codes.InvalidArgument, "INVALID_CHOICE", fmt.Sprintf("%s %q is not in the catalog", c.kind, c.id))
		}
		if err != nil {
			return grpcx.Internal(s.log, "CreatePost", err)
		}
		if !needsEntitlement {
			continue
		}
		var owned bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM game.entitlements
			WHERE player_id = $1 AND item_id = $2 AND (expires_at IS NULL OR expires_at > $3))`,
			playerID, c.id, now).Scan(&owned); err != nil {
			return grpcx.Internal(s.log, "CreatePost", err)
		}
		if !owned {
			return grpcx.Fail(codes.FailedPrecondition, "ENTITLEMENT_REQUIRED", fmt.Sprintf("%s %q is not owned", c.kind, c.id))
		}
	}
	return nil
}

// pickPhoto picks the background photo deterministically from the post_id.
func (s *Service) pickPhoto(ctx context.Context, postID uuid.UUID, destinationID string) (photoRow, error) {
	rows, err := s.pool.Query(ctx, `SELECT photo_id, s3_key, photographer, license, anchor_foot_x,
		anchor_foot_y, anchor_scale, light_azimuth_deg
		FROM game.destination_photos WHERE destination_id = $1 AND active ORDER BY photo_id`, destinationID)
	if err != nil {
		return photoRow{}, grpcx.Internal(s.log, "CreatePost", err)
	}
	defer rows.Close()
	var photos []photoRow
	for rows.Next() {
		var p photoRow
		var photographer, license string
		if err := rows.Scan(&p.id, &p.s3Key, &photographer, &license, &p.footX, &p.footY, &p.scale, &p.lightAzimuth); err != nil {
			return photoRow{}, grpcx.Internal(s.log, "CreatePost", err)
		}
		p.attribution = fmt.Sprintf("%s, Mapillary, %s", photographer, license)
		photos = append(photos, p)
	}
	if err := rows.Err(); err != nil {
		return photoRow{}, grpcx.Internal(s.log, "CreatePost", err)
	}
	if len(photos) == 0 {
		return photoRow{}, grpcx.Fail(codes.FailedPrecondition, "DESTINATION_UNAVAILABLE", "the destination has no background photos yet")
	}
	return photos[detrand.Intn(postID.String(), 0, len(photos))], nil
}

func (s *Service) insertPost(ctx context.Context, playerID, postID, key uuid.UUID, hash []byte,
	req *gamev1.CreatePostRequest, in postInput, cost int64, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// Lock the player row so two concurrent requests can't both get under the daily limit.
	if _, err := tx.Exec(ctx, `SELECT 1 FROM game.players WHERE player_id = $1 FOR UPDATE`, playerID); err != nil {
		return err
	}
	dateWIB := clock.DateWIB(now)
	var today int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM game.posts
		WHERE player_id = $1 AND date_wib = $2 AND status <> 'failed'`, playerID, dateWIB).Scan(&today); err != nil {
		return err
	}
	if today >= s.cfg.DailyPostLimit {
		return grpcx.Fail(codes.FailedPrecondition, "DAILY_LIMIT",
			fmt.Sprintf("the limit of %d posts per day has been reached", s.cfg.DailyPostLimit))
	}

	if _, err := tx.Exec(ctx, `INSERT INTO game.posts (post_id, player_id, idol_id, idempotency_key,
		request_hash, destination_id, photo_id, outfit_id, pose_id, expression_id, caption, express,
		credit_cost, status, date_wib, created_at, requested_publish_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'queued', $14, $15, $16)`,
		postID, playerID, in.idolID, key, hash, in.destinationID, in.photo.id, req.GetOutfitId(),
		req.GetPoseId(), req.GetExpressionId(), strings.TrimSpace(req.GetCaption()), req.GetExpress(),
		cost, dateWIB, now, in.publishAt); err != nil {
		return err
	}

	lane := gamev1.PhotoLane_PHOTO_LANE_BATCH
	if req.GetExpress() {
		lane = gamev1.PhotoLane_PHOTO_LANE_EXPRESS
	}
	ev := &gamev1.PostSubmitted{
		PostId:     postID.String(),
		PlayerId:   playerID.String(),
		HoldId:     postID.String(),
		CreditCost: cost,
		Variant:    0,
		Lane:       lane,
		Choice: &gamev1.RenderChoice{
			IdolId:           in.idolID.String(),
			IdolTags:         in.idolTags,
			OutfitId:         req.GetOutfitId(),
			PoseId:           req.GetPoseId(),
			ExpressionId:     req.GetExpressionId(),
			LocalPublishHour: in.localHour,
		},
		Background: &gamev1.Background{
			PhotoId:         in.photo.id.String(),
			S3Key:           in.photo.s3Key,
			FootX:           in.photo.footX,
			FootY:           in.photo.footY,
			Scale:           in.photo.scale,
			LightAzimuthDeg: int32(in.photo.lightAzimuth),
			Attribution:     in.photo.attribution,
		},
	}
	msg, err := outbox.NewEvent(source, events.TopicPostSubmitted, playerID.String(), ev, now)
	if err != nil {
		return err
	}
	msg.PlayerID = &playerID
	if err := outbox.Insert(ctx, tx, schema, msg); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// currentFollowers = the snapshot + unfolded events with effective_at <= at.
func currentFollowers(ctx context.Context, q querier, playerID uuid.UUID, at time.Time) (int64, error) {
	var v int64
	err := q.QueryRow(ctx, `SELECT
		COALESCE((SELECT followers FROM game.follower_snapshots WHERE player_id = $1), 0)
		+ COALESCE((SELECT sum(delta_followers) FROM game.popularity_events
		            WHERE player_id = $1 AND folded_at IS NULL AND effective_at <= $2), 0)::bigint`,
		playerID, at).Scan(&v)
	if v < 0 {
		v = 0
	}
	return v, err
}
