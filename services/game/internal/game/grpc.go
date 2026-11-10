package game

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	gamev1 "ivo/gen/ivo/game/v1"
	"ivo/pkg/grpcx"
)

// postSelect reads posts with the likes and comments that are already effective
// at $1. The caller appends the WHERE clause, starting from $2.
const postSelect = `SELECT p.post_id, p.player_id, p.idol_id, p.destination_id, p.outfit_id, p.pose_id,
	p.expression_id, p.caption, p.status, p.express, p.credit_cost, p.created_at,
	p.requested_publish_at, p.published_at, p.failure_reason, ph.photographer, ph.license,
	COALESCE((SELECT sum(e.likes) FROM game.popularity_events e
	          WHERE e.post_id = p.post_id AND e.effective_at <= $1), 0)::bigint,
	COALESCE((SELECT sum(e.comments) FROM game.popularity_events e
	          WHERE e.post_id = p.post_id AND e.effective_at <= $1), 0)::bigint
	FROM game.posts p JOIN game.destination_photos ph ON ph.photo_id = p.photo_id`

var statusToProto = map[string]gamev1.PostStatus{
	"draft":    gamev1.PostStatus_POST_STATUS_DRAFT,
	"queued":   gamev1.PostStatus_POST_STATUS_QUEUED,
	"review":   gamev1.PostStatus_POST_STATUS_REVIEW,
	"ready":    gamev1.PostStatus_POST_STATUS_READY,
	"rejected": gamev1.PostStatus_POST_STATUS_REJECTED,
	"failed":   gamev1.PostStatus_POST_STATUS_FAILED,
}

func (s *Service) scanPost(row interface{ Scan(...any) error }, now time.Time) (*gamev1.Post, error) {
	var (
		postID, playerID, idolID                                 uuid.UUID
		dest, outfit, pose, expr, caption, st, photographer, lic string
		express                                                  bool
		cost, likes, comments                                    int64
		createdAt, requested                                     time.Time
		publishedAt                                              *time.Time
		failure                                                  *string
	)
	if err := row.Scan(&postID, &playerID, &idolID, &dest, &outfit, &pose, &expr, &caption, &st, &express,
		&cost, &createdAt, &requested, &publishedAt, &failure, &photographer, &lic, &likes, &comments); err != nil {
		return nil, err
	}
	p := &gamev1.Post{
		PostId:             postID.String(),
		PlayerId:           playerID.String(),
		IdolId:             idolID.String(),
		DestinationId:      dest,
		OutfitId:           outfit,
		PoseId:             pose,
		ExpressionId:       expr,
		Caption:            caption,
		Status:             statusToProto[st],
		Express:            express,
		CreditCost:         cost,
		CreatedAt:          timestamppb.New(createdAt),
		RequestedPublishAt: timestamppb.New(requested),
		EstimatedReadyAt:   timestamppb.New(createdAt.Add(s.cfg.RenderEstimate)),
		PhotoAttribution:   fmt.Sprintf("Background photo: %s, Mapillary, %s", photographer, lic),
		Likes:              likes,
		Comments:           comments,
	}
	if publishedAt != nil {
		p.PublishedAt = timestamppb.New(*publishedAt)
		p.Published = st == "ready" && !publishedAt.After(now)
	}
	if failure != nil {
		p.FailureReason = *failure
	}
	return p, nil
}

func (s *Service) loadPost(ctx context.Context, playerID, postID uuid.UUID) (*gamev1.Post, error) {
	now := s.clock.Now()
	return s.scanPost(s.pool.QueryRow(ctx, postSelect+` WHERE p.post_id = $2 AND p.player_id = $3`,
		now, postID, playerID), now)
}

// GetPost returns a post that belongs to the player.
func (s *Service) GetPost(ctx context.Context, req *gamev1.GetPostRequest) (*gamev1.GetPostResponse, error) {
	playerID, err1 := uuid.Parse(req.GetPlayerId())
	postID, err2 := uuid.Parse(req.GetPostId())
	if err1 != nil || err2 != nil {
		return nil, grpcx.Fail(codes.InvalidArgument, "INVALID_ID", "invalid player_id or post_id")
	}
	post, err := s.loadPost(ctx, playerID, postID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, grpcx.Fail(codes.NotFound, "NOT_FOUND", "post not found")
	}
	if err != nil {
		return nil, grpcx.Internal(s.log, "GetPost", err)
	}
	return &gamev1.GetPostResponse{Post: post}, nil
}

// ListPosts returns the player's posts, newest first. In M1 the page_token is
// an offset; it becomes a keyset once the data grows.
func (s *Service) ListPosts(ctx context.Context, req *gamev1.ListPostsRequest) (*gamev1.ListPostsResponse, error) {
	playerID, err := uuid.Parse(req.GetPlayerId())
	if err != nil {
		return nil, grpcx.Fail(codes.InvalidArgument, "INVALID_PLAYER", "invalid player_id")
	}
	size, offset := pageParams(req.GetPageSize(), req.GetPageToken())
	now := s.clock.Now()
	where := ` WHERE p.player_id = $2`
	if !req.GetIncludeUnpublished() {
		where += ` AND p.status = 'ready' AND p.published_at <= $1`
	}
	rows, err := s.pool.Query(ctx, postSelect+where+` ORDER BY p.created_at DESC, p.post_id LIMIT $3 OFFSET $4`,
		now, playerID, size+1, offset)
	if err != nil {
		return nil, grpcx.Internal(s.log, "ListPosts", err)
	}
	defer rows.Close()
	resp := &gamev1.ListPostsResponse{}
	for rows.Next() {
		post, err := s.scanPost(rows, now)
		if err != nil {
			return nil, grpcx.Internal(s.log, "ListPosts", err)
		}
		resp.Posts = append(resp.Posts, post)
	}
	if err := rows.Err(); err != nil {
		return nil, grpcx.Internal(s.log, "ListPosts", err)
	}
	if len(resp.Posts) > size {
		resp.Posts = resp.Posts[:size]
		resp.NextPageToken = strconv.Itoa(offset + size)
	}
	return resp, nil
}

// GetPostContext returns the caption and descriptive text for the comment prompt.
func (s *Service) GetPostContext(ctx context.Context, req *gamev1.GetPostContextRequest) (*gamev1.GetPostContextResponse, error) {
	postID, err := uuid.Parse(req.GetPostId())
	if err != nil {
		return nil, grpcx.Fail(codes.InvalidArgument, "INVALID_ID", "invalid post_id")
	}
	resp := &gamev1.GetPostContextResponse{PostId: postID.String()}
	err = s.pool.QueryRow(ctx, `SELECT p.caption, i.name, i.personality, d.name, d.description,
		o.name, po.name, e.name
		FROM game.posts p
		JOIN game.idols i ON i.idol_id = p.idol_id
		JOIN game.destinations d ON d.destination_id = p.destination_id
		JOIN game.catalog_items o ON o.item_id = p.outfit_id
		JOIN game.catalog_items po ON po.item_id = p.pose_id
		JOIN game.catalog_items e ON e.item_id = p.expression_id
		WHERE p.post_id = $1`, postID).Scan(&resp.Caption, &resp.IdolName, &resp.IdolPersonality,
		&resp.DestinationName, &resp.DestinationDescription, &resp.OutfitName, &resp.PoseName, &resp.ExpressionName)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, grpcx.Fail(codes.NotFound, "NOT_FOUND", "post not found")
	}
	if err != nil {
		return nil, grpcx.Internal(s.log, "GetPostContext", err)
	}
	return resp, nil
}

// GetPopularity computes followers and the season score at call time.
func (s *Service) GetPopularity(ctx context.Context, req *gamev1.GetPopularityRequest) (*gamev1.GetPopularityResponse, error) {
	playerID, err := uuid.Parse(req.GetPlayerId())
	if err != nil {
		return nil, grpcx.Fail(codes.InvalidArgument, "INVALID_PLAYER", "invalid player_id")
	}
	now := s.clock.Now()
	followers, err := currentFollowers(ctx, s.pool, playerID, now)
	if err != nil {
		return nil, grpcx.Internal(s.log, "GetPopularity", err)
	}
	resp := &gamev1.GetPopularityResponse{Followers: followers, AsOf: timestamppb.New(now)}

	var seasonID uuid.UUID
	err = s.pool.QueryRow(ctx, `SELECT season_id FROM game.seasons WHERE status = 'active'`).Scan(&seasonID)
	switch {
	case err == nil:
		resp.SeasonId = seasonID.String()
		if err := s.pool.QueryRow(ctx, `SELECT
			COALESCE((SELECT score FROM game.season_scores WHERE season_id = $1 AND player_id = $2), 0)
			+ COALESCE((SELECT sum(delta_score) FROM game.popularity_events
			            WHERE season_id = $1 AND player_id = $2 AND folded_at IS NULL AND effective_at <= $3), 0)::bigint`,
			seasonID, playerID, now).Scan(&resp.SeasonScore); err != nil {
			return nil, grpcx.Internal(s.log, "GetPopularity", err)
		}
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, grpcx.Internal(s.log, "GetPopularity", err)
	}

	rows, err := s.pool.Query(ctx, `SELECT destination_id FROM game.destinations
		WHERE active AND follower_threshold <= $1 ORDER BY follower_threshold, destination_id`, followers)
	if err != nil {
		return nil, grpcx.Internal(s.log, "GetPopularity", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, grpcx.Internal(s.log, "GetPopularity", err)
		}
		resp.UnlockedDestinationIds = append(resp.UnlockedDestinationIds, id)
	}
	return resp, rows.Err()
}

// GetLeaderboard reads season_scores directly from Postgres in M1. The Valkey
// sorted set comes later, together with Valkey in Compose.
func (s *Service) GetLeaderboard(ctx context.Context, req *gamev1.GetLeaderboardRequest) (*gamev1.GetLeaderboardResponse, error) {
	var seasonID uuid.UUID
	var err error
	if req.GetSeasonId() != "" {
		if seasonID, err = uuid.Parse(req.GetSeasonId()); err != nil {
			return nil, grpcx.Fail(codes.InvalidArgument, "INVALID_ID", "invalid season_id")
		}
	} else if err = s.pool.QueryRow(ctx, `SELECT season_id FROM game.seasons WHERE status = 'active'`).Scan(&seasonID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, grpcx.Fail(codes.NotFound, "NO_ACTIVE_SEASON", "there is no active season")
		}
		return nil, grpcx.Internal(s.log, "GetLeaderboard", err)
	}
	size, offset := pageParams(req.GetPageSize(), req.GetPageToken())
	rows, err := s.pool.Query(ctx, `SELECT s.player_id, p.display_name, i.name, s.score, s.as_of
		FROM game.season_scores s
		JOIN game.players p ON p.player_id = s.player_id
		JOIN game.idols i ON i.player_id = s.player_id
		WHERE s.season_id = $1
		ORDER BY s.score DESC, s.player_id
		LIMIT $2 OFFSET $3`, seasonID, size+1, offset)
	if err != nil {
		return nil, grpcx.Internal(s.log, "GetLeaderboard", err)
	}
	defer rows.Close()
	resp := &gamev1.GetLeaderboardResponse{SeasonId: seasonID.String()}
	var asOf time.Time
	for rows.Next() {
		var playerID uuid.UUID
		e := &gamev1.LeaderboardEntry{Rank: int32(offset + len(resp.Entries) + 1)}
		var rowAsOf time.Time
		if err := rows.Scan(&playerID, &e.DisplayName, &e.IdolName, &e.Score, &rowAsOf); err != nil {
			return nil, grpcx.Internal(s.log, "GetLeaderboard", err)
		}
		e.PlayerId = playerID.String()
		if rowAsOf.After(asOf) {
			asOf = rowAsOf
		}
		resp.Entries = append(resp.Entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, grpcx.Internal(s.log, "GetLeaderboard", err)
	}
	if len(resp.Entries) > size {
		resp.Entries = resp.Entries[:size]
		resp.NextPageToken = strconv.Itoa(offset + size)
	}
	if !asOf.IsZero() {
		resp.AsOf = timestamppb.New(asOf)
	}
	return resp, nil
}

func pageParams(size int32, token string) (int, int) {
	n := int(size)
	if n <= 0 || n > 50 {
		n = 20
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		offset = 0
	}
	return n, offset
}
