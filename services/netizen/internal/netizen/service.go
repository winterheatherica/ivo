// Package netizen implements the netizen service: it decides reactions when a
// post goes live, writes comments per chunk, and serves comment lists
// (docs/detail/flow-3-publish-and-reactions.md).
//
// M1 writes every comment from code templates. LLM routing (Cloudflare, Modal,
// xAI) follows in M3 through the same path.
package netizen

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	gamev1 "ivo/gen/ivo/game/v1"
	netizenv1 "ivo/gen/ivo/netizen/v1"
	"ivo/pkg/clock"
	"ivo/pkg/config"
	"ivo/pkg/events"
	"ivo/pkg/grpcx"
	"ivo/pkg/ids"
	"ivo/pkg/kafka"
	"ivo/services/netizen/internal/engine"
)

const source = "ivo/netizen"

// Config is the netizen service's configuration.
type Config struct {
	GRPCAddr     string
	MongoURI     string
	MongoDB      string
	Brokers      []string
	GameAddr     string
	DelayScaleBP int  // 10000 = real time; dev uses a small number
	PersonaCount int  // the number of personas to seed
	ChunkSize    int  // personas per comment chunk
	RunReactions bool // the post.published consumer
	RunComments  bool // the netizen.comment.requested worker
}

// LoadConfig reads Config from the environment.
func LoadConfig() Config {
	return Config{
		GRPCAddr:     config.String("GRPC_ADDR", ":9090"),
		MongoURI:     config.String("MONGO_URI", "mongodb://localhost:27017"),
		MongoDB:      config.String("MONGO_DB", "netizen"),
		Brokers:      config.List("KAFKA_BROKERS", []string{"localhost:19092"}),
		GameAddr:     config.String("GAME_ADDR", "localhost:9091"),
		DelayScaleBP: config.Int("DELAY_SCALE_BP", 10000),
		PersonaCount: config.Int("PERSONA_COUNT", 5000),
		ChunkSize:    config.Int("CHUNK_SIZE", 15),
		RunReactions: config.Bool("RUN_REACTIONS", true),
		RunComments:  config.Bool("RUN_COMMENTS", true),
	}
}

// Service holds netizen's dependencies and the personas in memory.
type Service struct {
	netizenv1.UnimplementedNetizenServiceServer

	store    *Store
	clock    clock.Clock
	log      *slog.Logger
	cfg      Config
	producer *kafka.Producer
	game     gamev1.GameServiceClient
	personas []engine.Persona
	byID     map[uuid.UUID]engine.Persona

	mu       sync.Mutex
	contexts map[uuid.UUID]cachedContext
}

type cachedContext struct {
	ctx     engine.PostContext
	expires time.Time
}

// New creates a Service with personas that are already loaded.
func New(store *Store, clk clock.Clock, log *slog.Logger, cfg Config, producer *kafka.Producer,
	game gamev1.GameServiceClient, personas []engine.Persona) *Service {
	byID := make(map[uuid.UUID]engine.Persona, len(personas))
	for _, p := range personas {
		byID[p.ID] = p
	}
	return &Service{store: store, clock: clk, log: log, cfg: cfg, producer: producer, game: game,
		personas: personas, byID: byID, contexts: map[uuid.UUID]cachedContext{}}
}

// ReactionHandlers are for the "netizen" consumer group.
func (s *Service) ReactionHandlers() map[string]kafka.Handler {
	return map[string]kafka.Handler{events.TopicPostPublished: s.onPostPublished}
}

// CommentHandlers are for the "netizen-comment" consumer group (autoscaled by KEDA in M5).
func (s *Service) CommentHandlers() map[string]kafka.Handler {
	return map[string]kafka.Handler{events.TopicCommentRequested: s.onCommentRequested}
}

// onPostPublished decides every reaction at once, then sends engagement.recorded
// and the comment chunks with deterministic event IDs. Reprocessing produces
// identical events (ADR-0013).
func (s *Service) onPostPublished(ctx context.Context, rec kafka.Record) error {
	var ev gamev1.PostPublished
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		return fmt.Errorf("corrupt post.published payload: %w", err)
	}
	postID, err1 := uuid.Parse(ev.GetPostId())
	playerID, err2 := uuid.Parse(ev.GetPlayerId())
	if err := errors.Join(err1, err2); err != nil {
		return fmt.Errorf("invalid ID: %w", err)
	}

	var run postRunDoc
	err := s.store.runs.FindOne(ctx, doc("_id", bin(postID))).Decode(&run)
	switch {
	case err == nil && run.Status != "reacting":
		return nil // the reactions were already decided
	case err != nil && !errors.Is(err, mongo.ErrNoDocuments):
		return err
	}

	in := ev.GetInputs()
	post := engine.Post{
		PostID:       postID,
		PublishedAt:  ev.GetPublishedAt().AsTime(),
		RegionCode:   ev.GetRegionCode(),
		StyleTags:    ev.GetStyleTags(),
		Followers:    in.GetFollowers(),
		NoveltyBP:    in.GetLocationNoveltyBp(),
		PenaltyBP:    in.GetFrequencyPenaltyBp(),
		RulesVersion: in.GetRulesVersion(),
	}
	res := engine.React(post, s.personas, engine.Params{DelayScaleBP: int32(s.cfg.DelayScaleBP)})
	chunks := engine.Chunks(res.Comments, s.cfg.ChunkSize)
	now := s.clock.Now()

	if errors.Is(err, mongo.ErrNoDocuments) {
		// The document is created before the chunks are sent, so the comment workers always find it.
		run = postRunDoc{ID: bin(postID), PlayerID: bin(playerID), Status: "reacting",
			RulesVersion: post.RulesVersion, PersonaVersion: engine.PersonaVersion,
			ChunkCount: int32(len(chunks)), ChunksDone: []int32{},
			Totals:         totalsDoc{Likes: res.TotalLikes, Comments: res.TotalComments},
			FirstCommentAt: res.FirstCommentAt, CreatedAt: now, UpdatedAt: now}
		if _, err := s.store.runs.InsertOne(ctx, run); err != nil && !mongo.IsDuplicateKeyError(err) {
			return err
		}
	}

	engagement := &netizenv1.EngagementRecorded{
		PostId:         postID.String(),
		PlayerId:       playerID.String(),
		RulesVersion:   post.RulesVersion,
		PersonaVersion: engine.PersonaVersion,
		TotalLikes:     res.TotalLikes,
		TotalComments:  res.TotalComments,
	}
	for _, b := range res.Buckets {
		engagement.Buckets = append(engagement.Buckets, &netizenv1.EngagementBucket{
			StartsAt: timestamppb.New(b.StartsAt), EffectiveAt: timestamppb.New(b.EffectiveAt),
			Likes: b.Likes, Comments: b.Comments})
	}
	if res.FirstCommentAt != nil {
		engagement.FirstCommentAt = timestamppb.New(*res.FirstCommentAt)
	}
	if err := s.producer.ProduceEvent(ctx, source, events.TopicEngagementRecorded, playerID.String(),
		ids.EventID(events.TopicEngagementRecorded, postID.String()), engagement, now); err != nil {
		return err
	}

	for _, c := range chunks {
		msg := &netizenv1.CommentRequested{PostId: postID.String(), PlayerId: playerID.String(),
			ChunkNo: c.No, ChunkCount: int32(len(chunks)), Voice: c.Voice}
		for _, p := range c.Comments {
			msg.Comments = append(msg.Comments, &netizenv1.PlannedComment{
				NetizenId: p.NetizenID.String(), PlannedAt: timestamppb.New(p.PlannedAt)})
		}
		chunkNo := strconv.Itoa(int(c.No))
		if err := s.producer.ProduceEvent(ctx, source, events.TopicCommentRequested, postID.String()+":"+chunkNo,
			ids.EventID(events.TopicCommentRequested, postID.String(), chunkNo), msg, now); err != nil {
			return err
		}
	}

	next := "reacted"
	if len(chunks) == 0 {
		next = "commented"
	}
	if _, err := s.store.runs.UpdateOne(ctx, doc("_id", bin(postID), "status", "reacting"),
		doc("$set", doc("status", next, "updated_at", now))); err != nil {
		return err
	}
	s.log.Info("reactions decided", "post_id", postID, "likes", res.TotalLikes,
		"comments", res.TotalComments, "chunks", len(chunks))
	return nil
}

// onCommentRequested writes the text for one chunk. Comments that already exist
// are skipped, so a reprocessed chunk never creates duplicate comments.
func (s *Service) onCommentRequested(ctx context.Context, rec kafka.Record) error {
	var ev netizenv1.CommentRequested
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		return fmt.Errorf("corrupt netizen.comment.requested payload: %w", err)
	}
	postID, err1 := uuid.Parse(ev.GetPostId())
	playerID, err2 := uuid.Parse(ev.GetPlayerId())
	if err := errors.Join(err1, err2); err != nil {
		return fmt.Errorf("invalid ID: %w", err)
	}
	postCtx, err := s.postContext(ctx, postID)
	if err != nil {
		return fmt.Errorf("GetPostContext: %w", err)
	}

	netizenIDs := make(bson.A, 0, len(ev.GetComments()))
	for _, c := range ev.GetComments() {
		if id, err := uuid.Parse(c.GetNetizenId()); err == nil {
			netizenIDs = append(netizenIDs, bin(id))
		}
	}
	existing := map[uuid.UUID]bool{}
	cur, err := s.store.comments.Find(ctx, doc("post_id", bin(postID), "netizen_id", doc("$in", netizenIDs)),
		options.Find().SetProjection(doc("netizen_id", 1)))
	if err != nil {
		return err
	}
	var found []commentDoc
	if err := cur.All(ctx, &found); err != nil {
		return err
	}
	for _, c := range found {
		existing[unbin(c.NetizenID)] = true
	}

	now := s.clock.Now()
	var models []mongo.WriteModel
	for _, c := range ev.GetComments() {
		netizenID, err := uuid.Parse(c.GetNetizenId())
		if err != nil || existing[netizenID] {
			continue
		}
		persona, ok := s.byID[netizenID]
		if !ok {
			s.log.Warn("unknown persona, skipping the comment", "netizen_id", netizenID)
			continue
		}
		commentID := ids.CommentID(postID, netizenID)
		planned := c.GetPlannedAt().AsTime()
		visible := planned
		if now.After(visible) {
			visible = now // late text: it appears as soon as it is ready
		}
		fields := doc(
			"post_id", bin(postID), "player_id", bin(playerID), "netizen_id", bin(netizenID),
			"text", engine.Render(persona, postCtx, ev.GetVoice(), commentID),
			"source", "template", "route", "template", "model", nil,
			"prompt_version", engine.TemplateVersion,
			"moderation", doc("score_bp", int32(0), "blocked_reason", nil),
			"planned_at", planned, "visible_at", visible, "created_at", now,
		)
		models = append(models, mongo.NewUpdateOneModel().SetFilter(doc("_id", bin(commentID))).
			SetUpdate(doc("$setOnInsert", fields)).SetUpsert(true))
	}
	if len(models) > 0 {
		if _, err := s.store.comments.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
			return err
		}
	}

	if _, err := s.store.runs.UpdateOne(ctx, doc("_id", bin(postID)),
		doc("$addToSet", doc("chunks_done", ev.GetChunkNo()), "$set", doc("updated_at", now))); err != nil {
		return err
	}
	_, err = s.store.runs.UpdateOne(ctx, doc(
		"_id", bin(postID),
		"status", doc("$in", bson.A{"reacting", "reacted"}),
		"$expr", doc("$eq", bson.A{doc("$size", "$chunks_done"), "$chunk_count"}),
	), doc("$set", doc("status", "commented")))
	return err
}

// postContext reads the prompt context from game and caches it in memory for 1 hour.
func (s *Service) postContext(ctx context.Context, postID uuid.UUID) (engine.PostContext, error) {
	now := s.clock.Now()
	s.mu.Lock()
	cached, ok := s.contexts[postID]
	s.mu.Unlock()
	if ok && now.Before(cached.expires) {
		return cached.ctx, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := s.game.GetPostContext(callCtx, &gamev1.GetPostContextRequest{PostId: postID.String()})
	if err != nil {
		return engine.PostContext{}, err
	}
	pc := engine.PostContext{IdolName: resp.GetIdolName(), DestinationName: resp.GetDestinationName(),
		OutfitName: resp.GetOutfitName(), PoseName: resp.GetPoseName()}
	s.mu.Lock()
	if len(s.contexts) > 10000 {
		s.contexts = map[uuid.UUID]cachedContext{}
	}
	s.contexts[postID] = cachedContext{ctx: pc, expires: now.Add(time.Hour)}
	s.mu.Unlock()
	return pc, nil
}

// ListComments returns comments with visible_at <= now, sorted by
// (visible_at, comment_id), with a keyset cursor.
func (s *Service) ListComments(ctx context.Context, req *netizenv1.ListCommentsRequest) (*netizenv1.ListCommentsResponse, error) {
	postID, err := uuid.Parse(req.GetPostId())
	if err != nil {
		return nil, grpcx.Fail(codes.InvalidArgument, "INVALID_ID", "invalid post_id")
	}
	size := int(req.GetPageSize())
	if size <= 0 || size > 100 {
		size = 50
	}
	now := s.clock.Now()
	filter := doc("post_id", bin(postID), "visible_at", doc("$lte", now))
	if tok := req.GetPageToken(); tok != "" {
		t, id, err := decodeCursor(tok)
		if err != nil {
			return nil, grpcx.Fail(codes.InvalidArgument, "INVALID_PAGE_TOKEN", "invalid page_token")
		}
		filter = doc("post_id", bin(postID), "$and", bson.A{
			doc("visible_at", doc("$lte", now)),
			doc("$or", bson.A{
				doc("visible_at", doc("$gt", t)),
				doc("visible_at", t, "_id", doc("$gt", bin(id))),
			}),
		})
	}
	docs, err := s.findComments(ctx, filter, int64(size+1))
	if err != nil {
		return nil, grpcx.Internal(s.log, "ListComments", err)
	}
	resp := &netizenv1.ListCommentsResponse{}
	for i, d := range docs {
		if i == size {
			last := docs[size-1]
			resp.NextPageToken = encodeCursor(last.VisibleAt, unbin(last.ID))
			break
		}
		resp.Comments = append(resp.Comments, s.toProto(d))
	}
	return resp, nil
}

// ListUpcomingComments is used by the gateway to schedule SSE. Its results must
// not be passed to the app before their visible_at arrives.
func (s *Service) ListUpcomingComments(ctx context.Context, req *netizenv1.ListUpcomingCommentsRequest) (*netizenv1.ListUpcomingCommentsResponse, error) {
	postID, err := uuid.Parse(req.GetPostId())
	if err != nil {
		return nil, grpcx.Fail(codes.InvalidArgument, "INVALID_ID", "invalid post_id")
	}
	window := 15 * time.Minute
	if w := req.GetWindow(); w != nil && w.AsDuration() > 0 && w.AsDuration() <= time.Hour {
		window = w.AsDuration()
	}
	now := s.clock.Now()
	docs, err := s.findComments(ctx, doc("post_id", bin(postID),
		"visible_at", doc("$gt", now, "$lte", now.Add(window))), 500)
	if err != nil {
		return nil, grpcx.Internal(s.log, "ListUpcomingComments", err)
	}
	resp := &netizenv1.ListUpcomingCommentsResponse{}
	for _, d := range docs {
		resp.Comments = append(resp.Comments, s.toProto(d))
	}
	return resp, nil
}

func (s *Service) findComments(ctx context.Context, filter bson.D, limit int64) ([]commentDoc, error) {
	cur, err := s.store.comments.Find(ctx, filter,
		options.Find().SetSort(doc("visible_at", 1, "_id", 1)).SetLimit(limit))
	if err != nil {
		return nil, err
	}
	var docs []commentDoc
	err = cur.All(ctx, &docs)
	return docs, err
}

func (s *Service) toProto(d commentDoc) *netizenv1.Comment {
	netizenID := unbin(d.NetizenID)
	p := s.byID[netizenID]
	return &netizenv1.Comment{
		CommentId:          unbin(d.ID).String(),
		PostId:             unbin(d.PostID).String(),
		NetizenId:          netizenID.String(),
		NetizenHandle:      p.Handle,
		NetizenDisplayName: p.DisplayName,
		Text:               d.Text,
		VisibleAt:          timestamppb.New(d.VisibleAt),
	}
}

func encodeCursor(t time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(t.UnixMilli(), 10) + "|" + id.String()))
}

func decodeCursor(tok string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	ms, idStr, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, uuid.Nil, errors.New("malformed cursor")
	}
	n, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	id, err := uuid.Parse(idStr)
	return time.UnixMilli(n).UTC(), id, err
}
