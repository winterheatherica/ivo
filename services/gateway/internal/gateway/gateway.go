// Package gateway is the HTTP entry point for the app and the admin web app.
// The gateway stores no domain data: every endpoint is forwarded to the owning
// service over gRPC (docs/detail/api-gateway.md).
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	gamev1 "ivo/gen/ivo/game/v1"
	netizenv1 "ivo/gen/ivo/netizen/v1"
	"ivo/pkg/config"
	"ivo/pkg/grpcx"
	"ivo/pkg/ids"
)

// Config is the gateway's configuration.
type Config struct {
	HTTPAddr    string
	GameAddr    string
	NetizenAddr string
	AuthMode    string // "dev": identity from the X-Player-Id header, until the auth server arrives in M3
}

// LoadConfig reads Config from the environment.
func LoadConfig() Config {
	return Config{
		HTTPAddr:    config.String("HTTP_ADDR", ":8080"),
		GameAddr:    config.String("GAME_ADDR", "localhost:9091"),
		NetizenAddr: config.String("NETIZEN_ADDR", "localhost:9092"),
		AuthMode:    config.String("AUTH_MODE", "jwt"),
	}
}

// Server translates REST into gRPC.
type Server struct {
	game    gamev1.GameServiceClient
	netizen netizenv1.NetizenServiceClient
	log     *slog.Logger
	cfg     Config
}

// New creates a Server.
func New(game gamev1.GameServiceClient, netizen netizenv1.NetizenServiceClient, log *slog.Logger, cfg Config) *Server {
	return &Server{game: game, netizen: netizen, log: log, cfg: cfg}
}

// Handler returns the HTTP router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("POST /v1/posts", s.auth(s.createPost))
	mux.Handle("GET /v1/posts", s.auth(s.listPosts))
	mux.Handle("GET /v1/posts/{post_id}", s.auth(s.getPost))
	mux.Handle("GET /v1/posts/{post_id}/comments", s.auth(s.listComments))
	mux.Handle("GET /v1/me/popularity", s.auth(s.popularity))
	mux.Handle("GET /v1/leaderboard", s.auth(s.leaderboard))
	return s.requestID(mux)
}

type requestIDKey struct{}

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-Id")
		if _, err := uuid.Parse(rid); err != nil {
			rid = ids.New().String()
		}
		w.Header().Set("X-Request-Id", rid)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, rid)))
		s.log.Info("http", "method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(), "request_id", rid)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// auth reads the player's identity. In M1 (AUTH_MODE=dev) the identity comes from
// the X-Player-Id header; M3 replaces it with JWT validation through JWKS.
func (s *Server) auth(next func(http.ResponseWriter, *http.Request, uuid.UUID)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.AuthMode != "dev" {
			writeProblem(w, r, http.StatusUnauthorized, "TOKEN_INVALID", "JWT authentication isn't available before M3.")
			return
		}
		playerID, err := uuid.Parse(r.Header.Get("X-Player-Id"))
		if err != nil {
			writeProblem(w, r, http.StatusUnauthorized, "TOKEN_INVALID", "The X-Player-Id header must contain a UUID.")
			return
		}
		next(w, r, playerID)
	})
}

// rpc prepares a gRPC context with a deadline and the x-request-id.
func rpc(r *http.Request, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	if rid, ok := r.Context().Value(requestIDKey{}).(string); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", rid)
	}
	return ctx, cancel
}

type createPostBody struct {
	DestinationID      string  `json:"destination_id"`
	OutfitID           string  `json:"outfit_id"`
	PoseID             string  `json:"pose_id"`
	ExpressionID       string  `json:"expression_id"`
	Caption            string  `json:"caption"`
	RequestedPublishAt *string `json:"requested_publish_at"`
	Express            bool    `json:"express"`
}

func (s *Server) createPost(w http.ResponseWriter, r *http.Request, playerID uuid.UUID) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeProblem(w, r, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "The Idempotency-Key header is required.")
		return
	}
	var body createPostBody
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "INVALID_BODY", "The JSON body is invalid.")
		return
	}
	req := &gamev1.CreatePostRequest{
		PlayerId:       playerID.String(),
		IdempotencyKey: key,
		DestinationId:  body.DestinationID,
		OutfitId:       body.OutfitID,
		PoseId:         body.PoseID,
		ExpressionId:   body.ExpressionID,
		Caption:        body.Caption,
		Express:        body.Express,
	}
	if body.RequestedPublishAt != nil {
		t, err := time.Parse(time.RFC3339, *body.RequestedPublishAt)
		if err != nil {
			writeProblem(w, r, http.StatusBadRequest, "PUBLISH_TIME_INVALID", "requested_publish_at must be in RFC 3339 format.")
			return
		}
		req.RequestedPublishAt = timestamppb.New(t)
	}
	ctx, cancel := rpc(r, 5*time.Second)
	defer cancel()
	resp, err := s.game.CreatePost(ctx, req)
	if err != nil {
		s.grpcError(w, r, err)
		return
	}
	code := http.StatusCreated
	if resp.GetReplayed() {
		code = http.StatusOK
	}
	writeJSON(w, code, toPostView(resp.GetPost()))
}

func (s *Server) listPosts(w http.ResponseWriter, r *http.Request, playerID uuid.UUID) {
	q := r.URL.Query()
	size, _ := strconv.Atoi(q.Get("page_size"))
	ctx, cancel := rpc(r, 2*time.Second)
	defer cancel()
	resp, err := s.game.ListPosts(ctx, &gamev1.ListPostsRequest{
		PlayerId:           playerID.String(),
		IncludeUnpublished: q.Get("include_unpublished") == "true",
		PageSize:           int32(size),
		PageToken:          q.Get("page_token"),
	})
	if err != nil {
		s.grpcError(w, r, err)
		return
	}
	views := make([]postView, 0, len(resp.GetPosts()))
	for _, p := range resp.GetPosts() {
		views = append(views, toPostView(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"posts": views, "next_page_token": resp.GetNextPageToken()})
}

func (s *Server) getPost(w http.ResponseWriter, r *http.Request, playerID uuid.UUID) {
	ctx, cancel := rpc(r, 2*time.Second)
	defer cancel()
	resp, err := s.game.GetPost(ctx, &gamev1.GetPostRequest{PlayerId: playerID.String(), PostId: r.PathValue("post_id")})
	if err != nil {
		s.grpcError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toPostView(resp.GetPost()))
}

func (s *Server) listComments(w http.ResponseWriter, r *http.Request, playerID uuid.UUID) {
	ctx, cancel := rpc(r, 3*time.Second)
	defer cancel()
	postID := r.PathValue("post_id")
	// Ownership is checked through game first; another player's post gets a 404.
	if _, err := s.game.GetPost(ctx, &gamev1.GetPostRequest{PlayerId: playerID.String(), PostId: postID}); err != nil {
		s.grpcError(w, r, err)
		return
	}
	size, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	resp, err := s.netizen.ListComments(ctx, &netizenv1.ListCommentsRequest{
		PostId: postID, PageSize: int32(size), PageToken: r.URL.Query().Get("page_token"),
	})
	if err != nil {
		s.grpcError(w, r, err)
		return
	}
	views := make([]commentView, 0, len(resp.GetComments()))
	for _, c := range resp.GetComments() {
		views = append(views, commentView{
			CommentID:          c.GetCommentId(),
			NetizenID:          c.GetNetizenId(),
			NetizenHandle:      c.GetNetizenHandle(),
			NetizenDisplayName: c.GetNetizenDisplayName(),
			IsAI:               true, // every netizen is labeled as an AI account (design §5)
			Text:               c.GetText(),
			VisibleAt:          c.GetVisibleAt().AsTime(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"comments": views, "next_page_token": resp.GetNextPageToken()})
}

func (s *Server) popularity(w http.ResponseWriter, r *http.Request, playerID uuid.UUID) {
	ctx, cancel := rpc(r, 2*time.Second)
	defer cancel()
	resp, err := s.game.GetPopularity(ctx, &gamev1.GetPopularityRequest{PlayerId: playerID.String()})
	if err != nil {
		s.grpcError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"followers":                resp.GetFollowers(),
		"season_id":                resp.GetSeasonId(),
		"season_score":             resp.GetSeasonScore(),
		"unlocked_destination_ids": nonNil(resp.GetUnlockedDestinationIds()),
		"as_of":                    resp.GetAsOf().AsTime(),
	})
}

func (s *Server) leaderboard(w http.ResponseWriter, r *http.Request, _ uuid.UUID) {
	q := r.URL.Query()
	size, _ := strconv.Atoi(q.Get("page_size"))
	ctx, cancel := rpc(r, 2*time.Second)
	defer cancel()
	resp, err := s.game.GetLeaderboard(ctx, &gamev1.GetLeaderboardRequest{
		SeasonId: q.Get("season_id"), PageSize: int32(size), PageToken: q.Get("page_token"),
	})
	if err != nil {
		s.grpcError(w, r, err)
		return
	}
	entries := make([]map[string]any, 0, len(resp.GetEntries()))
	for _, e := range resp.GetEntries() {
		entries = append(entries, map[string]any{"rank": e.GetRank(), "player_id": e.GetPlayerId(),
			"display_name": e.GetDisplayName(), "idol_name": e.GetIdolName(), "score": e.GetScore()})
	}
	out := map[string]any{"season_id": resp.GetSeasonId(), "entries": entries,
		"next_page_token": resp.GetNextPageToken()}
	if resp.GetAsOf() != nil {
		out["as_of"] = resp.GetAsOf().AsTime()
	}
	writeJSON(w, http.StatusOK, out)
}

type postView struct {
	PostID             string     `json:"post_id"`
	Status             string     `json:"status"`
	Published          bool       `json:"published"`
	DestinationID      string     `json:"destination_id"`
	OutfitID           string     `json:"outfit_id"`
	PoseID             string     `json:"pose_id"`
	ExpressionID       string     `json:"expression_id"`
	Caption            string     `json:"caption"`
	Express            bool       `json:"express"`
	CreditCost         int64      `json:"credit_cost"`
	CreatedAt          time.Time  `json:"created_at"`
	RequestedPublishAt time.Time  `json:"requested_publish_at"`
	EstimatedReadyAt   time.Time  `json:"estimated_ready_at"`
	PublishedAt        *time.Time `json:"published_at,omitempty"`
	ImageURL           string     `json:"image_url,omitempty"`
	PhotoAttribution   string     `json:"photo_attribution"`
	Likes              int64      `json:"likes"`
	Comments           int64      `json:"comments"`
	FailureReason      string     `json:"failure_reason,omitempty"`
}

func toPostView(p *gamev1.Post) postView {
	v := postView{
		PostID:             p.GetPostId(),
		Status:             shortEnum(p.GetStatus().String(), "POST_STATUS_"),
		Published:          p.GetPublished(),
		DestinationID:      p.GetDestinationId(),
		OutfitID:           p.GetOutfitId(),
		PoseID:             p.GetPoseId(),
		ExpressionID:       p.GetExpressionId(),
		Caption:            p.GetCaption(),
		Express:            p.GetExpress(),
		CreditCost:         p.GetCreditCost(),
		CreatedAt:          p.GetCreatedAt().AsTime(),
		RequestedPublishAt: p.GetRequestedPublishAt().AsTime(),
		EstimatedReadyAt:   p.GetEstimatedReadyAt().AsTime(),
		ImageURL:           p.GetImageUrl(),
		PhotoAttribution:   p.GetPhotoAttribution(),
		Likes:              p.GetLikes(),
		Comments:           p.GetComments(),
		FailureReason:      p.GetFailureReason(),
	}
	if p.GetPublishedAt() != nil {
		t := p.GetPublishedAt().AsTime()
		v.PublishedAt = &t
	}
	return v
}

type commentView struct {
	CommentID          string    `json:"comment_id"`
	NetizenID          string    `json:"netizen_id"`
	NetizenHandle      string    `json:"netizen_handle"`
	NetizenDisplayName string    `json:"netizen_display_name"`
	IsAI               bool      `json:"is_ai"`
	Text               string    `json:"text"`
	VisibleAt          time.Time `json:"visible_at"`
}

// shortEnum turns POST_STATUS_QUEUED into "queued".
func shortEnum(name, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

type problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Detail    string `json:"detail,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

var problemTitles = map[string]string{
	"INSUFFICIENT_CREDIT":    "Insufficient credit",
	"DAILY_LIMIT":            "Daily post limit reached",
	"DESTINATION_LOCKED":     "Destination is still locked",
	"IDEMPOTENCY_KEY_REUSED": "Idempotency-Key already used",
	"INVALID_CHOICE":         "Invalid choice",
	"CAPTION_REJECTED":       "Caption rejected",
	"PUBLISH_TIME_INVALID":   "Invalid publish time",
	"NOT_FOUND":              "Not found",
	"TOKEN_INVALID":          "Authentication failed",
	"UNAVAILABLE":            "Service temporarily unavailable",
}

// writeProblem writes an error in the Problem Details format (RFC 9457).
func writeProblem(w http.ResponseWriter, r *http.Request, code int, errCode, detail string) {
	title, ok := problemTitles[errCode]
	if !ok {
		title = http.StatusText(code)
	}
	rid, _ := r.Context().Value(requestIDKey{}).(string)
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(problem{
		Type:      "/errors/" + strings.ReplaceAll(strings.ToLower(errCode), "_", "-"),
		Title:     title,
		Status:    code,
		Code:      errCode,
		Detail:    detail,
		RequestID: rid,
	})
}

// httpStatus maps gRPC statuses to HTTP (docs/detail/api-gateway.md#errors).
func httpStatus(code codes.Code, reason string) int {
	switch code {
	case codes.InvalidArgument:
		if reason == "IDEMPOTENCY_KEY_REUSED" {
			return http.StatusUnprocessableEntity
		}
		return http.StatusBadRequest
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.FailedPrecondition:
		return http.StatusConflict
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable, codes.DeadlineExceeded:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func (s *Server) grpcError(w http.ResponseWriter, r *http.Request, err error) {
	st := status.Convert(err)
	reason := grpcx.Reason(err)
	code := httpStatus(st.Code(), reason)
	detail := st.Message()
	switch {
	case code == http.StatusServiceUnavailable:
		reason, detail = "UNAVAILABLE", "Try again with the same Idempotency-Key."
	case code == http.StatusInternalServerError:
		s.log.Error("error from a service", "code", st.Code().String(), "err", err)
		reason, detail = "INTERNAL", ""
	case reason == "":
		reason = strings.ToUpper(st.Code().String())
	}
	if errors.Is(r.Context().Err(), context.Canceled) {
		return // the client already closed the connection
	}
	writeProblem(w, r, code, reason, detail)
}
