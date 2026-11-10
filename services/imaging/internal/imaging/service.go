// Package imaging implements the imaging service: it receives post.submitted,
// queues photo jobs, sends them to the renderer in batches, then publishes the
// results (docs/detail/flow-2-photo-session.md).
//
// M1 uses the image_jobs table as the queue (SELECT ... FOR UPDATE SKIP
// LOCKED) and a placeholder renderer. M2 adds RabbitMQ for delivery and the
// GPU worker on Modal, as described in ADR-0014.
package imaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	gamev1 "ivo/gen/ivo/game/v1"
	imagingv1 "ivo/gen/ivo/imaging/v1"
	"ivo/pkg/clock"
	"ivo/pkg/config"
	"ivo/pkg/events"
	"ivo/pkg/ids"
	"ivo/pkg/kafka"
	"ivo/pkg/outbox"
)

const (
	schema = "imaging"
	source = "ivo/imaging"
)

// safetyNegativeTags always go into the negative prompt (design §5).
var safetyNegativeTags = []string{"nsfw", "explicit", "nude", "loli", "child", "lowres",
	"bad_anatomy", "bad_hands", "watermark"}

// Config is the imaging service's configuration.
type Config struct {
	DatabaseURL   string
	Brokers       []string
	BatchSize     int
	BatchInterval time.Duration // a batch is sent when the queue is full or this interval has passed
	PollInterval  time.Duration
	RelayInterval time.Duration
}

// LoadConfig reads Config from the environment.
func LoadConfig() Config {
	return Config{
		DatabaseURL:   config.String("DATABASE_URL", "postgres://ivo:ivo@localhost:5432/ivo?sslmode=disable"),
		Brokers:       config.List("KAFKA_BROKERS", []string{"localhost:19092"}),
		BatchSize:     config.Int("BATCH_SIZE", 20),
		BatchInterval: config.Duration("BATCH_INTERVAL", time.Hour),
		PollInterval:  config.Duration("POLL_INTERVAL", time.Minute),
		RelayInterval: config.Duration("RELAY_INTERVAL", time.Second),
	}
}

// Service holds imaging's dependencies.
type Service struct {
	pool     *pgxpool.Pool
	clock    clock.Clock
	log      *slog.Logger
	cfg      Config
	renderer Renderer
}

// New creates a Service.
func New(pool *pgxpool.Pool, clk clock.Clock, log *slog.Logger, cfg Config, renderer Renderer) *Service {
	return &Service{pool: pool, clock: clk, log: log, cfg: cfg, renderer: renderer}
}

// Handlers returns the imaging consumer group's handlers per topic.
func (s *Service) Handlers() map[string]kafka.Handler {
	return map[string]kafka.Handler{events.TopicPostSubmitted: s.onPostSubmitted}
}

// onPostSubmitted creates a job with the status queued. A duplicate event never
// creates a duplicate job, thanks to processed_events and the unique (post_id, variant).
func (s *Service) onPostSubmitted(ctx context.Context, rec kafka.Record) error {
	var ev gamev1.PostSubmitted
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		return fmt.Errorf("corrupt post.submitted payload: %w", err)
	}
	postID, err1 := uuid.Parse(ev.GetPostId())
	playerID, err2 := uuid.Parse(ev.GetPlayerId())
	holdID, err3 := uuid.Parse(ev.GetHoldId())
	if err := errors.Join(err1, err2, err3); err != nil {
		return fmt.Errorf("invalid ID: %w", err)
	}
	spec, err := s.buildSpec(ctx, &ev)
	if err != nil {
		return err
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	lane := "batch"
	if ev.GetLane() == gamev1.PhotoLane_PHOTO_LANE_EXPRESS {
		lane = "express"
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	now := s.clock.Now()
	fresh, err := outbox.MarkProcessed(ctx, tx, schema, "imaging."+rec.Topic, rec.Meta.ID, now)
	if err != nil || !fresh {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO imaging.image_jobs (job_id, post_id, player_id, hold_id,
		variant, lane, status, render_spec, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7, $8)
		ON CONFLICT (post_id, variant) DO NOTHING`,
		ids.JobID(postID, int(ev.GetVariant())), postID, playerID, holdID, ev.GetVariant(), lane, specJSON, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// buildSpec translates catalog IDs into prompt tags.
func (s *Service) buildSpec(ctx context.Context, ev *gamev1.PostSubmitted) (RenderSpec, error) {
	choice := ev.GetChoice()
	itemIDs := []string{choice.GetOutfitId(), choice.GetPoseId(), choice.GetExpressionId()}
	rows, err := s.pool.Query(ctx, `SELECT item_id, positive_tags, negative_tags
		FROM imaging.render_catalog WHERE item_id = ANY($1)`, itemIDs)
	if err != nil {
		return RenderSpec{}, err
	}
	defer rows.Close()
	tags := map[string][2][]string{}
	for rows.Next() {
		var id string
		var pos, neg []string
		if err := rows.Scan(&id, &pos, &neg); err != nil {
			return RenderSpec{}, err
		}
		tags[id] = [2][]string{pos, neg}
	}
	if err := rows.Err(); err != nil {
		return RenderSpec{}, err
	}

	spec := RenderSpec{Width: 832, Height: 1216}
	spec.PositiveTags = append(spec.PositiveTags, choice.GetIdolTags()...)
	spec.NegativeTags = append(spec.NegativeTags, safetyNegativeTags...)
	for _, id := range itemIDs {
		t, ok := tags[id]
		if !ok {
			s.log.Warn("item missing from render_catalog, using its ID as a tag", "item_id", id)
			spec.PositiveTags = append(spec.PositiveTags, strings.ReplaceAll(id, "-", "_"))
			continue
		}
		spec.PositiveTags = append(spec.PositiveTags, t[0]...)
		spec.NegativeTags = append(spec.NegativeTags, t[1]...)
	}
	spec.PositiveTags = append(spec.PositiveTags, lightingTags(choice.GetLocalPublishHour())...)
	bg := ev.GetBackground()
	spec.Background = BackgroundSpec{S3Key: bg.GetS3Key(), FootX: bg.GetFootX(), FootY: bg.GetFootY(),
		Scale: bg.GetScale(), LightAzimuthDeg: bg.GetLightAzimuthDeg()}
	return spec, nil
}

func lightingTags(hour int32) []string {
	switch {
	case hour >= 5 && hour < 10:
		return []string{"morning", "soft_lighting"}
	case hour >= 10 && hour < 16:
		return []string{"daylight", "blue_sky"}
	case hour >= 16 && hour < 19:
		return []string{"sunset", "golden_hour"}
	default:
		return []string{"night", "city_lights"}
	}
}

// RunDispatcher sends a batch when the queue is full or BatchInterval has passed
// since the last batch. The express lane is sent immediately, one job per batch.
func (s *Service) RunDispatcher(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	var lastBatch time.Time
	for {
		for {
			n, err := s.dispatch(ctx, "express", 1)
			if err != nil && ctx.Err() == nil {
				s.log.Error("express dispatch failed", "err", err)
			}
			if err != nil || n == 0 {
				break
			}
		}
		due, err := s.batchDue(ctx, lastBatch)
		if err != nil && ctx.Err() == nil {
			s.log.Error("checking the batch queue failed", "err", err)
		}
		if due {
			if _, err := s.dispatch(ctx, "batch", s.cfg.BatchSize); err != nil && ctx.Err() == nil {
				s.log.Error("batch dispatch failed", "err", err)
			}
			lastBatch = s.clock.Now()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Service) batchDue(ctx context.Context, lastBatch time.Time) (bool, error) {
	var queued int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM imaging.image_jobs
		WHERE status = 'queued' AND lane = 'batch'`).Scan(&queued); err != nil {
		return false, err
	}
	if queued == 0 {
		return false, nil
	}
	return queued >= s.cfg.BatchSize || !s.clock.Now().Before(lastBatch.Add(s.cfg.BatchInterval)), nil
}

// dispatch marks up to limit jobs as dispatched in one batch, calls the
// renderer, then records the result of each job.
func (s *Service) dispatch(ctx context.Context, lane string, limit int) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	rows, err := tx.Query(ctx, `SELECT job_id, render_spec FROM imaging.image_jobs
		WHERE status = 'queued' AND lane = $1
		ORDER BY created_at, job_id
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, lane, limit)
	if err != nil {
		return 0, err
	}
	var jobs []Job
	var jobIDs []uuid.UUID
	for rows.Next() {
		var j Job
		var spec []byte
		if err := rows.Scan(&j.ID, &spec); err != nil {
			rows.Close()
			return 0, err
		}
		if err := json.Unmarshal(spec, &j.Spec); err != nil {
			rows.Close()
			return 0, fmt.Errorf("corrupt render_spec in job %s: %w", j.ID, err)
		}
		jobs = append(jobs, j)
		jobIDs = append(jobIDs, j.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(jobs) == 0 {
		return 0, nil
	}

	batchID := ids.New()
	now := s.clock.Now()
	if _, err := tx.Exec(ctx, `INSERT INTO imaging.image_batches (batch_id, lane, status, job_count, started_at)
		VALUES ($1, $2, 'running', $3, $4)`, batchID, lane, len(jobs), now); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE imaging.image_jobs SET status = 'dispatched', batch_id = $1,
		dispatched_at = $2 WHERE job_id = ANY($3)`, batchID, now, jobIDs); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	s.log.Info("batch dispatched", "batch_id", batchID, "lane", lane, "jobs", len(jobs))

	results, err := s.renderer.Render(ctx, batchID, jobs)
	if err != nil {
		s.finishBatch(ctx, batchID, "failed", 0)
		return len(jobs), errors.Join(fmt.Errorf("rendering batch %s failed", batchID), err, s.requeue(ctx, jobIDs))
	}
	var gpu float64
	for _, r := range results {
		gpu += r.GPUSeconds
		if err := s.record(ctx, r); err != nil {
			s.log.Error("recording the job result failed", "job_id", r.JobID, "err", err)
		}
	}
	s.finishBatch(ctx, batchID, "done", gpu)
	return len(jobs), nil
}

func (s *Service) finishBatch(ctx context.Context, batchID uuid.UUID, status string, gpu float64) {
	if _, err := s.pool.Exec(ctx, `UPDATE imaging.image_batches SET status = $2, finished_at = $3,
		gpu_seconds = $4 WHERE batch_id = $1`, batchID, status, s.clock.Now(), gpu); err != nil {
		s.log.Error("closing the batch failed", "batch_id", batchID, "err", err)
	}
}

// requeue puts jobs back in the queue after a technical failure, or marks them
// failed once their retries are used up.
func (s *Service) requeue(ctx context.Context, jobIDs []uuid.UUID) error {
	for _, id := range jobIDs {
		if err := s.record(ctx, Result{JobID: id, Outcome: imagingv1.RenderOutcome_RENDER_OUTCOME_ERROR,
			Error: "batch failed"}); err != nil {
			return err
		}
	}
	return nil
}

// record stores one job's result and writes its outgoing event in one transaction.
func (s *Service) record(ctx context.Context, r Result) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	now := s.clock.Now()

	var postID, playerID, holdID uuid.UUID
	var variant, retries int32
	err = tx.QueryRow(ctx, `SELECT post_id, player_id, hold_id, variant, retry_count
		FROM imaging.image_jobs WHERE job_id = $1 AND status = 'dispatched' FOR UPDATE`, r.JobID).
		Scan(&postID, &playerID, &holdID, &variant, &retries)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // already recorded
	}
	if err != nil {
		return err
	}

	scores, err := json.Marshal(r.NSFWScores)
	if err != nil {
		return err
	}
	var event proto.Message
	var topic string
	switch r.Outcome {
	case imagingv1.RenderOutcome_RENDER_OUTCOME_PASSED:
		if _, err := tx.Exec(ctx, `UPDATE imaging.image_jobs SET status = 'completed', attempts = $2,
			gpu_seconds = $3, nsfw_scores = $4, image_tags = $5, output_s3_key = $6, finished_at = $7
			WHERE job_id = $1`, r.JobID, r.Attempts, r.GPUSeconds, scores, r.ImageTags, r.S3Key, now); err != nil {
			return err
		}
		topic = events.TopicImageCompleted
		event = &imagingv1.ImageCompleted{JobId: r.JobID.String(), PostId: postID.String(),
			PlayerId: playerID.String(), HoldId: holdID.String(), Variant: variant, S3Key: r.S3Key,
			ImageTags: r.ImageTags, Attempts: r.Attempts}
	case imagingv1.RenderOutcome_RENDER_OUTCOME_REJECTED:
		if _, err := tx.Exec(ctx, `UPDATE imaging.image_jobs SET status = 'rejected', attempts = $2,
			gpu_seconds = $3, nsfw_scores = $4, failure_reason = 'nsfw_rejected', finished_at = $5
			WHERE job_id = $1`, r.JobID, r.Attempts, r.GPUSeconds, scores, now); err != nil {
			return err
		}
		topic = events.TopicImageFailed
		event = &imagingv1.ImageFailed{JobId: r.JobID.String(), PostId: postID.String(), PlayerId: playerID.String(),
			HoldId: holdID.String(), Variant: variant, Reason: imagingv1.ImageFailureReason_IMAGE_FAILURE_REASON_NSFW_REJECTED}
	case imagingv1.RenderOutcome_RENDER_OUTCOME_ERROR:
		if retries < 3 {
			_, err := tx.Exec(ctx, `UPDATE imaging.image_jobs SET status = 'queued', batch_id = NULL,
				dispatched_at = NULL, retry_count = retry_count + 1 WHERE job_id = $1`, r.JobID)
			if err != nil {
				return err
			}
			return tx.Commit(ctx)
		}
		if _, err := tx.Exec(ctx, `UPDATE imaging.image_jobs SET status = 'failed',
			failure_reason = 'technical', finished_at = $2 WHERE job_id = $1`, r.JobID, now); err != nil {
			return err
		}
		topic = events.TopicImageFailed
		event = &imagingv1.ImageFailed{JobId: r.JobID.String(), PostId: postID.String(), PlayerId: playerID.String(),
			HoldId: holdID.String(), Variant: variant, Reason: imagingv1.ImageFailureReason_IMAGE_FAILURE_REASON_TECHNICAL}
	default:
		return fmt.Errorf("render outcome %s is not supported in M1 yet", r.Outcome)
	}

	msg, err := outbox.NewEvent(source, topic, playerID.String(), event, now)
	if err != nil {
		return err
	}
	msg.PlayerID = &playerID
	if err := outbox.Insert(ctx, tx, schema, msg); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
