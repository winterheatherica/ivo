// Package outbox writes events to the outbox table inside the data transaction,
// and a relay publishes them once available_at has passed (ADR-0008, ADR-0012).
// The package also records processed_events for idempotent consumers.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"ivo/pkg/clock"
	"ivo/pkg/events"
	"ivo/pkg/ids"
	"ivo/pkg/pgdb"
)

// Outbox message destinations.
const (
	DestinationKafka    = "kafka"
	DestinationRabbitMQ = "rabbitmq"
)

// Message is one outbox row.
type Message struct {
	EventID     uuid.UUID
	Destination string
	Topic       string
	Key         string
	EventType   string
	Payload     []byte
	Headers     map[string]string
	PlayerID    *uuid.UUID
	OccurredAt  time.Time
	AvailableAt time.Time
}

// NewEvent prepares a Kafka message from a Protobuf payload, with a UUIDv7
// event ID and available_at = now. For the delayed outbox, change AvailableAt.
func NewEvent(source, topic, key string, payload proto.Message, now time.Time) (Message, error) {
	body, err := proto.Marshal(payload)
	if err != nil {
		return Message{}, fmt.Errorf("marshal %s: %w", topic, err)
	}
	id := ids.New()
	meta := events.Meta{ID: id, Type: events.Type(topic), Source: source, Time: now}
	return Message{
		EventID:     id,
		Destination: DestinationKafka,
		Topic:       topic,
		Key:         key,
		EventType:   meta.Type,
		Payload:     body,
		Headers:     meta.Headers(),
		OccurredAt:  now,
		AvailableAt: now,
	}, nil
}

// Insert writes m to <schema>.outbox inside the caller's transaction.
func Insert(ctx context.Context, tx pgx.Tx, schema string, m Message) error {
	headers, err := json.Marshal(m.Headers)
	if err != nil {
		return err
	}
	dest := m.Destination
	if dest == "" {
		dest = DestinationKafka
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+pgdb.Table(schema, "outbox")+`
		(event_id, destination, topic, message_key, event_type, payload, headers,
		 player_id, occurred_at, available_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		m.EventID, dest, m.Topic, m.Key, m.EventType, m.Payload, headers,
		m.PlayerID, m.OccurredAt, m.AvailableAt)
	return err
}

// MarkProcessed records the event for this consumer inside the caller's transaction.
// A false result means the event was already processed and its effects must be skipped.
func MarkProcessed(ctx context.Context, tx pgx.Tx, schema, consumer string, eventID uuid.UUID, now time.Time) (bool, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO `+pgdb.Table(schema, "processed_events")+`
		(consumer, event_id, processed_at) VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, consumer, eventID, now)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// Publisher sends one message and waits for the broker's confirmation.
type Publisher interface {
	Publish(ctx context.Context, m Message) error
}

// Relay publishes outbox rows that are due. Several replicas may run at the same
// time because rows are taken with FOR UPDATE SKIP LOCKED.
type Relay struct {
	Pool       *pgxpool.Pool
	Schema     string
	Publishers map[string]Publisher
	Clock      clock.Clock
	Interval   time.Duration
	BatchSize  int
	Log        *slog.Logger
}

// Run runs the relay until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		for {
			n, err := r.Flush(ctx)
			if err != nil {
				if ctx.Err() == nil {
					r.Log.Error("outbox relay failed", "schema", r.Schema, "err", err)
				}
				break
			}
			if n < r.BatchSize {
				break
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

type pending struct {
	id  int64
	msg Message
}

// Flush publishes one batch of rows that are due and returns how many succeeded.
// A row that fails gets its attempts and last_error recorded, and is tried
// again on the next tick.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	table := pgdb.Table(r.Schema, "outbox")
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	now := r.Clock.Now()
	rows, err := tx.Query(ctx, `SELECT id, event_id, destination, topic, message_key, event_type,
		payload, headers, occurred_at
		FROM `+table+`
		WHERE published_at IS NULL AND available_at <= $1
		ORDER BY available_at, id
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, now, r.BatchSize)
	if err != nil {
		return 0, err
	}
	var batch []pending
	for rows.Next() {
		var p pending
		var headers []byte
		if err := rows.Scan(&p.id, &p.msg.EventID, &p.msg.Destination, &p.msg.Topic, &p.msg.Key,
			&p.msg.EventType, &p.msg.Payload, &headers, &p.msg.OccurredAt); err != nil {
			rows.Close()
			return 0, err
		}
		if err := json.Unmarshal(headers, &p.msg.Headers); err != nil {
			rows.Close()
			return 0, fmt.Errorf("corrupt headers in outbox row %d: %w", p.id, err)
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	var published []int64
	var failID int64
	var failErr error
	for _, p := range batch {
		pub, ok := r.Publishers[p.msg.Destination]
		if !ok {
			failID, failErr = p.id, fmt.Errorf("publisher %q is not configured", p.msg.Destination)
			break
		}
		if err := pub.Publish(ctx, p.msg); err != nil {
			failID, failErr = p.id, err
			break
		}
		published = append(published, p.id)
	}

	if len(published) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE `+table+` SET published_at = $1, attempts = attempts + 1
			WHERE id = ANY($2)`, now, published); err != nil {
			return 0, err
		}
	}
	if failErr != nil {
		if _, err := tx.Exec(ctx, `UPDATE `+table+` SET attempts = attempts + 1, last_error = $2
			WHERE id = $1`, failID, failErr.Error()); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(published), failErr
}
