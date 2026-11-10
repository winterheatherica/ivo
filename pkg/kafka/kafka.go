// Package kafka wraps franz-go for Apache Kafka: a producer for the outbox and for
// direct events, and a consumer group with retries and a DLQ
// (docs/detail/README.md#consumers).
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"ivo/pkg/events"
	"ivo/pkg/outbox"
)

// DefaultRetries are the retry delays inside the consumer before a message goes to the DLQ.
var DefaultRetries = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}

// Producer sends messages to Kafka and waits for the ack.
type Producer struct {
	client *kgo.Client
}

// NewProducer creates an idempotent producer (the franz-go default).
func NewProducer(brokers []string) (*Producer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	return &Producer{client: cl}, nil
}

// Close closes the producer's connections.
func (p *Producer) Close() { p.client.Close() }

// Publish sends one outbox message. It satisfies outbox.Publisher.
func (p *Producer) Publish(ctx context.Context, m outbox.Message) error {
	rec := &kgo.Record{Topic: m.Topic, Key: []byte(m.Key), Value: m.Payload, Headers: toHeaders(m.Headers)}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

// ProduceEvent sends an event directly, without the outbox, using the caller's
// event ID. Used by netizen, whose idempotency comes from deterministic IDs.
func (p *Producer) ProduceEvent(ctx context.Context, source, topic, key string, eventID uuid.UUID, msg proto.Message, now time.Time) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", topic, err)
	}
	meta := events.Meta{ID: eventID, Type: events.Type(topic), Source: source, Time: now}
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: body, Headers: toHeaders(meta.Headers())}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

func toHeaders(m map[string]string) []kgo.RecordHeader {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]kgo.RecordHeader, 0, len(keys))
	for _, k := range keys {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(m[k])})
	}
	return out
}

// Record is a message passed to a handler.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Meta      events.Meta
}

// Handler processes one message. Handlers must be idempotent, because delivery
// is at-least-once and a handler can be called again on retry.
type Handler func(ctx context.Context, rec Record) error

// Consumer runs one consumer group for several topics.
type Consumer struct {
	Brokers  []string
	Group    string
	Name     string // the service name, for the x-consumer header
	Handlers map[string]Handler
	Retries  []time.Duration
	Log      *slog.Logger
}

// Run processes messages until ctx is cancelled. Offsets are committed after
// every message from one poll has been processed or moved to the DLQ.
func (c *Consumer) Run(ctx context.Context) error {
	topics := make([]string, 0, len(c.Handlers))
	for t := range c.Handlers {
		topics = append(topics, t)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.Group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			cl.AllowRebalance()
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.Log.Error("fetch failed", "topic", topic, "partition", partition, "err", err)
		})
		var procErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if procErr == nil {
				procErr = c.handle(ctx, cl, r)
			}
		})
		if procErr != nil {
			cl.AllowRebalance()
			if ctx.Err() != nil {
				return nil
			}
			return procErr
		}
		if err := cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.Log.Error("offset commit failed", "err", err)
		}
		cl.AllowRebalance()
	}
}

func (c *Consumer) handle(ctx context.Context, cl *kgo.Client, r *kgo.Record) error {
	rec := Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value,
		Headers: make(map[string]string, len(r.Headers))}
	for _, h := range r.Headers {
		rec.Headers[h.Key] = string(h.Value)
	}
	meta, err := events.ParseMeta(rec.Headers)
	if err != nil {
		return c.deadLetter(ctx, cl, r, err)
	}
	rec.Meta = meta

	handler := c.Handlers[r.Topic]
	retries := c.Retries
	if retries == nil {
		retries = DefaultRetries
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		if lastErr = handler(ctx, rec); lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt >= len(retries) {
			break
		}
		c.Log.Warn("handler failed, retrying", "topic", r.Topic, "event_id", meta.ID,
			"attempt", attempt+1, "err", lastErr)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retries[attempt]):
		}
	}
	return c.deadLetter(ctx, cl, r, lastErr)
}

func (c *Consumer) deadLetter(ctx context.Context, cl *kgo.Client, r *kgo.Record, cause error) error {
	headers := append([]kgo.RecordHeader{}, r.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: events.HeaderError, Value: []byte(cause.Error())},
		kgo.RecordHeader{Key: events.HeaderConsumer, Value: []byte(c.Name + "." + r.Topic)},
	)
	dlq := &kgo.Record{Topic: events.DLQ(r.Topic), Key: r.Key, Value: r.Value, Headers: headers}
	if err := cl.ProduceSync(ctx, dlq).FirstErr(); err != nil {
		return errors.Join(fmt.Errorf("failed to send to %s", dlq.Topic), err)
	}
	c.Log.Error("message moved to the DLQ", "topic", r.Topic, "partition", r.Partition,
		"offset", r.Offset, "err", cause)
	return nil
}
