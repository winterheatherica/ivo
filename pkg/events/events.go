// Package events holds the topic names and CloudEvents headers (binary mode) for
// Kafka messages (docs/detail/README.md#events).
package events

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Kafka topic names.
const (
	TopicPostSubmitted        = "post.submitted"
	TopicPostPublished        = "post.published"
	TopicImageCompleted       = "image.completed"
	TopicImageFailed          = "image.failed"
	TopicImageReviewRequested = "image.review.requested"
	TopicEngagementRecorded   = "engagement.recorded"
	TopicCommentRequested     = "netizen.comment.requested"
	TopicUsageRecorded        = "usage.recorded"
)

// Header names.
const (
	HeaderSpecVersion = "ce_specversion"
	HeaderID          = "ce_id"
	HeaderType        = "ce_type"
	HeaderSource      = "ce_source"
	HeaderTime        = "ce_time"
	HeaderContentType = "content-type"
	HeaderTraceParent = "traceparent"
	HeaderReplayOf    = "replay_of"
	HeaderError       = "x-error"
	HeaderConsumer    = "x-consumer"
)

// DLQ returns the DLQ topic name for a source topic.
func DLQ(topic string) string { return topic + ".dlq" }

// Type returns the ce_type for a topic, for example ivo.post.submitted.v1.
func Type(topic string) string { return "ivo." + topic + ".v1" }

// Meta is the event metadata carried in the headers.
type Meta struct {
	ID          uuid.UUID
	Type        string
	Source      string
	Time        time.Time
	TraceParent string
}

// Headers turns Meta into binary-mode CloudEvents headers.
func (m Meta) Headers() map[string]string {
	h := map[string]string{
		HeaderSpecVersion: "1.0",
		HeaderID:          m.ID.String(),
		HeaderType:        m.Type,
		HeaderSource:      m.Source,
		HeaderTime:        m.Time.UTC().Format(time.RFC3339Nano),
		HeaderContentType: "application/protobuf",
	}
	if m.TraceParent != "" {
		h[HeaderTraceParent] = m.TraceParent
	}
	return h
}

// ParseMeta reads the CloudEvents headers. ce_id is required because idempotency depends on it.
func ParseMeta(h map[string]string) (Meta, error) {
	id, err := uuid.Parse(h[HeaderID])
	if err != nil {
		return Meta{}, fmt.Errorf("invalid %s header: %w", HeaderID, err)
	}
	m := Meta{ID: id, Type: h[HeaderType], Source: h[HeaderSource], TraceParent: h[HeaderTraceParent]}
	if raw := h[HeaderTime]; raw != "" {
		if m.Time, err = time.Parse(time.RFC3339Nano, raw); err != nil {
			return Meta{}, fmt.Errorf("invalid %s header: %w", HeaderTime, err)
		}
	}
	return m, nil
}
