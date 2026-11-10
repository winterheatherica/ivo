// Package ids creates entity IDs. New entities use UUIDv7; derived IDs that
// must be deterministic use UUIDv5 with fixed namespaces
// (docs/detail/README.md#ids).
package ids

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// The namespaces must never change once there is data, because old IDs could
// no longer be recomputed.
var (
	NSPost    = uuid.MustParse("de398215-92f3-4a8e-98f3-3e2cd9ed7e6d")
	NSReroll  = uuid.MustParse("7ad35208-2b23-424f-831c-a6bba9d19ae5")
	NSJob     = uuid.MustParse("78eaaf92-44ad-4dcc-bb56-0ea6f30697f1")
	NSComment = uuid.MustParse("0bab841a-1ff3-4e6b-84a1-08b296d2eb66")
	NSEvent   = uuid.MustParse("f75efc61-a535-427c-abc9-9af6d53081e0")
	NSNetizen = uuid.MustParse("4d2fde18-7d89-4bb8-8a3a-bd8786574879")
)

// New creates a UUIDv7 for a new entity.
func New() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// PostID = UUIDv5(NS_POST, "player_id:idempotency_key").
func PostID(playerID, idempotencyKey uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(NSPost, []byte(playerID.String()+":"+idempotencyKey.String()))
}

// RerollHoldID = UUIDv5(NS_REROLL, "player_id:idempotency_key").
func RerollHoldID(playerID, idempotencyKey uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(NSReroll, []byte(playerID.String()+":"+idempotencyKey.String()))
}

// JobID = UUIDv5(NS_JOB, "post_id:variant").
func JobID(postID uuid.UUID, variant int) uuid.UUID {
	return uuid.NewSHA1(NSJob, []byte(fmt.Sprintf("%s:%d", postID, variant)))
}

// CommentID = UUIDv5(NS_COMMENT, "post_id:netizen_id").
func CommentID(postID, netizenID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(NSComment, []byte(postID.String()+":"+netizenID.String()))
}

// NetizenID = UUIDv5(NS_NETIZEN, "persona:<number>"), used when seeding personas.
func NetizenID(index int) uuid.UUID {
	return uuid.NewSHA1(NSNetizen, []byte(fmt.Sprintf("persona:%d", index)))
}

// EventID = UUIDv5(NS_EVENT, the parts joined with ":").
func EventID(parts ...string) uuid.UUID {
	return uuid.NewSHA1(NSEvent, []byte(strings.Join(parts, ":")))
}
