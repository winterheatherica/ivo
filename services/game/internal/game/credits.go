package game

import (
	"context"

	"github.com/google/uuid"
)

// Credits holds and releases credit for posts (ADR-0011). M1 uses Free;
// M2 replaces it with a gRPC client for billing.HoldCredit and VoidHold.
type Credits interface {
	Hold(ctx context.Context, holdID, playerID uuid.UUID, amount int64) error
	Void(ctx context.Context, holdID uuid.UUID, reason string) error
}

// Free holds nothing. Used until the billing service exists.
type Free struct{}

// Hold always succeeds.
func (Free) Hold(context.Context, uuid.UUID, uuid.UUID, int64) error { return nil }

// Void does nothing.
func (Free) Void(context.Context, uuid.UUID, string) error { return nil }
