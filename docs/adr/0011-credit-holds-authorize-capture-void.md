# ADR-0011: Credit holds with authorize, capture, void

- **Status:** Accepted
- **Date:** 9 November 2026, design version 3
- **Supersedes:** the credit reservation with a 30-minute TTL from version 2
- **Related:** [Design §18](../system-design.md#18-billing-and-monetization), [ADR-0014](0014-image-jobs-in-postgres.md)

## Context

In version 2, game called `ReserveCredit` before a post entered the queue, and the reservation was released automatically after 30 minutes. Yet batches run hourly, the p95 target for a photo session is 70 minutes, and an NSFW review can take up to 24 hours. A job that arrives right after a batch has left would lose its reservation before the photo is done.

When `image.completed` arrives, billing then has only two bad options: give the photo away for free, or deduct a balance that may already be spent and break the rule that "the balance is never negative".

The real problem isn't the number 30. A wall-clock TTL was used for two different things: cleaning up orphaned reservations (game crashed before the post was stored) and holding credit while the work is in progress.

## Decision

Use the card payment pattern: authorize, capture, void.

- `HoldCredit(hold_id = post_id)` creates a `pending` hold. Repeated calls with the same ID are idempotent.
- billing also consumes `post.submitted`. When that event arrives, the hold becomes `attached`.
- A `pending` hold that isn't attached within 30 minutes is voided by a sweeper. This is the only place a TTL is still used. If game's transaction fails, game calls `VoidHold` right away instead of waiting for the sweeper.
- An `attached` hold has no time limit and only ends through the job's outcome: `image.completed` captures it, and `image.failed` voids it.
- Reconciliation every 15 minutes checks holds that have been attached for more than 3 hours against `imaging.GetJobStatus`.
- If `post.submitted` arrives for a hold that was already voided (billing's consumer fell more than 30 minutes behind), billing tries to hold again. If that fails, billing publishes `credit.hold.failed`.

## Alternatives considered

- **Raising the TTL to several hours.** Postpones the problem without solving it, because a 24-hour review or a Modal outage can still outlast it. Players' credit would also stay locked for a long time whenever a reservation is orphaned.
- **Fully asynchronous holds.** game stores the post first, then billing holds the credit from the event. No orphaned reservations, but players lose the instant rejection when their balance is too low.
- **Deduct credit up front, refund on failure.** Simple, but refunds must always succeed, and the ledger fills up with back-and-forth entries.

## Consequences

- The hourly queue, the 24-hour review, or a Modal outage can no longer release a hold before the outcome is known.
- There are new statuses (`pending`, `attached`, `captured`, `voided`), one more consumer in billing, and the `credit.hold.failed` topic.
- A new property test: a hold is never both captured and voided.
- The worst case is billing falling more than 30 minutes behind, the balance already being spent, and the job already rendered. The result is one free photo that shows up in reconciliation, and this risk is accepted knowingly.
- Cosmetics don't use holds, because entitlements belong to billing itself, so a purchase needs only one local transaction.
