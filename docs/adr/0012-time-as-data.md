# ADR-0012: Time as data

- **Status:** Accepted
- **Date:** 9 November 2026, design version 3
- **Related:** [Design §7](../system-design.md#7-time-model), [ADR-0002](0002-lazy-deterministic-popularity.md), [ADR-0008](0008-transactional-outbox.md)

## Context

Version 2 computed popularity lazily, but didn't explain how the things that depend on the clock would happen:

- a post goes live at the chosen time;
- comments appear gradually over several hours;
- "new comment" pushes are sent;
- the leaderboard needs every player's score at once.

The usual answer to all of these is a scheduler: a job that wakes up at a certain time and runs something. A scheduler is a component that can die, run late, or run twice, and it is hard to test without waiting for real time to pass.

## Decision

Effects are written ahead of time together with the time they take effect, and readers filter on that time.

1. **`effective_at` on every effect.** A PopularityEvent has `effective_at` and a comment has `visible_at`; queries filter on `<= now()`. Gradual release needs no process at all.
2. **A delayed outbox.** When a moment has to trigger an action in another service, the row is written to the outbox with `available_at`, and the relay only publishes rows that are due. For example, `post.published` with `available_at = published_at`. Other services use the same pattern in their own tables, for example `send_at` in channels.
3. **Periodic folds for global aggregates.** The leaderboard is folded every 5 minutes into a Postgres snapshot and a Valkey sorted set.

Every service gets the time from `Clock` (`pkg/clock`), never directly from the system clock.

## Alternatives considered

- **A central scheduler (cron or a dedicated service).** One more component that has to stay alive. If it dies, events are missed, and it is hard to make transactional with the data.
- **Delayed messages in RabbitMQ.** A good fit for short retries, but not for thousands of comments spread over hours. Queues also can't be queried, for example "which comments are visible at 21:00?".
- **A Valkey sorted set as a delay queue.** Fast, but Valkey is treated as a cache, so its contents can be lost.

## Consequences

- Gradual release can't be missed and can't happen twice, because there is nothing to "run".
- A whole season can be simulated in seconds with an accelerated `Clock`.
- A delayed row can be published after a row that was written later, so every event carries `occurred_at` or `effective_at`, and consumers never depend on write order.
- Indexes on the time columns are mandatory, and the relay needs to monitor its delay (`now - available_at`).
- Things that really need an active action (pushes, image batches, the hold sweeper) still have a trigger, but the trigger reads a table instead of keeping state of its own.
