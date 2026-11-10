# ADR-0013: Netizen idempotency through deterministic IDs, not an outbox

- **Status:** Accepted
- **Date:** 9 November 2026, design version 3
- **Related:** [Design §10](../system-design.md#10-netizen-pipeline), [ADR-0008](0008-transactional-outbox.md), [ADR-0020](0020-mongodb-for-netizens.md)

## Context

Version 2 required every consumer to record `processed_events` in the same transaction as its effects, and put the outbox and `processed_events` in Aurora. But the netizen service's effects (comments and processing status) are stored in MongoDB. Postgres and MongoDB share no transaction, so that guarantee couldn't hold for netizen.

## Decision

Build on the determinism netizen already has:

- Reactions are drawn with a seed from `post_id` and `netizen_id`, and every input number is frozen in the `post.published` payload. Processing the same event twice always gives the same decisions.
- The `event_id` of `engagement.recorded` and of the `netizen.comment.requested` messages is derived from `post_id`, the event type, and the chunk number. Redelivered events are discarded by each consumer through its own `processed_events`.
- Comments are unique per (`post_id`, `netizen_id`). Retries are upserts, and the LLM is not called for netizens whose comment already exists.
- The order is produce first, then commit the offset (at-least-once).

## Alternatives considered

- **An outbox in MongoDB.** Needs MongoDB to run as a replica set so multi-document transactions are available, plus a second relay with a different pattern from the other services.
- **Moving comments to Postgres.** The outbox pattern would be uniform, but comments are the highest-volume data, and Aurora storage is limited to 1 GiB.

## Consequences

- netizen has no outbox relay, and replaying `post.published` is safe.
- Idempotency depends on determinism. If an unseeded random decision ever slips in, the guarantee breaks, so a "replay gives identical results" property test is required in CI.
- The LLM text itself isn't deterministic, but the upsert keeps the first version, so only one version is ever stored.
- Interviews get two patterns to compare: the transactional outbox in the Postgres services, and deterministic replay in netizen.
