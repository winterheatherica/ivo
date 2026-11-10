# ADR-0008: Transactional outbox

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2, extended in version 3
- **Related:** [Design §13](../system-design.md#13-inter-service-communication), [ADR-0012](0012-time-as-data.md), [ADR-0013](0013-deterministic-netizen-idempotency.md), [ADR-0014](0014-image-jobs-in-postgres.md)

## Context

When game creates a post, it has to store the data and send `post.submitted`. If the two are done separately (a dual write), a crash in between loses the event, so the post is never processed. Or the reverse happens: the event goes out without the data, and a ghost post appears.

## Decision

- Data changes and the outbox row are written in one Postgres transaction.
- The relay picks unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes them, and marks them published. Delivery is at-least-once.
- Consumers are idempotent: the `event_id` is recorded in `processed_events` in the same transaction as its effects.
- **Version 3:**
  - the `available_at` column makes the outbox double as a scheduler ([ADR-0012](0012-time-as-data.md));
  - imaging's relay also sends jobs to RabbitMQ ([ADR-0014](0014-image-jobs-in-postgres.md));
  - published rows are kept for 3 days as a replay backup ([ADR-0010](0010-single-node-kafka.md)).

## Alternatives considered

- **Publish after commit.** The event is lost if the process dies between the commit and the publish.
- **CDC with Debezium.** No outbox table needed, but it adds Kafka Connect, another JVM process, plus logical replication on an 8 GiB node. Logical replication also prevents Aurora from auto-pausing.
- **Kafka transactions.** They only cover Kafka, not the database.

## Consequences

- Every Postgres-backed service gets an extra table and a relay process, and events are delayed by at most one polling interval.
- Duplicates can happen, so every consumer must be idempotent.
- netizen doesn't use this pattern because its data is in MongoDB ([ADR-0013](0013-deterministic-netizen-idempotency.md)).
