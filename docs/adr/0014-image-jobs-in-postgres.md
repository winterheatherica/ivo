# ADR-0014: `image_jobs` in Postgres, RabbitMQ only for delivery

- **Status:** Accepted
- **Date:** 9 November 2026, design version 3
- **Related:** [Design §9](../system-design.md#9-photo-pipeline), [ADR-0003](0003-gpu-on-modal-with-hourly-batches.md), [ADR-0008](0008-transactional-outbox.md)

## Context

In version 2, imaging consumed `post.submitted` from the event bus and then put the job on RabbitMQ. That had two problems:

1. Publishing to RabbitMQ and committing the Kafka offset are not atomic. If imaging crashes between the two, the job is queued twice and the GPU is paid for twice.
2. The dispatcher could hold messages unacked while waiting for the hourly batch or while the GPU works. RabbitMQ then applies its delivery acknowledgement timeout (30 minutes by default): the consumer is cancelled and the message is redelivered, so the job could be rendered again.

There is also a question that is almost certain to come up in interviews: with Kafka and Postgres already in place, why RabbitMQ too? Honestly, RabbitMQ is in IVO because job postings often ask for it ([design §27](../system-design.md#27-cv-coverage-map)). This ADR makes sure its role is a genuine fit, not an add-on.

## Decision

- The `image_jobs` table in Postgres becomes the source of truth for job status: `queued`, `dispatched`, `completed`, `review`, `rejected`, `failed`. Its key is unique per `post_id` and regeneration number.
- imaging writes the job, `processed_events`, and the outbox row in one transaction. The outbox relay sends `job_id` to RabbitMQ with publisher confirms.
- RabbitMQ is used for its strengths as a work queue:
  - two lanes, `image.batch` and `image.express`;
  - an ack per job;
  - delayed retries through a TTL and a dead-letter exchange;
  - a higher priority for retried jobs.
- The dispatcher checks the queue depth without taking messages. When a batch leaves, it takes up to 20 messages, marks the jobs `dispatched` with a `batch_id`, acks the messages, and only then calls Modal. Jobs whose status isn't `queued` are skipped.

## Alternatives considered

- **A queue in Postgres only (`SELECT ... FOR UPDATE SKIP LOCKED`).** The simplest option and enough for this volume, but delayed retries and priorities would have to be written by hand.
- **A Kafka topic per lane.** A partitioned log has no per-message ack. One slow or failing job blocks its partition (head-of-line blocking), and delayed retries need an extra topic per delay.
- **RabbitMQ as the source of truth (version 2).** Job status can't be queried, and the atomicity and timeout problems remain.

## Consequences

- Duplicate messages never trigger duplicate renders, and the ack timeout is never reached.
- A watchdog moves `dispatched` jobs that exceed the batch time limit back to `queued`. Because seed = hash(`job_id`, `attempt`), the re-render produces the same image.
- imaging's outbox relay has two destinations, Kafka for events and RabbitMQ for jobs, so `pkg/outbox` has to support swappable publishers.
- The interview answer to "why RabbitMQ" becomes concrete: per-job acks and delayed retries, which are awkward in Kafka, while Postgres still holds the truth.
