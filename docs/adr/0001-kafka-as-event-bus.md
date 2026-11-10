# ADR-0001: Apache Kafka as the event bus

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2; revised on 10 November 2026, when Apache Kafka replaced Redpanda
- **Related:** [Design §13](../system-design.md#13-inter-service-communication), [ADR-0008](0008-transactional-outbox.md), [ADR-0010](0010-single-node-kafka.md)

## Context

One post drives six services: imaging, billing, game, netizen, channels, and analytics. Several consumers read the same events independently, each player's events must stay in order, and events need to be re-readable when a new consumer is added or a bug is fixed. Everything runs on one k3s node with 8 GiB of RAM.

Version 2 picked Redpanda because it is a single binary without a JVM. Since Apache Kafka 4.0, Kafka only runs in KRaft mode: one process can be both broker and controller, and ZooKeeper is gone. That closes most of the operational gap, while Kafka stays what most teams run in production and what job postings ask for.

## Decision

Apache Kafka in KRaft mode is the event bus. Locally and on the k3s node, a single process is both broker and controller, with a capped heap. Events are keyed by `player_id` (or `post_id`) so their order is preserved within one partition, with 7-day retention. Every service has its own consumer group, and services use franz-go, a pure Go Kafka client.

## Alternatives considered

- **Redpanda (version 2).** Compatible with the Kafka API, a single binary without a JVM, and lighter on RAM. But its BSL license is more restrictive than Kafka's Apache-2.0, and the portfolio is stronger when it runs the broker that teams actually use.
- **RabbitMQ alone.** A good fit as a work queue, but messages are gone once they are acked. There is no log to re-read, and every new consumer needs its own queue and binding.
- **Amazon SQS and SNS.** Managed and cheap, but there is no replay from a log. Per-key ordering is only available through FIFO queues, and locally they can only be emulated.

## Consequences

- One JVM process with its heap capped at 512–768 MB, about 1–1.5 GB of RAM on the node.
- Replay is just a consumer group offset reset; partitions are the unit of parallelism.
- The Apache-2.0 license puts no limits on how the broker is used.
- Automatic topic creation is off, because every topic has its own partition count and retention. An init job creates the topics instead.
- A single node without replication is a separate trade-off ([ADR-0010](0010-single-node-kafka.md)).
