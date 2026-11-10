# ADR-0010: Single-node Kafka without replication

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2, mitigation added in version 3; the broker became Apache Kafka on 10 November 2026
- **Related:** [Design §21](../system-design.md#21-infrastructure-and-deployment), [ADR-0001](0001-kafka-as-event-bus.md), [ADR-0008](0008-transactional-outbox.md)

## Context

Kafka replication needs at least three brokers on different machines, and a fault-tolerant KRaft quorum needs three controllers. IVO's AWS environment has only one 8 GiB node, and Kafka alone already uses 1–1.5 GB of it.

## Decision

One KRaft node that is both broker and controller, with a replication factor of 1. **Version 3:** its data is stored on the persistent EBS volume, so a restarted broker or a recreated node still finds its log and its cluster metadata.

## Alternatives considered

- **Three brokers on one node.** Three times the RAM with no real resilience, because they all die with the node.
- **Managed Kafka, for example Amazon MSK or Confluent Cloud.** Real resilience, but the cost is beyond the available credit.

## Consequences

- If the disk fails, events that haven't been consumed yet can be lost. The mitigation: published outbox rows are kept for 3 days, so the relay can publish them again, and idempotent consumers absorb the duplicates.
- This trade-off is written down on purpose as interview material: when replication is worth paying for, and what to do without it.
- After moving to several nodes, the replication factor goes up to 3 and the controllers move to a quorum of their own, without changing service code.
