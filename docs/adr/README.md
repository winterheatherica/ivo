# Architecture Decision Records

Every important architecture decision in IVO is written up as a short ADR: context, decision, alternatives considered, and consequences. The full design is in [system-design.md](../system-design.md).

From now on, accepted ADRs are not edited. If a decision changes, write a new ADR that supersedes it, and change the old ADR's status to "Superseded by ADR-NNNN".

| ADR | Decision | Origin |
|---|---|---|
| [0001](0001-kafka-as-event-bus.md) | Apache Kafka as the event bus | v2, broker changed in v3 |
| [0002](0002-lazy-deterministic-popularity.md) | Popularity computed lazily and deterministically | v2, extended in v3 |
| [0003](0003-gpu-on-modal-with-hourly-batches.md) | GPUs on Modal with hourly batches | v2 |
| [0004](0004-qwen-on-cloudflare-with-modal-fallback.md) | Qwen on Cloudflare Workers AI with a fallback to Modal | v2, updated in v3 |
| [0005](0005-nsfw-moderation-cascade.md) | NSFW moderation as a cascade | v2 |
| [0006](0006-aurora-serverless.md) | Aurora Serverless, not PostgreSQL inside the cluster | v2, strengthened in v3 |
| [0007](0007-token-signing-through-vault-transit.md) | Token signing through Vault Transit | v2, updated in v3 |
| [0008](0008-transactional-outbox.md) | Transactional outbox | v2, extended in v3 |
| [0009](0009-sfw-and-18-plus.md) | SFW and 18+ | v2 |
| [0010](0010-single-node-kafka.md) | Single-node Kafka without replication | v2, mitigated in v3 |
| [0011](0011-credit-holds-authorize-capture-void.md) | Credit holds with authorize, capture, void | v3 |
| [0012](0012-time-as-data.md) | Time as data | v3 |
| [0013](0013-deterministic-netizen-idempotency.md) | Netizen idempotency through deterministic IDs, not an outbox | v3 |
| [0014](0014-image-jobs-in-postgres.md) | `image_jobs` in Postgres, RabbitMQ only for delivery | v3 |
| [0015](0015-persistent-and-ephemeral-terraform.md) | Terraform split into persistent and ephemeral stacks | v3 |
| [0016](0016-admin-web-through-gateway.md) | The admin web app goes through the gateway instead of consuming events | v3 |
| [0017](0017-text-only-comment-prompts.md) | Text-based comment prompts, without sending the image | v3 |
| [0018](0018-span-links-at-async-boundaries.md) | Span links at async boundaries | v3 |
| [0019](0019-account-deletion-saga-and-crypto-shredding.md) | Account deletion through a saga and crypto-shredding | v3 |
| [0020](0020-mongodb-for-netizens.md) | MongoDB for netizens and comments | v2, reasoning written in v3 |

## Next ADR candidates

These decisions are already made in the design, but their reasoning hasn't been written up yet:

- BigQuery for analytics, not Athena or Redshift on AWS.
- KEDA for autoscaling the comment workers.
- Valkey, not Redis.
- Next.js on Cloudflare Workers for the admin web app.

## Template

```markdown
# ADR-NNNN: Decision title

- **Status:** Proposed | Accepted | Superseded by ADR-NNNN
- **Date:**
- **Related:**

## Context

What problem we face, and which constraints apply.

## Decision

What we chose, in enough detail to implement it.

## Alternatives considered

Other options, and why they weren't chosen.

## Consequences

What becomes easier, what becomes harder, and which risks we accept.
```
