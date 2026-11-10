# ADR-0016: The admin web app goes through the gateway instead of consuming events

- **Status:** Accepted
- **Date:** 9 November 2026, design version 3
- **Related:** [Design §12](../system-design.md#12-system-architecture), [Design §13](../system-design.md#13-inter-service-communication)

## Context

Version 2 listed the admin web app (Next.js on Cloudflare Workers) as a consumer of `image.review.requested` and of every `*.dlq` topic. Workers run per request with an execution time limit, so they can't stay alive as members of a Kafka consumer group. On top of that, the review queue and DLQ messages are data, and every piece of data in IVO must have exactly one owning service.

## Decision

- The review queue belongs to imaging. Its contents are stored in the `review_items` table and exposed through the gRPC methods `ListReviews` and `ResolveReview`. The `image.review.requested` topic stays for notifications (channels), post status (game), and analytics.
- DLQ messages are consumed by a new worker, `ops`, which stores them in a table and provides `ListDeadLetters` and `Replay`. A replay republishes the message to its source topic with a `replay_of` header.
- The admin web app only talks to the gateway, through `/admin/*` routes with RBAC (`reviewer`, `admin`). The gateway forwards requests to the owning service over gRPC.

## Alternatives considered

- **The admin web app reads directly through a Kafka REST proxy or a Kafka web UI.** Broker access is exposed to the internet, and there is still nowhere to store review decisions.
- **DLQs stored in each service's own table.** Clean ownership, but every service would need its own admin endpoints for the same job.
- **A separate admin backend.** One more service for work that the gateway and the owning services can already handle.

## Consequences

- There is one new worker (`ops`) with a small RAM footprint.
- Every admin action goes through the gateway with the admin's identity, so it can be audited.
- The admin web app can stay on Cloudflare Workers because it only needs HTTP.
