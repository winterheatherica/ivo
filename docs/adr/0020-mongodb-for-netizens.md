# ADR-0020: MongoDB for netizens and comments

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2, reasoning written in version 3
- **Related:** [Design §14](../system-design.md#14-data-storage), [ADR-0013](0013-deterministic-netizen-idempotency.md)

## Context

IVO already uses PostgreSQL, so the question "why MongoDB too?" is almost certain to come up. Honestly, MongoDB is in IVO because job postings often ask for it ([design §27](../system-design.md#27-cv-coverage-map)). This ADR makes sure it gets a job it is genuinely suited for.

Netizen data has a different character from the rest:

- Comments are the highest-volume data, about 1 GB per month at 1,000 DAU, while Aurora storage on the Free plan is limited to 1 GiB.
- Comments are written once and read per post. Their metadata changes as experiments evolve: model, prompt version, moderation score, text source.
- Netizen personas are nested documents: interests, active hours, speaking style, and true character.

## Decision

MongoDB stores netizen personas, comments, and per-post processing status, and only the netizen service accesses it. Comments are indexed by (`post_id`, `visible_at`) and unique per (`post_id`, `netizen_id`).

## Alternatives considered

- **PostgreSQL with JSONB.** One database fewer, and a uniform outbox pattern. But the comment volume would press against Aurora's 1 GiB Free plan limit right away.
- **DynamoDB.** Managed and with a free quota, but its query patterns per attribute are more limited, it is tied to AWS, and it doesn't run the same way locally.

## Consequences

- The heaviest write load never touches Aurora.
- There is no shared transaction with Postgres, so netizen uses deterministic idempotency instead of an outbox ([ADR-0013](0013-deterministic-netizen-idempotency.md)).
- MongoDB runs in the cluster with a capped cache (0.4–0.5 GB), and its data is on the persistent EBS volume.
- The SSPL license is safe as long as MongoDB isn't sold as a database service.
