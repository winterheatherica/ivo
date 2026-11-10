# State machines

Three entities have lifecycles that span several services: Post (owned by game), ImageJob (owned by imaging), and CreditHold (owned by billing). Their diagrams are in the design ([Post](../system-design.md#6-domain-model), [ImageJob](../system-design.md#9-photo-pipeline), [CreditHold](../system-design.md#18-billing-and-monetization)). This document details every transition: its trigger, its conditions, and its effects.

Rules for all three:

- Transitions are written only by the service that owns the entity, in the same transaction as the outbox row for the outgoing event.
- Every transition is idempotent. A trigger that arrives twice, or arrives when the entity is already in the target status, changes nothing.
- A trigger that arrives when the entity is already in a different final status is recorded as an anomaly (log and metric), then ignored.

## Post

The stored statuses are `draft`, `queued`, `review`, `ready`, `rejected`, and `failed`. "Published" is not stored: it is computed at read time as `status = 'ready'` and `published_at <= now()`.

`draft` is reserved for a save-a-plan feature. In v1, `CreatePost` creates the post directly with the status `queued`.

| From | To | Trigger | Condition | Effect |
|---|---|---|---|---|
| — | `queued` | `CreatePost` | Validation passes, `HoldCredit` succeeds, the daily limit isn't reached | Outbox `post.submitted` |
| `queued` | `review` | `image.review.requested` | — | — |
| `queued`, `review` | `ready` | `image.completed` with `variant = 0` | — | `published_at`, `frozen_inputs`, outbox `post.published` with `available_at = published_at` |
| `queued`, `review` | `rejected` | `image.failed` with reason `NSFW_REJECTED`, `REVIEW_REJECTED`, or `REVIEW_EXPIRED` | — | `failure_reason` |
| `queued` | `failed` | `image.failed` with reason `TECHNICAL` or `HOLD_FAILED`, or `credit.hold.failed` | — | `failure_reason` |

**Invariants:**

- `ready` always has `published_at`, `image_s3_key`, and `frozen_inputs`. A `CHECK` constraint in the database enforces this.
- `published_at` never changes once it is set, because `post.published` is already scheduled with that value.
- A regeneration (`variant >= 1`) doesn't change the Post status. Its result becomes an additional photo choice, and its flow is detailed in M2.

## ImageJob

| From | To | Trigger | Condition | Effect |
|---|---|---|---|---|
| — | `queued` | `post.submitted` | No job exists yet for (`post_id`, `variant`) | Outbox to RabbitMQ (`image.batch` or `image.express`) |
| `queued` | `dispatched` | The dispatcher takes the message | The status is still `queued` | `batch_id`, `dispatched_at`; the message is acked after the commit |
| `dispatched` | `completed` | Result `PASSED` | — | `output_s3_key`, `image_tags`; outbox `image.completed` and `usage.recorded` |
| `dispatched` | `review` | Result `REVIEW` | — | A `review_items` row; outbox `image.review.requested` and `usage.recorded` |
| `dispatched` | `rejected` | Result `REJECTED` after 3 attempts | — | Outbox `image.failed` (`NSFW_REJECTED`) and `usage.recorded` |
| `dispatched` | `queued` | Result `ERROR`, a batch timeout, or the watchdog | `retry_count < 3` | `retry_count + 1`; outbox to `image.retry.batch` or `image.retry.express` |
| `dispatched` | `failed` | Result `ERROR` | `retry_count = 3` | Outbox `image.failed` (`TECHNICAL`) |
| `queued` | `failed` | `credit.hold.failed` | — | Outbox `image.failed` (`HOLD_FAILED`) |
| `review` | `completed` | `ResolveReview` approves | `review_items.decision IS NULL` | Copy the object to `posts/`; outbox `image.completed` |
| `review` | `rejected` | `ResolveReview` rejects, or the sweeper after 24 hours | `review_items.decision IS NULL` | Delete the review object; outbox `image.failed` (`REVIEW_REJECTED` or `REVIEW_EXPIRED`) |

**Invariants:**

- `dispatched` always has a `batch_id`, and `completed` always has an `output_s3_key`. `CHECK` constraints enforce both.
- The seed of each attempt depends only on `job_id` and the attempt number, so a job that goes back to `queued` produces the same image.

## CreditHold

Every transition that changes the balance writes one ledger entry with `source_id = hold_id:generation`. The triple (`source_type`, `source_id`, `entry_type`) is unique, so the database rejects duplicate entries.

| From | To | Trigger | Condition | Ledger entry and effect |
|---|---|---|---|---|
| — | `pending` | `HoldCredit` | `available >= amount` | `hold`: available decreases, held increases |
| `pending` | `attached` | `post.submitted` | — | No entry; `attached_at` is set |
| `pending` | `voided` | `VoidHold`, or the sweeper after 30 minutes | — | `void`: held decreases, available increases |
| `pending`, `attached` | `captured` | `image.completed` | — | `capture`: held decreases |
| `pending`, `attached` | `voided` | `image.failed` | — | `void`: held decreases, available increases |
| `captured` | `captured` | A late `post.submitted` | — | No effect |
| `voided` | `attached` | A late `post.submitted` | `available >= amount` | `generation + 1`; a new `hold` entry for that generation |
| `voided` | `voided` | A late `post.submitted` | `available < amount` | No entry; outbox `credit.hold.failed` |

The transitions from `pending` straight to `captured` or `voided` are intentional. `post.submitted` and `image.completed` are on different topics, so their arrival order at billing isn't guaranteed. If billing's `post.submitted` consumer falls behind, the job result can arrive first.

**Invariants:**

- Within one generation, a hold is never both captured and voided.
- `available`, `held`, and `debt` are never negative. `CHECK` constraints on `billing.accounts` enforce this, so a bug in the code can't make a balance negative.
- For every account, `available + held` equals the total of incoming entries minus the total of outgoing entries. A nightly job checks the balance columns against the sum of the ledger entries.
