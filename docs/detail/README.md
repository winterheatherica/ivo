# Detailed design

The documents in this folder take [system design version 3](../system-design.md) down to a level that can be implemented directly in M1 and M2. The contracts between services are in [`proto/`](../../proto/README.md).

| Document | Contents |
|---|---|
| [Flow 1: creating a post](flow-1-create-post.md) | From tapping "post" until the post is queued and the credit is held |
| [Flow 2: photo session](flow-2-photo-session.md) | The queue, the Modal batch, results, and review, until the post is ready to publish |
| [Flow 3: publishing and reactions](flow-3-publish-and-reactions.md) | The publish time, netizen reactions, comments, popularity, push, and SSE |
| [State machines](state-machine.md) | Post, ImageJob, and CreditHold transitions with their triggers and effects |
| [API gateway](api-gateway.md) | REST endpoints, the error format, idempotency, rate limits, and SSE |
| [Postgres schema](postgres-schema.md) | Tables per service schema, constraints, and important queries |
| [MongoDB schema](mongodb-schema.md) | The netizen service's collections with their validators and indexes |

Other flows (top-ups, rewarded ads, account deletion, and admin) are detailed when their milestones arrive.

## Shared conventions

### Naming

Everything is written in English, from prose to identifiers in code, Protobuf, and the database. Status names in the documents are the same values that the code stores.

### IDs

- New entities use application-generated UUIDv7, because they are time-ordered and friendly to B-tree indexes.
- Derived IDs that must be deterministic use UUIDv5 with fixed namespaces from `pkg/ids`.

| ID | Formula | Why deterministic |
|---|---|---|
| `post_id` | UUIDv5(`NS_POST`, `player_id:idempotency_key`) | A `CreatePost` retry reuses the same post and hold |
| `hold_id` | Same as `post_id`. For a regeneration: UUIDv5(`NS_REROLL`, `player_id:idempotency_key`) | `HoldCredit` is idempotent |
| `job_id` | UUIDv5(`NS_JOB`, `post_id:variant`) | A duplicate event never creates a duplicate job |
| `comment_id` | UUIDv5(`NS_COMMENT`, `post_id:netizen_id`) | A comment retry is an upsert |
| `event_id` from netizen | UUIDv5(`NS_EVENT`, `topic:post_id`), plus `:chunk_no` for comment chunks | A replay produces the same events |
| `netizen_id` | UUIDv5(`NS_NETIZEN`, `persona:<number>`) | The persona seed is the same in every environment, so reactions can be compared |

```text
NS_POST    = de398215-92f3-4a8e-98f3-3e2cd9ed7e6d
NS_REROLL  = 7ad35208-2b23-424f-831c-a6bba9d19ae5
NS_JOB     = 78eaaf92-44ad-4dcc-bb56-0ea6f30697f1
NS_COMMENT = 0bab841a-1ff3-4e6b-84a1-08b296d2eb66
NS_EVENT   = f75efc61-a535-427c-abc9-9af6d53081e0
NS_NETIZEN = 4d2fde18-7d89-4bb8-8a3a-bd8786574879
```

The namespaces must never change once there is data, because old IDs could no longer be recomputed.

### Time

- Every time column is `timestamptz` and stored in UTC.
- Daily limits (the daily reward, the post limit) use `date_wib`, the date in the `Asia/Jakarta` time zone.
- Time always comes from `Clock` (`pkg/clock`), never from `time.Now()` or `now()` in SQL. That is why time columns have no `DEFAULT now()`.
- Engagement buckets are 15 minutes long, aligned to minutes 00, 15, 30, and 45 UTC. A bucket's numbers apply at its `effective_at`, the time of the last reaction in that bucket, so they are never counted before the reactions happen.

### Numbers

- Credits are stored as `bigint` in whole credits.
- Costs are stored as `bigint` in micro-USD (1 USD = 1,000,000), so sums are exact.
- Multipliers and probabilities in event payloads are integer basis points (0–10,000).

### Deterministic randomness

Every draw uses the same function:

```text
u(material, k) = uint64_big_endian(SHA-256(material + ":" + k)[0:8]) >> 11, divided by 2^53
```

The result is a number in [0, 1). The material for netizen reactions is `post_id:netizen_id:rules_version`, and `k` tells the draws apart: `k = 0` for the reaction type, `k = 1` for the delay, `k = 2` for the true-character voice.

- **No `math/rand`.** The function above is easy to write identically in Go and Python (for evals), and its results don't depend on the Go version.
- **Floats stay deterministic.** Deployment is amd64 only. Float expressions that decide outcomes are wrapped in explicit `float64(...)` conversions, because the Go specification allows the compiler to fuse a multiply and an add into an FMA, whose result can differ slightly.
- **Image seed** = `uint64_big_endian(SHA-256(job_id + ":" + attempt)[0:8]) & 0x7FFF_FFFF_FFFF_FFFF`. The GPU worker computes the seed, so it doesn't need to be stored.

### Events

One topic per event type, binary Protobuf payloads, and keys as listed in the [topic table](../system-design.md#13-inter-service-communication). Headers follow CloudEvents binary mode for Kafka:

| Header | Contents |
|---|---|
| `ce_specversion` | `1.0` |
| `ce_id` | `event_id` (UUID) |
| `ce_type` | `ivo.<topic>.v1`, for example `ivo.post.submitted.v1` |
| `ce_source` | `ivo/<service>`, for example `ivo/game` |
| `ce_time` | `occurred_at` in RFC 3339 |
| `content-type` | `application/protobuf` |
| `traceparent` | W3C trace context |
| `replay_of` | The DLQ message ID, only on a replay |

| Topic | Partitions | Retention |
|---|---|---|
| `netizen.comment.requested` | 12, more than KEDA's `maxReplicaCount` (8) | 7 days |
| `*.dlq` | 1 | 14 days |
| Other topics | 3 | 7 days |

The replication factor is always 1, because there is only one broker ([ADR-0010](../adr/0010-single-node-kafka.md)).

### Consumers

- One consumer group per service (`game`, `billing`, `imaging`, and so on). The netizen comment workers have their own group, `netizen-comment`.
- The `processed_events.consumer` column holds `<service>.<topic>`, for example `imaging.post.submitted`.
- Retries happen inside the consumer with delays of 1, 5, and 30 seconds. After that the message goes to `<topic>.dlq` with its original headers plus `x-error` and `x-consumer`, and then the offset is committed. One partition is held up for at most about 40 seconds.
- A replay from ops republishes the message with the same `ce_id`. Other consumers that already succeeded skip it through `processed_events`.

### gRPC

- Authentication uses a client-credentials JWT from auth in the `authorization` metadata, with the target service's name as the audience.
- Other metadata: `traceparent` and `x-request-id`.
- The default deadline is 2 seconds. `CreatePost` gets 5 seconds because it also calls billing.
- The player's identity is sent as the `player_id` field of the request, filled in by the gateway from the `sub` claim of the player's token. Services never accept `player_id` directly from the app.

### S3 bucket

One private bucket for every object; outside access only goes through presigned URLs.

| Prefix | Owner | Contents | Lifecycle |
|---|---|---|---|
| `assets/destinations/` | game | Curated Mapillary photos | Permanent |
| `posts/{post_id}/` | imaging | Final photos in WebP | Permanent until the account is deleted |
| `review/` | imaging | Images waiting for review, encrypted with SSE-KMS | 7 days |
| `exports/{request_id}/` | Every service | Parts of a player's data export | 2 days |
| `backups/` | Infrastructure scripts | Vault Raft snapshots and MongoDB dumps | 14 days |
