# API gateway

The gateway is the only HTTP entry point for the Android app and the admin web app. It stores no domain data: every endpoint is forwarded to the owning service over gRPC. Login itself is handled by the auth server (OAuth 2.0 with PKCE) on a separate host.

| Host | Contents |
|---|---|
| `api.<domain>` | gateway: `/v1/*` for players and `/admin/*` for admins |
| `auth.<domain>` | auth server: `/authorize`, `/token`, `/.well-known/jwks.json` |

## Player endpoints

Every endpoint needs a player access token (`Authorization: Bearer`).

| Method | Path | Forwarded to | Notes |
|---|---|---|---|
| `POST` | `/v1/posts` | `game.CreatePost` | Requires an `Idempotency-Key`; see [flow 1](flow-1-create-post.md) |
| `GET` | `/v1/posts` | `game.ListPosts` | Query `include_unpublished`, `page_size`, `page_token` |
| `GET` | `/v1/posts/{post_id}` | `game.GetPost` | — |
| `GET` | `/v1/posts/{post_id}/comments` | `game.GetPost`, then `netizen.ListComments` | Ownership is checked first through `GetPost` |
| `GET` | `/v1/posts/{post_id}/stream` | SSE | See the SSE section below |
| `GET` | `/v1/me/balance` | `billing.GetBalance` | — |
| `POST` | `/v1/me/daily-reward` | `billing.ClaimDailyReward` | Idempotent per WIB date without any extra header |
| `GET` | `/v1/me/popularity` | `game.GetPopularity` | — |
| `GET` | `/v1/leaderboard` | `game.GetLeaderboard` | Query `season_id`; empty means the active season |
| `POST` | `/v1/me/export` | `game.RequestExport` | Requires an `Idempotency-Key` |
| `DELETE` | `/v1/me` | auth (revoke tokens), then `game.RequestDeletion` | Also available through a web page, as Play Store policy requires |

## Admin endpoints

The admin web app calls these endpoints with a token from the auth server that carries the `reviewer` or `admin` role.

| Method | Path | Forwarded to | Role |
|---|---|---|---|
| `GET` | `/admin/reviews` | `imaging.ListReviews` | `reviewer` |
| `POST` | `/admin/reviews/{job_id}/approve` | `imaging.ResolveReview` | `reviewer` |
| `POST` | `/admin/reviews/{job_id}/reject` | `imaging.ResolveReview` | `reviewer` |
| `GET` | `/admin/dead-letters` | `ops.ListDeadLetters` | `admin` |
| `POST` | `/admin/dead-letters/{id}/replay` | `ops.Replay` | `admin` |

Every admin action is recorded in the gateway's audit log, together with the admin's identity and the `x-request-id`.

## JSON format

- The gateway uses protojson with `UseProtoNames`, so field names are `snake_case`, as in Protobuf.
- Times are RFC 3339 in UTC.
- Enums are mapped to short lowercase strings (`queued`, `ready`), not Protobuf enum names (`POST_STATUS_QUEUED`).
- Pagination uses an opaque `page_token`; an empty `next_page_token` means the last page.

## Errors

Every error uses the Problem Details format (RFC 9457) with `Content-Type: application/problem+json`:

```json
{
  "type": "/errors/insufficient-credit",
  "title": "Insufficient credit",
  "status": 409,
  "code": "INSUFFICIENT_CREDIT",
  "detail": "Needs 10 credits, but only 4 are available.",
  "request_id": "0193a1f3-0d2e-7c11-a7b4-6e2f9c0d1a2b"
}
```

`code` comes from `ErrorInfo.reason` in the gRPC status; the app decides what to show from `code`, not from `title`.

| gRPC status | HTTP | Example `code` |
|---|---|---|
| `INVALID_ARGUMENT` | 400 | `INVALID_CHOICE`, `CAPTION_REJECTED`, `PUBLISH_TIME_INVALID` |
| `INVALID_ARGUMENT` with the reason `IDEMPOTENCY_KEY_REUSED` | 422 | `IDEMPOTENCY_KEY_REUSED` |
| `UNAUTHENTICATED` | 401 | `TOKEN_INVALID` |
| `PERMISSION_DENIED` | 403 | `FORBIDDEN` |
| `NOT_FOUND` | 404 | `NOT_FOUND`, also for posts that belong to another player |
| `FAILED_PRECONDITION` | 409 | `INSUFFICIENT_CREDIT`, `DAILY_LIMIT`, `DESTINATION_LOCKED` |
| `RESOURCE_EXHAUSTED` | 429 | `RATE_LIMITED`, with a `Retry-After` header |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED` | 503 | `UNAVAILABLE`; the app may retry with the same Idempotency-Key |

## Idempotency

- `Idempotency-Key` holds a UUID that the app creates once per player action and reuses on retries.
- For `POST /v1/posts`, the key becomes part of `post_id` (UUIDv5). So idempotency is guaranteed by the database, not by a cache.
- For other endpoints that must be idempotent, the gateway stores the response in Valkey for 24 hours, under the key `idem:{player_id}:{key}`.

## Rate limits

Rate limits use GCRA in Valkey (a Lua script), so they apply the same way across every gateway replica.

| Scope | Limit |
|---|---|
| Per player, every endpoint | 60 requests per minute |
| Per player, `POST /v1/posts` | 20 requests per hour (game still enforces the daily limit) |
| Per IP | 300 requests per minute |

## SSE

`GET /v1/posts/{post_id}/stream` sends these events:

| Event | Data | When |
|---|---|---|
| `comment` | One comment, with `id` = `comment_id` | When its `visible_at` arrives, or when a late text is finished |
| `engagement` | `{"likes": 120, "comments": 9}` | When the numbers change, checked every 60 seconds |
| `status` | `{"status": "ready", "published": true}` | When the post's status changes |

- The gateway sends a `: ping` comment every 25 seconds so proxies don't drop the connection.
- When reconnecting, the app sends `Last-Event-ID`, and the gateway sends the missed comments.
- How the gateway schedules events is explained in [flow 3](flow-3-publish-and-reactions.md#sse-for-the-post-screen).
