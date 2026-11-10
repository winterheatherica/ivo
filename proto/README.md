# Protobuf contracts

Every gRPC RPC and event payload in IVO is defined here, so one schema change can be checked against every service at once ([design §13](../docs/system-design.md#13-inter-service-communication)).

| Package | File | Contents |
|---|---|---|
| `ivo.game.v1` | [`game_service.proto`](ivo/game/v1/game_service.proto) | `GameService`: posts, popularity, leaderboard, account deletion |
| | [`events.proto`](ivo/game/v1/events.proto) | `post.submitted`, `post.published` |
| `ivo.billing.v1` | [`billing_service.proto`](ivo/billing/v1/billing_service.proto) | `BillingService`: holds, balance, daily reward, cost summary |
| | [`events.proto`](ivo/billing/v1/events.proto) | `credit.hold.failed`, `payment.completed`, `entitlement.granted`, `entitlement.revoked` |
| `ivo.imaging.v1` | [`imaging_service.proto`](ivo/imaging/v1/imaging_service.proto) | `ImagingService`: reviews and job status |
| | [`events.proto`](ivo/imaging/v1/events.proto) | `image.completed`, `image.failed`, `image.review.requested` |
| | [`queue.proto`](ivo/imaging/v1/queue.proto) | The RabbitMQ message for photo jobs |
| | [`gpu.proto`](ivo/imaging/v1/gpu.proto) | The HTTP JSON contract between imaging and the GPU worker on Modal |
| `ivo.netizen.v1` | [`netizen_service.proto`](ivo/netizen/v1/netizen_service.proto) | `NetizenService`: comment lists |
| | [`events.proto`](ivo/netizen/v1/events.proto) | `engagement.recorded`, `netizen.comment.requested` |
| `ivo.usage.v1` | [`usage.proto`](ivo/usage/v1/usage.proto) | `usage.recorded` |
| `ivo.player.v1` | [`events.proto`](ivo/player/v1/events.proto) | Account deletion and data export |
| `ivo.ops.v1` | [`ops_service.proto`](ivo/ops/v1/ops_service.proto) | `OpsService`: DLQ and replay |

## Rules

- Lint uses buf's `STANDARD` rules, and `buf breaking` uses the `FILE` rules against the `main` branch.
- Every topic has exactly one message. The topic name, key, producer, and consumers are written in that message's comment.
- Event metadata (ID, time, trace) lives in the CloudEvents headers, not in the payload ([conventions](../docs/detail/README.md#events)).
- New fields are always added with new numbers. A number that has been used must never be changed or reused; to remove a field, reserve its number with `reserved`.
- Breaking changes use a new package version (`v2`) and a new topic, for example `post.submitted.v2`.
- IDs are sent as UUID strings. Times use `google.protobuf.Timestamp`.

## Commands

```bash
buf lint
```

```bash
buf breaking --against ".git#branch=main,subdir=proto"
```

```bash
buf generate
```

Run `buf generate` from the repository root; it reads `buf.gen.yaml` there and writes the Go code to `gen/`.
