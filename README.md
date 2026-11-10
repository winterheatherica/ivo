# Idol Venturing Out (IVO)

An influencer-manager simulation game for Android. The player manages the career of a virtual idol who starts in her home city and then travels to other provinces and countries. Every post is a photo of the idol at a real location, and 5,000 AI netizens react to it.

The core principles: **code owns the game state, AI models only fill in content**, and **time is data**.

## Status: M1 (local foundation)

The core flow already runs end to end locally: a post is created, enters the photo queue, goes live at the chosen time, and then gradually gets likes and comments from netizens.

| Part | M1 | Coming later |
|---|---|---|
| Post → photo → publish → reactions flow | Working | — |
| Photos | Placeholder renderer, queue in a Postgres table | M2: Modal L4, RabbitMQ, MinIO |
| Credits | Not held yet (`game.Free`) | M2: the billing service with holds |
| Comments | Templates from code, deterministic | M3: Qwen 3.8 27B and grok-4.3 |
| Authentication | `X-Player-Id` header (dev mode) | M3: the Rust auth server and JWTs |
| Leaderboard | Read from Postgres | Valkey sorted set |
| SSE and push | Not yet | M4 |

## Running locally

Prerequisites: Docker Desktop. The smoke test also needs `curl` and Python 3.

```bash
docker compose -f deploy/compose/compose.yaml up --build -d
```

```bash
bash scripts/smoke.sh
```

The smoke test creates one post, checks idempotency, waits for the post to go live, then waits for netizen comments. In the dev environment the render estimate is 1 minute and reaction delays run 100 times faster, so the whole flow finishes within a few minutes.

An example manual request:

```bash
curl -s -X POST localhost:8080/v1/posts -H "X-Player-Id: 6f1d9c1e-0000-4000-8000-000000000001" -H "Idempotency-Key: $(python -c 'import uuid; print(uuid.uuid4())')" -H "Content-Type: application/json" -d '{"destination_id":"id-jakarta-kota-tua","outfit_id":"casual-denim","pose_id":"standing-wave","expression_id":"smile","caption":"Afternoon at Kota Tua!"}'
```

The other endpoints are in [docs/detail/api-gateway.md](docs/detail/api-gateway.md).

## Development

```bash
go test ./...
```

```bash
npx @bufbuild/buf@1.75.0 generate
```

- The Go code generated from `proto/` is committed in `gen/`, so building doesn't need buf. The `protoc-gen-go` and `protoc-gen-go-grpc` plugins are recorded as `tool` dependencies in `go.mod`.
- The minimum Go version is 1.26; `go.mod` asks for toolchain 1.27.3, which Go downloads automatically.
- The module path is a temporary `ivo`. Change it to the GitHub repo path when the repo is published.
- On Linux or in CI, the `Makefile` has shortcuts for the commands above.

## Structure

```text
proto/           Protobuf contracts (buf)
gen/             generated Go code
pkg/             shared libraries: clock, ids, detrand, events, outbox, kafka, pgdb, grpcx
services/
  gateway/       REST to gRPC
  game/          posts, popularity, seasons, leaderboard
  imaging/       photo queue and batches
  netizen/       deterministic reactions, personas, comments
deploy/          Dockerfile and Docker Compose
scripts/         smoke test
docs/            system design, ADRs, and detailed design
```

## Documentation

- [System design, version 3](docs/system-design.md)
- [Architecture Decision Records](docs/adr/README.md)
- [Detailed design](docs/detail/README.md): flows, state machines, the API, and data schemas
- [Protobuf contracts](proto/README.md)
