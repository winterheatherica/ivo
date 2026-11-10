# ADR-0003: GPUs on Modal with hourly batches

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2
- **Related:** [Design §9](../system-design.md#9-photo-pipeline), [ADR-0014](0014-image-jobs-in-postgres.md)

## Context

Every post needs an Illustrious XL render, background removal, compositing, and two NSFW filter stages, all on a GPU. The volume is small and arrives unevenly.

The AWS Free plan only allows a few instance types (`t3.micro`, `t3.small`, `t4g.micro`, `t4g.small`, `c7i-flex.large`, `m7i-flex.large`), none of them with a GPU. An always-on GPU also burns credit while it sits idle. On the other hand, the game can tolerate waiting: the photo session is shown as "photo shoot in progress".

## Decision

- The GPU worker runs on Modal L4, scales to zero, and is billed per second, with $30 of free credit per month.
- Jobs are collected and sent every hour, or when the queue reaches 20 jobs. The express lane (paid with credits) is sent immediately.
- The steps from rendering to the NSFW filter run in one container per batch, so the model is loaded only once.
- The `prod` environment is for production, and the restricted `personal` environment is for personal experiments.

## Alternatives considered

- **EC2 GPUs.** Not available on the Free plan, and an always-on instance is far more expensive for such a small volume.
- **Serverless without batching.** Every image could trigger its own cold start and model load.
- **A third-party image generation API per image.** Easier, but BiRefNet, compositing, and the NSFW filter couldn't be combined in one container that we control.

## Consequences

- A p95 wait time of up to about 70 minutes. This is accepted as a game mechanic and recorded as a queue metric.
- Cost per image, the cold start ratio, and the best batch size become experiments.
- imaging needs a circuit breaker and delayed retries for when Modal has problems.
- Dependence on a single GPU vendor. The worker is written as plain Python functions so it can be moved.
