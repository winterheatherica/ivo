# ADR-0004: Qwen on Cloudflare Workers AI with a fallback to Modal

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2, updated in version 3
- **Related:** [Design §10](../system-design.md#10-netizen-pipeline), [Design §15](../system-design.md#15-llm-layer), [ADR-0017](0017-text-only-comment-prompts.md)

## Context

Netizen comments are IVO's largest LLM workload. The model must write natural, casual comments in distinct persona voices, have an open license, and be cheap. Workers AI gives a free allowance of 10,000 Neurons per day, roughly 300–600 short comments. Modal is already used for the image GPUs and can run the same model through vLLM.

## Decision

- Regular comment model: Qwen 3.8 27B (Apache-2.0), on two routes that run the same model.
- Cloudflare Workers AI is used while our own Neurons counter (in Valkey) is under the limit and there is no HTTP 429 with code `3036`. Code `3040` (out of capacity) is retried with backoff.
- The rest is processed in one hourly run on Modal with vLLM, using 4-bit quantization on an L4.
- **Version 3:**
  - the Cloudflare allowance goes to the comments with the earliest `visible_at`;
  - the input is text only;
  - the Neurons counter is rebuilt from the cost ledger when Valkey restarts.

## Alternatives considered

- **A commercial API for every comment.** The easiest integration, but a higher cost per token at this volume, and dependence on a single provider.
- **Modal only.** Cloudflare's free allowance goes to waste, and the first comments have to wait for a batch.
- **Cloudflare only.** The allowance is too small, and on Workers Paid the overage is billed right away.

## Consequences

- One model on two routes keeps the persona voices consistent whichever route is used.
- According to the capacity estimate, at 1,000 DAU more than 99% of comments go through Modal. The Cloudflare allowance is practically only useful for making the first comments appear sooner, not for saving money.
- The Neurons counter must be accurate, because on Workers Paid an overage doesn't produce an error.
- The "Cloudflare vs Modal" and "Comment model size" experiments decide whether 27B is really needed.
