# ADR-0018: Span links at async boundaries

- **Status:** Accepted
- **Date:** 9 November 2026, design version 3
- **Related:** [Design §20](../system-design.md#20-observability)

## Context

Version 2 wanted one tap on "post" to be traceable to the first comment in a single trace. But that journey can take 1–2 hours and pass through a batch. One Modal batch span processes up to 20 jobs from 20 different traces, while a span can only have one parent. Traces that live for hours also make tail sampling and the Tempo view harder.

## Decision

- Traces are cut at async boundaries that are long or that combine many messages.
- The Modal batch span gets one span link to each job inside it, following the OpenTelemetry semantic conventions for messaging.
- Events processed long after they were created, such as `post.published` at the publish time, start a new trace linked to the trace that created them.
- Every span carries a `post_id` attribute, so a post's whole journey can be found with the TraceQL query `{ span.post_id = "..." }`.

## Alternatives considered

- **One long trace (version 2).** Impossible for a batch with many parents, and a trace that stays "unfinished" for hours breaks sampling.
- **Correlation through logs only.** Durations and the cause-and-effect links between spans are lost.

## Consequences

- Every trace is short and complete, and long journeys are reconstructed through links and `post_id`.
- Batch wait time is still recorded as a queue metric, not as a span.
- M6's done criterion is rewritten: what is required is that one post's journey can be traced, not that it is "one trace".
