# ADR-0017: Text-based comment prompts, without sending the image

- **Status:** Accepted
- **Date:** 9 November 2026, design version 3
- **Related:** [Design §10](../system-design.md#10-netizen-pipeline), [ADR-0004](0004-qwen-on-cloudflare-with-modal-fallback.md)

## Context

Version 2 said Qwen could see the image, so comments could mention what is in the photo. But sending the image to the LLM adds many input tokens to every request, limits the choice to multimodal models, and makes requests slower.

Yet IVO's code already knows exactly what is in the photo, because code assembled its prompt: destination, outfit, pose, expression, framing, and lighting. The photo pipeline also already computes WD Tagger tags from the final image for the NSFW filter.

## Decision

- Comment prompts contain text only: the post's structured context, the caption, the publish time, and the WD Tagger tags from the final image.
- The WD Tagger tags travel from `image.completed` to the Post, and then into the `post.published` payload.

## Alternatives considered

- **Sending the image to a multimodal model (version 2).** Comments could pick up details that aren't in the prompt, but token costs go up and the choice of models narrows.
- **An automatic caption from a vision model, once per post.** Richer than tags, but it adds one more model call for information that code mostly already knows.

## Consequences

- Input tokens per request go down, and smaller text models can be evaluated ([design §25](../system-design.md#25-experiments)).
- If a render drifts from its prompt, for example a different pose, a comment could mention something that isn't in the photo. WD Tagger tags from the final image reduce this risk.
- The "Comments without images" experiment measures whether eval scores drop, and it is the basis for reversing this decision if needed.
