# ADR-0005: NSFW moderation as a cascade

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2
- **Related:** [Design §5](../system-design.md#5-content-and-age-policy), [Design §11](../system-design.md#11-nsfw-moderation), [ADR-0009](0009-sfw-and-18-plus.md)

## Context

Illustrious XL can produce adult content, while IVO is an SFW app. Prevention in the prompt (a tag allowlist and a negative prompt) reduces the risk but guarantees nothing. Rendered images must be checked before they are stored, with measurable precision and recall, without adding meaningful cost on top of rendering.

## Decision

- **Stage 1:** a small WD Tagger v3 variant (Apache-2.0). A high questionable or explicit rating is rejected, while a high sensitive rating goes to the review queue.
- **Stage 2:** an anime NSFW segmentation classifier, only for images that pass stage 1.
- Rejected images are never stored; only the scores, the model version, and the hash are recorded.
- New thresholds are tested in shadow mode first, and admin decisions become labeled data.

## Alternatives considered

- **A single classifier.** Simpler, but it forces a choice between recall and cost, with no second stage to catch what slips through.
- **Manual review of every image.** It can't keep up with the volume, and admins would have to look at all of the risky content.
- **An external moderation API.** Images leave the system, every call costs money, and its behavior can't be evaluated against our own set.

## Consequences

- Both stages finish within milliseconds on the same GPU as the render.
- Precision and recall per threshold, and the share of images that stop at stage 1, become CV numbers.
- The stage 2 classifier is based on Ultralytics (AGPL-3.0) with no clear license for its weights, so it must be replaced or licensed before a public release.
- Prevention at the player level (generation limits and cooldowns) is still needed, because the GPU cost is already spent by the time an image is rejected.
