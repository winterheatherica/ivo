# ADR-0009: SFW and 18+

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2
- **Related:** [Design §5](../system-design.md#5-content-and-age-policy), [ADR-0005](0005-nsfw-moderation-cascade.md)

## Context

IVO shows virtual idols made by an image model, and that model can produce adult content. The Play Store and DOKU don't accept apps with sexually explicit content. The project also has to be safe to show to recruiters, and PP Tunas requires age verification that goes beyond a self-declaration checkbox.

## Decision

- IVO is an SFW app for users aged 18+.
- Every idol is an adult (in her 20s) and original, never a real idol or a character from an existing franchise.
- Netizens are labeled as AI accounts.
- Layered prevention and detection: a tag allowlist, a negative prompt, the NSFW cascade, text moderation, and player reports.

## Alternatives considered

- **Adult content.** Rejected by the Play Store and DOKU, and it can't be shown to anyone.
- **All ages.** Child-protection obligations become far heavier, and the content risk isn't worth it.

## Consequences

- An age verification method that satisfies PP Tunas has to be settled before the public release.
- Text moderation for captions and comments is still needed, and its classifier hasn't been chosen yet.
- The NSFW evaluation set comes from our own output, without any data involving children.
