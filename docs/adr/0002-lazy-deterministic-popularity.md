# ADR-0002: Popularity computed lazily and deterministically

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2, extended in version 3
- **Related:** [Design §8](../system-design.md#8-popularity-engine), [ADR-0012](0012-time-as-data.md)

## Context

Every player has followers and a popularity score that change with netizen reactions. These numbers decide which regions unlock and where the player ranks on the leaderboard, so they must be fair, explainable, and impossible to manipulate from the app.

The naive approach is a job that updates every player every minute with random numbers. Its load grows with the number of players (not with activity), its results can't be reproduced, and it is hard to test.

## Decision

- Popularity is computed from events, not stored as a number that keeps being updated. The value at time t is the snapshot plus every PopularityEvent with `effective_at <= t`.
- All randomness uses a seed from a hash of `post_id` and `netizen_id`, so the result is the same on any replica and after a restart.
- **Version 3:** inputs that change over time, such as the followers at publish time, are frozen in the `post.published` payload.
- **Version 3:** every event stores its `rules_version`, and the engine takes a `Clock` interface.

## Alternatives considered

- **A periodic job for every player.** Simple, but its cost grows with the number of players, including inactive ones.
- **Unseeded randomness.** A replay or retry produces different numbers, so a duplicate event could change the result.
- **Real-time simulation per netizen.** The most "alive" option, but expensive, and impossible to test in seconds.

## Consequences

- Testable with property tests: the same seed always gives the same result, and a whole season can be simulated in a few seconds.
- Auditable: "why did my followers drop yesterday?" is answered from the event log.
- Global aggregates such as the leaderboard still need a periodic fold ([ADR-0012](0012-time-as-data.md)).
- Changing the formula weights requires a new `rules_version` so that history doesn't change.
