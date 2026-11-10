# ADR-0019: Account deletion through a saga and crypto-shredding

- **Status:** Accepted
- **Date:** 9 November 2026, design version 3
- **Related:** [Design §17](../system-design.md#17-privacy-data-export-and-account-deletion)

## Context

The Play Store requires apps that let users create an account to offer account deletion both in the app and on the web. One player's data in IVO is spread across seven places:

- game;
- imaging, including S3;
- netizen (MongoDB);
- billing;
- channels;
- analytics (BigQuery);
- auth.

No transaction can span all of them, and BigQuery isn't designed for frequent DELETEs.

## Decision

- game coordinates, because game owns the Player entity. The gateway revokes every token through auth, then calls `game.RequestDeletion`.
- game writes the DeletionRequest and the `player.deletion.requested` outbox row in one transaction, and deletes that player's delayed outbox rows at the same time.
- Every service deletes its own data, then publishes `player.deletion.completed`. Once every confirmation is in, game asks auth to delete the identity.
- Analytics uses `player_pid = HMAC(player_key, player_id)`. Once the key is deleted, none of that player's rows in BigQuery can be linked back to the person (crypto-shredding).
- The billing ledger is pseudonymized, not deleted, because it is a record of transactions.
- Data export uses the same saga pattern.

## Alternatives considered

- **One service deletes directly from every database.** Breaks data ownership, and any schema change in another service could break it.
- **DELETE in BigQuery.** Expensive and slow to run per player.
- **Soft delete only.** Doesn't satisfy the obligation to delete data.

## Consequences

- Deletion is eventually consistent and monitored: a request that is still incomplete after 24 hours triggers an alert.
- Data in Kafka disappears on its own within 7 days (retention), and MongoDB backups within 14 days (S3 lifecycle).
- Every new service that stores player data must consume `player.deletion.requested`.
