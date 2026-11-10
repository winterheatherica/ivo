# ADR-0006: Aurora Serverless, not PostgreSQL inside the cluster

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2, strengthened in version 3
- **Related:** [Design §14](../system-design.md#14-data-storage), [Design §21](../system-design.md#21-infrastructure-and-deployment), [ADR-0015](0015-persistent-and-ephemeral-terraform.md)

## Context

game, billing, imaging, auth, and ops need a relational database with transactions for the ledger, the outbox, and `processed_events`. The k3s cluster runs on a single 8 GiB node that is often shut down to save credit. The AWS Free plan offers Aurora PostgreSQL Serverless through the express configuration, limited to 4 ACU and 1 GiB per cluster.

## Decision

- Aurora PostgreSQL Serverless through the express configuration in the Jakarta region, with a schema and database user per service and no cross-schema queries.
- Connections use IAM authentication; Go services create an IAM token for every connection.
- **Version 3:** Aurora lives in the persistent Terraform stack, so the primary data survives when the cluster is shut down.

## Alternatives considered

- **PostgreSQL inside k3s.** Free, but it eats the node's RAM, backups have to be handled by hand, and the data lives on the node's volume.
- **Provisioned RDS.** An always-on instance uses more credit than such a small load justifies.

## Consequences

- The 1 GiB limit makes retention rules mandatory ([design §14](../system-design.md#14-data-storage)), and the high-volume comments are stored in MongoDB ([ADR-0020](0020-mongodb-for-netizens.md)).
- Aurora only auto-pauses when there are no user connections and the minimum capacity is set to 0 ACU. Whether the express configuration supports this is checked in M5.
- Resuming after a pause takes about 15 seconds, or 30 seconds or more after a pause longer than 24 hours, so service connection timeouts are set to at least 30 seconds.
- After upgrading to the Paid plan, the ACU and storage limits are lifted without code changes.
