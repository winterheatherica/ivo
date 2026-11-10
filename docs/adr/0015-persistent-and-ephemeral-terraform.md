# ADR-0015: Terraform split into persistent and ephemeral stacks

- **Status:** Accepted
- **Date:** 9 November 2026, design version 3
- **Related:** [Design §21](../system-design.md#21-infrastructure-and-deployment), [ADR-0006](0006-aurora-serverless.md), [ADR-0007](0007-token-signing-through-vault-transit.md)

## Context

The AWS credit only lasts about 6 weeks if the environment is always on, so version 2 shut it down with `terraform destroy` when it wasn't in use. The problem is that Vault, MongoDB, and Kafka keep their data on the EC2 volume. Every destroy wiped out:

- the Transit key that signs JWTs, invalidating every old token;
- the secrets in Vault KV;
- every netizen persona and comment;
- events that hadn't been consumed yet.

If the KMS key for auto-unseal were deleted too, even Vault's backups could never be opened again.

## Decision

Two Terraform root modules with separate state:

| Stack | Contents | When it is destroyed |
|---|---|---|
| `persistent` | State bucket, ECR, KMS, S3 for photos and backups, IAM and GitHub OIDC, Budgets, Aurora, the cluster's EBS data volume | Never, for as long as the project runs |
| `ephemeral` | VPC, public subnets, the k3s EC2 instance, the public IPv4 address, security groups, the EBS volume attachment | Whenever it isn't in use |

- A gp3 EBS volume in the same AZ as the node is mounted as k3s storage, so Vault (Raft), MongoDB, and Kafka survive when the EC2 instance is destroyed.
- `make down` takes a Vault Raft snapshot and a `mongodump` to S3, waits for the queues to drain, then destroys the ephemeral stack. `make up` does the reverse, and restores only if the volume is new.
- The ephemeral stack reads the persistent stack's outputs (volume ID, AZ, KMS ARN) through remote state.

## Alternatives considered

- **One stack, with a full restore from backup every time it comes up.** Possible, but every restore is slow and failure-prone, and the backups still need a persistent home.
- **Stopping the instance instead of destroying it.** The volume stays, but the IPv4 address, the root volume, and other resources keep being billed. The ability to build the environment from scratch with one command would also never be tested.
- **Moving all state to managed services.** The tidiest option, but it adds cost (for example managed MongoDB) and removes the Kubernetes part this project wants to show.

## Consequences

- Data and cryptographic identities survive, while compute costs only appear while the ephemeral stack is up.
- The persistent stack has a small fixed cost (about $2 per month for EBS, plus S3 and KMS), plus Aurora if it can't auto-pause.
- The EBS volume is tied to one AZ, so the node must be created in that AZ.
- M5's done criterion becomes stricter: the environment can be brought up and down with one command without losing data.
