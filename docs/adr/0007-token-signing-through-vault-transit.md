# ADR-0007: Token signing through Vault Transit

- **Status:** Accepted
- **Date:** 9 November 2026, decided in design version 2, updated in version 3
- **Related:** [Design §16](../system-design.md#16-security), [ADR-0015](0015-persistent-and-ephemeral-terraform.md)

## Context

The self-built auth server (Rust, from my thesis) issues JWTs for players and services. If the private signing key leaks, anyone can mint valid tokens. Keys also need to rotate without invalidating tokens that are still valid.

## Decision

- The auth server asks Vault Transit to sign every JWT, so the private key never leaves Vault.
- **Version 3:** ECDSA (ES256) keys are signed with `marshaling_algorithm=jws`. Vault returns a JWS signature in URL-safe base64, so the auth server only has to strip the `vault:v1:` prefix and make sure there is no `=` padding.
- The JWKS publishes the latest key version and the previous one, told apart by `kid`; keys rotate monthly.
- Verification never touches Vault: the gateway uses a cached JWKS.

## Alternatives considered

- **A key in a file or a Kubernetes Secret.** The fastest option, but the key could be read from a pod or a backup, and rotation is manual.
- **AWS KMS asymmetric signing.** A similar security model, but tied to AWS, and it can't run with the same behavior in Docker Compose.

## Consequences

- Every token issuance adds one call to Vault. The added p95 latency is measured in the "Vault Transit vs local signing" experiment.
- Token issuance depends on Vault, but token validation doesn't.
- The Transit key must survive when the cluster is shut down, so Vault's data and the KMS key for unsealing live in the persistent stack ([ADR-0015](0015-persistent-and-ephemeral-terraform.md)).
