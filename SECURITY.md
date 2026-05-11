# Security Policy

## Reporting a Vulnerability

If you believe you have found a security vulnerability in Muninn, please report it privately rather than opening a public issue.

**Preferred:** use GitHub's [private vulnerability reporting](https://github.com/lgreene03/muninn/security/advisories/new) on this repository.

**Alternative:** email the maintainer listed in the repository's GitHub profile, with subject line `[muninn-security]`.

Please include:

- A description of the issue.
- Steps to reproduce.
- The affected version (git SHA or release tag).
- The impact you believe it has.
- Any suggested mitigation, if known.

You will receive an acknowledgement within 7 days. We aim to publish a fix or mitigation within 30 days of a confirmed report, depending on severity.

## Scope

Muninn is a local-first research infrastructure project. The realistic threat surface in the MVP is small but not zero. In-scope reports include:

- Vulnerabilities in the ingestion API or query API that allow unauthorized access, data corruption, or remote code execution.
- Vulnerabilities in event validation that allow malformed events to corrupt the event log or break deterministic replay.
- Vulnerabilities in dependencies that materially affect Muninn's runtime.
- Configuration defaults that expose sensitive surfaces unintentionally.

Out of scope:

- Findings that require an attacker who already has local shell access to the host running Muninn.
- Issues in third-party services Muninn integrates with (report those to the relevant project).
- Denial-of-service via unbounded event submission to a local development deployment — this is documented and not a security issue at the MVP scope.
- Speculative reports without reproduction steps.

## Supported Versions

Muninn is pre-1.0. Only `main` is currently supported. Once tagged releases exist, the latest minor version will receive security fixes.

## Coordinated Disclosure

We follow standard coordinated disclosure: a private fix lands first, then a public advisory is published with credit to the reporter (unless anonymity is requested).

## Hardening Notes (for Operators)

Muninn is **not** hardened for adversarial environments. If you deploy it outside `localhost`:

- Put authenticated reverse-proxy authentication in front of the query API and ingestion API.
- Never expose Redpanda, PostgreSQL, or MinIO ports to the public internet without authentication.
- Use TLS for any port exposed beyond the host.
- Store secrets in environment variables or a secret manager — never in committed configuration.

See also the upcoming `docs/steering/SECURITY_MODEL.md` for the broader threat model.
