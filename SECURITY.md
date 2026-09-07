# Security Policy

This is a portfolio/demo project: a dimensional engagement warehouse built
against **MiniStack** (a local AWS-compatible emulator), not a real AWS
account, and using only synthetic, seeded data (see `CLAUDE.md`). There is
no production deployment, no real customer data, and no SLA for support or
response times.

## Reporting a Vulnerability

If you find a security issue in this repository (e.g. a dependency with a
known CVE, or a credential accidentally committed), please open a GitHub
issue or contact the maintainer directly via the profile linked on
[santiago-ven-ai](https://github.com/santiago-ven-ai).

Since this project runs entirely against local infrastructure (MiniStack)
with fake credentials and synthetic data, there is no bug bounty and no
guaranteed response time — reports are handled on a best-effort basis.

## Scope

- In scope: this repository's own code and its pinned dependencies.
- Out of scope: MiniStack itself, and any third-party services referenced
  only as stubs (e.g. the Terraform Azure export under `terraform/azure/`).
