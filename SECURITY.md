# Security policy

## Reporting a vulnerability

If you believe you've found a security issue in Raincloud, please report it
privately rather than opening a public issue.

Preferred channel: **GitHub's private vulnerability reporting** —
[open an advisory](https://github.com/spiraldb/raincloud/security/advisories/new)
from the repo's Security tab. Reports submitted that way are visible only to
the maintainers.

Fallback: email **raincloud@spiraldb.com** with subject prefix `[security]`.

Please include:

- A description of the issue and its impact.
- Steps to reproduce, or a minimal proof-of-concept.
- The commit SHA or release version where you observed the issue.

## What to expect

- We aim to acknowledge reports within **14 days**.
- Medium-or-higher-severity issues, once confirmed, are patched within
  **60 days** of acknowledgement; critical issues are patched as quickly as we
  can.
- Once a fix ships, we'll publish an advisory crediting the reporter (unless
  you'd rather stay anonymous).

## Scope

The pipeline code (everything under `scripts/`, plus `sources.json`,
`pyproject.toml`, and the schema files) is in scope. The datasets themselves
are upstream third-party data — vulnerabilities in the data should be reported
to the upstream source listed under each dataset's `license.source_url`.
