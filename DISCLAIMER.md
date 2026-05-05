# Disclaimers and reporting

This document covers Raincloud's posture on third-party datasets, license
diligence, content disclaimers, and how to report a dataset for removal.
For vulnerability reporting see [`SECURITY.md`](SECURITY.md); for the
optional hydrate stage's safety model see [`HYDRATING.md`](HYDRATING.md).

## The catalog is provided "as is"

`sources.json` is a curated **catalog** of public datasets — it documents
*where to fetch* upstream data, not the data itself. The URLs it points at
are third-party resources outside our control. Things that can happen to
those resources without notice:

- **Link rot** — the URL stops resolving or moves.
- **Content drift** — the bytes behind a URL change, sometimes silently.
  `expect.rows` and `expect.sha256` catch many cases at validate time, but
  not all (and `--loose` explicitly relaxes them).
- **Supply-chain compromise** — an upstream could be attacked and serve
  swapped-in content. We have no way to detect this in advance.

Raincloud is provided **AS IS**, without warranties of any kind. Treat
every fetch as third-party content arriving over the public internet —
not as something we have audited byte-for-byte.

## Content and association

We make no claim of association with, endorsement of, or curation
authority over the underlying assets fetched via `sources.json`. The
catalog points at upstream sources (publishers, hosts, dataset
aggregators); the bytes returned by those URLs are produced and
maintained by the upstream parties, not by us.

Some entries are broad-web crawls (FineWeb, Common-Crawl-derived
corpora, and similar). We mark such entries with a
`license.scrape_advisory` field and surface them with a ⚠ glyph in
[`docs/v1/datasets.md`](docs/v1/datasets.md) because we view them as
elevated-risk for unaudited or low-provenance content. The honest
reality is broader: **any** fetched file may contain questionable or
offensive material — upstream providers do their own quality and
moderation work to varying degrees, and we do not re-audit it.

If you encounter content in a fetched file that you believe warrants
removing the entry from `sources.json`, **open a PR** with the removal
or email `raincloud@spiraldb.com` with subject prefix `[compliance]`.
We will review and act in good faith.

## License diligence

Each dataset entry declares its license under `license.spdx`. We rely on
the upstream publisher's own declaration of license and redistribution
permission. We have tried to play it safe — and to prefer datasets whose
license would *permit* redistribution even though we don't currently
redistribute — but mistakes are possible.

If you believe a dataset's license is misrepresented or that a dataset
shouldn't be listed, see [Reporting](#reporting) below.

## Reporting

If a dataset should be removed (license, copyright, takedown, content
concerns, or any other reason):

- **Open a GitHub issue** at
  [github.com/spiraldb/raincloud/issues](https://github.com/spiraldb/raincloud/issues)
  with a short explanation of the concern.
- **Or email** `raincloud@spiraldb.com` with subject prefix `[compliance]`.
  Anonymous reports are welcome.

For security vulnerabilities specifically, use the channel in
[`SECURITY.md`](SECURITY.md), not this one.

## User responsibility

Running `python -m scripts.pipeline.fetch <slug>` (or `build`, which calls
fetch) makes HTTP requests against the URLs in `sources.json` and writes
the returned bytes to local disk. That decision rests with the user.

We have mitigated what we reasonably can:

- HTTPS-only delivery for HTTP fetches.
- Optional content-hash verification (`expect.sha256`) and row-count
  validation (`expect.rows`), enforced unless the user passes `--loose`.
- A two-flag bypass on the optional hydrate stage so a single
  accidentally-typed flag can't open the safety filter — see
  [`HYDRATING.md`](HYDRATING.md).
- Static manifest validation (`scripts.pipeline.validate_manifest`) that
  runs in CI on every change to `sources.json`.

We cannot guarantee against a future supply-chain attack on any of the
upstream sources we link to. We rely on the community — including you,
if you're reading this — to keep the links safe and current.

## Stewardship

`sources.json` is meant to be an authoritative "yellow pages" for
high-quality public datasets — a baseline the community can trust and
build on. We will steward it carefully (validating new entries, removing
dead or problematic ones) and hope contributors will treat it the same
way. That mutual care is what keeps a community-maintained catalog
useful over time.
