# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Publish built artifacts, gated on snapshot sha256 match.

    python -m raincloud.pipeline.publish <slug>... --store /path/to/store --catalogs /path/to/catalogs
    python -m raincloud.pipeline.publish <slug>... --mirror s3://bucket/prefix
    python -m raincloud.pipeline.publish --all --mirror file:///tmp/mirror --dry-run

`--store` places artifacts in a machine's shared content store (one copy per
artifact, at its artifact key) and `--catalogs` then releases the catalog by
rewriting latest.json. `--mirror` uploads off-machine.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
import uuid
from contextlib import ExitStack
from pathlib import Path

import fsspec

from raincloud._cache import EXT, sha256_file
from raincloud._locking import locked
from raincloud._resolve import artifact_key
from raincloud._transport import filesystem_url

from .lifecycle import operation_lock
from .selection import select_or_exit
from .spec import load_manifest
from .spec import outputs_root as _outputs_root


class PublishMismatch(Exception):
    """On-disk artifact sha256 disagrees with the snapshot."""


# Every artifact format the loader can fetch (`_cache.EXT`). The canonical
# .arrow.zstd leads because every exporter reads from it; it is published and
# sha-gated like the export formats. A format absent on disk is skipped.
_PUBLISH_FORMATS = ("arrow", *(f for f in EXT if f != "arrow"))


def scrape_advisory_slugs(manifest, slugs):
    """Subset of `slugs` whose license carries a non-null `scrape_advisory`.

    These aggregate or reference content whose underlying licenses have not been
    cleared for redistribution (public-web scrapes, Common Crawl derivatives,
    Amazon-Conditions-of-Use-governed review corpora). `publish` refuses them by
    default: building and using such artifacts locally is the customary research
    posture, but uploading them to a shared mirror is the one act those terms
    actually forbid. Tolerant of specs with no `license` block (test fixtures).
    """
    by_slug = {d["slug"]: d for d in manifest.get("datasets", [])}
    return [s for s in slugs
            if (by_slug.get(s, {}).get("license") or {}).get("scrape_advisory")]


def no_redistribution_slugs(manifest, slugs):
    """Subset of `slugs` whose license sets `redistribution_permitted` to False.

    An independent gate from `scrape_advisory_slugs`: the advisory flags a *gap*
    between an aggregator's declared license and uncleared underlying content,
    while this is the spec stating outright that the license does not grant
    redistribution. A slug can trip either or both (the Amazon Reviews corpus
    trips both); each gate has its own override, so clearing one never silently
    clears the other. Tolerant of specs with no `license` block (test fixtures).
    """
    by_slug = {d["slug"]: d for d in manifest.get("datasets", [])}
    return [s for s in slugs
            if (by_slug.get(s, {}).get("license") or {}).get("redistribution_permitted")
            is False]


def plan_uploads(slugs, snapshot, *, outputs_root: Path, version: int = 1, verified_digests=None, store=None,
                 snapshot_path: Path | None = None):
    """Return [(local_path, remote_key)] for present artifacts.

    `outputs_root` is the UNVERSIONED `outputs/` directory; the `v{version}`
    segment is supplied by the loader's `artifact_key` (so local path, remote
    key, and the path the loader reads are derived from ONE source). `version`
    is threaded from the snapshot's/manifest's schema_version so a v2 catalog
    uploads `v2/...` keys (defaults to 1, matching the existing v1 callers). An
    artifact whose on-disk sha256 disagrees with the snapshot raises
    PublishMismatch; an artifact with no snapshot checksum yet (None) is
    included without a gate (e.g. a freshly built, not-yet-snapshotted slug).
    `snapshot_path` only names the snapshot in that error.
    """
    slug_snaps = snapshot.get("slugs", {})
    plan: list[tuple[Path, str]] = []
    for slug in slugs:
        snap = slug_snaps.get(slug, {})
        for fmt in _PUBLISH_FORMATS:
            key = artifact_key(slug, fmt, version)  # "v{n}/<slug>/<fmt>/<slug>.<ext>"
            local = outputs_root / key
            if not local.exists():
                continue
            if store is not None and (store / key).is_file() and os.path.samefile(local, store / key):
                # Already in the store: not re-hashed. Placed by an earlier publish,
                # it was sha-checked then; when data_dir IS the store (a build
                # straight into it) nothing sha-checked it, and --catalogs only
                # checks its size.
                continue
            expected = snap.get(f"{fmt}_sha256")
            actual = sha256_file(local)
            if expected is not None and actual != expected:
                where = snapshot_path or "the catalog snapshot"
                raise PublishMismatch(
                    f"{slug}/{fmt}: on-disk sha256 {actual[:12]}… != {expected[:12]}… "
                    f"recorded in {where}. Only bytes the catalog records can be "
                    f"published under it, and a local build of someone else's "
                    f"catalog legitimately differs. A maintainer releasing a "
                    f"rebuild regenerates the snapshot first with `python -m "
                    f"raincloud.pipeline.docs` (a file this install built takes its "
                    f"sha and writer from the build record; `snapshot --rehash` is "
                    f"only for a file with no build record) and commits it."
                )
            if verified_digests is not None:
                verified_digests[local] = actual
            plan.append((local, key))
    return plan


def _place(local: Path, dest: Path, *, releasing: bool = True) -> bool:
    """Put verified `local` at `dest` in a content store; False if already there.

    A hard link on the same filesystem (instant, shared inode), else a copy;
    either way the artifact appears by rename, so readers see the old file or
    the new one. The caller has already checked `local` against the catalog.
    Replacing a different file is announced when no catalog release follows
    (`releasing=False`): a released catalog naming that key may now disagree.
    """
    if dest.is_file() and os.path.samefile(local, dest):
        return False
    if dest.is_file() and not releasing:
        print(f"  [warn] replacing {dest}: a released catalog that names it keeps its "
              "recorded size/sha until you release again with --catalogs", file=sys.stderr)
    dest.parent.mkdir(parents=True, exist_ok=True)
    incoming = dest.parent / f".{dest.name}.{uuid.uuid4().hex}.incoming"
    try:
        try:
            os.link(local, incoming)
        except OSError:
            shutil.copy2(local, incoming)
        os.replace(incoming, dest)
    finally:
        incoming.unlink(missing_ok=True)
    return True


def missing_from_store(snapshot: dict, store: Path, version: int,
                       planned: dict[str, Path] | None = None) -> list[str]:
    """Artifact keys the snapshot names that `store` will not hold at that size.

    A released catalog must only name what the store can serve; otherwise
    `load` reports a prepared dataset as missing on every machine using it.
    `planned` maps keys about to be placed to their local source, which counts
    as present at its own size, so the check can run BEFORE anything is placed.
    """
    planned = planned or {}
    missing = []
    for slug, snap in snapshot.get("slugs", {}).items():
        for fmt in _PUBLISH_FORMATS:
            nbytes, sha = snap.get(f"{fmt}_bytes"), snap.get(f"{fmt}_sha256")
            if nbytes is None and sha is None:
                continue
            key = artifact_key(slug, fmt, version)
            path = planned.get(key, store / key)
            if not path.is_file() or (nbytes is not None and path.stat().st_size != nbytes):
                missing.append(key)
    return missing


def _upload(local: Path, target: str, *, expected_sha256: str | None = None) -> None:
    """Stream `local` to the fsspec URL `target` via a temp key + rename.

    Writing straight to the canonical key would leave a truncated object there
    on a mid-stream crash, which the loader's mirror path would then warn-and-
    adopt (non-strict) or adopt silently (no sha). Uploading to `<key>.<uuid>.part`
    and renaming means the final key only ever appears complete.
    """
    fs, path = fsspec.core.url_to_fs(filesystem_url(target))
    parent = path.rsplit("/", 1)[0] if "/" in path else ""
    if parent:
        # url_to_fs's LocalFileSystem defaults to auto_mkdir=False; object
        # stores treat this as a no-op. Guard so a fresh mirror prefix works.
        try:
            fs.makedirs(parent, exist_ok=True)
        except (FileExistsError, NotImplementedError):
            pass
    tmp = f"{path}.{uuid.uuid4().hex}.part"
    try:
        digest = hashlib.sha256()
        with open(local, "rb") as src, fs.open(tmp, "wb") as out:
            for chunk in iter(lambda: src.read(1024 * 1024), b""):
                digest.update(chunk)
                out.write(chunk)
        if expected_sha256 is not None and digest.hexdigest() != expected_sha256:
            raise PublishMismatch(f"{local}: source changed during publication")
        fs.mv(tmp, path)
    except Exception:
        # Best-effort cleanup of the remote temp object. A failure here must not
        # replace the original exception, but it must not vanish either: a
        # leftover `.part` on the mirror is the thing someone has to go delete.
        try:
            if fs.exists(tmp):
                fs.rm(tmp)
        except Exception as cleanup_error:  # noqa: BLE001 — never mask the original
            print(f"  [warn] could not remove partial upload {tmp}: "
                  f"{type(cleanup_error).__name__}: {cleanup_error}", file=sys.stderr)
        raise


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="python -m raincloud.pipeline.publish",
        description="Publish built artifacts into this machine's shared store "
                    "(--store, optionally releasing a catalog) or to an off-machine "
                    "mirror (--mirror).")
    ap.add_argument("slugs", nargs="*")
    ap.add_argument("--all", action="store_true")
    target = ap.add_mutually_exclusive_group(required=True)
    target.add_argument("--store", type=Path,
                        help="this machine's shared content store, e.g. /path/to/store")
    target.add_argument("--mirror", help="off-machine fsspec base, e.g. s3://b/p")
    ap.add_argument("--catalogs", type=Path,
                    help="with --store: pack directory whose latest.json is moved to this catalog")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--allow-scrape-advisory", action="store_true",
        help="publish slugs whose license carries a scrape_advisory (refused by "
             "default — their underlying content is not cleared for redistribution)")
    ap.add_argument(
        "--allow-no-redistribution", action="store_true",
        help="publish slugs whose license sets redistribution_permitted=false "
             "(refused by default — the license does not grant redistribution)")
    return ap


def _main(argv=None) -> int:
    # Parse first: `--help` and usage errors must not resolve a catalog or wait
    # on the data_dir write lock a build may hold for hours. A dry run writes
    # nothing, so it takes no lock at all.
    ap = _parser()
    args = ap.parse_args(argv)
    if args.all and args.slugs:
        ap.error("pass slugs or --all, not both")
    if not args.all and not args.slugs:
        ap.error("pass slugs or --all")
    if args.catalogs and not args.store:
        ap.error("--catalogs releases a catalog over a --store; pass --store")
    if args.mirror and args.mirror.split("://", 1)[0].lower() in ("http", "https"):
        # Refused before any planning or lock: fsspec can read over HTTP but
        # not write, and the failure would otherwise come mid-upload.
        ap.error(f"--mirror {args.mirror}: an http(s) mirror is read-only; publish to a writable "
                 "store (s3://, gs://, file://, ...) and serve it over HTTP from there")
    if args.dry_run:
        from raincloud.catalogs import current, operation, resolve_context
        from raincloud.config import get_config
        config = get_config()
        with operation(config, current() or resolve_context(config)):
            return _run(ap, args)
    with operation_lock():
        return _run(ap, args)


def _run(ap, args) -> int:
    manifest = load_manifest()
    # The license gates cannot speak about a slug with no spec, so every name
    # is checked first: an unknown one exits 2 with a did-you-mean, as in every
    # stage CLI. There is no legitimate way to publish a slug with no recipe
    # (its artifact key and license both come from the manifest), so this has
    # no override. `--all` includes hydrated datasets: publishing reads only
    # what is already built.
    slugs = [spec["slug"] for spec in
             select_or_exit(ap, manifest, args.slugs, all_=args.all, include_hydrated=True, quiet=True)]

    # Default-block on two independent license gates: a mirror upload IS
    # redistribution, and neither a scrape_advisory nor redistribution_permitted=
    # false clears it. Each gate has its own --allow-* override, so clearing one
    # never silently clears the other (a slug tripping both needs both flags).
    # --all skips blocked slugs and keeps going; an explicit publish that leaves
    # nothing to upload fails loudly so it doesn't read as a no-op success.

    # The license gates guard redistribution off this machine. A --store is
    # the machine's own shared data area: its users already get every slug
    # the catalog names, so the gates do not apply there.
    if args.store and (args.allow_scrape_advisory or args.allow_no_redistribution):
        print("note: the license gates apply to --mirror only; --allow-* has no "
              "effect with --store", file=sys.stderr)
    gates = () if args.store else (
        (scrape_advisory_slugs(manifest, slugs), args.allow_scrape_advisory,
         "license carries a scrape_advisory — underlying content not cleared for "
         "redistribution", "--allow-scrape-advisory"),
        (no_redistribution_slugs(manifest, slugs), args.allow_no_redistribution,
         "license sets redistribution_permitted=false", "--allow-no-redistribution"),
    )
    blocked: set[str] = set()
    for hit, allowed, reason, flag in gates:
        if allowed:
            continue
        for s in sorted(hit):
            print(f"refusing {s}: {reason} (pass {flag} to override)", file=sys.stderr)
        blocked.update(hit)
    if blocked:
        slugs = [s for s in slugs if s not in blocked]
        if not slugs:
            print(f"refused all {len(blocked)} requested slug(s); nothing to publish",
                  file=sys.stderr)
            return 1

    # Gate against the selected catalog's snapshot, the one the loader trusts.
    # The version is the manifest's: the local artifacts live under
    # outputs/v{manifest version} and artifact_key must re-add that same segment.
    from raincloud.catalogs import current, resolve_context
    context = current() or resolve_context()
    if context.snapshot_path is None:
        # A snapshot for another schema_version is dropped by resolve_context,
        # which would silently turn the sha gate off for every artifact.
        print(f"refusing to publish: the selected catalog ({context.source}) has no "
              f"snapshot for schema_version {manifest.get('schema_version')!r}; expected "
              f"docs/v{manifest.get('schema_version')}/snapshot.json or RAINCLOUD_SNAPSHOT",
              file=sys.stderr)
        return 1
    snapshot = context.snapshot
    version = manifest.get("schema_version")
    if type(version) is not int:
        raise SystemExit("manifest declares no schema_version; refusing to guess an upload namespace")
    digests = {}
    try:
        plan = plan_uploads(slugs, snapshot,
                            outputs_root=_outputs_root(manifest).parent, version=version,
                            verified_digests=digests, snapshot_path=context.snapshot_path,
                            store=args.store.expanduser().absolute() if args.store else None)
    except PublishMismatch as exc:
        print(f"refusing to publish: {exc}", file=sys.stderr)
        return 1
    if not args.all:
        # A slug named on the command line with nothing built here is a mistake,
        # not a successful publish of zero files.
        root = _outputs_root(manifest).parent
        unbuilt = [s for s in slugs if not any((root / artifact_key(s, fmt, version)).is_file()
                                               for fmt in _PUBLISH_FORMATS)]
        if unbuilt:
            print(f"refusing to publish: nothing built for {', '.join(unbuilt)} under {root}; "
                  f"build first (`raincloud build <slug>`)", file=sys.stderr)
            return 1
    if args.store:
        return _publish_store(args, plan, snapshot, version)
    # Pin the validated bytes before any upload can interleave with a writer.
    base = args.mirror.rstrip("/")
    for local, key in plan:
        target = f"{base}/{key}"
        print(("DRY " if args.dry_run else "") + f"{local} -> {target}")
        if not args.dry_run:
            _upload(local, target, expected_sha256=digests[local])
    print(f"{'planned' if args.dry_run else 'published'} {len(plan)} artifact(s)")
    return 0


def _publish_store(args, plan, snapshot, version) -> int:
    from raincloud.catalogs import current, release, resolve_context
    from raincloud.config import get_config

    store = args.store.expanduser().absolute()
    placed = 0
    with ExitStack() as stack:
        # The operation lock already holds the configured data_dir's lock; take
        # the store's only when it is a different directory.
        if not args.dry_run and store.resolve() != get_config().data_dir.resolve():
            store.mkdir(parents=True, exist_ok=True)
            stack.enter_context(locked(store / ".raincloud-write.lock"))
        if args.catalogs:
            # Refuse BEFORE placing anything: store keys are not content-addressed,
            # so a placement followed by a refused release would leave the store
            # serving bytes the live catalog does not name.
            missing = missing_from_store(snapshot, store, version,
                                         planned={key: local for local, key in plan})
            if missing:
                for key in missing[:20]:
                    print(f"  missing from store: {key}", file=sys.stderr)
                print(f"refusing to release the catalog: it names {len(missing)} artifact(s) "
                      f"{store} would not hold; publish those slugs first", file=sys.stderr)
                return 1
        for local, key in plan:
            dest = store / key
            if args.dry_run:
                print(f"DRY {local} -> {dest}")
                continue
            if _place(local, dest, releasing=bool(args.catalogs)):
                placed += 1
                print(f"{local} -> {dest}")
        print(f"{'planned' if args.dry_run else 'placed'} {len(plan) if args.dry_run else placed} "
              f"artifact(s) in {store}")
        if not args.catalogs:
            return 0
        bundle = (current() or resolve_context()).bundle
        if args.dry_run:
            print(f"DRY release catalog {bundle.revision} -> {args.catalogs / 'latest.json'}")
            return 0
        print(f"released catalog {release(bundle, args.catalogs.expanduser().absolute())}")
        return 0


def main(argv=None) -> int:
    return _main(argv)


if __name__ == "__main__":
    sys.exit(main())
