#!/usr/bin/env python3
"""
Helpers for openshift/lineage-openshift-ai.yaml (multi-document).

Used by deploy.sh (infra vs Jobs) and deploy-dsp.sh (DSPA objects only).
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys


def _split_docs(raw: str) -> list[str]:
    docs: list[str] = []
    for chunk in re.split(r"^---\s*$", raw, flags=re.M):
        chunk = chunk.strip()
        if not chunk or "apiVersion:" not in chunk:
            continue
        docs.append(chunk)
    return docs


def _kind(doc: str) -> str | None:
    m = re.search(r"^kind:\s*(\S+)\s*$", doc, re.M)
    return m.group(1) if m else None


def _metadata_name(doc: str) -> str | None:
    m = re.search(r"^  name:\s*(\S+)\s*$", doc, re.M)
    return m.group(1) if m else None


def load_manifest(path: pathlib.Path) -> list[str]:
    if str(path) == "-":
        raw = sys.stdin.read()
    else:
        raw = path.read_text()
    return _split_docs(raw)


def main() -> None:
    here = pathlib.Path(__file__).resolve().parent
    default_path = here / "lineage-openshift-ai.yaml"

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "-f",
        "--manifest",
        type=pathlib.Path,
        default=default_path,
        help="path to lineage-openshift-ai.yaml",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("emit-infra", help="stdout: all documents except batch/v1 Job")

    sub.add_parser("emit-dspa", help="stdout: DSPA secret + DataSciencePipelinesApplication only")

    p_mj = sub.add_parser(
        "materialize-jobs",
        help="write each Job to DIR/job-<name>.yaml; print DIR on stdout",
    )
    p_mj.add_argument("dir", type=pathlib.Path, help="output directory (created)")

    args = ap.parse_args()
    path: pathlib.Path = args.manifest
    if str(path) != "-" and not path.is_file():
        print(f"not found: {path}", file=sys.stderr)
        sys.exit(1)
    docs = load_manifest(path)

    if args.cmd == "emit-infra":
        infra = [d for d in docs if _kind(d) != "Job"]
        sys.stdout.write("\n---\n".join(infra) + "\n")
        return

    if args.cmd == "emit-dspa":
        picked: list[str] = []
        for d in docs:
            k = _kind(d)
            n = _metadata_name(d)
            if k == "Secret" and n == "dashboard-dspa-secret":
                picked.append(d)
            if k == "DataSciencePipelinesApplication" and n == "dspa":
                picked.append(d)
        sys.stdout.write("\n---\n".join(picked) + "\n")
        return

    if args.cmd == "materialize-jobs":
        d: pathlib.Path = args.dir
        d.mkdir(parents=True, exist_ok=True)
        for doc in docs:
            if _kind(doc) != "Job":
                continue
            name = _metadata_name(doc) or "unknown"
            (d / f"job-{name}.yaml").write_text(doc.rstrip() + "\n")
        print(d)
        return

    raise SystemExit(f"unknown cmd {args.cmd!r}")


if __name__ == "__main__":
    main()
