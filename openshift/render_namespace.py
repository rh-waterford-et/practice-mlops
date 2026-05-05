#!/usr/bin/env python3
"""Rewrite lineage-openshift-ai.yaml for another OpenShift project (IS path + metadata.namespace + MinIO DNS).

Usage:
    python3 openshift/render_namespace.py openshift/lineage-openshift-ai.yaml fkm

Does not alter experiment names like customer_churn_lineage."""

from __future__ import annotations

import pathlib
import re
import sys


def render(text: str, ns: str) -> str:
    text = re.sub(
        r"(^kind:\s*Namespace\s*\nmetadata:\s*\n\s*name:\s*)lineage(\s*$)",
        rf"\g<1>{ns}\2",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"^(\s*namespace:\s*)lineage(\s*)$",
        rf"\g<1>{ns}\2",
        text,
        flags=re.MULTILINE,
    )
    text = text.replace("mlflow-minio.lineage.svc", f"mlflow-minio.{ns}.svc")
    text = text.replace(
        "openshift-image-registry.svc:5000/lineage/",
        f"openshift-image-registry.svc:5000/{ns}/",
    )
    return text


def main() -> None:
    if len(sys.argv) != 3:
        print("usage: render_namespace.py <manifest.yaml> <namespace>", file=sys.stderr)
        sys.exit(2)
    path = pathlib.Path(sys.argv[1])
    ns = sys.argv[2]
    text = path.read_text()
    sys.stdout.write(render(text, ns))


if __name__ == "__main__":
    main()
