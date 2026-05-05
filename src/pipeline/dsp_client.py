"""Shared Kubeflow / OpenShift Data Science Pipelines (DSP) client helpers."""

from __future__ import annotations

import os
import subprocess
import sys

import urllib3
from kfp.client import Client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_NAMESPACE = os.environ.get("OPENSHIFT_APP_NAMESPACE", "lineage")
DEFAULT_DSPA_NAME = "dspa"


def get_dsp_route_host(namespace: str = DEFAULT_NAMESPACE, dspa_name: str = DEFAULT_DSPA_NAME) -> str:
    """Return ``https://<route-host>`` for the ds-pipeline route, or internal service URL."""
    result = subprocess.run(
        [
            "oc",
            "get",
            "route",
            f"ds-pipeline-{dspa_name}",
            "-n",
            namespace,
            "-o",
            "jsonpath={.spec.host}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return f"https://ds-pipeline-{dspa_name}.{namespace}.svc:8888"
    host = result.stdout.strip()
    if host.startswith("http://") or host.startswith("https://"):
        return host
    return f"https://{host}"


def get_oc_token() -> str:
    result = subprocess.run(
        ["oc", "whoami", "-t"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("ERROR: Could not get token. Run 'oc login' first.", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def connect_dsp_client(
    *,
    namespace: str = DEFAULT_NAMESPACE,
    dspa_name: str = DEFAULT_DSPA_NAME,
) -> Client:
    """KFP client for the OpenShift AI pipeline API (TLS, bearer token from ``oc``)."""
    return Client(
        host=get_dsp_route_host(namespace, dspa_name),
        existing_token=get_oc_token(),
        ssl_ca_cert=False,
    )
